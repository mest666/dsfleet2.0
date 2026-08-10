"""Orchestrator — owns the supervisor registry and the fleet-level control surface.

Public API consumed by the telemetry layer:
    await orch.start() / stop()
    await orch.pause_all() / resume_all()
    await orch.restart(instance_id) / stop_instance(id) / start_instance(id)
    orch.snapshot() -> FleetSnapshot
    orch.metrics()  -> FleetMetrics
    orch.get(id)    -> InstanceSupervisor | None
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import time
from pathlib import Path
from typing import Optional

from .config import AppConfig
from .events import EventBus
from .models import (FleetMetrics, FleetSnapshot, HealthStatus, InstanceState,
                     Severity)
from .supervisor import InstanceSupervisor
from .window import WindowManager

log = logging.getLogger("dsfleet.orchestrator")

__all__ = ["Orchestrator"]


class Orchestrator:
    def __init__(self, cfg: AppConfig, bus: Optional[EventBus] = None) -> None:
        self.cfg = cfg
        self.bus = bus or EventBus()
        self.windows = WindowManager(cfg.window_layout)
        self.supervisors: dict[str, InstanceSupervisor] = {
            i.id: InstanceSupervisor(i, cfg, self.bus, self.windows) for i in cfg.instances
        }
        self.paused: bool = False
        self.started_at: Optional[float] = None
        self._tasks: list[asyncio.Task[None]] = []
        self._breaches: tuple[str, ...] = ()
        self.preflight_report = None
        self._stopping = asyncio.Event()
        self._state_path: Path = cfg.state_dir / "fleet.json"

    # ================================================================ lifecycle

    async def start(self) -> None:
        self.cfg.ensure_dirs()
        self.started_at = time.time()
        self._stopping.clear()

        self._run_preflight()
        self._apply_affinity_plan()

        cells = [(s.cfg.id, s.cfg.grid_cell) for s in self.supervisors.values()
                 if s.cfg.grid_cell is not None]
        if cells:
            log.info("layout:\n%s", self.windows.describe(cells))

        enabled = [s for s in self.supervisors.values() if s.cfg.enabled]
        log.info("starting %d/%d instances", len(enabled), len(self.supervisors))
        results = await asyncio.gather(*(s.start() for s in enabled), return_exceptions=True)
        for sup, res in zip(enabled, results):
            if isinstance(res, BaseException):
                log.error("[%s] failed to start: %r", sup.cfg.id, res)
                self.bus.emit("lifecycle", f"failed to start: {res!r}",
                              severity=Severity.ERROR, instance_id=sup.cfg.id)

        self._tasks = [
            asyncio.create_task(self._evaluate_loop(), name="fleet:evaluate"),
            asyncio.create_task(self._persist_loop(), name="fleet:persist"),
        ]
        self.bus.emit("fleet", f"orchestrator online with {len(enabled)} instances",
                      severity=Severity.INFO)

    async def stop(self) -> None:
        self._stopping.set()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._tasks.clear()

        log.info("stopping %d supervisors", len(self.supervisors))
        results = await asyncio.gather(
            *(s.stop() for s in self.supervisors.values()), return_exceptions=True)
        for sup, res in zip(self.supervisors.values(), results):
            if isinstance(res, BaseException):
                log.error("[%s] error during stop: %r", sup.cfg.id, res)
        with contextlib.suppress(Exception):
            self._persist()
        self.bus.emit("fleet", "orchestrator offline", severity=Severity.INFO)

    # ================================================================ control

    def get(self, instance_id: str) -> Optional[InstanceSupervisor]:
        return self.supervisors.get(instance_id)

    def ids(self) -> list[str]:
        return list(self.supervisors)

    async def pause_all(self) -> dict[str, bool]:
        self.paused = True
        results = await asyncio.gather(
            *(s.pause() for s in self.supervisors.values()), return_exceptions=True)
        out = {sid: (r is True) for sid, r in zip(self.supervisors, results)}
        suspended = sum(1 for v in out.values() if v)
        self.bus.emit("fleet",
                      f"fleet PAUSED ({suspended}/{len(out)} processes suspended)",
                      severity=Severity.WARNING, data={"suspended": suspended})
        return out

    async def resume_all(self) -> None:
        self.paused = False
        await asyncio.gather(
            *(s.resume() for s in self.supervisors.values()), return_exceptions=True)
        self.bus.emit("fleet", "fleet RESUMED", severity=Severity.INFO)

    async def pause_instance(self, instance_id: str) -> bool:
        sup = self.get(instance_id)
        if sup is None:
            return False
        await sup.pause()
        return True

    async def resume_instance(self, instance_id: str) -> bool:
        sup = self.get(instance_id)
        if sup is None:
            return False
        await sup.resume()
        return True

    async def restart(self, instance_id: str, reason: str = "manual") -> bool:
        sup = self.get(instance_id)
        if sup is None:
            return False
        await sup.restart(reason)
        return True

    async def restart_all(self, reason: str = "manual fleet restart") -> int:
        targets = [s for s in self.supervisors.values() if s.cfg.enabled]
        await asyncio.gather(*(s.restart(reason) for s in targets), return_exceptions=True)
        self.bus.emit("fleet", f"fleet restart issued for {len(targets)} instances",
                      severity=Severity.WARNING)
        return len(targets)

    async def stop_instance(self, instance_id: str) -> bool:
        sup = self.get(instance_id)
        if sup is None:
            return False
        await sup.stop()
        return True

    async def start_instance(self, instance_id: str) -> bool:
        sup = self.get(instance_id)
        if sup is None:
            return False
        await sup.start()
        return True

    # ================================================================ preflight & affinity

    def _run_preflight(self) -> None:
        pf = self.cfg.preflight
        if not pf.enabled:
            return
        try:
            from .preflight import PreflightConfig, Verdict, run_preflight
            report = run_preflight(
                instance_count=sum(1 for i in self.cfg.instances if i.enabled),
                runtime_dir=self.cfg.runtime_dir,
                cfg=PreflightConfig(
                    per_instance_mb=pf.per_instance_mb,
                    os_reserve_gb=pf.os_reserve_gb,
                    warn_headroom_gb=pf.warn_headroom_gb,
                    min_pagefile_gb=pf.min_pagefile_gb,
                    min_disk_free_gb=pf.min_disk_free_gb,
                ),
            )
        except Exception:
            log.exception("preflight failed to run; continuing without it")
            return

        self.preflight_report = report
        log.info("preflight:\n%s", report.render())
        severity = {"ok": Severity.INFO, "warn": Severity.WARNING,
                    "fail": Severity.CRITICAL}[report.verdict.value]
        self.bus.emit("fleet", f"preflight {report.verdict.value.upper()}: "
                               f"{'; '.join(report.reasons[:3])}",
                      severity=severity,
                      data={"verdict": report.verdict.value, "reasons": report.reasons})

        if report.verdict is Verdict.FAIL and pf.block_on_fail:
            raise RuntimeError(
                "preflight FAILED and preflight.block_on_fail is true:\n"
                + report.render()
                + "\n\nSet preflight.block_on_fail=false to override.")

    def _apply_affinity_plan(self) -> None:
        aff = self.cfg.affinity
        if not aff.enabled or aff.strategy == "none":
            return
        try:
            from .affinity import AffinityAllocator
            allocator = AffinityAllocator(
                strategy=aff.strategy,
                reserve_cpus=aff.reserve_cpus,
                cpus_per_instance=aff.cpus_per_instance,
            )
            ids = [s.cfg.id for s in self.supervisors.values() if s.cfg.enabled]
            plans = allocator.plan(ids, explicit=dict(aff.explicit), priority=aff.priority)
            for plan in plans:
                sup = self.supervisors.get(plan.instance_id)
                if sup is not None:
                    sup.affinity_cpus = plan.cpus
                    sup.priority = plan.priority
            log.info("cpu affinity:\n%s", allocator.describe(plans))
        except Exception:
            log.exception("affinity planning failed; instances will run unpinned")

    # ================================================================ emergency

    async def emergency_stop(self, reason: str = "EMERGENCY STOP") -> int:
        """Kill every managed process immediately — no graceful shutdown, no restarts.

        Sets the shutdown flag on every supervisor first so nothing relaunches into the
        gap, then kills all process trees concurrently.
        """
        log.critical("EMERGENCY STOP: %s", reason)
        self.bus.emit("fleet", f"EMERGENCY STOP — {reason}", severity=Severity.CRITICAL)
        self.paused = True

        killed = 0
        for sup in self.supervisors.values():
            sup._shutdown.set()   # noqa: SLF001 — deliberate: halt restarts first
            sup._wake.set()       # noqa: SLF001

        async def _kill(sup) -> bool:
            pid = sup.pid
            if pid is None:
                return False
            try:
                await sup._kill_tree(pid)  # noqa: SLF001
                return True
            except Exception:
                log.exception("[%s] emergency kill failed", sup.cfg.id)
                return False

        results = await asyncio.gather(
            *(_kill(s) for s in self.supervisors.values()), return_exceptions=True)
        killed = sum(1 for r in results if r is True)

        for sup in self.supervisors.values():
            sup._set_state(InstanceState.STOPPED, reason, Severity.CRITICAL)  # noqa: SLF001

        log.critical("EMERGENCY STOP complete: %d process trees killed", killed)
        self.bus.emit("fleet", f"emergency stop complete — {killed} trees killed",
                      severity=Severity.CRITICAL)
        return killed

    # ================================================================ metrics

    def metrics(self) -> FleetMetrics:
        snaps = [s.snapshot() for s in self.supervisors.values()]
        total = len(snaps)
        counts = {st: 0 for st in InstanceState}
        for s in snaps:
            counts[s.state] += 1
        healthy = sum(1 for s in snaps if s.health is HealthStatus.HEALTHY)
        degraded = sum(1 for s in snaps if s.health is HealthStatus.DEGRADED)
        unhealthy = sum(1 for s in snaps if s.health is HealthStatus.UNHEALTHY)
        restarts_hour = sum(
            s._recent_restarts(3600.0) for s in self.supervisors.values())  # noqa: SLF001
        hb_ages = [s.heartbeat_age_s for s in snaps if s.heartbeat_age_s is not None]

        m = FleetMetrics(
            total=total,
            running=counts[InstanceState.RUNNING],
            healthy=healthy,
            degraded=degraded,
            unhealthy=unhealthy,
            quarantined=counts[InstanceState.QUARANTINED],
            paused=counts[InstanceState.PAUSED],
            stopped=counts[InstanceState.STOPPED],
            restarts_last_hour=restarts_hour,
            healthy_ratio=(healthy / total) if total else 0.0,
            fleet_uptime_s=0.0 if self.started_at is None else time.time() - self.started_at,
            max_heartbeat_age_s=max(hb_ages) if hb_ages else None,
            breaches=self._breaches,
        )
        return m

    def snapshot(self) -> FleetSnapshot:
        return FleetSnapshot(
            ts=time.time(),
            paused=self.paused,
            instances=[s.snapshot() for s in self.supervisors.values()],
            metrics=self.metrics(),
        )

    def _compute_breaches(self, m: FleetMetrics) -> tuple[str, ...]:
        t = self.cfg.metrics
        out: list[str] = []
        if m.total and m.healthy_ratio < t.min_healthy_ratio:
            out.append(f"healthy_ratio {m.healthy_ratio:.0%} < {t.min_healthy_ratio:.0%}")
        if m.quarantined > t.max_quarantined:
            out.append(f"quarantined {m.quarantined} > {t.max_quarantined}")
        if m.restarts_last_hour > t.max_restarts_per_hour:
            out.append(f"restarts/h {m.restarts_last_hour} > {t.max_restarts_per_hour}")
        if m.max_heartbeat_age_s is not None and m.max_heartbeat_age_s > t.max_heartbeat_age_s:
            out.append(f"heartbeat age {m.max_heartbeat_age_s:.0f}s > "
                       f"{t.max_heartbeat_age_s:.0f}s")
        return tuple(out)

    async def _evaluate_loop(self) -> None:
        interval = self.cfg.metrics.evaluate_interval_s
        previous: tuple[str, ...] = ()
        while not self._stopping.is_set():
            try:
                await asyncio.sleep(interval)
                if self.paused:
                    continue
                m = self.metrics()
                current = self._compute_breaches(m)
                self._breaches = current

                new = [b for b in current if b not in previous]
                cleared = [b for b in previous if b not in current]
                for breach in new:
                    self.bus.emit("fleet", f"threshold breach: {breach}",
                                  severity=Severity.ERROR, data=m.as_dict())
                if cleared and not current:
                    self.bus.emit("fleet", "all fleet thresholds back within limits",
                                  severity=Severity.INFO, data=m.as_dict())
                previous = current
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("evaluate loop error")

    # ================================================================ persistence

    def _persist(self) -> None:
        payload = self.snapshot().as_dict()
        tmp = self._state_path.with_suffix(".json.tmp")
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True)
            tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
            os.replace(tmp, self._state_path)
        except OSError as exc:
            log.warning("state persist failed: %s", exc)
            with contextlib.suppress(OSError):
                tmp.unlink(missing_ok=True)

    async def _persist_loop(self) -> None:
        interval = self.cfg.metrics.snapshot_persist_interval_s
        if interval <= 0:
            return
        while not self._stopping.is_set():
            try:
                await asyncio.sleep(interval)
                await asyncio.to_thread(self._persist)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("persist loop error")
