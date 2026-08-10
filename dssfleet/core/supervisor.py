"""InstanceSupervisor — one asyncio task per managed process.

State machine
-------------
    STOPPED ──start()──► STARTING ──healthy──► RUNNING ◄──────────┐
       ▲                    │                    │                │
       │                    │ launch fail        │ degraded       │ recovered
       │                    ▼                    ▼                │
       │                 RESTARTING ◄──── DEGRADED ──► UNHEALTHY ─┘
       │                    │                                │
       │                    │ crash breaker trips            │ terminate+backoff
       │                    ▼                                ▼
       └──stop()──── STOPPING            QUARANTINED     RESTARTING
                        │                                     │
                        ▼                                     ▼
                     STOPPED                              STARTING

pause() suspends the process (best-effort, psutil) and halts supervision → PAUSED.
resume() resumes it and re-enters the supervise loop.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random
import signal
import sys
import time
from collections import deque
from pathlib import Path
from typing import Deque, Optional

from .config import AppConfig, InstanceConfig
from .events import EventBus
from .health import HealthContext, HealthMonitor
from .isolation import IsolationError, LaunchSpec, build_strategy
from .models import (HealthReport, HealthStatus, InstanceSnapshot, InstanceState,
                     Severity, TERMINAL_STATES)
from .window import WindowManager

log = logging.getLogger("dsfleet.supervisor")

IS_WINDOWS = sys.platform.startswith("win")

try:  # optional: enables suspend/resume and reliable tree-kill
    import psutil  # type: ignore
    HAVE_PSUTIL = True
except ImportError:  # pragma: no cover
    psutil = None  # type: ignore
    HAVE_PSUTIL = False

__all__ = ["InstanceSupervisor"]


class InstanceSupervisor:
    def __init__(self, cfg: InstanceConfig, app: AppConfig, bus: EventBus,
                 windows: WindowManager) -> None:
        self.cfg = cfg
        self.app = app
        self.bus = bus
        self.windows = windows

        self.state: InstanceState = InstanceState.STOPPED
        self.health: HealthStatus = HealthStatus.UNKNOWN
        self.state_since: float = time.time()

        self._proc: Optional[asyncio.subprocess.Process] = None
        self._task: Optional[asyncio.Task[None]] = None
        self._log_task: Optional[asyncio.Task[None]] = None
        # Single long-lived waiter per process: repeatedly creating and cancelling
        # proc.wait() races the child watcher and can lose the real exit status.
        self._wait_task: Optional[asyncio.Task[int]] = None
        self._shutdown = asyncio.Event()
        self._wake = asyncio.Event()          # interrupts backoff sleep
        self._lock = asyncio.Lock()

        self._strategy = build_strategy(cfg.isolation)
        self._monitor = HealthMonitor(cfg.health, cfg, hang_probe=self.windows.is_hung)
        self._ctx = HealthContext(instance_id=cfg.id, startup_grace_s=cfg.startup_grace_s)

        self.window_handle: Optional[int] = None
        self.started_at: Optional[float] = None
        # Assigned by the Orchestrator from the affinity plan before start().
        self.affinity_cpus: tuple[int, ...] = ()
        self.priority: Optional[str] = None
        self.restarts_total: int = 0
        self.last_exit_code: Optional[int] = None
        self.last_error: Optional[str] = None
        self.last_report: Optional[HealthReport] = None

        self._restart_history: Deque[float] = deque(maxlen=256)
        self._backoff: float = cfg.restart.backoff_initial_s
        self._healthy_since: Optional[float] = None
        self._log_path = app.log_dir / f"{cfg.id}.log"

    # ================================================================ state

    def _set_state(self, new: InstanceState, reason: str = "",
                   severity: Optional[Severity] = None) -> None:
        if new is self.state:
            return
        old, self.state = self.state, new
        self.state_since = time.time()
        if severity is None:
            severity = {
                InstanceState.RUNNING: Severity.INFO,
                InstanceState.DEGRADED: Severity.WARNING,
                InstanceState.UNHEALTHY: Severity.ERROR,
                InstanceState.QUARANTINED: Severity.CRITICAL,
                InstanceState.RESTARTING: Severity.WARNING,
            }.get(new, Severity.INFO)
        msg = f"{old.value} → {new.value}" + (f" ({reason})" if reason else "")
        log.info("[%s] %s", self.cfg.id, msg)
        self.bus.emit("state_change", msg, severity=severity, instance_id=self.cfg.id,
                      data={"from": old.value, "to": new.value, "reason": reason,
                            "pid": self.pid})

    @property
    def pid(self) -> Optional[int]:
        return self._proc.pid if self._proc and self._proc.returncode is None else None

    @property
    def process_alive(self) -> bool:
        return self._proc is not None and self._proc.returncode is None

    @property
    def uptime_s(self) -> float:
        return 0.0 if self.started_at is None else max(0.0, time.time() - self.started_at)

    def _recent_restarts(self, window_s: Optional[float] = None) -> int:
        window = window_s if window_s is not None else self.cfg.restart.crash_window_s
        cutoff = time.time() - window
        return sum(1 for t in self._restart_history if t >= cutoff)

    def snapshot(self) -> InstanceSnapshot:
        return InstanceSnapshot(
            id=self.cfg.id,
            state=self.state,
            health=self.health,
            pid=self.pid,
            enabled=self.cfg.enabled,
            since=self.state_since,
            started_at=self.started_at,
            uptime_s=self.uptime_s,
            restarts_total=self.restarts_total,
            restarts_recent=self._recent_restarts(),
            last_exit_code=self.last_exit_code,
            last_error=self.last_error,
            heartbeat_age_s=self._ctx.last_hb_age_s,
            consecutive_failures=self._monitor.consecutive_failures,
            tags=self.cfg.tags,
        )

    # ================================================================ lifecycle

    async def start(self) -> None:
        async with self._lock:
            if self._task and not self._task.done():
                return
            self._shutdown.clear()
            self._wake.clear()
            self._task = asyncio.create_task(self._supervise(), name=f"sup:{self.cfg.id}")

    async def stop(self, *, timeout: Optional[float] = None) -> None:
        self._shutdown.set()
        self._wake.set()
        task = self._task
        if task and not task.done():
            grace = timeout or (self.cfg.shutdown_grace_s + 10.0)
            try:
                await asyncio.wait_for(asyncio.shield(task), timeout=grace)
            except asyncio.TimeoutError:
                log.warning("[%s] supervise task did not exit in %.0fs; cancelling",
                            self.cfg.id, grace)
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        await self._terminate_process(reason="stop requested")
        self._set_state(InstanceState.STOPPED, "stopped by operator")

    async def restart(self, reason: str = "manual restart") -> None:
        """Force a restart, clearing quarantine and the backoff ladder."""
        async with self._lock:
            self._backoff = self.cfg.restart.backoff_initial_s
            self._restart_history.clear()
            self._monitor.reset()
        if self.state is InstanceState.QUARANTINED or self._task is None or self._task.done():
            self._set_state(InstanceState.RESTARTING, reason)
            await self.start()
            return
        await self._terminate_process(reason=reason)
        self._wake.set()

    async def pause(self) -> bool:
        if self.state is InstanceState.PAUSED:
            return True
        suspended = await self._suspend_tree()
        self._set_state(InstanceState.PAUSED,
                        "suspended" if suspended else "supervision halted")
        return suspended

    async def resume(self) -> None:
        if self.state is not InstanceState.PAUSED:
            return
        await self._resume_tree()
        if self.process_alive:
            self._set_state(InstanceState.RUNNING, "resumed")
        else:
            self._set_state(InstanceState.RESTARTING, "resumed, process gone")
            self._wake.set()
            if self._task is None or self._task.done():
                await self.start()

    # ================================================================ supervise loop

    async def _supervise(self) -> None:
        try:
            while not self._shutdown.is_set():
                if not self.cfg.enabled:
                    self._set_state(InstanceState.STOPPED, "disabled in config")
                    return

                launched = await self._launch()
                if not launched:
                    if not await self._await_backoff("launch failed"):
                        return
                    continue

                await self._monitor_loop()

                if self._shutdown.is_set():
                    break

                # Process ended (crash, exit, or health-triggered termination).
                self._restart_history.append(time.time())
                self.restarts_total += 1

                if not self.cfg.restart.enabled:
                    self._set_state(InstanceState.STOPPED, "restart disabled")
                    return

                if self._recent_restarts() >= self.cfg.restart.crash_threshold:
                    self._set_state(
                        InstanceState.QUARANTINED,
                        f"{self._recent_restarts()} restarts in "
                        f"{self.cfg.restart.crash_window_s:.0f}s",
                        Severity.CRITICAL)
                    self.bus.emit(
                        "quarantine",
                        f"quarantined after {self._recent_restarts()} restarts; "
                        f"manual /restart {self.cfg.id} required",
                        severity=Severity.CRITICAL, instance_id=self.cfg.id)
                    return

                if not await self._await_backoff("crash/exit"):
                    return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # supervisor must never die silently
            self.last_error = f"supervisor fault: {exc!r}"
            log.exception("[%s] supervise loop crashed", self.cfg.id)
            self.bus.emit("lifecycle", f"supervisor fault: {exc!r}",
                          severity=Severity.CRITICAL, instance_id=self.cfg.id)
            self._set_state(InstanceState.QUARANTINED, "supervisor fault", Severity.CRITICAL)
        finally:
            await self._terminate_process(reason="supervise loop exit")
            if self.state not in TERMINAL_STATES:
                self._set_state(InstanceState.STOPPED, "supervise loop exit")

    async def _await_backoff(self, reason: str) -> bool:
        """Sleep the backoff interval. Returns False if shutdown was requested."""
        jitter = 1.0 + random.uniform(-self.cfg.restart.backoff_jitter,
                                      self.cfg.restart.backoff_jitter)
        delay = max(0.0, self._backoff * jitter)
        self._set_state(InstanceState.RESTARTING, f"{reason}; retry in {delay:.1f}s")
        self._backoff = min(self._backoff * self.cfg.restart.backoff_factor,
                            self.cfg.restart.backoff_max_s)
        self._wake.clear()
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self._wake.wait(), timeout=delay)
        return not self._shutdown.is_set()

    # ================================================================ launch

    async def _launch(self) -> bool:
        self._set_state(InstanceState.STARTING, "launching")
        self.health = HealthStatus.UNKNOWN
        self.window_handle = None
        self._monitor.reset()
        self._healthy_since = None

        try:
            spec: LaunchSpec = self._strategy.materialize(self.cfg)
        except IsolationError as exc:
            self.last_error = f"isolation: {exc}"
            log.error("[%s] isolation failed: %s", self.cfg.id, exc)
            self.bus.emit("lifecycle", f"isolation failed: {exc}",
                          severity=Severity.ERROR, instance_id=self.cfg.id)
            return False

        if spec.cwd and not Path(spec.cwd).is_dir():
            self.last_error = f"cwd does not exist: {spec.cwd}"
            log.error("[%s] %s", self.cfg.id, self.last_error)
            return False

        try:
            self._log_path.parent.mkdir(parents=True, exist_ok=True)
            self._rotate_log_if_needed()
            kwargs: dict = {
                "cwd": spec.cwd,
                "env": spec.env,
                "stdin": asyncio.subprocess.DEVNULL,
                "stdout": asyncio.subprocess.PIPE,
                "stderr": asyncio.subprocess.STDOUT,
            }
            if IS_WINDOWS:
                kwargs["creationflags"] = spec.creationflags
            else:
                kwargs["start_new_session"] = spec.start_new_session
            self._proc = await asyncio.create_subprocess_exec(*spec.argv, **kwargs)
        except (OSError, ValueError) as exc:
            self.last_error = f"spawn failed: {exc}"
            log.error("[%s] spawn failed: %s", self.cfg.id, exc)
            self.bus.emit("lifecycle", f"spawn failed: {exc}",
                          severity=Severity.ERROR, instance_id=self.cfg.id)
            return False

        self.started_at = time.time()
        self._ctx.started_at = self.started_at
        self.last_error = None
        log.info("[%s] launched pid=%s: %s", self.cfg.id, self._proc.pid, spec.redacted())
        for note in spec.notes:
            log.debug("[%s] %s", self.cfg.id, note)
        self.bus.emit("lifecycle", f"launched pid={self._proc.pid}",
                      severity=Severity.INFO, instance_id=self.cfg.id,
                      data={"pid": self._proc.pid, "argv": list(spec.argv)})

        self._wait_task = asyncio.create_task(self._proc.wait(), name=f"wait:{self.cfg.id}")
        self._log_task = asyncio.create_task(self._pump_logs(), name=f"log:{self.cfg.id}")
        asyncio.create_task(self._place_window(), name=f"win:{self.cfg.id}")
        asyncio.create_task(self._apply_resource_limits(self._proc.pid),
                            name=f"aff:{self.cfg.id}")
        return True

    async def _apply_resource_limits(self, pid: int) -> None:
        """Pin CPUs and set priority. Deliberately fire-and-forget: a failure here
        degrades performance but must never prevent the instance from running."""
        if not self.affinity_cpus and not self.priority:
            return
        try:
            from .affinity import apply_affinity, apply_priority
            if self.affinity_cpus:
                await asyncio.to_thread(apply_affinity, pid, self.affinity_cpus)
            if self.priority:
                await asyncio.to_thread(apply_priority, pid, self.priority)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("[%s] applying resource limits failed", self.cfg.id)

    async def _place_window(self) -> None:
        try:
            pid = self.pid
            if pid is None:
                return
            handle = await self.windows.place(pid, self.cfg.grid_cell,
                                              self.cfg.window_title_match)
            if handle is not None and self.pid == pid:
                self.window_handle = handle
                self._ctx.window_handle = handle
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("[%s] window placement failed", self.cfg.id)

    # ================================================================ logging

    def _rotate_log_if_needed(self) -> None:
        if self.cfg.log_max_bytes <= 0:
            return
        try:
            if not self._log_path.exists() or self._log_path.stat().st_size < self.cfg.log_max_bytes:
                return
            for i in range(self.cfg.log_backups, 0, -1):
                src = self._log_path.with_suffix(f".log.{i}") if i > 1 else self._log_path
                dst = self._log_path.with_suffix(f".log.{i + 1}")
                if i == self.cfg.log_backups and dst.exists():
                    dst.unlink(missing_ok=True)
                if src.exists():
                    src.replace(dst)
        except OSError as exc:
            log.warning("[%s] log rotation failed: %s", self.cfg.id, exc)

    async def _pump_logs(self) -> None:
        proc = self._proc
        if proc is None or proc.stdout is None:
            return
        written = 0
        try:
            with self._log_path.open("ab", buffering=0) as fh:
                header = f"\n=== start pid={proc.pid} at {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n"
                fh.write(header.encode())
                while True:
                    chunk = await proc.stdout.read(8192)
                    if not chunk:
                        break
                    fh.write(chunk)
                    written += len(chunk)
                    if self.cfg.log_max_bytes and written > self.cfg.log_max_bytes:
                        fh.flush()
                        break  # rotation happens on next launch
        except asyncio.CancelledError:
            raise
        except OSError as exc:
            log.warning("[%s] log pump I/O error: %s", self.cfg.id, exc)
        except Exception:
            log.exception("[%s] log pump failed", self.cfg.id)

    # ================================================================ monitoring

    async def _monitor_loop(self) -> None:
        proc = self._proc
        assert proc is not None
        interval = self.cfg.health.interval_s
        last_reapply = time.time()
        reapply_every = self.windows.layout.reapply_interval_s

        while not self._shutdown.is_set():
            if self.state is InstanceState.PAUSED:
                await asyncio.sleep(min(1.0, interval))
                continue

            # Did the process exit on its own? Poll the shared waiter, never cancel it.
            waiter = self._wait_task
            if waiter is not None:
                done, _ = await asyncio.wait({waiter}, timeout=interval)
                if done:
                    self.last_exit_code = proc.returncode
                    sev = Severity.INFO if proc.returncode == 0 else Severity.ERROR
                    self.bus.emit("lifecycle", f"process exited with code {proc.returncode}",
                                  severity=sev, instance_id=self.cfg.id,
                                  data={"exit_code": proc.returncode,
                                        "uptime_s": round(self.uptime_s, 1)})
                    await self._reap_log_task()
                    return
            else:
                await asyncio.sleep(interval)

            self._ctx.pid = self.pid
            self._ctx.process_alive = self.process_alive
            self._ctx.window_handle = self.window_handle

            try:
                report = await self._monitor.evaluate(self._ctx)
            except Exception:
                log.exception("[%s] health evaluation error", self.cfg.id)
                continue

            self.last_report = report
            previous = self.health
            self.health = report.status

            if report.status is HealthStatus.HEALTHY:
                if self._healthy_since is None:
                    self._healthy_since = time.time()
                elif (time.time() - self._healthy_since) >= self.cfg.restart.healthy_reset_s:
                    if self._backoff != self.cfg.restart.backoff_initial_s:
                        log.info("[%s] stable for %.0fs; backoff ladder reset",
                                 self.cfg.id, self.cfg.restart.healthy_reset_s)
                    self._backoff = self.cfg.restart.backoff_initial_s
                self._set_state(InstanceState.RUNNING, "healthy")
            else:
                self._healthy_since = None

            if report.status is HealthStatus.DEGRADED:
                self._set_state(InstanceState.DEGRADED, report.summary())
            elif report.status is HealthStatus.UNHEALTHY:
                self._set_state(InstanceState.UNHEALTHY, report.summary(), Severity.ERROR)
                self.bus.emit("health", f"unhealthy: {report.summary()}",
                              severity=Severity.ERROR, instance_id=self.cfg.id,
                              data={"checks": [r.name for r in report.failed]})
                await self._terminate_process(reason=f"unhealthy: {report.summary()}")
                await self._reap_log_task()
                return

            if previous is not report.status and report.status is HealthStatus.HEALTHY \
                    and previous in (HealthStatus.DEGRADED, HealthStatus.UNHEALTHY):
                self.bus.emit("health", "recovered to healthy",
                              severity=Severity.INFO, instance_id=self.cfg.id)

            if reapply_every > 0 and (time.time() - last_reapply) >= reapply_every:
                last_reapply = time.time()
                if self.window_handle is not None and self.cfg.grid_cell is not None:
                    with contextlib.suppress(Exception):
                        await self.windows.reapply(self.window_handle, self.cfg.grid_cell)

    async def _reap_log_task(self) -> None:
        task = self._log_task
        self._log_task = None
        if task and not task.done():
            with contextlib.suppress(asyncio.TimeoutError, asyncio.CancelledError, Exception):
                await asyncio.wait_for(task, timeout=5.0)
            if not task.done():
                task.cancel()

    # ================================================================ process control

    async def _terminate_process(self, reason: str = "") -> None:
        proc = self._proc
        if proc is None or proc.returncode is not None:
            return
        self._set_state(InstanceState.STOPPING, reason)
        pid = proc.pid
        grace = self.cfg.shutdown_grace_s
        log.info("[%s] terminating pid=%s (%s)", self.cfg.id, pid, reason)

        # 1. polite signal
        try:
            if IS_WINDOWS:
                with contextlib.suppress(OSError, ValueError):
                    proc.send_signal(signal.CTRL_BREAK_EVENT)  # type: ignore[attr-defined]
                proc.terminate()
            else:
                if self._strategy.mode != "container":
                    with contextlib.suppress(ProcessLookupError, OSError):
                        os.killpg(os.getpgid(pid), signal.SIGTERM)
                proc.terminate()
        except ProcessLookupError:
            pass
        except Exception:
            log.exception("[%s] graceful terminate failed", self.cfg.id)

        if await self._await_exit(grace):
            self.last_exit_code = proc.returncode
            log.info("[%s] exited gracefully (code=%s)", self.cfg.id, proc.returncode)
            await self._reap_log_task()
            return
        log.warning("[%s] pid=%s ignored terminate after %.0fs; killing tree",
                    self.cfg.id, pid, grace)

        # 2. hard kill, whole tree
        await self._kill_tree(pid)
        if await self._await_exit(10.0):
            self.last_exit_code = proc.returncode
        else:
            self.last_error = f"pid {pid} unkillable"
            log.error("[%s] pid=%s survived SIGKILL", self.cfg.id, pid)
            self.bus.emit("lifecycle", f"pid {pid} could not be killed",
                          severity=Severity.CRITICAL, instance_id=self.cfg.id)
        await self._reap_log_task()

    async def _await_exit(self, timeout: float) -> bool:
        """Wait for the current process to exit. Never cancels the shared waiter."""
        proc = self._proc
        if proc is None:
            return True
        waiter = self._wait_task
        if waiter is None:
            try:
                await asyncio.wait_for(proc.wait(), timeout=timeout)
                return True
            except asyncio.TimeoutError:
                return False
        done, _ = await asyncio.wait({waiter}, timeout=timeout)
        return bool(done)

    async def _kill_tree(self, pid: int) -> None:
        def _sync_kill() -> None:
            if HAVE_PSUTIL:
                try:
                    parent = psutil.Process(pid)
                    children = parent.children(recursive=True)
                    for child in children:
                        with contextlib.suppress(psutil.Error):
                            child.kill()
                    psutil.wait_procs(children, timeout=5)
                    with contextlib.suppress(psutil.Error):
                        parent.kill()
                    return
                except psutil.NoSuchProcess:
                    return
                except psutil.Error:
                    log.debug("psutil kill failed; falling back", exc_info=True)
            if IS_WINDOWS:
                os.system(f"taskkill /F /T /PID {pid} >NUL 2>&1")
            else:
                with contextlib.suppress(ProcessLookupError, OSError):
                    os.killpg(os.getpgid(pid), signal.SIGKILL)
                with contextlib.suppress(ProcessLookupError, OSError):
                    os.kill(pid, signal.SIGKILL)

        try:
            await asyncio.wait_for(asyncio.to_thread(_sync_kill), timeout=20.0)
        except asyncio.TimeoutError:
            log.error("[%s] kill-tree timed out for pid=%s", self.cfg.id, pid)
        except Exception:
            log.exception("[%s] kill-tree error for pid=%s", self.cfg.id, pid)

    async def _suspend_tree(self) -> bool:
        pid = self.pid
        if pid is None or not HAVE_PSUTIL:
            if pid is not None:
                log.warning("[%s] psutil not installed; pause halts supervision only",
                            self.cfg.id)
            return False

        def _sync() -> bool:
            try:
                p = psutil.Process(pid)
                for child in p.children(recursive=True):
                    with contextlib.suppress(psutil.Error):
                        child.suspend()
                p.suspend()
                return True
            except psutil.Error:
                return False

        try:
            return await asyncio.to_thread(_sync)
        except Exception:
            log.exception("[%s] suspend failed", self.cfg.id)
            return False

    async def _resume_tree(self) -> bool:
        pid = self.pid
        if pid is None or not HAVE_PSUTIL:
            return False

        def _sync() -> bool:
            try:
                p = psutil.Process(pid)
                p.resume()
                for child in p.children(recursive=True):
                    with contextlib.suppress(psutil.Error):
                        child.resume()
                return True
            except psutil.Error:
                return False

        try:
            return await asyncio.to_thread(_sync)
        except Exception:
            log.exception("[%s] resume failed", self.cfg.id)
            return False

    # ================================================================ misc

    def tail_log(self, lines: int = 25, max_bytes: int = 65536) -> str:
        try:
            if not self._log_path.exists():
                return "(no log yet)"
            size = self._log_path.stat().st_size
            with self._log_path.open("rb") as fh:
                fh.seek(max(0, size - max_bytes))
                data = fh.read()
            text = data.decode("utf-8", errors="replace")
            return "\n".join(text.splitlines()[-lines:]) or "(empty)"
        except OSError as exc:
            return f"(log unreadable: {exc})"
