"""Composite health evaluation.

Each check returns a CheckResult; the monitor aggregates into a HealthReport:
  - any fatal failure          → UNHEALTHY
  - any non-fatal failure      → DEGRADED
  - no checks executed         → UNKNOWN
  - all pass                   → HEALTHY

Checks are run concurrently with an individual timeout so one wedged probe cannot
stall the supervise loop.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import socket

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Awaitable, Callable, Optional

from .config import HealthConfig, InstanceConfig
from .models import CheckResult, HealthReport, HealthStatus

log = logging.getLogger("dsfleet.health")

__all__ = ["HealthContext", "HealthMonitor"]

_A2S_INFO = b"\xFF\xFF\xFF\xFFTSource Engine Query\x00"


@dataclass(slots=True)
class HealthContext:
    """Live per-instance facts the checks need. Owned by the supervisor."""
    instance_id: str
    pid: Optional[int] = None
    process_alive: bool = False
    window_handle: Optional[int] = None
    started_at: Optional[float] = None
    startup_grace_s: float = 20.0
    last_hb_seq: Optional[int] = None
    last_hb_age_s: Optional[float] = None

    @property
    def in_startup_grace(self) -> bool:
        return (self.started_at is not None
                and (time.time() - self.started_at) < self.startup_grace_s)


HangProbe = Callable[[int, int], Awaitable[Optional[bool]]]


class HealthMonitor:
    """Stateful across evaluations (tracks heartbeat sequence + failure streaks)."""

    def __init__(self, cfg: HealthConfig, instance: InstanceConfig,
                 hang_probe: Optional[HangProbe] = None) -> None:
        self.cfg = cfg
        self.instance = instance
        self._hang_probe = hang_probe
        self._last_seq: Optional[int] = None
        self._last_seq_change: float = 0.0
        self.consecutive_failures = 0

    def reset(self) -> None:
        self._last_seq = None
        self._last_seq_change = 0.0
        self.consecutive_failures = 0

    # ------------------------------------------------------------ checks

    async def _check_process(self, ctx: HealthContext) -> CheckResult:
        if ctx.process_alive:
            return CheckResult("process", True, f"pid={ctx.pid}")
        return CheckResult("process", False, "process is not running", fatal=True)

    async def _check_heartbeat(self, ctx: HealthContext) -> Optional[CheckResult]:
        hb = self.cfg.heartbeat
        if not hb.enabled or hb.path is None:
            return None
        path: Path = hb.path
        now = time.time()
        try:
            st = os.stat(path)
        except FileNotFoundError:
            ctx.last_hb_age_s = None
            if ctx.in_startup_grace:
                return CheckResult("heartbeat", True, "absent (startup grace)")
            return CheckResult("heartbeat", False, f"heartbeat file missing: {path}")
        except OSError as exc:
            return CheckResult("heartbeat", False, f"stat failed: {exc}")

        ts = st.st_mtime
        seq: Optional[int] = None
        payload: dict = {}
        try:
            raw = path.read_text(encoding="utf-8")
            if raw.strip():
                payload = json.loads(raw)
                if isinstance(payload, dict):
                    if isinstance(payload.get("ts"), (int, float)):
                        ts = float(payload["ts"])
                    if isinstance(payload.get("seq"), int):
                        seq = payload["seq"]
        except (OSError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            # Torn read during a non-atomic write: fall back to mtime, do not fail.
            log.debug("heartbeat parse fallback for %s: %s", ctx.instance_id, exc)

        age = max(0.0, now - ts)
        ctx.last_hb_age_s = age
        ctx.last_hb_seq = seq

        if age > hb.max_age_s:
            if ctx.in_startup_grace:
                return CheckResult("heartbeat", True, f"stale {age:.1f}s (startup grace)")
            return CheckResult("heartbeat", False, f"stale by {age:.1f}s (max {hb.max_age_s:.0f}s)",
                               fatal=True, data={"age_s": age, "seq": seq})

        if hb.require_increasing_seq and seq is not None:
            if self._last_seq is None or seq > self._last_seq:
                self._last_seq, self._last_seq_change = seq, now
            elif (now - self._last_seq_change) > hb.max_age_s and not ctx.in_startup_grace:
                return CheckResult(
                    "heartbeat", False,
                    f"seq frozen at {seq} for {now - self._last_seq_change:.0f}s",
                    fatal=True, data={"seq": seq})

        detail = f"age={age:.1f}s" + (f" seq={seq}" if seq is not None else "")
        return CheckResult("heartbeat", True, detail, data={"age_s": age, "seq": seq,
                                                            "payload": payload})

    async def _check_window(self, ctx: HealthContext) -> Optional[CheckResult]:
        wc = self.cfg.window_responsive
        if not wc.enabled or self._hang_probe is None:
            return None
        if ctx.window_handle is None:
            if wc.required and not ctx.in_startup_grace:
                return CheckResult("window", False, "no top-level window resolved")
            return CheckResult("window", True, "window not resolved (tolerated)")
        hung = await self._hang_probe(ctx.window_handle, wc.timeout_ms)
        if hung is None:
            return CheckResult("window", True, "hang probe unsupported")
        if hung:
            return CheckResult("window", False,
                               f"UI thread unresponsive >{wc.timeout_ms}ms",
                               fatal=True, data={"hwnd": ctx.window_handle})
        return CheckResult("window", True, "responsive")

    async def _check_port(self, ctx: HealthContext) -> Optional[CheckResult]:
        pc = self.cfg.port
        if not pc.enabled or pc.port is None:
            return None
        if ctx.in_startup_grace:
            return CheckResult("port", True, "startup grace")
        if pc.protocol == "tcp":
            return await self._check_tcp(pc.host, pc.port, pc.timeout_s)
        return await self._check_a2s(pc.host, pc.port, pc.timeout_s)

    @staticmethod
    async def _check_tcp(host: str, port: int, timeout: float) -> CheckResult:
        writer = None
        try:
            _reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port), timeout=timeout)
            return CheckResult("port", True, f"tcp {host}:{port} open")
        except asyncio.TimeoutError:
            return CheckResult("port", False, f"tcp {host}:{port} timeout after {timeout:.1f}s")
        except OSError as exc:
            return CheckResult("port", False, f"tcp {host}:{port} refused: {exc}")
        finally:
            if writer is not None:
                writer.close()
                try:
                    await writer.wait_closed()
                except (OSError, asyncio.TimeoutError):
                    pass

    @staticmethod
    async def _check_a2s(host: str, port: int, timeout: float) -> CheckResult:
        """A2S_INFO probe with one challenge-response retry (Source truncated-reply flow)."""
        def probe() -> tuple[bool, str]:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.settimeout(timeout)
            try:
                sock.sendto(_A2S_INFO, (host, port))
                data, _ = sock.recvfrom(4096)
                if len(data) >= 5 and data[4:5] == b"A":  # challenge
                    challenge = data[5:9]
                    sock.sendto(_A2S_INFO + challenge, (host, port))
                    data, _ = sock.recvfrom(4096)
                if len(data) >= 5 and data[:4] == b"\xFF\xFF\xFF\xFF" and data[4:5] == b"I":
                    return True, f"a2s {host}:{port} responded ({len(data)}B)"
                return False, f"a2s {host}:{port} malformed reply ({len(data)}B)"
            except socket.timeout:
                return False, f"a2s {host}:{port} timeout after {timeout:.1f}s"
            except OSError as exc:
                return False, f"a2s {host}:{port} error: {exc}"
            finally:
                sock.close()

        try:
            ok, detail = await asyncio.wait_for(asyncio.to_thread(probe), timeout=timeout + 2.0)
        except asyncio.TimeoutError:
            ok, detail = False, f"a2s {host}:{port} probe hard timeout"
        return CheckResult("port", ok, detail)

    # ------------------------------------------------------------ aggregate

    async def evaluate(self, ctx: HealthContext) -> HealthReport:
        coros = [
            self._check_process(ctx),
            self._check_heartbeat(ctx),
            self._check_window(ctx),
            self._check_port(ctx),
        ]
        budget = max(2.0, self.cfg.interval_s * 2.0)
        try:
            raw = await asyncio.wait_for(
                asyncio.gather(*coros, return_exceptions=True), timeout=budget)
        except asyncio.TimeoutError:
            self.consecutive_failures += 1
            return HealthReport(HealthStatus.UNHEALTHY,
                                [CheckResult("monitor", False,
                                             f"evaluation exceeded {budget:.0f}s", fatal=True)])

        results: list[CheckResult] = []
        for item in raw:
            if item is None:
                continue
            if isinstance(item, BaseException):
                log.exception("health check raised for %s", ctx.instance_id, exc_info=item)
                results.append(CheckResult("check", False, f"internal error: {item!r}"))
                continue
            results.append(item)

        if not results:
            return HealthReport(HealthStatus.UNKNOWN, results)

        failures = [r for r in results if not r.ok]
        if not failures:
            self.consecutive_failures = 0
            return HealthReport(HealthStatus.HEALTHY, results)

        self.consecutive_failures += 1
        fatal = any(r.fatal for r in failures)
        if fatal or self.consecutive_failures >= self.cfg.failures_to_unhealthy:
            return HealthReport(HealthStatus.UNHEALTHY, results)
        return HealthReport(HealthStatus.DEGRADED, results)
