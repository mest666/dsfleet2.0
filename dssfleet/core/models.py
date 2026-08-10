"""Domain model: states, events, snapshots. No I/O, no side effects."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping, Optional, Sequence

__all__ = [
    "InstanceState", "HealthStatus", "Severity", "CheckResult", "HealthReport",
    "Event", "InstanceSnapshot", "FleetSnapshot", "FleetMetrics", "TERMINAL_STATES",
    "ACTIVE_STATES",
]


class InstanceState(str, Enum):
    STOPPED = "stopped"
    STARTING = "starting"
    RUNNING = "running"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    RESTARTING = "restarting"
    STOPPING = "stopping"
    PAUSED = "paused"
    QUARANTINED = "quarantined"

    @property
    def emoji(self) -> str:
        return {
            InstanceState.STOPPED: "⚪",
            InstanceState.STARTING: "🔵",
            InstanceState.RUNNING: "🟢",
            InstanceState.DEGRADED: "🟡",
            InstanceState.UNHEALTHY: "🟠",
            InstanceState.RESTARTING: "🔄",
            InstanceState.STOPPING: "🔽",
            InstanceState.PAUSED: "⏸",
            InstanceState.QUARANTINED: "🔴",
        }[self]


TERMINAL_STATES = frozenset({InstanceState.STOPPED, InstanceState.QUARANTINED, InstanceState.PAUSED})
ACTIVE_STATES = frozenset({InstanceState.RUNNING, InstanceState.DEGRADED, InstanceState.UNHEALTHY})


class HealthStatus(str, Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class Severity(str, Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

    @property
    def rank(self) -> int:
        return {"debug": 0, "info": 1, "warning": 2, "error": 3, "critical": 4}[self.value]

    @property
    def emoji(self) -> str:
        return {"debug": "🔍", "info": "ℹ️", "warning": "⚠️", "error": "❗", "critical": "🚨"}[self.value]


@dataclass(slots=True, frozen=True)
class CheckResult:
    name: str
    ok: bool
    detail: str = ""
    fatal: bool = False           # fatal failure short-circuits straight to UNHEALTHY
    data: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class HealthReport:
    status: HealthStatus
    results: Sequence[CheckResult] = ()
    ts: float = field(default_factory=time.time)

    @property
    def failed(self) -> list[CheckResult]:
        return [r for r in self.results if not r.ok]

    def summary(self) -> str:
        if self.status is HealthStatus.HEALTHY:
            return "all checks passing"
        bad = self.failed
        return "; ".join(f"{r.name}: {r.detail or 'failed'}" for r in bad) or self.status.value


@dataclass(slots=True, frozen=True)
class Event:
    kind: str                      # state_change | health | restart | quarantine | fleet | lifecycle
    instance_id: Optional[str]
    severity: Severity
    message: str
    ts: float = field(default_factory=time.time)
    data: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class InstanceSnapshot:
    id: str
    state: InstanceState
    health: HealthStatus
    pid: Optional[int]
    enabled: bool
    since: float                   # timestamp of last state transition
    started_at: Optional[float]    # timestamp of current process start
    uptime_s: float
    restarts_total: int
    restarts_recent: int
    last_exit_code: Optional[int]
    last_error: Optional[str]
    heartbeat_age_s: Optional[float]
    consecutive_failures: int
    tags: Sequence[str] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "state": self.state.value,
            "health": self.health.value,
            "pid": self.pid,
            "enabled": self.enabled,
            "since": self.since,
            "started_at": self.started_at,
            "uptime_s": round(self.uptime_s, 2),
            "restarts_total": self.restarts_total,
            "restarts_recent": self.restarts_recent,
            "last_exit_code": self.last_exit_code,
            "last_error": self.last_error,
            "heartbeat_age_s": None if self.heartbeat_age_s is None else round(self.heartbeat_age_s, 2),
            "consecutive_failures": self.consecutive_failures,
            "tags": list(self.tags),
        }


@dataclass(slots=True)
class FleetMetrics:
    total: int
    running: int
    healthy: int
    degraded: int
    unhealthy: int
    quarantined: int
    paused: int
    stopped: int
    restarts_last_hour: int
    healthy_ratio: float
    fleet_uptime_s: float
    max_heartbeat_age_s: Optional[float]
    breaches: Sequence[str] = ()

    @property
    def ok(self) -> bool:
        return not self.breaches

    def as_dict(self) -> dict[str, Any]:
        return {
            "total": self.total, "running": self.running, "healthy": self.healthy,
            "degraded": self.degraded, "unhealthy": self.unhealthy,
            "quarantined": self.quarantined, "paused": self.paused, "stopped": self.stopped,
            "restarts_last_hour": self.restarts_last_hour,
            "healthy_ratio": round(self.healthy_ratio, 4),
            "fleet_uptime_s": round(self.fleet_uptime_s, 2),
            "max_heartbeat_age_s": (None if self.max_heartbeat_age_s is None
                                    else round(self.max_heartbeat_age_s, 2)),
            "breaches": list(self.breaches),
        }


@dataclass(slots=True)
class FleetSnapshot:
    ts: float
    paused: bool
    instances: Sequence[InstanceSnapshot]
    metrics: FleetMetrics

    def as_dict(self) -> dict[str, Any]:
        return {
            "ts": self.ts,
            "paused": self.paused,
            "metrics": self.metrics.as_dict(),
            "instances": [i.as_dict() for i in self.instances],
        }


def fmt_duration(seconds: Optional[float]) -> str:
    """Compact human duration: 3d04h, 2h11m, 5m02s, 12s."""
    if seconds is None:
        return "-"
    s = int(max(0.0, seconds))
    d, rem = divmod(s, 86400)
    h, rem = divmod(rem, 3600)
    m, sec = divmod(rem, 60)
    if d:
        return f"{d}d{h:02d}h"
    if h:
        return f"{h}h{m:02d}m"
    if m:
        return f"{m}m{sec:02d}s"
    return f"{sec}s"
