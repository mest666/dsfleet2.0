"""CPU affinity allocation.

Why this exists
---------------
A Source dedicated server is effectively one hot thread plus a few helpers. Ten of
them on a Ryzen with the scheduler free to migrate threads produces constant cross-CCX
hops: every migration is an L3 miss, and on Zen the inter-CCX latency penalty is large
enough to show up directly in tick time variance. Pinning each instance to a fixed
subset of cores removes the migrations.

Strategy
--------
`physical_first` (default)
    Assign one physical core per instance, walking SMT-sibling pairs so instance N and
    instance N+1 never share a physical core until every physical core is used. Only
    once every physical core is occupied does it start doubling up onto SMT siblings.

`sequential`
    Naive contiguous chunks of logical CPUs. Simple, fine on Intel E-core-free parts.

`explicit`
    Whatever the config says, verbatim.

Core 0 is reserved for the OS and the orchestrator itself by default — leaving it in
the pool means the scheduler puts a game server on the same core as the interrupt
handlers and the DPC latency shows up as tick spikes.
"""
from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from typing import Optional, Sequence

log = logging.getLogger("dsfleet.affinity")

__all__ = ["AffinityPlan", "AffinityAllocator", "apply_affinity", "apply_priority",
           "detect_topology"]

IS_WINDOWS = sys.platform.startswith("win")

try:
    import psutil  # type: ignore
    HAVE_PSUTIL = True
except ImportError:  # pragma: no cover
    psutil = None  # type: ignore
    HAVE_PSUTIL = False


@dataclass(slots=True, frozen=True)
class Topology:
    logical: int
    physical: int

    @property
    def smt_width(self) -> int:
        return max(1, self.logical // max(1, self.physical))


def detect_topology() -> Topology:
    logical = os.cpu_count() or 1
    physical = logical
    if HAVE_PSUTIL:
        try:
            physical = psutil.cpu_count(logical=False) or logical
        except Exception:
            pass
    return Topology(logical=logical, physical=physical)


@dataclass(slots=True, frozen=True)
class AffinityPlan:
    instance_id: str
    cpus: tuple[int, ...]
    priority: Optional[str] = None

    def describe(self) -> str:
        return f"{self.instance_id}: cpus={list(self.cpus)} priority={self.priority or '-'}"


class AffinityAllocator:
    """Deterministic: the same instance list always yields the same plan."""

    def __init__(self, strategy: str = "physical_first", reserve_cpus: Sequence[int] = (0,),
                 cpus_per_instance: int = 1, topology: Optional[Topology] = None) -> None:
        if strategy not in {"physical_first", "sequential", "explicit", "none"}:
            raise ValueError(f"unknown affinity strategy {strategy!r}")
        self.strategy = strategy
        self.topology = topology or detect_topology()
        self.reserve = set(reserve_cpus)
        self.cpus_per_instance = max(1, cpus_per_instance)

    def _pool(self) -> list[int]:
        pool = [c for c in range(self.topology.logical) if c not in self.reserve]
        if not pool:  # never hand back an empty set — that would pin nothing
            log.warning("affinity reserve consumed every CPU; ignoring reserve")
            pool = list(range(self.topology.logical))
        return pool

    def _ordered_physical_first(self) -> list[int]:
        """Order logical CPUs so consecutive picks land on distinct physical cores.

        Linux and Windows both enumerate SMT siblings as adjacent logical ids on
        current AMD parts (0,1 = core 0; 2,3 = core 1; ...). Walking with a stride of
        smt_width visits one thread of every physical core first, then the siblings.
        """
        width = self.topology.smt_width
        pool = self._pool()
        if width <= 1:
            return pool
        ordered: list[int] = []
        for offset in range(width):
            ordered += [c for c in pool if c % width == offset]
        return ordered

    def plan(self, instance_ids: Sequence[str],
             explicit: Optional[dict[str, Sequence[int]]] = None,
             priority: Optional[str] = None) -> list[AffinityPlan]:
        if self.strategy == "none":
            return []

        if self.strategy == "explicit":
            explicit = explicit or {}
            out = []
            for iid in instance_ids:
                cpus = tuple(sorted(set(explicit.get(iid, ()))))
                if cpus:
                    out.append(AffinityPlan(iid, cpus, priority))
            return out

        ordered = (self._ordered_physical_first() if self.strategy == "physical_first"
                   else self._pool())
        if not ordered:
            return []

        plans: list[AffinityPlan] = []
        cursor = 0
        for iid in instance_ids:
            cpus = tuple(ordered[(cursor + k) % len(ordered)]
                         for k in range(self.cpus_per_instance))
            cursor += self.cpus_per_instance
            plans.append(AffinityPlan(iid, tuple(sorted(set(cpus))), priority))

        total_needed = len(instance_ids) * self.cpus_per_instance
        if total_needed > len(ordered):
            log.warning(
                "affinity oversubscribed: %d instances × %d cpus > %d available; "
                "instances will share cores",
                len(instance_ids), self.cpus_per_instance, len(ordered))
        return plans

    def describe(self, plans: Sequence[AffinityPlan]) -> str:
        head = (f"strategy={self.strategy} topology={self.topology.physical}P/"
                f"{self.topology.logical}L smt={self.topology.smt_width} "
                f"reserved={sorted(self.reserve)}")
        return "\n".join([head, *(f"  {p.describe()}" for p in plans)])


# --------------------------------------------------------------------------- apply

_PRIORITY_MAP = {
    "idle": "IDLE_PRIORITY_CLASS",
    "below_normal": "BELOW_NORMAL_PRIORITY_CLASS",
    "normal": "NORMAL_PRIORITY_CLASS",
    "above_normal": "ABOVE_NORMAL_PRIORITY_CLASS",
    "high": "HIGH_PRIORITY_CLASS",
}
_NICE_MAP = {"idle": 19, "below_normal": 10, "normal": 0, "above_normal": -5, "high": -10}


def apply_affinity(pid: int, cpus: Sequence[int]) -> bool:
    """Best-effort pin. Returns True on success; never raises."""
    if not cpus:
        return False
    if not HAVE_PSUTIL:
        log.debug("psutil unavailable; cannot set affinity for pid=%s", pid)
        return False
    try:
        proc = psutil.Process(pid)
        proc.cpu_affinity(list(cpus))
        log.info("pinned pid=%s to cpus=%s", pid, list(cpus))
        return True
    except psutil.NoSuchProcess:
        return False
    except (psutil.AccessDenied, psutil.Error, AttributeError, OSError, ValueError) as exc:
        log.warning("could not set affinity for pid=%s: %s", pid, exc)
        return False


def apply_priority(pid: int, priority: Optional[str]) -> bool:
    """Set scheduling priority. `high` on Windows maps to HIGH_PRIORITY_CLASS.

    Note: running ten servers at HIGH starves the orchestrator's own event loop and
    the health checks start timing out. `above_normal` is the sane ceiling for a
    fleet; `high` is only appropriate for a single instance under measurement.
    """
    if not priority or not HAVE_PSUTIL:
        return False
    key = priority.lower()
    try:
        proc = psutil.Process(pid)
        if IS_WINDOWS:
            const = _PRIORITY_MAP.get(key)
            if const is None or not hasattr(psutil, const):
                log.warning("unknown priority %r", priority)
                return False
            proc.nice(getattr(psutil, const))
        else:
            if key not in _NICE_MAP:
                log.warning("unknown priority %r", priority)
                return False
            proc.nice(_NICE_MAP[key])
        log.info("set pid=%s priority=%s", pid, key)
        return True
    except psutil.NoSuchProcess:
        return False
    except (psutil.AccessDenied, psutil.Error, OSError) as exc:
        log.warning("could not set priority for pid=%s: %s", pid, exc)
        return False
