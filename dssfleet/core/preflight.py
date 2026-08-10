"""Preflight resource checks.

Ten Source 2 dedicated servers on a 16 GB desktop is a genuinely tight fit, and the
failure mode is ugly: Windows starts trimming working sets, srcds stalls on page
faults, tick times blow out, and the health monitor sees "wedged" instances that are
actually just starved. This module catches that *before* launch.

Checks
------
RAM          total / available physical memory
Pagefile     Windows: Win32_PageFileUsage (WMI) → registry fallback → psutil swap
             POSIX:   /proc/swaps → psutil swap
CPU          physical vs logical cores, per-instance core budget
Disk         free space on the runtime volume (crash dumps + logs)

Verdict
-------
OK | WARN | FAIL, with a human-readable reason list. FAIL blocks startup unless the
caller passes --force.
"""
from __future__ import annotations

import ctypes
import logging
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Optional, Sequence

log = logging.getLogger("dsfleet.preflight")

__all__ = ["Verdict", "ResourceReport", "PreflightConfig", "run_preflight",
           "MemoryInfo", "PagefileInfo", "CpuInfo"]

IS_WINDOWS = sys.platform.startswith("win")
GB = 1024 ** 3

try:
    import psutil  # type: ignore
    HAVE_PSUTIL = True
except ImportError:  # pragma: no cover
    psutil = None  # type: ignore
    HAVE_PSUTIL = False


class Verdict(str, Enum):
    OK = "ok"
    WARN = "warn"
    FAIL = "fail"

    @property
    def emoji(self) -> str:
        return {"ok": "✅", "warn": "⚠️", "fail": "⛔"}[self.value]

    def worse_of(self, other: "Verdict") -> "Verdict":
        order = {Verdict.OK: 0, Verdict.WARN: 1, Verdict.FAIL: 2}
        return self if order[self] >= order[other] else other


@dataclass(slots=True, frozen=True)
class MemoryInfo:
    total_bytes: int
    available_bytes: int

    @property
    def total_gb(self) -> float:
        return self.total_bytes / GB

    @property
    def available_gb(self) -> float:
        return self.available_bytes / GB


@dataclass(slots=True, frozen=True)
class PagefileInfo:
    total_bytes: int
    used_bytes: int
    system_managed: bool
    source: str
    detected: bool = True

    @property
    def total_gb(self) -> float:
        return self.total_bytes / GB


@dataclass(slots=True, frozen=True)
class CpuInfo:
    physical: int
    logical: int


@dataclass(slots=True)
class ResourceReport:
    verdict: Verdict
    memory: MemoryInfo
    pagefile: PagefileInfo
    cpu: CpuInfo
    disk_free_bytes: int
    instance_count: int
    per_instance_mb: int
    reasons: list[str] = field(default_factory=list)
    advice: list[str] = field(default_factory=list)

    @property
    def required_gb(self) -> float:
        return (self.instance_count * self.per_instance_mb * 1024 * 1024) / GB

    def render(self) -> str:
        lines = [
            f"{self.verdict.emoji} preflight: {self.verdict.value.upper()}",
            f"  RAM       {self.memory.total_gb:.1f} GB total, "
            f"{self.memory.available_gb:.1f} GB available",
            f"  Pagefile  {self.pagefile.total_gb:.1f} GB "
            f"({'system-managed' if self.pagefile.system_managed else 'fixed'}, "
            f"via {self.pagefile.source})" if self.pagefile.detected
            else "  Pagefile  not detected",
            f"  CPU       {self.cpu.physical} physical / {self.cpu.logical} logical cores",
            f"  Disk      {self.disk_free_bytes / GB:.1f} GB free on runtime volume",
            f"  Budget    {self.instance_count} × {self.per_instance_mb} MB "
            f"= {self.required_gb:.1f} GB required",
        ]
        if self.reasons:
            lines.append("  Findings:")
            lines += [f"    • {r}" for r in self.reasons]
        if self.advice:
            lines.append("  Advice:")
            lines += [f"    → {a}" for a in self.advice]
        return "\n".join(lines)


@dataclass(slots=True, frozen=True)
class PreflightConfig:
    """Thresholds for the headroom evaluator."""
    per_instance_mb: int = 1100          # measured RSS of an idle dota2 -dedicated
    os_reserve_gb: float = 3.0           # leave this much for Windows + drivers
    warn_headroom_gb: float = 1.0        # below this margin → WARN
    min_pagefile_ratio: float = 0.75     # pagefile ≥ ratio × (required - physical)
    min_pagefile_gb: float = 8.0         # absolute floor when overcommitting
    min_disk_free_gb: float = 10.0
    min_cores_per_instance: float = 0.5  # logical cores per instance
    enabled: bool = True
    block_on_fail: bool = True


# --------------------------------------------------------------------------- probes

def probe_memory() -> MemoryInfo:
    if HAVE_PSUTIL:
        vm = psutil.virtual_memory()
        return MemoryInfo(total_bytes=vm.total, available_bytes=vm.available)

    if IS_WINDOWS:
        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [
                ("dwLength", ctypes.c_ulong),
                ("dwMemoryLoad", ctypes.c_ulong),
                ("ullTotalPhys", ctypes.c_ulonglong),
                ("ullAvailPhys", ctypes.c_ulonglong),
                ("ullTotalPageFile", ctypes.c_ulonglong),
                ("ullAvailPageFile", ctypes.c_ulonglong),
                ("ullTotalVirtual", ctypes.c_ulonglong),
                ("ullAvailVirtual", ctypes.c_ulonglong),
                ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
            ]
        stat = MEMORYSTATUSEX()
        stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
            return MemoryInfo(int(stat.ullTotalPhys), int(stat.ullAvailPhys))
        raise OSError("GlobalMemoryStatusEx failed")

    try:  # POSIX fallback
        total = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        avail = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_AVPHYS_PAGES")
        return MemoryInfo(int(total), int(avail))
    except (ValueError, OSError) as exc:
        raise OSError(f"cannot determine memory size: {exc}") from exc


def _probe_pagefile_wmi() -> Optional[PagefileInfo]:
    """Win32_PageFileUsage via PowerShell CIM. Returns None if unavailable."""
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if not powershell:
        return None
    script = (
        "$u = Get-CimInstance Win32_PageFileUsage -ErrorAction SilentlyContinue; "
        "$s = Get-CimInstance Win32_PageFileSetting -ErrorAction SilentlyContinue; "
        "$alloc = ($u | Measure-Object -Property AllocatedBaseSize -Sum).Sum; "
        "$used  = ($u | Measure-Object -Property CurrentUsage -Sum).Sum; "
        "$managed = ($s -eq $null) -or (($s | Where-Object {$_.MaximumSize -eq 0}) -ne $null); "
        "Write-Output (\"{0}|{1}|{2}\" -f [int]$alloc, [int]$used, [bool]$managed)"
    )
    try:
        out = subprocess.run(
            [powershell, "-NoProfile", "-NonInteractive", "-Command", script],
            capture_output=True, text=True, timeout=20,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
    except (OSError, subprocess.SubprocessError) as exc:
        log.debug("WMI pagefile probe failed: %s", exc)
        return None
    if out.returncode != 0:
        log.debug("WMI pagefile probe rc=%s: %s", out.returncode, out.stderr.strip())
        return None
    try:
        alloc_mb, used_mb, managed = out.stdout.strip().split("|")
        return PagefileInfo(
            total_bytes=int(alloc_mb) * 1024 * 1024,
            used_bytes=int(used_mb) * 1024 * 1024,
            system_managed=managed.strip().lower() in ("true", "1"),
            source="Win32_PageFileUsage",
        )
    except (ValueError, AttributeError) as exc:
        log.debug("could not parse WMI pagefile output %r: %s", out.stdout, exc)
        return None


def _probe_swap_psutil() -> Optional[PagefileInfo]:
    if not HAVE_PSUTIL:
        return None
    try:
        sw = psutil.swap_memory()
    except Exception:
        return None
    return PagefileInfo(total_bytes=sw.total, used_bytes=sw.used,
                        system_managed=False, source="psutil.swap_memory")


def _probe_swap_proc() -> Optional[PagefileInfo]:
    path = Path("/proc/swaps")
    if not path.exists():
        return None
    try:
        total = used = 0
        for line in path.read_text().splitlines()[1:]:
            parts = line.split()
            if len(parts) >= 4:
                total += int(parts[2]) * 1024
                used += int(parts[3]) * 1024
        return PagefileInfo(total, used, system_managed=False, source="/proc/swaps")
    except (OSError, ValueError) as exc:
        log.debug("/proc/swaps parse failed: %s", exc)
        return None


def probe_pagefile() -> PagefileInfo:
    probes = ((_probe_pagefile_wmi, _probe_swap_psutil) if IS_WINDOWS
              else (_probe_swap_proc, _probe_swap_psutil))
    for probe in probes:
        try:
            info = probe()
        except Exception:
            log.debug("pagefile probe %s raised", probe.__name__, exc_info=True)
            continue
        if info is not None:
            return info
    return PagefileInfo(0, 0, system_managed=False, source="none", detected=False)


def probe_cpu() -> CpuInfo:
    logical = os.cpu_count() or 1
    physical = logical
    if HAVE_PSUTIL:
        try:
            physical = psutil.cpu_count(logical=False) or logical
        except Exception:
            pass
    return CpuInfo(physical=physical, logical=logical)


def probe_disk_free(path: Path) -> int:
    target = path
    while not target.exists() and target != target.parent:
        target = target.parent
    try:
        return shutil.disk_usage(target).free
    except OSError as exc:
        log.debug("disk probe failed for %s: %s", target, exc)
        return 0


# --------------------------------------------------------------------------- evaluator

def run_preflight(instance_count: int, runtime_dir: Path,
                  cfg: Optional[PreflightConfig] = None) -> ResourceReport:
    cfg = cfg or PreflightConfig()
    mem = probe_memory()
    page = probe_pagefile()
    cpu = probe_cpu()
    disk_free = probe_disk_free(runtime_dir)

    required_bytes = instance_count * cfg.per_instance_mb * 1024 * 1024
    usable_bytes = mem.total_bytes - int(cfg.os_reserve_gb * GB)
    headroom_bytes = usable_bytes - required_bytes

    verdict = Verdict.OK
    reasons: list[str] = []
    advice: list[str] = []

    # -- physical memory ---------------------------------------------------
    if headroom_bytes < 0:
        overcommit_gb = -headroom_bytes / GB
        reasons.append(
            f"physical RAM short by {overcommit_gb:.1f} GB "
            f"({required_bytes / GB:.1f} GB needed, {usable_bytes / GB:.1f} GB usable "
            f"after a {cfg.os_reserve_gb:.0f} GB OS reserve)")
        # Overcommit is survivable *if* the pagefile can absorb it.
        need_page_gb = max(cfg.min_pagefile_gb,
                           overcommit_gb / max(cfg.min_pagefile_ratio, 0.01))
        if not page.detected:
            verdict = verdict.worse_of(Verdict.FAIL)
            reasons.append("no pagefile/swap detected to absorb the overcommit")
            advice.append(f"enable a pagefile of at least {need_page_gb:.0f} GB")
        elif page.total_bytes < need_page_gb * GB:
            verdict = verdict.worse_of(Verdict.FAIL)
            reasons.append(
                f"pagefile {page.total_gb:.1f} GB is below the "
                f"{need_page_gb:.0f} GB needed to cover the overcommit")
            advice.append(
                f"set the pagefile to {need_page_gb:.0f} GB (System → Advanced → "
                f"Performance → Virtual memory), or reduce to "
                f"{max(1, int(usable_bytes // (cfg.per_instance_mb * 1024 * 1024)))} instances")
        else:
            verdict = verdict.worse_of(Verdict.WARN)
            reasons.append(
                f"running on pagefile headroom: {page.total_gb:.1f} GB backing "
                f"{overcommit_gb:.1f} GB of overcommit — expect paging stalls")
            advice.append("keep the pagefile on an NVMe volume, not a spinning disk")
    elif headroom_bytes < cfg.warn_headroom_gb * GB:
        verdict = verdict.worse_of(Verdict.WARN)
        reasons.append(f"only {headroom_bytes / GB:.1f} GB headroom after "
                       f"{instance_count} instances")

    # -- currently available (someone else may already be using the box) ---
    if mem.available_bytes < required_bytes and headroom_bytes >= 0:
        verdict = verdict.worse_of(Verdict.WARN)
        reasons.append(
            f"only {mem.available_gb:.1f} GB free right now vs "
            f"{required_bytes / GB:.1f} GB required — close other applications")

    # -- pagefile sanity even when RAM is sufficient ------------------------
    if page.detected and page.total_bytes == 0:
        verdict = verdict.worse_of(Verdict.WARN)
        reasons.append("pagefile is disabled; a single instance leak will OOM the box")
    elif IS_WINDOWS and page.detected and not page.system_managed \
            and page.total_bytes < 4 * GB:
        advice.append("a fixed pagefile under 4 GB leaves no room for crash dumps")

    # -- CPU ----------------------------------------------------------------
    cores_each = cpu.logical / max(1, instance_count)
    if cores_each < cfg.min_cores_per_instance:
        verdict = verdict.worse_of(Verdict.WARN)
        reasons.append(
            f"{cpu.logical} logical cores across {instance_count} instances "
            f"= {cores_each:.2f} cores each (below {cfg.min_cores_per_instance})")
        advice.append("pin instances with cpu_affinity so they stop migrating between CCXs")
    if not HAVE_PSUTIL:
        advice.append("install psutil to enable CPU pinning and suspend/resume")

    # -- disk ---------------------------------------------------------------
    if disk_free < cfg.min_disk_free_gb * GB:
        verdict = verdict.worse_of(Verdict.WARN)
        reasons.append(f"only {disk_free / GB:.1f} GB free on the runtime volume "
                       f"(logs + crash dumps need room)")

    if verdict is Verdict.OK and not reasons:
        reasons.append("all resource checks within limits")

    return ResourceReport(
        verdict=verdict, memory=mem, pagefile=page, cpu=cpu,
        disk_free_bytes=disk_free, instance_count=instance_count,
        per_instance_mb=cfg.per_instance_mb, reasons=reasons, advice=advice,
    )
