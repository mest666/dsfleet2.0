"""Config loading: JSON → typed dataclasses, with env expansion, defaults merge and
semantic validation. Schema validation via jsonschema when available (optional dep)."""
from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

__all__ = ["AppConfig", "InstanceConfig", "load_config", "ConfigError"]

_ENV_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_ID_RE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")


class ConfigError(ValueError):
    """Raised for any malformed or semantically invalid configuration."""


# --------------------------------------------------------------------------- helpers

def _expand(value: Any) -> Any:
    """Recursively expand ${VAR} and ${VAR:-default} in strings."""
    if isinstance(value, str):
        def sub(m: re.Match[str]) -> str:
            name, default = m.group(1), m.group(2)
            got = os.environ.get(name)
            if got is None:
                if default is None:
                    raise ConfigError(f"environment variable {name!r} referenced but not set")
                return default
            return got
        return _ENV_RE.sub(sub, value)
    if isinstance(value, dict):
        return {k: _expand(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_expand(v) for v in value]
    return value


def _deep_merge(base: Mapping[str, Any], over: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(base)
    for k, v in over.items():
        if isinstance(v, Mapping) and isinstance(out.get(k), Mapping):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = v
    return out


def _get(d: Mapping[str, Any], path: str, default: Any = None) -> Any:
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, Mapping) or part not in cur:
            return default
        cur = cur[part]
    return cur


# --------------------------------------------------------------------------- dataclasses

@dataclass(slots=True, frozen=True)
class HeartbeatConfig:
    enabled: bool = False
    path: Optional[Path] = None
    max_age_s: float = 30.0
    require_increasing_seq: bool = True


@dataclass(slots=True, frozen=True)
class WindowCheckConfig:
    enabled: bool = False
    timeout_ms: int = 2000
    required: bool = False


@dataclass(slots=True, frozen=True)
class PortCheckConfig:
    enabled: bool = False
    host: str = "127.0.0.1"
    port: Optional[int] = None
    protocol: str = "tcp"
    timeout_s: float = 3.0


@dataclass(slots=True, frozen=True)
class HealthConfig:
    interval_s: float = 5.0
    failures_to_unhealthy: int = 3
    heartbeat: HeartbeatConfig = field(default_factory=HeartbeatConfig)
    window_responsive: WindowCheckConfig = field(default_factory=WindowCheckConfig)
    port: PortCheckConfig = field(default_factory=PortCheckConfig)


@dataclass(slots=True, frozen=True)
class RestartConfig:
    enabled: bool = True
    backoff_initial_s: float = 3.0
    backoff_factor: float = 2.0
    backoff_max_s: float = 300.0
    backoff_jitter: float = 0.2
    crash_window_s: float = 300.0
    crash_threshold: int = 5
    healthy_reset_s: float = 120.0


@dataclass(slots=True, frozen=True)
class IsolationConfig:
    mode: str = "none"
    home_template: Optional[str] = None
    env_overrides: Mapping[str, str] = field(default_factory=dict)
    user: Optional[str] = None
    domain: str = "."
    password_env: Optional[str] = None
    container_image: Optional[str] = None
    container_runtime: str = "docker"
    container_args: Sequence[str] = ()
    container_volumes: Sequence[str] = ()
    wrapper_argv: Sequence[str] = ()


@dataclass(slots=True, frozen=True)
class GridCell:
    row: int
    col: int
    row_span: int = 1
    col_span: int = 1


@dataclass(slots=True, frozen=True)
class WindowLayoutConfig:
    backend: str = "auto"
    enabled: bool = True
    origin_x: int = 0
    origin_y: int = 0
    width: int = 1920
    height: int = 1080
    rows: int = 1
    cols: int = 1
    gutter: int = 0
    padding: int = 0
    topmost: bool = False
    activate: bool = False
    strip_decorations: bool = False
    placement_retries: int = 12
    placement_retry_delay_s: float = 1.0
    reapply_interval_s: float = 0.0


@dataclass(slots=True, frozen=True)
class AffinityConfig:
    enabled: bool = False
    strategy: str = "physical_first"
    reserve_cpus: Sequence[int] = (0,)
    cpus_per_instance: int = 1
    priority: Optional[str] = None
    explicit: Mapping[str, Sequence[int]] = field(default_factory=dict)


@dataclass(slots=True, frozen=True)
class PreflightSettings:
    enabled: bool = True
    block_on_fail: bool = True
    per_instance_mb: int = 1100
    os_reserve_gb: float = 3.0
    warn_headroom_gb: float = 1.0
    min_pagefile_gb: float = 8.0
    min_disk_free_gb: float = 10.0


@dataclass(slots=True, frozen=True)
class MetricsConfig:
    min_healthy_ratio: float = 0.8
    max_quarantined: int = 1
    max_restarts_per_hour: int = 12
    max_heartbeat_age_s: float = 30.0
    evaluate_interval_s: float = 15.0
    snapshot_persist_interval_s: float = 30.0


@dataclass(slots=True, frozen=True)
class TelegramConfig:
    enabled: bool = False
    token: str = ""
    admin_ids: frozenset[int] = frozenset()
    alert_chat_ids: Sequence[int] = ()
    alerts_enabled: bool = True
    alert_min_severity: str = "warning"
    alert_coalesce_s: float = 5.0
    command_rate_limit_s: float = 0.7
    destructive_commands_require_confirm: bool = True


@dataclass(slots=True, frozen=True)
class InstanceConfig:
    id: str
    executable: str
    args: Sequence[str] = ()
    cwd: Optional[Path] = None
    env: Mapping[str, str] = field(default_factory=dict)
    enabled: bool = True
    grid_cell: Optional[GridCell] = None
    window_title_match: Optional[str] = None
    startup_grace_s: float = 20.0
    shutdown_grace_s: float = 15.0
    log_max_bytes: int = 33_554_432
    log_backups: int = 3
    health: HealthConfig = field(default_factory=HealthConfig)
    restart: RestartConfig = field(default_factory=RestartConfig)
    isolation: IsolationConfig = field(default_factory=IsolationConfig)
    tags: Sequence[str] = ()
    rcon_port: Optional[int] = None
    rcon_password: Optional[str] = None
    lane_role: Optional[str] = None      # informational: top | mid | bot | support


@dataclass(slots=True, frozen=True)
class AppConfig:
    runtime_dir: Path
    log_level: str
    window_layout: WindowLayoutConfig
    metrics: MetricsConfig
    telegram: TelegramConfig
    instances: Sequence[InstanceConfig]
    affinity: AffinityConfig = field(default_factory=AffinityConfig)
    preflight: PreflightSettings = field(default_factory=PreflightSettings)
    source_path: Optional[Path] = None

    @property
    def heartbeat_dir(self) -> Path:
        return self.runtime_dir / "hb"

    @property
    def log_dir(self) -> Path:
        return self.runtime_dir / "logs"

    @property
    def state_dir(self) -> Path:
        return self.runtime_dir / "state"

    def ensure_dirs(self) -> None:
        for d in (self.runtime_dir, self.heartbeat_dir, self.log_dir, self.state_dir):
            d.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------------------- builders

def _build_health(raw: Mapping[str, Any], runtime_dir: Path, iid: str) -> HealthConfig:
    hb_raw = raw.get("heartbeat", {}) or {}
    hb_path = hb_raw.get("path")
    hb = HeartbeatConfig(
        enabled=bool(hb_raw.get("enabled", False)),
        path=Path(hb_path) if hb_path else runtime_dir / "hb" / f"{iid}.json",
        max_age_s=float(hb_raw.get("max_age_s", 30.0)),
        require_increasing_seq=bool(hb_raw.get("require_increasing_seq", True)),
    )
    w_raw = raw.get("window_responsive", {}) or {}
    win = WindowCheckConfig(
        enabled=bool(w_raw.get("enabled", False)),
        timeout_ms=int(w_raw.get("timeout_ms", 2000)),
        required=bool(w_raw.get("required", False)),
    )
    p_raw = raw.get("port", {}) or {}
    port = PortCheckConfig(
        enabled=bool(p_raw.get("enabled", False)),
        host=str(p_raw.get("host", "127.0.0.1")),
        port=int(p_raw["port"]) if p_raw.get("port") is not None else None,
        protocol=str(p_raw.get("protocol", "tcp")),
        timeout_s=float(p_raw.get("timeout_s", 3.0)),
    )
    if port.enabled and port.port is None:
        raise ConfigError(f"instance {iid!r}: health.port.enabled requires health.port.port")
    return HealthConfig(
        interval_s=float(raw.get("interval_s", 5.0)),
        failures_to_unhealthy=int(raw.get("failures_to_unhealthy", 3)),
        heartbeat=hb, window_responsive=win, port=port,
    )


def _build_restart(raw: Mapping[str, Any]) -> RestartConfig:
    return RestartConfig(
        enabled=bool(raw.get("enabled", True)),
        backoff_initial_s=float(raw.get("backoff_initial_s", 3.0)),
        backoff_factor=float(raw.get("backoff_factor", 2.0)),
        backoff_max_s=float(raw.get("backoff_max_s", 300.0)),
        backoff_jitter=float(raw.get("backoff_jitter", 0.2)),
        crash_window_s=float(raw.get("crash_window_s", 300.0)),
        crash_threshold=int(raw.get("crash_threshold", 5)),
        healthy_reset_s=float(raw.get("healthy_reset_s", 120.0)),
    )


def _build_isolation(raw: Mapping[str, Any], iid: str) -> IsolationConfig:
    mode = str(raw.get("mode", "none"))
    if mode not in {"none", "env_separation", "runas", "container", "wrapper"}:
        raise ConfigError(f"instance {iid!r}: unknown isolation mode {mode!r}")
    cfg = IsolationConfig(
        mode=mode,
        home_template=raw.get("home_template"),
        env_overrides=dict(raw.get("env_overrides", {}) or {}),
        user=raw.get("user"),
        domain=str(raw.get("domain", ".")),
        password_env=raw.get("password_env"),
        container_image=raw.get("container_image"),
        container_runtime=str(raw.get("container_runtime", "docker")),
        container_args=tuple(raw.get("container_args", ()) or ()),
        container_volumes=tuple(raw.get("container_volumes", ()) or ()),
        wrapper_argv=tuple(raw.get("wrapper_argv", ()) or ()),
    )
    if mode == "runas" and not (cfg.user and cfg.password_env):
        raise ConfigError(f"instance {iid!r}: isolation 'runas' requires user and password_env")
    if mode == "container" and not cfg.container_image:
        raise ConfigError(f"instance {iid!r}: isolation 'container' requires container_image")
    if mode == "wrapper" and not cfg.wrapper_argv:
        raise ConfigError(f"instance {iid!r}: isolation 'wrapper' requires wrapper_argv")
    if mode == "env_separation" and not cfg.home_template:
        raise ConfigError(f"instance {iid!r}: isolation 'env_separation' requires home_template")
    return cfg


def _build_instance(raw: Mapping[str, Any], defaults: Mapping[str, Any], runtime_dir: Path) -> InstanceConfig:
    merged = _deep_merge(defaults, raw)
    iid = str(merged.get("id", "")).strip()
    if not _ID_RE.match(iid):
        raise ConfigError(f"invalid instance id {iid!r} (allowed: [A-Za-z0-9._-]{{1,64}})")
    executable = merged.get("executable")
    if not executable:
        raise ConfigError(f"instance {iid!r}: 'executable' is required")

    cell_raw = merged.get("grid_cell")
    cell = None
    if cell_raw:
        cell = GridCell(
            row=int(cell_raw["row"]), col=int(cell_raw["col"]),
            row_span=int(cell_raw.get("row_span", 1)),
            col_span=int(cell_raw.get("col_span", 1)),
        )
        if cell.row < 0 or cell.col < 0 or cell.row_span < 1 or cell.col_span < 1:
            raise ConfigError(f"instance {iid!r}: invalid grid_cell {cell_raw!r}")

    if merged.get("window_title_match"):
        try:
            re.compile(merged["window_title_match"])
        except re.error as exc:
            raise ConfigError(f"instance {iid!r}: bad window_title_match regex: {exc}") from exc

    cwd = merged.get("cwd")
    return InstanceConfig(
        id=iid,
        executable=str(executable),
        args=tuple(str(a) for a in merged.get("args", ()) or ()),
        cwd=Path(cwd) if cwd else None,
        env={str(k): str(v) for k, v in (merged.get("env") or {}).items()},
        enabled=bool(merged.get("enabled", True)),
        grid_cell=cell,
        window_title_match=merged.get("window_title_match"),
        startup_grace_s=float(merged.get("startup_grace_s", 20.0)),
        shutdown_grace_s=float(merged.get("shutdown_grace_s", 15.0)),
        log_max_bytes=int(merged.get("log_max_bytes", 33_554_432)),
        log_backups=int(merged.get("log_backups", 3)),
        health=_build_health(merged.get("health", {}) or {}, runtime_dir, iid),
        restart=_build_restart(merged.get("restart", {}) or {}),
        isolation=_build_isolation(merged.get("isolation", {}) or {}, iid),
        tags=tuple(merged.get("tags", ()) or ()),
        rcon_port=int(merged["rcon_port"]) if merged.get("rcon_port") is not None else None,
        rcon_password=merged.get("rcon_password"),
        lane_role=merged.get("lane_role"),
    )


def _build_window_layout(raw: Mapping[str, Any]) -> WindowLayoutConfig:
    region = raw.get("region", {}) or {}
    grid = raw.get("grid", {}) or {}
    origin = raw.get("origin", {}) or {}
    cfg = WindowLayoutConfig(
        backend=str(raw.get("backend", "auto")),
        enabled=bool(raw.get("enabled", True)),
        origin_x=int(origin.get("x", 0)),
        origin_y=int(origin.get("y", 0)),
        width=int(region.get("width", 1920)),
        height=int(region.get("height", 1080)),
        rows=int(grid.get("rows", 1)),
        cols=int(grid.get("cols", 1)),
        gutter=int(raw.get("gutter", 0)),
        padding=int(raw.get("padding", 0)),
        topmost=bool(raw.get("topmost", False)),
        activate=bool(raw.get("activate", False)),
        strip_decorations=bool(raw.get("strip_decorations", False)),
        placement_retries=int(raw.get("placement_retries", 12)),
        placement_retry_delay_s=float(raw.get("placement_retry_delay_s", 1.0)),
        reapply_interval_s=float(raw.get("reapply_interval_s", 0.0)),
    )
    if cfg.backend not in {"auto", "win32", "x11", "noop"}:
        raise ConfigError(f"window_layout.backend {cfg.backend!r} is not supported")
    if cfg.rows < 1 or cfg.cols < 1:
        raise ConfigError("window_layout.grid rows/cols must be >= 1")
    if cfg.width < 1 or cfg.height < 1:
        raise ConfigError("window_layout.region width/height must be >= 1")
    return cfg


def _build_telegram(raw: Mapping[str, Any]) -> TelegramConfig:
    if not raw:
        return TelegramConfig(enabled=False)
    enabled = bool(raw.get("enabled", True))
    token = str(raw.get("token", "") or "")
    admins = frozenset(int(x) for x in (raw.get("admin_ids") or ()))
    if enabled and not token:
        raise ConfigError("telegram.enabled is true but telegram.token is empty")
    if enabled and not admins:
        raise ConfigError("telegram.enabled is true but telegram.admin_ids is empty")
    sev = str(raw.get("alert_min_severity", "warning"))
    if sev not in {"debug", "info", "warning", "error", "critical"}:
        raise ConfigError(f"telegram.alert_min_severity {sev!r} invalid")
    return TelegramConfig(
        enabled=enabled,
        token=token,
        admin_ids=admins,
        alert_chat_ids=tuple(int(x) for x in (raw.get("alert_chat_ids") or admins)),
        alerts_enabled=bool(raw.get("alerts_enabled", True)),
        alert_min_severity=sev,
        alert_coalesce_s=float(raw.get("alert_coalesce_s", 5.0)),
        command_rate_limit_s=float(raw.get("command_rate_limit_s", 0.7)),
        destructive_commands_require_confirm=bool(
            raw.get("destructive_commands_require_confirm", True)),
    )


# --------------------------------------------------------------------------- entrypoint

def _validate_schema(doc: Mapping[str, Any], schema_path: Path) -> None:
    try:
        import jsonschema  # type: ignore
    except ImportError:
        return  # optional dependency; semantic validation below still runs
    try:
        schema = json.loads(schema_path.read_text(encoding="utf-8"))
    except OSError:
        return
    try:
        jsonschema.validate(doc, schema)
    except jsonschema.ValidationError as exc:  # pragma: no cover - message passthrough
        loc = "/".join(str(p) for p in exc.absolute_path) or "<root>"
        raise ConfigError(f"schema violation at {loc}: {exc.message}") from exc


def load_config(path: str | os.PathLike[str]) -> AppConfig:
    p = Path(path).expanduser().resolve()
    try:
        raw_text = p.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read config {p}: {exc}") from exc
    try:
        doc = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"invalid JSON in {p} (line {exc.lineno}, col {exc.colno}): {exc.msg}") from exc
    if not isinstance(doc, dict):
        raise ConfigError("config root must be a JSON object")

    _validate_schema(doc, p.parent / "config.schema.json")
    doc = _expand(doc)

    runtime_dir_raw = doc.get("runtime_dir")
    if not runtime_dir_raw:
        raise ConfigError("'runtime_dir' is required")
    runtime_dir = Path(str(runtime_dir_raw)).expanduser()

    defaults = doc.get("defaults", {}) or {}
    raw_instances = doc.get("instances") or []
    if not raw_instances:
        raise ConfigError("'instances' must contain at least one entry")

    instances: list[InstanceConfig] = []
    seen: set[str] = set()
    for raw_i in raw_instances:
        inst = _build_instance(raw_i, defaults, runtime_dir)
        if inst.id in seen:
            raise ConfigError(f"duplicate instance id {inst.id!r}")
        seen.add(inst.id)
        instances.append(inst)

    layout = _build_window_layout(doc.get("window_layout", {}) or {})

    # Grid capacity & collision validation, with automatic row-major assignment.
    occupied: dict[tuple[int, int], str] = {}
    auto_slots = [(r, c) for r in range(layout.rows) for c in range(layout.cols)]
    assigned: list[InstanceConfig] = []
    for inst in instances:
        cell = inst.grid_cell
        if cell is None:
            # defer; assign after explicit cells are reserved
            assigned.append(inst)
            continue
        for r in range(cell.row, cell.row + cell.row_span):
            for c in range(cell.col, cell.col + cell.col_span):
                if r >= layout.rows or c >= layout.cols:
                    raise ConfigError(
                        f"instance {inst.id!r}: grid_cell ({r},{c}) exceeds grid "
                        f"{layout.rows}x{layout.cols}")
                if (r, c) in occupied:
                    raise ConfigError(
                        f"instance {inst.id!r}: grid cell ({r},{c}) already used by "
                        f"{occupied[(r, c)]!r}")
                occupied[(r, c)] = inst.id
        assigned.append(inst)

    free = [slot for slot in auto_slots if slot not in occupied]
    final: list[InstanceConfig] = []
    for inst in assigned:
        if inst.grid_cell is not None:
            final.append(inst)
            continue
        if not free:
            if layout.enabled and layout.backend != "noop":
                raise ConfigError(
                    f"instance {inst.id!r}: no free grid slot in {layout.rows}x{layout.cols} grid")
            final.append(inst)
            continue
        r, c = free.pop(0)
        occupied[(r, c)] = inst.id
        final.append(replace(inst, grid_cell=GridCell(row=r, col=c)))

    metrics_raw = doc.get("metrics", {}) or {}
    metrics = MetricsConfig(
        min_healthy_ratio=float(metrics_raw.get("min_healthy_ratio", 0.8)),
        max_quarantined=int(metrics_raw.get("max_quarantined", 1)),
        max_restarts_per_hour=int(metrics_raw.get("max_restarts_per_hour", 12)),
        max_heartbeat_age_s=float(metrics_raw.get("max_heartbeat_age_s", 30.0)),
        evaluate_interval_s=float(metrics_raw.get("evaluate_interval_s", 15.0)),
        snapshot_persist_interval_s=float(metrics_raw.get("snapshot_persist_interval_s", 30.0)),
    )

    aff_raw = doc.get("affinity", {}) or {}
    strategy = str(aff_raw.get("strategy", "physical_first"))
    if strategy not in {"physical_first", "sequential", "explicit", "none"}:
        raise ConfigError(f"affinity.strategy {strategy!r} invalid")
    priority = aff_raw.get("priority")
    if priority is not None and priority not in {
            "idle", "below_normal", "normal", "above_normal", "high"}:
        raise ConfigError(f"affinity.priority {priority!r} invalid")
    affinity = AffinityConfig(
        enabled=bool(aff_raw.get("enabled", False)),
        strategy=strategy,
        reserve_cpus=tuple(int(c) for c in (aff_raw.get("reserve_cpus", (0,)) or ())),
        cpus_per_instance=int(aff_raw.get("cpus_per_instance", 1)),
        priority=priority,
        explicit={str(k): tuple(int(c) for c in v)
                  for k, v in (aff_raw.get("explicit", {}) or {}).items()},
    )
    if affinity.strategy == "explicit" and affinity.enabled:
        unknown = set(affinity.explicit) - {i.id for i in final}
        if unknown:
            raise ConfigError(f"affinity.explicit references unknown instances: "
                              f"{sorted(unknown)}")

    pf_raw = doc.get("preflight", {}) or {}
    preflight = PreflightSettings(
        enabled=bool(pf_raw.get("enabled", True)),
        block_on_fail=bool(pf_raw.get("block_on_fail", True)),
        per_instance_mb=int(pf_raw.get("per_instance_mb", 1100)),
        os_reserve_gb=float(pf_raw.get("os_reserve_gb", 3.0)),
        warn_headroom_gb=float(pf_raw.get("warn_headroom_gb", 1.0)),
        min_pagefile_gb=float(pf_raw.get("min_pagefile_gb", 8.0)),
        min_disk_free_gb=float(pf_raw.get("min_disk_free_gb", 10.0)),
    )

    log_level = str(doc.get("log_level", "INFO")).upper()
    if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
        raise ConfigError(f"log_level {log_level!r} invalid")

    return AppConfig(
        runtime_dir=runtime_dir,
        log_level=log_level,
        window_layout=layout,
        metrics=metrics,
        telegram=_build_telegram(doc.get("telegram", {}) or {}),
        instances=tuple(final),
        affinity=affinity,
        preflight=preflight,
        source_path=p,
    )
