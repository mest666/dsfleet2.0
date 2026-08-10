# dsfleet — Dedicated Server Fleet Orchestrator

Supervised multi-instance launcher for Source 2 dedicated servers (`srcds`) running
bot-mode / custom-game scenarios, with deterministic window layout, freeze detection,
crash recovery, isolation hooks, and an async Telegram control plane.

## 1. System Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                                  main.py                                     │
│  config load → validate → build Orchestrator → build TelemetryBot → run      │
└───────────────┬──────────────────────────────────────────┬───────────────────┘
                │                                          │
                v                                          v
┌───────────────────────────────────┐      ┌──────────────────────────────────┐
│         Orchestrator              │      │      TelemetryController         │
│  (core/orchestrator.py)           │◄─────┤      (telemetry/bot.py)          │
│                                   │ API  │                                  │
│  • registry: {id → Supervisor}    │      │  aiogram v3 Dispatcher           │
│  • pause_all / resume_all         │      │   ├─ AdminAuthMiddleware         │
│  • restart(id) / stop(id)         │      │   ├─ ThrottleMiddleware          │
│  • snapshot() → FleetSnapshot     │      │   ├─ Router: /status /instance   │
│  • metrics() → FleetMetrics       │      │   │           /pause /resume     │
│  • threshold evaluator            │      │   │           /restart /metrics  │
└───────────────┬───────────────────┘      │   │           /alerts /quarantine│
                │ owns N                   │   └─ AlertPump ◄── EventBus      │
                v                          └──────────────┬───────────────────┘
┌──────────────────────────────────────────┐              │ subscribe()
│      InstanceSupervisor  (× N)           │              │
│      (core/supervisor.py)                │              │
│                                          │   ┌──────────┴───────────┐
│  state machine:                          │   │       EventBus       │
│   STOPPED → STARTING → RUNNING           │──►│    (core/events.py)  │
│     ↘ DEGRADED ↘ UNHEALTHY               │   │  fan-out, bounded,   │
│        ↘ RESTARTING ↘ QUARANTINED        │   │  drop-oldest queues  │
│     ↘ PAUSED  ↘ STOPPING → STOPPED       │   └──────────────────────┘
│                                          │
│  supervise loop:                         │
│   1. IsolationStrategy.materialize()     │
│   2. asyncio.create_subprocess_exec      │
│   3. WindowManager.place(pid)            │
│   4. HealthMonitor.evaluate() @ interval │
│   5. on fail → terminate → backoff       │
│   6. crash-window breaker → QUARANTINED  │
└───┬──────────────┬───────────────┬───────┘
    │              │               │
    v              v               v
┌─────────────┐ ┌────────────────┐ ┌────────────────────────────────────────┐
│ Isolation   │ │ HealthMonitor  │ │            WindowManager               │
│ (isolation) │ │  (health.py)   │ │             (window.py)                │
│             │ │                │ │                                        │
│ • none      │ │ ProcessAlive   │ │ Win32 backend (ctypes/user32):         │
│ • env_sep   │ │ Heartbeat(file)│ │  EnumWindows → GetWindowThreadProcessId │
│ • runas     │ │ WindowResponsive│ │  → SetWindowPos(grid cell)            │
│ • container │ │ PortProbe(TCP/ │ │ X11 backend: wmctrl / xdotool          │
│ • wrapper   │ │   UDP srcds)   │ │ Noop backend: headless                 │
└─────────────┘ └────────────────┘ └────────────────────────────────────────┘
                        │
                        v
              ┌───────────────────────┐
              │  srcds_* / dota bot   │
              │  instance process     │
              │  writes heartbeat →   │
              │  runtime/hb/<id>.json │
              └───────────────────────┘
```

### Control & data flow

| Path | Mechanism |
|---|---|
| Orchestrator → Bot | `EventBus.subscribe()` bounded queue; `AlertPump` drains and pushes |
| Bot → Orchestrator | direct `await orchestrator.<method>()` — single event loop, no IPC |
| Supervisor → Orchestrator | shared `EventBus` + `InstanceState` snapshot read |
| Instance → Supervisor | heartbeat file (atomic write), exit code, window responsiveness, port |

### Failure taxonomy

| Symptom | Detector | Action |
|---|---|---|
| Process exits | `returncode is not None` | backoff restart |
| Hung UI thread | `IsHungAppWindow` + `SendMessageTimeoutW(WM_NULL)` | SIGTERM → grace → kill → restart |
| Silent hang (headless) | heartbeat mtime/seq staleness | terminate → restart |
| Port dead | TCP connect / UDP A2S probe | DEGRADED → UNHEALTHY after N consecutive |
| Crash loop | ≥ `crash_threshold` restarts inside `crash_window_s` | QUARANTINED, alert, manual `/restart` |

## 2. Runtime layout

```
<runtime_dir>/
  hb/<instance_id>.json       heartbeat sink (written by instance or wrapper)
  logs/<instance_id>.log      merged stdout/stderr, rotated by size
  state/fleet.json            last-known snapshot (crash-safe restart)
```

## 3. Files

| File | Purpose |
|---|---|
| `config.schema.json` | JSON Schema (draft-07) for the unified config |
| `config.example.json` | Working 6-instance example |
| `core/models.py` | Enums, dataclasses, snapshots, events |
| `core/config.py` | Loader, env expansion, semantic validation |
| `core/events.py` | Bounded fan-out async event bus |
| `core/window.py` | Win32 / X11 / noop window placement backends |
| `core/health.py` | Composite health checks |
| `core/isolation.py` | Session, container, wrapper isolation strategies |
| `core/supervisor.py` | Per-instance state machine + restart policy |
| `core/orchestrator.py` | Fleet controller, threshold evaluator, persistence |
| `telemetry/bot.py` | aiogram v3 controller, auth, alert pump |
| `main.py` | Entrypoint, signal handling, graceful shutdown |

## 4. Heartbeat contract

The supervised process (or a thin wrapper) writes, at ≤ `interval_s`:

```json
{"seq": 1421, "ts": 1754812800.42, "phase": "running", "tick": 30.0, "players": 10}
```

Write to `<id>.json.tmp` then `os.replace` — the reader tolerates partial files but
atomic replace removes the race entirely. Staleness is judged on `ts` if present,
otherwise on file mtime; `seq` must be strictly increasing or the instance is
considered wedged even if the file is being rewritten.
