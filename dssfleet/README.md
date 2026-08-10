# dsfleet — deployment

## Install

```bash
pip install aiogram>=3.7 psutil jsonschema
#   aiogram    — Telegram control plane (optional; orchestrator runs headless without it)
#   psutil     — pause/resume via process suspend + reliable tree-kill (strongly recommended)
#   jsonschema — validates config against config.schema.json at load (optional)
```

Windows window management needs no extra packages — `core/window.py` binds `user32`
directly through `ctypes`. X11 needs `xdotool` (and optionally `wmctrl`) on `PATH`.

## Run

```bash
export DSFLEET_TG_TOKEN=123456:AA...          # referenced as ${DSFLEET_TG_TOKEN} in config
python main.py --config config.json --dry-run # validate + print resolved grid geometry
python main.py --config config.json
```

`--dry-run` resolves every `${VAR}`, validates the schema, checks grid collisions and
prints the exact pixel rectangle each instance will be placed at.

## Heartbeat wrapper

Any supervised process can publish liveness by writing its heartbeat file atomically.
Minimum viable emitter:

```python
import json, os, time
path = os.environ["DSFLEET_HEARTBEAT"]           # or runtime_dir/hb/<id>.json
seq = 0
while True:
    seq += 1
    tmp = path + ".tmp"
    with open(tmp, "w") as fh:
        json.dump({"seq": seq, "ts": time.time(), "phase": "running"}, fh)
    os.replace(tmp, path)
    time.sleep(5)
```

`require_increasing_seq` catches the case a process keeps rewriting the file from a
timer thread while its main loop is wedged — mtime alone would look healthy.

## Telegram admin ids

Send `/whoami` to the bot from the account you want to authorise, read the id from the
reply, add it to `telegram.admin_ids`, restart. Until an id is listed, every update from
it is dropped by the outer middleware before reaching a handler.

## Operational notes

- **Quarantine is deliberate.** After `crash_threshold` restarts inside `crash_window_s`
  the supervisor stops trying and alerts. Clear it with `/restart <id>`, which also
  resets the backoff ladder and the crash history.
- **`pause` without psutil** halts supervision but cannot suspend the process; the log
  says which happened, and `/status` shows `PAUSED` either way.
- **`runas` isolation on Windows** uses `runas /savecred`, which requires the credential
  to have been saved once interactively. For unattended deployment, run dsfleet as a
  service and replace `RunAsStrategy` with a `CreateProcessAsUser` implementation — the
  strategy interface (`materialize() -> LaunchSpec`) is the only contract to satisfy.
- **DPI awareness** is set at `Win32Backend` construction. Without it Windows silently
  rescales `SetWindowPos` coordinates on high-DPI monitors and the grid drifts.
- **`reapply_interval_s`** re-asserts geometry periodically; useful because Source
  engine clients often reposition themselves after a resolution or mode change.
- Logs: `<runtime_dir>/logs/<id>.log` (size-rotated), orchestrator log at
  `<runtime_dir>/logs/dsfleet.log`. Last snapshot at `<runtime_dir>/state/fleet.json`.

## Verified behaviour

Smoke-tested end to end on a three-instance fleet: a healthy heartbeat emitter stayed
`RUNNING`; a process exiting with code 3 every two seconds tripped the crash breaker and
went `QUARANTINED`; a process that wrote one heartbeat then wedged was detected as stale,
terminated, restarted, and eventually quarantined. Threshold evaluation produced
`healthy_ratio 33% < 50%` and `quarantined 2 > 1`, and SIGINT drained all supervisors
cleanly.

## Extending toward the simulation layer

The supervisor is transport-agnostic about what it runs. For scenario control on the
dedicated servers, the natural next component is an RCON client (`-usercon` is already in
the example args) driving `dota_bot_*` convars and custom-game addon state, with the
addon publishing phase transitions into the heartbeat JSON — `HealthMonitor` already
parses and exposes that payload, so scenario state surfaces in `/instance <id>` for free.

---

# Part 2 — scaling, GUI, RCON and the Lua scenario layer

## New modules

| File | Purpose |
|---|---|
| `core/preflight.py` | RAM / pagefile / CPU / disk checks with a headroom evaluator |
| `core/affinity.py` | CCX-aware CPU pinning and priority control |
| `core/rcon.py` | Source RCON client + fleet fan-out controller |
| `gui/dashboard.py` | Dear PyGui 2×5 dashboard |
| `main_gui.py` | GUI entrypoint (orchestrator on a worker thread) |
| `tools/build_items_lua.py` | JSON item profile → generated Lua |
| `scripts/vscripts/bots/*.lua` | Hero selection, 2+1+2 laning, item purchasing |
| `config.10x.json` | Ten-instance srcds fleet |

## Running the GUI

```bash
pip install dearpygui psutil
python main_gui.py --config config.10x.json
```

Dear PyGui owns the main thread; the orchestrator runs its asyncio loop on a worker.
The GUI calls `Orchestrator.snapshot()` directly (a pure, non-awaiting read of already
materialised fields) and routes every *mutating* action through
`asyncio.run_coroutine_threadsafe`, never awaiting on the render thread.

**EMERGENCY STOP is two-stage**: the first click arms the button for five seconds, the
second fires. It sets the shutdown flag on every supervisor *before* killing, so nothing
relaunches into the gap, then kills all process trees concurrently. Verified in an
integration test: three live processes, three trees killed, nothing respawned, all
supervisors left in `STOPPED`.

The region dropdown selects which **host** you are targeting — it labels your own
server endpoints, not a matchmaking region.

## Preflight on your box

For 16 GB with ten instances at ~1.1 GB each: 16 − 3 (OS reserve) = 13 GB usable versus
10.7 GB required, leaving ~2.3 GB headroom → `OK`. Push to twelve instances and it flips
to overcommit and starts demanding pagefile coverage. The `per_instance_mb` default of
1100 is an estimate — measure your actual idle RSS with `psutil` once a server is up and
set it from data.

If the verdict is `FAIL`, startup raises unless you set `preflight.block_on_fail: false`.

## CPU pinning

`physical_first` on an 8-core/16-thread Ryzen with `reserve_cpus: [0, 1]`:

```
srv-01 → [2]   srv-02 → [4]   srv-03 → [6]   srv-04 → [8]
srv-05 → [10]  srv-06 → [12]  srv-07 → [14]
srv-08 → [3]   srv-09 → [5]   srv-10 → [7]
```

The first seven land on distinct physical cores; only after those are exhausted does it
start doubling onto SMT siblings. Reserving cores 0–1 keeps interrupt handling and the
orchestrator's own event loop off the game servers.

`priority: "below_normal"` is deliberate. Running ten servers at `high` starves the
orchestrator loop and the health probes start timing out — which looks exactly like the
instances hanging.

## Launch arguments — what actually applies

Several client-side flags do nothing on a dedicated server. Of the set commonly passed:

| Flag | On `-dedicated` |
|---|---|
| `-high`, `-nocrashdialog`, `-console`, `-usercon` | applies |
| `-novid`, `-nojoy` | harmless no-ops (no client to skip video for) |
| `+fps_max`, `+mat_viewportscale`, `-frame_limit` | **client-only** — no renderer exists server-side |
| `-low` | conflicts with `-high`; use the config's `affinity.priority` instead |

Server-side equivalents that do matter are in `config.10x.json`:
`+sv_hibernate_when_empty 0` (keeps tick running for measurement),
`+tv_enable 0` (SourceTV doubles the network cost per instance),
`+sv_lan 1` (no GC round-trip on startup).

## RCON

```python
from core.rcon import RconController, ScenarioCommands

ctl = RconController(default_password="...")
ctl.register_from_config(cfg.instances)          # reads rcon_port / rcon_password

await ctl.sequence("srv-01", ScenarioCommands.load_bot_match(game_mode=23))
results = await ctl.broadcast("dota_bot_reload_scripts")   # hot-reload Lua fleet-wide
```

Requires `-usercon` and `rcon_password` set on the server. The client handles the
multipart-response sentinel, transparent reconnect on idle-socket drops, and auth
rejection. Codec round-trip tested against a mock Valve-protocol server, including a
split response and a rejected password.

## Lua scenario layer

Deploy to `<dota>/game/dota/scripts/vscripts/bots/`:

- **`hero_selection.lua`** — picks heroes and returns the `playerID → LANE_*` map.
  The 2+1+2 split lives in `LANE_LAYOUT`; the engine hands each bot its lane and
  `mode_laning_generic.lua` reads it back via `bot:GetAssignedLane()`.
  Player ids are explicitly sorted, because unstable ordering would silently reshuffle
  lanes between runs and make comparisons meaningless.
- **`mode_laning_generic.lua`** — DEPLOY → HOLD → RETREAT → RESET waypoint state
  machine with conditional last-hitting (only swings when the hit kills, so the lane
  doesn't push and drag the equilibrium point). Emits a greppable
  `[dsfleet.bot] t=… gpm=… xpm=… lh=…` line every 15s, which dsfleet's log pump captures
  into `<runtime_dir>/logs/<id>.log`.
- **`item_purchase_generic.lua`** — flattens a declarative build into a queue and buys
  via `ActionImmediate_PurchaseItem`. No shop UI, no courier hotkeys; the engine handles
  delivery.

Build profiles stay in JSON and compile to Lua:

```bash
python tools/build_items_lua.py profiles/items.example.json \
  --out "D:/steamcmd/dota2ds/game/dota/scripts/vscripts/bots/item_builds.lua"
```

Then `dota_bot_reload_scripts` over RCON picks it up without a server restart.

## Caveats worth knowing

- **The Lua is structurally checked, not executed.** No Lua interpreter was available
  offline, so the scripts passed a block-balance check only. More importantly, the bot
  API drifts between patches — diff against the shipped reference scripts in
  `game/dota/scripts/vscripts/bots/` before trusting function names.
- **The waypoint coordinates are approximations.** They path sensibly but the exact lane
  equilibrium points shift with map revisions. Print `bot:GetLocation()` in-game and tune
  them before using the setup for measurement.
- **The Win32 and Dear PyGui paths are untested here** (Linux container, no display).
  Compile-clean, but they need a pass on real hardware.
- **`per_instance_mb` is an estimate.** Measure yours.
