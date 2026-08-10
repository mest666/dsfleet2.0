"""Dear PyGui fleet dashboard.

Threading model
---------------
Dear PyGui owns the main thread; the asyncio orchestrator runs on a worker thread with
its own event loop. The two never touch each other's objects directly:

    GUI thread   --  asyncio.run_coroutine_threadsafe(coro, loop)  -->  loop thread
    loop thread  --  Orchestrator.snapshot() (pure read, no await)  -->  GUI thread

`snapshot()` builds plain dataclasses from already-materialised supervisor fields and
never awaits, so calling it from the render thread is safe. Every *mutating* action
(emergency stop, restart, pause) goes through `run_coroutine_threadsafe` and is never
awaited on the render thread — blocking there would freeze the UI.

Rendering cost
--------------
Dear PyGui draws through the GPU, and the render loop only touches widget values that
actually changed. The snapshot poll runs at REFRESH_HZ (default 4), decoupled from the
frame rate, so CPU cost stays flat regardless of vsync.
"""
from __future__ import annotations

import asyncio
import logging
import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional, Sequence

import dearpygui.dearpygui as dpg

from core.models import FleetSnapshot, InstanceSnapshot, InstanceState, fmt_duration
from core.orchestrator import Orchestrator

log = logging.getLogger("dsfleet.gui")

__all__ = ["Dashboard", "run_dashboard"]

REFRESH_HZ = 4.0
GRID_ROWS = 2
GRID_COLS = 5

# RGBA. Colour maps state, not health, because state is what an operator acts on.
COLOR_GREEN = (32, 132, 62, 255)      # RUNNING
COLOR_YELLOW = (188, 149, 26, 255)    # STARTING / RESTARTING / DEGRADED
COLOR_RED = (168, 42, 42, 255)        # UNHEALTHY / QUARANTINED
COLOR_GRAY = (78, 82, 90, 255)        # STOPPED / PAUSED / STOPPING
COLOR_TEXT = (238, 240, 244, 255)
COLOR_DIM = (168, 174, 184, 255)
COLOR_BG = (22, 24, 28, 255)
COLOR_PANEL = (32, 35, 41, 255)

STATE_COLORS: dict[InstanceState, tuple[int, int, int, int]] = {
    InstanceState.RUNNING: COLOR_GREEN,
    InstanceState.STARTING: COLOR_YELLOW,
    InstanceState.RESTARTING: COLOR_YELLOW,
    InstanceState.DEGRADED: COLOR_YELLOW,
    InstanceState.UNHEALTHY: COLOR_RED,
    InstanceState.QUARANTINED: COLOR_RED,
    InstanceState.STOPPED: COLOR_GRAY,
    InstanceState.STOPPING: COLOR_GRAY,
    InstanceState.PAUSED: COLOR_GRAY,
}

STATE_LABELS: dict[InstanceState, str] = {
    InstanceState.RUNNING: "IN MATCH",
    InstanceState.STARTING: "STARTING",
    InstanceState.RESTARTING: "RESTARTING",
    InstanceState.DEGRADED: "DEGRADED",
    InstanceState.UNHEALTHY: "UNHEALTHY",
    InstanceState.QUARANTINED: "CRASHED",
    InstanceState.STOPPED: "IDLE",
    InstanceState.STOPPING: "STOPPING",
    InstanceState.PAUSED: "PAUSED",
}


@dataclass(slots=True)
class CellTags:
    """Widget ids for one grid cell, resolved once at build time."""
    panel: int | str
    header: int | str
    state: int | str
    uptime: int | str
    restarts: int | str
    heartbeat: int | str
    theme: int | str


class Dashboard:
    def __init__(self, orch: Orchestrator, loop: asyncio.AbstractEventLoop,
                 on_shutdown: Optional[Callable[[], None]] = None,
                 title: str = "dsfleet — dedicated server fleet") -> None:
        self.orch = orch
        self.loop = loop
        self.on_shutdown = on_shutdown
        self.title = title

        self._cells: dict[str, CellTags] = {}
        self._order: list[str] = []
        self._last_poll = 0.0
        self._snapshot: Optional[FleetSnapshot] = None
        self._status_line = "initialising…"
        self._emergency_armed = False
        self._emergency_armed_at = 0.0
        self._lock = threading.Lock()

    # ---------------------------------------------------------------- helpers

    def _submit(self, coro) -> None:
        """Fire a coroutine onto the orchestrator loop without blocking the UI."""
        try:
            future = asyncio.run_coroutine_threadsafe(coro, self.loop)
        except RuntimeError as exc:
            log.error("cannot submit to orchestrator loop: %s", exc)
            self._set_status(f"loop unavailable: {exc}")
            return

        def _done(fut) -> None:
            try:
                fut.result()
            except asyncio.CancelledError:
                pass
            except Exception as exc:  # surface to the operator, don't swallow
                log.exception("dashboard action failed")
                self._set_status(f"action failed: {exc!r}")

        future.add_done_callback(_done)

    def _set_status(self, text: str) -> None:
        with self._lock:
            self._status_line = text

    # ---------------------------------------------------------------- callbacks

    def _cb_emergency(self, sender, app_data, user_data) -> None:
        """Two-stage: first click arms, second within 5s fires.

        A single misclick killing ten servers mid-measurement is worse than an extra
        click, and a modal dialog would block the render loop.
        """
        now = time.monotonic()
        if not self._emergency_armed or (now - self._emergency_armed_at) > 5.0:
            self._emergency_armed = True
            self._emergency_armed_at = now
            dpg.set_item_label("btn_emergency", "!!  CONFIRM STOP  !!")
            self._set_status("EMERGENCY STOP armed — click again within 5s to confirm")
            return

        self._emergency_armed = False
        dpg.set_item_label("btn_emergency", "EMERGENCY STOP")
        self._set_status("EMERGENCY STOP issued — killing all process trees")
        self._submit(self.orch.emergency_stop("dashboard emergency stop"))

    def _cb_pause_resume(self, sender, app_data, user_data) -> None:
        if self.orch.paused:
            self._set_status("resuming fleet…")
            self._submit(self.orch.resume_all())
        else:
            self._set_status("pausing fleet…")
            self._submit(self.orch.pause_all())

    def _cb_restart_all(self, sender, app_data, user_data) -> None:
        self._set_status("fleet restart issued")
        self._submit(self.orch.restart_all("dashboard restart"))

    def _cb_restart_one(self, sender, app_data, user_data) -> None:
        instance_id = user_data
        self._set_status(f"restart issued for {instance_id}")
        self._submit(self.orch.restart(instance_id, "dashboard restart"))

    # ---------------------------------------------------------------- build

    def _build_cell_theme(self, tag: str) -> int | str:
        with dpg.theme() as theme:
            with dpg.theme_component(dpg.mvAll):
                dpg.add_theme_color(dpg.mvThemeCol_ChildBg, COLOR_GRAY,
                                    category=dpg.mvThemeCat_Core)
                dpg.add_theme_style(dpg.mvStyleVar_ChildRounding, 6,
                                    category=dpg.mvThemeCat_Core)
                dpg.add_theme_style(dpg.mvStyleVar_WindowPadding, 10, 8,
                                    category=dpg.mvThemeCat_Core)
        return theme

    def _build_global_theme(self) -> int | str:
        with dpg.theme() as theme:
            with dpg.theme_component(dpg.mvAll):
                dpg.add_theme_color(dpg.mvThemeCol_WindowBg, COLOR_BG)
                dpg.add_theme_color(dpg.mvThemeCol_ChildBg, COLOR_PANEL)
                dpg.add_theme_color(dpg.mvThemeCol_Text, COLOR_TEXT)
                dpg.add_theme_color(dpg.mvThemeCol_Button, (52, 57, 66, 255))
                dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, (70, 76, 88, 255))
                dpg.add_theme_style(dpg.mvStyleVar_FrameRounding, 4)
        return theme

    def _build_emergency_theme(self) -> int | str:
        with dpg.theme() as theme:
            with dpg.theme_component(dpg.mvAll):
                dpg.add_theme_color(dpg.mvThemeCol_Button, (150, 30, 30, 255))
                dpg.add_theme_color(dpg.mvThemeCol_ButtonHovered, (196, 44, 44, 255))
                dpg.add_theme_color(dpg.mvThemeCol_ButtonActive, (220, 60, 60, 255))
                dpg.add_theme_style(dpg.mvStyleVar_FramePadding, 16, 12)
        return theme

    def build(self) -> None:
        ids = self.orch.ids()
        self._order = ids[: GRID_ROWS * GRID_COLS]
        if len(ids) > len(self._order):
            log.warning("dashboard grid holds %d cells; %d instances not shown",
                        len(self._order), len(ids) - len(self._order))

        dpg.create_context()
        global_theme = self._build_global_theme()
        emergency_theme = self._build_emergency_theme()

        with dpg.window(tag="root"):
            # ---- header -------------------------------------------------
            with dpg.group(horizontal=True):
                dpg.add_text("dsfleet", tag="txt_title")
                dpg.add_spacer(width=20)
                dpg.add_text("", tag="txt_fleet_summary", color=COLOR_DIM)

            dpg.add_separator()

            with dpg.group(horizontal=True):
                dpg.add_button(label="Pause fleet", tag="btn_pause", width=130,
                               callback=self._cb_pause_resume)
                dpg.add_button(label="Restart all", tag="btn_restart", width=130,
                               callback=self._cb_restart_all)
                dpg.add_spacer(width=30)
                dpg.add_text("Server region:", color=COLOR_DIM)
                dpg.add_combo(
                    items=["local (LAN)", "自宅 / Tokyo host", "Frankfurt host",
                           "Singapore host"],
                    default_value="local (LAN)", tag="cmb_region", width=190,
                )
                dpg.add_spacer(width=30)
                dpg.add_button(label="EMERGENCY STOP", tag="btn_emergency",
                               width=220, height=40, callback=self._cb_emergency)
                dpg.bind_item_theme("btn_emergency", emergency_theme)

            dpg.add_separator()
            dpg.add_spacer(height=6)

            # ---- 2 x 5 grid ---------------------------------------------
            index = 0
            for _row in range(GRID_ROWS):
                with dpg.group(horizontal=True):
                    for _col in range(GRID_COLS):
                        if index >= len(self._order):
                            dpg.add_spacer(width=232)
                            index += 1
                            continue
                        iid = self._order[index]
                        index += 1
                        theme = self._build_cell_theme(iid)
                        with dpg.child_window(width=228, height=132,
                                              tag=f"cell_{iid}", border=False) as panel:
                            header = dpg.add_text(iid[:22])
                            state = dpg.add_text("—")
                            dpg.add_spacer(height=4)
                            uptime = dpg.add_text("uptime  —", color=COLOR_DIM)
                            restarts = dpg.add_text("restarts —", color=COLOR_DIM)
                            heartbeat = dpg.add_text("hb      —", color=COLOR_DIM)
                            dpg.add_spacer(height=2)
                            dpg.add_button(label="restart", width=76, height=22,
                                           user_data=iid, callback=self._cb_restart_one)
                        dpg.bind_item_theme(panel, theme)
                        self._cells[iid] = CellTags(
                            panel=panel, header=header, state=state, uptime=uptime,
                            restarts=restarts, heartbeat=heartbeat, theme=theme)
                dpg.add_spacer(height=6)

            dpg.add_separator()
            dpg.add_text("", tag="txt_status", color=COLOR_DIM)
            dpg.add_text("", tag="txt_breaches", color=(224, 128, 96, 255))

        dpg.bind_theme(global_theme)
        dpg.create_viewport(title=self.title, width=1240, height=540,
                            resizable=True, vsync=True)
        dpg.setup_dearpygui()
        dpg.show_viewport()
        dpg.set_primary_window("root", True)

    # ---------------------------------------------------------------- update

    def _apply_cell(self, iid: str, snap: Optional[InstanceSnapshot]) -> None:
        cell = self._cells.get(iid)
        if cell is None:
            return
        if snap is None:
            dpg.set_value(cell.state, "MISSING")
            return

        color = STATE_COLORS.get(snap.state, COLOR_GRAY)
        label = STATE_LABELS.get(snap.state, snap.state.value.upper())

        # Rebinding the theme colour is cheaper than rebuilding the theme.
        dpg.set_value(cell.state, f"{label}")
        dpg.set_value(cell.uptime, f"uptime   {fmt_duration(snap.uptime_s)}")
        dpg.set_value(cell.restarts,
                      f"restarts {snap.restarts_total} ({snap.restarts_recent} recent)")
        hb = "—" if snap.heartbeat_age_s is None else f"{snap.heartbeat_age_s:.0f}s"
        dpg.set_value(cell.heartbeat, f"hb       {hb}  pid {snap.pid or '—'}")

        for item in dpg.get_item_children(cell.theme, 1) or []:
            for entry in dpg.get_item_children(item, 1) or []:
                if dpg.get_item_type(entry) == "mvAppItemType::mvThemeColor":
                    dpg.configure_item(entry, value=color)
                    break

    def update(self) -> None:
        now = time.monotonic()
        if (now - self._last_poll) < (1.0 / REFRESH_HZ):
            return
        self._last_poll = now

        try:
            snapshot = self.orch.snapshot()
        except Exception:
            log.exception("snapshot failed")
            self._set_status("snapshot failed — see log")
            return
        self._snapshot = snapshot

        by_id = {i.id: i for i in snapshot.instances}
        for iid in self._order:
            self._apply_cell(iid, by_id.get(iid))

        m = snapshot.metrics
        flag = "PAUSED" if snapshot.paused else ("NOMINAL" if m.ok else "BREACH")
        dpg.set_value(
            "txt_fleet_summary",
            f"{flag}   |   {m.healthy}/{m.total} healthy   |   "
            f"{m.running} running   {m.quarantined} quarantined   |   "
            f"restarts/h {m.restarts_last_hour}   |   "
            f"fleet uptime {fmt_duration(m.fleet_uptime_s)}")
        dpg.set_item_label("btn_pause", "Resume fleet" if snapshot.paused else "Pause fleet")

        if self._emergency_armed and (time.monotonic() - self._emergency_armed_at) > 5.0:
            self._emergency_armed = False
            dpg.set_item_label("btn_emergency", "EMERGENCY STOP")
            self._set_status("emergency stop disarmed (timeout)")

        with self._lock:
            status = self._status_line
        dpg.set_value("txt_status", status)
        dpg.set_value("txt_breaches",
                      "  •  ".join(m.breaches) if m.breaches else "")

    # ---------------------------------------------------------------- run

    def run(self) -> None:
        self.build()
        self._set_status("connected to orchestrator")
        try:
            while dpg.is_dearpygui_running():
                self.update()
                dpg.render_dearpygui_frame()
        except Exception:
            log.exception("render loop crashed")
        finally:
            try:
                dpg.destroy_context()
            except Exception:
                pass
            if self.on_shutdown is not None:
                try:
                    self.on_shutdown()
                except Exception:
                    log.exception("shutdown callback failed")


def run_dashboard(orch: Orchestrator, loop: asyncio.AbstractEventLoop,
                  on_shutdown: Optional[Callable[[], None]] = None) -> None:
    Dashboard(orch, loop, on_shutdown=on_shutdown).run()
