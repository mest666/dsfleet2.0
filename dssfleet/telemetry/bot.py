"""Telegram control plane (aiogram v3).

Security model
--------------
* An **outer** middleware rejects every update whose `from_user.id` is not in
  `telegram.admin_ids`. Outer placement means unauthorised updates never reach a
  filter or handler.
* Destructive commands (`/restart all`, `/stop`, `/pause`) require an inline
  confirmation bound to a single-use nonce, the issuing user id, and a TTL.
* Per-user token-bucket throttle guards against command floods.

Commands
--------
    /status                 fleet overview
    /instance <id>          detail for one instance
    /log <id> [lines]       tail of an instance log
    /metrics                fleet metrics + threshold breaches
    /events [n]             recent event history
    /pause  [id]            suspend fleet or one instance
    /resume [id]            resume fleet or one instance
    /restart <id|all>       restart (clears quarantine + backoff)
    /stop <id>              stop supervision of one instance
    /startinst <id>         start supervision of one instance
    /alerts on|off|status   toggle push alerts
    /whoami                 show your Telegram id
"""
from __future__ import annotations

import asyncio
import contextlib
import html
import logging
import secrets
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramAPIError, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, CommandObject
from aiogram.types import (CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup,
                           Message, TelegramObject, Update)

from core.config import TelegramConfig
from core.events import Subscription
from core.models import Event, Severity, fmt_duration
from core.orchestrator import Orchestrator

log = logging.getLogger("dsfleet.telegram")

__all__ = ["TelemetryController"]

TG_MAX = 4000  # keep clear of the 4096 hard limit after HTML expansion
CONFIRM_TTL_S = 60.0


def esc(text: object) -> str:
    return html.escape(str(text), quote=False)


def chunks(text: str, size: int = TG_MAX) -> list[str]:
    if len(text) <= size:
        return [text]
    out, buf = [], []
    length = 0
    for line in text.splitlines(keepends=True):
        if length + len(line) > size and buf:
            out.append("".join(buf))
            buf, length = [], 0
        buf.append(line)
        length += len(line)
    if buf:
        out.append("".join(buf))
    return out


# ============================================================ middlewares

class AdminAuthMiddleware(BaseMiddleware):
    """Outer middleware: drop every update from a non-admin before routing."""

    def __init__(self, admin_ids: frozenset[int]) -> None:
        self.admin_ids = admin_ids
        self._denied: dict[int, float] = {}

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        uid = getattr(user, "id", None)
        if uid in self.admin_ids:
            return await handler(event, data)

        # Reply at most once per minute per stranger; never leak fleet details.
        now = time.time()
        if uid is not None and now - self._denied.get(uid, 0.0) > 60.0:
            self._denied[uid] = now
            log.warning("unauthorised access attempt from uid=%s (%s)",
                        uid, getattr(user, "username", "?"))
            with contextlib.suppress(TelegramAPIError):
                if isinstance(event, Update) and event.message:
                    await event.message.answer("⛔ Not authorised.")
                elif isinstance(event, Message):
                    await event.answer("⛔ Not authorised.")
                elif isinstance(event, CallbackQuery):
                    await event.answer("Not authorised.", show_alert=True)
        return None


class ThrottleMiddleware(BaseMiddleware):
    """Simple per-user minimum interval between accepted commands."""

    def __init__(self, min_interval_s: float) -> None:
        self.min_interval = min_interval_s
        self._last: dict[int, float] = {}

    async def __call__(self, handler, event: TelegramObject, data: dict[str, Any]) -> Any:
        if self.min_interval <= 0:
            return await handler(event, data)
        user = data.get("event_from_user")
        uid = getattr(user, "id", None)
        if uid is None:
            return await handler(event, data)
        now = time.monotonic()
        last = self._last.get(uid, 0.0)
        if now - last < self.min_interval:
            if isinstance(event, CallbackQuery):
                with contextlib.suppress(TelegramAPIError):
                    await event.answer("Slow down.", show_alert=False)
            return None
        self._last[uid] = now
        return await handler(event, data)


# ============================================================ confirmations

@dataclass(slots=True)
class PendingAction:
    action: str
    arg: Optional[str]
    user_id: int
    expires_at: float


class ConfirmStore:
    def __init__(self) -> None:
        self._items: dict[str, PendingAction] = {}

    def issue(self, action: str, arg: Optional[str], user_id: int) -> str:
        self.prune()
        nonce = secrets.token_urlsafe(8)
        self._items[nonce] = PendingAction(action, arg, user_id,
                                           time.time() + CONFIRM_TTL_S)
        return nonce

    def take(self, nonce: str, user_id: int) -> Optional[PendingAction]:
        self.prune()
        item = self._items.pop(nonce, None)
        if item is None or item.user_id != user_id:
            return None
        return item

    def prune(self) -> None:
        now = time.time()
        for k in [k for k, v in self._items.items() if v.expires_at < now]:
            self._items.pop(k, None)


# ============================================================ controller

class TelemetryController:
    def __init__(self, cfg: TelegramConfig, orch: Orchestrator) -> None:
        self.cfg = cfg
        self.orch = orch
        self.bot = Bot(token=cfg.token,
                       default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        self.dp = Dispatcher()
        self.router = Router(name="dsfleet")
        self.confirms = ConfirmStore()
        self.alerts_enabled = cfg.alerts_enabled
        self._min_rank = Severity(cfg.alert_min_severity).rank
        self._sub: Optional[Subscription] = None
        self._pump: Optional[asyncio.Task[None]] = None
        self._poller: Optional[asyncio.Task[None]] = None

        self.dp.update.outer_middleware(AdminAuthMiddleware(cfg.admin_ids))
        self.dp.message.middleware(ThrottleMiddleware(cfg.command_rate_limit_s))
        self.dp.callback_query.middleware(ThrottleMiddleware(cfg.command_rate_limit_s))
        self._register()
        self.dp.include_router(self.router)

    # ------------------------------------------------------------ lifecycle

    async def start(self) -> None:
        self._sub = self.orch.bus.subscribe("telegram", maxsize=1024)
        self._pump = asyncio.create_task(self._alert_pump(), name="tg:alerts")
        self._poller = asyncio.create_task(self._run_polling(), name="tg:polling")
        await self.broadcast("🟢 <b>dsfleet controller online</b>\n" + self._fleet_line(),
                             force=True)

    async def stop(self) -> None:
        with contextlib.suppress(Exception):
            await self.broadcast("⚪ <b>dsfleet controller shutting down</b>", force=True)
        for task in (self._pump, self._poller):
            if task and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
        if self._sub:
            self._sub.close()
        with contextlib.suppress(Exception):
            await self.dp.stop_polling()
        with contextlib.suppress(Exception):
            await self.bot.session.close()

    async def _run_polling(self) -> None:
        backoff = 2.0
        while True:
            try:
                await self.bot.delete_webhook(drop_pending_updates=True)
                await self.dp.start_polling(self.bot, handle_signals=False,
                                            allowed_updates=["message", "callback_query"])
                return
            except asyncio.CancelledError:
                raise
            except TelegramAPIError as exc:
                log.error("telegram polling error: %s; retrying in %.0fs", exc, backoff)
            except Exception:
                log.exception("unexpected polling failure; retrying in %.0fs", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 120.0)

    # ------------------------------------------------------------ sending

    async def _send(self, chat_id: int, text: str,
                    markup: Optional[InlineKeyboardMarkup] = None) -> None:
        for part in chunks(text):
            for attempt in range(3):
                try:
                    await self.bot.send_message(chat_id, part, reply_markup=markup,
                                                disable_web_page_preview=True)
                    break
                except TelegramRetryAfter as exc:
                    await asyncio.sleep(exc.retry_after + 0.5)
                except TelegramForbiddenError:
                    log.warning("chat %s blocked the bot; skipping", chat_id)
                    return
                except TelegramAPIError as exc:
                    log.warning("send to %s failed (attempt %d): %s", chat_id, attempt + 1, exc)
                    await asyncio.sleep(1.0 + attempt)
            else:
                log.error("giving up sending to chat %s", chat_id)

    async def broadcast(self, text: str, *, force: bool = False) -> None:
        if not force and not self.alerts_enabled:
            return
        targets = self.cfg.alert_chat_ids or tuple(self.cfg.admin_ids)
        await asyncio.gather(*(self._send(cid, text) for cid in targets),
                             return_exceptions=True)

    # ------------------------------------------------------------ alert pump

    async def _alert_pump(self) -> None:
        assert self._sub is not None
        buffer: list[Event] = []
        while True:
            try:
                event = await self._sub.get()
                if event.severity.rank < self._min_rank:
                    continue
                buffer.append(event)

                # Coalesce a burst into one message.
                deadline = time.monotonic() + self.cfg.alert_coalesce_s
                while self.cfg.alert_coalesce_s > 0:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    try:
                        nxt = await asyncio.wait_for(self._sub.get(), timeout=remaining)
                    except asyncio.TimeoutError:
                        break
                    if nxt.severity.rank >= self._min_rank:
                        buffer.append(nxt)
                    if len(buffer) >= 20:
                        break

                if not self.alerts_enabled:
                    buffer.clear()
                    continue
                await self.broadcast(self._render_alerts(buffer))
                buffer.clear()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("alert pump error")
                buffer.clear()
                await asyncio.sleep(1.0)

    @staticmethod
    def _render_alerts(events: list[Event]) -> str:
        if len(events) == 1:
            e = events[0]
            who = f" <code>{esc(e.instance_id)}</code>" if e.instance_id else ""
            return f"{e.severity.emoji} <b>{esc(e.kind)}</b>{who}\n{esc(e.message)}"
        lines = [f"{events[0].severity.emoji} <b>{len(events)} events</b>"]
        for e in events:
            stamp = time.strftime("%H:%M:%S", time.localtime(e.ts))
            who = f" <code>{esc(e.instance_id)}</code>" if e.instance_id else ""
            lines.append(f"<code>{stamp}</code> {e.severity.emoji}{who} {esc(e.message)}")
        return "\n".join(lines)

    # ------------------------------------------------------------ rendering

    def _fleet_line(self) -> str:
        m = self.orch.metrics()
        flag = "⏸ PAUSED" if self.orch.paused else ("✅ OK" if m.ok else "❗ BREACH")
        return (f"{flag} — {m.healthy}/{m.total} healthy · "
                f"{m.quarantined} quarantined · uptime {fmt_duration(m.fleet_uptime_s)}")

    def _render_status(self) -> str:
        snap = self.orch.snapshot()
        m = snap.metrics
        head = "⏸ <b>FLEET PAUSED</b>" if snap.paused else (
            "✅ <b>FLEET NOMINAL</b>" if m.ok else "❗ <b>FLEET DEGRADED</b>")
        lines = [
            head,
            f"<code>healthy   {m.healthy}/{m.total}  ({m.healthy_ratio:.0%})</code>",
            f"<code>running   {m.running}   degraded {m.degraded}   unhealthy {m.unhealthy}</code>",
            f"<code>paused    {m.paused}   stopped  {m.stopped}   quarantined {m.quarantined}</code>",
            f"<code>restarts/h {m.restarts_last_hour}   uptime {fmt_duration(m.fleet_uptime_s)}</code>",
            "",
        ]
        for i in snap.instances:
            hb = "-" if i.heartbeat_age_s is None else f"{i.heartbeat_age_s:.0f}s"
            lines.append(
                f"{i.state.emoji} <b>{esc(i.id)}</b> "
                f"<code>{i.state.value:<11}</code> "
                f"up {fmt_duration(i.uptime_s):>6} · hb {hb:>5} · "
                f"rst {i.restarts_total}"
            )
        if m.breaches:
            lines.append("\n<b>Threshold breaches</b>")
            lines += [f"• {esc(b)}" for b in m.breaches]
        return "\n".join(lines)

    def _render_instance(self, iid: str) -> str:
        sup = self.orch.get(iid)
        if sup is None:
            return f"❓ Unknown instance <code>{esc(iid)}</code>"
        s = sup.snapshot()
        report = sup.last_report
        lines = [
            f"{s.state.emoji} <b>{esc(s.id)}</b> — <code>{s.state.value}</code> / "
            f"<code>{s.health.value}</code>",
            f"<code>pid        {s.pid or '-'}</code>",
            f"<code>uptime     {fmt_duration(s.uptime_s)}</code>",
            f"<code>in state   {fmt_duration(time.time() - s.since)}</code>",
            f"<code>restarts   {s.restarts_total} total / {s.restarts_recent} recent</code>",
            f"<code>exit code  {s.last_exit_code if s.last_exit_code is not None else '-'}</code>",
            f"<code>heartbeat  {'-' if s.heartbeat_age_s is None else f'{s.heartbeat_age_s:.1f}s'}</code>",
            f"<code>window     {sup.window_handle or '-'}</code>",
            f"<code>isolation  {esc(sup.cfg.isolation.mode)}</code>",
            f"<code>tags       {esc(', '.join(s.tags) or '-')}</code>",
        ]
        if s.last_error:
            lines.append(f"\n⚠️ <b>Last error:</b> {esc(s.last_error)}")
        if report:
            lines.append("\n<b>Checks</b>")
            for r in report.results:
                lines.append(f"{'✅' if r.ok else '❌'} <code>{esc(r.name)}</code> "
                             f"{esc(r.detail)}")
        return "\n".join(lines)

    # ------------------------------------------------------------ handlers

    def _register(self) -> None:
        r = self.router

        @r.message(Command("start", "help"))
        async def cmd_help(msg: Message) -> None:
            await msg.answer(
                "<b>dsfleet controller</b>\n\n"
                "/status — fleet overview\n"
                "/instance &lt;id&gt; — instance detail\n"
                "/log &lt;id&gt; [lines] — tail instance log\n"
                "/metrics — metrics + breaches\n"
                "/events [n] — recent events\n"
                "/pause [id] — suspend fleet or instance\n"
                "/resume [id] — resume fleet or instance\n"
                "/restart &lt;id|all&gt; — restart, clears quarantine\n"
                "/stop &lt;id&gt; · /startinst &lt;id&gt;\n"
                "/alerts on|off|status\n"
                "/whoami"
            )

        @r.message(Command("whoami"))
        async def cmd_whoami(msg: Message) -> None:
            uid = msg.from_user.id if msg.from_user else "?"
            await msg.answer(f"uid <code>{uid}</code> · chat <code>{msg.chat.id}</code>")

        @r.message(Command("status"))
        async def cmd_status(msg: Message) -> None:
            for part in chunks(self._render_status()):
                await msg.answer(part, reply_markup=self._status_keyboard())

        @r.message(Command("metrics"))
        async def cmd_metrics(msg: Message) -> None:
            m = self.orch.metrics()
            body = "\n".join(f"<code>{esc(k):<20}{esc(v)}</code>"
                             for k, v in m.as_dict().items() if k != "breaches")
            breaches = ("\n\n<b>Breaches</b>\n" + "\n".join(f"• {esc(b)}" for b in m.breaches)
                        ) if m.breaches else "\n\n✅ all thresholds within limits"
            await msg.answer(f"<b>Fleet metrics</b>\n{body}{breaches}")

        @r.message(Command("instance"))
        async def cmd_instance(msg: Message, command: CommandObject) -> None:
            iid = (command.args or "").strip()
            if not iid:
                await msg.answer("Usage: <code>/instance &lt;id&gt;</code>\nKnown: "
                                 + ", ".join(f"<code>{esc(i)}</code>" for i in self.orch.ids()))
                return
            await msg.answer(self._render_instance(iid),
                             reply_markup=self._instance_keyboard(iid))

        @r.message(Command("log"))
        async def cmd_log(msg: Message, command: CommandObject) -> None:
            parts = (command.args or "").split()
            if not parts:
                await msg.answer("Usage: <code>/log &lt;id&gt; [lines]</code>")
                return
            iid = parts[0]
            lines = 25
            if len(parts) > 1:
                try:
                    lines = max(1, min(200, int(parts[1])))
                except ValueError:
                    await msg.answer("Line count must be an integer.")
                    return
            sup = self.orch.get(iid)
            if sup is None:
                await msg.answer(f"❓ Unknown instance <code>{esc(iid)}</code>")
                return
            tail = await asyncio.to_thread(sup.tail_log, lines)
            for part in chunks(f"<b>{esc(iid)}</b> — last {lines} lines\n"
                               f"<pre>{esc(tail)}</pre>"):
                await msg.answer(part)

        @r.message(Command("events"))
        async def cmd_events(msg: Message, command: CommandObject) -> None:
            try:
                n = max(1, min(50, int((command.args or "15").strip())))
            except ValueError:
                n = 15
            history = self.orch.bus.history(limit=n)
            if not history:
                await msg.answer("No events recorded yet.")
                return
            lines = []
            for e in history:
                stamp = time.strftime("%H:%M:%S", time.localtime(e.ts))
                who = f" <code>{esc(e.instance_id)}</code>" if e.instance_id else ""
                lines.append(f"<code>{stamp}</code> {e.severity.emoji}{who} {esc(e.message)}")
            for part in chunks("\n".join(lines)):
                await msg.answer(part)

        @r.message(Command("alerts"))
        async def cmd_alerts(msg: Message, command: CommandObject) -> None:
            arg = (command.args or "status").strip().lower()
            if arg == "on":
                self.alerts_enabled = True
                await msg.answer("🔔 Alerts <b>enabled</b>.")
            elif arg == "off":
                self.alerts_enabled = False
                await msg.answer("🔕 Alerts <b>disabled</b>.")
            else:
                state = "enabled" if self.alerts_enabled else "disabled"
                await msg.answer(f"Alerts are <b>{state}</b> "
                                 f"(min severity <code>{esc(self.cfg.alert_min_severity)}</code>, "
                                 f"{len(self.cfg.alert_chat_ids)} target chats).")

        @r.message(Command("pause"))
        async def cmd_pause(msg: Message, command: CommandObject) -> None:
            iid = (command.args or "").strip()
            if iid:
                ok = await self.orch.pause_instance(iid)
                await msg.answer(f"⏸ Paused <code>{esc(iid)}</code>" if ok
                                 else f"❓ Unknown instance <code>{esc(iid)}</code>")
                return
            await self._request_confirm(msg, "pause_all", None,
                                        "Pause the <b>entire fleet</b>?")

        @r.message(Command("resume"))
        async def cmd_resume(msg: Message, command: CommandObject) -> None:
            iid = (command.args or "").strip()
            if iid:
                ok = await self.orch.resume_instance(iid)
                await msg.answer(f"▶️ Resumed <code>{esc(iid)}</code>" if ok
                                 else f"❓ Unknown instance <code>{esc(iid)}</code>")
                return
            await self.orch.resume_all()
            await msg.answer("▶️ <b>Fleet resumed.</b>\n" + self._fleet_line())

        @r.message(Command("restart"))
        async def cmd_restart(msg: Message, command: CommandObject) -> None:
            arg = (command.args or "").strip()
            if not arg:
                await msg.answer("Usage: <code>/restart &lt;id|all&gt;</code>")
                return
            if arg == "all":
                await self._request_confirm(msg, "restart_all", None,
                                            "Restart <b>every</b> instance?")
                return
            if self.orch.get(arg) is None:
                await msg.answer(f"❓ Unknown instance <code>{esc(arg)}</code>")
                return
            await self.orch.restart(arg, reason=f"telegram:{msg.from_user.id if msg.from_user else '?'}")
            await msg.answer(f"🔄 Restart issued for <code>{esc(arg)}</code>")

        @r.message(Command("stop"))
        async def cmd_stop(msg: Message, command: CommandObject) -> None:
            iid = (command.args or "").strip()
            if not iid:
                await msg.answer("Usage: <code>/stop &lt;id&gt;</code>")
                return
            if self.orch.get(iid) is None:
                await msg.answer(f"❓ Unknown instance <code>{esc(iid)}</code>")
                return
            await self._request_confirm(msg, "stop", iid,
                                        f"Stop supervision of <code>{esc(iid)}</code>?")

        @r.message(Command("startinst"))
        async def cmd_startinst(msg: Message, command: CommandObject) -> None:
            iid = (command.args or "").strip()
            ok = await self.orch.start_instance(iid) if iid else False
            await msg.answer(f"🔵 Started <code>{esc(iid)}</code>" if ok
                             else "Usage: <code>/startinst &lt;id&gt;</code>")

        # -- callbacks ---------------------------------------------------

        @r.callback_query(F.data.startswith("confirm:"))
        async def on_confirm(cb: CallbackQuery) -> None:
            if cb.from_user is None or cb.data is None:
                await cb.answer()
                return
            nonce = cb.data.split(":", 1)[1]
            pending = self.confirms.take(nonce, cb.from_user.id)
            if pending is None:
                await cb.answer("Expired or not yours.", show_alert=True)
                return
            await cb.answer("Executing…")
            text = await self._execute(pending.action, pending.arg, cb.from_user.id)
            with contextlib.suppress(TelegramAPIError):
                await cb.message.edit_text(text)

        @r.callback_query(F.data == "cancel")
        async def on_cancel(cb: CallbackQuery) -> None:
            await cb.answer("Cancelled.")
            with contextlib.suppress(TelegramAPIError):
                await cb.message.edit_text("❎ Cancelled.")

        @r.callback_query(F.data.startswith("refresh:"))
        async def on_refresh(cb: CallbackQuery) -> None:
            target = (cb.data or "refresh:fleet").split(":", 1)[1]
            text = self._render_status() if target == "fleet" else self._render_instance(target)
            markup = (self._status_keyboard() if target == "fleet"
                      else self._instance_keyboard(target))
            await cb.answer("Refreshed")
            with contextlib.suppress(TelegramAPIError):
                await cb.message.edit_text(text[:TG_MAX], reply_markup=markup)

        @r.callback_query(F.data.startswith("act:"))
        async def on_action(cb: CallbackQuery) -> None:
            if cb.from_user is None or cb.data is None:
                await cb.answer()
                return
            _, action, arg = cb.data.split(":", 2)
            if self.cfg.destructive_commands_require_confirm and action in {"stop", "restart"}:
                nonce = self.confirms.issue(action, arg, cb.from_user.id)
                await cb.answer()
                with contextlib.suppress(TelegramAPIError):
                    await cb.message.answer(
                        f"Confirm <b>{esc(action)}</b> on <code>{esc(arg)}</code>?",
                        reply_markup=self._confirm_keyboard(nonce))
                return
            await cb.answer("Executing…")
            text = await self._execute(action, arg, cb.from_user.id)
            with contextlib.suppress(TelegramAPIError):
                await cb.message.answer(text)

    # ------------------------------------------------------------ actions

    async def _request_confirm(self, msg: Message, action: str, arg: Optional[str],
                               prompt: str) -> None:
        if not self.cfg.destructive_commands_require_confirm:
            uid = msg.from_user.id if msg.from_user else 0
            await msg.answer(await self._execute(action, arg, uid))
            return
        uid = msg.from_user.id if msg.from_user else 0
        nonce = self.confirms.issue(action, arg, uid)
        await msg.answer(f"{prompt}\n<i>Expires in {int(CONFIRM_TTL_S)}s.</i>",
                         reply_markup=self._confirm_keyboard(nonce))

    async def _execute(self, action: str, arg: Optional[str], uid: int) -> str:
        reason = f"telegram:{uid}"
        try:
            if action == "pause_all":
                result = await self.orch.pause_all()
                n = sum(1 for v in result.values() if v)
                return (f"⏸ <b>Fleet paused.</b> {n}/{len(result)} processes suspended.\n"
                        + self._fleet_line())
            if action == "resume_all":
                await self.orch.resume_all()
                return "▶️ <b>Fleet resumed.</b>\n" + self._fleet_line()
            if action == "restart_all":
                n = await self.orch.restart_all(reason)
                return f"🔄 Restart issued for <b>{n}</b> instances."
            if action == "restart" and arg:
                ok = await self.orch.restart(arg, reason)
                return (f"🔄 Restart issued for <code>{esc(arg)}</code>" if ok
                        else f"❓ Unknown instance <code>{esc(arg)}</code>")
            if action == "stop" and arg:
                ok = await self.orch.stop_instance(arg)
                return (f"🔽 Stopped <code>{esc(arg)}</code>" if ok
                        else f"❓ Unknown instance <code>{esc(arg)}</code>")
            if action == "pause" and arg:
                ok = await self.orch.pause_instance(arg)
                return (f"⏸ Paused <code>{esc(arg)}</code>" if ok
                        else f"❓ Unknown instance <code>{esc(arg)}</code>")
            if action == "resume" and arg:
                ok = await self.orch.resume_instance(arg)
                return (f"▶️ Resumed <code>{esc(arg)}</code>" if ok
                        else f"❓ Unknown instance <code>{esc(arg)}</code>")
            return f"Unsupported action <code>{esc(action)}</code>"
        except Exception as exc:
            log.exception("action %s(%s) failed", action, arg)
            return f"❗ Action failed: <code>{esc(repr(exc))}</code>"

    # ------------------------------------------------------------ keyboards

    @staticmethod
    def _confirm_keyboard(nonce: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Confirm", callback_data=f"confirm:{nonce}"),
            InlineKeyboardButton(text="❎ Cancel", callback_data="cancel"),
        ]])

    def _status_keyboard(self) -> InlineKeyboardMarkup:
        toggle = ("▶️ Resume fleet", "act:resume_all:-") if self.orch.paused else \
                 ("⏸ Pause fleet", "act:pause_all:-")
        return InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔄 Refresh", callback_data="refresh:fleet"),
            InlineKeyboardButton(text=toggle[0], callback_data=toggle[1]),
        ]])

    @staticmethod
    def _instance_keyboard(iid: str) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="🔄 Refresh", callback_data=f"refresh:{iid}"),
            InlineKeyboardButton(text="♻️ Restart", callback_data=f"act:restart:{iid}"),
            InlineKeyboardButton(text="🔽 Stop", callback_data=f"act:stop:{iid}"),
        ]])
