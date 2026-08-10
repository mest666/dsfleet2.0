"""Bounded, drop-oldest fan-out event bus. Publishers never block or raise."""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import AsyncIterator, Deque, Mapping, Optional, Any

from .models import Event, Severity

log = logging.getLogger("dsfleet.events")

__all__ = ["EventBus", "Subscription"]


class Subscription:
    """A single consumer's view of the bus. Use as an async iterator."""

    __slots__ = ("_queue", "_bus", "_closed", "dropped", "name")

    def __init__(self, bus: "EventBus", maxsize: int, name: str) -> None:
        self._bus = bus
        self._queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=maxsize)
        self._closed = False
        self.dropped = 0
        self.name = name

    def _offer(self, event: Event) -> None:
        if self._closed:
            return
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            try:                       # drop-oldest keeps the freshest signal
                self._queue.get_nowait()
                self._queue.task_done()
            except asyncio.QueueEmpty:  # pragma: no cover - race, harmless
                pass
            self.dropped += 1
            try:
                self._queue.put_nowait(event)
            except asyncio.QueueFull:  # pragma: no cover
                pass

    async def get(self) -> Event:
        return await self._queue.get()

    def get_nowait(self) -> Optional[Event]:
        try:
            return self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return None

    def qsize(self) -> int:
        return self._queue.qsize()

    def close(self) -> None:
        self._closed = True
        self._bus._unsubscribe(self)

    async def __aenter__(self) -> "Subscription":
        return self

    async def __aexit__(self, *exc: object) -> None:
        self.close()

    def __aiter__(self) -> AsyncIterator[Event]:
        return self._iter()

    async def _iter(self) -> AsyncIterator[Event]:
        while not self._closed:
            yield await self._queue.get()


class EventBus:
    """Synchronous publish, asynchronous consume. Keeps a rolling history buffer."""

    def __init__(self, history: int = 500, queue_size: int = 512) -> None:
        self._subs: list[Subscription] = []
        self._history: Deque[Event] = deque(maxlen=history)
        self._queue_size = queue_size

    # -- producer side -----------------------------------------------------
    def publish(self, event: Event) -> None:
        self._history.append(event)
        for sub in tuple(self._subs):
            try:
                sub._offer(event)
            except Exception:  # a broken consumer must never break a producer
                log.exception("event delivery failed for subscriber %s", sub.name)

    def emit(
        self,
        kind: str,
        message: str,
        *,
        severity: Severity = Severity.INFO,
        instance_id: Optional[str] = None,
        data: Optional[Mapping[str, Any]] = None,
    ) -> None:
        self.publish(Event(
            kind=kind, instance_id=instance_id, severity=severity,
            message=message, data=dict(data or {}),
        ))

    # -- consumer side -----------------------------------------------------
    def subscribe(self, name: str = "anon", maxsize: Optional[int] = None) -> Subscription:
        sub = Subscription(self, maxsize or self._queue_size, name)
        self._subs.append(sub)
        return sub

    def _unsubscribe(self, sub: Subscription) -> None:
        try:
            self._subs.remove(sub)
        except ValueError:
            pass

    def history(self, limit: int = 50, min_severity: Severity = Severity.DEBUG) -> list[Event]:
        rank = min_severity.rank
        out = [e for e in self._history if e.severity.rank >= rank]
        return out[-limit:]

    @property
    def subscriber_count(self) -> int:
        return len(self._subs)
