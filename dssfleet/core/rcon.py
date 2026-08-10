"""Source RCON (Valve) client and fleet controller.

Protocol
--------
Little-endian TCP framing:

    int32 size      (bytes after this field)
    int32 id        (caller-chosen; echoed back)
    int32 type      (3 = AUTH, 2 = AUTH_RESPONSE / EXECCOMMAND, 0 = RESPONSE_VALUE)
    body            null-terminated ASCII
    byte  0         (trailing empty string terminator)

Auth failure is signalled by an AUTH_RESPONSE whose id is -1. Long command output is
split across multiple RESPONSE_VALUE packets with no explicit terminator, so this
client uses the standard sentinel trick: after the real command it sends a second,
deliberately empty command; when the sentinel's response arrives, everything before it
belonged to the real command.

Requires `-usercon` on the server and both `rcon_password` and `net_start` set.
"""
from __future__ import annotations

import asyncio
import logging
import struct
from dataclasses import dataclass
from typing import Optional, Sequence

log = logging.getLogger("dsfleet.rcon")

__all__ = ["RconError", "RconAuthError", "RconClient", "RconController", "RconResult"]

SERVERDATA_AUTH = 3
SERVERDATA_AUTH_RESPONSE = 2
SERVERDATA_EXECCOMMAND = 2
SERVERDATA_RESPONSE_VALUE = 0

MAX_PACKET = 4096
HEADER = struct.Struct("<iii")


class RconError(RuntimeError):
    """Transport or protocol failure."""


class RconAuthError(RconError):
    """Password rejected by the server."""


@dataclass(slots=True)
class RconResult:
    instance_id: str
    command: str
    ok: bool
    body: str = ""
    error: Optional[str] = None


class RconClient:
    """One persistent connection to one server. Not safe for concurrent use —
    the internal lock serialises commands so callers don't have to."""

    def __init__(self, host: str, port: int, password: str, *,
                 timeout: float = 8.0, name: str = "") -> None:
        self.host = host
        self.port = port
        self._password = password
        self.timeout = timeout
        self.name = name or f"{host}:{port}"
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock = asyncio.Lock()
        self._next_id = 1
        self.authenticated = False

    # -- framing ----------------------------------------------------------

    def _alloc_id(self) -> int:
        self._next_id = (self._next_id % 0x7FFFFFFE) + 1
        return self._next_id

    @staticmethod
    def _encode(req_id: int, req_type: int, body: str) -> bytes:
        payload = body.encode("utf-8", errors="replace") + b"\x00\x00"
        return struct.pack("<i", 8 + len(payload)) + struct.pack("<ii", req_id, req_type) + payload

    async def _read_exact(self, n: int) -> bytes:
        assert self._reader is not None
        try:
            return await asyncio.wait_for(self._reader.readexactly(n), timeout=self.timeout)
        except asyncio.IncompleteReadError as exc:
            raise RconError(f"{self.name}: connection closed mid-packet") from exc
        except asyncio.TimeoutError as exc:
            raise RconError(f"{self.name}: read timed out after {self.timeout:.0f}s") from exc

    async def _read_packet(self) -> tuple[int, int, str]:
        raw_size = await self._read_exact(4)
        (size,) = struct.unpack("<i", raw_size)
        if size < 10 or size > MAX_PACKET * 4:
            raise RconError(f"{self.name}: implausible packet size {size}")
        payload = await self._read_exact(size)
        req_id, req_type = struct.unpack("<ii", payload[:8])
        body = payload[8:-2].decode("utf-8", errors="replace")
        return req_id, req_type, body

    # -- connection -------------------------------------------------------

    async def connect(self) -> None:
        await self.close()
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), timeout=self.timeout)
        except asyncio.TimeoutError as exc:
            raise RconError(f"{self.name}: connect timed out") from exc
        except OSError as exc:
            raise RconError(f"{self.name}: connect failed: {exc}") from exc

        auth_id = self._alloc_id()
        self._writer.write(self._encode(auth_id, SERVERDATA_AUTH, self._password))
        await self._writer.drain()

        # Servers may emit an empty RESPONSE_VALUE before AUTH_RESPONSE.
        for _ in range(3):
            resp_id, resp_type, _ = await self._read_packet()
            if resp_type != SERVERDATA_AUTH_RESPONSE:
                continue
            if resp_id == -1:
                await self.close()
                raise RconAuthError(f"{self.name}: rcon password rejected")
            if resp_id == auth_id:
                self.authenticated = True
                log.info("rcon authenticated to %s", self.name)
                return
        await self.close()
        raise RconError(f"{self.name}: no valid AUTH_RESPONSE received")

    async def close(self) -> None:
        self.authenticated = False
        writer, self._writer, self._reader = self._writer, None, None
        if writer is None:
            return
        try:
            writer.close()
            await asyncio.wait_for(writer.wait_closed(), timeout=3.0)
        except (OSError, asyncio.TimeoutError):
            pass

    # -- commands ---------------------------------------------------------

    async def execute(self, command: str) -> str:
        async with self._lock:
            if not self.authenticated:
                await self.connect()
            try:
                return await self._execute_locked(command)
            except RconError:
                # One transparent reconnect: srcds drops idle rcon sockets.
                log.debug("%s: retrying %r after transport error", self.name, command)
                await self.connect()
                return await self._execute_locked(command)

    async def _execute_locked(self, command: str) -> str:
        assert self._writer is not None
        cmd_id = self._alloc_id()
        sentinel_id = self._alloc_id()
        self._writer.write(self._encode(cmd_id, SERVERDATA_EXECCOMMAND, command))
        self._writer.write(self._encode(sentinel_id, SERVERDATA_RESPONSE_VALUE, ""))
        try:
            await self._writer.drain()
        except OSError as exc:
            raise RconError(f"{self.name}: write failed: {exc}") from exc

        parts: list[str] = []
        for _ in range(64):  # bound the multipart loop
            resp_id, _resp_type, body = await self._read_packet()
            if resp_id == sentinel_id:
                return "".join(parts)
            if resp_id == cmd_id:
                parts.append(body)
        raise RconError(f"{self.name}: multipart response exceeded 64 packets")

    async def __aenter__(self) -> "RconClient":
        await self.connect()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.close()


class RconController:
    """Fan-out command execution across the fleet.

    Clients are created lazily and cached; a server that is down simply yields a
    failed RconResult rather than raising, so one dead instance never aborts a
    fleet-wide command.
    """

    def __init__(self, default_password: str = "", host: str = "127.0.0.1",
                 timeout: float = 8.0) -> None:
        self.default_password = default_password
        self.host = host
        self.timeout = timeout
        self._clients: dict[str, RconClient] = {}

    def register(self, instance_id: str, port: int,
                 password: Optional[str] = None, host: Optional[str] = None) -> RconClient:
        client = RconClient(host or self.host, port,
                            password or self.default_password,
                            timeout=self.timeout, name=instance_id)
        self._clients[instance_id] = client
        return client

    def register_from_config(self, instances: Sequence) -> int:
        """Register every instance in an AppConfig that declares an rcon_port."""
        count = 0
        for inst in instances:
            port = getattr(inst, "rcon_port", None)
            if port is None:
                continue
            self.register(inst.id, port, getattr(inst, "rcon_password", None))
            count += 1
        log.info("rcon: registered %d instances", count)
        return count

    def get(self, instance_id: str) -> Optional[RconClient]:
        return self._clients.get(instance_id)

    @property
    def instance_ids(self) -> list[str]:
        return list(self._clients)

    async def execute(self, instance_id: str, command: str) -> RconResult:
        client = self._clients.get(instance_id)
        if client is None:
            return RconResult(instance_id, command, False,
                              error=f"no rcon client registered for {instance_id!r}")
        try:
            body = await client.execute(command)
            return RconResult(instance_id, command, True, body=body)
        except RconAuthError as exc:
            log.error("rcon auth failure on %s: %s", instance_id, exc)
            return RconResult(instance_id, command, False, error=str(exc))
        except RconError as exc:
            log.warning("rcon failure on %s: %s", instance_id, exc)
            return RconResult(instance_id, command, False, error=str(exc))
        except Exception as exc:
            log.exception("unexpected rcon error on %s", instance_id)
            return RconResult(instance_id, command, False, error=repr(exc))

    async def broadcast(self, command: str,
                        targets: Optional[Sequence[str]] = None) -> list[RconResult]:
        ids = list(targets) if targets is not None else self.instance_ids
        results = await asyncio.gather(*(self.execute(i, command) for i in ids),
                                       return_exceptions=True)
        out: list[RconResult] = []
        for iid, res in zip(ids, results):
            if isinstance(res, BaseException):
                out.append(RconResult(iid, command, False, error=repr(res)))
            else:
                out.append(res)
        return out

    async def sequence(self, instance_id: str, commands: Sequence[str],
                       delay_s: float = 0.0) -> list[RconResult]:
        """Run commands in order on one instance, stopping at the first failure."""
        out: list[RconResult] = []
        for cmd in commands:
            result = await self.execute(instance_id, cmd)
            out.append(result)
            if not result.ok:
                break
            if delay_s:
                await asyncio.sleep(delay_s)
        return out

    async def close(self) -> None:
        await asyncio.gather(*(c.close() for c in self._clients.values()),
                             return_exceptions=True)


# --------------------------------------------------------------------------- scenarios

class ScenarioCommands:
    """Canonical command sequences for bot-mode scenario control.

    These are ordinary server convars; the heavy lifting lives in the Lua bot scripts
    under scripts/vscripts/bots/, which the server loads from the addon.
    """

    @staticmethod
    def load_bot_match(game_mode: int = 23, difficulty: int = 2) -> list[str]:
        return [
            f"dota_bot_set_difficulty {difficulty}",
            "dota_bot_populate 1",
            f"dota_force_gamemode {game_mode}",
            "dota_start_ai_game 1",
            "map dota",
        ]

    @staticmethod
    def apply_script_reload() -> list[str]:
        """Hot-reload bot Lua without restarting the server."""
        return ["dota_bot_reload_scripts", "script_reload"]

    @staticmethod
    def instrument_netgraph() -> list[str]:
        return ["sv_netspike 100", "net_showtcpstats 1", "stats"]

    @staticmethod
    def status() -> list[str]:
        return ["status", "net_status"]
