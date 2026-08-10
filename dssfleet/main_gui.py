#!/usr/bin/env python3
"""dsfleet GUI entrypoint.

    python main_gui.py --config config.10x.json

Dear PyGui requires the main thread for its render loop (a hard requirement on macOS,
and the path of least resistance everywhere else), so the asyncio orchestrator runs on
a worker thread. Shutdown is bidirectional: closing the window stops the loop, and a
fatal orchestrator error closes the window.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import threading
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parent))

from core.config import AppConfig, ConfigError, load_config  # noqa: E402
from core.orchestrator import Orchestrator                   # noqa: E402
from main import setup_logging                               # noqa: E402

log = logging.getLogger("dsfleet.gui")


class OrchestratorThread(threading.Thread):
    """Owns an event loop and the Orchestrator running inside it."""

    def __init__(self, cfg: AppConfig) -> None:
        super().__init__(name="orchestrator", daemon=True)
        self.cfg = cfg
        self.loop = asyncio.new_event_loop()
        self.orch: Optional[Orchestrator] = None
        self.ready = threading.Event()
        self.error: Optional[BaseException] = None
        self._stop = asyncio.Event()

    def run(self) -> None:
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_until_complete(self._main())
        except Exception as exc:
            self.error = exc
            log.exception("orchestrator thread crashed")
            self.ready.set()
        finally:
            try:
                pending = asyncio.all_tasks(self.loop)
                for task in pending:
                    task.cancel()
                if pending:
                    self.loop.run_until_complete(
                        asyncio.gather(*pending, return_exceptions=True))
            except Exception:
                log.debug("task drain failed", exc_info=True)
            self.loop.close()
            log.info("orchestrator loop closed")

    async def _main(self) -> None:
        self._stop = asyncio.Event()
        self.orch = Orchestrator(self.cfg)
        try:
            await self.orch.start()
        except Exception:
            self.ready.set()
            raise
        self.ready.set()
        await self._stop.wait()
        await self.orch.stop()

    def shutdown(self) -> None:
        if self.loop.is_closed():
            return
        self.loop.call_soon_threadsafe(self._stop.set)


def main() -> int:
    ap = argparse.ArgumentParser(prog="dsfleet-gui")
    ap.add_argument("--config", "-c", default="config.json")
    args = ap.parse_args()

    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2

    setup_logging(cfg)

    try:
        import dearpygui.dearpygui  # noqa: F401
    except ImportError:
        print("Dear PyGui is not installed. Run: pip install dearpygui",
              file=sys.stderr)
        return 3

    from gui.dashboard import Dashboard

    worker = OrchestratorThread(cfg)
    worker.start()

    if not worker.ready.wait(timeout=120.0):
        print("orchestrator failed to become ready within 120s", file=sys.stderr)
        worker.shutdown()
        return 1

    if worker.error is not None or worker.orch is None:
        print(f"orchestrator startup failed: {worker.error}", file=sys.stderr)
        return 1

    dashboard = Dashboard(worker.orch, worker.loop, on_shutdown=worker.shutdown)
    try:
        dashboard.run()
    finally:
        worker.shutdown()
        worker.join(timeout=90.0)
        if worker.is_alive():
            log.warning("orchestrator thread did not exit cleanly")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
