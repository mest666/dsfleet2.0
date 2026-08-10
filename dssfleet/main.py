#!/usr/bin/env python3
"""dsfleet entrypoint.

    python main.py --config config.json
    python main.py --config config.json --dry-run     # validate + print layout, exit
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import logging.handlers
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from core.config import AppConfig, ConfigError, load_config   # noqa: E402
from core.orchestrator import Orchestrator                    # noqa: E402


def setup_logging(cfg: AppConfig) -> None:
    cfg.ensure_dirs()
    root = logging.getLogger()
    root.setLevel(getattr(logging, cfg.log_level, logging.INFO))
    for h in list(root.handlers):
        root.removeHandler(h)

    fmt = logging.Formatter(
        "%(asctime)s %(levelname)-7s %(name)-22s %(message)s", "%Y-%m-%d %H:%M:%S")

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    root.addHandler(console)

    try:
        fileh = logging.handlers.RotatingFileHandler(
            cfg.log_dir / "dsfleet.log", maxBytes=16 * 1024 * 1024,
            backupCount=5, encoding="utf-8")
        fileh.setFormatter(fmt)
        root.addHandler(fileh)
    except OSError as exc:
        root.warning("file logging disabled: %s", exc)

    logging.getLogger("aiogram").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def install_signal_handlers(loop: asyncio.AbstractEventLoop, stop: asyncio.Event) -> None:
    def _trigger() -> None:
        logging.getLogger("dsfleet").info("shutdown signal received")
        loop.call_soon_threadsafe(stop.set)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _trigger)
        except (NotImplementedError, AttributeError, ValueError):
            # Windows: fall back to the default handler.
            signal.signal(sig, lambda *_: _trigger())


async def run(cfg: AppConfig) -> int:
    log = logging.getLogger("dsfleet")
    orch = Orchestrator(cfg)
    controller = None

    if cfg.telegram.enabled:
        try:
            from telemetry.bot import TelemetryController
            controller = TelemetryController(cfg.telegram, orch)
        except ImportError as exc:
            log.error("telegram controller unavailable (%s); continuing headless", exc)
        except Exception:
            log.exception("telegram controller init failed; continuing headless")

    stop = asyncio.Event()
    install_signal_handlers(asyncio.get_running_loop(), stop)

    await orch.start()
    if controller is not None:
        try:
            await controller.start()
        except Exception:
            log.exception("telegram controller failed to start")
            controller = None

    log.info("dsfleet running — %d instances", len(orch.supervisors))
    await stop.wait()

    log.info("shutting down…")
    if controller is not None:
        with contextlib.suppress(Exception):
            await asyncio.wait_for(controller.stop(), timeout=20.0)
    with contextlib.suppress(Exception):
        await asyncio.wait_for(orch.stop(), timeout=120.0)
    log.info("shutdown complete")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(prog="dsfleet")
    ap.add_argument("--config", "-c", default="config.json")
    ap.add_argument("--dry-run", action="store_true",
                    help="validate config, print the resolved layout, then exit")
    args = ap.parse_args()

    try:
        cfg = load_config(args.config)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 2

    setup_logging(cfg)
    log = logging.getLogger("dsfleet")
    log.info("loaded %s — %d instances, runtime_dir=%s",
             cfg.source_path, len(cfg.instances), cfg.runtime_dir)

    if args.dry_run:
        from core.window import WindowManager
        wm = WindowManager(cfg.window_layout)
        cells = [(i.id, i.grid_cell) for i in cfg.instances if i.grid_cell]
        print(wm.describe(cells))
        for i in cfg.instances:
            print(f"  {i.id:<16} isolation={i.isolation.mode:<14} "
                  f"exec={i.executable}")
        return 0

    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    try:
        return asyncio.run(run(cfg))
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
