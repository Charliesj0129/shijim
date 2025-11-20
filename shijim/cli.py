"""Command-line entrypoint for the Shijim stack."""

from __future__ import annotations

import argparse
import logging
import os
import threading
import time
from datetime import datetime, time as dt_time, timedelta, timezone
from pathlib import Path
from typing import Callable

try:  # Python 3.9+
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover - fallback when tzdata is missing
    ZoneInfo = None  # type: ignore

from shijim.bus import InMemoryEventBus
from shijim.events.normalizers import (
    normalize_book_futures,
    normalize_book_stock,
    normalize_tick_futures,
    normalize_tick_stock,
)
from shijim.gateway import (
    CollectorContext,
    ShioajiSession,
    SubscriptionManager,
    SubscriptionPlan,
    attach_quote_callbacks,
    get_top_volume_universe,
    shard_config_from_env,
    shard_universe,
)
from shijim.recorder import ClickHouseWriter, IngestionWorker, RawWriter

logger = logging.getLogger("shijim.cli")
if ZoneInfo:
    try:
        TAIWAN_TZ = ZoneInfo("Asia/Taipei")
    except Exception:  # pragma: no cover - fallback if tzdata is missing
        TAIWAN_TZ = timezone(timedelta(hours=8))
else:  # pragma: no cover - fallback on very old Python
    TAIWAN_TZ = timezone(timedelta(hours=8))

MARKET_OPEN_TIME = dt_time(hour=8, minute=30)
MARKET_CLOSE_TIME = dt_time(hour=13, minute=45)


def _taipei_now() -> datetime:
    return datetime.now(tz=TAIWAN_TZ)


def _ensure_trading_window(
    now_fn: Callable[[], datetime] = _taipei_now,
    sleep_func: Callable[[float], None] = time.sleep,
) -> bool:
    """Sleep until market open; abort if past the close."""
    now = now_fn()
    open_dt = datetime.combine(now.date(), MARKET_OPEN_TIME, tzinfo=TAIWAN_TZ)
    close_dt = datetime.combine(now.date(), MARKET_CLOSE_TIME, tzinfo=TAIWAN_TZ)

    if now >= close_dt:
        logger.info("Local time %s is past market close (%s); exiting.", now, close_dt)
        return False

    if now < open_dt:
        wait_seconds = max((open_dt - now).total_seconds(), 0)
        logger.info(
            "Local time %s is before market open (%s); sleeping %.0f seconds.",
            now,
            open_dt,
            wait_seconds,
        )
        if wait_seconds > 0:
            sleep_func(wait_seconds)
    return True


def _schedule_market_close(
    worker: IngestionWorker,
    now_fn: Callable[[], datetime] = _taipei_now,
) -> threading.Timer | None:
    """Schedule a stop() call for the worker when the market closes."""
    now = now_fn()
    close_dt = datetime.combine(now.date(), MARKET_CLOSE_TIME, tzinfo=TAIWAN_TZ)
    delay = (close_dt - now).total_seconds()
    if delay <= 0:
        logger.info("Market already closed; stopping worker immediately.")
        worker.stop()
        return None
    timer = threading.Timer(delay, worker.stop)
    timer.daemon = True
    timer.start()
    logger.info("Scheduled worker.stop() at %s (in %.0f seconds).", close_dt, delay)
    return timer


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="shijim",
        description="Shioaji-based market data ingestion + recorder sandbox.",
    )
    parser.add_argument(
        "--simulation",
        action="store_true",
        help="Force simulation mode regardless of environment variables.",
    )
    return parser


def _raw_root() -> Path:
    return Path(os.getenv("SHIJIM_RAW_DIR", "raw"))


def _clickhouse_writer() -> ClickHouseWriter:
    dsn = os.getenv("CLICKHOUSE_DSN", "clickhouse://localhost")
    fallback_dir = os.getenv("SHIJIM_FALLBACK_DIR")
    return ClickHouseWriter(dsn=dsn, fallback_dir=fallback_dir)


def main(argv: list[str] | None = None) -> int:
    """Minimal bootstrap for local smoke tests."""
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger.info("Starting Shijim CLI (simulation=%s).", args.simulation)

    exit_code = 0
    if not _ensure_trading_window():
        return exit_code

    session = ShioajiSession(mode="simulation" if args.simulation else "live")
    manager: SubscriptionManager | None = None
    worker: IngestionWorker | None = None
    stop_timer: threading.Timer | None = None
    try:
        try:
            api = session.login()

            bus = InMemoryEventBus()
            context = CollectorContext(
                bus=bus,
                fut_tick_normalizer=normalize_tick_futures,
                fut_book_normalizer=normalize_book_futures,
                stk_tick_normalizer=normalize_tick_stock,
                stk_book_normalizer=normalize_book_stock,
            )
            attach_quote_callbacks(api, context)

            universe = get_top_volume_universe(api, limit=1000)
            shard = shard_config_from_env()
            sharded_universe = shard_universe(universe, shard)
            logger.info(
                "Worker shard %s/%s subscribing to %s futures + %s stocks (of %s/%s).",
                shard.shard_id,
                shard.total_shards,
                len(sharded_universe.futures),
                len(sharded_universe.stocks),
                len(universe.futures),
                len(universe.stocks),
            )
            manager = SubscriptionManager(
                session=session,
                plan=SubscriptionPlan(
                    futures=sharded_universe.futures,
                    stocks=sharded_universe.stocks,
                ),
            )
            manager.subscribe_universe()

            worker = IngestionWorker(
                bus=bus,
                raw_writer=RawWriter(root=_raw_root()),
                analytical_writer=_clickhouse_writer(),
            )
            stop_timer = _schedule_market_close(worker)

            logger.info("Shijim bootstrap complete; press Ctrl+C to exit.")
            try:
                worker.run_forever()
            except KeyboardInterrupt:
                logger.info("Shutdown requested by user.")
            except Exception:
                exit_code = 1
                logger.exception("Fatal error while running ingestion loop.")
        except Exception:
            exit_code = 1
            logger.exception("Failed to bootstrap Shijim CLI.")
    finally:
        if stop_timer is not None:
            stop_timer.cancel()
        if manager is not None:
            manager.unsubscribe_all()
        if worker is not None:
            worker.stop()
        session.logout()

    return exit_code
