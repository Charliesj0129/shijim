"""Command-line entrypoint for the Shijim stack."""

from __future__ import annotations

import argparse
import logging

from pathlib import Path

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
    get_smoke_test_universe,
)
from shijim.recorder import ClickHouseWriter, IngestionWorker, RawWriter

logger = logging.getLogger("shijim.cli")


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


def main(argv: list[str] | None = None) -> int:
    """Minimal bootstrap for local smoke tests."""
    parser = build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger.info("Starting Shijim CLI (simulation=%s).", args.simulation)

    exit_code = 0
    session = ShioajiSession(mode="simulation" if args.simulation else "live")
    manager: SubscriptionManager | None = None
    worker: IngestionWorker | None = None
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

            universe = get_smoke_test_universe()
            manager = SubscriptionManager(
                session=session,
                plan=SubscriptionPlan(futures=universe.futures, stocks=universe.stocks),
            )
            manager.subscribe_universe()

            worker = IngestionWorker(
                bus=bus,
                raw_writer=RawWriter(root=Path("raw")),
                analytical_writer=ClickHouseWriter(dsn="clickhouse://localhost"),
            )

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
        if manager is not None:
            manager.unsubscribe_all()
        if worker is not None:
            worker.stop()
        session.logout()

    return exit_code
