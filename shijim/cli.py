"""Command-line entrypoint for the Shijim stack."""

from __future__ import annotations

import argparse
import logging

from shijim.bus import EventPublisher
from shijim.gateway import CallbackAdapter, ShioajiSession
from shijim.recorder import IngestionWorker, RawWriter

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

    session = ShioajiSession()
    api = session.connect()

    publisher = EventPublisher()
    callback_adapter = CallbackAdapter(api=api, event_publisher=publisher)
    callback_adapter.attach()

    subscriber = publisher.queue
    worker = IngestionWorker(subscriber=subscriber, raw_writer=RawWriter())

    logger.info("Shijim bootstrap complete; press Ctrl+C to exit.")
    try:
        worker.run_forever()
    except KeyboardInterrupt:
        logger.info("Shutdown requested by user.")
    finally:
        session.disconnect()

    return 0
