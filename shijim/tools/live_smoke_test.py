"""Live smoke test entrypoint for Shijim."""

from __future__ import annotations

import logging
import os
import threading
import time
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

logger = logging.getLogger(__name__)


class CountingRawWriter(RawWriter):
    """RawWriter variant that keeps counts for smoke-test summaries."""

    def __post_init__(self) -> None:  # pragma: no cover - trivial
        super().__post_init__()
        self.tick_count = 0
        self.book_count = 0
        self.seen_symbols: set[str] = set()

    def write_batch(self, ticks, books):  # type: ignore[override]
        self.tick_count += len(ticks)
        self.book_count += len(books)
        for event in list(ticks) + list(books):
            self.seen_symbols.add(event.symbol)
        super().write_batch(ticks, books)


def _require_credentials() -> bool:
    missing = [name for name in ("SHIOAJI_API_KEY", "SHIOAJI_SECRET_KEY") if not os.getenv(name)]
    if missing:
        logger.error("Missing required Shioaji credentials: %s", ", ".join(missing))
        return False
    return True


def run_smoke_test(duration_seconds: int | None = None) -> int:
    duration = duration_seconds or int(os.getenv("SHIJIM_SMOKE_DURATION_SECONDS", "60"))
    logger.info("Starting live smoke test for %s seconds.", duration)

    if not _require_credentials():
        return 1

    exit_code = 0
    session = ShioajiSession()
    bus = InMemoryEventBus()
    raw_dir = Path(os.getenv("SHIJIM_SMOKE_RAW_DIR", "raw_smoke"))
    raw_writer = CountingRawWriter(root=raw_dir)
    clickhouse_dsn = os.getenv("SHIJIM_CLICKHOUSE_DSN", "clickhouse://localhost")
    click_writer = ClickHouseWriter(dsn=clickhouse_dsn, client=None)
    worker = IngestionWorker(bus=bus, raw_writer=raw_writer, analytical_writer=click_writer)
    worker_thread = threading.Thread(target=worker.run_forever, daemon=True)
    worker_started = False

    manager: SubscriptionManager | None = None

    try:
        api = session.login()
        context = CollectorContext(
            bus=bus,
            fut_tick_normalizer=normalize_tick_futures,
            fut_book_normalizer=normalize_book_futures,
            stk_tick_normalizer=normalize_tick_stock,
            stk_book_normalizer=normalize_book_stock,
        )
        attach_quote_callbacks(api, context)

        universe = get_smoke_test_universe()
        logger.info(
            "Smoke test subscribing to %s futures + %s stocks.",
            len(universe.futures),
            len(universe.stocks),
        )
        manager = SubscriptionManager(
            session=session,
            plan=SubscriptionPlan(futures=universe.futures, stocks=universe.stocks),
        )
        manager.subscribe_universe()

        worker_thread.start()
        worker_started = True

        deadline = time.time() + duration  # pragma: no cover - smoke only
        while time.time() < deadline:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Smoke test interrupted by user.")
        exit_code = 1
    except Exception:
        logger.exception("Smoke test failed.")
        exit_code = 1
    finally:
        if manager is not None:
            manager.unsubscribe_all()
        worker.stop()
        if worker_started:
            worker_thread.join(timeout=5)
        raw_writer.close_all()
        session.logout()

    logger.info(
        "Smoke test complete: ticks=%s books=%s symbols=%s",
        raw_writer.tick_count,
        raw_writer.book_count,
        sorted(raw_writer.seen_symbols),
    )
    return exit_code


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return run_smoke_test()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
