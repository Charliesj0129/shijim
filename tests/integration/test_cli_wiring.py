from __future__ import annotations

import importlib.util
import threading
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

from shijim.bus import InMemoryEventBus
from shijim.events import MDBookEvent, MDTickEvent
from shijim.recorder.clickhouse_writer import ClickHouseWriter
from shijim.recorder.ingestion import IngestionWorker
from shijim.recorder.raw_writer import RawWriter

CLICKHOUSE_AVAILABLE = importlib.util.find_spec("clickhouse_driver") is not None


class SpyRawWriter(RawWriter):
    def __init__(self) -> None:
        self._tmp = TemporaryDirectory()
        super().__init__(root=Path(self._tmp.name))
        self.written: list[tuple[list[MDTickEvent], list[MDBookEvent]]] = []

    def write_batch(self, ticks, books):
        self.written.append((list(ticks), list(books)))
        super().write_batch(ticks, books)

    def close_all(self) -> None:
        super().close_all()
        self._tmp.cleanup()


class SpyCHWriter(ClickHouseWriter):
    def __init__(self) -> None:
        super().__init__(dsn="ch://test", client="mock")
        self.batches: list[tuple[list[MDTickEvent], list[MDBookEvent]]] = []

    def write_batch(self, ticks, books):
        self.batches.append((list(ticks), list(books)))
        super().write_batch(ticks, books)


@pytest.mark.skipif(
    not CLICKHOUSE_AVAILABLE,
    reason="ClickHouse integration tests require clickhouse-driver",
)
def test_cli_wiring_end_to_end():
    bus = InMemoryEventBus()
    raw_writer = SpyRawWriter()
    ch_writer = SpyCHWriter()

    worker = IngestionWorker(
        bus=bus,
        raw_writer=raw_writer,
        analytical_writer=ch_writer,
        flush_interval=0.1,
        max_buffer_events=2,
    )

    thread = threading.Thread(target=worker.run_forever, daemon=True)
    thread.start()

    events = [
        MDTickEvent(ts_ns=1, symbol="TXF", asset_type="futures", exchange="TAIFEX"),
        MDBookEvent(ts_ns=2, symbol="TXF", asset_type="futures", exchange="TAIFEX"),
        MDTickEvent(ts_ns=3, symbol="2330", asset_type="stock", exchange="TWSE"),
    ]
    for event in events:
        bus.publish(event)

    time.sleep(0.5)
    worker.stop()
    bus.publish(MDTickEvent(ts_ns=999, symbol="STOP", asset_type="futures", exchange="TAIFEX"))
    thread.join(timeout=1)

    raw_writer.close_all()

    assert any(batch[0] for batch in raw_writer.written)
    flat_ticks = [tick for batch in raw_writer.written for tick in batch[0]]
    flat_books = [book for batch in raw_writer.written for book in batch[1]]
    assert len(flat_ticks) == 2
    assert len(flat_books) == 1
