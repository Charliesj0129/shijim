from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from pathlib import Path
from types import MethodType
from typing import Iterable, List

from shijim.bus.event_bus import EventBus
from shijim.events.schema import MDBookEvent, MDTickEvent
from shijim.recorder.clickhouse_writer import ClickHouseWriter
from shijim.recorder.ingestion import IngestionWorker
from shijim.recorder.raw_writer import RawWriter


class FakeBus(EventBus):
    def __init__(self, events: Iterable[object]) -> None:
        self._events = list(events)

    def publish(self, event):
        self._events.append(event)

    def subscribe(self, event_type: str | None = None, timeout: float | None = None):
        for event in list(self._events):
            yield event


class HeartbeatBus(EventBus):
    """EventBus that never receives events but emits heartbeat None values."""

    def publish(self, event):
        return None

    def subscribe(self, event_type: str | None = None, timeout: float | None = None):
        interval = timeout if timeout is not None else 0.01
        while True:
            time.sleep(interval)
            yield None


@dataclass
class SpyWriter:
    batches: List[tuple[list[MDTickEvent], list[MDBookEvent]]] = None

    def __post_init__(self) -> None:
        if self.batches is None:
            self.batches = []

    def write_batch(self, ticks, books):
        self.batches.append((list(ticks), list(books)))

    def flush(self, force: bool = True):  # noqa: D401 - simple spy
        return None


def _tick(ts: int) -> MDTickEvent:
    return MDTickEvent(
        ts_ns=ts,
        symbol="TXF",
        asset_type="futures",
        exchange="TAIFEX",
    )


def _book(ts: int) -> MDBookEvent:
    return MDBookEvent(
        ts_ns=ts,
        symbol="TXF",
        asset_type="futures",
        exchange="TAIFEX",
    )


def test_ingestion_worker_drains_bus_and_writes_batches():
    events = [_tick(1), _book(2), _tick(3)]
    bus = FakeBus(events)
    raw_writer = SpyWriter()
    ch_writer = SpyWriter()

    worker = IngestionWorker(
        bus=bus,
        raw_writer=raw_writer,
        analytical_writer=ch_writer,
        max_buffer_events=2,
        flush_interval=0.0,
    )
    worker.run_forever()

    assert raw_writer.batches
    assert raw_writer.batches == ch_writer.batches
    flat_ticks = [tick for batch in raw_writer.batches for tick in batch[0]]
    flat_books = [book for batch in raw_writer.batches for book in batch[1]]
    assert len(flat_ticks) == 2
    assert len(flat_books) == 1


def test_ingestion_worker_flushes_during_quiet_periods():
    bus = HeartbeatBus()
    raw_writer = SpyWriter()
    ch_writer = SpyWriter()
    worker = IngestionWorker(
        bus=bus,
        raw_writer=raw_writer,
        analytical_writer=ch_writer,
        max_buffer_events=5,
        flush_interval=0.05,
    )

    flush_calls: list[float] = []
    original_flush = worker.flush

    def instrumented_flush(self):
        flush_calls.append(self.clock())
        return original_flush()

    worker.flush = MethodType(instrumented_flush, worker)

    thread = threading.Thread(target=worker.run_forever, daemon=True)
    thread.start()
    time.sleep(0.2)
    worker.stop()
    thread.join(timeout=1)

    assert not thread.is_alive()
    # Expect at least one periodic flush plus the final flush in finally.
    assert len(flush_calls) >= 2


def test_ingestion_worker_stop_triggers_final_flush():
    bus = HeartbeatBus()
    raw_writer = SpyWriter()
    ch_writer = SpyWriter()
    worker = IngestionWorker(
        bus=bus,
        raw_writer=raw_writer,
        analytical_writer=ch_writer,
        max_buffer_events=5,
        flush_interval=10.0,
    )

    final_flush_called = threading.Event()
    original_flush = worker.flush

    def instrumented_flush(self):
        result = original_flush()
        if self._stop_event.is_set():
            final_flush_called.set()
        return result

    worker.flush = MethodType(instrumented_flush, worker)

    thread = threading.Thread(target=worker.run_forever, daemon=True)
    thread.start()
    time.sleep(0.1)
    worker.stop()
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert final_flush_called.wait(timeout=1)


def test_ingestion_flush_returns_quickly_with_async_writers(tmp_path: Path):
    class SlowClient:
        def __init__(self) -> None:
            self.calls = 0

        def execute(self, sql, rows):  # noqa: D401
            self.calls += 1
            time.sleep(0.1)

    raw_writer = RawWriter(root=tmp_path / "raw")
    ch_writer = ClickHouseWriter(dsn="ch://test", client=SlowClient(), flush_threshold=1)
    worker = IngestionWorker(
        bus=FakeBus([]),
        raw_writer=raw_writer,
        analytical_writer=ch_writer,
        max_buffer_events=1,
        flush_interval=0.0,
    )

    worker._handle_event(_tick(1))
    start = time.perf_counter()
    worker.flush()
    duration = time.perf_counter() - start

    assert duration < 0.05

    raw_writer.drain_async()
    ch_writer.drain_async()
    ch_writer.close()
    raw_writer.close_all()
