from __future__ import annotations

from shijim.events.schema import MDBookEvent, MDTickEvent
from shijim.recorder.ingestion import IngestionWorker


class FakeBus:
    def __init__(self, events):
        self._events = list(events)

    def subscribe(self, event_type: str | None = None):
        if event_type is None:
            filtered = self._events
        else:
            filtered = [event for event in self._events if event.type == event_type]
        return iter(filtered)


class RecordingWriter:
    def __init__(self) -> None:
        self.batches: list[tuple[list[object], list[object]]] = []

    def write_batch(self, ticks, books):  # noqa: ANN001
        self.batches.append((list(ticks), list(books)))

    def flush(self, force: bool = True):  # pragma: no cover - simple spy
        return None


def _tick(symbol: str, ts: int) -> MDTickEvent:
    return MDTickEvent(
        ts=ts,
        symbol=symbol,
        asset_type="futures",
        exchange="TAIFEX",
        price=100.0,
        size=1,
    )


def _book(symbol: str, ts: int) -> MDBookEvent:
    return MDBookEvent(
        ts=ts,
        symbol=symbol,
        asset_type="stock",
        exchange="TWSE",
        bid_prices=[100.0],
        bid_volumes=[1],
        ask_prices=[100.5],
        ask_volumes=[1],
    )


def test_ingestion_worker_batches_and_flushes() -> None:
    events = [
        _tick("TXF", 1),
        _book("2330", 2),
        _tick("TXF", 3),
        _tick("TXF", 4),
    ]
    bus = FakeBus(events)
    raw_writer = RecordingWriter()
    analytics_writer = RecordingWriter()

    worker = IngestionWorker(
        bus=bus,
        raw_writer=raw_writer,
        analytical_writer=analytics_writer,
        max_buffer_events=2,
        flush_interval=60.0,
    )

    worker.run_forever()

    assert len(raw_writer.batches) == 2
    assert raw_writer.batches == analytics_writer.batches

    tick_counts = [len(ticks) for ticks, _ in raw_writer.batches]
    book_counts = [len(books) for _, books in raw_writer.batches]

    assert tick_counts == [1, 2]
    assert book_counts == [1, 0]
