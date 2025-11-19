from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List

from shijim.bus.event_bus import EventBus
from shijim.events.schema import MDBookEvent, MDTickEvent
from shijim.recorder.ingestion import IngestionWorker


class FakeBus(EventBus):
    def __init__(self, events: Iterable[object]) -> None:
        self._events = list(events)

    def publish(self, event):
        self._events.append(event)

    def subscribe(self, event_type: str | None = None):
        for event in list(self._events):
            yield event


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
        ts=ts,
        symbol="TXF",
        asset_type="futures",
        exchange="TAIFEX",
    )


def _book(ts: int) -> MDBookEvent:
    return MDBookEvent(
        ts=ts,
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
