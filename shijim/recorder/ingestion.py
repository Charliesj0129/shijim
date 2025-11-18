"""Recorder ingestion worker implementation."""

from __future__ import annotations

import itertools
import time
from dataclasses import dataclass, field
from typing import Callable, Iterator, Sequence

from shijim.bus import EventBus
from shijim.events.schema import BaseMDEvent, MDBookEvent, MDTickEvent
from shijim.recorder.raw_writer import RawWriter
from shijim.recorder.clickhouse_writer import ClickHouseWriter


@dataclass
class IngestionWorker:
    """Consumes EventBus streams and forwards them to writer backends."""

    bus: EventBus
    raw_writer: RawWriter
    analytical_writer: ClickHouseWriter
    max_buffer_events: int = 500
    flush_interval: float = 1.0
    clock: Callable[[], float] = time.monotonic
    _ticks_buffer: list[MDTickEvent] = field(default_factory=list, init=False)
    _books_buffer: list[MDBookEvent] = field(default_factory=list, init=False)
    _last_flush: float = field(default=0.0, init=False)

    def run_forever(self) -> None:
        """Continuously pull events from the EventBus and flush on thresholds."""
        self._last_flush = self.clock()
        streams: list[Iterator[BaseMDEvent]] = [
            iter(self.bus.subscribe("MD_TICK")),
            iter(self.bus.subscribe("MD_BOOK")),
        ]
        try:
            for event in self._merge_streams(streams):
                self._handle_event(event)
        finally:
            self.flush()

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _merge_streams(self, streams: Sequence[Iterator[BaseMDEvent]]) -> Iterator[BaseMDEvent]:
        """Round-robin between the subscribed iterators until all are exhausted."""
        active = list(streams)
        while active:
            next_active: list[Iterator[BaseMDEvent]] = []
            for stream in active:
                try:
                    yield next(stream)
                    next_active.append(stream)
                except StopIteration:
                    continue
            active = next_active

    def _handle_event(self, event: BaseMDEvent) -> None:
        if event.type == "MD_TICK" and isinstance(event, MDTickEvent):
            self._ticks_buffer.append(event)
        elif event.type == "MD_BOOK" and isinstance(event, MDBookEvent):
            self._books_buffer.append(event)
        else:
            return

        if self._should_flush():
            self.flush()

    def _should_flush(self) -> bool:
        total_events = len(self._ticks_buffer) + len(self._books_buffer)
        if total_events >= self.max_buffer_events:
            return True
        return (self.clock() - self._last_flush) >= self.flush_interval

    def flush(self) -> None:
        """Flush any buffered events to both raw and analytical writers."""
        if not self._ticks_buffer and not self._books_buffer:
            self._last_flush = self.clock()
            return
        ticks = list(self._ticks_buffer)
        books = list(self._books_buffer)
        self._ticks_buffer.clear()
        self._books_buffer.clear()
        self.raw_writer.write_batch(ticks, books)
        self.analytical_writer.write_batch(ticks, books)
        self._last_flush = self.clock()
