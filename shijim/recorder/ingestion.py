"""Recorder ingestion worker implementation."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable

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
    max_buffer_events: int = 1_000
    flush_interval: float = 1.0
    max_batch_events: int = 512
    max_batch_wait: float = 0.01
    poll_timeout: float = 0.1
    clock: Callable[[], float] = time.monotonic
    _ticks_buffer: list[MDTickEvent] = field(default_factory=list, init=False)
    _books_buffer: list[MDBookEvent] = field(default_factory=list, init=False)
    _last_flush: float = field(default=0.0, init=False)
    _stop_event: threading.Event = field(default_factory=threading.Event, init=False)

    def __post_init__(self) -> None:
        self._enable_async_writer(self.raw_writer)
        self._enable_async_writer(self.analytical_writer)

    def run_forever(self) -> None:
        """Continuously pull events from the EventBus and flush on thresholds."""
        self._last_flush = self.clock()
        events = self.bus.subscribe(None, timeout=self.poll_timeout)
        try:
            while not self._stop_event.is_set():
                batch_count = 0
                batch_deadline = self.clock() + self.max_batch_wait
                while batch_count < self.max_batch_events:
                    try:
                        event = next(events)
                    except StopIteration:
                        self._stop_event.set()
                        break
                    if self._stop_event.is_set():
                        break
                    if event is None:
                        # Heartbeat; allow periodic flush checks
                        break
                    self._handle_event(event)
                    if self._should_flush():
                        self.flush()
                    batch_count += 1
                    if self._stop_event.is_set() or self.clock() >= batch_deadline:
                        break
                if self._should_flush():
                    self.flush()
            self.flush()
        finally:
            self.flush()
            self._drain_async_writers()

    def stop(self) -> None:
        """Signal the ingestion loop to stop."""
        self._stop_event.set()

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
        self.analytical_writer.flush(force=True)
        self._last_flush = self.clock()

    def _handle_event(self, event: BaseMDEvent) -> None:
        if isinstance(event, MDTickEvent):
            self._ticks_buffer.append(event)
        elif isinstance(event, MDBookEvent):
            self._books_buffer.append(event)

    def _enable_async_writer(self, writer: object) -> None:
        enable = getattr(writer, "enable_async", None)
        if callable(enable):
            enable()

    def _drain_async_writers(self) -> None:
        for writer in (self.raw_writer, self.analytical_writer):
            drain = getattr(writer, "drain_async", None)
            if callable(drain):
                drain()
