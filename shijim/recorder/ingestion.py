"""Queue-based ingestion worker."""

from __future__ import annotations

import logging
from queue import Empty
from threading import Event
from typing import Optional

from shijim.events import MDBookEvent, MDTickEvent

logger = logging.getLogger(__name__)


class IngestionWorker:
    """Consume events from a subscriber and dispatch to writers."""

    def __init__(self, subscriber, raw_writer, clickhouse_writer=None) -> None:
        self._subscriber = subscriber
        self._raw_writer = raw_writer
        self._clickhouse_writer = clickhouse_writer

    def run_once(self, timeout: Optional[float] = None) -> bool:
        """Process a single event; return False on timeout."""
        try:
            event = self._subscriber.get(timeout=timeout)
        except Empty:
            return False

        if not isinstance(event, (MDTickEvent, MDBookEvent)):
            logger.debug("Skipping unsupported event type: %r", type(event))
            return True

        self._raw_writer.append(event)
        if self._clickhouse_writer:
            self._clickhouse_writer.append(event)
        return True

    def run_forever(self, stop_event: Optional[Event] = None, timeout: float = 1.0) -> None:
        """Continuously process events until stop_event is set."""
        while not (stop_event and stop_event.is_set()):
            processed = self.run_once(timeout=timeout)
            if not processed:
                continue
