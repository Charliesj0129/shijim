"""Queue-backed event subscriber."""

from __future__ import annotations

from multiprocessing import Queue
from queue import Empty
from typing import Generator, Optional


class EventSubscriber:
    """Consume events published onto a shared queue."""

    def __init__(self, queue: Queue) -> None:
        self._queue = queue

    def get(self, timeout: Optional[float] = None):
        """Retrieve the next event regardless of type."""
        return self._queue.get(timeout=timeout)

    def subscribe(self, event_type: str) -> Generator:
        """Yield events of the requested type as they arrive."""
        while True:
            event = self.get()
            if getattr(event, "type", None) == event_type:
                yield event

    def drain(self):
        """Non-blocking drain helper used mostly in tests."""
        drained = []
        while True:
            try:
                drained.append(self._queue.get_nowait())
            except Empty:
                break
        return drained
