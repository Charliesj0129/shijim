"""Queue-based event publisher."""

from __future__ import annotations

from multiprocessing import Queue
from typing import Optional

from shijim.events import MDBookEvent, MDTickEvent


class EventPublisher:
    """Publish normalized events into a multiprocessing queue."""

    def __init__(self, queue: Optional[Queue] = None) -> None:
        self._queue: Queue = queue or Queue()

    @property
    def queue(self) -> Queue:
        return self._queue

    def publish(self, event: MDTickEvent | MDBookEvent) -> None:
        self._queue.put(event)

    def publish_tick(self, event: MDTickEvent) -> None:
        self.publish(event)

    def publish_book(self, event: MDBookEvent) -> None:
        self.publish(event)
