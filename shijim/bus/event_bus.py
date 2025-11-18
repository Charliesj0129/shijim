"""In-process event bus abstractions used by gateway and recorder components."""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from threading import Condition, Lock
from typing import Deque, Dict, Iterable, Protocol

from shijim.events.schema import BaseMDEvent

logger = logging.getLogger(__name__)


class EventBus(Protocol):
    """Minimal interface for publishing/consuming normalized market data events."""

    def publish(self, event: BaseMDEvent) -> None:
        """Enqueue an event for downstream consumers."""

    def subscribe(self, event_type: str | None = None) -> Iterable[BaseMDEvent]:
        """Yield events as they arrive, optionally filtered by type."""


@dataclass
class InMemoryEventBus:
    """Reference event bus using simple queues for local development/testing."""

    max_queue_size: int = 10_000
    _queues: Dict[str, Deque[BaseMDEvent]] = field(default_factory=lambda: defaultdict(deque))
    _lock: Lock = field(default_factory=Lock)
    _not_empty: Condition = field(init=False)

    def __post_init__(self) -> None:
        self._not_empty = Condition(self._lock)

    def publish(self, event: BaseMDEvent) -> None:
        """Store events in memory, dropping oldest entries if queue is full."""
        with self._lock:
            for etype in (event.type, "*"):
                queue = self._queue_for(etype)
                if len(queue) >= self.max_queue_size:
                    queue.popleft()
                    logger.warning(
                        "EventBus backlog exceeded max_queue_size=%s for %s; dropping oldest event.",
                        self.max_queue_size,
                        etype,
                    )
                queue.append(event)
            self._not_empty.notify_all()

    def subscribe(self, event_type: str | None = None) -> Iterable[BaseMDEvent]:
        """Iterate over queued events, blocking until new data arrives."""
        queue = self._queue_for(event_type or "*")
        while True:
            with self._lock:
                while not queue:
                    self._not_empty.wait()
                event = queue.popleft()
            yield event

    def _queue_for(self, event_type: str) -> Deque[BaseMDEvent]:
        return self._queues[event_type]
