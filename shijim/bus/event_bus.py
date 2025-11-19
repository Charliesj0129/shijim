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

    def subscribe(
        self,
        event_type: str | None = None,
        timeout: float | None = None,
    ) -> Iterable[BaseMDEvent | None]:
        """
        Yield events for the given type.

        If timeout is set and expires, yield None as a heartbeat.
        """


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

    def subscribe(
        self,
        event_type: str | None = None,
        timeout: float | None = None,
    ) -> Iterable[BaseMDEvent | None]:
        """
        Iterate over queued events, optionally emitting heartbeat None values.

        When timeout is provided and expires without new data, None is yielded
        so call sites can perform periodic housekeeping.
        """
        queue = self._queue_for(event_type or "*")
        while True:
            event: BaseMDEvent | None = None
            with self._lock:
                while not queue:
                    notified = self._not_empty.wait(timeout=timeout)
                    if timeout is None or notified:
                        continue
                    break
                if queue:
                    event = queue.popleft()
            if event is not None:
                yield event
            elif timeout is not None:
                yield None

    def _queue_for(self, event_type: str) -> Deque[BaseMDEvent]:
        return self._queues[event_type]
