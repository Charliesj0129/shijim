"""In-process event bus abstractions used by gateway and recorder components."""

from __future__ import annotations

import logging
from collections import defaultdict, deque
from dataclasses import dataclass, field
from threading import Condition, Lock, RLock
from typing import Deque, Dict, Iterable, Protocol, List
import queue

from shijim.events.schema import BaseMDEvent

logger = logging.getLogger(__name__)


class EventBus(Protocol):
    """Minimal interface for publishing/consuming normalized market data events.

    Concrete implementations can choose between competing-consumer semantics
    (one queue shared across subscribers) or broadcast semantics (per-subscriber
    queues). The recorder defaults to the competing-consumer bus to avoid
    unbounded fan-out, while other components may opt into broadcast delivery.
    """

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
    """Reference competing-consumer event bus for local development/testing."""

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


@dataclass
class BroadcastEventBus:
    """Broadcast event bus where each subscriber receives every matching event.

    Each subscriber gets a dedicated queue. This increases memory usage with the
    number of subscribers but guarantees that all listeners observe every
    published event (subject to their topic filter).
    """

    max_queue_size: int = 10_000
    _lock: RLock = field(default_factory=RLock)
    _subscriptions: Dict[str, List[queue.Queue]] = field(default_factory=dict, init=False)

    def publish(self, event: BaseMDEvent) -> None:
        """Fan-out the event to all subscribers of its type and \"*\"."""
        targets: list[queue.Queue] = []
        with self._lock:
            for topic in (event.type, "*"):
                queues = self._subscriptions.get(topic)
                if queues:
                    targets.extend(list(queues))
        for q in targets:
            try:
                q.put_nowait(event)
            except queue.Full:
                try:
                    _ = q.get_nowait()
                except queue.Empty:
                    pass
                logger.warning(
                    "BroadcastEventBus queue exceeded max_queue_size=%s; dropping oldest event.",
                    self.max_queue_size,
                )
                q.put_nowait(event)

    def subscribe(
        self,
        event_type: str | None = None,
        timeout: float | None = None,
    ) -> Iterable[BaseMDEvent | None]:
        """
        Yield events for the given type, emitting None heartbeats on timeout.

        Each subscriber owns its own queue, so every published event is delivered
        to each subscriber independently (subject to the configured topic filter).
        """
        topic = event_type or "*"
        q: queue.Queue = queue.Queue(maxsize=self.max_queue_size)
        with self._lock:
            self._subscriptions.setdefault(topic, []).append(q)

        def iterator() -> Iterable[BaseMDEvent | None]:
            try:
                while True:
                    try:
                        event = q.get(timeout=timeout) if timeout is not None else q.get()
                    except queue.Empty:
                        yield None
                        continue
                    yield event
            finally:
                with self._lock:
                    subscribers = self._subscriptions.get(topic)
                    if subscribers and q in subscribers:
                        subscribers.remove(q)

        return iterator()
