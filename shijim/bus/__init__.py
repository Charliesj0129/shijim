"""In-process event bus abstractions used by gateway and recorder components."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Protocol

from shijim.events.schema import BaseMDEvent


class EventBus(Protocol):
    """Minimal interface for publishing/consuming normalized market data events."""

    def publish(self, event: BaseMDEvent) -> None:
        """Enqueue an event for downstream consumers."""

    def subscribe(self, event_type: str | None = None) -> Iterable[BaseMDEvent]:
        """Yield events as they arrive, optionally filtered by type."""


@dataclass
class InMemoryEventBus:
    """Reference event bus using simple queues for local development/testing."""

    _queue: list[BaseMDEvent] = field(default_factory=list)

    def publish(self, event: BaseMDEvent) -> None:
        """Store events in memory (placeholder implementation)."""
        raise NotImplementedError("Queueing/push mechanics will be implemented later.")

    def subscribe(self, event_type: str | None = None) -> Iterable[BaseMDEvent]:
        """Iterate over queued events, ignoring filtering until implemented."""
        raise NotImplementedError("Subscription/iteration semantics placeholder.")


__all__ = ["EventBus", "InMemoryEventBus"]
