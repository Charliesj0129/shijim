"""Queue-based event publisher."""

from __future__ import annotations

from typing import Optional

from shijim.bus.event_bus import EventBus, InMemoryEventBus
from shijim.events import MDBookEvent, MDTickEvent


class EventPublisher:
    """Publish normalized events into an EventBus."""

    def __init__(self, bus: Optional[EventBus] = None) -> None:
        self._bus: EventBus = bus or InMemoryEventBus()

    @property
    def bus(self) -> EventBus:
        return self._bus

    def publish(self, event: MDTickEvent | MDBookEvent) -> None:
        self._bus.publish(event)

    def publish_tick(self, event: MDTickEvent) -> None:
        self.publish(event)

    def publish_book(self, event: MDBookEvent) -> None:
        self.publish(event)
