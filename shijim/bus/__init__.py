"""Simple event bus primitives."""

from __future__ import annotations

from .publisher import EventPublisher
from .subscriber import EventSubscriber

__all__ = ["EventPublisher", "EventSubscriber"]
