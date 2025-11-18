"""Observer interfaces used for monitoring throughput, gaps, and latency."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from shijim.events.schema import BaseMDEvent


class QuoteObserver(Protocol):
    """Common interface implemented by any monitoring observer."""

    def on_event(self, event: BaseMDEvent) -> None:
        """Receive a normalized event emitted by the gateway/recorder."""


@dataclass
class ThroughputMonitor:
    """Tracks event throughput (per second/minute) for alerting/metrics."""

    window_secs: int = 60

    def on_event(self, event: BaseMDEvent) -> None:
        """Update counters/statistics for the configured sliding window."""
        raise NotImplementedError("Throughput tracking placeholder.")


@dataclass
class GapDetector:
    """Detects long silences per symbol and emits gap definitions."""

    tolerance_ns: int

    def on_event(self, event: BaseMDEvent) -> None:
        """Track last-seen timestamp per symbol and identify missing data."""
        raise NotImplementedError("Gap detection placeholder.")


@dataclass
class LatencyMonitor:
    """Computes now-ts minus event.ts to catch infrastructure delay."""

    samples: list[int] = field(default_factory=list)

    def on_event(self, event: BaseMDEvent) -> None:
        """Record latency samples for future reporting."""
        raise NotImplementedError("Latency sampling placeholder.")
