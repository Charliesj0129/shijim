"""Observer interfaces used for monitoring throughput, gaps, and latency."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol
import time
from collections import deque
import logging

from shijim.events.schema import BaseMDEvent

logger = logging.getLogger(__name__)

try:
    from prometheus_client import Counter, Gauge, Histogram
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    # Mock classes if prometheus_client is missing
    class MockMetric:
        def inc(self, amount=1): pass
        def set(self, value): pass
        def observe(self, value): pass
        def time(self): return self
        def labels(self, **kwargs): return self
        def __enter__(self): pass
        def __exit__(self, *args): pass
    
    Counter = Gauge = Histogram = lambda *args, **kwargs: MockMetric()

# Metrics
OBSERVER_THROUGHPUT = Gauge("observer_throughput_events_per_sec", "Current event throughput")
OBSERVER_GAPS_TOTAL = Counter("observer_gaps_total", "Total data gaps detected", ["symbol"])
OBSERVER_LATENCY_SECONDS = Histogram(
    "observer_latency_seconds", 
    "End-to-end latency from exchange timestamp",
    buckets=(0.0001, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, float("inf"))
)


class QuoteObserver(Protocol):
    """Common interface implemented by any monitoring observer."""

    def on_event(self, event: BaseMDEvent) -> None:
        """Receive a normalized event emitted by the gateway/recorder."""


@dataclass
class ThroughputMonitor:
    """Tracks event throughput (per second) for alerting/metrics."""

    window_secs: int = 60
    _counts: deque = field(default_factory=deque)
    _last_update: float = field(default_factory=time.monotonic)
    update_interval: float = 1.0

    def on_event(self, event: BaseMDEvent) -> None:
        """Update counters/statistics for the configured sliding window."""
        now = time.monotonic()
        self._counts.append(now)
        
        # Prune old events
        while self._counts and (now - self._counts[0] > self.window_secs):
            self._counts.popleft()
            
        # Update metric periodically based on time
        if now - self._last_update >= self.update_interval:
             rate = len(self._counts) / self.window_secs if self.window_secs > 0 else 0
             OBSERVER_THROUGHPUT.set(rate)
             self._last_update = now


@dataclass
class GapDetector:
    """Detects long silences per symbol and emits gap definitions."""

    tolerance_ns: int
    _last_ts: dict[str, int] = field(default_factory=dict)

    def on_event(self, event: BaseMDEvent) -> None:
        """Track last-seen timestamp per symbol and identify missing data."""
        # BaseMDEvent guarantees symbol presence
        symbol = event.symbol
        ts = event.ts_ns
        
        if ts == 0:
            return

        last = self._last_ts.get(symbol)
        if last is not None:
            diff = ts - last
            if diff > self.tolerance_ns:
                logger.warning(f"Gap detected for {symbol}: {diff} ns > {self.tolerance_ns}")
                OBSERVER_GAPS_TOTAL.labels(symbol=symbol).inc()
        
        self._last_ts[symbol] = ts


@dataclass
class LatencyMonitor:
    """Computes now - event.ts_ns to catch infrastructure delay."""

    samples: list[int] = field(default_factory=list)
    max_samples: int = 1000
    _last_skew_warning: float = field(default=0.0)

    def on_event(self, event: BaseMDEvent) -> None:
        """Record latency samples for future reporting."""
        ts = event.ts_ns
        if ts == 0:
            return
            
        now_ns = time.time_ns()
        latency_ns = now_ns - ts
        latency_sec = latency_ns / 1e9
        
        if latency_ns < 0:
            # Clock skew or future timestamp detected
            now = time.monotonic()
            if now - self._last_skew_warning > 60: # Rate limit warning
                logger.warning(f"Negative latency detected: {latency_ns} ns. Possible clock skew.")
                self._last_skew_warning = now
            return

        # Update Histogram
        OBSERVER_LATENCY_SECONDS.observe(latency_sec)
        
        if len(self.samples) < self.max_samples:
            self.samples.append(latency_ns)
        else:
            self.samples.pop(0)
            self.samples.append(latency_ns)
