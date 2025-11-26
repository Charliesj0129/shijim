"""Hawkes Process Intensity Feature.

Wraps the Rust implementation of Hawkes Intensity.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

try:
    from shijim_indicators import RustHawkesIntensity
    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class HawkesConfig:
    baseline: float
    alpha: float
    beta: float


@dataclass(slots=True)
class HawkesSignal:
    ts_ns: int
    symbol: str
    intensity: float


class HawkesEstimator:
    """Estimates Hawkes Process intensity using the Rust backend."""

    def __init__(self, config: HawkesConfig):
        if not RUST_AVAILABLE:
            raise ImportError("shijim_indicators is required for HawkesEstimator")

        self.config = config
        self._rust_calc = RustHawkesIntensity(
            baseline=config.baseline,
            alpha=config.alpha,
            beta=config.beta
        )

    def update(self, ts_ns: int, symbol: str) -> HawkesSignal:
        """Update intensity with a new event at the given timestamp.

        Args:
            ts_ns: Timestamp in nanoseconds.
            symbol: Symbol identifier.

        Returns:
            HawkesSignal with the updated intensity immediately after the event.
        """
        # Rust expects seconds as float
        ts_sec = ts_ns / 1_000_000_000.0

        try:
            intensity = self._rust_calc.update(ts_sec)
            return HawkesSignal(
                ts_ns=ts_ns,
                symbol=symbol,
                intensity=intensity
            )
        except Exception as e:
            logger.error("Error in Rust Hawkes calculation: %s", e)
            # Return current intensity as fallback if update fails?
            # Or re-raise? For now, let's return 0.0 or last known.
            # But since we can't easily get last known without querying, let's re-raise or return 0.
            raise e

    def get_intensity_at(self, ts_ns: int) -> float:
        """Query intensity at a specific time without adding an event."""
        ts_sec = ts_ns / 1_000_000_000.0
        return self._rust_calc.intensity_at(ts_sec)

    def reset(self):
        """Reset the estimator state."""
        self._rust_calc.reset()
