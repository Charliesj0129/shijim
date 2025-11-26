"""VPIN (Volume-Synchronized Probability of Informed Trading) Feature.

Wraps the Rust implementation of VPIN.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

try:
    from shijim_indicators import RustVpinCalculator
    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class VPINConfig:
    bucket_volume: float
    window_size: int


@dataclass(slots=True)
class VPINSignal:
    ts_ns: int
    symbol: str
    vpin_value: float


class VPINCalculator:
    """Calculates VPIN using the Rust backend."""

    def __init__(self, config: VPINConfig):
        if not RUST_AVAILABLE:
            raise ImportError("shijim_indicators is required for VPINCalculator")

        self.config = config
        self._rust_calc = RustVpinCalculator(
            bucket_volume=config.bucket_volume,
            window_size=config.window_size
        )

    def update(self, signed_volume: float, ts_ns: int, symbol: str) -> Optional[VPINSignal]:
        """Update VPIN with a new trade's signed volume.

        Args:
            signed_volume: Positive for buy, negative for sell.
            ts_ns: Timestamp in nanoseconds.
            symbol: Symbol identifier.

        Returns:
            VPINSignal if a new VPIN value is available (bucket completed), else None.
        """
        try:
            vpin_val = self._rust_calc.update_signed_volume(signed_volume)
            if vpin_val is not None:
                return VPINSignal(
                    ts_ns=ts_ns,
                    symbol=symbol,
                    vpin_value=vpin_val
                )
        except Exception as e:
            logger.error("Error in Rust VPIN calculation: %s", e)

        return None

    def reset(self):
        """Reset the calculator state."""
        self._rust_calc.reset()
