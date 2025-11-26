"""Order Flow Imbalance (OFI) Feature Calculator.

Implements OFI logic based on Cont et al. and BDD_SPEC.md.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np

from shijim.events.schema import MDBookEvent

logger = logging.getLogger(__name__)

try:
    from shijim_indicators import RustOfiCalculator
    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False
    logger.warning("shijim_indicators not found. Using Python fallback for OFI.")


@dataclass(slots=True)
class OFISignal:
    """Output signal from OFI calculation."""
    
    ts_ns: int
    symbol: str
    ofi_value: float
    # We can add more fields later like 'ofi_depth' or 'tfi'


class OFICalculator:
    """Calculates Order Flow Imbalance from consecutive book events."""

    def __init__(self):
        self._prev_book: dict[str, MDBookEvent] = {}
        self._rust_calc: Optional[RustOfiCalculator] = None
        if RUST_AVAILABLE:
            self._rust_calc = RustOfiCalculator()

    def calculate(self, event: MDBookEvent) -> Optional[OFISignal]:
        """Compute OFI for the given book event relative to the previous state."""
        if self._rust_calc:
            return self._calculate_rust(event)
        return self._calculate_python(event)

    def _calculate_rust(self, event: MDBookEvent) -> Optional[OFISignal]:
        """Use Rust implementation for OFI calculation."""
        # Convert lists to numpy arrays for Rust
        # Note: This conversion adds overhead, but the calculation is faster.
        # Ideally, MDBookEvent should hold numpy arrays directly.
        bid_prices = np.array(event.bid_prices, dtype=np.float64)
        bid_volumes = np.array(event.bid_volumes, dtype=np.float64)
        ask_prices = np.array(event.ask_prices, dtype=np.float64)
        ask_volumes = np.array(event.ask_volumes, dtype=np.float64)

        try:
            ofi_val = self._rust_calc.update_from_levels(
                bid_prices, bid_volumes, ask_prices, ask_volumes
            )
            if ofi_val is None:
                return None
            
            return OFISignal(
                ts_ns=event.ts_ns,
                symbol=event.symbol,
                ofi_value=ofi_val
            )
        except Exception as e:
            logger.error("Error in Rust OFI calculation: %s", e)
            return None

    def _calculate_python(self, event: MDBookEvent) -> Optional[OFISignal]:
        """Use Python implementation for OFI calculation."""
        symbol = event.symbol
        prev = self._prev_book.get(symbol)
        
        # Update state
        self._prev_book[symbol] = event
        
        if prev is None:
            return None
            
        # Extract BBO (Best Bid/Offer)
        # Assuming lists are sorted: bid[0] is best (highest), ask[0] is best (lowest)
        # MDBookEvent schema: bid_prices, bid_volumes, ask_prices, ask_volumes
        
        if not event.bid_prices or not event.ask_prices:
            return OFISignal(ts_ns=event.ts_ns, symbol=symbol, ofi_value=0.0)
        if not prev.bid_prices or not prev.ask_prices:
            return OFISignal(ts_ns=event.ts_ns, symbol=symbol, ofi_value=0.0)

        b_n = event.bid_prices[0]
        q_n_b = event.bid_volumes[0]
        a_n = event.ask_prices[0]
        q_n_a = event.ask_volumes[0]
        
        b_prev = prev.bid_prices[0]
        q_prev_b = prev.bid_volumes[0]
        a_prev = prev.ask_prices[0]
        q_prev_a = prev.ask_volumes[0]
        
        # Calculate Bid contribution
        # I(b_n >= b_{n-1}) * q_n^b
        term1 = q_n_b if b_n >= b_prev else 0
        # - I(b_n <= b_{n-1}) * q_{n-1}^b
        term2 = q_prev_b if b_n <= b_prev else 0
        
        bid_contrib = term1 - term2
        
        # Calculate Ask contribution
        # - I(a_n <= a_{n-1}) * q_n^a
        term3 = q_n_a if a_n <= a_prev else 0
        # + I(a_n >= a_{n-1}) * q_{n-1}^a
        term4 = q_prev_a if a_n >= a_prev else 0
        
        ask_contrib = -term3 + term4
        
        ofi = bid_contrib + ask_contrib
        
        return OFISignal(
            ts_ns=event.ts_ns,
            symbol=symbol,
            ofi_value=float(ofi)
        )


class OFIAccumulator:
    """Accumulates OFI values over time windows."""
    
    def __init__(self, interval_seconds: float = 1.0):
        self.interval_ns = int(interval_seconds * 1_000_000_000)
        self._calculator = OFICalculator()
        self._accumulators: dict[str, float] = {}
        self._last_emit: dict[str, int] = {}
        
    def process(self, event: MDBookEvent) -> Optional[OFISignal]:
        """Process an event and return an accumulated signal if the window has passed."""
        symbol = event.symbol
        
        # Initialize start of window if new symbol
        if self._last_emit.get(symbol) is None:
            self._last_emit[symbol] = event.ts_ns
            
        ofi = self._calculator.calculate(event)
        if ofi is None:
            return None
            
        current_acc = self._accumulators.get(symbol, 0.0)
        self._accumulators[symbol] = current_acc + ofi.ofi_value
        
        last_emit = self._last_emit[symbol]
        
        # Debug print
        # print(f"DEBUG: symbol={symbol}, ts={event.ts_ns}, last={last_emit}, diff={event.ts_ns - last_emit}, interval={self.interval_ns}")
            
        if event.ts_ns - last_emit >= self.interval_ns:
            # Emit accumulated value
            result = OFISignal(
                ts_ns=event.ts_ns,
                symbol=symbol,
                ofi_value=self._accumulators[symbol]
            )
            # Reset
            self._accumulators[symbol] = 0.0
            self._last_emit[symbol] = event.ts_ns
            return result
            
        return None
