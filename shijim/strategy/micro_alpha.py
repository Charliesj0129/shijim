from __future__ import annotations

import logging
from dataclasses import dataclass

from shijim.bus import EventBus
from shijim.events.schema import BaseMDEvent, MDBookEvent
from shijim.features.ofi import OFIAccumulator, OFISignal

logger = logging.getLogger(__name__)


@dataclass
class MicroAlphaConfig:
    symbol: str
    ofi_threshold: float = 10.0
    max_position: int = 10
    order_qty: int = 1
    accumulator_interval: float = 1.0


class MicroAlphaStrategy:
    """
    Micro Alpha Strategy using Order Flow Imbalance (OFI).

    Logic:
    1. Accumulate OFI over a time window (e.g., 1s).
    2. If OFI > threshold, BUY.
    3. If OFI < -threshold, SELL.
    4. Respect max position limits.
    """

    def __init__(self, bus: EventBus, config: MicroAlphaConfig):
        self.bus = bus
        self.config = config
        self.ofi_accumulator = OFIAccumulator(interval_seconds=config.accumulator_interval)
        self.position = 0
        self.active = False
        self.signals: list[OFISignal] = [] # For debugging/telemetry

    def start(self) -> None:
        """Start the strategy."""
        self.active = True
        logger.info("MicroAlphaStrategy started for %s", self.config.symbol)

    def stop(self) -> None:
        """Stop the strategy."""
        self.active = False
        logger.info("MicroAlphaStrategy stopped")

    def on_event(self, event: BaseMDEvent) -> None:
        """Process incoming market data events."""
        if not self.active:
            return

        if isinstance(event, MDBookEvent):
            if event.symbol == self.config.symbol:
                self.on_book(event)

    def on_book(self, event: MDBookEvent) -> None:
        """Process book updates to calculate OFI."""
        signal = self.ofi_accumulator.process(event)
        if signal:
            self.on_signal(signal)

    def on_signal(self, signal: OFISignal) -> None:
        """React to generated OFI signals."""
        self.signals.append(signal)
        logger.info("OFI Signal for %s: %.2f", signal.symbol, signal.ofi_value)

        if signal.ofi_value > self.config.ofi_threshold:
            self.execute_buy()
        elif signal.ofi_value < -self.config.ofi_threshold:
            self.execute_sell()

    def execute_buy(self) -> None:
        """Execute a buy order (mock)."""
        if self.position + self.config.order_qty <= self.config.max_position:
            logger.info("SIGNAL BUY %s %s @ MARKET", self.config.order_qty, self.config.symbol)
            self.position += self.config.order_qty
            # In real system: self.order_manager.send_order(...)
        else:
            logger.debug("Buy signal ignored: Max position reached (%s)", self.position)

    def execute_sell(self) -> None:
        """Execute a sell order (mock)."""
        if self.position - self.config.order_qty >= -self.config.max_position:
            logger.info("SIGNAL SELL %s %s @ MARKET", self.config.order_qty, self.config.symbol)
            self.position -= self.config.order_qty
            # In real system: self.order_manager.send_order(...)
        else:
            logger.debug("Sell signal ignored: Max position reached (%s)", self.position)
