from __future__ import annotations

from dataclasses import dataclass
from queue import SimpleQueue
from time import monotonic
from typing import List, Optional, Protocol

from shijim.strategy.engine import OrderRequest, OrderRequestAction


@dataclass
class RiskResult:
    passed: bool
    reason: Optional[str] = None


@dataclass
class RiskManagerConfig:
    max_order_qty: float
    max_position: float
    price_deviation: float
    max_orders_per_sec: int


class GatewayProtocol(Protocol):
    def send(self, orders: List[OrderRequest]): ...


class RiskEvent:
    def __init__(self, msg: str, payload: Optional[dict] = None):
        self.msg = msg
        self.payload = payload or {}


class FatFingerGuard:
    def __init__(self, config: RiskManagerConfig, ref_price: float) -> None:
        self.config = config
        self.ref_price = ref_price

    def set_reference_price(self, price: float) -> None:
        self.ref_price = price

    def check(self, order: OrderRequest) -> RiskResult:
        if order.action == OrderRequestAction.CANCEL:
            return RiskResult(True)
        if order.price is None:
            return RiskResult(True)
        deviation = abs(order.price - self.ref_price) / self.ref_price
        if deviation > self.config.price_deviation:
            return RiskResult(False, "PriceDeviation")
        if order.quantity > self.config.max_order_qty:
            return RiskResult(False, "MaxOrderQty")
        return RiskResult(True)


class PositionGuard:
    def __init__(self, config: RiskManagerConfig, position: float) -> None:
        self.config = config
        self.position = position

    def update_position(self, filled_qty: float, side: str) -> None:
        self.position += filled_qty if side.upper() == "BUY" else -filled_qty

    def check(self, order: OrderRequest) -> RiskResult:
        if order.action == OrderRequestAction.CANCEL:
            return RiskResult(True)
        next_position = self.position + order.quantity
        if next_position > self.config.max_position:
            return RiskResult(False, "PositionLimit")
        return RiskResult(True)


class RateLimiter:
    def __init__(self, max_per_sec: int) -> None:
        self.max_per_sec = max_per_sec
        self.tokens = max_per_sec
        self.last_refill = monotonic()

    def _refill(self) -> None:
        now = monotonic()
        elapsed = now - self.last_refill
        if elapsed >= 1.0:
            self.tokens = self.max_per_sec
            self.last_refill = now

    def check(self) -> RiskResult:
        self._refill()
        if self.tokens <= 0:
            return RiskResult(False, "RateLimit")
        self.tokens -= 1
        return RiskResult(True)


class KillSwitch:
    def __init__(self) -> None:
        self.active = False

    def activate(self) -> None:
        self.active = True

    def deactivate(self) -> None:
        self.active = False

    def check(self, order: OrderRequest) -> RiskResult:
        if not self.active:
            return RiskResult(True)
        if order.action == OrderRequestAction.CANCEL:
            return RiskResult(True)
        return RiskResult(False, "KillSwitch")
