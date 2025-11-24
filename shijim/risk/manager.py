from __future__ import annotations

from dataclasses import dataclass
from queue import SimpleQueue
from typing import List, Sequence

from shijim.strategy.engine import OrderRequest
from .guards import (
    RiskManagerConfig,
    FatFingerGuard,
    PositionGuard,
    RateLimiter,
    KillSwitch,
    RiskResult,
)


@dataclass
class RiskAwareGateway:
    inner_gateway: any
    config: RiskManagerConfig
    event_queue: SimpleQueue
    market_price: float
    position: float = 0.0

    def __post_init__(self) -> None:
        self.finger_guard = FatFingerGuard(self.config, self.market_price)
        self.position_guard = PositionGuard(self.config, self.position)
        self.rate_limiter = RateLimiter(self.config.max_orders_per_sec)
        self.kill_switch = KillSwitch()

    def update_market_price(self, price: float) -> None:
        self.market_price = price
        self.finger_guard.set_reference_price(price)

    def update_position(self, filled_qty: float, side: str) -> None:
        self.position_guard.update_position(filled_qty, side)

    def activate_kill(self) -> None:
        self.kill_switch.activate()

    def deactivate_kill(self) -> None:
        self.kill_switch.deactivate()

    def send(self, orders: Sequence[OrderRequest]) -> List:
        valid: List[OrderRequest] = []
        for order in orders:
            result = self._check(order)
            if result.passed:
                valid.append(order)
            else:
                self.event_queue.put({"type": "RiskReject", "reason": result.reason, "order": order})
        if not valid:
            return []
        return self.inner_gateway.send(valid)

    def _check(self, order: OrderRequest) -> RiskResult:
        for guard in (self.kill_switch, self.finger_guard, self.position_guard):
            res = guard.check(order)
            if not res.passed:
                return res
        rate = self.rate_limiter.check()
        if not rate.passed:
            return rate
        return RiskResult(True)
