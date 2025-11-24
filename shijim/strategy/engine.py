from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum, auto
from typing import List, Optional

from .ofi import BboState, OfiCalculator


class OrderState(Enum):
    IDLE = auto()
    WORKING = auto()
    CHASING = auto()
    FILLED = auto()


class OrderRequestAction(Enum):
    CANCEL = auto()
    CANCEL_REPLACE = auto()


@dataclass
class OrderRequest:
    action: OrderRequestAction
    price: Optional[float]
    quantity: float
    reason: str
    symbol: Optional[str] = None
    side: Optional[str] = None
    internal_id: Optional[str] = None
    broker_order_id: Optional[str] = None


@dataclass
class StrategyConfig:
    chase_threshold: float
    max_chase_round: int


@dataclass
class OrderStateManager:
    state: OrderState = OrderState.IDLE
    chase_count: int = 0

    def transition(self, new_state: OrderState) -> None:
        self.state = new_state


@dataclass
class SmartChasingEngine:
    config: StrategyConfig
    side: str  # "BUY" only for now
    order_price: float
    order_qty: float
    order_manager: OrderStateManager = field(default_factory=OrderStateManager)
    ofi_calculator: OfiCalculator | None = None

    def __post_init__(self) -> None:
        if self.order_manager.state == OrderState.IDLE:
            self.order_manager.state = OrderState.WORKING
        if self.ofi_calculator is None:
            initial = BboState(self.order_price, self.order_qty, self.order_price, self.order_qty)
            self.ofi_calculator = OfiCalculator(initial_state=initial)
        self.logs: List[str] = []

    def on_tick(self, bbo: BboState, ofi_override: Optional[float] = None) -> List[OrderRequest]:
        self.logs.clear()
        if self.order_manager.state == OrderState.CHASING:
            return []
        if self.order_manager.state == OrderState.IDLE:
            return []

        assert self.ofi_calculator is not None
        ofi_result = self.ofi_calculator.process_tick(
            bbo.bid_price, bbo.bid_size, bbo.ask_price, bbo.ask_size
        )
        ofi_signal = ofi_override if ofi_override is not None else ofi_result.net_ofi
        market_bid = bbo.bid_price
        price_diff = market_bid - self.order_price

        if self.order_manager.chase_count >= self.config.max_chase_round and price_diff > 0:
            self.order_manager.transition(OrderState.IDLE)
            req = OrderRequest(
                action=OrderRequestAction.CANCEL,
                price=None,
                quantity=self.order_qty,
                reason="MaxChaseReached",
            )
            return [req]

        if price_diff <= 0:
            return []

        should_chase = price_diff > self.config.chase_threshold
        alpha_push = price_diff >= self.config.chase_threshold and ofi_signal > 0

        if should_chase is False and alpha_push is False:
            return []

        if ofi_signal < 0 and should_chase:
            self.logs.append("Hold: Negative Alpha Protection")
            return []

        reason = "AlphaDriven" if alpha_push and ofi_signal > 0 else "PriceDrift"
        self.order_price = market_bid
        self.order_manager.chase_count += 1
        self.order_manager.transition(OrderState.CHASING)

        req = OrderRequest(
            action=OrderRequestAction.CANCEL_REPLACE,
            price=market_bid,
            quantity=self.order_qty,
            reason=reason,
        )
        return [req]
