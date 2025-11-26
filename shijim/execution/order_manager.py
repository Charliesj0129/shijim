from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Protocol

from shijim.strategy.engine import OrderRequest, OrderRequestAction

logger = logging.getLogger(__name__)

# Try to import shioaji constants, else mock
try:
    import shioaji as sj
    SJAction = getattr(sj.constant, "Action", None)
    SJPriceType = getattr(sj.constant, "OrderPriceType", getattr(sj.constant, "PriceType", None))
    SJOrderType = getattr(sj.constant, "OrderType", None)
except ImportError:
    SJAction = None
    SJPriceType = None
    SJOrderType = None

if SJAction is None:
    class SJAction:
        Buy = "Buy"
        Sell = "Sell"

if SJPriceType is None:
    class SJPriceType:
        Limit = "Limit"
        Market = "Market"

if SJOrderType is None:
    class SJOrderType:
        ROD = "ROD"
        IOC = "IOC"
        FOK = "FOK"


class ShioajiClientProtocol(Protocol):
    def place_order(self, contract: Any, order: Any, timeout: int = 0) -> Any: ...
    def cancel_order(self, account: Any, order_id: str, timeout: int = 0) -> Any: ...
    def Order(self, *args, **kwargs) -> Any: ...


@dataclass
class OrderState:
    internal_id: str
    status: str = "PENDING"
    broker_id: Optional[str] = None
    submitted_at: float = field(default_factory=time.time)
    filled_qty: float = 0.0
    avg_price: float = 0.0
    last_error: Optional[str] = None


class NonBlockingOrderManager:
    """
    Manages order execution with non-blocking calls (timeout=0).
    Tracks order state via internal_id.
    """

    def __init__(self, api: ShioajiClientProtocol, contract_resolver: Any, account: Any = None):
        self.api = api
        self.contract_resolver = contract_resolver
        self.account = account
        self.orders: Dict[str, OrderState] = {}

    def send_order(self, req: OrderRequest) -> None:
        """Send an order request non-blocking."""
        if not req.internal_id:
            logger.warning("OrderRequest missing internal_id")
            return

        # Initialize state if not exists, or update if needed
        if req.internal_id not in self.orders:
            self.orders[req.internal_id] = OrderState(internal_id=req.internal_id)
        
        if req.action == OrderRequestAction.CANCEL:
            self._cancel_order(req)
        else:
            # For new orders or replacements, we might want to reset some state?
            # But if it's a REPLACE, we need the old broker_id to cancel it first?
            # The current logic handles CANCEL separately.
            # If it's a new order (BUY/SELL), we proceed.
            self._place_order(req)

    def _place_order(self, req: OrderRequest) -> None:
        try:
            contract = self.contract_resolver(req.symbol)
            action = SJAction.Buy if (req.side or "BUY").upper() == "BUY" else SJAction.Sell
            
            order = self.api.Order(
                price=req.price,
                quantity=req.quantity,
                action=action,
                price_type=SJPriceType.Limit,
                order_type=SJOrderType.ROD,
            )

            # Critical: timeout=0 for non-blocking
            trade = self.api.place_order(contract, order, timeout=0)

            # Optimistic update if trade object returned immediately
            if trade:
                broker_id = getattr(trade, "order_id", None) or getattr(trade, "id", None)
                if broker_id:
                    self.orders[req.internal_id].broker_id = str(broker_id)
                    self.orders[req.internal_id].status = "SUBMITTED"
                    logger.info("Order %s submitted, broker_id=%s", req.internal_id, broker_id)
                else:
                    # Async submission, ID might come later via callback
                    self.orders[req.internal_id].status = "SUBMITTED_ASYNC"

        except Exception as e:
            logger.error("Failed to place order %s: %s", req.internal_id, e)
            self.orders[req.internal_id].status = "FAILED"
            self.orders[req.internal_id].last_error = str(e)

    def _cancel_order(self, req: OrderRequest) -> None:
        # Find broker_id from internal_id
        state = self.orders.get(req.internal_id)
        if not state or not state.broker_id:
            logger.warning("Cannot cancel order %s: No broker_id found", req.internal_id)
            return

        try:
            self.api.cancel_order(self.account, state.broker_id, timeout=0)
            logger.info("Cancel sent for %s (broker_id=%s)", req.internal_id, state.broker_id)
        except Exception as e:
            logger.error("Failed to cancel order %s: %s", req.internal_id, e)

    def update_from_callback(self, broker_id: str, status: str, filled_qty: float, price: float) -> None:
        """Update order state from broker callback."""
        # Reverse lookup (inefficient, but simple for now)
        # In production, maintain broker_id -> internal_id map
        target_id = None
        for internal_id, state in self.orders.items():
            if state.broker_id == broker_id:
                target_id = internal_id
                break
        
        if target_id:
            state = self.orders[target_id]
            state.status = status
            state.filled_qty = filled_qty
            state.avg_price = price
            logger.debug("Updated order %s: %s", target_id, status)
