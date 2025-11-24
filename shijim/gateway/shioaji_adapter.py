from __future__ import annotations

from dataclasses import dataclass
from queue import SimpleQueue
from typing import Any, Dict, Optional, Protocol, Sequence

from shijim.strategy.engine import OrderRequest, OrderRequestAction

try:  # pragma: no cover
    import shioaji as sj  # type: ignore

    SJAction = getattr(sj.constant, "Action", None)
    SJPriceType = getattr(sj.constant, "PriceType", None)
    SJOrderType = getattr(sj.constant, "OrderType", None)
except ImportError:  # pragma: no cover
    SJAction = None
    SJPriceType = None
    SJOrderType = None

if SJAction is None:  # pragma: no cover
    class SJAction:
        Buy = "Buy"
        Sell = "Sell"

if SJPriceType is None:  # pragma: no cover
    class SJPriceType:
        Limit = "Limit"

if SJOrderType is None:  # pragma: no cover
    class SJOrderType:
        ROD = "ROD"


class ShioajiClientProtocol(Protocol):
    def place_order(self, contract: Any, order: Any) -> Any: ...

    def cancel_order(self, account: Any, order: Any) -> None: ...

    def Order(self, *args, **kwargs) -> Any: ...


class ContractResolver(Protocol):
    def __call__(self, symbol: str) -> Any: ...


@dataclass
class AdapterResponse:
    internal_id: Optional[str]
    broker_order_id: Optional[str]
    status: str


@dataclass
class ExecutionReport:
    broker_order_id: str
    status: str
    filled_qty: float
    filled_price: float


class ShioajiAdapter:
    def __init__(
        self,
        api: ShioajiClientProtocol,
        contract_resolver: ContractResolver,
        event_queue: SimpleQueue,
        account: Optional[Any] = None,
    ) -> None:
        self.api = api
        self.contract_resolver = contract_resolver
        self.event_queue = event_queue
        self.account = account
        self.order_mapping: Dict[str, Any] = {}

    def send(self, requests: Sequence[OrderRequest]) -> Sequence[AdapterResponse]:
        responses: list[AdapterResponse] = []
        for req in requests:
            if req.action == OrderRequestAction.CANCEL:
                self._cancel_request(req)
                responses.append(
                    AdapterResponse(req.internal_id, req.broker_order_id, "CancelSent")
                )
            elif req.action == OrderRequestAction.CANCEL_REPLACE:
                broker_id = self._cancel_request(req)
                response = self._place_request(req)
                if req.internal_id:
                    self.order_mapping[req.internal_id] = response.broker_order_id
                responses.append(response)
            else:
                response = self._place_request(req)
                if req.internal_id:
                    self.order_mapping[req.internal_id] = response.broker_order_id
                responses.append(response)
        return responses

    def _place_request(self, req: OrderRequest) -> AdapterResponse:
        try:
            contract = self.contract_resolver(req.symbol or "")
            order = self.api.Order(
                price=req.price,
                quantity=req.quantity,
                action=SJAction.Buy if (req.side or "BUY").upper() == "BUY" else SJAction.Sell,
                price_type=SJPriceType.Limit,
                order_type=SJOrderType.ROD,
            )
            trade = self.api.place_order(contract, order)
            broker_id = getattr(trade, "order_id", None) or getattr(trade, "id", None)
            if req.internal_id and broker_id:
                self.order_mapping[req.internal_id] = broker_id
            return AdapterResponse(req.internal_id, broker_id, "OrderSent")
        except Exception as exc:  # pragma: no cover - executed in tests
            self._enqueue_rejection(req, str(exc))
            return AdapterResponse(req.internal_id, None, "Rejected")

    def _cancel_request(self, req: OrderRequest) -> Optional[str]:
        broker_id = req.broker_order_id
        if req.internal_id and not broker_id:
            broker_id = self.order_mapping.get(req.internal_id)
        if not broker_id:
            return None
        try:
            self.api.cancel_order(self.account, broker_id)
        except Exception as exc:  # pragma: no cover
            self._enqueue_rejection(req, f"CancelFailed: {exc}")
        return broker_id

    def register_callback(self, event: Any) -> None:
        broker_id = getattr(event, "order_id", None) or getattr(event, "id", "")
        status = getattr(event, "status", "")
        filled_qty = float(getattr(event, "deal_quantity", 0.0) or getattr(event, "quantity", 0.0))
        filled_price = float(getattr(event, "deal_price", 0.0) or getattr(event, "price", 0.0))
        report = ExecutionReport(
            broker_order_id=str(broker_id),
            status=status,
            filled_qty=filled_qty,
            filled_price=filled_price,
        )
        self.event_queue.put(report)

    def _enqueue_rejection(self, req: OrderRequest, reason: str) -> None:
        event = {
            "type": "OrderRejected",
            "internal_id": req.internal_id,
            "reason": reason,
        }
        self.event_queue.put(event)
