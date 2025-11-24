from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol, Sequence

from shijim.strategy.engine import SmartChasingEngine
from shijim.strategy.ofi import BboState, OfiCalculator


class HftFeed(Protocol):
    def __iter__(self) -> Iterable[dict]: ...


class HbtExecutor(Protocol):
    def submit_buy_order(self, price: float, qty: float) -> None: ...

    def cancel_order(self, broker_order_id: str | None = None) -> None: ...


@dataclass
class HftBacktestAdapter:
    engine: SmartChasingEngine
    ofi: OfiCalculator
    executor: HbtExecutor

    def run(self, feed: Iterable[dict]) -> None:
        for tick in feed:
            bbo = BboState(
                bid_price=tick.get("bid_price", tick["price"]),
                bid_size=tick.get("bid_size", tick["qty"]),
                ask_price=tick.get("ask_price", tick["price"]),
                ask_size=tick.get("ask_size", tick["qty"]),
            )
            ofi_result = self.ofi.process_tick(
                bbo.bid_price, bbo.bid_size, bbo.ask_price, bbo.ask_size
            )
            actions = self.engine.on_tick(bbo, ofi_override=ofi_result.net_ofi)
            self._dispatch(actions)

    def _dispatch(self, actions: Sequence) -> None:
        for action in actions:
            reason = getattr(action, "reason", "")
            if reason and "CancelReplace" in reason:
                self.executor.cancel_order(action.broker_order_id)
            if getattr(action, "price", None):
                self.executor.submit_buy_order(action.price, action.quantity)
