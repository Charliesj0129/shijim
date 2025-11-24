from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class BboState:
    bid_price: float
    bid_size: float
    ask_price: float
    ask_size: float


@dataclass(frozen=True)
class OfiComputation:
    bid_imbalance: float
    ask_imbalance: float
    net_ofi: float
    state: BboState


class OfiCalculator:
    """
    Calculates Order Flow Imbalance given the latest BBO snapshot.
    The calculator keeps the previous BBO state and applies the
    bid/ask side rules described in the OFI definition.
    """

    def __init__(self, initial_state: Optional[BboState] = None) -> None:
        self._prev_state: Optional[BboState] = initial_state

    @property
    def prev_state(self) -> Optional[BboState]:
        return self._prev_state

    def reset(self) -> None:
        self._prev_state = None

    def process_tick(
        self,
        bid_price: float,
        bid_size: float,
        ask_price: float,
        ask_size: float,
    ) -> OfiComputation:
        current = BboState(bid_price, bid_size, ask_price, ask_size)

        if self._prev_state is None:
            # Cold start, publish neutral signal and seed the state.
            self._prev_state = current
            return OfiComputation(0.0, 0.0, 0.0, current)

        prev = self._prev_state
        bid_imbalance = self._bid_imbalance(prev, current)
        ask_imbalance = self._ask_imbalance(prev, current)
        net = bid_imbalance - ask_imbalance
        self._prev_state = current
        return OfiComputation(bid_imbalance, ask_imbalance, net, current)

    @staticmethod
    def _bid_imbalance(prev: BboState, curr: BboState) -> float:
        if curr.bid_price > prev.bid_price:
            return curr.bid_size
        if curr.bid_price == prev.bid_price:
            return curr.bid_size - prev.bid_size
        return -prev.bid_size

    @staticmethod
    def _ask_imbalance(prev: BboState, curr: BboState) -> float:
        if curr.ask_price < prev.ask_price:
            return curr.ask_size
        if curr.ask_price == prev.ask_price:
            return curr.ask_size - prev.ask_size
        return -prev.ask_size
