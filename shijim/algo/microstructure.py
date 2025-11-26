from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Sequence

import numpy as np


@dataclass
class OrderBookLevels:
    bid_prices: Sequence[float]
    bid_sizes: Sequence[float]
    ask_prices: Sequence[float]
    ask_sizes: Sequence[float]


@dataclass
class MlofiSnapshot:
    bids: List[float]
    asks: List[float]


@dataclass
class MlofiResult:
    vector: np.ndarray
    pca_level: float


@dataclass
class MlofiCalculator:
    depth: int = 5
    prev_bids: List[float] = field(default_factory=list)
    prev_asks: List[float] = field(default_factory=list)
    prev_bid_prices: List[float] = field(default_factory=list)
    prev_ask_prices: List[float] = field(default_factory=list)

    def _ensure_prev(self, levels: OrderBookLevels) -> None:
        if not self.prev_bids:
            self.prev_bids = list(levels.bid_sizes[: self.depth])
            self.prev_asks = list(levels.ask_sizes[: self.depth])
            self.prev_bid_prices = list(levels.bid_prices[: self.depth])
            self.prev_ask_prices = list(levels.ask_prices[: self.depth])

    def update(self, levels: OrderBookLevels) -> MlofiResult:
        self._ensure_prev(levels)
        bids = list(levels.bid_sizes[: self.depth])
        asks = list(levels.ask_sizes[: self.depth])
        prev_bid_prices = (
            list(self.prev_bid_prices)
            if self.prev_bid_prices
            else list(levels.bid_prices[: self.depth])
        )
        prev_ask_prices = (
            list(self.prev_ask_prices)
            if self.prev_ask_prices
            else list(levels.ask_prices[: self.depth])
        )

        bid_flow = np.zeros(self.depth, dtype=float)
        ask_flow = np.zeros(self.depth, dtype=float)

        for idx in range(self.depth):
            current = bids[idx]
            prev = self.prev_bids[idx]
            if levels.bid_prices[idx] > prev_bid_prices[idx]:
                bid_flow[idx] = current
            elif levels.bid_prices[idx] == prev_bid_prices[idx]:
                bid_flow[idx] = current - prev
            else:
                bid_flow[idx] = -prev

            curr_ask = asks[idx]
            prev_ask = self.prev_asks[idx]
            if levels.ask_prices[idx] < prev_ask_prices[idx]:
                ask_flow[idx] = curr_ask
            elif levels.ask_prices[idx] == prev_ask_prices[idx]:
                ask_flow[idx] = curr_ask - prev_ask
            else:
                ask_flow[idx] = -prev_ask

        vector = bid_flow - ask_flow
        self.prev_bids = bids
        self.prev_asks = asks
        self.prev_bid_prices = list(levels.bid_prices[: self.depth])
        self.prev_ask_prices = list(levels.ask_prices[: self.depth])

        pca_level = float(np.sum(vector))
        return MlofiResult(vector=vector, pca_level=pca_level)
