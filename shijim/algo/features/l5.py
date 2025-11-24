from __future__ import annotations

import math
from typing import Sequence

import numpy as np


def calc_depth_ratio(bid_sizes: Sequence[float], ask_sizes: Sequence[float]) -> float:
    bid_sum = float(np.sum(bid_sizes))
    ask_sum = float(np.sum(ask_sizes))
    if ask_sum == 0:
        return math.inf
    return bid_sum / ask_sum


def calc_micro_price(best_bid: float, best_ask: float, bid_size: float, ask_size: float) -> float:
    denom = bid_size + ask_size
    if denom == 0:
        return (best_bid + best_ask) / 2.0
    return (best_ask * bid_size + best_bid * ask_size) / denom


def calc_book_slope(prices: Sequence[float], cumulative_sizes: Sequence[float]) -> float:
    prices_arr = np.asarray(prices, dtype=float)
    cum = np.asarray(cumulative_sizes, dtype=float)
    if len(prices_arr) != len(cum) or len(prices_arr) < 2:
        raise ValueError("Need at least two levels for slope calculation")
    x = np.log(np.clip(cum, 1e-9, None))
    slope, _ = np.polyfit(x, prices_arr, deg=1)
    return float(slope)
