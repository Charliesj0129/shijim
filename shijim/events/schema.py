"""Broker-neutral market data event definitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class MDTickEvent:
    """Normalized trade event emitted by the gateway."""

    ts: Optional[int]
    symbol: str
    asset_type: str
    exchange: str
    price: Optional[float] = None
    size: Optional[float] = None
    side: Optional[str] = None
    total_volume: Optional[float] = None
    total_amount: Optional[float] = None
    bid_total_vol: Optional[float] = None
    ask_total_vol: Optional[float] = None
    extras: Dict[str, Any] = field(default_factory=dict)
    type: str = field(init=False, default="MD_TICK")


@dataclass(frozen=True)
class MDBookEvent:
    """Normalized order-book snapshot event (5 levels)."""

    ts: Optional[int]
    symbol: str
    asset_type: str
    exchange: str
    bid_prices: List[float] = field(default_factory=list)
    bid_volumes: List[int] = field(default_factory=list)
    ask_prices: List[float] = field(default_factory=list)
    ask_volumes: List[int] = field(default_factory=list)
    bid_total_vol: Optional[float] = None
    ask_total_vol: Optional[float] = None
    underlying_price: Optional[float] = None
    extras: Dict[str, Any] = field(default_factory=dict)
    type: str = field(init=False, default="MD_BOOK")
