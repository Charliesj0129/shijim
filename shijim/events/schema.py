"""Broker-neutral market data event definitions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

AssetType = Literal["futures", "stock"]
EventTypeTick = Literal["MD_TICK"]
EventTypeBook = Literal["MD_BOOK"]


@dataclass(slots=True)
class BaseMDEvent:
    """Common envelope shared by every event traveling across the EventBus.

    Attributes:
        ts_ns: Nanoseconds since Unix epoch when the broker reported the event.
    """

    ts_ns: int
    symbol: str
    asset_type: AssetType
    exchange: str
    extras: dict[str, Any] = field(default_factory=dict)
    type: str = field(init=False, default="MD_EVENT")


@dataclass(slots=True)
class MDTickEvent(BaseMDEvent):
    """Normalized tick trades or top-of-book snapshots."""

    type: EventTypeTick = field(init=False, default="MD_TICK")
    price: float | None = None
    size: int | None = None
    side: Literal["buy", "sell", "none"] = "none"
    total_volume: int | None = None
    total_amount: float | None = None
    price_chg: float | None = None
    pct_chg: float | None = None


@dataclass(slots=True)
class MDBookEvent(BaseMDEvent):
    """Normalized level-2 order book snapshot (top 5 bid/ask)."""

    type: EventTypeBook = field(init=False, default="MD_BOOK")
    bid_prices: list[float] = field(default_factory=list)
    bid_volumes: list[int] = field(default_factory=list)
    ask_prices: list[float] = field(default_factory=list)
    ask_volumes: list[int] = field(default_factory=list)
    bid_total_vol: int | None = None
    ask_total_vol: int | None = None
    underlying_price: float | None = None


__all__ = [
    "AssetType",
    "BaseMDEvent",
    "MDBookEvent",
    "MDTickEvent",
]
