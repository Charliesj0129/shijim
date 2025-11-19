"""Functions that convert raw Shioaji structures into broker-neutral events."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable

from shijim.events.schema import MDBookEvent, MDTickEvent

logger = logging.getLogger(__name__)


def normalize_tick_futures(tick: Any, exchange: Any) -> MDTickEvent:
    """Convert a `TickFOPv1` payload into an `MDTickEvent`."""
    return _normalize_tick(tick=tick, exchange=exchange, asset_type="futures")


def normalize_tick_stock(tick: Any, exchange: Any) -> MDTickEvent:
    """Convert a `TickSTKv1` payload into an `MDTickEvent`."""
    return _normalize_tick(tick=tick, exchange=exchange, asset_type="stock")


def normalize_book_futures(bidask: Any, exchange: Any) -> MDBookEvent:
    """Convert a futures `BidAsk` payload into an `MDBookEvent`."""
    return _normalize_book(bidask=bidask, exchange=exchange, asset_type="futures")


def normalize_book_stock(bidask: Any, exchange: Any) -> MDBookEvent:
    """Convert a stock `BidAsk` payload into an `MDBookEvent`."""
    return _normalize_book(bidask=bidask, exchange=exchange, asset_type="stock")


# --------------------------------------------------------------------------- #
# Internal helpers
# --------------------------------------------------------------------------- #
def _normalize_tick(*, tick: Any, exchange: Any, asset_type: str) -> MDTickEvent:
    ts_ns = _datetime_to_ns(getattr(tick, "datetime", None))
    symbol = getattr(tick, "code", "")
    price = _to_float(getattr(tick, "close", None))
    volume = _to_int(getattr(tick, "volume", None))
    total_volume = _to_int(getattr(tick, "total_volume", None))
    tick_type = getattr(tick, "tick_type", None)

    extras = {
        "open": _to_float(getattr(tick, "open", None)),
        "avg_price": _to_float(getattr(tick, "avg_price", None)),
        "high": _to_float(getattr(tick, "high", None)),
        "low": _to_float(getattr(tick, "low", None)),
        "amount": _to_float(getattr(tick, "amount", None)),
        "total_amount": _to_float(getattr(tick, "total_amount", None)),
        "volume": volume,
        "tick_type": tick_type,
        "price_chg": _to_float(getattr(tick, "price_chg", None)),
        "pct_chg": _to_float(getattr(tick, "pct_chg", None)),
        "bid_side_total_vol": _to_int(getattr(tick, "bid_side_total_vol", None)),
        "ask_side_total_vol": _to_int(getattr(tick, "ask_side_total_vol", None)),
        "bid_side_total_cnt": _to_int(getattr(tick, "bid_side_total_cnt", None)),
        "ask_side_total_cnt": _to_int(getattr(tick, "ask_side_total_cnt", None)),
        "underlying_price": _to_float(getattr(tick, "underlying_price", None)),
        "chg_type": getattr(tick, "chg_type", None),
        "suspend": getattr(tick, "suspend", None),
        "simtrade": getattr(tick, "simtrade", None),
        "intraday_odd": getattr(tick, "intraday_odd", None),
    }

    return MDTickEvent(
        ts_ns=ts_ns,
        symbol=symbol,
        asset_type=asset_type,
        exchange=_exchange_name(exchange),
        price=price,
        size=volume,
        side=_tick_type_to_side(tick_type),
        total_volume=total_volume,
        total_amount=extras["total_amount"],
        price_chg=extras["price_chg"],
        pct_chg=extras["pct_chg"],
        extras=extras,
    )


def _normalize_book(*, bidask: Any, exchange: Any, asset_type: str) -> MDBookEvent:
    ts_ns = _datetime_to_ns(getattr(bidask, "datetime", None))
    symbol = getattr(bidask, "code", "")
    bid_prices = _decimal_list_to_floats(getattr(bidask, "bid_price", []))
    ask_prices = _decimal_list_to_floats(getattr(bidask, "ask_price", []))
    bid_volumes = _int_list(getattr(bidask, "bid_volume", []))
    ask_volumes = _int_list(getattr(bidask, "ask_volume", []))

    extras = {
        "diff_bid_vol": _int_list(getattr(bidask, "diff_bid_vol", [])),
        "diff_ask_vol": _int_list(getattr(bidask, "diff_ask_vol", [])),
        "first_derived_bid_price": _to_float(getattr(bidask, "first_derived_bid_price", None)),
        "first_derived_ask_price": _to_float(getattr(bidask, "first_derived_ask_price", None)),
        "first_derived_bid_vol": _to_int(getattr(bidask, "first_derived_bid_vol", None)),
        "first_derived_ask_vol": _to_int(getattr(bidask, "first_derived_ask_vol", None)),
        "simtrade": getattr(bidask, "simtrade", None),
    }

    return MDBookEvent(
        ts_ns=ts_ns,
        symbol=symbol,
        asset_type=asset_type,
        exchange=_exchange_name(exchange),
        bid_prices=bid_prices,
        bid_volumes=bid_volumes,
        ask_prices=ask_prices,
        ask_volumes=ask_volumes,
        bid_total_vol=_to_int(getattr(bidask, "bid_total_vol", None)),
        ask_total_vol=_to_int(getattr(bidask, "ask_total_vol", None)),
        underlying_price=_to_float(getattr(bidask, "underlying_price", None)),
        extras=extras,
    )


def _datetime_to_ns(dt: datetime | None) -> int:
    if dt is None:
        return 0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000_000)


def _to_float(value: Any) -> float | None:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _to_int(value: Any) -> int | None:
    if isinstance(value, (int, float)):
        return int(value)
    return None


def _decimal_list_to_floats(values: Iterable[Any]) -> list[float]:
    result: list[float] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, Decimal):
            result.append(float(value))
        elif isinstance(value, (int, float)):
            result.append(float(value))
    return result


def _int_list(values: Iterable[Any]) -> list[int]:
    result: list[int] = []
    for value in values:
        if isinstance(value, (int, float)):
            result.append(int(value))
    return result


def _tick_type_to_side(value: int | None) -> str:
    if value == 1:
        return "buy"
    if value == 2:
        return "sell"
    return "none"


def _exchange_name(exchange: Any) -> str:
    if exchange is None:
        return ""
    return getattr(exchange, "value", None) or getattr(exchange, "name", "") or str(exchange)
