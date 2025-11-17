"""Normalization helpers converting Shioaji structures into schema events."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Iterable, Mapping, MutableMapping, Sequence

from .schema import MDBookEvent, MDTickEvent


def normalize_tick(tick: Any, asset_type: str, exchange: Any | None = None) -> MDTickEvent:
    """Convert a Shioaji Tick object into a :class:`MDTickEvent`."""
    symbol = _extract_symbol(tick)
    exchange_str = _stringify_exchange(exchange or getattr(tick, "exchange", None))
    ts_ns = _datetime_to_ns(getattr(tick, "datetime", None))

    event = MDTickEvent(
        ts=ts_ns,
        symbol=symbol,
        asset_type=asset_type,
        exchange=exchange_str,
        price=_to_float(_first_non_none(getattr(tick, attr, None) for attr in ("close", "price"))),
        size=_to_float(getattr(tick, "volume", None)),
        side=_infer_side(getattr(tick, "tick_type", None)),
        total_volume=_to_float(getattr(tick, "total_volume", None)),
        total_amount=_to_float(getattr(tick, "total_amount", None)),
        bid_total_vol=_to_float(
            _first_non_none(
                getattr(tick, attr, None) for attr in ("bid_total_vol", "bid_side_total_vol")
            )
        ),
        ask_total_vol=_to_float(
            _first_non_none(
                getattr(tick, attr, None) for attr in ("ask_total_vol", "ask_side_total_vol")
            )
        ),
        extras=_extract_extras(
            tick,
            exclude={
                "code",
                "symbol",
                "exchange",
                "datetime",
                "close",
                "price",
                "volume",
                "tick_type",
                "total_volume",
                "total_amount",
                "bid_total_vol",
                "bid_side_total_vol",
                "ask_total_vol",
                "ask_side_total_vol",
            },
        ),
    )
    return event


def normalize_book(bidask: Any, asset_type: str, exchange: Any | None = None) -> MDBookEvent:
    """Convert a BidAsk object into an :class:`MDBookEvent`."""
    symbol = _extract_symbol(bidask)
    exchange_str = _stringify_exchange(exchange or getattr(bidask, "exchange", None))
    ts_ns = _datetime_to_ns(getattr(bidask, "datetime", None))

    bid_prices = _to_float_list(getattr(bidask, "bid_price", []))
    ask_prices = _to_float_list(getattr(bidask, "ask_price", []))
    bid_volumes = _to_int_list(getattr(bidask, "bid_volume", []))
    ask_volumes = _to_int_list(getattr(bidask, "ask_volume", []))

    event = MDBookEvent(
        ts=ts_ns,
        symbol=symbol,
        asset_type=asset_type,
        exchange=exchange_str,
        bid_prices=bid_prices,
        bid_volumes=bid_volumes,
        ask_prices=ask_prices,
        ask_volumes=ask_volumes,
        bid_total_vol=_to_float(
            _first_non_none(
                getattr(bidask, attr, None) for attr in ("bid_total_vol", "bid_side_total_vol")
            )
        ),
        ask_total_vol=_to_float(
            _first_non_none(
                getattr(bidask, attr, None) for attr in ("ask_total_vol", "ask_side_total_vol")
            )
        ),
        underlying_price=_to_float(getattr(bidask, "underlying_price", None)),
        extras=_extract_extras(
            bidask,
            exclude={
                "code",
                "symbol",
                "exchange",
                "datetime",
                "bid_price",
                "ask_price",
                "bid_volume",
                "ask_volume",
                "bid_total_vol",
                "ask_total_vol",
                "bid_side_total_vol",
                "ask_side_total_vol",
                "underlying_price",
            },
        ),
    )
    return event


# --------------------------------------------------------------------------- #
# Helper functions
# --------------------------------------------------------------------------- #
def _extract_symbol(obj: Any) -> str:
    for attr in ("code", "symbol"):
        value = getattr(obj, attr, None)
        if isinstance(value, str) and value:
            return value
    return ""


def _stringify_exchange(exchange: Any) -> str:
    if exchange is None:
        return ""
    value = getattr(exchange, "value", exchange)
    return str(value)


def _datetime_to_ns(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return int(value.timestamp() * 1_000_000_000)
    return None


def _to_float(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    return value


def _to_float_list(seq: Sequence[Any]) -> list[float]:
    result: list[float] = []
    for item in seq:
        if isinstance(item, Decimal):
            result.append(float(item))
        elif isinstance(item, (int, float)):
            result.append(float(item))
        elif item is None:
            result.append(0.0)
        else:
            try:
                result.append(float(item))  # type: ignore[arg-type]
            except Exception:
                result.append(0.0)
    return result


def _to_int_list(seq: Sequence[Any]) -> list[int]:
    result: list[int] = []
    for item in seq:
        if item is None:
            result.append(0)
        else:
            try:
                result.append(int(item))  # type: ignore[arg-type]
            except Exception:
                result.append(0)
    return result


def _infer_side(tick_type: Any) -> str | None:
    if tick_type is None:
        return None
    mapping = {1: "buy", 2: "sell", 0: "na"}
    return mapping.get(tick_type) if isinstance(tick_type, int) else str(tick_type)


def _extract_extras(obj: Any, exclude: set[str]) -> MutableMapping[str, Any]:
    extras: MutableMapping[str, Any] = {}
    attributes = {}

    if hasattr(obj, "__dict__"):
        attributes.update(getattr(obj, "__dict__"))
    else:
        for name in dir(obj):
            if name.startswith("_"):
                continue
            attributes[name] = getattr(obj, name, None)

    for key, value in attributes.items():
        if key in exclude:
            continue
        extras[key] = _sanitize(value)

    return extras


def _sanitize(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, datetime):
        return _datetime_to_ns(value)
    if isinstance(value, Mapping):
        return {k: _sanitize(v) for k, v in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_sanitize(item) for item in value]
    return value


def _first_non_none(values: Iterable[Any]) -> Any:
    for value in values:
        if value is not None:
            return value
    return None
