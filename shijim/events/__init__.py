"""Broker-neutral event schema and normalization helpers."""

from __future__ import annotations

from .normalizers import (
    normalize_book_futures,
    normalize_book_stock,
    normalize_tick_futures,
    normalize_tick_stock,
)
from .schema import MDBookEvent, MDTickEvent


def normalize_tick(asset_type_or_tick, exchange=None, tick=None, *, asset_type=None):
    """Backward-compatible helper choosing the proper type-specific normalizer."""

    if isinstance(asset_type_or_tick, str):
        asset = asset_type_or_tick
        payload = tick
        exch = exchange
    else:
        payload = asset_type_or_tick
        asset = asset_type
        exch = exchange

    if asset is None or payload is None:
        raise ValueError("asset_type and tick payload must be provided")

    asset_key = asset.lower()
    if asset_key in {"futures", "future", "fop"}:
        return normalize_tick_futures(payload, exchange=exch)
    if asset_key in {"stock", "stocks"}:
        return normalize_tick_stock(payload, exchange=exch)
    raise ValueError(f"Unsupported asset_type {asset}")


def normalize_book(asset_type_or_book, exchange=None, book=None, *, asset_type=None):
    """Backward-compatible helper choosing the proper type-specific normalizer."""

    if isinstance(asset_type_or_book, str):
        asset = asset_type_or_book
        payload = book
        exch = exchange
    else:
        payload = asset_type_or_book
        asset = asset_type
        exch = exchange

    if asset is None or payload is None:
        raise ValueError("asset_type and book payload must be provided")

    asset_key = asset.lower()
    if asset_key in {"futures", "future", "fop"}:
        return normalize_book_futures(payload, exchange=exch)
    if asset_key in {"stock", "stocks"}:
        return normalize_book_stock(payload, exchange=exch)
    raise ValueError(f"Unsupported asset_type {asset}")


__all__ = [
    "MDBookEvent",
    "MDTickEvent",
    "normalize_book",
    "normalize_book_futures",
    "normalize_book_stock",
    "normalize_tick",
    "normalize_tick_futures",
    "normalize_tick_stock",
]
