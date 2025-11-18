"""Functions that convert raw Shioaji structures into broker-neutral events."""

from __future__ import annotations

from typing import Any

from shijim.events.schema import MDBookEvent, MDTickEvent


def normalize_tick_futures(tick: Any, exchange: Any) -> MDTickEvent:
    """Convert a `TickFOPv1` payload into an `MDTickEvent`."""
    raise NotImplementedError("Futures tick normalization placeholder.")


def normalize_tick_stock(tick: Any, exchange: Any) -> MDTickEvent:
    """Convert a `TickSTKv1` payload into an `MDTickEvent`."""
    raise NotImplementedError("Stock tick normalization placeholder.")


def normalize_book_futures(bidask: Any, exchange: Any) -> MDBookEvent:
    """Convert a futures `BidAsk` payload into an `MDBookEvent`."""
    raise NotImplementedError("Futures order book normalization placeholder.")


def normalize_book_stock(bidask: Any, exchange: Any) -> MDBookEvent:
    """Convert a stock `BidAsk` payload into an `MDBookEvent`."""
    raise NotImplementedError("Stock order book normalization placeholder.")
