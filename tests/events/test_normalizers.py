from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from shijim.events.normalizers import (
    normalize_book_futures,
    normalize_book_stock,
    normalize_tick_futures,
    normalize_tick_stock,
)


class DummyExchange:
    def __init__(self, value: str) -> None:
        self.value = value


class TickDummy:
    def __init__(self, code: str) -> None:
        self.code = code
        self.datetime = datetime(2024, 1, 1, 9, 0, 0)
        self.close = Decimal("150.5")
        self.volume = 10
        self.total_volume = 100
        self.tick_type = 1
        self.price_chg = Decimal("0.5")
        self.pct_chg = Decimal("0.33")
        self.bid_side_total_vol = 50
        self.ask_side_total_vol = 60


class BidAskDummy:
    def __init__(self, code: str) -> None:
        self.code = code
        self.datetime = datetime(2024, 1, 1, 9, 0, 0)
        self.bid_price = [Decimal("150.0"), Decimal("149.5")]
        self.ask_price = [Decimal("151.0"), Decimal("151.5")]
        self.bid_volume = [5, 4]
        self.ask_volume = [3, 2]
        self.bid_total_vol = 9
        self.ask_total_vol = 5


def test_normalize_tick_stock_fields() -> None:
    tick = TickDummy("2330")
    event = normalize_tick_stock(tick, DummyExchange("TSE"))

    assert event.symbol == "2330"
    assert event.asset_type == "stock"
    assert event.exchange == "TSE"
    assert event.price == pytest.approx(150.5)
    assert event.size == 10
    assert event.side == "buy"
    assert event.total_volume == 100
    assert event.price_chg == pytest.approx(0.5)
    assert event.pct_chg == pytest.approx(0.33)
    assert event.ts > 0


def test_normalize_tick_futures_sets_asset_type() -> None:
    tick = TickDummy("TXF202401")
    event = normalize_tick_futures(tick, DummyExchange("TAIFEX"))

    assert event.asset_type == "futures"
    assert event.exchange == "TAIFEX"


def test_normalize_book_stock_levels() -> None:
    ba = BidAskDummy("2330")
    event = normalize_book_stock(ba, DummyExchange("TSE"))

    assert event.asset_type == "stock"
    assert event.bid_prices == [150.0, 149.5]
    assert event.ask_prices == [151.0, 151.5]
    assert event.bid_volumes == [5, 4]
    assert event.ask_volumes == [3, 2]
    assert event.bid_total_vol == 9
    assert event.ask_total_vol == 5


def test_normalize_book_futures_inherits_stock_logic() -> None:
    ba = BidAskDummy("TXF")
    event = normalize_book_futures(ba, DummyExchange("TAIFEX"))

    assert event.asset_type == "futures"
    assert event.exchange == "TAIFEX"
