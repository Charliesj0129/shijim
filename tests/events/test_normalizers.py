from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace

from shijim.events.normalizers import normalize_book, normalize_tick


class DummyTick:
    def __init__(self):
        self.code = "TXFG1"
        self.datetime = datetime(2023, 1, 1, 0, 0, 0)
        self.close = Decimal("12345.6")
        self.volume = 2
        self.tick_type = 1
        self.total_volume = 10
        self.total_amount = Decimal("123456.7")
        self.bid_side_total_vol = 5
        self.ask_side_total_vol = 6
        self.note = "extra"


class DummyBook:
    def __init__(self):
        self.code = "2330"
        self.datetime = datetime(2023, 1, 1, 0, 0, 1)
        self.bid_price = [Decimal("100.5"), Decimal("100.0")]
        self.ask_price = [Decimal("101.0"), Decimal("101.5"), Decimal("102.0")]
        self.bid_volume = [10, None, 5]
        self.ask_volume = [8, 6, 4, None]
        self.bid_total_vol = 15
        self.ask_total_vol = 25
        self.underlying_price = Decimal("999.9")
        self.diff_bid_vol = [1, 2, 3]


def test_normalize_tick_converts_decimal_and_adds_extras():
    tick = DummyTick()
    exchange = SimpleNamespace(value="TAIFEX")
    event = normalize_tick(tick, asset_type="futures", exchange=exchange)

    assert event.type == "MD_TICK"
    assert event.symbol == "TXFG1"
    assert event.asset_type == "futures"
    assert event.exchange == "TAIFEX"
    assert event.price == 12345.6
    assert event.size == 2
    assert event.side == "buy"
    assert event.total_volume == 10
    assert event.total_amount == 123456.7
    assert event.bid_total_vol == 5
    assert event.ask_total_vol == 6
    assert event.extras["note"] == "extra"
    assert event.ts == int(tick.datetime.timestamp() * 1_000_000_000)


def test_normalize_book_handles_partial_levels_and_underlying_price():
    book = DummyBook()
    event = normalize_book(book, asset_type="stock", exchange="TSE")

    assert event.type == "MD_BOOK"
    assert event.symbol == "2330"
    assert event.exchange == "TSE"
    assert event.bid_prices[:2] == [100.5, 100.0]
    assert event.ask_prices[:3] == [101.0, 101.5, 102.0]
    assert event.bid_volumes == [10, 0, 5]
    assert event.ask_volumes[:3] == [8, 6, 4]
    assert event.bid_total_vol == 15
    assert event.ask_total_vol == 25
    assert event.underlying_price == 999.9
    assert event.extras["diff_bid_vol"] == [1, 2, 3]
    assert event.ts == int(book.datetime.timestamp() * 1_000_000_000)
