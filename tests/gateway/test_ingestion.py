from datetime import datetime

import pytest

from shijim.gateway.ingestion import ShioajiIngestor


class DummyWriter:
    pass


class Quote:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


def test_tick_ingestion():
    calls = {}

    def tick(writer, sec_id, price, size, ts):
        calls["tick"] = (writer, sec_id, price, size, ts)

    ingestor = ShioajiIngestor(
        DummyWriter(),
        {"2330": 1001},
        tick_publisher=tick,
        book_publisher=lambda *args, **kwargs: None,
        snapshot_publisher=lambda *args, **kwargs: None,
        system_publisher=lambda *args, **kwargs: None,
    )
    quote = Quote(code="2330", close=500.0, volume=5, simtrade=0, datetime="2023-01-01 09:00:01.123")
    ingestor.on_quote(quote)
    assert calls["tick"][1:4] == (1001, 500.0, 5)


def test_simtrade_filtered():
    tick_called = False

    def tick(*args, **kwargs):
        nonlocal tick_called
        tick_called = True

    ingestor = ShioajiIngestor(
        DummyWriter(),
        {"2330": 1001},
        tick_publisher=tick,
        book_publisher=lambda *args, **kwargs: None,
        snapshot_publisher=lambda *args, **kwargs: None,
        system_publisher=lambda *args, **kwargs: None,
    )
    quote = Quote(code="2330", close=500.0, volume=5, simtrade=1)
    ingestor.on_quote(quote)
    assert tick_called is False


def test_book_ingestion():
    orders = {}

    def book(writer, sec_id, bids, asks, ts):
        orders["book"] = (sec_id, bids, asks)

    ingestor = ShioajiIngestor(
        DummyWriter(),
        {"TXFL5": 2001},
        tick_publisher=lambda *args, **kwargs: None,
        book_publisher=book,
        snapshot_publisher=lambda *args, **kwargs: None,
        system_publisher=lambda *args, **kwargs: None,
    )
    quote = Quote(
        code="TXFL5",
        bid_price=[15000, 14999],
        bid_volume=[10, 20],
        ask_price=[15001, 15002],
        ask_volume=[5, 15],
    )
    ingestor.on_quote(quote)
    assert orders["book"][0] == 2001
    assert orders["book"][1] == [(15000.0, 10), (14999.0, 20)]
    assert orders["book"][2] == [(15001.0, 5), (15002.0, 15)]


def test_snapshot_ingestion():
    snapshot_called = {}

    def snapshot(writer, sec_id, close, high, open_px, ts):
        snapshot_called["snapshot"] = (sec_id, close, high, open_px)

    ingestor = ShioajiIngestor(
        DummyWriter(),
        {"2330": 1001},
        tick_publisher=lambda *args, **kwargs: None,
        book_publisher=lambda *args, **kwargs: None,
        snapshot_publisher=snapshot,
        system_publisher=lambda *args, **kwargs: None,
    )
    snap = Quote(close=500.0, high=505.0, open=495.0, datetime=datetime(2023, 1, 1, 9, 0, 1))
    ingestor.process_snapshot("2330", snap)
    assert snapshot_called["snapshot"] == (1001, 500.0, 505.0, 495.0)


def test_event_ingestion():
    system_called = {}

    def system(writer, code):
        system_called["code"] = code

    ingestor = ShioajiIngestor(
        DummyWriter(),
        {"2330": 1001},
        tick_publisher=lambda *args, **kwargs: None,
        book_publisher=lambda *args, **kwargs: None,
        snapshot_publisher=lambda *args, **kwargs: None,
        system_publisher=system,
    )
    event = Quote(code=13)
    ingestor.on_event(event)
    assert system_called["code"] == 13
