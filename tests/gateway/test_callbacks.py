from __future__ import annotations

from types import SimpleNamespace

import pytest

from shijim.gateway.callbacks import CallbackAdapter


class FakePublisher:
    def __init__(self) -> None:
        self.ticks = []
        self.books = []

    def publish_tick(self, event):
        self.ticks.append(event)

    def publish_book(self, event):
        self.books.append(event)


class FakeAPI:
    def __init__(self) -> None:
        self.registered = {}

    def _decorator(self, key):
        def register(func):
            self.registered[key] = func
            return func

        return register

    def on_tick_fop_v1(self):
        return self._decorator("fut_tick")

    def on_bidask_fop_v1(self):
        return self._decorator("fut_book")

    def on_tick_stk_v1(self):
        return self._decorator("stk_tick")

    def on_bidask_stk_v1(self):
        return self._decorator("stk_book")


@pytest.fixture()
def publisher():
    return FakePublisher()


def test_attach_registers_all_callbacks(publisher):
    api = FakeAPI()
    adapter = CallbackAdapter(api=api, event_publisher=publisher)
    adapter.attach()

    handlers = {
        "fut_tick": CallbackAdapter.on_fut_tick,
        "fut_book": CallbackAdapter.on_fut_book,
        "stk_tick": CallbackAdapter.on_stk_tick,
        "stk_book": CallbackAdapter.on_stk_book,
    }
    for key, func in handlers.items():
        registered = api.registered[key]
        assert registered.__self__ is adapter
        assert registered.__func__ is func


def test_fut_tick_callback_normalizes_and_publishes(monkeypatch, publisher):
    api = FakeAPI()
    adapter = CallbackAdapter(api=api, event_publisher=publisher)
    tick = SimpleNamespace(code="TXF")
    normalized = {"type": "MD_TICK"}

    def fake_normalizer(asset_type, exchange, raw):
        assert asset_type == "futures"
        assert raw is tick
        return normalized

    monkeypatch.setattr("shijim.gateway.callbacks.normalize_tick", fake_normalizer)
    adapter.on_fut_tick("TAIFEX", tick)

    assert publisher.ticks == [normalized]


def test_stock_book_callback_normalizes_and_publishes(monkeypatch, publisher):
    api = FakeAPI()
    adapter = CallbackAdapter(api=api, event_publisher=publisher)
    book = SimpleNamespace(code="2330")
    normalized = {"type": "MD_BOOK"}

    def fake_normalizer(asset_type, exchange, raw):
        assert asset_type == "stock"
        assert raw is book
        return normalized

    monkeypatch.setattr("shijim.gateway.callbacks.normalize_book", fake_normalizer)
    adapter.on_stk_book("TSE", book)

    assert publisher.books == [normalized]
