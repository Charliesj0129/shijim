from __future__ import annotations

from typing import Iterable

from shijim.events.schema import BaseMDEvent, MDTickEvent
from shijim.gateway.context import CollectorContext


class FakeBus:
    def __init__(self) -> None:
        self.events: list[BaseMDEvent] = []

    def publish(self, event: BaseMDEvent) -> None:
        self.events.append(event)

    def publish_many(self, events: Iterable[BaseMDEvent]) -> None:
        self.events.extend(events)


def _tick_event(symbol: str, asset_type: str) -> MDTickEvent:
    return MDTickEvent(
        ts_ns=1,
        symbol=symbol,
        asset_type=asset_type,
        exchange="TAIFEX",
    )


def test_collector_context_publish_many() -> None:
    bus = FakeBus()

    ctx = CollectorContext(
        bus=bus,
        fut_tick_normalizer=lambda p, e=None: _tick_event("TXF", "futures"),
        fut_book_normalizer=lambda p, e=None: _tick_event("TXF", "futures"), # Dummy
        stk_tick_normalizer=lambda p, e=None: _tick_event("2330", "stock"),
        stk_book_normalizer=lambda p, e=None: _tick_event("2330", "stock"), # Dummy
    )

    events = [
        _tick_event("TXF1", "futures"),
        _tick_event("TXF2", "futures"),
        _tick_event("2330", "stock"), # Should be filtered out if we expect futures
    ]

    # Test futures
    ctx.publish_many(events, "futures")

    assert len(bus.events) == 2
    assert bus.events[0].symbol == "TXF1"
    assert bus.events[1].symbol == "TXF2"

    # Test stocks
    bus.events.clear()
    ctx.publish_many(events, "stock")

    assert len(bus.events) == 1
    assert bus.events[0].symbol == "2330"
