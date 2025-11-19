from __future__ import annotations

import types

from shijim.gateway.context import CollectorContext, attach_quote_callbacks
from shijim.events.schema import MDBookEvent, MDTickEvent


class FakeBus:
    def __init__(self) -> None:
        self.events: list[object] = []

    def publish(self, event) -> None:  # noqa: ANN001 - intentionally loose
        self.events.append(event)


def _tick_event(symbol: str, asset_type: str, exchange: str = "X") -> MDTickEvent:
    return MDTickEvent(
        ts_ns=1,
        symbol=symbol,
        asset_type=asset_type,
        exchange=exchange,
    )


def _book_event(symbol: str, asset_type: str, exchange: str = "X") -> MDBookEvent:
    return MDBookEvent(
        ts_ns=2,
        symbol=symbol,
        asset_type=asset_type,
        exchange=exchange,
    )


def test_collector_context_publishes_events() -> None:
    bus = FakeBus()

    ctx = CollectorContext(
        bus=bus,
        fut_tick_normalizer=lambda payload, exchange=None: _tick_event("TXF", "futures", exchange),  # type: ignore[arg-type]
        fut_book_normalizer=lambda payload, exchange=None: _book_event("TXF", "futures", exchange),  # type: ignore[arg-type]
        stk_tick_normalizer=lambda payload, exchange=None: _tick_event("2330", "stock", exchange),  # type: ignore[arg-type]
        stk_book_normalizer=lambda payload, exchange=None: _book_event("2330", "stock", exchange),  # type: ignore[arg-type]
    )

    ctx.on_fut_tick("TAIFEX", object())
    ctx.on_fut_book("TAIFEX", object())
    ctx.on_stk_tick("TWSE", object())
    ctx.on_stk_book("TWSE", object())

    assert [event.symbol for event in bus.events] == ["TXF", "TXF", "2330", "2330"]
    assert all(event.asset_type in {"futures", "stock"} for event in bus.events)


def test_attach_quote_callbacks_binds_handlers() -> None:
    bus = FakeBus()
    invoked: list[str] = []

    def make_normalizer(name: str, asset_type: str):
        def _normalize(payload, exchange=None):  # noqa: ANN001, ANN202
            invoked.append(name)
            event_cls = MDTickEvent if "tick" in name else MDBookEvent
            return event_cls(
                ts_ns=10,
                symbol=f"{name}-SYM",
                asset_type=asset_type,
                exchange=exchange or "X",
            )

        return _normalize

    ctx = CollectorContext(
        bus=bus,
        fut_tick_normalizer=make_normalizer("fut_tick", "futures"),
        fut_book_normalizer=make_normalizer("fut_book", "futures"),
        stk_tick_normalizer=make_normalizer("stk_tick", "stock"),
        stk_book_normalizer=make_normalizer("stk_book", "stock"),
    )

    class FakeAPI:
        def __init__(self) -> None:
            self.context = None
            self.callbacks: dict[str, types.FunctionType] = {}

        def set_context(self, ctx) -> None:  # noqa: ANN001
            self.context = ctx

        def _register(self, key: str):
            def decorator(func):
                self.callbacks[key] = func
                return func

            return decorator

        def on_tick_fop_v1(self, *, bind: bool):
            assert bind
            return self._register("fut_tick")

        def on_bidask_fop_v1(self, *, bind: bool):
            assert bind
            return self._register("fut_book")

        def on_tick_stk_v1(self, *, bind: bool):
            assert bind
            return self._register("stk_tick")

        def on_bidask_stk_v1(self, *, bind: bool):
            assert bind
            return self._register("stk_book")

    api = FakeAPI()
    attach_quote_callbacks(api, ctx)

    assert api.context is ctx
    assert set(api.callbacks) == {"fut_tick", "fut_book", "stk_tick", "stk_book"}

    api.callbacks["fut_tick"]("TAIFEX", object())
    api.callbacks["fut_book"]("TAIFEX", object())
    api.callbacks["stk_tick"]("TWSE", object())
    api.callbacks["stk_book"]("TWSE", object())

    assert len(bus.events) == 4
    assert invoked == ["fut_tick", "fut_book", "stk_tick", "stk_book"]
