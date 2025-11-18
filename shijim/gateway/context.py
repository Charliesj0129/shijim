"""Collector context bound into Shioaji streaming callbacks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from shijim.bus import EventBus
from shijim.events.schema import BaseMDEvent, MDBookEvent, MDTickEvent


class Normalizer(Protocol):
    """Protocol for functions that convert broker payloads to event objects."""

    def __call__(self, payload: Any, exchange: Any = None) -> MDTickEvent | MDBookEvent:
        """Return a normalized market-data event."""


@dataclass(slots=True)
class AssetRouting:
    """Maps futures/stock handlers to the expected `asset_type` values."""

    futures_asset_type: str = "futures"
    stock_asset_type: str = "stock"


@dataclass
class CollectorContext:
    """Bridges Shioaji callbacks with our EventBus using broker-neutral schemas."""

    bus: EventBus
    fut_tick_normalizer: Normalizer
    fut_book_normalizer: Normalizer
    stk_tick_normalizer: Normalizer
    stk_book_normalizer: Normalizer
    asset_routing: AssetRouting = field(default_factory=AssetRouting)

    def on_fut_tick(self, exchange: Any, tick: Any) -> None:
        """Normalize futures tick payloads and forward them to the bus."""
        event = self.fut_tick_normalizer(tick, exchange=exchange)
        self._publish(event, self.asset_routing.futures_asset_type)

    def on_fut_book(self, exchange: Any, bidask: Any) -> None:
        """Handle futures BidAsk snapshots (top 5 book levels)."""
        event = self.fut_book_normalizer(bidask, exchange=exchange)
        self._publish(event, self.asset_routing.futures_asset_type)

    def on_stk_tick(self, exchange: Any, tick: Any) -> None:
        """Normalize stock ticks, ensuring outputs follow the MD_TICK schema."""
        event = self.stk_tick_normalizer(tick, exchange=exchange)
        self._publish(event, self.asset_routing.stock_asset_type)

    def on_stk_book(self, exchange: Any, bidask: Any) -> None:
        """Process stock BidAsk data in a non-blocking fashion."""
        event = self.stk_book_normalizer(bidask, exchange=exchange)
        self._publish(event, self.asset_routing.stock_asset_type)

    def _publish(self, event: MDTickEvent | MDBookEvent, expected_asset_type: str) -> None:
        if not isinstance(event, BaseMDEvent):
            raise TypeError("Normalizers must return BaseMDEvent instances.")
        if event.asset_type != expected_asset_type:
            raise ValueError(
                f"Normalizer returned asset_type={event.asset_type!r}; "
                f"expected {expected_asset_type!r}."
            )
        self.bus.publish(event)


def attach_quote_callbacks(api: Any, ctx: CollectorContext) -> None:
    """Bind Shioaji quote callbacks to the collector context."""

    api.set_context(ctx)

    @api.on_tick_fop_v1(bind=True)
    def _on_fut_tick(exchange: Any, tick: Any) -> None:
        ctx.on_fut_tick(exchange, tick)

    @api.on_bidask_fop_v1(bind=True)
    def _on_fut_book(exchange: Any, bidask: Any) -> None:
        ctx.on_fut_book(exchange, bidask)

    @api.on_tick_stk_v1(bind=True)
    def _on_stk_tick(exchange: Any, tick: Any) -> None:
        ctx.on_stk_tick(exchange, tick)

    @api.on_bidask_stk_v1(bind=True)
    def _on_stk_book(exchange: Any, bidask: Any) -> None:
        ctx.on_stk_book(exchange, bidask)
