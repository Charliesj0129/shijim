"""Adapter bridging Shioaji callbacks to the internal event bus.

Reference docs (``sinotrade_tutor_md``):
    * ``advanced/quote_binding.md`` – describes the ``@api.on_*`` decorators.
    * ``advanced/nonblock.md`` – warns against performing blocking work inside callbacks.
    * ``advanced/quote_manager_basic.md`` – demonstrates lightweight handlers and
      pushing events onto queues for downstream processing.
"""

from __future__ import annotations

import logging
from typing import Any

from shijim.events import normalize_book, normalize_tick

logger = logging.getLogger(__name__)


class CallbackAdapter:
    """Registers Shioaji callbacks and forwards normalized events to a publisher."""

    def __init__(self, api: Any, event_publisher: Any) -> None:
        self._api = api
        self._publisher = event_publisher

    def attach(self) -> None:
        """Register the adapter handlers with the Shioaji API."""
        self._api.on_tick_fop_v1()(self.on_fut_tick)
        self._api.on_bidask_fop_v1()(self.on_fut_book)
        self._api.on_tick_stk_v1()(self.on_stk_tick)
        self._api.on_bidask_stk_v1()(self.on_stk_book)
        logger.info("CallbackAdapter attached to Shioaji API.")

    # ------------------------------------------------------------------ #
    # Callbacks (futures)
    # ------------------------------------------------------------------ #
    def on_fut_tick(self, exchange: Any, tick: Any) -> None:
        event = normalize_tick("futures", exchange, tick)
        self._publisher.publish_tick(event)

    def on_fut_book(self, exchange: Any, bidask: Any) -> None:
        event = normalize_book("futures", exchange, bidask)
        self._publisher.publish_book(event)

    # ------------------------------------------------------------------ #
    # Callbacks (stocks)
    # ------------------------------------------------------------------ #
    def on_stk_tick(self, exchange: Any, tick: Any) -> None:
        event = normalize_tick("stock", exchange, tick)
        self._publisher.publish_tick(event)

    def on_stk_book(self, exchange: Any, bidask: Any) -> None:
        event = normalize_book("stock", exchange, bidask)
        self._publisher.publish_book(event)
