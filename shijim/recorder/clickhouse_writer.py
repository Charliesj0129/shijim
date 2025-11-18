"""Analytical writer scaffold for ClickHouse or similar columnar stores."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Sequence

from shijim.events.schema import MDBookEvent, MDTickEvent


@dataclass
class ClickHouseWriter:
    """Buffers normalized events and flushes them into analytical tables."""

    dsn: str
    client: Any | None = None
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))

    def write_batch(
        self,
        ticks: Sequence[MDTickEvent],
        books: Sequence[MDBookEvent],
    ) -> None:
        """Convert normalized events into ClickHouse-ready rows."""
        tick_rows = [self._tick_row(event) for event in ticks]
        book_rows = [self._book_row(event) for event in books]

        if not tick_rows and not book_rows:
            return

        self.logger.debug(
            "Prepared %s tick rows and %s order book rows for ClickHouse (dsn=%s).",
            len(tick_rows),
            len(book_rows),
            self.dsn,
        )
        # TODO: integrate with clickhouse-driver / HTTP client for actual inserts.
        if self.client is not None:
            self._send_to_clickhouse(tick_rows, table="ticks")
            self._send_to_clickhouse(book_rows, table="orderbook")

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _tick_row(self, event: MDTickEvent) -> dict[str, Any]:
        return {
            "ts": event.ts,
            "symbol": event.symbol,
            "asset_type": event.asset_type,
            "exchange": event.exchange,
            "price": event.price,
            "size": event.size,
            "side": event.side,
            "total_volume": event.total_volume,
            "total_amount": event.total_amount,
            "extras": event.extras,
        }

    def _book_row(self, event: MDBookEvent) -> dict[str, Any]:
        return {
            "ts": event.ts,
            "symbol": event.symbol,
            "asset_type": event.asset_type,
            "exchange": event.exchange,
            "bid_prices": event.bid_prices,
            "bid_volumes": event.bid_volumes,
            "ask_prices": event.ask_prices,
            "ask_volumes": event.ask_volumes,
            "bid_total_vol": event.bid_total_vol,
            "ask_total_vol": event.ask_total_vol,
            "underlying_price": event.underlying_price,
            "extras": event.extras,
        }

    def _send_to_clickhouse(self, rows: list[dict[str, Any]], *, table: str) -> None:
        if not rows:
            return
        if hasattr(self.client, "execute"):
            # Placeholder call; actual implementation would pass proper INSERT statements.
            self.client.execute(f"-- INSERT INTO {table}", rows)
