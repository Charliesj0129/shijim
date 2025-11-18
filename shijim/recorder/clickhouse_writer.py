"""Analytical writer for ClickHouse or similar columnar stores."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, List, Sequence

import orjson

from shijim.events.schema import MDBookEvent, MDTickEvent


@dataclass
class ClickHouseWriter:
    """Buffers normalized events and flushes them into analytical tables."""

    dsn: str
    client: Any | None = None
    flush_threshold: int = 1_000
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))
    _tick_buffer: list[MDTickEvent] = field(default_factory=list, init=False)
    _book_buffer: list[MDBookEvent] = field(default_factory=list, init=False)

    _tick_columns: Sequence[str] = (
        "trading_day",
        "ts",
        "symbol",
        "asset_type",
        "exchange",
        "price",
        "size",
        "side",
        "total_volume",
        "total_amount",
        "extras",
    )
    _book_columns: Sequence[str] = (
        "trading_day",
        "ts",
        "symbol",
        "asset_type",
        "exchange",
        "bid_prices",
        "bid_volumes",
        "ask_prices",
        "ask_volumes",
        "bid_total_vol",
        "ask_total_vol",
        "underlying_price",
        "extras",
    )

    def write_batch(
        self,
        ticks: Sequence[MDTickEvent],
        books: Sequence[MDBookEvent],
    ) -> None:
        """Buffer events and flush when thresholds are met."""
        if ticks:
            self._tick_buffer.extend(ticks)
        if books:
            self._book_buffer.extend(books)
        self.flush(force=False)

    def flush(self, force: bool = True) -> None:
        """Flush buffered events to ClickHouse."""
        if force or len(self._tick_buffer) >= self.flush_threshold:
            self._flush_ticks()
        if force or len(self._book_buffer) >= self.flush_threshold:
            self._flush_books()

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _flush_ticks(self) -> None:
        if not self._tick_buffer:
            return
        rows = [self._tick_row(event) for event in self._tick_buffer]
        self._tick_buffer.clear()
        self._send_to_clickhouse(rows, table="ticks", columns=self._tick_columns)

    def _flush_books(self) -> None:
        if not self._book_buffer:
            return
        rows = [self._book_row(event) for event in self._book_buffer]
        self._book_buffer.clear()
        self._send_to_clickhouse(rows, table="orderbook", columns=self._book_columns)

    def _tick_row(self, event: MDTickEvent) -> tuple[Any, ...]:
        trading_day = self._trading_day(event.ts)
        extras = orjson.dumps(event.extras or {}).decode("utf-8")
        return (
            trading_day,
            event.ts,
            event.symbol,
            event.asset_type,
            event.exchange,
            event.price,
            event.size,
            event.side,
            event.total_volume,
            event.total_amount,
            extras,
        )

    def _book_row(self, event: MDBookEvent) -> tuple[Any, ...]:
        trading_day = self._trading_day(event.ts)
        extras = orjson.dumps(event.extras or {}).decode("utf-8")
        return (
            trading_day,
            event.ts,
            event.symbol,
            event.asset_type,
            event.exchange,
            event.bid_prices,
            event.bid_volumes,
            event.ask_prices,
            event.ask_volumes,
            event.bid_total_vol,
            event.ask_total_vol,
            event.underlying_price,
            extras,
        )

    def _send_to_clickhouse(
        self,
        rows: List[tuple[Any, ...]],
        *,
        table: str,
        columns: Sequence[str],
    ) -> None:
        if not rows:
            return
        if self.client is None:
            self.logger.warning("No ClickHouse client configured; dropping %s rows for %s.", len(rows), table)
            return
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES"
        try:
            self.client.execute(sql, rows)
        except Exception as exc:  # noqa: BLE001
            self.logger.error("ClickHouse insert failed for %s: %s", table, exc)

    def _trading_day(self, ts_ns: int | None) -> str:
        if not ts_ns:
            return "1970-01-01"
        dt = datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
