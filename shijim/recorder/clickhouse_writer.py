"""Analytical writer for ClickHouse or similar columnar stores."""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Iterable, List, Sequence

import orjson

from shijim.events.schema import BaseMDEvent, MDBookEvent, MDTickEvent


FAILED_BATCH_HISTORY_LIMIT = 32


@dataclass(frozen=True)
class FailedBatchMeta:
    """Lightweight record of recent ClickHouse failures."""

    kind: str
    count: int
    timestamp: float


@dataclass
class ClickHouseWriter:
    """Buffers normalized events and flushes them into analytical tables."""

    dsn: str
    client: Any | None = None
    flush_threshold: int = 1_000
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))
    fallback_dir: Path | str | None = None
    _tick_buffer: list[MDTickEvent] = field(default_factory=list, init=False)
    _book_buffer: list[MDBookEvent] = field(default_factory=list, init=False)
    failed_batch_history: Deque[FailedBatchMeta] = field(
        default_factory=lambda: deque(maxlen=FAILED_BATCH_HISTORY_LIMIT),
        init=False,
    )

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

    def __post_init__(self) -> None:
        if self.fallback_dir is not None and not isinstance(self.fallback_dir, Path):
            self.fallback_dir = Path(self.fallback_dir)
        if isinstance(self.fallback_dir, Path):
            self.fallback_dir.mkdir(parents=True, exist_ok=True)

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
        batch = list(self._tick_buffer)
        try:
            rows = [self._tick_row(event) for event in batch]
            self._send_to_clickhouse(rows, table="ticks", columns=self._tick_columns)
        except Exception as exc:  # noqa: BLE001
            self.logger.error(
                "Failed to flush %s tick events to ClickHouse: %s",
                len(batch),
                exc,
                exc_info=True,
            )
            self._handle_failed_batch(batch, kind="ticks")
        else:
            self._tick_buffer.clear()

    def _flush_books(self) -> None:
        if not self._book_buffer:
            return
        batch = list(self._book_buffer)
        try:
            rows = [self._book_row(event) for event in batch]
            self._send_to_clickhouse(rows, table="orderbook", columns=self._book_columns)
        except Exception as exc:  # noqa: BLE001
            self.logger.error(
                "Failed to flush %s order book events to ClickHouse: %s",
                len(batch),
                exc,
                exc_info=True,
            )
            self._handle_failed_batch(batch, kind="books")
        else:
            self._book_buffer.clear()

    def _tick_row(self, event: MDTickEvent) -> tuple[Any, ...]:
        """Convert a tick event to a ClickHouse row (ts stored as nanoseconds)."""
        trading_day = self._trading_day(event.ts_ns)
        extras = orjson.dumps(event.extras or {}).decode("utf-8")
        return (
            trading_day,
            event.ts_ns,
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
        """Convert a book snapshot to a ClickHouse row (ts stored as nanoseconds)."""
        trading_day = self._trading_day(event.ts_ns)
        extras = orjson.dumps(event.extras or {}).decode("utf-8")
        return (
            trading_day,
            event.ts_ns,
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

    def _handle_failed_batch(self, batch: Sequence[BaseMDEvent], *, kind: str) -> None:
        if not batch:
            return
        records: list[tuple[str, bytes]] = []
        for event in batch:
            trading_day = self._trading_day(getattr(event, "ts_ns", None))
            try:
                payload = orjson.dumps(asdict(event))
            except Exception as exc:  # noqa: BLE001
                self.logger.error(
                    "Failed to serialize %s event for fallback: %s",
                    kind,
                    exc,
                    exc_info=True,
                )
                continue
            records.append((trading_day, payload))
        if not records:
            return
        self._persist_failed_records(kind, records)
        self.failed_batch_history.append(
            FailedBatchMeta(kind=kind, count=len(records), timestamp=time.time()),
        )
        self.logger.warning("Stored %s failed %s events for fallback.", len(records), kind)

    def _persist_failed_records(self, kind: str, records: list[tuple[str, bytes]]) -> None:
        if not isinstance(self.fallback_dir, Path):
            return
        for trading_day, payload in records:
            day = trading_day or "unknown"
            path = self.fallback_dir / kind / f"{day}.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("ab") as fh:
                fh.write(payload + b"\n")

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
            raise RuntimeError(f"No ClickHouse client configured for table {table}.")
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES"
        self.client.execute(sql, rows)

    def _trading_day(self, ts_ns: int | None) -> str:
        if not ts_ns:
            return "1970-01-01"
        dt = datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
