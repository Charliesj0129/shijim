"""Analytical writer for ClickHouse or similar columnar stores."""

from __future__ import annotations

import logging
import queue
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Iterable, List, Sequence
import threading

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
    _fallback_alerted: bool = field(default=False, init=False)
    async_queue_max_batches: int = 256
    async_enqueue_timeout: float = 0.1
    _async_enabled: bool = field(default=False, init=False)
    _task_queue: queue.Queue | None = field(default=None, init=False)
    _worker_thread: threading.Thread | None = field(default=None, init=False)
    _worker_stop: threading.Event = field(default_factory=threading.Event, init=False)

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
        if not ticks and not books:
            return
        if self._async_enabled:
            batch = (list(ticks), list(books))
            self._submit_task(("write", batch[0], batch[1], False))
            return
        self._write_batch_sync(ticks, books)

    def flush(self, force: bool = True) -> None:
        """Flush buffered events to ClickHouse."""
        if self._async_enabled:
            self._submit_task(("flush", [], [], force))
            return
        self._flush_sync(force)

    def insert_events(
        self,
        ticks: Sequence[MDTickEvent],
        books: Sequence[MDBookEvent],
    ) -> tuple[int, int]:
        """Insert already-buffered events without touching the internal queues."""
        tick_rows = [self._tick_row(event) for event in ticks] if ticks else []
        if tick_rows:
            self._send_to_clickhouse(tick_rows, table="ticks", columns=self._tick_columns)
        book_rows = [self._book_row(event) for event in books] if books else []
        if book_rows:
            self._send_to_clickhouse(book_rows, table="orderbook", columns=self._book_columns)
        return len(tick_rows), len(book_rows)

    def get_failed_batch_summary(self) -> dict[str, Any]:
        """Expose lightweight metadata about recent fallback usage."""
        history = list(self.failed_batch_history)
        summary: dict[str, Any] = {"recent_failures": len(history)}
        if history:
            last = history[-1]
            summary.update(
                {
                    "last_failure_kind": last.kind,
                    "last_failure_count": last.count,
                    "last_failure_time": last.timestamp,
                }
            )
            return summary

    def enable_async(self, queue_max_batches: int | None = None) -> None:
        """Enable asynchronous batching so write_batch/flush calls enqueue work."""
        if self._async_enabled:
            return
        max_batches = queue_max_batches or self.async_queue_max_batches
        self._task_queue = queue.Queue(maxsize=max_batches)
        self._worker_stop.clear()
        self._async_enabled = True
        self._worker_thread = threading.Thread(
            target=self._worker_loop,
            name="ClickHouseWriterWorker",
            daemon=True,
        )
        self._worker_thread.start()

    def drain_async(self) -> None:
        """Block until all queued tasks have been processed."""
        if self._async_enabled and self._task_queue is not None:
            self._task_queue.join()

    def close(self) -> None:
        """Shutdown the async worker thread if enabled."""
        if not self._async_enabled:
            return
        self.drain_async()
        self._shutdown_async_worker()

    def _submit_task(
        self,
        task: tuple[str, list[MDTickEvent], list[MDBookEvent], bool],
    ) -> None:
        assert self._task_queue is not None
        try:
            self._task_queue.put(task, timeout=self.async_enqueue_timeout)
        except queue.Full:
            kind, ticks, books, _ = task
            logger.error(
                "ClickHouseWriter async queue full; dropping %s batch (%s ticks / %s books).",
                kind,
                len(ticks),
                len(books),
            )

    def _worker_loop(self) -> None:
        assert self._task_queue is not None
        while True:
            item = self._task_queue.get()
            if item is None:
                self._task_queue.task_done()
                break
            kind, ticks, books, force = item
            try:
                if kind == "write":
                    self._write_batch_sync(ticks, books)
                elif kind == "flush":
                    self._flush_sync(force)
            finally:
                self._task_queue.task_done()

    def _write_batch_sync(
        self,
        ticks: Sequence[MDTickEvent],
        books: Sequence[MDBookEvent],
    ) -> None:
        if ticks:
            self._tick_buffer.extend(ticks)
        if books:
            self._book_buffer.extend(books)
        if len(self._tick_buffer) >= self.flush_threshold:
            self._flush_ticks()
        if len(self._book_buffer) >= self.flush_threshold:
            self._flush_books()

    def _flush_sync(self, force: bool) -> None:
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
        grouped: Dict[Path, list[bytes]] = defaultdict(list)
        for trading_day, payload in records:
            day = trading_day or "unknown"
            path = self.fallback_dir / kind / f"{day}.jsonl"
            grouped[path].append(payload)
        for path, payloads in grouped.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("ab") as fh:
                for payload in payloads:
                    fh.write(payload + b"\n")
            self.logger.warning("Fallback file %s appended with %s %s events.", path, len(payloads), kind)
        if grouped and not self._fallback_alerted:
            self.logger.warning("ClickHouse fallback activated; writing failed batches under %s", self.fallback_dir)
            self._fallback_alerted = True

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

    def _shutdown_async_worker(self) -> None:
        if not self._async_enabled or self._task_queue is None:
            return
        self._worker_stop.set()
        self._task_queue.put(None)
        if self._worker_thread is not None:
            self._worker_thread.join(timeout=5)
        self._worker_thread = None
        self._task_queue = None
        self._async_enabled = False
        self._worker_stop.clear()
