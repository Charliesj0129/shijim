"""Analytical writer for ClickHouse or similar columnar stores."""

from __future__ import annotations

import gzip
import logging
import os
import queue
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Sequence

import orjson
import requests

from shijim.events.schema import BaseMDEvent, MDBookEvent, MDTickEvent

logger = logging.getLogger(__name__)

try:
    from prometheus_client import Gauge
    FALLBACK_SIZE_BYTES = Gauge(
        "ingestion_fallback_size_bytes", "Total bytes currently in fallback storage"
    )
except ImportError:
    class MockGauge:
        def set(self, value): pass
        def inc(self, amount=1): pass
        def dec(self, amount=1): pass
    FALLBACK_SIZE_BYTES = MockGauge()

FAILED_BATCH_HISTORY_LIMIT = 32


@dataclass
class RetryPolicy:
    max_retries: int = 3
    base_delay: float = 0.1
    max_delay: float = 1.0
    backoff_factor: float = 2.0


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
    flush_threshold: int = 5_000
    async_insert: bool = False
    async_insert_wait: bool = False
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
    dropped_metric: Any | None = None
    _async_enabled: bool = field(default=False, init=False)
    _task_queue: queue.Queue | None = field(default=None, init=False)
    _worker_thread: threading.Thread | None = field(default=None, init=False)
    _worker_stop: threading.Event = field(default_factory=threading.Event, init=False)
    _last_flush_ts: float = field(default=0.0, init=False)
    flush_interval_seconds: float = 1.0
    use_http: bool = False
    http_url: str | None = None
    http_auth: tuple[str, str] | None = None
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)

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
        env_threshold = os.getenv("SHIJIM_CH_FLUSH_THRESHOLD")
        if env_threshold:
            try:
                self.flush_threshold = max(int(env_threshold), 1)
            except ValueError:
                self.logger.warning(
                    "Invalid SHIJIM_CH_FLUSH_THRESHOLD=%s; using %s",
                    env_threshold,
                    self.flush_threshold,
                )
        env_async = os.getenv("SHIJIM_CH_ASYNC_INSERT")
        if env_async is not None:
            self.async_insert = env_async.lower() in ("1", "true", "yes")
        env_wait = os.getenv("SHIJIM_CH_ASYNC_WAIT")
        if env_wait is not None:
            self.async_insert_wait = env_wait.lower() in ("1", "true", "yes")
        env_interval = os.getenv("SHIJIM_CH_FLUSH_INTERVAL_SEC")
        if env_interval:
            try:
                self.flush_interval_seconds = float(env_interval)
            except ValueError:
                self.logger.warning(
                    "Invalid SHIJIM_CH_FLUSH_INTERVAL_SEC=%s; using %s",
                    env_interval,
                    self.flush_interval_seconds,
                )

        # HTTP configuration
        if self.dsn and (self.dsn.startswith("http://") or self.dsn.startswith("https://")):
            self.use_http = True
            self.http_url = self.dsn
            # Parse auth if needed, but for now assume dsn is just url or handled elsewhere
            # Actually requests can handle user:pass@host in url? Yes.

        if self.fallback_dir is not None and not isinstance(self.fallback_dir, Path):
            self.fallback_dir = Path(self.fallback_dir)
        if isinstance(self.fallback_dir, Path):
            self.fallback_dir.mkdir(parents=True, exist_ok=True)
            self._update_fallback_size()

        self._last_flush_ts = time.time()

        if not self.use_http and self.client is None and self.dsn:
            try:
                from clickhouse_driver import Client  # type: ignore

                self.client = Client.from_url(self.dsn)
            except ImportError:
                raise ImportError(
                    "clickhouse-driver is required for ClickHouseWriter (when not using HTTP). "
                    "Install it via: pip install '.[clickhouse]' or pip install clickhouse-driver"
                )
            except Exception as exc:  # noqa: BLE001
                self.logger.warning(
                    "Failed to initialize ClickHouse client from DSN %s: %s",
                    self.dsn,
                    exc,
                )

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
            if self.dropped_metric:
                self.dropped_metric.labels(writer="clickhouse").inc(len(ticks) + len(books))
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
        now = time.time()
        if len(self._tick_buffer) >= self.flush_threshold or self._due_for_flush(now):
            self._flush_ticks()
        if len(self._book_buffer) >= self.flush_threshold or self._due_for_flush(now):
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
            self._last_flush_ts = time.time()

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
            self._last_flush_ts = time.time()

    def _due_for_flush(self, now: float | None = None) -> bool:
        now = now or time.time()
        return (now - self._last_flush_ts) >= self.flush_interval_seconds

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
                payload = orjson.dumps(event)
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
        added_bytes = 0
        for trading_day, payload in records:
            day = trading_day or "unknown"
            path = self.fallback_dir / kind / f"{day}.jsonl"
            grouped[path].append(payload)
        for path, payloads in grouped.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("ab") as fh:
                for payload in payloads:
                    line = payload + b"\n"
                    fh.write(line)
                    added_bytes += len(line)
            self.logger.warning(
                "Fallback file %s appended with %s %s events.", path, len(payloads), kind
            )

        if added_bytes > 0:
            FALLBACK_SIZE_BYTES.inc(added_bytes)

        if grouped and not self._fallback_alerted:
            self.logger.warning(
                "ClickHouse fallback activated; writing failed batches under %s",
                self.fallback_dir,
            )
            self._fallback_alerted = True

    def _update_fallback_size(self) -> None:
        """Recalculate total size of fallback directory."""
        if not isinstance(self.fallback_dir, Path):
            return
        total = 0
        for root, _, files in os.walk(self.fallback_dir):
            for name in files:
                try:
                    total += os.path.getsize(os.path.join(root, name))
                except OSError:
                    pass
        FALLBACK_SIZE_BYTES.set(total)

    def _send_to_clickhouse(
        self,
        rows: List[tuple[Any, ...]],
        *,
        table: str,
        columns: Sequence[str],
    ) -> None:
        if not rows:
            return

        if self.use_http:
            self._send_http(rows, table=table, columns=columns)
            return

        if self.client is None:
            raise RuntimeError(
                f"No ClickHouse client configured for table {table} "
                f"(dsn={self.dsn!r}). Provide a client or set CLICKHOUSE_DSN."
            )
        sql = f"INSERT INTO {table} ({', '.join(columns)}) VALUES"
        if self.async_insert:
            settings = {
                "async_insert": 1,
                "wait_for_async_insert": int(self.async_insert_wait),
            }
            try:
                self.client.execute(sql, rows, settings=settings)
                return
            except TypeError:
                # Fallback for clients that don't support settings kwarg
                self.logger.warning(
                    "Client does not support settings kwarg; retrying without async_insert."
                )
        self.client.execute(sql, rows)

    def _send_http(
        self,
        rows: List[tuple[Any, ...]],
        *,
        table: str,
        columns: Sequence[str],
    ) -> None:
        """Send data via HTTP interface using JSONEachRow format and gzip compression."""
        if not self.http_url:
            raise RuntimeError("HTTP URL not configured")

        # Convert rows to JSONEachRow format
        # rows are tuples matching columns order
        # We need to convert them to dicts or just construct JSON objects
        # Actually JSONEachRow expects newline delimited JSON objects

        # Optimization: Use a generator or list comprehension to build the body
        # But we need to map columns to values.

        # This might be slow in Python for large batches.
        # But we are already doing row conversion in _tick_row/_book_row which returns tuples.
        # Maybe we should have returned dicts there if we knew we were using JSONEachRow?
        # But clickhouse-driver expects tuples.

        # Let's construct the JSON lines.
        json_lines = []
        for row in rows:
            record = dict(zip(columns, row))
            # We need to ensure types are JSON serializable.
            # _tick_row returns some JSON strings for extras, we should probably parse them back?
            # Or just leave them as strings if ClickHouse expects String/JSON?
            # Wait, `extras` in _tick_row is `orjson.dumps(...).decode()`.
            # If we put it in a dict and dump it again, it will be a string containing JSON.
            # If the ClickHouse column is String, that's fine.

            # However, `orjson.dumps` is fast.
            json_lines.append(orjson.dumps(record))

        body = b"\n".join(json_lines)
        compressed_body = gzip.compress(body)

        query = f"INSERT INTO {table} FORMAT JSONEachRow"
        params = {}
        if self.async_insert:
            params["async_insert"] = 1
            params["wait_for_async_insert"] = 1 if self.async_insert_wait else 0

        url = self.http_url
        if "?" not in url:
            url += "?"
        else:
            url += "&"

        # We append query to URL or pass as param?
        # Usually `query` param or body. But for INSERT, the body is data.
        # So query goes in `query` param.
        params["query"] = query

        headers = {
            "Content-Encoding": "gzip",
            # "Content-Type": "application/x-ndjson" # or text/plain
        }

        # Retry logic
        for attempt in range(self.retry_policy.max_retries + 1):
            try:
                resp = requests.post(
                    url,
                    params=params,
                    data=compressed_body,
                    headers=headers,
                    timeout=10, # TODO: make configurable
                    auth=self.http_auth
                )
                resp.raise_for_status()
                return
            except requests.RequestException as exc:
                if attempt < self.retry_policy.max_retries:
                    sleep_time = min(
                        self.retry_policy.max_delay,
                        self.retry_policy.base_delay * (self.retry_policy.backoff_factor ** attempt)
                    )
                    self.logger.warning(
                        "HTTP insert failed (attempt %s/%s): %s. Retrying in %ss...",
                        attempt + 1,
                        self.retry_policy.max_retries,
                        exc,
                        sleep_time
                    )
                    time.sleep(sleep_time)
                else:
                    self.logger.error(
                        "HTTP insert failed after %s retries: %s",
                        self.retry_policy.max_retries,
                        exc,
                    )
                    raise

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
