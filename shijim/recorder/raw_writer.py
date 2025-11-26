"""Append-only raw log writer with per-symbol/day rotation."""

from __future__ import annotations

import logging
import queue
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, IO, Sequence, Tuple

import orjson

from shijim.events.schema import MDBookEvent, MDTickEvent

logger = logging.getLogger(__name__)


@dataclass
class _FileState:
    path: Path
    handle: IO[bytes]
    index: int
    event_count: int = 0
    bytes_written: int = 0


@dataclass
class RawWriter:
    """Writes JSONL log files grouped by trading day + symbol with rotation."""

    root: Path
    max_file_size_bytes: int = 512 * 1024 * 1024
    max_events_per_file: int = 1_000_000
    async_queue_max_batches: int = 256
    async_enqueue_timeout: float = 0.1
    dropped_metric: Any | None = None
    _states: Dict[Tuple[str, str], _FileState] = field(default_factory=dict, init=False)
    _async_enabled: bool = field(default=False, init=False)
    _task_queue: queue.Queue | None = field(default=None, init=False)
    _worker_thread: threading.Thread | None = field(default=None, init=False)
    _worker_stop: threading.Event = field(default_factory=threading.Event, init=False)

    def __post_init__(self) -> None:
        self.root = Path(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

    def write_batch(
        self,
        ticks: Sequence[MDTickEvent],
        books: Sequence[MDBookEvent],
    ) -> None:
        """Serialize events to JSONL files grouped by trading day + symbol."""
        if self._async_enabled:
            batch = (list(ticks), list(books))
            if not batch[0] and not batch[1]:
                return
            self._submit_async(batch)
            return
        self._write_batch_sync(ticks, books)

    def enable_async(self, queue_max_batches: int | None = None) -> None:
        """Enable asynchronous batching so enqueue calls return quickly."""
        if self._async_enabled:
            return
        max_batches = queue_max_batches or self.async_queue_max_batches
        self._task_queue = queue.Queue(maxsize=max_batches)
        self._async_enabled = True
        self._worker_stop.clear()
        self._worker_thread = threading.Thread(
            target=self._drain_loop,
            name="RawWriterWorker",
            daemon=True,
        )
        self._worker_thread.start()

    def drain_async(self) -> None:
        """Block until all enqueued batches have been processed."""
        if self._async_enabled and self._task_queue is not None:
            self._task_queue.join()

    def _submit_async(self, batch: tuple[list[MDTickEvent], list[MDBookEvent]]) -> None:
        assert self._task_queue is not None
        try:
            self._task_queue.put(batch, timeout=self.async_enqueue_timeout)
        except queue.Full:
            if self.dropped_metric:
                self.dropped_metric.labels(writer="raw").inc(len(batch[0]) + len(batch[1]))
            logger.error(
                "RawWriter async queue full; dropping batch of %s ticks / %s books.",
                len(batch[0]),
                len(batch[1]),
            )

    def _drain_loop(self) -> None:
        assert self._task_queue is not None
        while True:
            item = self._task_queue.get()
            if item is None:
                self._task_queue.task_done()
                break
            ticks, books = item
            try:
                self._write_batch_sync(ticks, books)
            finally:
                self._task_queue.task_done()

    def _write_batch_sync(
        self,
        ticks: Sequence[MDTickEvent],
        books: Sequence[MDBookEvent],
    ) -> None:
        touched: set[Tuple[str, str]] = set()
        for event in list(ticks) + list(books):
            touched.add(self.write_event(event))

        for key in touched:
            state = self._states.get(key)
            if state is not None:
                state.handle.flush()

    def write_event(self, event: MDTickEvent | MDBookEvent) -> Tuple[str, str]:
        trading_day = self._trading_day(getattr(event, "ts_ns", None))
        symbol = event.symbol or "unknown"
        key = (trading_day, symbol)
        state = self._ensure_state(trading_day, symbol)
        if (
            state.bytes_written >= self.max_file_size_bytes
            or state.event_count >= self.max_events_per_file
        ):
            state = self._rotate(trading_day, symbol, state)
        try:
            payload = orjson.dumps(event)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to serialize raw event %s: %s", event, exc, exc_info=True)
            return key
        line = payload + b"\n"
        state.handle.write(line)
        state.event_count += 1
        state.bytes_written += len(line)
        return key

    def current_file_info(self, symbol: str, trading_day: str) -> Tuple[Path, int]:
        """Return current file path + index for testing or inspection."""
        state = self._states.get((trading_day, symbol))
        if state is None:
            symbol_dir = self._symbol_dir(trading_day, symbol)
            return symbol_dir / "md_events_0001.jsonl", 1
        return state.path, state.index

    def close_all(self) -> None:
        """Close all open file handles."""
        if self._async_enabled:
            self.drain_async()
            self._shutdown_async_worker()
        for state in self._states.values():
            try:
                state.handle.flush()
                state.handle.close()
            except Exception:  # noqa: BLE001
                pass
        self._states.clear()

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _ensure_state(self, trading_day: str, symbol: str) -> _FileState:
        key = (trading_day, symbol)
        state = self._states.get(key)
        if state is None:
            state = self._open_latest_state(trading_day, symbol)
            self._states[key] = state
        return state

    def _open_latest_state(self, trading_day: str, symbol: str) -> _FileState:
        symbol_dir = self._symbol_dir(trading_day, symbol)
        symbol_dir.mkdir(parents=True, exist_ok=True)
        existing = sorted(symbol_dir.glob("md_events_*.jsonl"))
        if existing:
            path = max(existing, key=self._file_index)
            index = self._file_index(path)
        else:
            index = 1
            path = symbol_dir / "md_events_0001.jsonl"
        state = self._create_state(path, index)
        if state.bytes_written >= self.max_file_size_bytes:
            state.handle.close()
            state = self._create_state(symbol_dir / f"md_events_{index + 1:04d}.jsonl", index + 1)
        return state

    def _rotate(self, trading_day: str, symbol: str, state: _FileState) -> _FileState:
        state.handle.flush()
        state.handle.close()
        symbol_dir = self._symbol_dir(trading_day, symbol)
        next_index = state.index + 1
        next_path = symbol_dir / f"md_events_{next_index:04d}.jsonl"
        new_state = self._create_state(next_path, next_index)
        self._states[(trading_day, symbol)] = new_state
        return new_state

    def _create_state(self, path: Path, index: int) -> _FileState:
        path.parent.mkdir(parents=True, exist_ok=True)
        existing_size = path.stat().st_size if path.exists() else 0
        handle = path.open("ab")
        return _FileState(path=path, handle=handle, index=index, event_count=0, bytes_written=existing_size)

    def _symbol_dir(self, trading_day: str, symbol: str) -> Path:
        safe_symbol = symbol or "unknown"
        return self.root / trading_day / f"symbol={safe_symbol}"

    def _trading_day(self, ts_ns: int | None) -> str:
        if not ts_ns:
            return "unknown"
        seconds = ts_ns / 1_000_000_000
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")

    @staticmethod
    def _file_index(path: Path) -> int:
        try:
            return int(path.stem.split("_")[-1])
        except ValueError:
            return 0

    def _shutdown_async_worker(self) -> None:
        if not self._async_enabled or self._task_queue is None:
            return
        self._worker_stop.set()
        # Ensure worker exits even if join() called again later.
        self._task_queue.put(None)
        if self._worker_thread is not None:
            self._worker_thread.join(timeout=5)
        self._worker_thread = None
        self._task_queue = None
        self._async_enabled = False
        self._worker_stop.clear()
