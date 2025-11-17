"""Buffered ClickHouse writer stub."""

from __future__ import annotations

import logging
import time
from dataclasses import asdict, is_dataclass
from typing import Any, Callable, List, Sequence

from shijim.events import MDBookEvent, MDTickEvent

logger = logging.getLogger(__name__)


class ClickHouseWriter:
    """Buffer events and flush them to ClickHouse (stubbed)."""

    def __init__(
        self,
        *,
        max_batch_size: int = 1_000,
        max_latency: float = 1.0,
        flush_callback: Callable[[List[dict]], None] | None = None,
    ) -> None:
        self.max_batch_size = max_batch_size
        self.max_latency = max_latency
        self._flush_callback = flush_callback or self._log_flush
        self._buffer: List[dict] = []
        self._last_flush = time.monotonic()

    def append(self, event: MDTickEvent | MDBookEvent) -> None:
        self._buffer.append(self._serialize(event))
        if len(self._buffer) >= self.max_batch_size or self._latency_exceeded():
            self.flush()

    def flush(self) -> None:
        if not self._buffer:
            return
        payload = self._buffer[:]
        self._buffer.clear()
        self._last_flush = time.monotonic()
        self._flush_callback(payload)

    # ------------------------------------------------------------------ #
    def _serialize(self, event) -> dict:
        if is_dataclass(event):
            return asdict(event)
        if isinstance(event, dict):
            return event
        raise TypeError(f"Unsupported event type: {type(event)!r}")

    def _latency_exceeded(self) -> bool:
        return (time.monotonic() - self._last_flush) >= self.max_latency

    @staticmethod
    def _log_flush(payload: Sequence[dict]) -> None:
        logger.debug("Flushing %s events to ClickHouse (stub).", len(payload))
