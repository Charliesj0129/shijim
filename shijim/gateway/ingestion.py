from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Mapping, Sequence, Tuple

try:  # pragma: no cover - runtime dependency
    import shijim_core
except ImportError:  # pragma: no cover
    shijim_core = None

logger = logging.getLogger(__name__)


def _default_ts_ns() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1_000_000_000)


def _to_timestamp_ns(value: Any) -> int:
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000_000)
    if isinstance(value, str):
        txt = value.strip().replace(" ", "T")
        try:
            dt = datetime.fromisoformat(txt)
        except ValueError:
            return _default_ts_ns()
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1_000_000_000)
    return _default_ts_ns()


def _zip_levels(prices: Sequence[Any], sizes: Sequence[Any]) -> List[Tuple[float, int]]:
    pairs: List[Tuple[float, int]] = []
    for price, qty in zip(prices, sizes):
        pairs.append((float(price), int(qty)))
    return pairs


class ShioajiIngestor:
    """Translate Shioaji callbacks into Rust RingBuffer writes."""

    def __init__(
        self,
        writer: Any,
        symbol_map: Mapping[str, int],
        *,
        allow_simtrade: bool = False,
        logger: logging.Logger | None = None,
        tick_publisher: Callable[..., Any] | None = None,
        book_publisher: Callable[..., Any] | None = None,
        snapshot_publisher: Callable[..., Any] | None = None,
        system_publisher: Callable[..., Any] | None = None,
    ) -> None:
        self.writer = writer
        self.symbol_map: Dict[str, int] = dict(symbol_map)
        self.allow_simtrade = allow_simtrade
        self.logger = logger or logging.getLogger(__name__)
        if tick_publisher is None or book_publisher is None or snapshot_publisher is None or system_publisher is None:
            if shijim_core is None:
                raise RuntimeError("shijim_core is required unless publishers are provided")
        self.publish_tick = tick_publisher or shijim_core.publish_tick_v1
        self.publish_quote = book_publisher or shijim_core.publish_quote_v1
        self.publish_snapshot = snapshot_publisher or shijim_core.publish_snapshot_v1
        self.publish_system = system_publisher or shijim_core.publish_system_event

    def on_quote(self, quote: Any) -> None:
        if not self.allow_simtrade and int(getattr(quote, "simtrade", 0)) == 1:
            self.logger.debug("Simtrade Skipped: %s", getattr(quote, "code", "?"))
            return
        code = getattr(quote, "code", None)
        sec_id = self.symbol_map.get(code or "")
        if sec_id is None:
            self.logger.warning("Unknown symbol %s", code)
            return
        ts = _to_timestamp_ns(getattr(quote, "datetime", None))
        if hasattr(quote, "bid_price") and getattr(quote, "bid_price"):
            bids = _zip_levels(getattr(quote, "bid_price", []), getattr(quote, "bid_volume", []))
            asks = _zip_levels(getattr(quote, "ask_price", []), getattr(quote, "ask_volume", []))
            self.publish_quote(self.writer, sec_id, bids, asks, ts)
        else:
            price = float(getattr(quote, "close", 0.0))
            size = int(getattr(quote, "volume", 0))
            self.publish_tick(self.writer, sec_id, price, size, ts)

    def process_snapshot(self, code: str, snapshot: Any) -> None:
        sec_id = self.symbol_map.get(code)
        if sec_id is None:
            self.logger.warning("Unknown snapshot code %s", code)
            return
        ts = _to_timestamp_ns(getattr(snapshot, "datetime", None))
        close = float(getattr(snapshot, "close", 0.0))
        high = float(getattr(snapshot, "high", close))
        open_px = float(getattr(snapshot, "open", close))
        self.publish_snapshot(self.writer, sec_id, close, high, open_px, ts)

    def on_event(self, event: Any) -> None:
        code = getattr(event, "code", None)
        if int(code or 0) == 13:
            self.publish_system(self.writer, 13)
