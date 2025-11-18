"""Append-only raw log writer."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from shijim.events.schema import MDBookEvent, MDTickEvent


@dataclass
class RawWriter:
    """Writes JSON/MsgPack log files that act as immutable ground-truth."""

    root: Path

    def write_batch(
        self,
        ticks: Sequence[MDTickEvent],
        books: Sequence[MDBookEvent],
    ) -> None:
        """Serialize events to JSONL files grouped by trading day + symbol."""
        all_events = list(ticks) + list(books)
        for event in all_events:
            self._write_event(event)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _write_event(self, event: MDTickEvent | MDBookEvent) -> None:
        trading_day = self._trading_day(event.ts)
        symbol_dir = self.root / trading_day / f"symbol={event.symbol}"
        symbol_dir.mkdir(parents=True, exist_ok=True)
        file_path = symbol_dir / "md_events_0001.jsonl"
        line = json.dumps(asdict(event), default=str, ensure_ascii=False)
        with file_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    def _trading_day(self, ts_ns: int) -> str:
        seconds = ts_ns / 1_000_000_000
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
