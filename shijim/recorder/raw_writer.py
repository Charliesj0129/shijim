"""Append-only raw log writer."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Sequence, Tuple

from shijim.events.schema import MDBookEvent, MDTickEvent


@dataclass
class RawWriter:
    """Writes JSONL log files with per-symbol/day rotation."""

    root: Path
    max_file_size_bytes: int = 512 * 1024 * 1024
    max_events_per_file: int = 1_000_000
    _file_state: Dict[Tuple[str, str], Tuple[Path, int, int]] = field(default_factory=dict, init=False)

    def write_batch(
        self,
        ticks: Sequence[MDTickEvent],
        books: Sequence[MDBookEvent],
    ) -> None:
        """Serialize events to JSONL files grouped by trading day + symbol."""
        for event in list(ticks) + list(books):
            self._write_event(event)

    def current_file_info(self, symbol: str, trading_day: str) -> Tuple[Path, int]:
        """Return the current file path and index for a symbol/day."""
        state = self._file_state.get((trading_day, symbol))
        if state is None:
            symbol_dir = self.root / trading_day / f"symbol={symbol}"
            return symbol_dir / "md_events_0001.jsonl", 1
        return state[0], state[1]

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _write_event(self, event: MDTickEvent | MDBookEvent) -> None:
        trading_day = self._trading_day(event.ts)
        symbol = event.symbol
        file_path, index = self._ensure_file(trading_day, symbol)
        line = json.dumps(asdict(event), ensure_ascii=False)
        with file_path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
        self._update_state(trading_day, symbol, file_path, index)

    def _ensure_file(self, trading_day: str, symbol: str) -> Tuple[Path, int]:
        state = self._file_state.get((trading_day, symbol))
        if state is None:
            symbol_dir = self.root / trading_day / f"symbol={symbol}"
            symbol_dir.mkdir(parents=True, exist_ok=True)
            file_path = symbol_dir / "md_events_0001.jsonl"
            self._file_state[(trading_day, symbol)] = (file_path, 1, 0)
            return file_path, 1
        file_path, index, events_written = state
        if (
            file_path.stat().st_size >= self.max_file_size_bytes
            or events_written >= self.max_events_per_file
        ):
            index += 1
            file_path = file_path.with_name(f"md_events_{index:04d}.jsonl")
            self._file_state[(trading_day, symbol)] = (file_path, index, 0)
        return file_path, index

    def _update_state(self, trading_day: str, symbol: str, file_path: Path, index: int) -> None:
        _, _, count = self._file_state.get((trading_day, symbol), (file_path, index, 0))
        self._file_state[(trading_day, symbol)] = (file_path, index, count + 1)

    def _trading_day(self, ts_ns: int) -> str:
        seconds = ts_ns / 1_000_000_000
        dt = datetime.fromtimestamp(seconds, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
