"""Append-only raw log writer with per-symbol/day rotation."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, IO, Sequence, Tuple

import orjson

from shijim.events.schema import MDBookEvent, MDTickEvent


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
    _states: Dict[Tuple[str, str], _FileState] = field(default_factory=dict, init=False)

    def __post_init__(self) -> None:
        self.root = Path(self.root)
        self.root.mkdir(parents=True, exist_ok=True)

    def write_batch(
        self,
        ticks: Sequence[MDTickEvent],
        books: Sequence[MDBookEvent],
    ) -> None:
        """Serialize events to JSONL files grouped by trading day + symbol."""
        touched: set[Tuple[str, str]] = set()
        for event in list(ticks) + list(books):
            touched.add(self.write_event(event))

        for key in touched:
            state = self._states.get(key)
            if state is not None:
                state.handle.flush()

    def write_event(self, event: MDTickEvent | MDBookEvent) -> Tuple[str, str]:
        trading_day = self._trading_day(getattr(event, "ts", None))
        symbol = event.symbol or "unknown"
        key = (trading_day, symbol)
        state = self._ensure_state(trading_day, symbol)
        if (
            state.bytes_written >= self.max_file_size_bytes
            or state.event_count >= self.max_events_per_file
        ):
            state = self._rotate(trading_day, symbol, state)
        payload = orjson.dumps(asdict(event))
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
