"""Append-only raw event writer."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from shijim.events import MDBookEvent, MDTickEvent


class RawWriter:
    """Persist events to JSONL files under raw/YYYY-MM-DD/symbol=.../."""

    def __init__(self, base_dir: str | Path = "raw") -> None:
        self.base_dir = Path(base_dir)

    def append(self, event: MDTickEvent | MDBookEvent) -> None:
        payload = self._serialize(event)
        file_path = self._event_file_path(event)
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with file_path.open("a", encoding="utf-8") as fh:
            json.dump(payload, fh, ensure_ascii=False)
            fh.write("\n")

    # ------------------------------------------------------------------ #
    # Helpers
    # ------------------------------------------------------------------ #
    def _serialize(self, event: MDTickEvent | MDBookEvent) -> dict[str, Any]:
        if is_dataclass(event):
            return asdict(event)
        if isinstance(event, dict):
            return event
        raise TypeError(f"Unsupported event type: {type(event)!r}")

    def _event_file_path(self, event: MDTickEvent | MDBookEvent) -> Path:
        trading_day = self._date_str(event.ts)
        symbol = event.symbol or "UNKNOWN"
        folder = self.base_dir / trading_day / f"symbol={symbol}"
        return folder / "md_events_0001.jsonl"

    @staticmethod
    def _date_str(ts: int | None) -> str:
        if ts is None:
            return "unknown"
        dt = datetime.fromtimestamp(ts / 1_000_000_000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")
