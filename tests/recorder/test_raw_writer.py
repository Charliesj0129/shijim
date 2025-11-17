from __future__ import annotations

import json
from pathlib import Path

from shijim.events import MDBookEvent, MDTickEvent
from shijim.recorder.raw_writer import RawWriter


def _event(ts: int, symbol: str) -> MDTickEvent:
    return MDTickEvent(
        ts=ts,
        symbol=symbol,
        asset_type="futures",
        exchange="TAIFEX",
        price=123.0,
        size=1,
    )


def test_raw_writer_creates_daily_files(tmp_path: Path):
    writer = RawWriter(base_dir=tmp_path / "raw")
    event = _event(1_000_000_000, "TXF")
    writer.append(event)

    expected_dir = tmp_path / "raw" / "1970-01-01" / "symbol=TXF"
    expected_file = expected_dir / "md_events_0001.jsonl"
    assert expected_file.exists()

    with expected_file.open() as fh:
        line = fh.readline()
    payload = json.loads(line)
    assert payload["symbol"] == "TXF"
    assert payload["type"] == "MD_TICK"


def test_raw_writer_handles_missing_ts(tmp_path: Path):
    writer = RawWriter(base_dir=tmp_path)
    event = _event(ts=None, symbol="2330")
    writer.append(event)
    assert (tmp_path / "unknown" / "symbol=2330" / "md_events_0001.jsonl").exists()
