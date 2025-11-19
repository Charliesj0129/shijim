from __future__ import annotations

import orjson
from pathlib import Path

from shijim.events import MDTickEvent
from shijim.recorder.raw_writer import RawWriter, _FileState


def _tick(ts: int, symbol: str) -> MDTickEvent:
    return MDTickEvent(
        ts=ts,
        symbol=symbol,
        asset_type="futures",
        exchange="TAIFEX",
        price=123.0,
        size=1,
    )


def test_raw_writer_creates_daily_files(tmp_path: Path):
    writer = RawWriter(root=tmp_path / "raw")
    event = _tick(1_000_000_000, "TXF")
    writer.write_event(event)
    writer.close_all()

    expected_dir = tmp_path / "raw" / "1970-01-01" / "symbol=TXF"
    expected_file = expected_dir / "md_events_0001.jsonl"
    assert expected_file.exists()

    with expected_file.open("rb") as fh:
        payload = orjson.loads(fh.readline())
    assert payload["symbol"] == "TXF"
    assert payload["type"] == "MD_TICK"


def test_raw_writer_handles_missing_ts(tmp_path: Path):
    writer = RawWriter(root=tmp_path)
    event = _tick(ts=0, symbol="2330")
    event.ts = None
    writer.write_event(event)
    writer.close_all()

    expected = tmp_path / "unknown" / "symbol=2330" / "md_events_0001.jsonl"
    assert expected.exists()


def test_raw_writer_rotates_when_event_threshold_hit(tmp_path: Path):
    writer = RawWriter(root=tmp_path, max_events_per_file=2, max_file_size_bytes=10**9)
    day_ts = 1_000_000_000
    for i in range(5):
        writer.write_event(_tick(day_ts + i, "TXF"))
    writer.close_all()

    symbol_dir = tmp_path / "1970-01-01" / "symbol=TXF"
    files = sorted(symbol_dir.glob("md_events_*.jsonl"))
    assert len(files) == 3

    total_events = 0
    for path in files:
        with path.open("rb") as fh:
            for line in fh:
                payload = orjson.loads(line)
                assert payload["symbol"] == "TXF"
                total_events += 1
    assert total_events == 5


class FakeHandle:
    def __init__(self) -> None:
        self.flush_calls = 0

    def write(self, data: bytes) -> None:  # pragma: no cover - simple spy
        pass

    def flush(self) -> None:  # pragma: no cover - simple spy
        self.flush_calls += 1

    def close(self) -> None:  # pragma: no cover - simple spy
        pass


class FlushTrackingWriter(RawWriter):
    def _create_state(self, path: Path, index: int) -> _FileState:  # type: ignore[override]
        path.parent.mkdir(parents=True, exist_ok=True)
        return _FileState(path=path, handle=FakeHandle(), index=index)


def test_write_batch_flushes_once_per_file(tmp_path: Path):
    writer = FlushTrackingWriter(root=tmp_path)
    events = [_tick(1_000_000_000 + i, "TXF") for i in range(3)]
    writer.write_batch(events, [])

    state = writer._states[("1970-01-01", "TXF")]
    assert isinstance(state.handle, FakeHandle)
    assert state.handle.flush_calls == 1
