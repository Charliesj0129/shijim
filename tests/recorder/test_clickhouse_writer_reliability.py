from __future__ import annotations

from pathlib import Path

from shijim.events import MDBookEvent, MDTickEvent
from shijim.recorder.clickhouse_writer import ClickHouseWriter


class FlakyClient:
    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("boom")
        self.calls.append((sql, rows))


def _tick(ts: int) -> MDTickEvent:
    return MDTickEvent(
        ts_ns=ts,
        symbol="TXF",
        asset_type="futures",
        exchange="TAIFEX",
        price=100.0,
        size=1,
    )


def _book(ts: int) -> MDBookEvent:
    return MDBookEvent(
        ts_ns=ts,
        symbol="TXF",
        asset_type="futures",
        exchange="TAIFEX",
        bid_prices=[100.0],
        bid_volumes=[1],
        ask_prices=[101.0],
        ask_volumes=[2],
    )


def _read_fallback(path: Path) -> list[str]:
    if not path.exists():
        return []
    data = path.read_text().strip()
    return [line for line in data.splitlines() if line]


def test_tick_buffer_clears_only_after_success(tmp_path):
    client = FlakyClient(fail_times=1)
    writer = ClickHouseWriter(dsn="ch://test", client=client, fallback_dir=tmp_path)

    events = [_tick(1), _tick(2)]
    writer.write_batch(events, [])

    writer.flush(force=True)
    assert len(writer._tick_buffer) == len(events)
    assert writer.failed_batches
    kind, records = writer.failed_batches[-1]
    assert kind == "ticks"
    assert len(records) == len(events)

    fallback_file = tmp_path / "ticks" / "1970-01-01.jsonl"
    assert len(_read_fallback(fallback_file)) == len(events)

    writer.flush(force=True)
    assert len(writer._tick_buffer) == 0
    assert len(writer.failed_batches) == 1  # no new fallback entry once success occurs


def test_book_buffer_retries_until_success(tmp_path):
    client = FlakyClient(fail_times=1)
    writer = ClickHouseWriter(dsn="ch://test", client=client, fallback_dir=tmp_path)

    events = [_book(1)]
    writer.write_batch([], events)

    writer.flush(force=True)
    assert len(writer._book_buffer) == len(events)
    assert writer.failed_batches[-1][0] == "books"

    writer.flush(force=True)
    assert len(writer._book_buffer) == 0
    assert len(writer.failed_batches) == 1


def test_fallback_populates_on_permanent_failure(tmp_path):
    client = FlakyClient(fail_times=10)
    writer = ClickHouseWriter(dsn="ch://test", client=client, fallback_dir=tmp_path)

    writer.write_batch([_tick(1)], [])
    writer.flush(force=True)
    writer.flush(force=True)

    assert len(writer._tick_buffer) == 1
    assert len(writer.failed_batches) == 2
    fallback_file = tmp_path / "ticks" / "1970-01-01.jsonl"
    assert len(_read_fallback(fallback_file)) == 2
