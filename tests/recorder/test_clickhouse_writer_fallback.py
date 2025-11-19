from __future__ import annotations

from pathlib import Path

from shijim.events import MDBookEvent, MDTickEvent
from shijim.recorder.clickhouse_writer import (
    ClickHouseWriter,
    FAILED_BATCH_HISTORY_LIMIT,
    FailedBatchMeta,
)


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


def _read_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return [line for line in path.read_text().splitlines() if line]


def test_tick_buffer_clears_only_after_success(tmp_path):
    client = FlakyClient(fail_times=1)
    writer = ClickHouseWriter(dsn="ch://test", client=client, fallback_dir=tmp_path)

    events = [_tick(1), _tick(2)]
    writer.write_batch(events, [])

    writer.flush(force=True)
    assert len(writer._tick_buffer) == len(events)
    assert writer.failed_batch_history
    latest: FailedBatchMeta = writer.failed_batch_history[-1]
    assert latest.kind == "ticks"
    assert latest.count == len(events)

    writer.flush(force=True)
    assert len(writer._tick_buffer) == 0
    assert len(writer.failed_batch_history) == 1


def test_book_buffer_retries_until_success(tmp_path):
    client = FlakyClient(fail_times=1)
    writer = ClickHouseWriter(dsn="ch://test", client=client, fallback_dir=tmp_path)

    events = [_book(1)]
    writer.write_batch([], events)

    writer.flush(force=True)
    assert len(writer._book_buffer) == len(events)

    writer.flush(force=True)
    assert len(writer._book_buffer) == 0
    assert len(writer.failed_batch_history) == 1
    assert writer.failed_batch_history[-1].kind == "books"


def test_failed_batches_history_is_bounded(tmp_path):
    failures = FAILED_BATCH_HISTORY_LIMIT + 5
    client = FlakyClient(fail_times=failures)
    writer = ClickHouseWriter(dsn="ch://test", client=client, fallback_dir=tmp_path)

    for i in range(failures):
        writer.write_batch([_tick(i)], [])
        writer.flush(force=True)

    history = list(writer.failed_batch_history)
    assert len(history) == FAILED_BATCH_HISTORY_LIMIT
    assert all(isinstance(entry, FailedBatchMeta) for entry in history)

    fallback_file = tmp_path / "ticks" / "1970-01-01.jsonl"
    assert fallback_file.exists()
    assert _read_lines(fallback_file)


def test_events_persisted_to_disk_on_failure(tmp_path):
    client = FlakyClient(fail_times=1)
    writer = ClickHouseWriter(dsn="ch://test", client=client, fallback_dir=tmp_path)

    events = [_tick(1), _tick(2), _tick(3)]
    writer.write_batch(events, [])
    writer.flush(force=True)

    fallback_file = tmp_path / "ticks" / "1970-01-01.jsonl"
    lines = _read_lines(fallback_file)
    assert len(lines) == len(events)
