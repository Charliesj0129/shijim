from __future__ import annotations

from dataclasses import asdict
from pathlib import Path

import orjson

from shijim.events import MDBookEvent, MDTickEvent
from shijim.recorder.clickhouse_writer import ClickHouseWriter
from shijim.tools.restore_failed_batches import RestoreStats, run_restore


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def execute(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        self.calls.append((sql, rows))


def _write_events(path: Path, events) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as fh:
        for event in events:
            fh.write(orjson.dumps(asdict(event)) + b"\n")


def _tick(ts: int) -> MDTickEvent:
    return MDTickEvent(ts_ns=ts, symbol=f"TXF{ts}", asset_type="futures", exchange="TAIFEX", price=100.0, size=1)


def _book(ts: int) -> MDBookEvent:
    return MDBookEvent(
        ts_ns=ts,
        symbol=f"TXF{ts}",
        asset_type="futures",
        exchange="TAIFEX",
        bid_prices=[100.0],
        bid_volumes=[1],
        ask_prices=[101.0],
        ask_volumes=[2],
    )


def test_run_restore_dry_run_counts_events(tmp_path: Path):
    fallback_dir = tmp_path / "fallback"
    ticks_path = fallback_dir / "ticks" / "1970-01-01.jsonl"
    books_path = fallback_dir / "books" / "1970-01-01.jsonl"

    _write_events(ticks_path, [_tick(1), _tick(2)])
    _write_events(books_path, [_book(1)])

    # add malformed line
    invalid_path = fallback_dir / "ticks" / "1970-01-02.jsonl"
    invalid_path.parent.mkdir(parents=True, exist_ok=True)
    invalid_path.write_text("not-json\n")

    stats = run_restore(fallback_dir=fallback_dir, mode="dry-run", writer=None, batch_size=1)

    assert isinstance(stats, RestoreStats)
    assert stats.files_processed == 3
    assert stats.tick_events == 2
    assert stats.book_events == 1
    assert stats.skipped_lines == 1
    assert stats.applied_ticks == 0
    assert stats.applied_books == 0


def test_run_restore_apply_inserts_rows(tmp_path: Path):
    fallback_dir = tmp_path / "fallback"
    _write_events(fallback_dir / "ticks" / "1970-01-01.jsonl", [_tick(1), _tick(2)])
    _write_events(fallback_dir / "books" / "1970-01-01.jsonl", [_book(3)])

    client = FakeClient()
    writer = ClickHouseWriter(dsn="ch://test", client=client, fallback_dir=None)

    stats = run_restore(fallback_dir=fallback_dir, mode="apply", writer=writer, batch_size=1)

    assert stats.applied_ticks == 2
    assert stats.applied_books == 1
    assert len(client.calls) == 3  # two tick inserts, one book insert
