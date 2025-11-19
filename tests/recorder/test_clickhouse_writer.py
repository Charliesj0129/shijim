from __future__ import annotations

from typing import Any

from shijim.events import MDBookEvent, MDTickEvent
from shijim.recorder.clickhouse_writer import ClickHouseWriter


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, list[tuple[Any, ...]]]] = []

    def execute(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
        self.calls.append((sql, rows))


def _tick(symbol: str, ts: int = 1) -> MDTickEvent:
    return MDTickEvent(
        ts_ns=ts,
        symbol=symbol,
        asset_type="futures",
        exchange="TAIFEX",
        price=100.0,
        size=1,
        side="buy",
    )


def _book(symbol: str, ts: int = 1) -> MDBookEvent:
    return MDBookEvent(
        ts_ns=ts,
        symbol=symbol,
        asset_type="futures",
        exchange="TAIFEX",
        bid_prices=[100.0],
        bid_volumes=[1],
        ask_prices=[101.0],
        ask_volumes=[2],
    )


def test_clickhouse_writer_flushes_tick_batches():
    client = FakeClient()
    writer = ClickHouseWriter(dsn="ch://test", client=client, flush_threshold=2)
    writer.write_batch([_tick("TXF1")], [])
    assert client.calls == []

    writer.write_batch([_tick("TXF2")], [])
    assert len(client.calls) == 1
    sql, rows = client.calls[0]
    assert "INSERT INTO ticks" in sql
    assert rows[0][2] == "TXF1"
    assert rows[1][2] == "TXF2"


def test_clickhouse_writer_flushes_books_on_force():
    client = FakeClient()
    writer = ClickHouseWriter(dsn="ch://test", client=client, flush_threshold=10)
    writer.write_batch([], [_book("TXF")])
    assert client.calls == []

    writer.flush(force=True)
    assert len(client.calls) == 1
    sql, rows = client.calls[0]
    assert "INSERT INTO orderbook" in sql
    assert rows[0][2] == "TXF"
