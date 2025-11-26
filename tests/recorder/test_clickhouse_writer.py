from __future__ import annotations

import time
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


def test_clickhouse_writer_async_write_batch_returns_quickly():
    class SlowClient(FakeClient):
        def execute(self, sql: str, rows: list[tuple[Any, ...]]) -> None:
            time.sleep(0.1)
            super().execute(sql, rows)

    client = SlowClient()
    writer = ClickHouseWriter(dsn="ch://test", client=client, flush_threshold=1)
    writer.enable_async()

    start = time.perf_counter()
    writer.write_batch([_tick("TXF1")], [])
    duration = time.perf_counter() - start

    assert duration < 0.05

    writer.drain_async()
    writer.close()
    assert len(client.calls) == 1
