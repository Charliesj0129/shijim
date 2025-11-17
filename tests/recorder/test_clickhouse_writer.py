from __future__ import annotations

from shijim.events import MDTickEvent
from shijim.recorder.clickhouse_writer import ClickHouseWriter


def make_event(symbol: str) -> MDTickEvent:
    return MDTickEvent(
        ts=1,
        symbol=symbol,
        asset_type="futures",
        exchange="TAIFEX",
    )


def test_clickhouse_writer_flushes_on_batch_size():
    flushed = []

    def on_flush(payload):
        flushed.append(payload)

    writer = ClickHouseWriter(max_batch_size=2, max_latency=100, flush_callback=on_flush)
    writer.append(make_event("TXF1"))
    assert flushed == []

    writer.append(make_event("TXF2"))
    assert len(flushed) == 1
    assert len(flushed[0]) == 2


def test_clickhouse_writer_manual_flush():
    flushed = []
    writer = ClickHouseWriter(max_batch_size=10, max_latency=100, flush_callback=flushed.append)
    writer.append(make_event("TXF"))
    writer.flush()
    assert len(flushed) == 1
