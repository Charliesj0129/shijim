from __future__ import annotations

from types import SimpleNamespace

from shijim.events.schema import MDTickEvent
from shijim.recorder.gap_replayer import GapDefinition, GapReplayer


class FakeAPI:
    def __init__(self, ticks):
        self._ticks = ticks
        self.calls: list[dict[str, object]] = []

    def ticks(self, **kwargs):
        self.calls.append(kwargs)
        return list(self._ticks)


class RecordingWriter:
    def __init__(self) -> None:
        self.batches: list[tuple[list[MDTickEvent], list[object]]] = []

    def write_batch(self, ticks, books):  # noqa: ANN001
        self.batches.append((list(ticks), list(books)))


def _normalizer(asset_type: str):
    def _normalize(tick, exchange=None):  # noqa: ANN001
        return MDTickEvent(
            ts=tick.ts,
            symbol=tick.symbol,
            asset_type=asset_type,
            exchange=exchange or tick.exchange,
            price=tick.price,
            size=tick.size,
        )

    return _normalize


def test_replay_gap_fetches_ticks_and_writes() -> None:
    ticks = [
        SimpleNamespace(ts=1, symbol="TXF", price=100, size=1, exchange="TAIFEX"),
        SimpleNamespace(ts=3, symbol="TXF", price=101, size=2, exchange="TAIFEX"),
        SimpleNamespace(ts=10, symbol="TXF", price=102, size=3, exchange="TAIFEX"),
    ]
    api = FakeAPI(ticks)
    writer = RecordingWriter()
    gap = GapDefinition(
        symbol="TXF",
        start_ts=0,
        end_ts=5,
        date="2024-01-01",
        asset_type="futures",
        exchange="TAIFEX",
        contract=object(),
    )
    replayer = GapReplayer(
        api=api,
        analytical_writer=writer,
        tick_normalizers={"futures": _normalizer("futures")},
    )

    events = replayer.replay_gap(gap)

    assert len(events) == 2
    assert [event.price for event in events] == [100, 101]
    assert writer.batches and len(writer.batches[0][0]) == 2
    assert writer.batches[0][1] == []

    call = api.calls[0]
    assert call["contract"] is gap.contract
    assert call["date"] == "1970-01-01"
    assert call["query_type"] == "AllDay"
    assert "start" not in call
    assert "end" not in call


def test_run_jobs_applies_throttling() -> None:
    gaps = [
        GapDefinition("TXF", 0, 5, "2024-01-01", "futures", contract=object()),
        GapDefinition("2330", 0, 5, "2024-01-02", "stock", contract=object()),
    ]

    api = FakeAPI([SimpleNamespace(ts=1, symbol="S", price=1, size=1, exchange="X")])
    writer = RecordingWriter()
    sleep_calls: list[float] = []

    replayer = GapReplayer(
        api=api,
        analytical_writer=writer,
        tick_normalizers={
            "futures": _normalizer("futures"),
            "stock": _normalizer("stock"),
        },
        throttle_seconds=0.5,
        sleep=sleep_calls.append,
    )

    results = replayer.run_jobs(gaps)

    assert len(results) == len(gaps)
    # Only one sleep call between jobs
    assert sleep_calls == [0.5]
