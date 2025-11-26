from __future__ import annotations

from types import SimpleNamespace

from shijim.recorder.gap_replayer import GapDefinition, GapReplayer


class RecordingWriter:
    def __init__(self) -> None:
        self.batches: list[tuple[list[object], list[object]]] = []

    def write_batch(self, ticks, books):  # noqa: ANN001
        self.batches.append((list(ticks), list(books)))


class CountingRateLimiter:
    def __init__(self) -> None:
        self.count = 0

    def consume(self, cost: float = 1.0, block: bool = True, sleep=None) -> bool:  # noqa: ANN001
        self.count += 1
        return True


class FlakyAPI:
    def __init__(self, responses: list[object]) -> None:
        self._responses = responses
        self.calls: list[tuple[object, str]] = []

    def ticks(self, contract=None, date=None, query_type=None):  # noqa: ANN001
        self.calls.append((contract, date))
        result = self._responses.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_gap_replayer_deduplicates_events():
    ticks = [
        SimpleNamespace(ts=1, symbol="TXF", price=100, size=1, exchange="TSE"),
        SimpleNamespace(ts=1, symbol="TXF", price=100, size=1, exchange="TSE"),  # duplicate ts
    ]
    api = FlakyAPI([ticks])
    writer = RecordingWriter()
    gap = GapDefinition(
        symbol="TXF",
        start_ts=0,
        end_ts=5,
        date="1970-01-01",
        asset_type="futures",
        contract=object(),
    )
    replayer = GapReplayer(
        api=api,
        analytical_writer=writer,
        tick_normalizers={"futures": lambda tick, exchange=None: SimpleNamespace(  # noqa: ANN001
            ts_ns=tick.ts,
            symbol=tick.symbol,
            asset_type="futures",
            exchange=exchange or tick.exchange,
            extras={},
        )},
    )

    events = replayer.replay_gap(gap)

    assert len(events) == 1
    assert writer.batches and len(writer.batches[0][0]) == 1


def test_rate_limiter_is_consumed_per_gap_call():
    api = FlakyAPI([[]])
    writer = RecordingWriter()
    limiter = CountingRateLimiter()
    gap = GapDefinition(
        symbol="2330",
        start_ts=0,
        end_ts=0,
        date="1970-01-01",
        asset_type="stock",
        contract=object(),
    )
    replayer = GapReplayer(
        api=api,
        analytical_writer=writer,
        tick_normalizers={"stock": lambda tick, exchange=None: tick},  # noqa: ANN001
        rate_limiter=limiter,
    )

    replayer.replay_gap(gap)

    assert limiter.count == 1
