from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from shijim.events import MDTickEvent
from shijim.gateway.replay import replay_ticks


class DummyContract:
    def __init__(self, code="2330", exchange="TSE"):
        self.code = code
        self.exchange = exchange


class DummyFuturesContract(DummyContract):
    pass


class DummySink:
    def __init__(self) -> None:
        self.events: list[MDTickEvent] = []

    def append(self, event: MDTickEvent) -> None:
        self.events.append(event)


class FakeAPI:
    def __init__(self, ticks_map):
        self.ticks_map = ticks_map
        self.calls: list[str] = []

    def ticks(self, contract, date, query_type):
        self.calls.append(date)
        return self.ticks_map.get(date)


def _timestamp(day_offset: int, seconds: int = 0) -> int:
    base = datetime(2023, 1, 1, tzinfo=timezone.utc) + timedelta(days=day_offset, seconds=seconds)
    return int(base.timestamp() * 1_000_000_000)


def _ticks_for(values):
    return SimpleNamespace(
        ts=[v["ts"] for v in values],
        close=[v.get("close", 1.0) for v in values],
        volume=[v.get("volume", 1) for v in values],
        bid_price=[v.get("bid_price", 0.0) for v in values],
        bid_volume=[v.get("bid_volume", 0) for v in values],
        ask_price=[v.get("ask_price", 0.0) for v in values],
        ask_volume=[v.get("ask_volume", 0) for v in values],
        tick_type=[v.get("tick_type", 1) for v in values],
    )


def test_replay_ticks_converts_and_routes_to_sink():
    start = _timestamp(0, 0)
    end = _timestamp(0, 10)
    ticks_map = {
        "2023-01-01": _ticks_for(
            [
                {"ts": start, "close": 10.0, "volume": 2},
                {"ts": end, "close": 11.0, "volume": 3},
            ]
        )
    }
    api = FakeAPI(ticks_map)
    sink = DummySink()
    contract = DummyFuturesContract(code="TXF")

    events = replay_ticks(contract, start, end, api=api, sink=sink, throttle=0)

    assert len(events) == 2
    assert sink.events == events
    assert all(event.symbol == "TXF" for event in events)
    assert api.calls == ["2023-01-01"]


def test_replay_ticks_handles_multiple_dates():
    start = _timestamp(0, 0)
    end = _timestamp(1, 5)
    ticks_map = {
        "2023-01-01": _ticks_for([{"ts": start}]),
        "2023-01-02": _ticks_for([{"ts": _timestamp(1, 1)}]),
    }
    api = FakeAPI(ticks_map)
    sink = DummySink()
    contract = DummyContract(code="2330")

    events = replay_ticks(contract, start, end, api=api, sink=sink, throttle=0)

    assert len(events) == 2
    assert api.calls == ["2023-01-01", "2023-01-02"]
