from __future__ import annotations

import json
from types import SimpleNamespace

import numpy as np

from shijim.tools.hft_converter import (
    BaseEventSource,
    GAP_FLAG,
    HftBacktestConverter,
    LatencyModelConfig,
    RawEvent,
)


class ListEventSource(BaseEventSource):
    def __init__(self, events, source_type="jsonl"):  # noqa: ANN001
        self._events = events
        self.source_type = source_type

    def iter_events(self, symbol, trading_day):  # noqa: ANN001
        yield from self._events


def test_standard_mbo_conversion(tmp_path):
    events = [
        RawEvent(
            exchange_ts=1_000,
            receive_ts=1_050,
            event_type=0,
            side=1,
            price=100.0,
            qty=1.0,
            order_id="abc",
            seq_id=1,
        )
    ]
    src = ListEventSource(events)
    converter = HftBacktestConverter(src, LatencyModelConfig(mean_ms=20.0))

    outputs = converter.convert("TXF", "2024-01-01", tmp_path)
    data = np.load(outputs["npz"])

    assert data["event_flag"][0] == 1  # Add
    assert data["exchange_ts"][0] == 1_000
    assert data["receive_ts"][0] == 1_050
    assert data["latency_ns"][0] >= 50


def test_clock_skew_clamping(tmp_path):
    events = [
        RawEvent(
            exchange_ts=1_000,
            receive_ts=900,  # negative latency
            event_type=0,
            side=1,
            price=100.0,
            qty=1.0,
            order_id="abc",
            seq_id=1,
        )
    ]
    src = ListEventSource(events)
    converter = HftBacktestConverter(src, LatencyModelConfig(), min_latency_ns=1_000)

    outputs = converter.convert("TXF", "2024-01-01", tmp_path)
    data = np.load(outputs["npz"])

    assert data["latency_ns"][0] == 1_000
    with open(outputs["meta"], "r", encoding="utf-8") as fh:
        meta = json.load(fh)
    assert meta["stats"]["negative_latency"] == 1


def test_seq_gap_inserts_gap_flag(tmp_path):
    events = [
        RawEvent(10, 10, 0, 1, 100.0, 1.0, "a", seq_id=100),
        RawEvent(20, 20, 0, 1, 101.0, 1.0, "b", seq_id=105),
    ]
    src = ListEventSource(events)
    converter = HftBacktestConverter(src, LatencyModelConfig())

    outputs = converter.convert("TXF", "2024-01-01", tmp_path)
    data = np.load(outputs["npz"])

    assert GAP_FLAG in data["event_flag"]
    gap_idx = list(data["event_flag"]).index(GAP_FLAG)
    assert data["qty"][gap_idx] == 4  # gap size stored in qty


def test_latency_model_export(tmp_path):
    events = [
        RawEvent(1, 2, 0, 1, 100.0, 1.0, "x", seq_id=1),
    ]
    src = ListEventSource(events)
    model = LatencyModelConfig(mode="lognormal", mean_ms=20.0, std_ms=5.0)
    converter = HftBacktestConverter(src, model)

    outputs = converter.convert("TXF", "2024-01-01", tmp_path)
    with open(outputs["latency_model"], "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    assert cfg["mode"] == "lognormal"
    assert cfg["mean_ms"] == 20.0
