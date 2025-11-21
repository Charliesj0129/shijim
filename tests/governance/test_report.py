from __future__ import annotations

from datetime import datetime, timezone

from shijim.governance.report import GapRange, GapReport


def test_gap_report_roundtrip_and_validation():
    gap = GapRange(
        symbol="2330",
        asset_type="stock",
        gap_type="tick",
        start_ts=100,
        end_ts=200,
        seq_start=10,
        seq_end=20,
        reason="tick_seq_gap",
    )
    report = GapReport(
        trading_day="2024-01-01",
        generated_at=datetime(2024, 1, 1, tzinfo=timezone.utc),
        auditor_version="v1",
        summary={"tick_gap_count": 1},
        metadata={"auditor_version": "v1"},
        tick_gaps=[gap],
        orderbook_gaps=[],
    )

    assert report.validate() == []

    data = report.to_dict()
    loaded = GapReport.from_dict(data)
    assert loaded.trading_day == "2024-01-01"
    assert loaded.tick_gaps[0].gap_type == "tick"
    assert loaded.summary["tick_gap_count"] == 1
