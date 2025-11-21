from __future__ import annotations

from shijim.governance.audit import DataAuditor, DataAuditorConfig


class FakeAuditor(DataAuditor):
    def __init__(self, query_results):
        super().__init__(client=object(), config=DataAuditorConfig())
        self._results = list(query_results)

    def _run_query(self, sql, params):  # noqa: D401
        if not self._results:
            raise RuntimeError("no more results")
        return self._results.pop(0)


def test_auditor_detects_tick_and_orderbook_gaps():
    # Ordered: tick gaps, tick stats, orderbook gaps
    query_results = [
        [("TXF", "futures", 11, 12, 100, 200)],
        [("TXF", "futures", 2, 1)],
        [("TXF", "futures", 1_000_000_000, 21_000_000_000, 10)],
    ]

    auditor = FakeAuditor(query_results)
    report = auditor.audit_trading_day("2024-01-01")

    assert report.summary["tick_gap_count"] == 1
    assert report.summary["orderbook_gap_count"] == 1
    assert report.summary["partial"] is False
    assert report.tick_gaps[0].gap_type == "tick"
    assert report.orderbook_gaps[0].gap_type == "orderbook"
    assert report.metadata["auditor_version"] == auditor.config.auditor_version
    assert report.summary["symbols_with_duplicates"] == ["TXF"]
    assert report.summary["symbols_with_resets"] == ["TXF"]
