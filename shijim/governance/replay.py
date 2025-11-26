from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path

from shijim.governance.report import GapRange, GapReport, load_report
from shijim.recorder.gap_replayer import GapDefinition, GapReplayer

logger = logging.getLogger(__name__)


@dataclass
class GapReplaySummary:
    trading_day: str
    total_gaps: int
    succeeded: int
    failed: int
    events_replayed: int
    failed_symbols: list[str] = field(default_factory=list)


class GapReplayOrchestrator:
    """Glue layer that feeds auditor output into the gap replayer."""

    def __init__(self, replayer: GapReplayer):
        self.replayer = replayer

    def run(self, report: GapReport) -> GapReplaySummary:
        gaps = list(report.all_gaps())
        events_replayed = 0
        failed_symbols: list[str] = []
        succeeded = 0
        failed = 0

        for idx, gap_range in enumerate(gaps):
            try:
                events = self.replayer.replay_gap(_to_gap_definition(gap_range, report.trading_day))
                events_replayed += len(events)
                succeeded += 1
            except Exception as exc:  # noqa: BLE001
                failed += 1
                failed_symbols.append(gap_range.symbol)
                logger.warning("Gap replay failed for %s: %s", gap_range.symbol, exc)
            should_sleep = (
                self.replayer.throttle_seconds > 0 and idx < len(gaps) - 1
            )
            if should_sleep:
                self.replayer.sleep(self.replayer.throttle_seconds)

        return GapReplaySummary(
            trading_day=report.trading_day,
            total_gaps=len(gaps),
            succeeded=succeeded,
            failed=failed,
            events_replayed=events_replayed,
            failed_symbols=failed_symbols,
        )


def _to_gap_definition(gap: GapRange, trading_day: str) -> GapDefinition:
    return GapDefinition(
        gap_type=gap.gap_type,
        symbol=gap.symbol,
        start_ts=gap.start_ts,
        end_ts=gap.end_ts,
        date=trading_day,
        asset_type=gap.asset_type,
        exchange=str(gap.metadata.get("exchange", "")),
        contract=gap.metadata.get("contract"),
    )


def load_gap_report(path: str | Path) -> GapReport:
    return load_report(Path(path))
