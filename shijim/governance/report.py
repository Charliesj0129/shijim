from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence


@dataclass(slots=True)
class GapRange:
    """Represents a missing data range discovered by the auditor."""

    symbol: str
    asset_type: str
    gap_type: str
    start_ts: int
    end_ts: int
    seq_start: int | None = None
    seq_end: int | None = None
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class GapReport:
    """Collection of tick/orderbook gaps for a given trading day."""

    trading_day: str
    generated_at: datetime
    auditor_version: str = "unknown"
    summary: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    tick_gaps: list[GapRange] = field(default_factory=list)
    orderbook_gaps: list[GapRange] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "trading_day": self.trading_day,
            "generated_at": self.generated_at.isoformat(),
            "auditor_version": self.auditor_version,
            "summary": self.summary,
            "metadata": self.metadata,
            "tick_gaps": [asdict(item) for item in self.tick_gaps],
            "orderbook_gaps": [asdict(item) for item in self.orderbook_gaps],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"))

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "GapReport":
        generated_raw = payload.get("generated_at")
        generated_at = (
            datetime.fromisoformat(generated_raw) if isinstance(generated_raw, str) else datetime.now(timezone.utc)
        )
        tick_gaps = [_gap_from_dict(item, default_type="tick") for item in payload.get("tick_gaps", [])]
        orderbook_gaps = [_gap_from_dict(item, default_type="orderbook") for item in payload.get("orderbook_gaps", [])]
        return cls(
            trading_day=payload.get("trading_day", ""),
            generated_at=generated_at,
            auditor_version=payload.get("auditor_version", "unknown"),
            summary=dict(payload.get("summary", {})),
            metadata=dict(payload.get("metadata", {})),
            tick_gaps=tick_gaps,
            orderbook_gaps=orderbook_gaps,
        )

    def all_gaps(self) -> Iterable[GapRange]:
        yield from self.tick_gaps
        yield from self.orderbook_gaps

    def validate(self) -> list[str]:
        errors: list[str] = []
        for gap in self.all_gaps():
            if not gap.symbol:
                errors.append("gap.symbol is empty")
            if gap.gap_type not in ("tick", "orderbook"):
                errors.append(f"gap_type invalid for {gap.symbol}: {gap.gap_type}")
            if gap.start_ts < 0 or gap.end_ts < 0:
                errors.append(f"negative timestamp for {gap.symbol}")
            if gap.end_ts < gap.start_ts:
                errors.append(f"end_ts < start_ts for {gap.symbol}")
            if gap.gap_type == "tick":
                if gap.seq_start is None or gap.seq_end is None:
                    errors.append(f"tick gap missing seq for {gap.symbol}")
                elif gap.seq_end < gap.seq_start:
                    errors.append(f"seq_end < seq_start for {gap.symbol}")
        return errors


def _gap_from_dict(item: dict[str, Any], default_type: str | None = None) -> GapRange:
    gap_type = item.get("gap_type") or item.get("type") or (default_type or "")
    return GapRange(
        symbol=item.get("symbol", ""),
        asset_type=item.get("asset_type", ""),
        gap_type=gap_type,
        start_ts=int(item.get("start_ts", 0)),
        end_ts=int(item.get("end_ts", 0)),
        seq_start=item.get("seq_start"),
        seq_end=item.get("seq_end"),
        reason=item.get("reason"),
        metadata=dict(item.get("metadata", {})),
    )


def write_report(report: GapReport, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(report.to_json(), encoding="utf-8")


def load_report(path: Path) -> GapReport:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return GapReport.from_dict(payload)
