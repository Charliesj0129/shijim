"""Gap replay utilities built on top of Shioaji historical endpoints."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, Protocol, Sequence

from shijim.events.normalizers import normalize_tick_futures, normalize_tick_stock
from shijim.events.schema import MDTickEvent
from shijim.recorder.clickhouse_writer import ClickHouseWriter

logger = logging.getLogger(__name__)

try:  # pragma: no cover - used only when shioaji is available
    from shioaji.constant import TicksQueryType
except Exception:  # pragma: no cover - fallback dummy for tests/environments without shioaji

    class TicksQueryType:  # type: ignore[override]
        Range = "Range"


class HistoricalAPI(Protocol):
    """Subset of the Shioaji API used for gap backfills."""

    def ticks(self, contract: object, **kwargs: Any) -> Iterable[object]:
        """Yield historical ticks for the specified time range."""


class TickNormalizer(Protocol):
    """Protocol representing a broker-specific tick normalizer."""

    def __call__(self, tick: Any, exchange: Any | None = None) -> MDTickEvent:
        """Convert a raw tick payload into an MDTickEvent."""


ContractResolver = Callable[["GapDefinition"], object]


@dataclass(slots=True)
class GapDefinition:
    """Describes a missing data range detected by monitoring components."""

    symbol: str
    start_ts: int
    end_ts: int
    date: str
    asset_type: str
    exchange: str | None = None
    contract: object | None = None


@dataclass
class GapReplayer:
    """Coordinates historical fetches to backfill missing tick data."""

    api: HistoricalAPI
    analytical_writer: ClickHouseWriter
    contract_resolver: ContractResolver | None = None
    tick_normalizers: dict[str, TickNormalizer] | None = None
    throttle_seconds: float = 0.0
    sleep: Callable[[float], None] = time.sleep
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))

    def replay_gap(self, gap: GapDefinition) -> Sequence[MDTickEvent]:
        """Fetch ticks for a gap and forward them to the analytical writer."""
        contract = self._resolve_contract(gap)
        normalizer = self._normalizer_for(gap.asset_type)

        try:
            raw_ticks = self.api.ticks(
                contract=contract,
                date=gap.date,
                query_type=TicksQueryType.Range,
                start=gap.start_ts,
                end=gap.end_ts,
            )
        except Exception as exc:  # noqa: BLE001
            self.logger.warning(
                "Historical ticks fetch failed for %s (%s): %s",
                gap.symbol,
                gap.date,
                exc,
            )
            return []

        events: list[MDTickEvent] = []
        for raw_tick in raw_ticks or []:
            event = normalizer(raw_tick, exchange=gap.exchange)
            if event.ts < gap.start_ts or event.ts > gap.end_ts:
                continue
            events.append(event)

        if events:
            self.analytical_writer.write_batch(events, [])  # no order book rows for historical ticks

        return events

    def run_jobs(self, gaps: Sequence[GapDefinition]) -> list[Sequence[MDTickEvent]]:
        """Replay a batch of gaps sequentially with optional throttling."""
        gap_list = list(gaps)
        results: list[Sequence[MDTickEvent]] = []
        for index, gap in enumerate(gap_list):
            events = self.replay_gap(gap)
            results.append(events)
            if self.throttle_seconds > 0 and index < len(gap_list) - 1:
                self.sleep(self.throttle_seconds)
        return results

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _resolve_contract(self, gap: GapDefinition) -> object:
        if gap.contract is not None:
            return gap.contract
        if self.contract_resolver is None:
            raise ValueError(f"No contract provided for symbol {gap.symbol}.")
        contract = self.contract_resolver(gap)
        if contract is None:
            raise ValueError(f"Contract resolver returned None for {gap.symbol}.")
        return contract

    def _normalizer_for(self, asset_type: str) -> TickNormalizer:
        normalizers = self.tick_normalizers or {
            "futures": normalize_tick_futures,
            "stock": normalize_tick_stock,
        }
        try:
            return normalizers[asset_type]
        except KeyError as exc:
            raise ValueError(f"No tick normalizer registered for {asset_type}.") from exc
