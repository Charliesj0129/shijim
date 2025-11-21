"""Gap replay utilities built on top of Shioaji historical endpoints."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Protocol, Sequence

from shijim.events.normalizers import normalize_tick_futures, normalize_tick_stock
from shijim.events.schema import MDTickEvent
from shijim.recorder.clickhouse_writer import ClickHouseWriter
from shijim.recorder.rate_limit import TokenBucketRateLimiter

logger = logging.getLogger(__name__)

try:  # pragma: no cover - used only when shioaji is available
    from shioaji.constant import TicksQueryType
except Exception:  # pragma: no cover - fallback dummy for tests/environments without shioaji

    class TicksQueryType:  # type: ignore[override]
        AllDay = "AllDay"


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
    """Describes a missing data range detected by monitoring components.

    start_ts and end_ts are expressed in nanoseconds since epoch.
    """

    symbol: str
    start_ts: int
    end_ts: int
    date: str
    asset_type: str
    exchange: str | None = None
    contract: object | None = None
    gap_type: str | None = "tick"


@dataclass
class GapReplayer:
    """Coordinates historical fetches to backfill missing tick data."""

    api: HistoricalAPI
    analytical_writer: ClickHouseWriter
    contract_resolver: ContractResolver | None = None
    tick_normalizers: dict[str, TickNormalizer] | None = None
    throttle_seconds: float = 0.0
    rate_limiter: TokenBucketRateLimiter | None = None
    max_retries: int = 3
    backoff_seconds: float = 1.0
    jitter_seconds: float = 0.05
    sleep: Callable[[float], None] = time.sleep
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))

    def replay_gap(self, gap: GapDefinition) -> Sequence[MDTickEvent]:
        """Fetch ticks for a gap and forward them to the analytical writer."""
        contract = self._resolve_contract(gap)
        if (gap.gap_type or "tick") != "tick":
            self.logger.info("Skipping non-tick gap %s (%s)", gap.symbol, gap.gap_type)
            return []
        normalizer = self._normalizer_for(gap.asset_type)

        events: list[MDTickEvent] = []
        for date_str in _date_range(gap.start_ts, gap.end_ts):
            raw_ticks = self._fetch_ticks_with_retry(gap, contract, date_str)

            for raw_tick in raw_ticks or []:
                try:
                    event = normalizer(raw_tick, exchange=gap.exchange)
                except Exception as exc:  # noqa: BLE001
                    self.logger.warning("Normalizer failed for %s: %s", gap.symbol, exc)
                    continue
                if gap.start_ts <= event.ts_ns <= gap.end_ts:
                    events.append(event)
            # TODO: handle pagination if api.ticks returns partial data.

        deduped = self._deduplicate_events(events, prefer_seq=False) if events else []
        if deduped:
            self.analytical_writer.write_batch(deduped, [])

        return deduped

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

    def _deduplicate_events(self, events: Sequence[MDTickEvent], *, prefer_seq: bool = False) -> list[MDTickEvent]:
        """Drop duplicates based on symbol/asset_type/ts_ns (and seq if present)."""
        seen: set[tuple[str, str, int, int | None]] = set()
        unique: list[MDTickEvent] = []
        for event in events:
            seq = None
            if prefer_seq:
                seq = event.extras.get("seq") if isinstance(event.extras, dict) else None  # type: ignore[assignment]
            key = (event.symbol, event.asset_type, event.ts_ns, seq)
            if key in seen:
                continue
            seen.add(key)
            unique.append(event)
        return unique

    def _fetch_ticks_with_retry(self, gap: GapDefinition, contract: object, date_str: str) -> Iterable[object]:
        last_exc: Exception | None = None
        for attempt in range(self.max_retries):
            if self.rate_limiter is not None:
                self.rate_limiter.consume(cost=1.0, block=True, sleep=self.sleep)
            try:
                return self.api.ticks(
                    contract=contract,
                    date=date_str,
                    query_type=TicksQueryType.AllDay,
                )
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                delay = self.backoff_seconds * (2**attempt) + self.jitter_seconds
                self.logger.warning(
                    "Historical ticks fetch failed for %s on %s (attempt %s/%s): %s",
                    gap.symbol,
                    date_str,
                    attempt + 1,
                    self.max_retries,
                    exc,
                )
                if attempt + 1 < self.max_retries:
                    self.sleep(delay)
        if last_exc is not None:
            raise last_exc
        return []


def _date_range(start_ns: int, end_ns: int) -> Iterable[str]:
    start_dt = datetime.fromtimestamp(start_ns / 1_000_000_000, tz=timezone.utc).date()
    end_dt = datetime.fromtimestamp(end_ns / 1_000_000_000, tz=timezone.utc).date()
    current = start_dt
    delta = timedelta(days=1)
    while current <= end_dt:
        yield current.isoformat()
        current += delta
