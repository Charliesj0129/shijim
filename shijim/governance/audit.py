from __future__ import annotations

import argparse
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from datetime import time as dt_time
from pathlib import Path
from typing import Any, Sequence

try:  # pragma: no cover - optional tzdata
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None

from shijim.governance.report import GapRange, GapReport, write_report

logger = logging.getLogger(__name__)

if ZoneInfo:
    try:
        TAIWAN_TZ = ZoneInfo("Asia/Taipei")
    except Exception:  # pragma: no cover - tzdata missing
        TAIWAN_TZ = timezone(timedelta(hours=8))
else:  # pragma: no cover
    TAIWAN_TZ = timezone(timedelta(hours=8))


@dataclass
class DataAuditorConfig:
    tick_table: str = "ticks"
    orderbook_table: str = "orderbook"
    seq_column: str = "seqno"
    ts_column: str = "ts"
    orderbook_gap_seconds: int = 5
    min_tick_in_gap: int = 1
    auditor_version: str = "1.0.0"
    reset_time_tolerance_seconds: int = 30
    orderbook_opening_gap_multiplier: float = 3.0
    stabilization_gap_multiplier: float = 2.0
    market_open_time: dt_time = dt_time(hour=8, minute=30)
    market_close_time: dt_time = dt_time(hour=13, minute=45)
    auction_window_end: dt_time = dt_time(hour=9, minute=0)
    after_hours_end: dt_time = dt_time(hour=14, minute=0)
    stabilization_windows: Sequence[tuple[dt_time, dt_time]] = ()
    expected_symbols: Sequence[str] | None = None
    max_retries: int = 3
    retry_base_delay: float = 1.0
    timezone: timezone = TAIWAN_TZ


class DataAuditor:
    """ClickHouse gap auditor for post-market validation."""

    def __init__(
        self,
        client: Any | None = None,
        *,
        dsn: str | None = None,
        config: DataAuditorConfig | None = None,
    ):
        self.config = config or DataAuditorConfig()
        self.client = client or self._build_client(dsn)

    def audit_trading_day(self, trading_day: str) -> GapReport:
        started = time.monotonic()
        partial_errors: list[str] = []

        try:
            tick_gaps, tick_stats, symbols_seen = self._detect_tick_gaps(trading_day)
        except Exception as exc:  # noqa: BLE001
            logger.error("Tick gap scan failed for %s: %s", trading_day, exc)
            tick_gaps = []
            tick_stats = {}
            symbols_seen = set()
            partial_errors.append(f"tick_scan_failed: {exc}")

        try:
            orderbook_gaps = self._detect_orderbook_gaps(trading_day)
        except Exception as exc:  # noqa: BLE001
            logger.error("Orderbook gap scan failed for %s: %s", trading_day, exc)
            orderbook_gaps = []
            partial_errors.append(f"orderbook_scan_failed: {exc}")

        absent_symbols = self._detect_absent_symbols(trading_day, symbols_seen, partial_errors)

        duration_ms = int((time.monotonic() - started) * 1000)
        summary = {
            "tick_gap_count": len(tick_gaps),
            "orderbook_gap_count": len(orderbook_gaps),
            "duration_ms": duration_ms,
            "partial": bool(partial_errors),
            "symbols_with_duplicates": sorted(
                [symbol for symbol, stats in tick_stats.items() if stats.get("dup_count", 0) > 0]
            ),
            "symbols_with_resets": sorted(
                [symbol for symbol, stats in tick_stats.items() if stats.get("reset_count", 0) > 0]
            ),
            "absent_symbols": sorted(absent_symbols),
        }
        metadata = {
            "auditor_version": self.config.auditor_version,
            "partial_errors": partial_errors,
            "tick_stats": tick_stats,
        }

        tick_gaps = self._sorted_tick_gaps(tick_gaps)
        orderbook_gaps = self._sorted_orderbook_gaps(orderbook_gaps)
        report = GapReport(
            trading_day=trading_day,
            generated_at=datetime.now(timezone.utc),
            auditor_version=self.config.auditor_version,
            summary=summary,
            metadata=metadata,
            tick_gaps=tick_gaps,
            orderbook_gaps=orderbook_gaps,
        )
        errors = report.validate()
        if errors:
            raise ValueError(f"GapReport validation failed: {errors}")
        return report

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _build_client(self, dsn: str | None) -> Any:
        if not dsn:
            return None
        try:
            from clickhouse_driver import Client  # type: ignore
        except Exception as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("clickhouse-driver is required for DataAuditor") from exc
        return Client.from_url(dsn)

    def _detect_tick_gaps(
        self, trading_day: str
    ) -> tuple[list[GapRange], dict[str, dict[str, int]], set[str]]:
        if self.client is None:
            raise RuntimeError("DataAuditor: ClickHouse client is required for tick scan.")

        rows = self._query_tick_gaps(trading_day)
        gaps: list[GapRange] = []
        symbols_seen: set[str] = set()
        for symbol, asset_type, seq_start, seq_end, start_ts, end_ts in rows:
            symbols_seen.add(symbol)
            gaps.append(
                GapRange(
                    symbol=symbol,
                    asset_type=str(asset_type or ""),
                    gap_type="tick",
                    start_ts=int(start_ts),
                    end_ts=int(end_ts),
                    seq_start=int(seq_start),
                    seq_end=int(seq_end),
                    reason="tick_seq_gap",
                    metadata={"trading_day": trading_day},
                )
            )

        stats_rows = self._query_tick_anomalies(trading_day)
        tick_stats: dict[str, dict[str, int]] = {}
        for symbol, asset_type, dup_count, reset_count in stats_rows:
            symbols_seen.add(symbol)
            tick_stats[symbol] = {
                "asset_type": str(asset_type or ""),
                "dup_count": int(dup_count or 0),
                "reset_count": int(reset_count or 0),
            }
        return gaps, tick_stats, symbols_seen

    def _detect_orderbook_gaps(self, trading_day: str) -> list[GapRange]:
        if self.client is None:
            raise RuntimeError("DataAuditor: ClickHouse client is required for orderbook scan.")
        rows = self._query_orderbook_gaps(trading_day)

        gaps: list[GapRange] = []
        for symbol, asset_type, start_ts, end_ts, tick_count in rows:
            if not self._should_emit_book_gap(start_ts, end_ts, int(tick_count or 0)):
                continue
            gaps.append(
                GapRange(
                    symbol=symbol,
                    asset_type=str(asset_type or ""),
                    gap_type="orderbook",
                    start_ts=int(start_ts),
                    end_ts=int(end_ts),
                    reason="orderbook_silence",
                    metadata={"tick_count": int(tick_count or 0), "trading_day": trading_day},
                )
            )
        return gaps

    def _detect_absent_symbols(
        self, trading_day: str, symbols_seen: set[str], partial_errors: list[str]
    ) -> set[str]:
        expected = set(self.config.expected_symbols or [])
        if not expected:
            return set()
        try:
            present = self._fetch_present_symbols(trading_day)
        except Exception as exc:  # noqa: BLE001
            partial_errors.append(f"symbol_presence_failed: {exc}")
            return set()
        return expected - present - symbols_seen

    def _fetch_present_symbols(self, trading_day: str) -> set[str]:
        cfg = self.config
        sql = f"""
            SELECT symbol
            FROM {cfg.tick_table}
            WHERE trading_day = %(trading_day)s
            GROUP BY symbol
        """
        rows = self._execute_with_retry(sql, {"trading_day": trading_day})
        return {row[0] for row in rows or []}

    def _query_tick_gaps(self, trading_day: str) -> list[tuple[Any, ...]]:
        cfg = self.config
        sql = f"""
            WITH gaps AS (
                SELECT
                    symbol,
                    anyLast(asset_type) AS asset_type,
                    {cfg.seq_column} AS curr_seq,
                    lag({cfg.seq_column}) OVER (
                        PARTITION BY symbol ORDER BY {cfg.seq_column}
                    ) AS prev_seq,
                    {cfg.ts_column} AS curr_ts,
                    lag({cfg.ts_column}) OVER (
                        PARTITION BY symbol ORDER BY {cfg.seq_column}
                    ) AS prev_ts
                FROM {cfg.tick_table}
                WHERE trading_day = %(trading_day)s
            )
            SELECT
                symbol,
                asset_type,
                toInt64(prev_seq + 1) AS seq_start,
                toInt64(curr_seq - 1) AS seq_end,
                toInt64(prev_ts) AS start_ts,
                toInt64(curr_ts) AS end_ts
            FROM gaps
            WHERE prev_seq IS NOT NULL AND (curr_seq - prev_seq) > 1
            ORDER BY symbol, seq_start
        """
        return self._execute_with_retry(sql, {"trading_day": trading_day})

    def _query_tick_anomalies(self, trading_day: str) -> list[tuple[Any, ...]]:
        cfg = self.config
        reset_ns = cfg.reset_time_tolerance_seconds * 1_000_000_000
        sql = f"""
            SELECT
                symbol,
                anyLast(asset_type) AS asset_type,
                sum(seq_delta = 0) AS dup_count,
                sum(seq_delta < 0 AND (ts_delta > %(reset_ns)s)) AS reset_count
            FROM (
                SELECT
                    symbol,
                    {cfg.seq_column} AS curr_seq,
                    lag({cfg.seq_column}) OVER (
                        PARTITION BY symbol ORDER BY {cfg.seq_column}
                    ) AS prev_seq,
                    {cfg.ts_column} AS curr_ts,
                    lag({cfg.ts_column}) OVER (
                        PARTITION BY symbol ORDER BY {cfg.seq_column}
                    ) AS prev_ts,
                    {cfg.seq_column} - lag({cfg.seq_column}) OVER (
                        PARTITION BY symbol ORDER BY {cfg.seq_column}
                    ) AS seq_delta,
                    {cfg.ts_column} - lag({cfg.ts_column}) OVER (
                        PARTITION BY symbol ORDER BY {cfg.seq_column}
                    ) AS ts_delta
                FROM {cfg.tick_table}
                WHERE trading_day = %(trading_day)s
            )
            WHERE prev_seq IS NOT NULL
            GROUP BY symbol
        """
        return self._execute_with_retry(sql, {"trading_day": trading_day, "reset_ns": reset_ns})

    def _query_orderbook_gaps(self, trading_day: str) -> list[tuple[Any, ...]]:
        cfg = self.config
        gap_ns = cfg.orderbook_gap_seconds * 1_000_000_000
        sql = f"""
            WITH book_windows AS (
                SELECT
                    symbol,
                    anyLast(asset_type) AS asset_type,
                    {cfg.ts_column} AS start_ts,
                    lead({cfg.ts_column}) OVER (
                        PARTITION BY symbol ORDER BY {cfg.ts_column}
                    ) AS end_ts
                FROM {cfg.orderbook_table}
                WHERE trading_day = %(trading_day)s
            )
            SELECT
                b.symbol,
                b.asset_type,
                toInt64(b.start_ts) AS start_ts,
                toInt64(b.end_ts) AS end_ts,
                count(t.{cfg.ts_column}) AS tick_count
            FROM book_windows AS b
            LEFT JOIN {cfg.tick_table} AS t
                ON t.symbol = b.symbol
                AND t.trading_day = %(trading_day)s
                AND t.{cfg.ts_column} BETWEEN b.start_ts AND b.end_ts
            WHERE b.end_ts IS NOT NULL
            GROUP BY b.symbol, b.asset_type, b.start_ts, b.end_ts
            HAVING (b.end_ts - b.start_ts) >= %(gap_ns)s AND tick_count >= %(min_tick)s
            ORDER BY b.symbol, b.start_ts
        """
        return self._execute_with_retry(
            sql,
            {"trading_day": trading_day, "gap_ns": gap_ns, "min_tick": self.config.min_tick_in_gap},
        )

    def _execute_with_retry(self, sql: str, params: dict[str, Any]) -> list[tuple[Any, ...]]:
        last_exc: Exception | None = None
        for attempt in range(self.config.max_retries):
            try:
                return self._run_query(sql, params)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                delay = self.config.retry_base_delay * (2**attempt)
                logger.warning(
                    "DataAuditor query failed (attempt %s/%s): %s",
                    attempt + 1,
                    self.config.max_retries,
                    exc,
                )
                if attempt + 1 >= self.config.max_retries:
                    break
                time.sleep(delay)
        if last_exc:
            raise last_exc
        return []

    def _run_query(self, sql: str, params: dict[str, Any]) -> list[tuple[Any, ...]]:
        if self.client is None:
            raise RuntimeError("No ClickHouse client configured.")
        return self.client.execute(sql, params)

    def _should_emit_book_gap(self, start_ts: int, end_ts: int, tick_count: int) -> bool:
        if tick_count < self.config.min_tick_in_gap:
            return False
        start_dt = self._to_local_datetime(start_ts)
        delta_ns = end_ts - start_ts
        required_ns = self._required_gap_ns(start_dt)
        if delta_ns < required_ns:
            return False
        if start_dt.time() > self.config.after_hours_end and tick_count == 0:
            return False
        return True

    def _required_gap_ns(self, start_dt: datetime) -> int:
        base_ns = self.config.orderbook_gap_seconds * 1_000_000_000
        if start_dt.time() <= self.config.auction_window_end:
            base_ns = int(base_ns * self.config.orderbook_opening_gap_multiplier)
        for window_start, window_end in self.config.stabilization_windows:
            if window_start <= start_dt.time() <= window_end:
                base_ns = int(base_ns * self.config.stabilization_gap_multiplier)
                break
        return base_ns

    def _to_local_datetime(self, ts_ns: int) -> datetime:
        seconds = ts_ns / 1_000_000_000
        return datetime.fromtimestamp(seconds, tz=self.config.timezone)

    def _sorted_tick_gaps(self, gaps: list[GapRange]) -> list[GapRange]:
        return sorted(
            gaps,
            key=lambda gap: (
                gap.symbol,
                gap.seq_start or 0,
                gap.start_ts,
                gap.end_ts,
            ),
        )

    def _sorted_orderbook_gaps(self, gaps: list[GapRange]) -> list[GapRange]:
        return sorted(gaps, key=lambda gap: (gap.symbol, gap.start_ts, gap.end_ts))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ClickHouse gap auditor for Shijim.")
    parser.add_argument("--trading-day", required=True, help="Trading day (YYYY-MM-DD) to audit.")
    parser.add_argument("--dsn", default="", help="ClickHouse DSN (clickhouse://user:pass@host:9000/db)")
    parser.add_argument("--tick-table", default="ticks", help="Tick table name")
    parser.add_argument("--orderbook-table", default="orderbook", help="Orderbook table name")
    parser.add_argument("--seq-column", default="seqno", help="Tick sequence column name")
    parser.add_argument(
        "--gap-seconds",
        type=int,
        default=5,
        help="Max allowed seconds without orderbook snapshot",
    )
    parser.add_argument(
        "--min-tick-in-gap",
        type=int,
        default=1,
        help="Minimum tick count inside a book gap",
    )
    parser.add_argument(
        "--output", default="missing_ranges.json", help="Path to write the gap report JSON"
    )
    parser.add_argument(
        "--auditor-version", default="1.0.0", help="Auditor version tag to embed in output"
    )
    return parser.parse_args()


def _main() -> int:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cfg = DataAuditorConfig(
        tick_table=args.tick_table,
        orderbook_table=args.orderbook_table,
        seq_column=args.seq_column,
        orderbook_gap_seconds=args.gap_seconds,
        min_tick_in_gap=args.min_tick_in_gap,
        auditor_version=args.auditor_version,
    )
    auditor = DataAuditor(dsn=args.dsn, config=cfg)
    report = auditor.audit_trading_day(args.trading_day)
    write_report(report, Path(args.output))
    logger.info(
        "Gap report generated for %s: %s tick gaps, %s orderbook gaps -> %s",
        args.trading_day,
        len(report.tick_gaps),
        len(report.orderbook_gaps),
        args.output,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(_main())
