from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Sequence

from shijim.gateway.sharding import ShardConfig
from shijim.gateway.subscriptions import SubscriptionPlan
from shijim.gateway.universe import _batched, _flatten_stock_contracts, _snapshot_volume

try:  # pragma: no cover - optional import for scanners
    from shioaji.constant import ScannerType
except Exception:  # pragma: no cover
    ScannerType = None  # type: ignore

try:  # pragma: no cover - tzdata optional
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None

logger = logging.getLogger(__name__)

if ZoneInfo:
    try:
        TAIWAN_TZ = ZoneInfo("Asia/Taipei")
    except Exception:  # pragma: no cover - tzdata missing
        TAIWAN_TZ = timezone(timedelta(hours=8))
else:  # pragma: no cover
    TAIWAN_TZ = timezone(timedelta(hours=8))


@dataclass(slots=True)
class RankedSymbol:
    code: str
    asset_type: str
    weight: float
    metadata: dict[str, Any] = field(default_factory=dict)

    def with_weight(self, weight: float) -> "RankedSymbol":
        updated = dict(self.metadata)
        updated["base_weight"] = self.weight
        return RankedSymbol(code=self.code, asset_type=self.asset_type, weight=weight, metadata=updated)


@dataclass
class RankedUniverse:
    futures: list[RankedSymbol] = field(default_factory=list)
    stocks: list[RankedSymbol] = field(default_factory=list)

    def as_plan(self) -> SubscriptionPlan:
        return SubscriptionPlan(
            futures=[item.code for item in self.futures],
            stocks=[item.code for item in self.stocks],
        )


class UniverseNavigator:
    """Strategy-driven selector for trading universes with load-aware sharding."""

    def __init__(
        self,
        api: object,
        *,
        clickhouse_client: Any | None = None,
        clickhouse_dsn: str | None = None,
        now_fn: Callable[[], datetime] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.api = api
        self.now_fn = now_fn or self._taipei_now
        self.logger = logger or logging.getLogger(__name__)
        self.client = clickhouse_client or self._build_clickhouse_client(clickhouse_dsn)

    def select_universe(
        self,
        strategies: Sequence[str],
        *,
        limit: int = 1000,
        min_size: int | None = None,
        lookback_days: int = 5,
        tick_table: str = "ticks",
        enforce_liquidity: bool = True,
    ) -> RankedUniverse:
        normalized = [name.strip().lower() for name in strategies if name.strip()]
        if not normalized:
            normalized = ["top_volume"]

        pool: dict[tuple[str, str], RankedSymbol] = {}
        for name in normalized:
            results = self._run_strategy(
                name=name,
                limit=limit,
                lookback_days=lookback_days,
                tick_table=tick_table,
            )
            weight_multiplier = self._strategy_weight(name)
            for symbol in results:
                symbol.weight *= weight_multiplier
                symbol.metadata.setdefault("sources", []).append(name)
                key = (symbol.asset_type, symbol.code)
                if key in pool:
                    existing = pool[key]
                    existing.weight += symbol.weight
                    existing.metadata.setdefault("sources", []).extend(symbol.metadata.get("sources", []))
                    existing.metadata.update({k: v for k, v in symbol.metadata.items() if k != "sources"})
                else:
                    pool[key] = symbol

        augmented = self._enrich_with_tick_rate(pool, tick_table=tick_table)

        futures = [item for item in augmented.values() if item.asset_type == "futures"]
        stocks = [item for item in augmented.values() if item.asset_type == "stock"]

        if enforce_liquidity:
            stocks = [item for item in stocks if item.weight > 0]
            futures = [item for item in futures if item.weight > 0]

        futures.sort(key=lambda item: item.weight, reverse=True)
        stocks.sort(key=lambda item: item.weight, reverse=True)

        universe = RankedUniverse(
            futures=futures[:limit],
            stocks=stocks[:limit],
        )
        return self._maybe_fill_with_top_volume(universe, target=min_size or limit)

    def shard_universe(self, universe: RankedUniverse, shard: ShardConfig) -> RankedUniverse:
        allocations = _bin_pack_symbols(universe.futures + universe.stocks, shard.total_shards)
        if shard.shard_id >= len(allocations):
            return RankedUniverse()
        assigned = allocations[shard.shard_id]
        return RankedUniverse(
            futures=[item for item in assigned if item.asset_type == "futures"],
            stocks=[item for item in assigned if item.asset_type == "stock"],
        )

    # ------------------------------------------------------------------ #
    # Strategy runners
    # ------------------------------------------------------------------ #
    def _run_strategy(
        self,
        *,
        name: str,
        limit: int,
        lookback_days: int,
        tick_table: str,
    ) -> list[RankedSymbol]:
        if name == "top_volume":
            return self._rank_top_volume(limit=limit)
        if name == "high_volatility":
            return self._rank_high_volatility(limit=limit, lookback_days=lookback_days, tick_table=tick_table)
        if name == "unusual_activity":
            return self._rank_unusual_activity(limit=limit, tick_table=tick_table)
        self.logger.warning("Unknown universe strategy: %s", name)
        return []

    def _rank_top_volume(self, *, limit: int) -> list[RankedSymbol]:
        # 若設定 UNIVERSE_USE_SCANNER != 1，直接取上市/上櫃合約前 N 檔，避免掃描為空
        if os.getenv("UNIVERSE_USE_SCANNER", "0") not in ("1", "true", "yes"):
            return _listed_stock_universe(self.api, limit)
        if ScannerType is None:
            self.logger.error("TopVolume: ScannerType unavailable; cannot use api.scanners.")
            return _listed_stock_universe(self.api, limit)
        max_retries = int(os.getenv("UNIVERSE_SCANNER_RETRIES", "3") or 3)
        backoff = float(os.getenv("UNIVERSE_SCANNER_BACKOFF", "0.5") or 0.5)
        scanner_types = _scanner_types_from_env()
        ascending = os.getenv("UNIVERSE_SCANNER_ASCENDING", "false").lower() in ("1", "true", "yes")
        scanner_date = os.getenv("UNIVERSE_SCANNER_DATE", self.now_fn().date().isoformat())
        scanner_count = min(limit, int(os.getenv("UNIVERSE_SCANNER_COUNT", str(limit or 200)) or 200))

        rankings: list[RankedSymbol] = []
        for stype in scanner_types:
            last_exc: Exception | None = None
            for attempt in range(1, max_retries + 1):
                try:
                    scanners = self.api.scanners(
                        scanner_type=stype,
                        date=scanner_date,
                        count=scanner_count,
                        ascending=ascending,
                    )
                    for item in scanners or []:
                        code = getattr(item, "code", None)
                        if not code or not _looks_like_stock_code(code):
                            continue
                        weight = _scanner_weight(item, stype)
                        rankings.append(
                            RankedSymbol(
                                code=str(code),
                                asset_type="stock",
                                weight=weight,
                                metadata={"source": f"top_volume_scanner_{stype.name.lower()}", "scanner_type": stype.name},
                            )
                        )
                    if rankings:
                        break
                    self.logger.warning("TopVolume: scanners %s returned empty on attempt %s/%s.", stype.name, attempt, max_retries)
                except Exception as exc:  # noqa: BLE001
                    last_exc = exc
                    if attempt < max_retries:
                        sleep_for = backoff * (2 ** (attempt - 1))
                        self.logger.warning(
                            "TopVolume: scanners %s failed (attempt %s/%s): %s; retrying in %.2fs",
                            stype.name,
                            attempt,
                            max_retries,
                            exc,
                            sleep_for,
                        )
                        time.sleep(sleep_for)
                    else:
                        self.logger.error("TopVolume: scanners %s failed after %s attempts: %s", stype.name, max_retries, exc)
            if rankings:
                break

        if not rankings:
            self.logger.warning("TopVolume: scanners empty; falling back to contracts list.")
            try:
                stocks_root = getattr(getattr(self.api, "Contracts", None), "Stocks", None)
            except Exception as exc:  # pragma: no cover
                self.logger.error("TopVolume: unable to load contracts for fallback: %s", exc)
                return []
            contracts = _primary_stock_contracts(stocks_root)
            if not contracts:
                contracts = _flatten_stock_contracts(stocks_root)
            fallback: list[RankedSymbol] = []
            seen: set[str] = set()
            for contract in contracts:
                code = getattr(contract, "code", None) or getattr(contract, "symbol", None)
                if not code or code in seen or not _looks_like_stock_code(code):
                    continue
                seen.add(code)
                fallback.append(RankedSymbol(code=code, asset_type="stock", weight=1.0, metadata={"source": "top_volume_fallback"}))
                if len(fallback) >= limit:
                    break
            return fallback
        return rankings[:limit]

    def _rank_high_volatility(
        self,
        *,
        limit: int,
        lookback_days: int,
        tick_table: str,
    ) -> list[RankedSymbol]:
        if self.client is None:
            self.logger.info("HighVolatility: ClickHouse client missing; skipping.")
            return []
        sql = f"""
            SELECT
                symbol,
                any(asset_type) AS asset_type,
                stddevPop(price) AS sigma,
                count() AS ticks
            FROM {tick_table}
            WHERE trading_day >= toDate(%(end_day)s) - %(lookback)s
            GROUP BY symbol
            HAVING ticks >= %(min_ticks)s AND sigma > 0
            ORDER BY sigma DESC
            LIMIT %(limit)s
        """
        params = {
            "end_day": self._taipei_now().date().isoformat(),
            "lookback": lookback_days,
            "min_ticks": 50,
            "limit": limit,
        }
        try:
            rows = self.client.execute(sql, params)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("HighVolatility: query failed: %s", exc)
            return []

        ranked: list[RankedSymbol] = []
        for symbol, asset_type, sigma, ticks in rows:
            ranked.append(
                RankedSymbol(
                    code=symbol,
                    asset_type=str(asset_type or "stock"),
                    weight=float(sigma or 0.0),
                    metadata={"source": "high_volatility", "ticks": int(ticks or 0)},
                )
            )
        return ranked

    def _rank_unusual_activity(self, *, limit: int, tick_table: str) -> list[RankedSymbol]:
        if self.client is None:
            self.logger.info("UnusualActivity: ClickHouse client missing; skipping.")
            return []
        now = self._taipei_now()
        window_minutes = int(os.getenv("PREMARKET_WINDOW_MINUTES", "20"))
        start_dt = now - timedelta(minutes=window_minutes)
        start_ns = int(start_dt.timestamp() * 1_000_000_000)
        end_ns = int(now.timestamp() * 1_000_000_000)

        sql = f"""
            SELECT
                symbol,
                any(asset_type) AS asset_type,
                sum(size) AS total_size,
                max(price) - min(price) AS price_span,
                count() AS ticks
            FROM {tick_table}
            WHERE ts BETWEEN %(start_ns)s AND %(end_ns)s
            GROUP BY symbol
            HAVING ticks >= %(min_ticks)s AND total_size > 0
            ORDER BY price_span DESC, total_size DESC
            LIMIT %(limit)s
        """
        params = {
            "start_ns": start_ns,
            "end_ns": end_ns,
            "min_ticks": 10,
            "limit": limit,
        }
        try:
            rows = self.client.execute(sql, params)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("UnusualActivity: query failed: %s", exc)
            return []

        ranked: list[RankedSymbol] = []
        for symbol, asset_type, total_size, price_span, ticks in rows:
            weight = float(price_span or 0.0) + float(total_size or 0.0) * 0.001
            ranked.append(
                RankedSymbol(
                    code=symbol,
                    asset_type=str(asset_type or "stock"),
                    weight=weight,
                    metadata={
                        "source": "unusual_activity",
                        "activity_ticks": int(ticks or 0),
                        "activity_volume": float(total_size or 0.0),
                        "price_span": float(price_span or 0.0),
                    },
                )
            )
        return ranked

    def _build_clickhouse_client(self, dsn: str | None) -> Any | None:
        if not dsn:
            return None
        try:
            from clickhouse_driver import Client  # type: ignore
        except Exception as exc:  # pragma: no cover - optional
            self.logger.warning("ClickHouse client unavailable: %s", exc)
            return None
        return Client.from_url(dsn)

    def _strategy_weight(self, name: str) -> float:
        base = {
            "top_volume": 1.0,
            "high_volatility": 1.2,
            "unusual_activity": 1.1,
        }
        return base.get(name, 1.0)

    def _taipei_now(self) -> datetime:
        return datetime.now(tz=TAIWAN_TZ)

    # ------------------------------------------------------------------ #
    # Load estimation and fallback helpers
    # ------------------------------------------------------------------ #
    def _enrich_with_tick_rate(
        self,
        pool: dict[tuple[str, str], RankedSymbol],
        *,
        tick_table: str,
    ) -> dict[tuple[str, str], RankedSymbol]:
        if self.client is None or not pool:
            return pool
        symbols = list({code for _, code in pool.keys()})
        if not symbols:
            return pool
        sql = f"""
            SELECT
                symbol,
                count() AS cnt,
                (max(ts) - min(ts)) AS span_ns
            FROM {tick_table}
            WHERE symbol IN %(symbols)s
              AND trading_day = toDate(%(day)s)
            GROUP BY symbol
        """
        params = {"symbols": symbols, "day": self._taipei_now().date().isoformat()}
        try:
            rows = self.client.execute(sql, params)
        except Exception as exc:  # noqa: BLE001
            self.logger.warning("Tick rate enrichment failed: %s", exc)
            return pool

        density: dict[str, float] = {}
        for symbol, count, span_ns in rows or []:
            if not span_ns or span_ns <= 0:
                continue
            density[str(symbol)] = float(count) / (float(span_ns) / 1_000_000_000)

        enriched: dict[tuple[str, str], RankedSymbol] = {}
        for key, symbol in pool.items():
            rate = density.get(symbol.code, 0.0)
            if rate > 0:
                weight = symbol.weight * (1.0 + rate / 1000.0)
                enriched[key] = symbol.with_weight(weight)
                enriched[key].metadata["tick_rate_hz"] = rate
            else:
                enriched[key] = symbol
        return enriched

    def _maybe_fill_with_top_volume(self, universe: RankedUniverse, target: int) -> RankedUniverse:
        total = len(universe.futures) + len(universe.stocks)
        if total >= target:
            return universe
        needed = target - total
        fallback = self._rank_top_volume(limit=needed)
        existing_codes = {s.code for s in universe.stocks}
        for symbol in fallback:
            if symbol.code in existing_codes:
                continue
            universe.stocks.append(symbol)
            existing_codes.add(symbol.code)
            if len(universe.stocks) + len(universe.futures) >= target:
                break
        universe.stocks.sort(key=lambda item: item.weight, reverse=True)
        return universe


def _bin_pack_symbols(symbols: Iterable[RankedSymbol], shard_count: int) -> list[list[RankedSymbol]]:
    shards: list[list[RankedSymbol]] = [[] for _ in range(max(shard_count, 1))]
    loads: list[float] = [0.0 for _ in shards]
    for symbol in sorted(symbols, key=lambda item: item.weight, reverse=True):
        idx = loads.index(min(loads))
        shards[idx].append(symbol)
        loads[idx] += max(symbol.weight, 0.0)
    return shards


def _looks_like_stock_code(code: str) -> bool:
    """Heuristic filter to avoid exotic codes when snapshots are unavailable."""
    if not code:
        return False
    trimmed = code.strip().upper()
    # default pattern：純數字 4~6 位，或數字+單一尾碼字母
    pattern = os.getenv("UNIVERSE_STOCK_CODE_REGEX", r"^\d{4,6}[A-Z]?$")
    try:
        return re.match(pattern, trimmed) is not None
    except re.error:
        return trimmed.isdigit() and 4 <= len(trimmed) <= 6


def _primary_stock_contracts(stocks_root: object | None) -> list[object]:
    """Prefer known exchanges (TSE/OTC) to avoid exotic nodes in flatten."""
    if stocks_root is None:
        return []
    preferred: list[object] = []
    for market in ("TSE", "OTC"):
        try:
            market_node = getattr(stocks_root, market, None)
            if isinstance(market_node, dict):
                preferred.extend(market_node.values())
            elif market_node is not None:
                try:
                    preferred.extend(list(market_node))
                except Exception:  # pragma: no cover - best effort
                    continue
        except Exception:  # pragma: no cover - defensive
            continue
    return preferred


def _scanner_types_from_env() -> list[Any]:
    default_types = ["VolumeRank", "AmountRank", "ChangePercentRank"]
    raw = os.getenv("UNIVERSE_SCANNER_TYPES")
    names = [name.strip() for name in raw.split(",")] if raw else default_types
    types: list[Any] = []
    if ScannerType is None:
        return types
    for name in names:
        stype = getattr(ScannerType, name, None)
        if stype is not None:
            types.append(stype)
    return types or ([ScannerType.VolumeRank] if ScannerType else [])


def _scanner_weight(item: Any, stype: Any) -> float:
    """Choose weight based on scanner type fields."""
    if stype == getattr(ScannerType, "AmountRank", None):
        return float(getattr(item, "total_amount", 0.0) or getattr(item, "amount", 0.0) or 1.0)
    if stype == getattr(ScannerType, "VolumeRank", None):
        return float(getattr(item, "total_volume", 0.0) or getattr(item, "volume", 0.0) or 1.0)
    return float(getattr(item, "total_volume", 0.0) or getattr(item, "volume", 0.0) or 1.0)


def _listed_stock_universe(api: object, limit: int) -> list[RankedSymbol]:
    """直接取上市/上櫃合約前 N 檔（排序後），避免 scanner/snapshot 為空。"""
    try:
        stocks_root = getattr(getattr(api, "Contracts", None), "Stocks", None)
        tse = getattr(stocks_root, "TSE", None) if stocks_root is not None else None
        otc = getattr(stocks_root, "OTC", None) if stocks_root is not None else None
    except Exception:
        return []
    contracts: list[object] = []
    for market in (tse, otc):
        if isinstance(market, dict):
            contracts.extend(market.values())
        elif market is not None:
            try:
                contracts.extend(list(market))
            except Exception:
                continue
    filtered: list[RankedSymbol] = []
    seen: set[str] = set()
    # 優先數字排序
    sorted_contracts = sorted(
        contracts,
        key=lambda c: (getattr(c, "code", "") or getattr(c, "symbol", "") or ""),
    )
    for contract in sorted_contracts:
        code = getattr(contract, "code", None) or getattr(contract, "symbol", None)
        if not code or code in seen or not _looks_like_stock_code(code):
            continue
        seen.add(code)
        filtered.append(RankedSymbol(code=code, asset_type="stock", weight=1.0, metadata={"source": "listed_fallback"}))
        if len(filtered) >= limit:
            break
    return filtered
