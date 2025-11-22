from __future__ import annotations

import logging
import os
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence

from shijim.gateway.sharding import ShardConfig, shard_config_from_env, shard_list

logger = logging.getLogger(__name__)
SNAPSHOT_BATCH_SIZE = 500


@dataclass(frozen=True)
class SmokeTestUniverse:
    """Small universe definition for smoke-test subscriptions."""

    futures: Sequence[str]
    stocks: Sequence[str]


def _parse_codes(value: str | None, default: Sequence[str]) -> list[str]:
    if not value:
        return list(default)
    parts = [item.strip() for item in value.split(",")]
    return [part for part in parts if part]


def get_smoke_test_universe() -> SmokeTestUniverse:
    """Return the default futures/stocks used in smoke tests.

    Override via env vars:
        * SHIJIM_SMOKE_FUTURES
        * SHIJIM_SMOKE_STOCKS
    """

    futures = _parse_codes(os.getenv("SHIJIM_SMOKE_FUTURES"), ["TXFR1"])
    stocks = _parse_codes(os.getenv("SHIJIM_SMOKE_STOCKS"), ["2330", "0050"])
    return SmokeTestUniverse(futures=futures, stocks=stocks)


def _flatten_stock_contracts(stocks_root: object | None) -> list[object]:
    """Traverse the Shioaji Contracts.Stocks structure and collect contracts."""
    if stocks_root is None:
        return []
    contracts: list[object] = []
    stack = [stocks_root]
    seen: set[int] = set()
    while stack:
        node = stack.pop()
        node_id = id(node)
        if node_id in seen:
            continue
        seen.add(node_id)
        if isinstance(node, dict):
            stack.extend(node.values())
            continue
        if isinstance(node, (list, tuple, set)):
            stack.extend(list(node))
            continue
        if isinstance(node, str) or node is None:
            continue
        # Heuristic: treat any object with a code as a stock contract leaf.
        if getattr(node, "code", None):
            contracts.append(node)
            continue
        try:
            node_vars = vars(node)
        except TypeError:
            continue
        attrs = [value for key, value in node_vars.items() if not key.startswith("_") and value is not None]
        stack.extend(attrs)
    return contracts


def _batched(items: Sequence[Any], size: int) -> list[list[Any]]:
    if size <= 0:
        raise ValueError("batch_size must be positive")
    return [list(items[idx : idx + size]) for idx in range(0, len(items), size)]


def _snapshot_volume(snapshot: Any) -> float:
    """Extract a sortable volume metric from a Shioaji snapshot."""
    if snapshot is None:
        return 0.0
    if isinstance(snapshot, dict):
        volume = (
            snapshot.get("yesterday_volume")
            or snapshot.get("total_volume")
            or snapshot.get("accumulate_trade_volume")
        )
        return float(volume or 0.0)
    volume = getattr(snapshot, "yesterday_volume", None)
    if volume is not None:
        return float(volume)
    for attr in ("total_volume", "accumulate_trade_volume"):
        value = getattr(snapshot, attr, None)
        if value is not None:
            return float(value)
    return 0.0


def get_top_volume_universe(api: object, limit: int = 1000, batch_size: int = SNAPSHOT_BATCH_SIZE) -> SmokeTestUniverse:
    """Return a universe containing the top-volume stocks across the market."""
    load_result = load_universe_with_retry(api, limit=limit, batch_size=batch_size)
    if load_result.source == "primary":
        logger.info("Universe loaded from primary source after %s attempt(s).", load_result.attempts)
    elif load_result.source == "fallback":
        logger.warning("Universe loaded from fallback after %s failed primary attempt(s).", load_result.attempts)
    return load_result.symbols


@dataclass(frozen=True)
class UniverseLoadResult:
    source: str
    symbols: SmokeTestUniverse
    attempts: int
    errors: list[str]


def load_universe_with_retry(api: object, *, limit: int = 1000, batch_size: int = SNAPSHOT_BATCH_SIZE) -> UniverseLoadResult:
    """Attempt to load universe with retries, falling back to safe list on failure."""
    max_retries = _int_env("UNIVERSE_MAX_RETRIES", 3)
    backoff = _float_env("UNIVERSE_RETRY_BACKOFF", 1.0)
    jitter = _float_env("UNIVERSE_RETRY_BACKOFF_JITTER", 0.2)

    errors: list[str] = []
    for attempt in range(1, max_retries + 1):
        try:
            primary = _load_top_volume(api, limit=limit, batch_size=batch_size)
            return UniverseLoadResult(source="primary", symbols=primary, attempts=attempt, errors=errors)
        except Exception as exc:  # noqa: BLE001
            err_msg = f"attempt={attempt} error={exc}"
            errors.append(err_msg)
            logger.warning("Universe load failed (%s/%s): %s", attempt, max_retries, exc)
            if attempt < max_retries:
                sleep_for = backoff * (2 ** (attempt - 1)) + random.uniform(0, jitter)
                time.sleep(sleep_for)

    # Fallback
    try:
        fallback_universe = _load_fallback_universe(limit=limit)
        logger.warning("Using fallback universe after %s failed attempts.", max_retries)
        return UniverseLoadResult(source="fallback", symbols=fallback_universe, attempts=max_retries, errors=errors)
    except Exception as exc:  # noqa: BLE001
        errors.append(f"fallback_error={exc}")
        logger.critical("Failed to load fallback universe; aborting. errors=%s", errors)
        raise


def _load_top_volume(api: object, limit: int, batch_size: int) -> SmokeTestUniverse:
    try:
        stocks_root = getattr(getattr(api, "Contracts", None), "Stocks", None)
    except Exception as exc:  # pragma: no cover - defensive
        raise RuntimeError(f"Unable to load stock contracts from API: {exc}") from exc

    contracts = _flatten_stock_contracts(stocks_root)
    if not contracts:
        raise RuntimeError("No stock contracts discovered from api.Contracts.Stocks.")

    rankings: list[tuple[str, float]] = []
    batches = _batched(contracts, batch_size)
    for batch in batches:
        try:
            snapshots = api.snapshots(batch)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Failed to fetch snapshots for batch size {len(batch)}: {exc}") from exc
        if snapshots is None:
            continue
        for contract, snapshot in zip(batch, snapshots):
            code = getattr(contract, "code", None) or getattr(contract, "symbol", None)
            if not code:
                continue
            rankings.append((code, _snapshot_volume(snapshot)))

    if not rankings:
        raise RuntimeError("No snapshots returned; top-volume universe is empty.")

    rankings.sort(key=lambda item: item[1], reverse=True)
    top_codes: list[str] = []
    seen: set[str] = set()
    for code, _ in rankings:
        if code in seen:
            continue
        seen.add(code)
        top_codes.append(code)
        if len(top_codes) >= limit:
            break

    return SmokeTestUniverse(futures=[], stocks=top_codes)


def _load_fallback_universe(limit: int) -> SmokeTestUniverse:
    path_str = os.getenv("UNIVERSE_FALLBACK_PATH")
    if path_str:
        path = Path(path_str)
        if not path.exists():
            raise RuntimeError(f"Fallback universe file not found: {path}")
        try:
            import json

            payload = json.loads(path.read_text(encoding="utf-8"))
            stocks = [str(code) for code in payload.get("stocks", [])][:limit]
            futures = [str(code) for code in payload.get("futures", [])][:limit]
            if not stocks and not futures:
                raise ValueError("Fallback file contains no symbols.")
            return SmokeTestUniverse(futures=futures, stocks=stocks)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"Failed to load fallback universe from {path}: {exc}") from exc

    # Built-in safe fallback
    safe_stocks = ["2330", "0050", "2317", "2412"][:limit]
    safe_futures = ["TXFR1"]
    return SmokeTestUniverse(futures=safe_futures, stocks=safe_stocks)


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def shard_universe(universe: SmokeTestUniverse, config: ShardConfig | None = None) -> SmokeTestUniverse:
    """Split a full-universe definition into a worker-specific slice."""
    config = config or shard_config_from_env()
    return SmokeTestUniverse(
        futures=shard_list(universe.futures, config),
        stocks=shard_list(universe.stocks, config),
    )
