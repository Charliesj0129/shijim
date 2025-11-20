from __future__ import annotations

import logging
import os
from dataclasses import dataclass
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
    try:
        stocks_root = getattr(getattr(api, "Contracts", None), "Stocks", None)
    except Exception as exc:  # pragma: no cover - defensive
        logger.warning("Unable to load stock contracts from API: %s", exc)
        return SmokeTestUniverse(futures=[], stocks=[])

    contracts = _flatten_stock_contracts(stocks_root)
    if not contracts:
        logger.warning("No stock contracts discovered from api.Contracts.Stocks.")
        return SmokeTestUniverse(futures=[], stocks=[])

    rankings: list[tuple[str, float]] = []
    batches = _batched(contracts, batch_size)
    for batch in batches:
        try:
            snapshots = api.snapshots(batch)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Failed to fetch snapshots for %s stock contracts: %s", len(batch), exc)
            continue
        if snapshots is None:
            continue
        for contract, snapshot in zip(batch, snapshots):
            code = getattr(contract, "code", None) or getattr(contract, "symbol", None)
            if not code:
                continue
            rankings.append((code, _snapshot_volume(snapshot)))

    if not rankings:
        logger.warning("No snapshots returned; top-volume universe is empty.")
        return SmokeTestUniverse(futures=[], stocks=[])

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


def shard_universe(universe: SmokeTestUniverse, config: ShardConfig | None = None) -> SmokeTestUniverse:
    """Split a full-universe definition into a worker-specific slice."""
    config = config or shard_config_from_env()
    return SmokeTestUniverse(
        futures=shard_list(universe.futures, config),
        stocks=shard_list(universe.stocks, config),
    )
