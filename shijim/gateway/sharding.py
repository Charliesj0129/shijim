"""Helpers for slicing a universe across worker shards."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import List, Sequence, TypeVar

T = TypeVar("T")


@dataclass(frozen=True)
class ShardConfig:
    """Represents the worker's shard assignment."""

    shard_id: int
    total_shards: int


def shard_config_from_env() -> ShardConfig:
    """Read shard configuration from SHARD_ID/TOTAL_SHARDS."""
    shard_raw = os.getenv("SHARD_ID", "0")
    total_raw = os.getenv("TOTAL_SHARDS", "1")
    shard_id = _safe_int(shard_raw, default=0)
    total_shards = _safe_int(total_raw, default=1)
    if total_shards <= 0:
        total_shards = 1
    if shard_id < 0 or shard_id >= total_shards:
        shard_id = 0
    return ShardConfig(shard_id=shard_id, total_shards=total_shards)


def get_shard_indices(total_items: int, config: ShardConfig | None = None) -> tuple[int, int]:
    """Return the [start, end) slice allocated to this shard."""
    if total_items <= 0:
        return (0, 0)
    config = config or shard_config_from_env()
    base = total_items // config.total_shards
    remainder = total_items % config.total_shards
    if config.shard_id < remainder:
        start = config.shard_id * (base + 1)
        end = start + (base + 1)
    else:
        start = config.shard_id * base + remainder
        end = start + base
    return (start, min(end, total_items))


def shard_list(items: Sequence[T], config: ShardConfig | None = None) -> List[T]:
    """Return the subset of items assigned to this shard."""
    start, end = get_shard_indices(len(items), config)
    return list(items[start:end])


def _safe_int(value: str, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return default
