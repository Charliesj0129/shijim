"""Gateway components for the Shijim market data stack."""

from __future__ import annotations

from .context import CollectorContext, attach_quote_callbacks
from .session import ShioajiSession
from .pool import ConnectionPool
from .filter import ContractFilter
from .sharding import (
    ShardConfig,
    get_shard_indices,
    shard_config_from_env,
    shard_list,
)
from .subscriptions import SubscriptionManager, SubscriptionPlan
from .universe import get_smoke_test_universe, get_top_volume_universe, shard_universe

__all__ = [
    "CollectorContext",
    "attach_quote_callbacks",
    "ShioajiSession",
    "ConnectionPool",
    "ContractFilter",
    "SubscriptionManager",
    "SubscriptionPlan",
    "get_smoke_test_universe",
    "get_top_volume_universe",
    "ShardConfig",
    "shard_config_from_env",
    "get_shard_indices",
    "shard_list",
    "shard_universe",
]
