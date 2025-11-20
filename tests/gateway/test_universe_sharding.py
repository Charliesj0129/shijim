from __future__ import annotations

from shijim.gateway.sharding import (
    ShardConfig,
    get_shard_indices,
    shard_config_from_env,
    shard_list,
)
from shijim.gateway.universe import SmokeTestUniverse, shard_universe


def test_shard_universe_is_covering_and_disjoint():
    universe = SmokeTestUniverse(
        futures=[f"TX{i}" for i in range(13)],
        stocks=[f"STK{i:03d}" for i in range(8)],
    )

    shards = [
        shard_universe(universe, ShardConfig(shard_id=idx, total_shards=5)) for idx in range(5)
    ]

    combined_futures = [code for shard in shards for code in shard.futures]
    combined_stocks = [code for shard in shards for code in shard.stocks]

    assert sorted(combined_futures) == sorted(universe.futures)
    assert len(combined_futures) == len(set(combined_futures))

    assert sorted(combined_stocks) == sorted(universe.stocks)
    assert len(combined_stocks) == len(set(combined_stocks))


def test_get_shard_indices_handles_remainders():
    assert get_shard_indices(5, ShardConfig(shard_id=0, total_shards=3)) == (0, 2)
    assert get_shard_indices(5, ShardConfig(shard_id=1, total_shards=3)) == (2, 4)
    assert get_shard_indices(5, ShardConfig(shard_id=2, total_shards=3)) == (4, 5)


def test_shard_list_returns_empty_for_small_slices():
    items = list(range(3))
    empty_shard = shard_list(items, ShardConfig(shard_id=3, total_shards=5))
    assert empty_shard == []


def test_shard_config_from_env_defaults_and_clamps(monkeypatch):
    monkeypatch.delenv("TOTAL_SHARDS", raising=False)
    monkeypatch.delenv("SHARD_ID", raising=False)

    default_cfg = shard_config_from_env()
    assert default_cfg.total_shards == 1
    assert default_cfg.shard_id == 0

    monkeypatch.setenv("TOTAL_SHARDS", "-3")
    monkeypatch.setenv("SHARD_ID", "99")
    clamped_cfg = shard_config_from_env()
    assert clamped_cfg.total_shards == 1
    assert clamped_cfg.shard_id == 0
