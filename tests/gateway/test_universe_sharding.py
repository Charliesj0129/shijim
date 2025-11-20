from __future__ import annotations

import types

from shijim.gateway.sharding import (
    ShardConfig,
    get_shard_indices,
    shard_config_from_env,
    shard_list,
)
from shijim.gateway.universe import SmokeTestUniverse, get_top_volume_universe, shard_universe


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


def test_get_top_volume_universe_batches_and_ranks():
    class DummyContract:
        def __init__(self, code: str) -> None:
            self.code = code

    class DummyAPI:
        def __init__(self, stocks: object, volumes: dict[str, int]) -> None:
            self.Contracts = types.SimpleNamespace(Stocks=stocks)
            self._volumes = volumes
            self.snapshot_calls: list[list[DummyContract]] = []

        def snapshots(self, batch):
            self.snapshot_calls.append(list(batch))
            return [{"yesterday_volume": self._volumes.get(contract.code, 0)} for contract in batch]

    stocks = {
        "TSE": {"2330": DummyContract("2330"), "2317": DummyContract("2317")},
        "OTC": {"0050": DummyContract("0050")},
    }
    api = DummyAPI(stocks, volumes={"2330": 5_000_000, "2317": 1_000_000, "0050": 100})

    universe = get_top_volume_universe(api, limit=2, batch_size=2)

    assert universe.stocks == ["2330", "2317"]
    assert universe.futures == []
    assert len(api.snapshot_calls) == 2
    assert all(len(batch) <= 2 for batch in api.snapshot_calls)
