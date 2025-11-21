from __future__ import annotations

from shijim.gateway.navigator import RankedSymbol, RankedUniverse, UniverseNavigator
from shijim.gateway.sharding import ShardConfig


def test_shard_universe_binpacks_by_weight():
    navigator = UniverseNavigator(api=object(), clickhouse_client=None)
    universe = RankedUniverse(
        futures=[],
        stocks=[
            RankedSymbol("A", "stock", 10.0),
            RankedSymbol("B", "stock", 5.0),
            RankedSymbol("C", "stock", 3.0),
            RankedSymbol("D", "stock", 2.0),
        ],
    )

    shard0 = navigator.shard_universe(universe, ShardConfig(shard_id=0, total_shards=2))
    shard1 = navigator.shard_universe(universe, ShardConfig(shard_id=1, total_shards=2))

    owned_codes = {item.code for item in shard0.stocks}.union({item.code for item in shard1.stocks})
    assert owned_codes == {"A", "B", "C", "D"}

    load0 = sum(item.weight for item in shard0.stocks)
    load1 = sum(item.weight for item in shard1.stocks)
    assert load0 == 10.0
    assert load1 == 10.0
