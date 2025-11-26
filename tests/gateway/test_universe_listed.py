from __future__ import annotations

from shijim.gateway.navigator import UniverseNavigator


class DummyContract:
    def __init__(self, code: str) -> None:
        self.code = code
        self.symbol = code


class DummyStocks:
    def __init__(self, count: int) -> None:
        # mimic api.Contracts.Stocks.TSE/OTC as dict of contracts
        self.TSE = {f"{i:04d}": DummyContract(f"{i:04d}") for i in range(1, count + 1)}
        self.OTC = {}


class DummyAPI:
    def __init__(self, count: int) -> None:
        self.Contracts = type("C", (), {"Stocks": DummyStocks(count)})


def test_listed_universe_returns_limit_items(monkeypatch):
    # ensure scanners are bypassed
    monkeypatch.delenv("UNIVERSE_USE_SCANNER", raising=False)
    api = DummyAPI(count=1200)
    navigator = UniverseNavigator(api=api, clickhouse_client=None)

    universe = navigator.select_universe(["top_volume"], limit=1000)

    assert len(universe.stocks) == 1000
    # ensure codes are numeric and sorted
    assert universe.stocks[0].code == "0001"
    assert universe.stocks[-1].code == "1000"
