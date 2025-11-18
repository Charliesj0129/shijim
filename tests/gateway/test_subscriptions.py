from __future__ import annotations

from types import SimpleNamespace

from shijim.gateway.subscriptions import SubscriptionManager, SubscriptionPlan


class DummyContract:
    def __init__(self, code: str) -> None:
        self.code = code


class FakeQuoteAPI:
    def __init__(self) -> None:
        self.subscribe_calls: list[dict] = []
        self.unsubscribe_calls: list[dict] = []

    def subscribe(self, contract, **kwargs):
        self.subscribe_calls.append({"contract": contract, **kwargs})

    def unsubscribe(self, contract, **kwargs):
        self.unsubscribe_calls.append({"contract": contract, **kwargs})


class FakeSession:
    def __init__(self, contracts: dict[tuple[str, str], DummyContract]) -> None:
        self._contracts_map = contracts
        self._api = SimpleNamespace(quote=FakeQuoteAPI())

    def get_api(self):
        return self._api

    def ensure_contracts_loaded(self) -> None:
        return None

    def get_contract(self, code: str, asset_type: str):
        return self._contracts_map[(asset_type, code)]


def _manager(plan: SubscriptionPlan, **kwargs) -> tuple[SubscriptionManager, FakeQuoteAPI]:
    contracts = {}
    for code in plan.futures:
        contracts[("futures", code)] = DummyContract(code)
    for code in plan.stocks:
        contracts[("stock", code)] = DummyContract(code)
    session = FakeSession(contracts)
    manager = SubscriptionManager(session=session, plan=plan, **kwargs)
    return manager, session.get_api().quote


def test_subscribe_universe_subscribes_tick_and_bidask() -> None:
    plan = SubscriptionPlan(futures=["TXF1", "TXF2"], stocks=["2330"])
    manager, quote = _manager(plan, batch_size=10, batch_sleep=0, sleep_func=lambda _d: None)

    manager.subscribe_universe()

    calls = quote.subscribe_calls
    expected = []
    for code in plan.futures + plan.stocks:
        expected.append(("tick", code))
        expected.append(("bidask", code))
    assert [(call["quote_type"], call["contract"].code) for call in calls] == expected


def test_subscribe_universe_batches_and_sleeps_between_batches() -> None:
    plan = SubscriptionPlan(futures=[f"TXF{i}" for i in range(5)])
    sleep_calls: list[float] = []
    manager, _ = _manager(
        plan,
        batch_size=2,
        batch_sleep=0.1,
        sleep_func=lambda duration: sleep_calls.append(duration),
    )

    manager.subscribe_universe()

    assert sleep_calls == [0.1, 0.1]


def test_subscribe_universe_skips_duplicates() -> None:
    plan = SubscriptionPlan(futures=["TXF1", "TXF1"], stocks=["2330", "2330"])
    manager, quote = _manager(plan, batch_size=1, batch_sleep=0)

    manager.subscribe_universe()
    manager.subscribe_universe()

    # only first call per asset_type/code pair should be issued
    assert len(quote.subscribe_calls) == 4


def test_unsubscribe_all_invokes_unsubscribe_for_each_contract() -> None:
    plan = SubscriptionPlan(futures=["TXF1"], stocks=["2330"])
    manager, quote = _manager(plan)

    manager.subscribe_universe()
    manager.unsubscribe_all()

    assert len(quote.unsubscribe_calls) == 4
