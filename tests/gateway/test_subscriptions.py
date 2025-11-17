from __future__ import annotations

from types import SimpleNamespace

from shijim.gateway.subscriptions import SubscriptionManager


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


def _manager(futures, stocks, **kwargs) -> SubscriptionManager:
    quote = FakeQuoteAPI()
    api = SimpleNamespace(quote=quote)
    manager = SubscriptionManager(
        api=api,
        futures_contracts=futures,
        stock_contracts=stocks,
        **kwargs,
    )
    return manager


def test_subscribe_universe_subscribes_tick_and_bidask():
    futures = [DummyContract("TXF1"), DummyContract("TXF2")]
    stocks = [DummyContract("2330")]
    manager = _manager(
        futures,
        stocks,
        batch_size=2,
        batch_sleep=0,
        sleep_func=lambda _duration: None,
    )

    manager.subscribe_universe()

    calls = manager._quote.subscribe_calls  # type: ignore[attr-defined]
    expected_order = []
    for contract in futures + stocks:
        expected_order.append(("tick", contract.code))
        expected_order.append(("bidask", contract.code))

    actual = [(call["quote_type"], call["contract"].code) for call in calls]
    assert actual == expected_order


def test_subscribe_universe_batches_and_sleeps_between_batches():
    futures = [DummyContract(f"TXF{i}") for i in range(5)]
    stocks: list[DummyContract] = []
    sleep_calls: list[float] = []

    manager = _manager(
        futures,
        stocks,
        batch_size=2,
        batch_sleep=0.1,
        sleep_func=lambda duration: sleep_calls.append(duration),
    )
    manager.subscribe_universe()

    assert sleep_calls == [0.1, 0.1]


def test_unsubscribe_all_invokes_unsubscribe_for_each_contract():
    futures = [DummyContract("TXF1")]
    stocks = [DummyContract("2330")]
    manager = _manager(
        futures,
        stocks,
        batch_size=10,
        batch_sleep=0,
        sleep_func=lambda _duration: None,
    )
    manager.subscribe_universe()
    manager.unsubscribe_all()

    calls = manager._quote.unsubscribe_calls  # type: ignore[attr-defined]
    assert len(calls) == 4  # Tick + BidAsk for 2 contracts
    types = [call["quote_type"] for call in calls]
    assert types.count("tick") == 2
    assert types.count("bidask") == 2
