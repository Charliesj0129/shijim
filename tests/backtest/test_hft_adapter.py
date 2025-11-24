import numpy as np

from shijim.backtest.converter import SbeEvent, convert_events_to_np
from shijim.backtest.adapter import HftBacktestAdapter
from shijim.strategy.engine import SmartChasingEngine, StrategyConfig, OrderStateManager, OrderState
from shijim.strategy.ofi import BboState, OfiCalculator


def test_convert_events_to_np():
    events = [
        SbeEvent(1, 1000, 1005, 1, 100.0, 2.0),
        SbeEvent(2, 1010, 1015, -1, 99.5, 1.0),
    ]
    arr = convert_events_to_np(events)
    assert arr.dtype.names == ("ev", "exch_ts", "local_ts", "side", "px", "qty")
    assert np.allclose(arr["px"], [100.0, 99.5])


class DummyExecutor:
    def __init__(self):
        self.submits = []
        self.cancels = []

    def submit_buy_order(self, price: float, qty: float):
        self.submits.append((price, qty))

    def cancel_order(self, broker_order_id=None):
        self.cancels.append(broker_order_id)


def create_engine():
    bbo = BboState(100.0, 10.0, 101.0, 10.0)
    cfg = StrategyConfig(chase_threshold=2.0, max_chase_round=3)
    manager = OrderStateManager(state=OrderState.WORKING)
    calc = OfiCalculator(initial_state=bbo)
    engine = SmartChasingEngine(cfg, "BUY", 100.0, 1.0, manager, calc)
    return engine, calc


def test_adapter_runs_strategy():
    engine, calc = create_engine()
    executor = DummyExecutor()
    adapter = HftBacktestAdapter(engine, calc, executor)

    feed = [
        {"price": 103.0, "qty": 10.0, "bid_price": 103.0, "bid_size": 10.0, "ask_price": 104.0, "ask_size": 10.0},
        {"price": 100.1, "qty": 10.0, "bid_price": 100.1, "bid_size": 10.0, "ask_price": 100.2, "ask_size": 10.0},
    ]

    adapter.run(feed)
    assert executor.submits  # expect at least one order submit from chasing
