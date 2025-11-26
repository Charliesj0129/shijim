from queue import SimpleQueue

import pytest

from shijim.dashboard.app import DashboardApp, SystemSnapshot
from shijim.runner import StrategyRunner
from shijim.strategy.engine import OrderState, OrderStateManager, SmartChasingEngine, StrategyConfig
from shijim.strategy.ofi import BboState, OfiCalculator


class MockReader:
    def __init__(self, payload: bytes):
        self.payload = payload

    def latest_bytes(self) -> bytes:
        return self.payload


class MockDecoder:
    def __init__(self, bbo: BboState):
        self.bbo = bbo

    def decode(self, payload: bytes) -> BboState:
        return self.bbo


class MockGateway:
    def __init__(self):
        self.orders = []
        self.reject_count = 0
        self.kill_switch_active = False

    def send(self, orders):
        self.orders.extend(orders)


def make_engine(price: float, bbo: BboState) -> SmartChasingEngine:
    config = StrategyConfig(chase_threshold=2.0, max_chase_round=3)
    manager = OrderStateManager(state=OrderState.WORKING)
    calc = OfiCalculator(initial_state=bbo)
    return SmartChasingEngine(config, "BUY", price, 1.0, manager, calc)


def test_runner_emits_snapshot():
    bbo_prev = BboState(100.0, 10.0, 101.0, 10.0)
    decoder = MockDecoder(BboState(105.0, 10.0, 106.0, 10.0))
    reader = MockReader(b"bytes")
    gateway = MockGateway()
    engine = make_engine(100.0, bbo_prev)
    snapshots = []

    runner = StrategyRunner(
        reader,
        decoder,
        engine.ofi_calculator,
        engine,
        gateway,
        metrics_callback=snapshots.append,
    )
    runner.tick()

    assert len(snapshots) == 1
    snap = snapshots[0]
    assert isinstance(snap, SystemSnapshot)
    assert snap.last_price == pytest.approx(105.0)
    assert snap.strategy_state in {"WORKING", "CHASING"}


def test_dashboard_app_headless_render_and_kill_switch():
    queue = SimpleQueue()
    snapshot = SystemSnapshot(
        timestamp=0.0,
        ingestion_lag=0.2,
        bid=100.0,
        ask=101.0,
        last_price=100.5,
        ofi=5.0,
        strategy_state="CHASING",
        active_orders=["Buy 1 @ 101"],
        position=2,
        kill_switch=True,
        reject_count=1,
        logs=["RISK REJECT"],
    )
    queue.put(snapshot)
    triggered = {"kill": False}

    def kill_callback():
        triggered["kill"] = True

    app = DashboardApp(queue, kill_switch_callback=kill_callback)
    rendered = app.run_headless_once()
    assert "Price=100.50" in rendered
    app.action_kill()
    assert triggered["kill"] is True
