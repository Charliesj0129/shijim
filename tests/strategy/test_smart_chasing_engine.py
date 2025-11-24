import pytest

from shijim.strategy.engine import (
    SmartChasingEngine,
    StrategyConfig,
    OrderStateManager,
    OrderState,
    OrderRequestAction,
)
from shijim.strategy.ofi import BboState, OfiCalculator


@pytest.fixture
def base_bbo() -> BboState:
    return BboState(100.0, 10.0, 101.0, 10.0)


def make_engine(base_bbo: BboState) -> SmartChasingEngine:
    calc = OfiCalculator(initial_state=base_bbo)
    manager = OrderStateManager(state=OrderState.WORKING)
    config = StrategyConfig(chase_threshold=2.0, max_chase_round=3)
    return SmartChasingEngine(
        config=config,
        side="BUY",
        order_price=100.0,
        order_qty=1.0,
        order_manager=manager,
        ofi_calculator=calc,
    )


def test_price_drift_triggers_chasing(base_bbo):
    engine = make_engine(base_bbo)
    result = engine.on_tick(BboState(103.0, 10.0, 101.0, 10.0), ofi_override=0.0)
    assert result and result[0].action == OrderRequestAction.CANCEL_REPLACE
    assert result[0].price == pytest.approx(103.0)
    assert engine.order_manager.state == OrderState.CHASING


def test_alpha_positive_chasing(base_bbo):
    engine = make_engine(base_bbo)
    result = engine.on_tick(BboState(102.0, 10.0, 101.0, 10.0), ofi_override=10.0)
    assert result and result[0].reason == "AlphaDriven"
    assert result[0].price == pytest.approx(102.0)


def test_negative_alpha_holds(base_bbo):
    engine = make_engine(base_bbo)
    result = engine.on_tick(BboState(102.5, 10.0, 101.0, 10.0), ofi_override=-20.0)
    assert result == []
    assert engine.order_manager.state == OrderState.WORKING
    assert "Hold: Negative Alpha Protection" in engine.logs


def test_state_guard_in_chasing(base_bbo):
    engine = make_engine(base_bbo)
    engine.order_manager.transition(OrderState.CHASING)
    assert engine.on_tick(BboState(105.0, 10.0, 101.0, 10.0), ofi_override=5.0) == []


def test_max_chase_round_triggers_cancel(base_bbo):
    engine = make_engine(base_bbo)
    engine.order_manager.chase_count = 3
    result = engine.on_tick(BboState(105.0, 10.0, 101.0, 10.0), ofi_override=0.0)
    assert result and result[0].action == OrderRequestAction.CANCEL
    assert result[0].price is None
    assert engine.order_manager.state == OrderState.IDLE
