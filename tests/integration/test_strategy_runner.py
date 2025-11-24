import pytest

from shijim.runner import StrategyRunner
from shijim.strategy.engine import (
    SmartChasingEngine,
    StrategyConfig,
    OrderStateManager,
    OrderState,
)
from shijim.strategy.ofi import BboState, OfiCalculator


class MockRingBuffer:
    def __init__(self, payload):
        self.payload = payload

    def latest_bytes(self):
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


class MockDecoder:
    def __init__(self, data: BboState):
        self.data = data

    def decode(self, payload: bytes):
        if payload == b"bad":
            from shijim.sbe.decoder import SBEDecodeError

            raise SBEDecodeError("bad")
        return self.data


class MockGateway:
    def __init__(self):
        self.orders = []

    def send(self, orders):
        self.orders.extend(orders)


def build_engine(price: float, ofi_state: BboState) -> SmartChasingEngine:
    config = StrategyConfig(chase_threshold=2.0, max_chase_round=3)
    calc = OfiCalculator(initial_state=ofi_state)
    manager = OrderStateManager(state=OrderState.WORKING)
    return SmartChasingEngine(
        config=config,
        side="BUY",
        order_price=price,
        order_qty=1.0,
        order_manager=manager,
        ofi_calculator=calc,
    )


def test_golden_path():
    prev = BboState(100.0, 10.0, 101.0, 10.0)
    engine = build_engine(100.0, prev)
    decoder = MockDecoder(BboState(105.0, 10.0, 101.0, 10.0))
    reader = MockRingBuffer(b"good")
    gateway = MockGateway()
    runner = StrategyRunner(reader, decoder, engine.ofi_calculator, engine, gateway)

    runner.tick()
    assert gateway.orders
    assert gateway.orders[0].price == pytest.approx(105.0)


def test_silence_path():
    prev = BboState(100.0, 10.0, 101.0, 10.0)
    engine = build_engine(100.0, prev)
    reader = MockRingBuffer(b"good")
    decoder = MockDecoder(BboState(100.1, 10.0, 101.0, 10.0))
    gateway = MockGateway()
    runner = StrategyRunner(reader, decoder, engine.ofi_calculator, engine, gateway)

    runner.tick()
    assert not gateway.orders


def test_corrupted_data_logs():
    prev = BboState(100.0, 10.0, 101.0, 10.0)
    engine = build_engine(100.0, prev)
    decoder = MockDecoder(BboState(0, 0, 0, 0))
    reader = MockRingBuffer(b"bad")
    gateway = MockGateway()
    logs = []

    def logger(msg):
        logs.append(msg)

    runner = StrategyRunner(reader, decoder, engine.ofi_calculator, engine, gateway, logger=logger)
    runner.tick()

    assert "Invalid Market Data" in logs
    assert not gateway.orders
    assert engine.order_manager.state == OrderState.WORKING


def test_latency_metrics_recorded():
    prev = BboState(100.0, 10.0, 101.0, 10.0)
    engine = build_engine(100.0, prev)
    decoder = MockDecoder(BboState(105.0, 10.0, 101.0, 10.0))
    reader = MockRingBuffer(b"good")
    gateway = MockGateway()
    runner = StrategyRunner(
        reader,
        decoder,
        engine.ofi_calculator,
        engine,
        gateway,
        metrics_enabled=True,
    )
    runner.tick()
    assert "TickProcessingTime" in runner.metrics
