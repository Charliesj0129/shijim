import pytest

from shijim.strategy.ofi import BboState, OfiCalculator


@pytest.fixture
def base_state() -> BboState:
    return BboState(bid_price=100.0, bid_size=10, ask_price=101.0, ask_size=10)


@pytest.fixture
def calculator(base_state: BboState) -> OfiCalculator:
    return OfiCalculator(initial_state=base_state)


def test_static_bid_addition(calculator: OfiCalculator):
    result = calculator.process_tick(100.0, 15.0, 101.0, 10.0)
    assert result.bid_imbalance == pytest.approx(5.0)
    assert result.ask_imbalance == pytest.approx(0.0)
    assert result.net_ofi == pytest.approx(5.0)


def test_bid_price_improvement_updates_state(calculator: OfiCalculator):
    result = calculator.process_tick(100.5, 5.0, 101.0, 10.0)
    assert result.bid_imbalance == pytest.approx(5.0)
    assert result.net_ofi == pytest.approx(5.0)
    assert calculator.prev_state == BboState(100.5, 5.0, 101.0, 10.0)


def test_ask_size_reduction_positive_ofi(calculator: OfiCalculator):
    result = calculator.process_tick(100.0, 10.0, 101.0, 2.0)
    assert result.ask_imbalance == pytest.approx(-8.0)
    assert result.net_ofi == pytest.approx(8.0)


def test_bid_price_decline_negative_ofi(calculator: OfiCalculator):
    result = calculator.process_tick(99.5, 20.0, 101.0, 10.0)
    assert result.bid_imbalance == pytest.approx(-10.0)
    assert result.net_ofi == pytest.approx(-10.0)


def test_process_next_tick_integration(base_state: BboState):
    snapshots = [
        {"bid_price": 100.0, "bid_size": 12.0, "ask_price": 101.0, "ask_size": 9.0}
    ]

    class FakeReader:
        def __init__(self, records):
            self.records = records
            self.idx = 0

        def next_bbo(self):
            data = self.records[self.idx]
            self.idx += 1
            return data

    class StrategyEngine:
        def __init__(self, reader, calculator):
            self.reader = reader
            self.calculator = calculator
            self.signals = []

        def process_next_tick(self):
            bbo = self.reader.next_bbo()
            result = self.calculator.process_tick(
                bbo["bid_price"],
                bbo["bid_size"],
                bbo["ask_price"],
                bbo["ask_size"],
            )
            self.signals.append(result)
            return result

    reader = FakeReader(snapshots)
    calc = OfiCalculator(initial_state=base_state)
    engine = StrategyEngine(reader, calc)

    result = engine.process_next_tick()
    assert result.net_ofi == pytest.approx(3.0)
    assert calc.prev_state == BboState(100.0, 12.0, 101.0, 9.0)
