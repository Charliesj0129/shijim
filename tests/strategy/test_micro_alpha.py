from unittest.mock import MagicMock

from shijim.events.schema import MDBookEvent
from shijim.strategy.micro_alpha import MicroAlphaConfig, MicroAlphaStrategy


def test_micro_alpha_strategy_initialization():
    bus = MagicMock()
    config = MicroAlphaConfig(symbol="TEST")
    strategy = MicroAlphaStrategy(bus, config)

    assert strategy.position == 0
    assert strategy.active is False

    strategy.start()
    assert strategy.active is True

    strategy.stop()
    assert strategy.active is False

def test_micro_alpha_signal_generation():
    bus = MagicMock()
    config = MicroAlphaConfig(symbol="TEST", ofi_threshold=5.0, accumulator_interval=1.0)
    strategy = MicroAlphaStrategy(bus, config)
    strategy.start()

    # T0: Initial book
    event0 = MDBookEvent(
        ts_ns=1_000_000_000, symbol="TEST", asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[10],
        ask_prices=[101.0], ask_volumes=[10]
    )
    strategy.on_event(event0)
    assert len(strategy.signals) == 0
    assert strategy.position == 0

    # T1: +1.0s, OFI=+10 (Bid vol increases by 10)
    event1 = MDBookEvent(
        ts_ns=2_000_000_000, symbol="TEST", asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[20],
        ask_prices=[101.0], ask_volumes=[10]
    )
    strategy.on_event(event1)

    # Should trigger signal and buy
    assert len(strategy.signals) == 1
    assert strategy.signals[0].ofi_value == 10.0
    assert strategy.position == 1 # Bought 1

    # T2: +1.0s, OFI=-10 (Bid vol decreases by 10)
    event2 = MDBookEvent(
        ts_ns=3_000_000_000, symbol="TEST", asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[10],
        ask_prices=[101.0], ask_volumes=[10]
    )
    strategy.on_event(event2)

    # Should trigger signal and sell
    assert len(strategy.signals) == 2
    assert strategy.signals[1].ofi_value == -10.0
    assert strategy.position == 0 # Sold 1, back to 0

def test_micro_alpha_position_limits():
    bus = MagicMock()
    config = MicroAlphaConfig(
        symbol="TEST", ofi_threshold=5.0, max_position=2, accumulator_interval=0.1
    )
    strategy = MicroAlphaStrategy(bus, config)
    strategy.start()

    # T0: Initial
    strategy.on_event(MDBookEvent(
        ts_ns=1_000_000_000, symbol="TEST", asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[10], ask_prices=[101.0], ask_volumes=[10]
    ))

    # T1: Buy 1 (OFI +10)
    strategy.on_event(MDBookEvent(
        ts_ns=1_100_000_000, symbol="TEST", asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[20], ask_prices=[101.0], ask_volumes=[10]
    ))
    assert strategy.position == 1

    # T2: Buy 2 (OFI +10)
    strategy.on_event(MDBookEvent(
        ts_ns=1_200_000_000, symbol="TEST", asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[30], ask_prices=[101.0], ask_volumes=[10]
    ))
    assert strategy.position == 2

    # T3: Buy 3 (OFI +10) -> Should be ignored
    strategy.on_event(MDBookEvent(
        ts_ns=1_300_000_000, symbol="TEST", asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[40], ask_prices=[101.0], ask_volumes=[10]
    ))
    assert strategy.position == 2

def test_micro_alpha_symbol_filtering():
    bus = MagicMock()
    config = MicroAlphaConfig(symbol="TEST")
    strategy = MicroAlphaStrategy(bus, config)
    strategy.start()

    # Wrong symbol
    event = MDBookEvent(
        ts_ns=1_000_000_000, symbol="OTHER", asset_type="stock", exchange="TSE",
        bid_prices=[100.0], bid_volumes=[10], ask_prices=[101.0], ask_volumes=[10]
    )
    strategy.on_event(event)

    # Should not process (accumulator state empty)
    assert len(strategy.signals) == 0
    assert "OTHER" not in strategy.ofi_accumulator._accumulators
