import importlib.util

import numpy as np
import pytest

from shijim.events.schema import MDBookEvent
from shijim.features.hawkes import HawkesConfig, HawkesEstimator
from shijim.features.ofi import OFICalculator
from shijim.features.vpin import VPINCalculator, VPINConfig

RUST_AVAILABLE = importlib.util.find_spec("shijim_indicators") is not None

@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extension not installed")
def test_integrated_ofi_calculator():
    calc = OFICalculator()
    # Verify it's using Rust backend
    assert calc._rust_calc is not None

    # Create dummy events
    event1 = MDBookEvent(
        ts_ns=1000, symbol="BTC",
        asset_type="SPOT", exchange="BINANCE",
        bid_prices=[100.0], bid_volumes=[1.0],
        ask_prices=[101.0], ask_volumes=[1.0]
    )
    event2 = MDBookEvent(
        ts_ns=2000, symbol="BTC",
        asset_type="SPOT", exchange="BINANCE",
        bid_prices=[100.0], bid_volumes=[2.0], # Bid size increased -> +1 OFI
        ask_prices=[101.0], ask_volumes=[1.0]
    )

    # First event initializes state, returns None (or 0 if depth missing, but here depth is present)
    # Actually RustOfiCalculator returns None on first update to set prev state.
    res1 = calc.calculate(event1)
    assert res1 is None

    res2 = calc.calculate(event2)
    assert res2 is not None
    assert res2.ofi_value == 1.0
    assert res2.symbol == "BTC"
    assert res2.ts_ns == 2000

@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extension not installed")
def test_integrated_vpin_calculator():
    config = VPINConfig(bucket_volume=10.0, window_size=2)
    calc = VPINCalculator(config)

    # Fill first bucket (10 volume)
    # Buy 10
    res = calc.update(10.0, 1000, "BTC")
    assert res is None # Bucket full, but window not full (need 2 buckets)

    # Fill second bucket (10 volume)
    # Sell 10
    res = calc.update(-10.0, 2000, "BTC")
    assert res is not None
    # Imbalance 1: |10 - 0| = 10
    # Imbalance 2: |0 - 10| = 10
    # Total Imbalance = 20
    # Total Volume = 20
    # VPIN = 20 / 20 = 1.0
    assert res.vpin_value == pytest.approx(1.0)
    assert res.symbol == "BTC"

@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extension not installed")
def test_integrated_hawkes_estimator():
    config = HawkesConfig(baseline=0.1, alpha=0.5, beta=1.0)
    est = HawkesEstimator(config)

    # t=0
    res = est.update(0, "BTC")
    # Intensity after jump: baseline + alpha = 0.1 + 0.5 = 0.6
    assert res.intensity == pytest.approx(0.6)

    # t=1s (1_000_000_000 ns)
    # Decay for 1s: (0.6 - 0.1) * exp(-1.0 * 1.0) + 0.1
    # = 0.5 * 0.3678 + 0.1 = 0.1839 + 0.1 = 0.2839
    # Jump: + 0.5 -> 0.7839
    res = est.update(1_000_000_000, "BTC")
    expected = 0.1 + (0.6 - 0.1) * np.exp(-1.0) + 0.5
    assert res.intensity == pytest.approx(expected)
