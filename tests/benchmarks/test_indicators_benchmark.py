import importlib.util

import pytest

from shijim.features.hawkes import HawkesConfig, HawkesEstimator
from shijim.features.vpin import VPINCalculator, VPINConfig

RUST_AVAILABLE = importlib.util.find_spec("shijim_indicators") is not None

@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extension not installed")
def test_benchmark_vpin_update(benchmark):
    config = VPINConfig(bucket_volume=1000.0, window_size=50)
    calc = VPINCalculator(config)

    def run_update():
        calc.update(10.0, 1000, "BTC")

    benchmark(run_update)

@pytest.mark.skipif(not RUST_AVAILABLE, reason="Rust extension not installed")
def test_benchmark_hawkes_update(benchmark):
    config = HawkesConfig(baseline=0.1, alpha=0.5, beta=1.0)
    est = HawkesEstimator(config)

    def run_update():
        est.update(1000, "BTC")

    benchmark(run_update)
