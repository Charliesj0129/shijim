from __future__ import annotations

import numpy as np
import pytest

shijim_indicators = pytest.importorskip("shijim_indicators")
RustVpinCalculator = shijim_indicators.RustVpinCalculator


def test_vpin_bucket_rollover_behavior():
    calc = RustVpinCalculator(bucket_volume=50.0, window_size=2)
    volumes = np.asarray([20.0, 30.0, -40.0, -20.0, 40.0, 10.0], dtype=np.float64)

    results = calc.update_signed_series(volumes)
    assert results[0] is None
    assert results[1] is None
    assert results[2] is None
    assert results[3] == pytest.approx(1.0)
    assert results[4] == pytest.approx(0.8)
    assert results[5] == pytest.approx(0.8)


def test_vpin_reset_and_invalid_inputs():
    calc = RustVpinCalculator(bucket_volume=100.0, window_size=3)

    # First two buckets completed → not enough history yet.
    assert calc.update_signed_volume(100.0) is None
    assert calc.update_signed_volume(-100.0) is None

    # Third bucket completes and yields the first VPIN observation.
    assert calc.update_signed_volume(100.0) == pytest.approx(1.0)

    calc.reset()
    assert calc.update_signed_volume(50.0) is None
    assert calc.update_signed_volume(-50.0) is None

    with pytest.raises(ValueError):
        calc.update_signed_volume(float("nan"))
