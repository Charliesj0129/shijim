from __future__ import annotations

import numpy as np
import pytest

shijim_indicators = pytest.importorskip("shijim_indicators")
RustHawkesIntensity = shijim_indicators.RustHawkesIntensity


def test_hawkes_exponential_kernel_updates():
    calc = RustHawkesIntensity(baseline=0.2, alpha=0.8, beta=1.5)

    first = calc.update(0.0)
    assert first == pytest.approx(1.0)

    second = calc.update(1.0)
    expected_second = 0.2 + (1.0 - 0.2) * np.exp(-1.5) + 0.8
    assert second == pytest.approx(expected_second)

    # Intensity decays between events without mutating state.
    assert calc.intensity_at(2.0) == pytest.approx(
        0.2 + (second - 0.2) * np.exp(-1.5)
    )

    batched = calc.update_many(np.asarray([3.0, 3.5], dtype=np.float64))
    # Should produce strictly positive intensities and match sequential calls.
    calc.reset()
    calc.update(0.0)
    calc.update(1.0)
    sequential = [calc.update(3.0), calc.update(3.5)]
    for lhs, rhs in zip(batched, sequential):
        assert lhs == pytest.approx(rhs)


def test_hawkes_monotonic_time_validation():
    calc = RustHawkesIntensity(baseline=0.1, alpha=0.3, beta=2.0)
    calc.update(0.5)
    with pytest.raises(ValueError):
        calc.update(0.4)

    with pytest.raises(ValueError):
        calc.intensity_at(-np.inf)
