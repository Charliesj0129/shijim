from __future__ import annotations

import numpy as np
import pytest

shijim_indicators = pytest.importorskip("shijim_indicators")
RustOfiCalculator = shijim_indicators.RustOfiCalculator


def _vec(values: list[float]) -> np.ndarray:
    return np.asarray(values, dtype=np.float64)


def test_ofi_zero_copy():
    calc = RustOfiCalculator()

    assert (
        calc.update_from_levels(
            _vec([100.0]),
            _vec([10.0]),
            _vec([101.0]),
            _vec([10.0]),
        )
        is None
    )

    result = calc.update_from_levels(
        _vec([100.5]),
        _vec([5.0]),
        _vec([101.0]),
        _vec([10.0]),
    )
    assert result == pytest.approx(5.0)

    result = calc.update_from_levels(
        _vec([100.5]),
        _vec([5.0]),
        _vec([100.8]),
        _vec([20.0]),
    )
    assert result == pytest.approx(-20.0)


def test_ofi_missing_levels_returns_zero():
    calc = RustOfiCalculator()
    calc.update_from_levels(_vec([100.0]), _vec([5.0]), _vec([101.0]), _vec([5.0]))

    result = calc.update_from_levels(_vec([]), _vec([]), _vec([101.0]), _vec([5.0]))
    assert result == pytest.approx(0.0)


def test_ofi_length_mismatch():
    calc = RustOfiCalculator()
    calc.update_from_levels(_vec([100.0]), _vec([5.0]), _vec([101.0]), _vec([5.0]))
    with pytest.raises(ValueError):
        calc.update_from_levels(_vec([100.0, 99.0]), _vec([5.0]), _vec([101.0]), _vec([5.0]))
