import numpy as np

from shijim.algo.microstructure import MlofiCalculator, OrderBookLevels


def test_deep_buy_pressure():
    calc = MlofiCalculator(depth=5)
    prev = OrderBookLevels(
        bid_prices=[100, 99.5, 99.0, 98.5, 98.0],
        bid_sizes=[10, 10, 10, 10, 10],
        ask_prices=[101, 101.5, 102.0, 102.5, 103.0],
        ask_sizes=[10, 10, 10, 10, 10],
    )
    calc.update(prev)

    current = OrderBookLevels(
        bid_prices=[100, 99.5, 99.0, 98.5, 98.0],
        bid_sizes=[10, 10, 100, 100, 100],
        ask_prices=[101, 101.5, 102.0, 102.5, 103.0],
        ask_sizes=[10, 10, 10, 10, 10],
    )
    result = calc.update(current)
    assert np.allclose(result.vector[:2], [0, 0])
    assert np.allclose(result.vector[2:], [90, 90, 90])
    assert result.pca_level > 200
