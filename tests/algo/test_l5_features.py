import numpy as np

from shijim.algo.features.l5 import calc_book_slope, calc_depth_ratio, calc_micro_price


def test_depth_ratio():
    ratio = calc_depth_ratio([10, 20, 30, 40, 50], [5, 5, 5, 5, 5])
    assert ratio == 6.0


def test_micro_price():
    mp = calc_micro_price(100.0, 101.0, 10, 5)
    np.testing.assert_almost_equal(mp, 1510 / 15)


def test_book_slope_large():
    prices = [100, 101, 103, 107, 115]
    cum_sizes = np.cumsum([10, 20, 30, 40, 50])
    slope = calc_book_slope(prices, cum_sizes)
    assert slope > 4.0
