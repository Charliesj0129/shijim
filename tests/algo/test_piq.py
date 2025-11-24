from shijim.algo.execution.piq import PiqEstimator


def test_trade_event_consumes_queue():
    piq = PiqEstimator.initialize(price=100.0, my_qty=1.0, total_exchange_qty=101.0)
    piq.on_trade(price=100.0, volume=20.0)
    assert piq.qty_ahead == 80.0
    assert piq.estimated_fill_probability() > 0.0


def test_cancel_event_probabilistic():
    piq = PiqEstimator(price=100.0, my_qty=1.0, total_qty=1000.0, qty_ahead=100.0)
    piq.on_quote(total_qty_at_price=950.0)
    assert piq.qty_ahead == 95.0


def test_queue_consumed_to_fill():
    piq = PiqEstimator(price=100.0, my_qty=1.0, total_qty=6.0, qty_ahead=5.0)
    piq.on_trade(price=100.0, volume=10.0)
    assert piq.qty_ahead == 0.0
