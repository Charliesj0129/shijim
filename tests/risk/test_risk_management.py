from queue import SimpleQueue

from shijim.risk.guards import RiskManagerConfig
from shijim.risk.manager import RiskAwareGateway
from shijim.strategy.engine import OrderRequest, OrderRequestAction


class MockGateway:
    def __init__(self):
        self.orders = []

    def send(self, orders):
        self.orders.extend(orders)
        return orders


def base_config():
    return RiskManagerConfig(
        max_order_qty=5,
        max_position=10,
        price_deviation=0.05,
        max_orders_per_sec=5,
    )


def make_order(price=100.0, qty=1.0, action=OrderRequestAction.CANCEL_REPLACE, reason="test"):
    return OrderRequest(
        action=action,
        price=price,
        quantity=qty,
        reason=reason,
        symbol="TEST",
        side="BUY",
    )


def test_price_deviation_rejected():
    queue = SimpleQueue()
    gateway = RiskAwareGateway(MockGateway(), base_config(), queue, market_price=100.0, position=2)
    gateway.send([make_order(price=110.0)])
    event = queue.get_nowait()
    assert event["reason"] == "PriceDeviation"


def test_max_order_qty_rejected():
    queue = SimpleQueue()
    gateway = RiskAwareGateway(MockGateway(), base_config(), queue, market_price=100.0, position=2)
    gateway.send([make_order(price=100.0, qty=10)])
    event = queue.get_nowait()
    assert event["reason"] == "MaxOrderQty"


def test_position_limit_rejected():
    queue = SimpleQueue()
    gateway = RiskAwareGateway(MockGateway(), base_config(), queue, market_price=100.0, position=8)
    gateway.send([make_order(price=100.0, qty=3)])
    event = queue.get_nowait()
    assert event["reason"] == "PositionLimit"


def test_rate_limit_rejected():
    queue = SimpleQueue()
    gw = RiskAwareGateway(MockGateway(), base_config(), queue, market_price=100.0, position=2)
    for _ in range(5):
        gw.rate_limiter.tokens = 0
    gw.rate_limiter.tokens = 0
    gw.finger_guard.set_reference_price(100.0)
    gw.send([make_order(price=100.0)])
    event = queue.get_nowait()
    assert event["reason"] == "RateLimit"


def test_kill_switch_block_new_allow_cancel():
    queue = SimpleQueue()
    gw = RiskAwareGateway(MockGateway(), base_config(), queue, market_price=100.0, position=2)
    gw.activate_kill()
    gw.send([make_order(price=100.0)])
    event = queue.get_nowait()
    assert event["reason"] == "KillSwitch"
    cancel_order = make_order(action=OrderRequestAction.CANCEL)
    gw.send([cancel_order])
    assert gw.inner_gateway.orders == [cancel_order]
