import time
from queue import SimpleQueue
from unittest.mock import MagicMock, patch

from shijim.risk.guards import RateLimiter
from shijim.risk.manager import RiskAwareGateway, RiskManagerConfig
from shijim.strategy.engine import OrderRequest, OrderRequestAction


def test_token_bucket_rate_limiter():
    # Rate 10/s, Burst 10
    limiter = RateLimiter(rate=10.0, burst=10)

    # Consume all tokens
    for _ in range(10):
        assert limiter.check().passed

    # Next one should fail
    assert not limiter.check().passed

    # Wait 0.1s -> should refill 1 token
    time.sleep(0.11)
    assert limiter.check().passed
    assert not limiter.check().passed

def test_risk_gateway_telemetry():
    inner = MagicMock()
    config = RiskManagerConfig(
        max_order_qty=100,
        max_position=100,
        price_deviation=0.1,
        max_orders_per_sec=10
    )
    queue = SimpleQueue()
    gateway = RiskAwareGateway(inner, config, queue, market_price=100.0)

    # Create invalid order (qty > max)
    order = OrderRequest(
        action=OrderRequestAction.CANCEL_REPLACE,
        price=100.0,
        quantity=101,
        reason="Test",
        internal_id="oid-1",
        symbol="TXF",
        side="BUY"
    )

    with patch("shijim.risk.manager.logging.getLogger") as mock_get_logger:
        mock_logger = MagicMock()
        mock_get_logger.return_value = mock_logger

        gateway.send([order])

        # Check queue
        event = queue.get()
        assert event["type"] == "RiskReject"
        assert event["reason"] == "MaxOrderQty"

        # Check logging
        mock_logger.warning.assert_called_once()
        args, _ = mock_logger.warning.call_args
        assert "RiskReject" in args[0]
        assert "oid-1" in args[1] or "oid-1" in args[0] # Depending on formatting

def test_risk_gateway_rate_limit_integration():
    inner = MagicMock()
    config = RiskManagerConfig(
        max_order_qty=100,
        max_position=100,
        price_deviation=0.1,
        max_orders_per_sec=1 # 1 per sec
    )
    queue = SimpleQueue()
    gateway = RiskAwareGateway(inner, config, queue, market_price=100.0)

    order = OrderRequest(
        action=OrderRequestAction.CANCEL_REPLACE,
        price=100.0,
        quantity=1,
        reason="Test",
        internal_id="oid-1",
        symbol="TXF",
        side="BUY"
    )

    # First pass
    gateway.send([order])
    assert inner.send.called

    # Second fail
    inner.reset_mock()
    gateway.send([order])
    assert not inner.send.called
    event = queue.get()
    assert event["reason"] == "RateLimit"
