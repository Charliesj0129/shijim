from unittest.mock import MagicMock

from shijim.execution.order_manager import NonBlockingOrderManager, OrderState
from shijim.strategy.engine import OrderRequest, OrderRequestAction


def test_send_order_non_blocking():
    api = MagicMock()
    resolver = MagicMock()
    manager = NonBlockingOrderManager(api, resolver)

    req = OrderRequest(
        action=OrderRequestAction.CANCEL_REPLACE,
        # Treated as place in this simple manager if not CANCEL
        price=100.0,
        quantity=1,
        reason="Test",
        symbol="TXF",
        side="BUY",
        internal_id="oid-1"
    )

    # Mock trade return
    trade = MagicMock()
    trade.order_id = "broker-1"
    api.place_order.return_value = trade

    manager.send_order(req)

    # Verify timeout=0
    api.place_order.assert_called_once()
    args, kwargs = api.place_order.call_args
    assert kwargs["timeout"] == 0

    # Verify state
    assert "oid-1" in manager.orders
    assert manager.orders["oid-1"].broker_id == "broker-1"
    assert manager.orders["oid-1"].status == "SUBMITTED"

def test_cancel_order_non_blocking():
    api = MagicMock()
    resolver = MagicMock()
    manager = NonBlockingOrderManager(api, resolver)

    # Setup existing order
    manager.orders["oid-1"] = OrderState(internal_id="oid-1", broker_id="broker-1")

    req = OrderRequest(
        action=OrderRequestAction.CANCEL,
        price=None, quantity=0, reason="Cancel",
        internal_id="oid-1"
    )

    manager.send_order(req)

    api.cancel_order.assert_called_once()
    args, kwargs = api.cancel_order.call_args
    assert args[1] == "broker-1"
    assert kwargs["timeout"] == 0

def test_update_from_callback():
    manager = NonBlockingOrderManager(MagicMock(), MagicMock())
    manager.orders["oid-1"] = OrderState(internal_id="oid-1", broker_id="broker-1")

    manager.update_from_callback("broker-1", "Filled", 1.0, 100.0)

    assert manager.orders["oid-1"].status == "Filled"
    assert manager.orders["oid-1"].filled_qty == 1.0
