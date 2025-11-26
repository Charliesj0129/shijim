from queue import SimpleQueue
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from shijim.gateway.shioaji_adapter import (
    ExecutionReport,
    ShioajiAdapter,
)
from shijim.strategy.engine import OrderRequest, OrderRequestAction


@pytest.fixture
def mock_api():
    api = MagicMock()
    api.Order.side_effect = lambda **kwargs: kwargs
    return api


@pytest.fixture
def contract():
    return SimpleNamespace(symbol="TXFL5")


def build_adapter(mock_api, contract):
    queue = SimpleQueue()
    def resolver(symbol):
        return contract
    return ShioajiAdapter(mock_api, resolver, queue), queue


def test_new_order_placement(mock_api, contract):
    adapter, _ = build_adapter(mock_api, contract)
    mock_trade = SimpleNamespace(order_id="S1")
    mock_api.place_order.return_value = mock_trade
    request = OrderRequest(
        action=OrderRequestAction.CANCEL_REPLACE,
        price=23000.0,
        quantity=1,
        reason="New",
        symbol="TXFL5",
        side="BUY",
        internal_id="A1",
    )
    resp = adapter.send([request])
    mock_api.place_order.assert_called_once()
    assert resp[0].broker_order_id == "S1"


def test_cancel_replace(mock_api, contract):
    adapter, _ = build_adapter(mock_api, contract)
    adapter.order_mapping["A1"] = "S1"
    mock_api.place_order.return_value = SimpleNamespace(order_id="S2")
    req = OrderRequest(
        action=OrderRequestAction.CANCEL_REPLACE,
        price=23005.0,
        quantity=1,
        reason="Chase",
        symbol="TXFL5",
        side="BUY",
        internal_id="A1",
    )
    adapter.send([req])
    mock_api.cancel_order.assert_called_once_with(None, "S1")
    mock_api.place_order.assert_called_once()


def test_callback_to_queue(mock_api, contract):
    adapter, queue = build_adapter(mock_api, contract)
    event = SimpleNamespace(order_id="S1", status="Filled", deal_quantity=1, deal_price=23000)
    adapter.register_callback(event)
    report = queue.get_nowait()
    assert isinstance(report, ExecutionReport)
    assert report.status == "Filled"


def test_rejection_event(mock_api, contract):
    adapter, queue = build_adapter(mock_api, contract)
    mock_api.place_order.side_effect = RuntimeError("Margin")
    req = OrderRequest(
        action=OrderRequestAction.CANCEL_REPLACE,
        price=1.0,
        quantity=1,
        reason="test",
        symbol="TXFL5",
        side="BUY",
        internal_id="E1",
    )
    adapter.send([req])
    event = queue.get_nowait()
    assert event["type"] == "OrderRejected"
