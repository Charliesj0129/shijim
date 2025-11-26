import logging
import sys
from unittest.mock import MagicMock, patch

# Mock shioaji
sys.modules["shioaji"] = MagicMock()
sys.modules["shioaji.constant"] = MagicMock()

from shijim.gateway.pool import ConnectionPool
from shijim.gateway.subscriptions import SubscriptionManager, SubscriptionPlan
from shijim.gateway.filter import ContractFilter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("verify_pool")

def test_pool_distribution():
    logger.info("Testing Pool Distribution...")
    
    # Mock sessions
    pool = ConnectionPool(size=2)
    # Manually inject mocks
    pool.sessions = [MagicMock(), MagicMock()]
    
    # Mock API and Contracts
    for i, session in enumerate(pool.sessions):
        api = session.get_api.return_value
        api.quote = MagicMock()
        
        # Mock Contracts
        stock_contracts = {}
        futures_contracts = {}
        
        # Create dummy contracts
        # 2330: Valid
        # 2317: Valid
        # 0050: Valid (ETF but allowed if not leveraged)
        # 00753L: Invalid (Leveraged)
        for code in ["2330", "2317", "0050", "00753L"]:
            contract = MagicMock()
            contract.code = code
            contract.exchange = "TSE"
            contract.type = "Stock"
            if code == "00753L":
                contract.type = "ETFLeveraged" # Simulate leveraged
            stock_contracts[code] = contract
            
        api.Contracts.Stocks = stock_contracts
        api.Contracts.Futures = futures_contracts
        
        # Mock get_contract
        def get_contract(code, asset_type):
            if asset_type == "stock":
                return stock_contracts.get(code)
            return None
        session.get_contract.side_effect = get_contract

    # Plan with mixed symbols
    plan = SubscriptionPlan(stocks=["2330", "2317", "0050", "00753L"])
    
    manager = SubscriptionManager(pool=pool, plan=plan)
    manager.subscribe_universe()
    
    # Check subscribe calls
    s1_quote = pool.sessions[0].get_api().quote
    s2_quote = pool.sessions[1].get_api().quote
    
    s1_calls = s1_quote.subscribe.call_count
    s2_calls = s2_quote.subscribe.call_count
    
    logger.info("Session 1 calls: %s", s1_calls)
    logger.info("Session 2 calls: %s", s2_calls)
    
    # We expect 3 valid symbols (2330, 2317, 0050). 00753L blocked.
    # Each symbol gets 2 subscribe calls (Tick, BidAsk).
    # Total valid calls = 3 * 2 = 6.
    
    total_calls = s1_calls + s2_calls
    if total_calls != 6:
        logger.error("Expected 6 calls, got %s", total_calls)
        # Debug which ones were called
        logger.info("S1 calls: %s", s1_quote.subscribe.call_args_list)
        logger.info("S2 calls: %s", s2_quote.subscribe.call_args_list)
        raise AssertionError(f"Expected 6 calls, got {total_calls}")

    # Verify distribution (at least 1 symbol on each, so >= 2 calls on each)
    # 3 symbols distributed over 2 sessions: 2 on one, 1 on other.
    # So 4 calls on one, 2 on other.
    assert s1_calls >= 2 and s2_calls >= 2, "Distribution failed (one session got nothing)"
    
    logger.info("Pool Distribution Test Passed!")

if __name__ == "__main__":
    test_pool_distribution()
