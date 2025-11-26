"""Contract filtering logic to enforce universe constraints."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Sequence

logger = logging.getLogger(__name__)

try:
    from prometheus_client import Counter
    FILTER_BLOCK_TOTAL = Counter(
        "shijim_gateway_contract_filter_block_total",
        "Total contracts blocked by filter",
        ["reason", "asset_type"]
    )
except ImportError:
    class MockCounter:
        def labels(self, **kwargs): return self
        def inc(self, amount=1): pass
    FILTER_BLOCK_TOTAL = MockCounter()


@dataclass
class ContractFilter:
    """Filters contracts to ensure only allowed instruments are subscribed."""

    allowed_exchanges: set[str] = field(default_factory=lambda: {"TSE", "OTC"})
    blocked_suffixes: set[str] = field(default_factory=lambda: {"L", "Q", "F", "R"})
    
    def is_allowed(self, code: str, contract: Any, asset_type: str) -> bool:
        """Check if a specific contract is allowed."""
        # 1. Suffix check (Fast fail)
        for suffix in self.blocked_suffixes:
            if code.endswith(suffix):
                FILTER_BLOCK_TOTAL.labels(reason="suffix", asset_type=asset_type).inc()
                return False

        # 2. Stock specific checks
        if asset_type == "stock":
            # Enforce digits only (User requirement)
            if not code.isdigit():
                FILTER_BLOCK_TOTAL.labels(reason="non_digit", asset_type=asset_type).inc()
                return False
            
            if not contract:
                # If we can't validate metadata, safe default is block? 
                # Or allow if it looks like a stock?
                # Let's log warning and block to be safe.
                logger.warning("Blocking %s: contract metadata missing", code)
                FILTER_BLOCK_TOTAL.labels(reason="missing_meta", asset_type=asset_type).inc()
                return False

            # Exchange check
            exchange = str(getattr(contract, "exchange", ""))
            if exchange not in self.allowed_exchanges:
                FILTER_BLOCK_TOTAL.labels(reason="exchange", asset_type=asset_type).inc()
                return False

            # Leveraged ETF check
            # Some contracts have 'category' or specific type fields
            # We check for "ETFLeveraged" as requested
            # Note: Shioaji contract attributes might vary. 
            # We check a few common places or the string representation.
            # But "type" isn't a standard attribute on all contracts.
            # We'll check if `category` or similar indicates leverage.
            # Actually, the user mentioned `type == "ETFLeveraged"`.
            # We will try to access it safely.
            ctype = getattr(contract, "type", None) # Some versions have this
            if ctype and str(ctype) == "ETFLeveraged":
                FILTER_BLOCK_TOTAL.labels(reason="leveraged_type", asset_type=asset_type).inc()
                return False
                
            # Also check name for "反" (Inverse) or "正2" (Leveraged 2x) if needed?
            # The suffix check (L/R) covers most, but some might not have it?
            # For now, rely on suffix + isdigit + type.

        return True

    def filter_codes(self, codes: Sequence[str], api: Any, asset_type: str) -> list[str]:
        """Filter a list of codes against the API contracts."""
        valid = []
        # Pre-fetch contracts map to avoid repeated lookups if possible
        # But api.Contracts.Stocks[code] is fast dict lookup.
        
        contracts_map = self._get_contracts_map(api, asset_type)
        
        for code in codes:
            contract = contracts_map.get(code)
            if self.is_allowed(code, contract, asset_type):
                valid.append(code)
            else:
                logger.debug("ContractFilter blocked %s %s", asset_type, code)
        
        return valid

    def _get_contracts_map(self, api: Any, asset_type: str) -> Any:
        # Return the dict-like object for lookups
        if asset_type == "stock":
            return getattr(api.Contracts, "Stocks", {})
        if asset_type == "futures":
            return getattr(api.Contracts, "Futures", {})
        return {}
