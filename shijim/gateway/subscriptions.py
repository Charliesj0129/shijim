"""Subscription manager wrapping Shioaji quote.subscribe calls.

References:
    * ``sinotrade_tutor_md/market_data/streaming/stocks.md``
    * ``sinotrade_tutor_md/market_data/streaming/futures.md``
    * ``sinotrade_tutor_md/advanced/quote_manager_basic.md``

Each contract is subscribed twice—Tick + BidAsk—using the documented call
patterns::

    api.quote.subscribe(contract, quote_type=QuoteType.Tick, version=QuoteVersion.v1)
    api.quote.subscribe(contract, quote_type=QuoteType.BidAsk, version=QuoteVersion.v1)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Iterable, List, Sequence

logger = logging.getLogger(__name__)

try:  # pragma: no cover - exercised indirectly in real envs
    from shioaji.constant import QuoteType, QuoteVersion  # type: ignore
except Exception:  # pragma: no cover - fallback for test env w/o Shioaji

    class QuoteType:  # type: ignore[override]
        Tick = "tick"
        BidAsk = "bidask"

    class QuoteVersion:  # type: ignore[override]
        v1 = "v1"


def _contract_key(contract: Any) -> str:
    """Return a stable identifier for a contract object."""
    for attr in ("code", "symbol", "name"):
        value = getattr(contract, attr, None)
        if isinstance(value, str) and value:
            return value
    return str(id(contract))


def _batched(items: Sequence[Any], batch_size: int) -> Iterable[Sequence[Any]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    length = len(items)
    for start in range(0, length, batch_size):
        yield items[start : start + batch_size]


SleepFunc = Callable[[float], None]


@dataclass
class SubscriptionManager:
    """Manage quote subscriptions across large futures/stock universes."""

    api: Any
    futures_contracts: Sequence[Any] = field(default_factory=list)
    stock_contracts: Sequence[Any] = field(default_factory=list)
    batch_size: int = 50
    batch_sleep: float = 0.25
    sleep_func: SleepFunc | None = None

    def __post_init__(self) -> None:
        self._quote = getattr(self.api, "quote", None)
        if self._quote is None:
            raise ValueError("API instance missing 'quote' attribute.")
        self._sleep = self.sleep_func or time.sleep
        self._subscribed: dict[str, Any] = {}

    def subscribe_universe(self) -> None:
        """Subscribe all configured contracts to Tick + BidAsk streams."""
        contracts: List[Any] = list(self.futures_contracts) + list(self.stock_contracts)
        total = len(contracts)
        if total == 0:
            logger.info("SubscriptionManager: no contracts supplied.")
            return

        processed = 0
        for batch_index, batch in enumerate(_batched(contracts, self.batch_size)):
            for contract in batch:
                processed += 1
                key = _contract_key(contract)
                if key in self._subscribed:
                    continue
                self._subscribe_contract(contract)
                self._subscribed[key] = contract
            logger.info("Subscribed %s / %s contracts", processed, total)
            is_last_batch = (batch_index + 1) * self.batch_size >= total
            if not is_last_batch and self.batch_sleep > 0:
                self._sleep(self.batch_sleep)

    def unsubscribe_all(self) -> None:
        """Unsubscribe every tracked contract safely."""
        for key, contract in list(self._subscribed.items()):
            self._unsubscribe_contract(contract)
            self._subscribed.pop(key, None)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _subscribe_contract(self, contract: Any) -> None:
        self._quote.subscribe(
            contract,
            quote_type=QuoteType.Tick,
            version=QuoteVersion.v1,
        )
        self._quote.subscribe(
            contract,
            quote_type=QuoteType.BidAsk,
            version=QuoteVersion.v1,
        )

    def _unsubscribe_contract(self, contract: Any) -> None:
        unsubscribe = getattr(self._quote, "unsubscribe", None)
        if not callable(unsubscribe):
            logger.warning("quote.unsubscribe missing; skipping.")
            return
        unsubscribe(contract, quote_type=QuoteType.Tick, version=QuoteVersion.v1)
        unsubscribe(contract, quote_type=QuoteType.BidAsk, version=QuoteVersion.v1)
