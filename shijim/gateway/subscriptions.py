"""Interfaces for managing Shioaji streaming subscriptions."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Callable, Iterator, Sequence

from shijim.gateway.session import ShioajiSession

try:  # pragma: no cover - depends on Shioaji
    from shioaji.constant import QuoteType, QuoteVersion  # type: ignore
except Exception:  # pragma: no cover - fallback for test env

    class QuoteType:  # type: ignore
        Tick = "tick"
        BidAsk = "bidask"

    class QuoteVersion:  # type: ignore
        v1 = "v1"


@dataclass(slots=True)
class SubscriptionPlan:
    """Defines which contracts (stocks/futures) should be subscribed."""

    futures: Sequence[str] = field(default_factory=list)
    stocks: Sequence[str] = field(default_factory=list)


@dataclass
class SubscriptionManager:
    """Coordinates batch subscription requests against the Shioaji quote API."""

    session: ShioajiSession
    plan: SubscriptionPlan = field(default_factory=SubscriptionPlan)
    batch_size: int = 50
    batch_sleep: float = 0.25
    sleep_func: Callable[[float], None] | None = None
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))

    def __post_init__(self) -> None:
        self._quote = self.session.get_api().quote
        self._sleep = self.sleep_func or time.sleep
        self._subscribed: dict[str, object] = {}
        self._resolved_contracts: dict[str, object] = {}

    def subscribe_universe(self) -> None:
        """Subscribe Tick and BidAsk streams for every configured contract."""
        targets = self._unique_targets()
        total = len(targets)
        if total == 0:
            self.logger.info("SubscriptionManager: no symbols provided.")
            return

        subscribed = 0
        for batch in self._batched(targets, self.batch_size):
            for asset_type, code in batch:
                key = self._key(asset_type, code)
                if key in self._subscribed:
                    continue
                contract = self._contract_for(asset_type, code)
                self._subscribe_contract(contract)
                self._subscribed[key] = contract
                subscribed += 1
                self.logger.info("Subscribed %s / %s contracts", subscribed, total)
            if subscribed < total and self.batch_sleep > 0:
                self._sleep(self.batch_sleep)

    def unsubscribe_all(self) -> None:
        """Reverse any previously issued subscriptions (best-effort)."""
        for key, contract in list(self._subscribed.items()):
            self._unsubscribe_contract(contract)
            self._subscribed.pop(key, None)

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _unique_targets(self) -> list[tuple[str, str]]:
        ordered = [("futures", code) for code in self.plan.futures] + [
            ("stock", code) for code in self.plan.stocks
        ]
        seen: set[str] = set()
        result: list[tuple[str, str]] = []
        for asset_type, code in ordered:
            key = self._key(asset_type, code)
            if key in seen:
                continue
            seen.add(key)
            result.append((asset_type, code))
        return result

    def _contract_for(self, asset_type: str, code: str):
        key = self._key(asset_type, code)
        if key not in self._resolved_contracts:
            contract = self.session.get_contract(code, asset_type)
            self._resolved_contracts[key] = contract
        return self._resolved_contracts[key]

    def _subscribe_contract(self, contract: object) -> None:
        self._quote.subscribe(contract, quote_type=QuoteType.Tick, version=QuoteVersion.v1)
        self._quote.subscribe(contract, quote_type=QuoteType.BidAsk, version=QuoteVersion.v1)

    def _unsubscribe_contract(self, contract: object) -> None:
        unsubscribe = getattr(self._quote, "unsubscribe", None)
        if callable(unsubscribe):
            unsubscribe(contract, quote_type=QuoteType.Tick, version=QuoteVersion.v1)
            unsubscribe(contract, quote_type=QuoteType.BidAsk, version=QuoteVersion.v1)

    def _key(self, asset_type: str, code: str) -> str:
        return f"{asset_type}:{code}"

    def _batched(self, items: Sequence[tuple[str, str]], size: int) -> Iterator[list[tuple[str, str]]]:
        if size <= 0:
            raise ValueError("batch_size must be positive")
        for start in range(0, len(items), size):
            yield list(items[start : start + size])
