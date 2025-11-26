"""Interfaces for managing Shioaji streaming subscriptions."""

from __future__ import annotations

import logging
import time
import threading
from concurrent.futures import ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from typing import Callable, Iterator, Sequence

from shijim.gateway.pool import ConnectionPool
from shijim.gateway.filter import ContractFilter

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

    pool: ConnectionPool
    plan: SubscriptionPlan = field(default_factory=SubscriptionPlan)
    max_subscriptions: int = 200  # Per-session cap
    batch_size: int = 50
    batch_sleep: float = 0.25
    sleep_func: Callable[[float], None] | None = None
    logger: logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))
    filter: ContractFilter = field(default_factory=ContractFilter)

    def __post_init__(self) -> None:
        self._sleep = self.sleep_func or time.sleep
        # Map key -> (contract, session_index)
        self._subscribed: dict[str, tuple[object, int]] = {}
        self._lock = threading.Lock()

    def subscribe_universe(self) -> None:
        """Subscribe Tick and BidAsk streams for every configured contract."""
        if not self.pool.sessions:
            self.logger.warning("No sessions in pool; cannot subscribe.")
            return

        # 1. Filter and Resolve Targets
        # Use the first session to resolve contracts/filter
        primary_session = self.pool.sessions[0]
        try:
            api = primary_session.get_api()
        except RuntimeError:
            self.logger.error("Primary session not logged in; cannot resolve contracts.")
            return

        valid_futures = self.filter.filter_codes(self.plan.futures, api, "futures")
        valid_stocks = self.filter.filter_codes(self.plan.stocks, api, "stock")

        targets = []
        for code in valid_futures:
            targets.append(("futures", code))
        for code in valid_stocks:
            targets.append(("stock", code))

        total = len(targets)
        if total == 0:
            self.logger.info("SubscriptionManager: no symbols provided (or all filtered).")
            return

        # 2. Distribute across sessions
        buckets: list[list[tuple[str, str]]] = [[] for _ in range(self.pool.size)]
        for i, target in enumerate(targets):
            buckets[i % self.pool.size].append(target)

        # 3. Fan-out subscriptions
        with ThreadPoolExecutor(max_workers=self.pool.size, thread_name_prefix="SubWorker") as executor:
            futures = []
            for i, bucket in enumerate(buckets):
                if not bucket:
                    continue
                session = self.pool.get_session(i)
                futures.append(executor.submit(self._subscribe_batch, session, i, bucket))
            wait(futures)

    def _subscribe_batch(self, session: ShioajiSession, session_idx: int, targets: list[tuple[str, str]]) -> None:
        """Subscribe a batch of targets to a specific session."""
        total = len(targets)
        if total > self.max_subscriptions:
            self.logger.warning(
                "Session %s target count %s exceeds limit %s; capping subscriptions.",
                session_idx,
                total,
                self.max_subscriptions,
            )
            targets = targets[: self.max_subscriptions]
            total = len(targets)

        quote = session.get_api().quote
        subscribed_count = 0
        
        for batch in self._batched(targets, self.batch_size):
            for asset_type, code in batch:
                key = self._key(asset_type, code)
                with self._lock:
                    if key in self._subscribed:
                        continue
                
                try:
                    contract = session.get_contract(code, asset_type)
                    self._subscribe_contract(quote, contract)
                    with self._lock:
                        self._subscribed[key] = (contract, session_idx)
                except Exception as exc:
                    self.logger.error("Failed to subscribe %s on session %s: %s", key, session_idx, exc)
                    continue
                
                subscribed_count += 1
            
            self.logger.info("Session %s subscribed %s / %s contracts", session_idx, subscribed_count, total)
            if subscribed_count < total and self.batch_sleep > 0:
                self._sleep(self.batch_sleep)

    def unsubscribe_all(self) -> None:
        """Reverse any previously issued subscriptions (best-effort)."""
        with self._lock:
            items = list(self._subscribed.items())
            self._subscribed.clear()

        for key, (contract, session_idx) in items:
            try:
                session = self.pool.get_session(session_idx)
                quote = session.get_api().quote
                self._unsubscribe_contract(quote, contract)
            except Exception:
                # Session might be logged out or invalid
                pass

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _key(self, asset_type: str, code: str) -> str:
        return f"{asset_type}:{code}"

    def _batched(self, items: Sequence[tuple[str, str]], size: int) -> Iterator[list[tuple[str, str]]]:
        if size <= 0:
            raise ValueError("batch_size must be positive")
        for start in range(0, len(items), size):
            yield list(items[start : start + size])

    def _subscribe_contract(self, quote: Any, contract: object) -> None:
        quote.subscribe(contract, quote_type=QuoteType.Tick, version=QuoteVersion.v1)
        quote.subscribe(contract, quote_type=QuoteType.BidAsk, version=QuoteVersion.v1)

    def _unsubscribe_contract(self, quote: Any, contract: object) -> None:
        unsubscribe = getattr(quote, "unsubscribe", None)
        if callable(unsubscribe):
            unsubscribe(contract, quote_type=QuoteType.Tick, version=QuoteVersion.v1)
            unsubscribe(contract, quote_type=QuoteType.BidAsk, version=QuoteVersion.v1)
