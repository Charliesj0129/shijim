"""Interfaces for managing Shioaji streaming subscriptions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Sequence


@dataclass(slots=True)
class SubscriptionPlan:
    """Defines which contracts (stocks/futures) should be subscribed."""

    futures: Sequence[Any] = field(default_factory=list)
    stocks: Sequence[Any] = field(default_factory=list)


@dataclass
class SubscriptionManager:
    """Coordinates batch subscription requests against the Shioaji quote API."""

    api: Any
    plan: SubscriptionPlan = field(default_factory=SubscriptionPlan)
    batch_size: int = 50

    def subscribe_universe(self) -> None:
        """Subscribe Tick and BidAsk streams for every configured contract."""
        raise NotImplementedError("Streaming subscription orchestration lives here.")

    def unsubscribe_all(self) -> None:
        """Reverse any previously issued subscriptions (best-effort)."""
        raise NotImplementedError("Cleanup logic will be implemented with real Shioaji bindings.")

    def iter_contracts(self) -> Iterable[Any]:
        """Yield futures first, then stocks, matching expected throttling strategies."""
        raise NotImplementedError("Determines iteration order + dedupe rules.")
