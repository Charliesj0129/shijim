"""Simple token-bucket rate limiter for API throttling."""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable


@dataclass
class TokenBucketRateLimiter:
    """Token-bucket limiter that can block until tokens are available."""

    rate_per_sec: float
    capacity: int
    clock: Callable[[], float] = time.monotonic
    _tokens: float = field(init=False)
    _last_refill: float = field(init=False)

    def __post_init__(self) -> None:
        if self.rate_per_sec <= 0:
            raise ValueError("rate_per_sec must be positive")
        if self.capacity <= 0:
            raise ValueError("capacity must be positive")
        self._tokens = float(self.capacity)
        self._last_refill = self.clock()

    def consume(
        self,
        cost: float = 1.0,
        *,
        block: bool = True,
        sleep: Callable[[float], None] = time.sleep,
    ) -> bool:
        """Consume tokens; optionally block until enough tokens are available."""
        if cost <= 0:
            return True
        while True:
            self._refill()
            if self._tokens >= cost:
                self._tokens -= cost
                return True
            if not block:
                return False
            deficit = cost - self._tokens
            sleep(max(deficit / self.rate_per_sec, 0))

    def _refill(self) -> None:
        now = self.clock()
        elapsed = now - self._last_refill
        if elapsed <= 0:
            return
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate_per_sec)
        self._last_refill = now
