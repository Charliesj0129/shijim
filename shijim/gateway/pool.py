"""Connection pool for managing multiple Shioaji sessions."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from typing import Iterator

from shijim.gateway.session import ShioajiSession

logger = logging.getLogger(__name__)

try:
    from prometheus_client import Gauge
    ACTIVE_CONNECTIONS = Gauge(
        "shijim_gateway_active_connections",
        "Number of active Shioaji sessions"
    )
except ImportError:
    class MockGauge:
        def set(self, value): pass
        def inc(self, amount=1): pass
        def dec(self, amount=1): pass
    ACTIVE_CONNECTIONS = MockGauge()


@dataclass
class ConnectionPool:
    """Manages a pool of Shioaji sessions to distribute load."""

    size: int = 5
    sessions: list[ShioajiSession] = field(default_factory=list)
    _logged_in_count: int = 0

    def __post_init__(self) -> None:
        if not self.sessions:
            for i in range(self.size):
                # We pass the logger to share configuration
                self.sessions.append(ShioajiSession(logger=logger))

    def login_all(self, jitter_min: float = 0.25, jitter_max: float = 0.5) -> None:
        """Login all sessions with jitter to avoid rate limits."""
        total = len(self.sessions)
        for i, session in enumerate(self.sessions):
            try:
                session.login()
                self._logged_in_count += 1
                ACTIVE_CONNECTIONS.set(self._logged_in_count)
                logger.info("Session %s/%s logged in.", i + 1, total)

                # Jitter sleep between logins, but not after the last one
                if i < total - 1:
                    delay = random.uniform(jitter_min, jitter_max)
                    time.sleep(delay)
            except Exception as exc:
                logger.error("Failed to login session %s/%s: %s", i + 1, total, exc)
                # Continue trying others

    def logout_all(self) -> None:
        """Logout all sessions."""
        for session in self.sessions:
            try:
                session.logout()
            except Exception:
                pass
        self._logged_in_count = 0
        ACTIVE_CONNECTIONS.set(0)

    def iter_sessions(self) -> Iterator[ShioajiSession]:
        """Yield all sessions."""
        return iter(self.sessions)

    def get_session(self, index: int) -> ShioajiSession:
        """Get a specific session by index (modulo size)."""
        if not self.sessions:
            raise RuntimeError("ConnectionPool is empty.")
        return self.sessions[index % len(self.sessions)]

    @property
    def logged_in_count(self) -> int:
        return self._logged_in_count
