"""Shioaji session management utilities.

The implementation mirrors the official Shioaji workflow documented in
``sinotrade_tutor_md/login.md`` and ``sinotrade_tutor_md/contract.md``:

```
import shioaji as sj
api = sj.Shioaji(simulation=True)
api.login(api_key="...", secret_key="...", contracts_timeout=10000)
api.fetch_contracts(contract_download=True)
```

The :class:`ShioajiSession` class handles environment-driven configuration,
ensures contract metadata (stocks, futures, options) is present before
handing the session back to callers, and keeps retry logic lightweight so the
gateway can recover from transient network/auth issues.
"""

from __future__ import annotations

import importlib
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

EnvBool = Callable[[Optional[str]], bool]


def _env_bool(value: Optional[str], default: bool = True) -> bool:
    """Parse environment variables like ``"true"`` / ``"0"`` into booleans."""
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on", "y"}


@dataclass
class SessionConfig:
    """Runtime configuration for :class:`ShioajiSession`."""

    api_key: str
    secret_key: str
    simulation: bool = True
    contracts_timeout: int = 10_000
    max_retries: int = 3
    retry_delay: float = 3.0


class ShioajiSession:
    """Manage the lifecycle of a Shioaji API instance."""

    def __init__(
        self,
        *,
        config: Optional[SessionConfig] = None,
        api_factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        self._config = config
        self._api_factory = api_factory
        self._api: Any = None
        self._accounts: Any = None

    @property
    def api(self) -> Any:
        """Return the active Shioaji API instance, if connected."""
        return self._api

    def connect(self) -> Any:
        """Establish a Shioaji session and preload contracts."""
        if self._api is not None:
            logger.debug("Shioaji session already established; reusing.")
            return self._api

        config = self._config or self._load_config_from_env()
        attempt = 0
        last_error: Optional[Exception] = None

        while attempt < config.max_retries:
            attempt += 1
            try:
                api = self._create_api(simulation=config.simulation)
                logger.info("Logging in to Shioaji (attempt %s/%s).", attempt, config.max_retries)
                accounts = api.login(
                    api_key=config.api_key,
                    secret_key=config.secret_key,
                    contracts_timeout=config.contracts_timeout,
                )
                self._api = api
                self._accounts = accounts
                self._ensure_contracts_ready(api)
                logger.info("Shioaji login successful. Contracts ready.")
                return api
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                logger.warning(
                    "Shioaji login attempt %s/%s failed: %s",
                    attempt,
                    config.max_retries,
                    exc,
                )
                self._teardown_api()
                if attempt < config.max_retries:
                    time.sleep(config.retry_delay)

        assert last_error is not None  # for mypy/pylint
        raise RuntimeError("Failed to connect to Shioaji after retries") from last_error

    def disconnect(self) -> None:
        """Logout and clear session state safely."""
        if self._api is None:
            logger.debug("Shioaji session already disconnected.")
            return

        logout_fn = getattr(self._api, "logout", None)
        if callable(logout_fn):
            try:
                logout_fn()
                logger.info("Shioaji session logged out.")
            except Exception as exc:  # noqa: BLE001
                logger.warning("Shioaji logout raised an exception: %s", exc)

        self._teardown_api()

    # --------------------------------------------------------------------- #
    # Internal helpers
    # --------------------------------------------------------------------- #
    def _load_config_from_env(self) -> SessionConfig:
        api_key = os.getenv("SHIOAJI_API_KEY")
        secret_key = os.getenv("SHIOAJI_SECRET_KEY")
        if not api_key or not secret_key:
            raise ValueError("Missing SHIOAJI_API_KEY/SHIOAJI_SECRET_KEY in environment.")
        simulation = _env_bool(os.getenv("SHIOAJI_SIMULATION"), default=True)
        return SessionConfig(api_key=api_key, secret_key=secret_key, simulation=simulation)

    def _create_api(self, *, simulation: bool) -> Any:
        if self._api_factory is not None:
            return self._api_factory(simulation=simulation)
        sj = importlib.import_module("shioaji")
        return sj.Shioaji(simulation=simulation)

    def _ensure_contracts_ready(self, api: Any) -> None:
        """Ensure stock/futures/options contracts exist or fetch them."""
        if self._contracts_ready(api):
            return

        fetch_contracts = getattr(api, "fetch_contracts", None)
        if not callable(fetch_contracts):
            raise RuntimeError("Shioaji API instance missing fetch_contracts.")

        logger.info("Fetching Shioaji contracts (stocks/futures/options).")
        fetch_contracts(contract_download=True)
        if not self._contracts_ready(api):
            raise RuntimeError("Contracts still unavailable after fetch_contracts.")

    def _contracts_ready(self, api: Any) -> bool:
        contracts = getattr(api, "Contracts", None)
        if contracts is None:
            return False
        required = ("Stocks", "Futures", "Options")
        ready = all(getattr(contracts, attr, None) for attr in required)
        if not ready:
            missing = [attr for attr in required if not getattr(contracts, attr, None)]
            logger.debug("Contracts missing categories: %s", ", ".join(missing))
        return ready

    def _teardown_api(self) -> None:
        self._api = None
        self._accounts = None
