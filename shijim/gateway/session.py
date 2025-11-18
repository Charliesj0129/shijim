"""Session management utilities around the Shioaji API."""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Literal, Protocol

import shioaji as sj


class ShioajiAPI(Protocol):
    """Subset of the Shioaji client used by the gateway."""

    def login(self, api_key: str, secret_key: str, **kwargs) -> object:
        """Authenticate against the broker API."""

    def logout(self) -> None:
        """Terminate the websocket session."""


@dataclass(slots=True)
class _SessionConfig:
    """Runtime configuration loaded from environment variables."""

    api_key: str
    secret_key: str
    ca_path: str | None
    simulation: bool
    contracts_timeout: int
    fetch_contracts: bool


class ShioajiSession:
    """Establishes and maintains a single Shioaji API session."""

    def __init__(
        self,
        *,
        mode: Literal["live", "simulation"] = "simulation",
        logger: logging.Logger | None = None,
        max_retries: int = 3,
        backoff_seconds: float = 1.0,
    ) -> None:
        if mode not in {"live", "simulation"}:
            raise ValueError("mode must be either 'live' or 'simulation'.")
        self._mode = mode
        self._logger = logger or logging.getLogger(__name__)
        self._max_retries = max_retries
        self._backoff = backoff_seconds
        self._api: ShioajiAPI | None = None
        self._config: _SessionConfig | None = None

    def login(self) -> ShioajiAPI:
        """Instantiate `sj.Shioaji` and perform the login handshake."""
        if self._api is not None:
            return self._api

        config = self._load_config()
        self._config = config
        last_error: Exception | None = None

        for attempt in range(1, self._max_retries + 1):
            api: ShioajiAPI | None = None
            try:
                api = sj.Shioaji(simulation=config.simulation)
                self._logger.info(
                    "Logging in to Shioaji (attempt %s/%s, mode=%s)",
                    attempt,
                    self._max_retries,
                    "live" if not config.simulation else "simulation",
                )
                login_kwargs = {
                    "api_key": config.api_key,
                    "secret_key": config.secret_key,
                    "contracts_timeout": config.contracts_timeout,
                    "fetch_contract": config.fetch_contracts,
                }
                if config.ca_path:
                    login_kwargs["ca_path"] = config.ca_path
                api.login(**login_kwargs)
                self._api = api
                self._logger.info("Shioaji login successful.")
                self.ensure_contracts_loaded()
                return api
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                self._logger.warning(
                    "Shioaji login attempt %s/%s failed: %s",
                    attempt,
                    self._max_retries,
                    exc,
                )
                if api is not None:
                    self._safe_logout(api)
                if attempt < self._max_retries:
                    time.sleep(self._backoff * attempt)

        raise RuntimeError("Failed to login to Shioaji after retries.") from last_error

    def logout(self) -> None:
        """Call `api.logout()` and clear local session state."""
        if self._api is None:
            return
        self._safe_logout(self._api)
        self._api = None

    def get_api(self) -> ShioajiAPI:
        """Return the authenticated Shioaji API instance."""
        if self._api is None:
            raise RuntimeError("login() must be called before accessing the API.")
        return self._api

    def ensure_contracts_loaded(self) -> None:
        """Ensure the Contracts cache is populated before use."""
        api = self.get_api()
        if self._contracts_ready(api):
            return

        self._logger.info("Shioaji contracts missing; fetching metadata from broker.")
        fetch_contracts = getattr(api, "fetch_contracts", None)
        if not callable(fetch_contracts):
            raise RuntimeError("Shioaji API missing fetch_contracts method.")
        fetch_contracts(contract_download=True)
        if not self._contracts_ready(api):
            raise RuntimeError("Shioaji contracts still unavailable after fetch_contracts.")
        self._logger.info("Shioaji contracts ready.")

    def get_contract(self, code: str, asset_type: str):
        """Return a contract by code and asset type."""
        self.ensure_contracts_loaded()
        api = self.get_api()
        asset_type = asset_type.lower()
        try:
            if asset_type in {"futures", "future", "fop"}:
                return api.Contracts.Futures[code]
            if asset_type in {"stock", "stocks"}:
                return api.Contracts.Stocks[code]
            if asset_type in {"option", "options"}:
                return api.Contracts.Options[code]
        except KeyError as exc:
            raise KeyError(f"Unknown contract code {code} for asset_type {asset_type}") from exc
        raise ValueError(f"Unsupported asset_type {asset_type}")

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _load_config(self) -> _SessionConfig:
        mode = os.getenv("SHIOAJI_MODE", self._mode)
        simulation = mode.lower() != "live"
        api_key = os.getenv("SHIOAJI_API_KEY")
        secret_key = os.getenv("SHIOAJI_SECRET_KEY")
        if not api_key or not secret_key:
            raise ValueError("SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY must be set.")
        ca_path = os.getenv("SHIOAJI_CA_PATH")
        contracts_timeout = int(os.getenv("SHIOAJI_CONTRACTS_TIMEOUT", "10000"))
        fetch_contracts = os.getenv("SHIOAJI_FETCH_CONTRACTS", "true").lower() not in {"0", "false", "no"}
        return _SessionConfig(
            api_key=api_key,
            secret_key=secret_key,
            ca_path=ca_path,
            simulation=simulation,
            contracts_timeout=contracts_timeout,
            fetch_contracts=fetch_contracts,
        )

    def _safe_logout(self, api: ShioajiAPI) -> None:
        try:
            api.logout()
            self._logger.info("Shioaji session logged out.")
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("Shioaji logout raised an exception: %s", exc)

    def _contracts_ready(self, api: ShioajiAPI) -> bool:
        contracts = getattr(api, "Contracts", None)
        if contracts is None:
            return False
        has_stocks = bool(getattr(contracts, "Stocks", None))
        has_futures = bool(getattr(contracts, "Futures", None))
        return has_stocks and has_futures
