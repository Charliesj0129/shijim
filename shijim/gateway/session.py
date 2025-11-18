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
class _Credentials:
    api_key: str
    secret_key: str
    person_id: str | None
    ca_path: str | None


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

    def login(self) -> ShioajiAPI:
        """Instantiate `sj.Shioaji` and perform the login handshake."""
        if self._api is not None:
            return self._api

        creds = self._load_credentials()
        simulation = self._mode != "live"
        last_error: Exception | None = None

        for attempt in range(1, self._max_retries + 1):
            api: ShioajiAPI | None = None
            try:
                api = sj.Shioaji(simulation=simulation)
                self._logger.info(
                    "Logging in to Shioaji (attempt %s/%s, mode=%s)",
                    attempt,
                    self._max_retries,
                    self._mode,
                )
                login_kwargs = {
                    "api_key": creds.api_key,
                    "secret_key": creds.secret_key,
                }
                if creds.person_id:
                    login_kwargs["person_id"] = creds.person_id
                if creds.ca_path:
                    login_kwargs["ca_path"] = creds.ca_path
                api.login(**login_kwargs)
                self._api = api
                self._logger.info("Shioaji login successful.")
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

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #
    def _load_credentials(self) -> _Credentials:
        api_key = os.getenv("SHIOAJI_API_KEY")
        secret_key = os.getenv("SHIOAJI_SECRET_KEY")
        if not api_key or not secret_key:
            raise ValueError("SHIOAJI_API_KEY and SHIOAJI_SECRET_KEY must be set.")
        person_id = os.getenv("SHIOAJI_PERSON_ID")
        ca_path = os.getenv("SHIOAJI_CA_PATH")
        return _Credentials(
            api_key=api_key,
            secret_key=secret_key,
            person_id=person_id,
            ca_path=ca_path,
        )

    def _safe_logout(self, api: ShioajiAPI) -> None:
        try:
            api.logout()
            self._logger.info("Shioaji session logged out.")
        except Exception as exc:  # noqa: BLE001
            self._logger.warning("Shioaji logout raised an exception: %s", exc)
