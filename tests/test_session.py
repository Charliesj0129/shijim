from __future__ import annotations

import pytest

import shijim.gateway.session as session_module
from shijim.gateway.session import ShioajiSession


class _FakeContracts:
    def __init__(self) -> None:
        self.Stocks = {"2330": object()}
        self.Futures = {"TXF": object()}


class _FakeAPI:
    def __init__(self, *, simulation: bool) -> None:
        self.simulation = simulation
        self.login_kwargs: dict[str, object] | None = None
        self.logout_calls = 0
        self.Contracts = _FakeContracts()

    def login(self, **kwargs) -> None:
        self.login_kwargs = kwargs

    def logout(self) -> None:
        self.logout_calls += 1

    def fetch_contracts(self, contract_download: bool = True) -> None:  # pragma: no cover
        return None


def _set_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SHIOAJI_API_KEY", "key")
    monkeypatch.setenv("SHIOAJI_SECRET_KEY", "secret")
    monkeypatch.setenv("SHIOAJI_CA_PATH", "/tmp/ca")


def test_login_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_credentials(monkeypatch)
    instances: list[_FakeAPI] = []

    def fake_factory(*, simulation: bool) -> _FakeAPI:
        api = _FakeAPI(simulation=simulation)
        instances.append(api)
        return api

    monkeypatch.setattr(session_module.sj, "Shioaji", fake_factory)

    session = ShioajiSession(mode="live")
    api = session.login()

    assert api is instances[0]
    assert api.simulation is False
    assert api.login_kwargs == {
        "api_key": "key",
        "secret_key": "secret",
        "ca_path": "/tmp/ca",
        "contracts_timeout": 10000,
        "fetch_contract": True,
    }

    assert session.get_api() is api

    session.logout()
    assert instances[0].logout_calls == 1


def test_login_retries_and_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_credentials(monkeypatch)
    attempts: list[int] = []

    class FailingAPI(_FakeAPI):
        def login(self, **kwargs) -> None:
            attempts.append(1)
            raise RuntimeError("bad credentials")

    def fake_factory(*, simulation: bool) -> FailingAPI:
        return FailingAPI(simulation=simulation)

    monkeypatch.setattr(session_module.sj, "Shioaji", fake_factory)
    sleep_calls: list[float] = []
    monkeypatch.setattr(session_module.time, "sleep", sleep_calls.append)

    max_retries = 3
    session = ShioajiSession(mode="simulation", max_retries=max_retries, backoff_seconds=0.1)

    with pytest.raises(RuntimeError, match="Failed to login"):
        session.login()

    assert len(attempts) == max_retries
    assert len(sleep_calls) == max_retries - 1

    with pytest.raises(RuntimeError):
        session.get_api()
