from __future__ import annotations

from types import SimpleNamespace

import pytest

from shijim.gateway.session import SessionConfig, ShioajiSession


class FakeShioaji:
    def __init__(self, *, simulation: bool) -> None:
        self.simulation = simulation
        self.login_calls = 0
        self.fetch_contracts_calls = 0
        self.logout_calls = 0
        self.Contracts = SimpleNamespace(Stocks=None, Futures=None, Options=None)

    def login(self, **kwargs):
        self.login_calls += 1
        return ["FUTURE", "STOCK"]

    def fetch_contracts(self, contract_download: bool = True) -> None:
        self.fetch_contracts_calls += 1
        self.Contracts = SimpleNamespace(
            Stocks={"2330": object()},
            Futures={"TXF": object()},
            Options={"TXO": object()},
        )

    def logout(self) -> bool:
        self.logout_calls += 1
        return True


def _session(api_factory, **config_kwargs) -> ShioajiSession:
    config = SessionConfig(api_key="k", secret_key="s", **config_kwargs)
    return ShioajiSession(config=config, api_factory=api_factory)


def test_connect_logs_in_and_fetches_contracts():
    instances: list[FakeShioaji] = []

    def factory(*, simulation: bool) -> FakeShioaji:
        api = FakeShioaji(simulation=simulation)
        instances.append(api)
        return api

    session = _session(factory, simulation=False, max_retries=1)
    api = session.connect()

    assert api is session.api
    assert api.simulation is False
    assert api.login_calls == 1
    assert api.fetch_contracts_calls == 1

    # Subsequent connect should reuse the existing session.
    same_api = session.connect()
    assert same_api is api
    assert api.login_calls == 1


def test_disconnect_is_idempotent():
    instance = FakeShioaji(simulation=True)

    def factory(*, simulation: bool) -> FakeShioaji:
        assert simulation is True
        return instance

    session = _session(factory, simulation=True, max_retries=1)
    session.connect()
    session.disconnect()
    session.disconnect()  # should not raise

    assert instance.logout_calls == 1
