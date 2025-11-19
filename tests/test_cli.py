from __future__ import annotations

import types

import pytest

import shijim.cli as cli


class DummySession:
    def __init__(self, calls):
        self.calls = calls

    def login(self):
        self.calls.append("login")
        return object()

    def logout(self):
        self.calls.append("logout")

    def get_contract(self, code, asset_type):  # pragma: no cover - not used
        return object()


class DummyManager:
    def __init__(self, *args, **kwargs):
        self.subscribed = False
        self.unsubscribed = False

    def subscribe_universe(self):
        self.subscribed = True

    def unsubscribe_all(self):
        self.unsubscribed = True


class DummyWorker:
    def __init__(self, *args, **kwargs):
        self.run_called = False
        self.stop_called = False

    def run_forever(self):
        self.run_called = True

    def stop(self):
        self.stop_called = True


class DummyContext:
    pass


def _patch_cli(monkeypatch, worker_cls=DummyWorker, manager_cls=DummyManager, raise_in_run=False):
    calls: list[str] = []
    session = DummySession(calls)
    monkeypatch.setattr(cli, "ShioajiSession", lambda mode="live": session)
    monkeypatch.setattr(cli, "InMemoryEventBus", lambda: object())
    monkeypatch.setattr(cli, "CollectorContext", lambda **kwargs: DummyContext())
    monkeypatch.setattr(cli, "attach_quote_callbacks", lambda api, ctx: None)
    monkeypatch.setattr(cli, "get_smoke_test_universe", lambda: types.SimpleNamespace(futures=["TXF"], stocks=["2330"]))

    class Manager(manager_cls):
        def __init__(self, *args, **kwargs):
            super().__init__()

    monkeypatch.setattr(cli, "SubscriptionManager", Manager)

    class Worker(worker_cls):
        def __init__(self, *args, **kwargs):
            super().__init__()

        def run_forever(self):
            if raise_in_run:
                raise RuntimeError("boom")
            return super().run_forever()

    monkeypatch.setattr(cli, "IngestionWorker", Worker)
    monkeypatch.setattr(cli, "RawWriter", lambda root: object())
    monkeypatch.setattr(cli, "ClickHouseWriter", lambda dsn: object())

    return calls, session, Manager, Worker


def test_cli_calls_login_and_logout(monkeypatch):
    calls, session, Manager, Worker = _patch_cli(monkeypatch)

    exit_code = cli.main([])

    assert exit_code == 0
    assert calls == ["login", "logout"]


def test_cli_returns_nonzero_on_worker_failure(monkeypatch, caplog):
    calls, session, Manager, Worker = _patch_cli(monkeypatch, raise_in_run=True)

    with caplog.at_level("ERROR"):
        exit_code = cli.main([])

    assert exit_code == 1
    assert calls == ["login", "logout"]
    assert any("Fatal error" in record.message for record in caplog.records)
