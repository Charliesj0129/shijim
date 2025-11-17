from __future__ import annotations

from queue import Empty

import pytest

from shijim.events import MDBookEvent, MDTickEvent
from shijim.recorder.ingestion import IngestionWorker


class DummySubscriber:
    def __init__(self, events):
        self.events = list(events)

    def get(self, timeout=None):
        if not self.events:
            raise Empty
        return self.events.pop(0)


class DummyWriter:
    def __init__(self):
        self.events = []

    def append(self, event):
        self.events.append(event)


def _tick_event():
    return MDTickEvent(
        ts=1,
        symbol="TXF",
        asset_type="futures",
        exchange="TAIFEX",
    )


def test_ingestion_worker_processes_events():
    event = _tick_event()
    subscriber = DummySubscriber([event])
    raw_writer = DummyWriter()
    ch_writer = DummyWriter()

    worker = IngestionWorker(subscriber, raw_writer, ch_writer)
    assert worker.run_once() is True
    assert raw_writer.events == [event]
    assert ch_writer.events == [event]


def test_ingestion_worker_skips_invalid_events(caplog):
    subscriber = DummySubscriber([{"type": "unknown"}])
    raw_writer = DummyWriter()
    worker = IngestionWorker(subscriber, raw_writer)

    assert worker.run_once() is True
    assert raw_writer.events == []
