from __future__ import annotations

from shijim.bus import InMemoryEventBus
from shijim.events import MDTickEvent


def _tick(ts: int) -> MDTickEvent:
    return MDTickEvent(ts_ns=ts, symbol=f"TXF{ts}", asset_type="futures", exchange="TAIFEX")


def test_competing_consumers_share_event_queue():
    bus = InMemoryEventBus()

    sub_a = bus.subscribe("MD_TICK", timeout=0)
    sub_b = bus.subscribe("MD_TICK", timeout=0)

    first = _tick(1)
    bus.publish(first)

    assert next(sub_a) is first
    assert next(sub_b) is None  # queue was empty after consumer A drained it

    second = _tick(2)
    bus.publish(second)

    assert next(sub_b) is second  # now consumer B drains the shared queue
    assert next(sub_a) is None
