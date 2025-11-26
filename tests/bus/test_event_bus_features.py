from __future__ import annotations

from shijim.bus import InMemoryEventBus, BroadcastEventBus
from shijim.events import MDTickEvent


def _tick(ts: int) -> MDTickEvent:
    return MDTickEvent(ts_ns=ts, symbol=f"TXF{ts}", asset_type="futures", exchange="TAIFEX")


def test_in_memory_publish_many_and_lag():
    bus = InMemoryEventBus()
    
    # Test initial lag
    assert bus.get_lag("MD_TICK") == {"MD_TICK": 0}
    assert bus.get_lag() == {}

    # Subscribe
    sub = bus.subscribe("MD_TICK", timeout=0)
    
    # Publish many
    events = [_tick(i) for i in range(1, 6)]
    bus.publish_many(events)
    
    # Check lag
    lag = bus.get_lag("MD_TICK")
    assert lag["MD_TICK"] == 5
    
    # Check lag all
    all_lag = bus.get_lag()
    assert all_lag["MD_TICK"] == 5
    
    # Consume
    consumed = []
    for _ in range(5):
        consumed.append(next(sub))
        
    assert consumed == events
    assert bus.get_lag("MD_TICK")["MD_TICK"] == 0


def test_broadcast_publish_many_and_lag():
    bus = BroadcastEventBus()
    
    # Subscribe
    sub_a = bus.subscribe("MD_TICK", timeout=0)
    sub_b = bus.subscribe("MD_TICK", timeout=0)
    
    # Publish many
    events = [_tick(i) for i in range(1, 6)]
    bus.publish_many(events)
    
    # Check lag (max lag among subscribers)
    lag = bus.get_lag("MD_TICK")
    assert lag["MD_TICK"] == 5
    
    # Consume from A
    next(sub_a)
    next(sub_a)
    
    # Lag should still be 5 because B hasn't consumed anything
    assert bus.get_lag("MD_TICK")["MD_TICK"] == 5
    
    # Consume all from B
    for _ in range(5):
        next(sub_b)
        
    # Lag should now be 3 (A has 3 left)
    assert bus.get_lag("MD_TICK")["MD_TICK"] == 3
    
    # Consume rest from A
    for _ in range(3):
        next(sub_a)
        
    assert bus.get_lag("MD_TICK")["MD_TICK"] == 0
