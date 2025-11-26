import time

from shijim.monitoring.observers import GapDetector, LatencyMonitor, ThroughputMonitor


class MockEvent:
    def __init__(self, ts_ns, symbol="TEST"):
        self.ts_ns = ts_ns
        self.symbol = symbol

def test_throughput_monitor():
    monitor = ThroughputMonitor(window_secs=1)
    # Just verify it doesn't crash and updates internal state
    monitor.on_event(MockEvent(0))
    assert len(monitor._counts) == 1

    # Simulate many events
    for _ in range(10):
        monitor.on_event(MockEvent(0))
    assert len(monitor._counts) == 11

def test_gap_detector(caplog):
    detector = GapDetector(tolerance_ns=100)

    # First event, no gap
    detector.on_event(MockEvent(1000, "SYM1"))

    # Second event, small gap (ok)
    detector.on_event(MockEvent(1050, "SYM1"))

    # Third event, large gap (should warn)
    detector.on_event(MockEvent(1200, "SYM1"))

    # Check logs
    assert "Gap detected for SYM1" in caplog.text
    assert "150 ns > 100" in caplog.text

def test_latency_monitor():
    monitor = LatencyMonitor(max_samples=5)
    now_ns = time.time_ns()

    # Event from 1ms ago
    monitor.on_event(MockEvent(now_ns - 1_000_000))

    assert len(monitor.samples) == 1
    assert monitor.samples[0] >= 1_000_000

    # Fill buffer
    for _ in range(10):
        monitor.on_event(MockEvent(now_ns))

    assert len(monitor.samples) == 5  # Max samples respected
