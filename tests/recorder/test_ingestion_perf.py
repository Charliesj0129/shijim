import time
from unittest.mock import MagicMock

from shijim.events.schema import MDTickEvent
from shijim.recorder.ingestion import IngestionWorker


def test_ingestion_flush_parallelism():
    # Mock writers
    raw_writer = MagicMock()
    analytical_writer = MagicMock()

    # Simulate latency
    def slow_write(*args, **kwargs):
        time.sleep(0.1)

    raw_writer.write_batch.side_effect = slow_write
    analytical_writer.write_batch.side_effect = slow_write

    # Setup worker
    bus = MagicMock()
    bus.subscribe.return_value = iter([])

    worker = IngestionWorker(
        bus=bus,
        raw_writer=raw_writer,
        analytical_writer=analytical_writer,
        max_buffer_events=10
    )

    # Add some data
    event = MDTickEvent(ts_ns=1, symbol="S", asset_type="stock", exchange="E", price=1.0, size=1)
    worker._ticks_buffer.append(event)

    # Measure flush time
    start = time.monotonic()
    worker.flush()
    duration = time.monotonic() - start

    # Cleanup
    worker._executor.shutdown()

    # Assertions
    # If sequential: 0.1 + 0.1 = 0.2s
    # If parallel: max(0.1, 0.1) = 0.1s + overhead
    # We expect duration < 0.15s
    print(f"Flush duration: {duration:.4f}s")
    assert duration < 0.18, f"Flush took too long: {duration}s (expected parallel execution)"
    assert duration >= 0.1, "Flush took too short, did it wait?"

    # Verify both were called
    raw_writer.write_batch.assert_called_once()
    analytical_writer.write_batch.assert_called_once()
