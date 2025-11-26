# Architecture & Performance Improvement Plan

Our throughput target (10^3 instruments × 10^5 events/sec) plus <5 ms p99 latency demands continuous tuning. Below are prioritized architectural upgrades with concrete actions, expected complexity, and projected gains.

## 1. Gateway & Event Bus
- **Current**: `CollectorContext` publishes directly into an `InMemoryEventBus` with coarse locking; futures/stocks分享同一個 queue。
- **Issues**:
  - Lock contention when callbacks burst (Python GIL + `Condition` wakeups)。
  - No per-topic backpressure enforcement; single slow consumer can evict hot symbols.
- **Actions**:
  1. Introduce a hybrid bus: multi-queue architecture（例如 per-asset asyncio.Queue 或 multiprocessing.Queue）搭配 dispatcher，減少共享鎖競爭。
  2. Add `publish_many` for vectorized event pushes from callbacks to cut per-event lock overhead (expected O(1) amortized vs O(N) now).
  3. Implement `lag_metrics` per topic, exported to Prometheus for adaptive throttling。
- **Impact**: >30% reduction in callback-to-bus latency under burst tests, more deterministic queue usage.
- **Complexity**: Medium.

## 2. Recorder Pipeline
- **Current**: `IngestionWorker` batches sequentially (`max_batch_events` 512) and flushes writers synchronously; histogram/metrics instrumentation is limited.
- **Issues**:
  - Flushing both `RawWriter` and `ClickHouseWriter` sequentially doubles latency per batch.
  - Async writer queues lack drop metrics; fallback activation is reactive.
- **Actions**:
  1. Parallelize raw vs analytical writers using a thread pool or `asyncio` to overlap serialization and network I/O.
  2. Add `INGESTION_QUEUE_DROPPED_TOTAL` counters plus logging for `async_enqueue_timeout` hits.
  3. Integrate zero-copy serialization (e.g., `orjson.dumps` on dataclass `__slots__` view) to reduce conversions.
  4. Tune `flush_interval` adaptively by measuring traffic rate (EWMA) and shrinking to keep per-batch latency <2 ms.
- **Impact**: halves flush latency, removes hidden drops, increases sustainable throughput above 150 k events/sec on commodity hardware.
- **Complexity**: Medium.

## 3. ClickHouse Writer
- **Current**: uses blocking client inserts with optional `async_insert` flags; fallback files accumulate silently.
- **Issues**:
  - Backpressure when ClickHouse is remote (RTT dominates) causing `_tick_buffer` to grow.
  - Fallback_dir lacks rotation or alerting thresholds.
- **Actions**:
  1. Switch to HTTP interface with `INSERT FORMAT JSONEachRow` and gzip payloads to minimize RTT (batch flush complexity O(N) vs O(N * handshake)).
  2. Add `fallback_size_bytes` gauge and alert when >X GB; auto-ship fallback files to object storage nightly.
  3. Support ClickHouse `async_insert_max_data_size` tuning via env, plus metrics for `_task_queue.qsize()`.
  4. Provide `RetryPolicy` abstraction (exponential backoff, jitter) to avoid hammering DB during incidents.
- **Impact**: reduces CPU time spent in network stack, provides deterministic failure handling, speeds up drains by ~40%.
- **Complexity**: Medium.

## 4. Strategy Loop
- **Current**: `StrategyRunner` reads from a generic reader; OFI calculation happens in Python using numpy-like operations.
- **Issues**:
  - Parsing raw bytes與建構 `BboState` 造成額外的 GC 負擔。
  - OFI/microstructure calculations allocate lists/ndarrays each tick。
- **Actions**:
  1. Move OFI + MLOFI calculators into `shijim_indicators` (PyO3) 並提供零拷貝 batch API；保留 numpy/Numba fallback。
  2. 提供 `decode_bbo(payload) -> tuple` 的純 Python 快取化實作，避免大量 dataclass 產生。
  3. Pre-allocate order state logs/lists to avoid per-tick growth; apply `__slots__` for `OrderRequest`.
  4. Add `perf` counters for `StrategyRunner.tick()` to capture percentile latencies (export via Prometheus Summary)。
- **Impact**: expected 2–3× reduction in per-tick CPU time; removes GC pauses that currently show up as 5 ms spikes.
- **Complexity**: Medium.

## 5. Risk & Order Routing
- **Current**: `RiskAwareGateway` checks guards sequentially and uses synchronous send path; rate limiter is leaky when `send` called in bursts.
- **Issues**:
  - No asynchronous order acknowledgement handling; will break once `timeout=0` is used (per `sinotrade_tutor_md/advanced/nonblock.md`).
  - Guard telemetry not persisted, so tuning thresholds is guesswork.
- **Actions**:
  1. Replace `RateLimiter` with a lock-free token bucket (per-side) leveraging monotonic time.
  2. Persist `RiskResult` outcomes + account snapshots (see `accounting/margin.md`) into ClickHouse to close the loop on risk settings.
  3. Introduce async order router (see FEATURES.md) with ack timeouts + kill-switch integration.
- **Impact**: ensures risk remains non-blocking, prepares gateway for real-time ack flows, and yields actionable stats for guard tuning.
- **Complexity**: Medium/Large.

## 6. Broker Integration & Limits
- **Current**: CLI enforces trading window but not traffic caps; subscription manager only rate-limits via sleeps.
- **Issues**:
  - Violating `sinotrade_tutor_md/limit.md` caps leads to kicked sessions.
- **Actions**:
  1. Poll `api.usage()` every minute, compute remaining bytes, adjust `batch_size` and optionally pause low-priority symbols.
  2. Add `SubscriptionPlan` priorities plus auto-unsubscribe/subscribe rotation for long-tail names.
  3. Provide per-connection metrics/dashboards for connection count, login attempts, and request frequency.
- **Impact**: keeps gateway compliant, reduces forced reconnects, improves fairness between shards.
- **Complexity**: Medium.

## 7. Backtest & Toolchain
- **Current**: converters and adapters are Python-based and single-threaded.
- **Issues**:
  - Large `.jsonl` → `.npz` conversion saturates CPU for hours; no streaming writer.
- **Actions**:
  1. Parallelize conversion using multiprocessing, chunking JSONL files and writing to shared memmap.
  2. Align converter schema with SBE definitions used by downstream simulators，避免重複解碼。
  3. Add deterministic seeds + metrics to `hft_converter` for reproducible test loads.
- **Impact**: reduces backfill time from hours to minutes, enabling nightly dataset refreshes.
- **Complexity**: Medium.

## 8. Observability & Alerting
- **Current**: Prometheus metrics exist but no alerting rules or dashboards are checked into repo.
- **Issues**:
  - Hard to detect slow degradations (high latency histograms, gap detectors).
- **Actions**:
  1. Ship default Grafana dashboards + Prometheus alert rules (SLOs for ingestion latency, bus backlog, ClickHouse fallback activation).
  2. Extend dashboard snapshots with `risk_state`, `latency_p99`, `gap_count`.
  3. Use distributed tracing (OpenTelemetry) around ingestion→writer→strategy path for flamegraph analysis.
- **Impact**: faster incident response, data to justify future optimizations, compliance-friendly audit logs.
- **Complexity**: Small/Medium.

---
Each item should be tracked with owners, deadlines, and perf KPIs (latency histograms, throughput counters). Re-run micro-benchmarks after each change to guard against regressions.

## Verification Results (2025-11-26)

### Rust Indicators (VPIN & Hawkes)
- **Integration**: `shijim.features` wrappers correctly delegate to `shijim_indicators` (Rust) with fallback to Python.
- **Correctness**: Verified via `tests/features/test_integrated_features.py` (10 tests passed).
- **Performance**:
  - **VPIN Update**: ~0.92 μs (mean) / 1,081 Kops/s
  - **Hawkes Update**: ~1.56 μs (mean) / 639 Kops/s
  - Measured via `pytest-benchmark` calling Python wrappers.
  - **Note**: Native Rust micro-benchmarks (`cargo bench`) were skipped due to linker issues with the current environment's `cc` shim. Python-side benchmarks confirm the target O(1) latency (<2μs).

### Next Steps
- Re-run native `cargo bench` once a system compiler is available to capture allocator-level profiles.
- Add CI hooks for `pytest-benchmark` to prevent regression.
