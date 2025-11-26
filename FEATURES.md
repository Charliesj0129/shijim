# Feature Backlog (Candidate Initiatives)

## Snapshot
| Feature | Target Outcome | Complexity | Key Dependencies |
| --- | --- | --- | --- |
| Dynamic Universe Service | Streaming ClickHouse + scanner fusion with shard-aware allocations | M | `shijim.gateway.navigator`, `sinotrade_tutor_md/market_data/*` |
| Rate-Adaptive Subscription Controller | Enforce broker traffic caps while maximizing coverage | M | `shijim.gateway.subscriptions`, `sinotrade_tutor_md/limit.md` |
| Historical Gap Filler & Replayer | Auto-backfill missing ticks via REST + spool to quarantine buffer | M/L | `sinotrade_tutor_md/market_data/historical.md`, `shijim.recorder.gap_replayer` |
| Rust Indicator Expansion | MLOFI/VPIN/Hawkes parity + batch APIs on `shijim_indicators` | M | `shijim_indicators`, `numpy` zero-copy bindings |
| Non-Blocking Order Router | Full support for Shioaji timeout=0 flows with stateful ACK tracking | L | `shijim.strategy.engine`, `shijim.risk.manager`, `sinotrade_tutor_md/advanced/nonblock.md` |
| ClickHouse Stream Tables & Tiered Storage | Reduce ingest latency + add iceberg fallback | M | `shijim.recorder.clickhouse_writer`, ClickHouse cluster |
| Risk & PnL Telemetry Plane | Persist guard hits + accounting snapshots for governance | M | `shijim.risk.*`, `sinotrade_tutor_md/accounting/*` |

## Details

### 1. Dynamic Universe Service
- **Problem**: current shard selection mixes ClickHouse lookbacks and scanner ranks inline; scaling to 10^5 symbols or multi-region ingestion risks blocking CLI startup.
- **Scope**:
  - Promote the logic inside `UniverseNavigator` into a service (REST/gRPC or streaming feed) that pre-computes ranked universes per strategy, shard, and regime (overnight, open auction, intraday).
  - Fuse broker scanners (`market_data/scanners.md`) with internal liquidity analytics (tick rate, queue metrics) for weightings; store under ClickHouse `universe_snapshots`.
  - Add caching + TTL semantics so ingestion nodes can hot swap universes without a restart.
- **Impact**: deterministic <1 s startup, better load balancing, easier experimentation with alternative ranking strategies (complexity O(N log N) sorting per refresh).
- **Risks**: requires schema for publishing universes, needs auth for remote service.

### 2. Rate-Adaptive Subscription Controller
- **Problem**: Shioaji limits (200 concurrent subscriptions, 10 tick queries per trading session burst) require careful sequencing; current manager only slow-sleeps batches.
- **Scope**:
  - Track real-time subscription counts and throttle per `limit.md` quotas (traffic, order counts).
  - Integrate `api.usage()` readings to compute remaining bytes and adjust `batch_size`/`batch_sleep`.
  - Add a `PriorityPlan` abstraction to stage high-touch instruments first and rotate long-tail names based on Observatory signals.
- **Impact**: maximizes coverage without tripping rate caps; avoids forced disconnects, enabling 10^3 instruments steady-state on a single gateway.
- **Complexity**: Medium—requires state machine + broker telemetry interface.

### 3. Historical Gap Filler & Replayer
- **Problem**: gap detector only logs warnings; manual replay/backfill slows investigations.
- **Scope**:
  - Extend `shijim.recorder.gap_replayer` to call `api.ticks`/`api.kbars` (`market_data/historical.md`) for missing windows, convert to normalized events, and append into一個隔離緩衝區供後續匯入。
  - Build automated SLO: if a symbol gap > tolerance persists, queue a backfill task and optionally publish to ClickHouse `gap_events`.
  - Provide CLI to replay those segments into strategy/backtest engines for QA.
- **Impact**: reduces data loss risk; ensures historical datasets remain continuous for models; throughput target ~50 symbols/minute using batch REST fetching.

### 4. Non-Blocking Order Router
- **Problem**: live trading will eventually require `timeout=0` order placement, yet current gateway expects synchronous replies.
- **Scope**:
  - Implement a `NonBlockingOrderManager` that wires Shioaji callbacks (`sinotrade_tutor_md/advanced/nonblock.md`) into stateful action tracking per `OrderRequest`.
  - Persist correlation IDs, sequence numbers, and ack timestamps; integrate with `RiskAwareGateway` to show kill-switch-friendly telemetry.
  - Offer pluggable policy for when to resend/cancel if ack not received within `receive_window`.
- **Impact**: unlocks overlapped order entry, reduces latency budgets by removing blocking RPCs, ensures compliance with broker semantics (requires eventual consistency).
- **Complexity**: Large—touches risk, strategy, CLI, and dashboards.

### 5. ClickHouse Stream Tables & Tiered Storage
- **Problem**: `ClickHouseWriter` flushes via INSERT VALUES; high TPS runs can see 5 ms+ lat spikes and dropped batches when `async_insert` queue backs up.
- **Scope**:
  - Switch to Kafka or `engine=Kafka` ingestion or `INSERT INTO ... FORMAT JSONEachRow` via HTTP with compression to reduce handshake overhead.
  - Add TTL-based tiering (fast SSD vs HDD) and automatic fallback to S3-compatible storage when fallback_dir fills.
  - Enhance `failed_batch_history` reporting and expose metrics for `async_queue` depth.
- **Impact**: increases sustained throughput beyond 10^5 events/sec and reduces CPU per flush; storage tiering guards against disk exhaustion.
- **Complexity**: Medium—requires Infra + ClickHouse coordination.

### 6. Risk & PnL Telemetry Plane
- **Problem**: guard hits and account snapshots (margin, settlements) are not persisted, making audits difficult.
- **Scope**:
  - Stream `RiskResult` failures, kill-switch flips, and rate-limit hits into ClickHouse/Redis for dashboards.
  - On an interval, poll broker accounting endpoints (`accounting/*`) to refresh cash, PnL, and exposure; join with strategy state for compliance alerts.
  - Provide Textual dashboard panes for risk state plus emit structured alerts when thresholds breach.
- **Impact**: improves governance, simplifies regulatory reporting, supports automated derisking logic tied to actual account balances.
- **Complexity**: Medium—requires backfill jobs, storage schema, and UI updates.

### 7. Rust Indicator Expansion
- **Problem**: 只有 OFI 配有 Rust 實作；MLOFI/VPIN/Hawkes 仍在 Python，無法在 10^5 events/sec 條件下持續運行，亦缺少批次 API 與回退策略。
- **Scope**:
  - 在 `shijim_indicators` 中為 MLOFI/VPIN/Hawkes 實作零拷貝 PyO3 類別與批次函式，並補齊單元測試/pytest 對照。
  - 提供 `FeatureCalculator` 介面讓策略層能自動偵測 Rust 擴充，若不可用則落回 numpy/Numba 版本。
  - 引入 Criterion/pytest-benchmark 以量化加速幅度，並在 CI 中確保 `maturin develop` + `pytest -k rust_*` 必須通過。
- **Impact**: 提供 2~5x+ 的指標計算速度、降低 Python GC 壓力，並統一批次 API 以支援回測/重播。
- **Complexity**: Medium—需同時處理 Rust/Numpy FFI、Python fallback 與測試基準。
