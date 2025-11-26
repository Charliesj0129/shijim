# Shijim / Micro Alpha Project Brief

## Mission & Constraints
- **Objective**: provide a reusable, latency-aware skeleton for ingesting Shioaji market data, persisting normalized events, running microstructure-aware strategies, and replaying feeds for backtests.
- **Latency budgets**: p99 tick→strategy 與 tick→storage 流程需維持在數毫秒內，即使面對 10^8 events/hour；我們透過事件匯流排與批次 writer 控制 enqueue/flush 延遲。
- **Reliability**: fail fast on schema violations yet never lose data—raw JSONL batches, ClickHouse fallbacks, and Prometheus-based observers must surface issues immediately.

## System Layout
| Layer | Responsibilities | Key Modules |
| --- | --- | --- |
| Gateway | Maintain Shioaji session, subscribe/unsubscribe thousands of symbols, normalize raw callbacks | `shijim.gateway.*`, `shijim.events.normalizers`, `sinotrade_tutor_md/*` (login/contracts/order semantics) |
| Event Bus | Fan-out to recorder, dashboards, strategy simulators | `shijim.bus.event_bus` |
| Recorder | Batch writes to append-only raw files + ClickHouse, detect gaps/latency | `shijim.recorder.*`, `shijim.monitoring.observers` |
| Strategy/Risk | Smart chasing logic, OFI-based context, fat-finger & position guards | `shijim.strategy.*`, `shijim.risk.*` |
| Indicator Acceleration (optional) | Rust-backed OFI/MLOFI/VPIN/Hawkes calculators via PyO3 | `shijim_indicators` |
| Backtest & Tools | Feed conversion + adapters for `hftbacktest`, data converters | `shijim.backtest.*`, `shijim.tools.*` |
| Dashboard | Textual TUI snapshots + kill switch hooks | `shijim.dashboard.*` |

## Data Flow
1. **Session bootstrap** (`shijim.cli.main`):
   - Resolve environment (jitter, shards, ClickHouse DSN, directories).
   - `ShioajiSession` (`shijim/gateway/session.py`) logs in using token credentials (**see** `sinotrade_tutor_md/login.md`, `token.md`), enforces contract cache, and exposes quote/order/account handles.
2. **Universe selection & sharding**:
   - `UniverseNavigator` applies scanner-driven or ClickHouse-powered ranking (top volume, volatility) then shards via `shard_config_from_env`.
   - Configurable env knobs (`UNIVERSE_*`, `SHARD_ID/TOTAL_SHARDS`) align ingestion load with throughput targets.
3. **Gateway callbacks**:
   - `CollectorContext` wires Shioaji `on_tick_*` and `on_bidask_*` callbacks into normalizers; `AssetRouting` ensures futures vs equities stay isolated.
   - Normalized `MDTickEvent`/`MDBookEvent` objects encode timestamps (ns), metadata, and L2 vectors in dataclasses.
4. **Event bus fan-out**:
   - `InMemoryEventBus` (competing consumer) and `BroadcastEventBus` (per-subscriber queue) provide bounded buffering with adaptive warnings when queue depth approaches `SHIJIM_BUS_MAX_QUEUE`.
5. **Recorder**:
   - `IngestionWorker` batches per type (ticks/books) with configurable thresholds (`max_buffer_events`, `max_batch_wait`) and flushes to:
     - `RawWriter`: JSONL rotation by trading day and symbol with async queue + rotation guard rails.
     - `ClickHouseWriter`: columnar batches (tick + book tables) with optional async insert; falls back to filesystem when DB outages occur.
   - Observers (throughput, gap detector, latency monitor) attach for Prometheus metrics.
6. **Strategy & execution**:
   - `SmartChasingEngine` consumes BBO snapshots and OFI (from `OfiCalculator`) to issue `OrderRequest`s, guarding chase rounds and alpha-confirmed moves.
   - `RiskAwareGateway` enforces fat-finger checks, per-side position caps, rate limiting, and kill switch semantics before real broker submission (`sinotrade_tutor_md/order/*` docs detail order/deal events and non-blocking behaviors).
- **Optional Rust acceleration**:
   - When `shijim_indicators` is built via `maturin`, strategy/feature modules can switch to zero-copy Rust OFI/MLOFI/VPIN/Hawkes calculators transparently; pure Python fallbacks remain available for dev/testing。
7. **Backtest integration**:
   - `shijim.backtest.adapter.HftBacktestAdapter` replays recorded feeds (`hftbacktest` structures) into live engines for parity checks.
   - `shijim.backtest.converter` bridges SBE-like events into `.npz` datasets.

## Broker Integration Notes (from `sinotrade_tutor_md`)
- **Credentialing**: API keys + secret tokens (with IP locks, expiration) are mandatory; calibrate system clocks to avoid `Sign data is timeout`. (See `login.md`, `token.md`, `prepare/token.md`.)
- **Contracts**: fetch via `api.fetch_contracts` or rely on cached loads; `Contracts.status` signals readiness (`contract.md`).
- **Traffic & rate limits**: order, data, and portfolio endpoints have sliding windows (e.g., ticks limited to 10 queries during trading hours, subscribe cap 200 contracts, API traffic quotas tied to recent volume). (`limit.md`)
- **Market data APIs**: streaming (`sinotrade_tutor_md/market_data/streaming/*.md`) vs historical (`historical.md`), credit enquiries, scanners, and snapshots all include schemas useful for validation and fallback fetchers.
- **Order lifecycle**: stock/futures order & deal callbacks include `operation`, `order`, `status`, and `contract` sections; `op_type` enumerations support new/cancel/update flows. Non-blocking submissions rely on callbacks or asynchronous placeholders (`order/*`, `order_deal_event/*`, `advanced/nonblock.md`).
- **Advanced playbooks**: tutorials cover quote managers, touch-price triggers, redis fan-out, and callback integration patterns that inform future automation modules (`advanced/*.md`, `callback/*`).
- **Account & settlement**: `accounting/*` docs describe balance, P&L, settlement, and margin APIs—useful for risk manager data feeds.

## Operational Guidance
- **Bootstrap**: `python -m shijim --simulation` for mock mode (set `SHIOAJI_SIMULATION=1` for CLI parity). Provide `SHIOAJI_API_KEY/SECRET_KEY`, `SHIJIM_RAW_DIR`, `SHIJIM_FALLBACK_DIR`, `CLICKHOUSE_DSN`, and optional `UNIVERSE_*` knobs.
- **Observability**: Prometheus metrics (ingestion throughput, batch sizes, latency histograms, risk rejects) feed into Grafana; `shijim.dashboard.app` offers a 10 Hz textual status board with kill switch keybinds.
- **Backfill & Conversion**: `shijim.tools.hft_converter` transforms raw JSONL or broker dumps into `hftbacktest` format。
- **Rust indicators**: `maturin develop --manifest-path shijim_indicators/Cargo.toml` builds PyO3 extensions; `pytest -k rust_ofi` validates parity before啟用。
- **Testing**: `ruff check shijim tests`, `pytest --maxfail=1 --disable-warnings`; `tests/` contains behavior-driven specs for OFI calculators, recorders, and gateway utilities.
- **Deployment**: Dockerfiles wrap the CLI + dependencies; GitHub Actions pipeline (lint → test → package → docker → release) ensures reproducible builds.

## Data Contracts & Interfaces
- **Events**: `MDTickEvent` and `MDBookEvent` (see `shijim/events/schema.py`) carry normalized data to every subsystem. Extend `extras` map carefully; keep types JSON-friendly for `orjson`.
- **Order API**: `OrderRequest` (strategy) → `RiskAwareGateway` ensures only validated actions reach real brokers; `sinotrade_tutor_md/order/` enumerates broker-side invariants (lot sizes, price types, combos).
- **Event Bus Contract**: `shijim/bus/event_bus.py` exposes competing-consumer與廣播兩種模式；策略/錄製/儀表板皆透過這個介面取得實時資料。

## Roadmap & Focus Areas
1. **Latency Audits**: instrumentation around `IngestionWorker` flush loops and ClickHouse async queue backpressure.
2. **Rust Indicator Parity**: extend `shijim_indicators` beyond OFI (MLOFI/VPIN/Hawkes)、加上批次 API 與 Python fallback 自動切換。
3. **Universe Service**: externalize ranking logic (ClickHouse, alternative data) to reduce CLI bootstrap coupling.
4. **Risk Telemetry**: persist risk rejects / kill switch activations for post-mortems。
5. **Broker Coverage**: align strategy/risk modules with all permutations from `sinotrade_tutor_md` (non-blocking orders, intraday odd-lot, combo legs) before enabling full automation。

## References
- Core codepaths: `shijim/cli.py`, `shijim/gateway/*`, `shijim/recorder/*`, `shijim/strategy/*`, `shijim/risk/*`, `shijim/runner.py`, `shijim_indicators/`.
- Broker docs: `sinotrade_tutor_md/` tree (login/token, limits, market data, order callbacks, accounting, advanced tutorials).

This document should be kept current as we evolve the stack; treat it as the single source of truth for collaborators ramping onto Micro Alpha.
