# Shijim

Shijim 是基於 Shioaji 的行情採集與資料治理工具鏈，涵蓋「即時訂閱 → 原始/分析落地 → 缺口稽核/補洞 → 動態交易宇宙 → HFT 回測資料轉換」。本 README 整合安裝、設定、CI、容器化與新版 Universe/Scanners 行為。

## 目錄
- [架構與模組](#架構與模組)
- [安裝方式](#安裝方式)
- [快速啟動](#快速啟動)
- [資料治理：稽核 & 補洞](#資料治理稽核--補洞)
- [動態交易宇宙](#動態交易宇宙)
- [HftBacktest 轉換器](#hftbacktest-轉換器)
- [環境變數與設定](#環境變數與設定)
- [CI / 測試](#ci--測試)
- [Docker / 部署](#docker--部署)

## 架構與模組
- `shijim/cli.py`：入口點，啟動 Shioaji session、訂閱分片宇宙、啟動 Recorder（RawWriter + ClickHouseWriter）。
- `shijim/gateway/`：Session 管理、回補、callbacks、Universe Navigator（策略選股：Scanners/上市櫃直取 + bin-packing 分片）。
- `shijim/recorder/`：
  - `raw_writer.py`：原始 JSONL（append-only）。
  - `clickhouse_writer.py`：分析層寫入，支援 async_insert、批次/時間觸發、fallback。
  - `ingestion.py`：批次消費 EventBus，降低函式呼叫/IO 次數。
  - `gap_replayer.py`：序號/時間缺口補洞，支援節流與重試。
- `shijim/governance/`：DataAuditor（缺口掃描）、報告格式、GapReplay 協調器。
- `shijim/tools/`：
  - `restore_failed_batches.py`：重播 fallback JSONL → ClickHouse。
  - `hft_converter.py`：MBO/L5 → HftBacktest `.npz`，保留 exchange_ts/receive_ts 與 latency 模型。
  - `debug_universe.py` / `test_scanners.py`：診斷 Universe/Scanners。
  - `live_smoke_test.py`：連線/訂閱基本自測。
- `tests/`：Pytest 覆蓋 gateway/recorder/governance/converter，CI 每版執行。

## 安裝方式
```bash
python -m pip install --upgrade pip
pip install -e .
pip install -e ".[clickhouse,dev]"   # 需要 ClickHouse 或開發工具 (pytest, flake8, pandas)
```
或 `pip install -r requirements.txt`。

## 快速啟動
1) 匯出金鑰：`SHIOAJI_API_KEY`、`SHIOAJI_SECRET_KEY`（或 `SHIOAJI_SIMULATION=1` 模擬）。
2) 儲存設定（可選）：`SHIJIM_RAW_DIR`、`SHIJIM_FALLBACK_DIR`、`CLICKHOUSE_DSN`。
3) 執行：
```bash
python -m shijim --simulation
```
- 啟動抖動（避免同時暴衝）：`--startup-jitter-seconds N` 或 `SHIJIM_STARTUP_JITTER_SEC`。
- 時段檢查：超過收盤會直接退出。

## 資料治理：稽核 & 補洞
- 稽核：
```bash
python -m shijim.governance.audit \
  --trading-day 2024-01-01 \
  --dsn "clickhouse://user:pass@host:9000/db" \
  --output missing_ranges.json
```
  - Tick：seqno 跳點判定；Orderbook：快照時間窗 + Tick 活動交叉檢測。
  - 報告欄位：`gap_type` (tick/orderbook)、`symbol`、`seq_start/seq_end`、`start_ts/end_ts`、`reason`、`metadata`。
- 補洞：
  - GapReplayer 讀取報告，對 tick 缺口呼叫 Shioaji `ticks()`，normalize → ClickHouse，去重 + 節流 + 重試；可再跑一次 Auditor 驗證 gap_count=0。

## 動態交易宇宙
- 預設使用 **Scanners VolumeRank/AmountRank/ChangePercentRank** 依序嘗試，參數：
  - `UNIVERSE_USE_SCANNER=1` 啟用（預設 0 直接取上市/上櫃合約）。
  - `UNIVERSE_SCANNER_TYPES=VolumeRank,AmountRank`（逗號分隔，依序嘗試）。
  - `UNIVERSE_SCANNER_DATE=YYYY-MM-DD`（Scanners 需要 date，例如 2025-11-21）。
  - `UNIVERSE_SCANNER_COUNT`（每次取回數，通常設為 `UNIVERSE_LIMIT`）。
  - 重試/退避：`UNIVERSE_SCANNER_RETRIES`、`UNIVERSE_SCANNER_BACKOFF`。
- 若 `UNIVERSE_USE_SCANNER=0`：直接取 `api.Contracts.Stocks.TSE/OTC` 清單，經 `UNIVERSE_STOCK_CODE_REGEX` 過濾，取前 `UNIVERSE_LIMIT` 檔。
- 分片：`SHARD_ID`/`TOTAL_SHARDS`，bin-packing 依 weight 分配。

## HftBacktest 轉換器
```bash
python -m shijim.tools.hft_converter \
  --source-type jsonl \
  --source raw/mbo/ \
  --symbol TXF \
  --trading-day 2024-01-01 \
  --output-dir out_npz \
  --latency-mode lognormal \
  --latency-mean-ms 20 --latency-std-ms 5
```
輸出：`*.npz`（含 event_flag/exchange_ts/receive_ts/latency_ns/...），`*.meta.json`，`*.latency_model.json`。負延遲會夾制，序號跳躍插入 GAP。

## 環境變數與設定
- Shioaji：`SHIOAJI_API_KEY`、`SHIOAJI_SECRET_KEY`、`SHIOAJI_SIMULATION`。
- 儲存：`SHIJIM_RAW_DIR`(default `raw/`)、`SHIJIM_FALLBACK_DIR`、`CLICKHOUSE_DSN`。
- 分片：`SHARD_ID`、`TOTAL_SHARDS`。
- Universe：
  - `UNIVERSE_USE_SCANNER` (0=上市櫃直取, 1=Scanners)
  - `UNIVERSE_SCANNER_TYPES`、`UNIVERSE_SCANNER_DATE`、`UNIVERSE_SCANNER_COUNT`、`UNIVERSE_SCANNER_RETRIES`、`UNIVERSE_SCANNER_BACKOFF`
  - `UNIVERSE_STOCK_CODE_REGEX`（預設 `^\d{4,6}[A-Z]?$`）
  - `UNIVERSE_LIMIT`、`UNIVERSE_LOOKBACK_DAYS`
- EventBus：`SHIJIM_BUS_MAX_QUEUE`（預設 100000，高水位警告 + 丟棄最舊）。
- ClickHouseWriter：`SHIJIM_CH_FLUSH_THRESHOLD`（預設 5000，建議 8000）、`SHIJIM_CH_FLUSH_INTERVAL_SEC`（預設 1s）、`SHIJIM_CH_ASYNC_INSERT`（建議 1）、`SHIJIM_CH_ASYNC_WAIT`（預設 0）。
- CLI 抖動：`SHIJIM_STARTUP_JITTER_SEC` 或 `--startup-jitter-seconds`。

## CI / 測試
- GitHub Actions：`.github/workflows/main.yml` 在 3.10/3.11/3.12 跑 flake8 + pytest，安裝 `-r requirements.txt` + `.[clickhouse,dev]`。
- 本地：`python -m pytest`，lint：`flake8 shijim tests`。

## Docker / 部署
```bash
docker build -t shijim:latest .
docker run --rm \
  -e SHIOAJI_API_KEY=xxx \
  -e SHIOAJI_SECRET_KEY=yyy \
  -e CLICKHOUSE_DSN="clickhouse://user:pass@clickhouse:9000/default" \
  -e SHIJIM_CH_ASYNC_INSERT=1 \
  -v /var/lib/shijim/raw:/data/raw \
  -v /var/lib/shijim/fallback:/data/fallback \
  shijim:latest \
  python -m shijim
```
`docker-compose.yml` 可啟動 ClickHouse + recorder，掛載 raw/fallback volume。雲端休眠/喚醒可參考 `.github/workflows/pause_cloud.yml` / `start_cloud.yml`。

## 其他診斷工具
- `python -m shijim.tools.debug_universe`：驗證 Universe 載入/分片。
- `python -m shijim.tools.test_scanners`：測試各 ScannerType，需設 `UNIVERSE_SCANNER_DATE` 與金鑰。
