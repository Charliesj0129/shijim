# Shijim

Shijim 是基於 Shioaji 的行情採集與資料治理工具鏈，涵蓋「即時訂閱 → 原始/分析落地 → 缺口稽核/補洞 → 動態交易宇宙 → HFT 回測資料轉換」。此 README 對部署、設定、CI、容器化等細節提供完整說明。

## 目錄
- [架構與模組](#架構與模組)
- [安裝方式](#安裝方式)
- [快速啟動](#快速啟動)
- [資料治理：稽核 & 補洞](#資料治理稽核--補洞)
- [動態交易宇宙 (Universe Navigator)](#動態交易宇宙-universe-navigator)
- [HftBacktest 轉換器](#hftbacktest-轉換器)
- [環境變數與設定](#環境變數與設定)
- [CI / 測試](#ci--測試)
- [Docker / 部署](#docker--部署)
- [性能優化與 async insert](#性能優化與-async-insert)

## 架構與模組
- `shijim/cli.py`：入口點，啟動 Shioaji session、訂閱分片宇宙、啟動 Recorder（RawWriter + ClickHouseWriter）。
- `shijim/gateway/`：Session 管理、回補、callbacks、Universe Navigator（策略選股 + bin-packing 分片）。
- `shijim/recorder/`：
  - `raw_writer.py`：原始 JSONL（append-only）。
  - `clickhouse_writer.py`：分析層寫入，支援 async_insert、批次/時間觸發、fallback。
  - `ingestion.py`：批次消費 EventBus，降低函數呼叫/IO 次數。
  - `gap_replayer.py`：序號/時間缺口補洞，支援節流與重試。
- `shijim/governance/`：DataAuditor（缺口掃描）、報告格式、GapReplay 協調器。
- `shijim/tools/`：
  - `restore_failed_batches.py`：重播 fallback JSONL → ClickHouse。
  - `hft_converter.py`：MBO/L5 → HftBacktest `.npz`，保留 exchange_ts/receive_ts 與 latency 模型。
  - `live_smoke_test.py`：基本連線/訂閱測試。
- `tests/`：Pytest 覆蓋 gateway/recorder/governance/converter，CI 每版執行。

## 安裝方式
```bash
python -m pip install --upgrade pip
pip install -e .             # 核心依賴
pip install -e ".[clickhouse,dev]"  # 若需 ClickHouse 與開發工具 (pytest, flake8)
```
- 或 `pip install -r requirements.txt` 安裝鎖定版本依賴。

## 快速啟動
1. 匯出金鑰：`SHIOAJI_API_KEY`、`SHIOAJI_SECRET_KEY`（可加 `SHIOAJI_SIMULATION=1`）。
2. （可選）設定儲存：`SHIJIM_RAW_DIR`、`SHIJIM_FALLBACK_DIR`、`CLICKHOUSE_DSN`。
3. 執行：
```bash
python -m shijim
```
Recorder 會依 shard 環境變數訂閱分片宇宙，並於收盤時間自動停止。

## 資料治理：稽核 & 補洞
### 稽核 (DataAuditor)
```bash
python -m shijim.governance.audit \
  --trading-day 2024-01-01 \
  --dsn "clickhouse://user:pass@host:9000/db" \
  --output missing_ranges.json
```
- Tick：seqno 跳點即缺口；Orderbook：快照時間窗 > 門檻且期間有 Tick 活動 → 缺口。
- 報告欄位：`gap_type` (tick/orderbook)、`symbol`、`seq_start/seq_end`、`start_ts/end_ts`、`reason`、`metadata`。  
- Summary 記錄 partial/重置/重複等統計。

### 補洞 (Gap Replayer)
- 讀取 `missing_ranges.json`，對 tick 缺口呼叫 Shioaji `ticks()`，normalize → ClickHouse，確保去重與批次寫入。
- 具 TokenBucket 節流 + 重試退避；序號缺口會插入 GAP 標記。
- 完成後建議再跑一次 Auditor 驗證 gap_count=0。

## 動態交易宇宙 (Universe Navigator)
每日 08:15 前策略化選股 + 分片：
```bash
UNIVERSE_STRATEGIES=top_volume,high_volatility,unusual_activity \
UNIVERSE_LIMIT=1200 \
UNIVERSE_LOOKBACK_DAYS=5 \
python -m shijim
```
- 策略：Top Volume（昨量/額）、High Volatility（近 N 日 ATR/stdev，需要 ClickHouse）、Unusual Activity（盤前跳動/量比，需要 ClickHouse）。
- 負載估計：結合當日 tick density + 流動/波動權重，供 bin-packing 平衡分片；分片以 `SHARD_ID`/`TOTAL_SHARDS` 控制。

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
- 產出：`*.npz` (event_flag/exchange_ts/receive_ts/latency_ns/price/qty/side/order_id/seq_id)、`*.meta.json`、`*.latency_model.json`。
- clock-skew 夾制（負延遲→最小延遲/偏移），序號跳躍插入 GAP。

## 環境變數與設定
- Shioaji：`SHIOAJI_API_KEY`、`SHIOAJI_SECRET_KEY`、`SHIOAJI_SIMULATION`。
- 儲存：`SHIJIM_RAW_DIR`(default `raw/`)、`SHIJIM_FALLBACK_DIR`、`CLICKHOUSE_DSN`。
- 分片：`SHARD_ID`、`TOTAL_SHARDS`。
- Universe：`UNIVERSE_STRATEGIES`、`UNIVERSE_LIMIT`、`UNIVERSE_LOOKBACK_DAYS`。
- EventBus：`SHIJIM_BUS_MAX_QUEUE`（預設 100000，高水位警告 + 丟棄最舊）。
- ClickHouseWriter：`SHIJIM_CH_FLUSH_THRESHOLD`（預設 5000，建議 8000）、`SHIJIM_CH_FLUSH_INTERVAL_SEC`（預設 1s）、`SHIJIM_CH_ASYNC_INSERT`（預設啟用建議 1）、`SHIJIM_CH_ASYNC_WAIT`（預設 0 不等待）。

## CI / 測試
- GitHub Actions：`.github/workflows/main.yml` 在 3.10/3.11/3.12 跑 flake8 + pytest，安裝 `-r requirements.txt` + `.[clickhouse,dev]`。
- 本地測試：`pytest`；Lint：`flake8 shijim tests`。

## Docker / 部署
- 建置：`docker build -t shijim:latest .`（使用 python:3.11-slim，預先安裝 requirements + clickhouse extra）。
- 執行：
```bash
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
- Compose：`docker-compose.yml` 提供 ClickHouse + recorder 範例；確保 raw/fallback volume 持久化。
- 雲端休眠/喚醒：`.github/workflows/pause_cloud.yml` / `start_cloud.yml`

## 性能優化與 async insert
- ClickHouseWriter：批次閾值預設 5000（建議 8000），時間觸發 flush 1s，支援 async_insert fire-and-forget；若客戶端不支援 settings 會自動降級同步執行。
- IngestionWorker：批次消費 EventBus（max_batch_events、max_batch_wait、poll_timeout），降低函式呼叫與 IO 次數。
- EventBus：預設 queue 100000，維持丟最舊保護，並在高水位/丟棄時記錄警告。

## 追蹤與回溯
- `*.meta.json` 記錄來源類型與 commit ref (`GIT_COMMIT`/`SOURCE_REF`)，方便回溯。
- Fallback JSONL 保證 at-least-once；透過 `restore_failed_batches.py` 可乾跑/套用，並支援 archive 搬移避免重播。
