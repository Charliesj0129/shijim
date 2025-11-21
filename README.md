# Shijim

Shijim 是一個以 Shioaji 為核心的行情採集與治理套件，提供「即時訂閱 → 原始/分析落地 → 缺口稽核/補洞 → 動態交易宇宙選股 → HFT 回測資料轉換」的端到端能力。專案聚焦三個目標：

1. **高品質行情採集**：Tick / L2 雙路寫入，原始檔 + ClickHouse 雙重持久化，Failover 時可落回 JSONL。  
2. **資料治理與缺口修復**：每日收盤後自動稽核序號/快照缺口，生成 `missing_ranges.json`，觸發 Gap Replayer 重新拉歷史補洞並再審核。  
3. **宇宙/回測工具鏈**：開盤前動態選股與分片，並提供 HftBacktest `.npz` 轉換器，保留 exchange_ts/receive_ts 供行情/下單延遲模型使用。

## 目錄

- [核心模組與路徑](#核心模組與路徑)
- [安裝方式](#安裝方式)
- [快速啟動](#快速啟動)
- [資料治理 (Audit & Replay)](#資料治理-audit--replay)
- [動態交易宇宙 (Universe Navigator)](#動態交易宇宙-universe-navigator)
- [HftBacktest 轉換器](#hftbacktest-轉換器)
- [環境變數與設定](#環境變數與設定)
- [測試與 CI](#測試與-ci)
- [Docker / 部署](#docker--部署)

## 核心模組與路徑

- `shijim/cli.py`：主要入口，啟動 Shioaji session、訂閱分片宇宙、啟動 Recorder。  
- `shijim/gateway/`：Session、Callback、訂閱管理、歷史回補、動態 Universe Navigator。  
- `shijim/recorder/`：RawWriter（原始 JSONL）、ClickHouseWriter（分析落地，含 fallback）、GapReplayer。  
- `shijim/governance/`：DataAuditor（缺口掃描）、報告格式、GapReplay 協調器。  
- `shijim/tools/`：
  - `restore_failed_batches.py`：ClickHouse fallback replay。
  - `hft_converter.py`：將 MBO/L5 轉為 HftBacktest `.npz`。  
  - `live_smoke_test.py`：基本連線/訂閱自測。
- `tests/`：Pytest 套件涵蓋 gateway/recorder/governance/converter。

## 安裝方式

```bash
python -m pip install --upgrade pip
pip install -e .             # 核心依賴
pip install -e ".[clickhouse,dev]"  # 如需 ClickHouse/開發工具 (pytest, flake8)
```

- 若僅需快速體驗，可直接 `pip install -r requirements.txt`。
- Dockerfile 會預設安裝 `[clickhouse]` extra，用於雲端環境。

## 快速啟動

1. 匯出必要金鑰：`SHIOAJI_API_KEY` / `SHIOAJI_SECRET_KEY`（可加 `SHIOAJI_SIMULATION=1` 走模擬）。  
2. 指定資料路徑（可選）：`SHIJIM_RAW_DIR`、`SHIJIM_FALLBACK_DIR`、`CLICKHOUSE_DSN`。  
3. 執行：

```bash
python -m shijim  # 會自動依環境變數分片訂閱、寫 Raw + ClickHouse
```

Recorder 會在收盤時間自動停止（config 內建 08:30 開盤 / 13:45 收盤）。

## 資料治理 (Audit & Replay)

### 稽核 (DataAuditor)

收盤後 14:00 建議排程：

```bash
python -m shijim.governance.audit \
  --trading-day 2024-01-01 \
  --dsn "clickhouse://user:pass@host:9000/db" \
  --output missing_ranges.json
```

- Tick 缺口：以 seqno 連續性判斷；Orderbook 缺口：以快照時間窗 + Tick 活動交叉檢測。  
- 報告欄位：`gap_type` (tick/orderbook)、`symbol`、`seq_start/seq_end`、`start_ts/end_ts`、`reason`、`metadata`。  
- Summary 會記錄忽略/夾制的負延遲次數、部分失敗等。

### 補洞 (Gap Replayer)

將 `missing_ranges.json` 餵給 GapReplayOrchestrator：

- 依 gap_type= tick 實際呼叫 Shioaji 歷史 `ticks()`，normalize 為 MDTickEvent，去重、批次寫入 ClickHouse。  
- 具備 TokenBucket 節流、重試退避；序號缺口會先行插入 GAP 標記。  
- 完成後可再跑一次 Auditor 確認 gap_count=0。

## 動態交易宇宙 (Universe Navigator)

每日 08:15 前執行策略化選股與分片：

```bash
UNIVERSE_STRATEGIES=top_volume,high_volatility,unusual_activity \
UNIVERSE_LIMIT=1200 \
UNIVERSE_LOOKBACK_DAYS=5 \
python -m shijim  # 內部自動呼叫 UniverseNavigator
```

- 內建策略：
  - Top Volume：昨日成交額/量排名。
  - High Volatility：近 N 日 ATR/stdev（需 ClickHouse）。
  - Unusual Activity：盤前跳動/量比異常（需 ClickHouse）。
- 負載估計：以當日 tick density + 流動性/波動度加權產出 weight，進行 bin-packing 平衡分片。  
- 分片環境變數：`SHARD_ID` / `TOTAL_SHARDS` 決定各 worker 訂閱集合。

## HftBacktest 轉換器

將 MBO/L5 轉為帶 feed latency 的 `.npz`：

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

輸出：
- `hft_<symbol>_<date>_<source>.npz`：包含 `event_flag/exchange_ts/receive_ts/latency_ns/price/qty/side/order_id/seq_id`。  
- `*.meta.json`：來源、統計（缺口數、負延遲校正常態）。  
- `*.latency_model.json`：供回測器套用的下單延遲模型設定。  
- 內建 clock-skew 夾制（負延遲則用最小延遲或偏移補償）。序號跳躍會插入 GAP 標記供後端重建。

## 環境變數與設定

- Shioaji：`SHIOAJI_API_KEY`、`SHIOAJI_SECRET_KEY`、`SHIOAJI_SIMULATION`。  
- 儲存：`SHIJIM_RAW_DIR`（預設 `raw/`）、`SHIJIM_FALLBACK_DIR`、`CLICKHOUSE_DSN`。  
- 分片：`SHARD_ID`、`TOTAL_SHARDS`。  
- Universe：`UNIVERSE_STRATEGIES`、`UNIVERSE_LIMIT`、`UNIVERSE_LOOKBACK_DAYS`。  
- 轉換器：`GIT_COMMIT` 或 `SOURCE_REF`（寫入 meta 以利追溯）。

## 測試與 CI

- 本地執行：`python -m pytest`。  
- Lint：`flake8 shijim tests`。  
- GitHub Actions（`.github/workflows/main.yml`）：在 3.10/3.11/3.12 跑 flake8 + pytest，依 `requirements.txt` 與 `.[clickhouse,dev]` 安裝。

## Docker / 部署

建置映像：

```bash
docker build -t shijim:latest .
```

本地執行（持久化 raw/fallback）：

```bash
docker run --rm \
  -e SHIOAJI_API_KEY=xxx \
  -e SHIOAJI_SECRET_KEY=yyy \
  -e CLICKHOUSE_DSN="clickhouse://user:pass@clickhouse:9000/default" \
  -v /var/lib/shijim/raw:/data/raw \
  -v /var/lib/shijim/fallback:/data/fallback \
  shijim:latest \
  python -m shijim
```

其他說明：
- `docker-compose.yml` 提供 ClickHouse + recorder 範例；記得掛載 raw/fallback volume。  
- 若要重播 fallback：在同樣的 volume 上執行 `python -m shijim.tools.restore_failed_batches --mode apply ...`。  
- 雲端休眠/喚醒：`pause_cloud.yml` / `start_cloud.yml` GitHub Actions 範例，可控制 Container Apps 縮放。
