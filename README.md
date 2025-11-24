# Micro Alpha / Shijim Stack

Shijim 是一套結合 **Rust 核心** 與 **Python 策略層** 的高頻交易骨架。  
它負責連線 Shioaji、寫入零拷貝 Ring Buffer、計算微結構信號、執行風控、並將資料投餵到回測與儀表板。

> “Rust for Math, Python for Logic” – 所有計算密集處理（SBE、MLOFI、Ring Buffer）位於 `shijim_core`，策略決策則維持 Python 的靈活度。

---

## 功能亮點

| 子系統 | 內容 |
| --- | --- |
| Gateway / Ingestion | Shioaji callback → Rust Ring Buffer (Tick / L2 / Snapshots / System Events) |
| Risk | Fat finger、position limit、rate limit、kill switch |
| Strategy Engine | Smart chasing、OFI / MLOFI、多層信號融合 |
| Algo Toolkit | `shijim.algo` 提供 L5 features、PIQ estimator、microstructure signals |
| Dashboard | Textual-based TUI 10Hz 指標刷新、手動觸發 kill switch |
| Backtest | `hftbacktest` 轉換與策略介接、Latency/Queue 模擬 |
| DevOps | pytest/ruff、Dockerized runner、GitHub Actions (lint→test→package→docker→deploy) |

---

## 架構

```
Shioaji Callbacks
    │
    ▼
shijim.gateway.ingestion  ──>  shijim_core (Rust SBE Encoder)
    │                               │
    │                        RingBuffer reader
    ▼                               ▼
SmartChasingEngine  ──>  RiskAwareGateway  ──>  Adapter (Shioaji/Backtest)
    │                               │
    ├── Algo: MLOFI / L5 features / PIQ / Kalman / Lead-Lag
    ├── Dashboard snapshot queue
    └── HftBacktest Adapter / Tools
```

---

## 環境需求

- **Python** `3.12` 或 `3.13`
- **Rust toolchain**（用於建置 `shijim_core`）
- Ubuntu 22.04 / Debian 12 / macOS 14 以上
- Shioaji API key/secret（或設定 `SHIOAJI_SIMULATION=1`）

---

## 安裝

```bash
python -m pip install --upgrade pip
pip install -e .                # 核心模組
pip install -e ".[clickhouse]"  # ClickHouse writer
pip install -e ".[dashboard]"   # Textual TUI
pip install -e ".[backtest]"    # HftBacktest 整合
pip install -e ".[dev]"         # 測試 / lint / maturin
```

或使用 `requirements.txt`（包含所有常用相依）：

```bash
pip install -r requirements.txt
```

編譯 Rust 擴充：`source .venv/bin/activate && maturin develop`

---

## 快速啟動

```bash
export SHIOAJI_API_KEY=xxx
export SHIOAJI_SECRET_KEY=yyy
python -m shijim --simulation            # 進入模擬環境
```

常用參數：
- `--startup-jitter-seconds`：多實例啟動抖動。
- `SHIJIM_RAW_DIR` / `SHIJIM_FALLBACK_DIR`：資料落地路徑。
- `CLICKHOUSE_DSN`：啟用 ClickHouse writer。
- `SHARD_ID` / `TOTAL_SHARDS`：Universe 分片。

---

## Dashboard

```bash
python -m shijim.dashboard.app
```

* 10Hz 刷新 Ring Buffer Lag、OFI、策略狀態、Active Orders。
* 紅色 log 代表 Risk Reject，`K` 鍵觸發 kill switch。
* `SystemSnapshot` 由 `StrategyRunner` 注入（可自訂 snapshot callback）。

---

## 嘗試 Backtest

1. 轉換資料：
   ```bash
   python -m shijim.tools.hft_converter raw/2023-10-27_TXFL5.jsonl out/2023-10-27_TXFL5.npz
   ```
2. 在 `hftbacktest` 內載入 `.npz`，使用 `shijim.backtest.adapter.HftBacktestAdapter` 封裝 `SmartChasingEngine`。
3. 針對每個事件呼叫 `adapter.run(feed)` 即可復用策略決策。

---

## 測試與靜態分析

```bash
pip install -e ".[dev,clickhouse,backtest,dashboard]"
ruff check shijim tests
pytest --maxfail=1 --disable-warnings
```

---

## Docker

```bash
docker build -t ghcr.io/your-org/shijim:latest .
docker run --rm \
  -e SHIOAJI_API_KEY=xxx \
  -e SHIOAJI_SECRET_KEY=yyy \
  ghcr.io/your-org/shijim:latest \
  python -m shijim --simulation
```

`docker-compose.yml` 已示範 Recorder + ClickHouse 的搭配部署。

---

## CI / CD

GitHub Actions (`.github/workflows/main.yml`) 流程：

1. **lint-and-test**：在 Py 3.12、3.13 執行 `ruff`、`pytest`。
2. **build**：產生 `wheel` + `sdist`，上傳成 artifacts。
3. **docker**：建置 container 映像、以 `docker save` 上傳供下載。
4. **release**：當標記 tag 時，自動發佈 GitHub Release，附上封裝與 Docker 映像。

---

## 目錄速覽

```
shijim/
  algo/                # features, microstructure, execution, learning, correlation
  bus/, events/, gateway/, recorder/, governance/
  tools/               # hft_converter, universe debugger, smoke tests
shijim_core/           # Rust Ring Buffer + SBE encoder
tests/                 # pytest BDD 風格覆蓋
```

---

## 貢獻指南

1. Fork + 建立 feature branch。
2. 執行 `ruff` / `pytest`。
3. 開 PR 時描述情境與測試結果；CI 會在 3.12/3.13 自動驗證並建置 artifacts。

歡迎提交新 signal / execution 模組，讓 Micro Alpha 更強大！  
如需支援請至 [Issues](https://github.com/your-org/shijim/issues) 回報。
