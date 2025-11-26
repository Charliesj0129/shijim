# BDD SPEC：微市場 Micro-Alpha 特徵工程演算法深度綜覽

微市場 Alpha 代表在微秒級限價訂單簿上捕捉供需失衡、流動性耗竭與資訊不對稱。本規格說明高頻策略在 BDD 測試中所需的理論背景、特徵家族與模型脈絡，確保測試案例與實際演算法需求對齊。

---

## 1. 緒論：高頻交易中的訊號本質與特徵工程演變
- Alpha 來源已從低頻財報轉向市場微結構動態；訊號核心在於 LOB 供需變化與交易毒性。
- 特徵工程需處理龐大 L2/L3 數據並維持毫秒內完成。
- 本綱要將特徵分為四大核心家族：訂單流失衡模型、知情交易指標、隨機強度模型、深度表徵架構。

### 1.1 市場微結構數據層級
1. **Level 1 (BBO)**：最佳買賣價及對應量，提供 Spread 與 Mid-Price。
2. **Level 2 (Market Depth)**：前 N 檔（5/10/20）深度，是多數特徵的主要資料來源。
3. **Level 3 (Order-by-Order)**：逐筆新增/取消/成交事件，可追蹤 Queue Position。

Tick 為事件驅動，需使用 Volume Synchronization 或 Dollar Bars 重新取樣以降低微結構噪音。

---

## 2. 訂單流失衡（OFI）與壓力指標體系
OFI 聚焦於訂單簿狀態變化率，推斷參與者意圖。

### 2.1 OFI 理論框架
- 根據 Cont 等人的模型，價格變由買賣壓不平衡驅動。
- 事件貢獻 \(e_n\) 取決於最佳買賣價相對前狀態的變化，精準分辨加價/減量行為。
- 累積 OFI 與短期價格變動呈線性關聯： \(\Delta P_t = \lambda \cdot OFI_t + \epsilon_t\)。

### 2.2 多層次 OFI 與衰減機制
- 將 OFI 擴展至前 5~10 檔，採遞減權重 \(w_i\) 計算 \(OFI_{deep}\)。
- 透過標準化 (NOFI) 消除成交活躍度波動。

### 2.3 交易流失衡（TFI）與 OFI 區辨
- OFI 反映限價單意圖；TFI 反映市價單行動。
- TFI 聚焦主動買賣成交差；在加密等碎片化市場常具領先性。
- 在特徵向量納入 OFI 與 TFI 並觀察背離可增強策略表現。

### 2.4 訂單簿斜率與壓力
- 斜率量化流動性彈性，靜態形狀影響支撐阻力。
- 壓力指標 \(Pressure = (Slope_{bid} - Slope_{ask}) / (Slope_{bid} + Slope_{ask})\) 用於過濾策略訊號。

---

## 3. 知情交易與流動性毒性特徵
### 3.1 VPIN（Volume-Synchronized Probability of Informed Trading）
- 使用體積桶平衡日內季節性，透過 Bulk Volume Classification 分拆買賣量。
- VPIN 為過去 n 個桶的平均訂單失衡率，可預警閃崩或波動飆升。

### 3.2 Kyle's Lambda
- 透過 \(\Delta P_t = \alpha + \lambda \cdot SignedVolume_t + \epsilon_t\) 估計流動性成本。
- 短期 Lambda 反映瞬間彈性；長期 Lambda 反映結構性流動性。
- Lambda Skew 暗示供需非對稱性。

---

## 4. 隨機強度特徵：Hawkes 過程
### 4.1 強度函數
- 單變量 Hawkes 條件強度：\(\lambda(t) = \mu + \sum_{t_i < t} \phi(t - t_i)\)。
- 多變量 Hawkes 捕捉買賣互激勵，提供更豐富的因果資訊。

### 4.2 核函數選擇
- **指數核**：遞歸更新、適合即時系統，但長記憶有限。
- **冪律核**：描述長記憶與重尾，但計算 \(O(N^2)\)。
- **Sum-of-Exp**：折衷方案。

衍生特徵包括分支比率、內生性比例與基於強度預測的 OFI 指標。

---

## 5. 深度學習在 LOB 表徵中的應用
### 5.1 DeepLOB
- 輸入為 \(T \times L \times C\) 張量（例如 100 × 10 × 4）。
- 層次化卷積融合價格/數量特徵，並用 LSTM 捕捉時間依賴。
- LOBster 透過對稱掩碼 Dropout 維持買賣對稱性。

### 5.2 TransLOB 與軸向注意力
- 引入 Axial Attention 分別沿特徵軸與時間軸計算注意力，降低 Transformer \(O(T^2)\) 成本。
- 亦可結合 VAE 做無監督預訓練。

---

## 6. 公式化因子與遺傳演算法適配
### 6.1 從日線到 Tick 的轉換
- 使用滾動視窗將日線統計轉為高頻版本。
- 透過 1D 卷積加速 rolling 運算，在 GPU 上並行多因子。

### 6.2 遺傳規劃
- 以運算符與原始數據為基因，演化產生新 Alpha 表達式。
- 以 IC 或 Sharpe 作為適應度，並加入正交化/剪枝避免共線。

---

## 7. 實作基礎：數據同步與微結構降噪
### 7.1 預平均
- 觀測價格 = 有效價格 + 微結構噪音；需在計算前做預平均以免波動率偏差。

### 7.2 體積同步
- 改用體積條確保每個樣本信息量相近，改善常態性並提高統計模型表現。

---

## 8. 結論與展望
- 建立四層特徵層級（物理、經濟、隨機、表徵）。
- 未來趨勢為多層融合，例如用 Hawkes 強度調整 DeepLOB 視窗，或以 VPIN 作為 gating 機制。
- FPGA/硬體推論與張量化 Alpha 計算是下一代競爭焦點。

---

## 附錄
### 表 1：核心失衡特徵比較
| 特徵 | 核心輸入 | 焦點 | 視野 | 洞察 |
| --- | --- | --- | --- | --- |
| 簡單成交量失衡 | L1 Bid/Ask | 靜態比率 | Ticks | 易受 Spoofing |
| OFI | L1/L2 Updates | 動態變化率 | 秒級 | 預測短期趨勢 |
| TFI | Executed Trades | 主動成交差 | 即時 | 均值回歸/衝擊判斷 |
| VPIN | Volume Buckets | 交易毒性 | 分鐘 | 逆選擇/波動預警 |

### 表 2：LOB 深度學習架構比較
| 架構 | 輸入張量 | 特徵組成 | 核心機制 | 優勢 |
| --- | --- | --- | --- | --- |
| DeepLOB | (100, 40) | 10 Levels × (Bid P/V, Ask P/V) | CNN + LSTM | 捕捉局部微結構 |
| TransLOB | (100, 40) | 同上 | 軸向注意力 | 捕捉長序列依賴 |
| LOBster | (100, 20) | 5 Levels | CNN + GRU + 對稱 Dropout | 計算效率佳 |

### 表 3：Hawkes 核函數指南
| 核函數 | 公式 | 衰減 | 成本 | 場景 |
| --- | --- | --- | --- | --- |
| 指數核 | \(\alpha e^{-\beta t}\) | 快速 | 低 | 實時強度估計 |
| 冪律核 | \(\alpha (c+t)^{-(1+\gamma)}\) | 慢/長記憶 | 高 | 離線分析 |
| Sum-of-Exp | \(\sum \alpha_k e^{-\beta_k t}\) | 可近似冪律 | 中 | 記憶/效率折衷 |

---

## 建議公式（Equation Suggestions）
為了讓 BDD 測試可引用具體數學定義，以下整理文章中提及的建議公式，並置於本文末尾：

1. **OFI 事件貢獻**  
   \[
   e_n = I_{(b_n \ge b_{n-1})} q_n^b - I_{(b_n \le b_{n-1})} q_{n-1}^b - I_{(a_n \le a_{n-1})} q_n^a + I_{(a_n \ge a_{n-1})} q_{n-1}^a
   \]
   > 用於區分加價、撤單、被動掛單變化，是 OFI BDD 場景的主軸。

2. **VPIN（Volume-Synchronized Probability of Informed Trading）**  
   \[
   VPIN = \frac{\sum_{\tau=1}^n |V_{\tau}^B - V_{\tau}^S|}{n \cdot V}
   \]
   > 為造市與風控 BDD 場景提供交易毒性量化標準。

3. **Hawkes 強度函數**  
   \[
   \lambda(t) = \mu + \sum_{t_i < t} \phi(t - t_i)
   \]
   > 建議在事件驅動 BDD 情節中檢查自激勵與互激勵的響應。

4. **Rolling/Alpha 卷積化**  
   \[
   Rolling(X, w) \rightarrow Conv1D(X, kernel = 1_w, padding = w-1)
   \]
   > 供 GPU/FPGA 平台實作用例使用，確保 BDD 規格明確涵蓋張量化實務。

以上建議區塊緊貼主文之後，方便策略、測試與模型團隊快速串聯文章內容與具體數學檢查條件。

---

## 實作註記
- 指標可透過 `shijim_indicators` (PyO3) 取得 Rust 版本。BDD 測試應同時對 Python 與 Rust 實作執行（例如 `pytest -k rust_ofi`）以確認行為一致。
- Rust 介面需支援零拷貝 numpy view (`PyReadonlyArray`) 和 stateful reset；測試資料應引用上述建議公式確保數值正確。
