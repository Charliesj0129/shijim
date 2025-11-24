Feature: 即時訂單流不平衡 (OFI) 計算
  為了 預測下一秒的價格變動方向
  作為 量化策略開發者 (Quant Developer)
  我要求 系統能根據連續兩筆 SBE 市場數據，即時計算出 OFI 值 (Order Flow Imbalance)。

  # ---------------------------------------------------------------------------
  # 背景：策略引擎狀態初始化
  # ---------------------------------------------------------------------------
  Background: 策略狀態準備
    Given 策略引擎已初始化 OFI 計算器
    And 上一時刻 (t-1) 的最佳買價 (Best Bid) 為: Price=100.0, Size=10
    And 上一時刻 (t-1) 的最佳賣價 (Best Ask) 為: Price=101.0, Size=10

  # ---------------------------------------------------------------------------
  # 場景 1: 靜態掛單增加 (Passive Liquidity Addition)
  # 驗證重點：價格不變，量增加 -> 正向買壓
  # ---------------------------------------------------------------------------
  Scenario: 買方在同價位增加掛單
    When 收到新時刻 (t) 的數據:
      | Field     | Value |
      | Bid Price | 100.0 |
      | Bid Size  | 15    |
      | Ask Price | 101.0 |
      | Ask Size  | 10    |
    Then 計算出的 Bid Imbalance (Delta W) 應為 +5 (15 - 10)
    And 計算出的 Ask Imbalance (Delta V) 應為 0
    And 最終 Net OFI 應為 +5

  # ---------------------------------------------------------------------------
  # 場景 2: 買方主動提價 (Aggressive Buying)
  # 驗證重點：價格上漲 -> 整個新量視為強烈買壓
  # ---------------------------------------------------------------------------
  Scenario: 買方向上報價 (Price Improvement)
    When 收到新時刻 (t) 的數據:
      | Field     | Value |
      | Bid Price | 100.5 |
      | Bid Size  | 5     |
      | Ask Price | 101.0 |
      | Ask Size  | 10    |
    Then 計算出的 Bid Imbalance (Delta W) 應為 +5 (Current Size)
    And 最終 Net OFI 應為 +5
    And 策略內部的 "上一時刻 Bid" 應更新為 Price=100.5, Size=5

  # ---------------------------------------------------------------------------
  # 場景 3: 賣方撤單或被吃單 (Cancellation / Consumption)
  # 驗證重點：賣方量減少 -> 賣壓減弱 -> 對 OFI 是正向貢獻
  # OFI = Bid流(0) - Ask流(-8) = +8 (看漲信號)
  # ---------------------------------------------------------------------------
  Scenario: 賣方在同價位減少掛單
    When 收到新時刻 (t) 的數據:
      | Field     | Value |
      | Bid Price | 100.0 |
      | Bid Size  | 10    |
      | Ask Price | 101.0 |
      | Ask Size  | 2     |
    Then 計算出的 Ask Imbalance (Delta V) 應為 -8 (2 - 10)
    And 最終 Net OFI 應為 +8

  # ---------------------------------------------------------------------------
  # 場景 4: 買方支撐崩潰 (Support Broken)
  # 驗證重點：買價下跌 -> 原本的支撐量視為負向買壓
  # ---------------------------------------------------------------------------
  Scenario: 最佳買價下跌
    When 收到新時刻 (t) 的數據:
      | Field     | Value |
      | Bid Price | 99.5  |
      | Bid Size  | 20    |
      | Ask Price | 101.0 |
      | Ask Size  | 10    |
    Then 計算出的 Bid Imbalance (Delta W) 應為 -10 (上一時刻的 Size * -1)
    And 最終 Net OFI 應為 -10

  # ---------------------------------------------------------------------------
  # 場景 5: 與 Ring Buffer 的整合 (Integration)
  # 驗證重點：從 Raw Bytes 到 Signal 的完整計算鏈路
  # ---------------------------------------------------------------------------
  Scenario: 從 Ring Buffer 讀取並更新 OFI
    Given Ring Buffer 中有一筆新的 SBE 數據 (Seq=100)
    When 策略呼叫 `process_next_tick()`
    Then 系統應解碼 SBE 並提取 BBO
    And 系統應計算並發布 OFI 信號
    And 系統應將當前 BBO 暫存為 "Previous State" 以供 Seq=101 使用
