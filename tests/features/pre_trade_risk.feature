Feature: 交易前風險控制 (Pre-Trade Risk Management)
  為了 防止程式邏輯錯誤或操作失誤導致鉅額損失
  作為 風控合規官 (Compliance Officer)
  我要求 系統在發送任何訂單到券商前，必須通過一系列嚴格的風險檢查。

  # ---------------------------------------------------------------------------
  # 背景：風控參數設定
  # ---------------------------------------------------------------------------
  Background: 風控層初始化
    Given 風控配置如下:
      | Rule           | Value |
      | MaxOrderQty    | 5     |
      | MaxPosition    | 10    |
      | PriceDeviation | 0.05  |
      | MaxOrdersPerSec| 5     |
    And 當前市場價格為 100.0
    And 當前持有部位為 2

  Scenario: 攔截價格異常的訂單
    When 策略發出買單 Price=110.0
    Then 風控層應拒絕訂單 Reason="PriceDeviation"

  Scenario: 攔截數量過大的訂單
    When 策略發出買單 Qty=10
    Then 風控層應拒絕訂單 Reason="MaxOrderQty"

  Scenario: 攔截導致超額部位的訂單
    Given 當前部位為 8
    When 策略發出買單 Qty=3
    Then 風控層應拒絕訂單 Reason="PositionLimit"

  Scenario: 攔截過於頻繁的下單請求
    Given 過去 1 秒已有 5 筆訂單
    When 策略嘗試發送第 6 筆訂單
    Then 風控層應拒絕訂單 Reason="RateLimit"

  Scenario: 觸發熔斷機制
    Given 系統 Kill Switch 已觸發
    When 發送新單
    Then 風控層應拒絕新單
    But 當策略發送撤單
    Then 風控層應允許撤單
