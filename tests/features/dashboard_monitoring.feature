Feature: 實時終端儀表板 (Real-time TUI Dashboard)
  為了 在盤中即時監控策略狀態與系統健康度
  作為 交易員 (Trader)
  我要求 系統提供一個基於終端的圖形介面，能以 10Hz 以上的頻率刷新關鍵指標，並支援手動介入。

  Background: 監控環境
    Given 策略 Runner 正在背景運行
    And 儀表板已連接至共享數據

  Scenario: 顯示即時市場與信號
    When 市場數據更新 Price=100.0 OFI=+5
    Then 儀表板應顯示 Price 100.0
    And Signal 區塊顯示 +5
    And Ingestion Lag 低於 1

  Scenario: 策略進入追價狀態
    When 策略狀態轉為 CHASING 並有訂單
    Then 儀表板 State 欄位顯示 CHASING
    And Active Orders 列表包含該訂單

  Scenario: 顯示風控拒絕警報
    When 風控拒絕 MaxOrderQty
    Then Log 區塊顯示 RISK REJECT
    And Reject Count 增加

  Scenario: 通過儀表板觸發 Kill Switch
    When 使用者按下 K 鍵
    Then 儀表板觸發 Kill Switch 信號
    And System Status 顯示 STOPPED
