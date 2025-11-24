Feature: HftBacktest 整合回測 (HftBacktest Integration)
  為了 利用業界標準框架驗證 Smart Chasing 策略的獲利能力
  作為 量化研究員
  我要求 系統能將 SBE 數據轉換為 HftBacktest 格式，並在框架內正確執行策略邏輯與損益計算。

  Background: HftBacktest 環境初始化
    Given 原始數據文件 "2023-10-27_TXFL5.sbe" 已存在
    And HftBacktest 框架已安裝
    And 策略參數 ChaseThreshold=2.0 OFI_Threshold=5

  Scenario: 將 SBE 數據轉換為 HftBacktest 格式
    When 執行數據轉換工具
    Then 應產出 "2023-10-27_TXFL5.npz"
    And 轉換後的數據包含正確 Event Flag
    And 價格欄位被正規化
    And 時間戳為微秒或奈秒

  Scenario: 在回測迴圈中執行智能追價
    Given HftBacktest 載入轉換後的數據
    And 延遲模型為 ConstantLatency(5ms)
    When 執行回測
    Then SmartChasingEngine.on_tick 應被呼叫
    And CancelReplace 指令轉為 submit/cancel

  Scenario: 驗證回測中的 OFI 計算
    When 回測時間為 T 發生 Bid Size Increase
    Then 引擎 OFI 值應為正
    And 觸發 Alpha Driven Chasing

  Scenario: 生成回測統計報告
    When 回測完成
    Then 輸出總損益與 Sharpe Ratio
    And 產生 PnL vs Time 數據
