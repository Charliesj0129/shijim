Feature: Shioaji Callback 數據攝取 (Shioaji Data Ingestion)
  為了 將 Shioaji 的即時行情餵入 Micro Alpha 系統
  作為 系統架構師
  我要求 系統能接收 Shioaji Tick/BidAsk Callback，過濾試撮數據，並透過 Rust 核心寫入 Ring Buffer。

  Background: 環境初始化
    Given Shioaji API 已連線
    And Rust RingBufferWriter 已初始化 (Slot Size = 256 Bytes)
    And 系統維護了一份 "2330" -> ID:1001 的映射表

  Scenario: 接收股票成交 Tick
    When Shioaji 傳入 QuoteSTKv1 code="2330" close=500 volume=5 simtrade=0
    Then Ingestor 應呼叫 publish_tick_v1(id=1001, price=500, size=5)
    And Ring Buffer Cursor 應前進 1

  Scenario: 過濾盤中試撮數據
    When simtrade=1
    Then Ingestor 應忽略該事件
    And Ring Buffer Cursor 不應移動

  Scenario: 接收五檔報價更新
    When Quote 含 bid/ask 列表
    Then Ingestor 呼叫 publish_quote_v1 寫入 Repeating Groups

  Scenario: 系統啟動時的初始快照
    When 收到 Snapshot close=500
    Then Ingestor 呼叫 publish_snapshot_v1

  Scenario: 偵測 Solace 斷線重連
    When event_callback code=13
    Then Ingestor 寫入 SystemEvent GapDetected
