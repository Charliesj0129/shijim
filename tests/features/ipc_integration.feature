Feature: Python 端零拷貝 IPC 整合
  為了 讓 shijim 能作為 Micro Alpha 架構中的策略層 (Strategy Layer)
  作為 系統架構師
  我希望 shijim 能透過共享記憶體直接讀取市場數據，而無需經過序列化/反序列化 (Pickle/JSON)

  Background:
    Given 系統環境已安裝 "iceoryx2" 或 "shared_memory" 相關綁定
    And 定義了一個共享記憶體段 "MarketData_L2_Ring"
    And 該記憶體段的結構定義為 "Timestamp(u64) + BestBidPrice(f64) + BestBidQty(f64)"

  Scenario: 讀取共享記憶體並映射為 Numpy Array
    Given 模擬生產者已在共享記憶體寫入一筆數據:
      | Field        | Value      |
      | Timestamp    | 1700000000 |
      | BestBidPrice | 2330.0     |
      | BestBidQty   | 105.5      |
    When shijim 的 IPC 訂閱者連接到 "MarketData_L2_Ring"
    And 調用 receive() 獲取數據指標
    Then 應能將該指標轉換為 Numpy Array (dtype=float64)
    And 讀取到的 BestBidPrice 應為 2330.0
    And 驗證數據記憶體地址位於共享記憶體區段內 (非 Python Heap)
    And 讀取過程不應觸發 Python 的 GC (Garbage Collection)

  Scenario: 處理環形緩衝區的數據覆蓋 (Wrap-Around)
    Given 模擬生產者快速寫入導致 Ring Buffer 繞行 (Wrap Around)
    And 生產者更新了 "LatestSequence" 為 100
    When shijim 訂閱者嘗試讀取舊的 Sequence (例如 50)
    Then IPC 介面應拋出 "DataLost" 或 "StaleReference" 警告
    And 訂閱者應自動跳轉至最新的 Sequence 100 進行讀取
