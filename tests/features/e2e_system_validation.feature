Feature: 全鏈路系統驗收 (End-to-End System Validation)
  為了 確保 Micro Alpha 交易系統具備上線實戰的能力
  作為 系統架構師
  我要求 驗證從 UDP 封包發送到 Python 策略接收的完整路徑，確保數據無損且過濾機制有效。

  # ---------------------------------------------------------------------------
  # 背景：搭建完整的測試環境 (Mini Production Environment)
  # ---------------------------------------------------------------------------
  Background: 系統啟動
    Given 系統環境為 Linux (或支援 POSIX SHM 的環境)
    And Rust Ingestor 已啟動並綁定至 "239.0.0.1:5000" (背景執行緒)
    And Python Reader 已成功 Attach 到共享記憶體
    And 測試用的 UDP 發送器 (MulticastSender) 已準備就緒

  # ---------------------------------------------------------------------------
  # 場景 1: 快樂路徑 - 數據透傳驗證 (The Happy Path)
  # 驗證重點：Raw Bytes 能否完整穿過 Rust Core 到達 Python 並被正確解碼。
  # ---------------------------------------------------------------------------
  Scenario: UDP SBE 封包的完整生命週期
    Given UDP 發送器準備了一個 SBE 封包:
      | Field        | Value   | Note |
      | TemplateID   | 2       | MarketData |
      | Price        | 2330.5  | Decimal64 |
      | TransactTime | 123456  | u64 |
    When 發送器發出該多播封包
    And 等待 10ms 讓數據流經系統 (Latency Buffer)
    Then Python Reader 的 `latest_bytes()` 應收到新的 Payload
    And 使用 SBEDecoder 解析該 Payload：
      | Field        | Expected Value |
      | TemplateID   | 2              |
      | Price        | 2330.5         |
    And 驗證 Ring Buffer Cursor 前進了 1

  # ---------------------------------------------------------------------------
  # 場景 2: 核心層過濾驗證 (Kernel Filtering)
  # 驗證重點：Rust Core 承諾的 "Filtering" 是否生效。
  # ---------------------------------------------------------------------------
  Scenario: 攔截 Heartbeat 封包
    Given 當前 Ring Buffer Cursor 為 N
    When UDP 發送器發出一個 TemplateID 為 0 (Heartbeat) 的封包
    And 等待 10ms
    Then Python Reader 的 Cursor 應仍維持為 N (未移動)
    And 共享記憶體內容 **不應** 被修改
    # 這證明了無效數據完全沒有進入 Shared Memory，節省了頻寬

  # ---------------------------------------------------------------------------
  # 場景 3: 壓力與連續性測試 (Stress & Continuity)
  # 驗證重點：在連續封包轟炸下，數據順序不亂，且無丟包 (在低負載下)。
  # ---------------------------------------------------------------------------
  Scenario: 連續數據流處理
    Given UDP 發送器準備發送 100 個連續的 SBE 封包
    And 每個封包的 Price 依序為 100.0, 101.0, ..., 199.0
    When 發送器開始高速發送 (Burst Mode)
    And Python Reader 在迴圈中持續讀取 (Busy Poll)
    Then Python 端應能依序讀取到所有 100 個價格
    And 最終 Ring Buffer Cursor 應增加 100
    And 不應發生 `SeqNum` 跳號或數據損壞

  # ---------------------------------------------------------------------------
  # 場景 4: 異常封包處理 (Resilience)
  # 驗證重點：Rust Ingestor 的強健性 (Truncation/Drop)。
  # ---------------------------------------------------------------------------
  Scenario: 超大封包 (Jumbo Frame) 處理
    Given Ring Buffer Slot Size 設定為 256 Bytes
    When UDP 發送器發出一個 300 Bytes 的巨型封包
    And 等待 10ms
    Then 系統不應崩潰 (No Panic)
    # 行為取決於您的實作：是截斷 (Truncate) 還是丟棄 (Drop)？
    # 假設我們在 Phase 4 選擇了截斷：
    And Python Reader 讀到的 Payload 長度應被限制為 248 (256 - Header)
    # 或者如果選擇丟棄：
    # And Ring Buffer Cursor 不應移動

  # ---------------------------------------------------------------------------
  # 場景 6: 單播回環模式 (Unicast Loopback Mode)
  # 驗證重點：開發與 CI 可在不具備 Multicast 權限時仍完成管線驗證。
  # ---------------------------------------------------------------------------
  Scenario: 測試環境下的單播綁定
    Given 系統配置為 "TESTING" 模式
    And 綁定地址設為 "127.0.0.1:5000" (非多播地址)
    When Rust 網關啟動
    Then 網關應偵測到 Unicast 地址
    And 網關不應嘗試加入多播組
    And Socket 應成功綁定至 127.0.0.1:5000
    And 數據攝取循環應正常運作
