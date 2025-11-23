Feature: 零拷貝環形緩衝區讀取器 (Zero-Copy Ring Buffer Reader)
  為了 讓 shijim 能以亞微秒級延遲獲取 Rust 核心產生的市場數據
  作為 系統架構師 (System Architect)
  我要求 IPC 模組必須能直接映射共享記憶體，並具備「序列號完整性檢查」與「覆蓋保護」機制。

  # ---------------------------------------------------------------------------
  # 背景設定：定義物理內存的初始狀態
  # ---------------------------------------------------------------------------
  Background: 共享記憶體環境初始化
    Given 系統存在一個名為 "shijim_market_data_l2" 的共享記憶體區段
    And 該區段的大小配置為:
      | Parameter      | Value | Unit  |
      | Header Size    | 128   | Bytes |
      | Slot Count     | 1024  | Slots |
      | Slot Size      | 48    | Bytes |
    And 記憶體佈局符合 "MicroAlpha_L2_Schema_v1" 規範
    And 生產者 (Producer) 的 Cache Line Padding 已正確設置 (64 bytes alignment)

  # ---------------------------------------------------------------------------
  # 場景 1：連接與結構映射 (Happy Path)
  # 驗證重點：Numpy 是否正確「騎」在共享記憶體上，而非複製數據。
  # ---------------------------------------------------------------------------
  Scenario: 成功掛載並建立 Numpy 視圖 (View)
    Given 生產者已初始化記憶體，將 `write_cursor` 設為 0
    And 生產者在 Slot[0] 寫入以下測試數據:
      | Field          | Value          | Type |
      | seq_num        | 100            | u64  |
      | best_bid_price | 2330.0         | f64  |
    When shijim IPC Reader 呼叫 `attach("shijim_market_data_l2")`
    Then 系統應返回一個有效的 Numpy Structured Array 物件
    And 該 Array 的 `base` 屬性應指向 Python 的 `mmap` 或 `buffer` 物件 (非 None)
    And 讀取 Array[0]['best_bid_price'] 應等於 2330.0
    And 修改 Numpy Array 中的值應直接改變共享記憶體內容 (驗證指針正確性)

  # ---------------------------------------------------------------------------
  # 場景 2：追趕讀取與序列號校驗 (The Catch-up Logic)
  # 驗證重點：消費者如何根據 Cursor 找到正確的讀取位置。
  # ---------------------------------------------------------------------------
  Scenario: 根據 Write Cursor 讀取最新數據
    Given 生產者已寫入 500 筆數據，當前 `write_cursor` 為 500
    And Slot[499] (索引 499) 的 `seq_num` 為 500
    When IPC Reader 請求讀取 `latest()`
    Then Reader 應定位到索引 499 (500 - 1)
    And 讀取到的 `header.seq_num` 必須嚴格等於 500
    And 若讀取到的 `seq_num` 不等於 500，Reader 應拋出 `DataIntegrityError`

  # ---------------------------------------------------------------------------
  # 場景 3：環形緩衝區繞行處理 (Wrap-Around Handling)
  # 驗證重點：當寫入量超過容量 (1024) 時，索引計算是否正確。
  # ---------------------------------------------------------------------------
  Scenario: 跨越緩衝區邊界的索引計算
    Given 緩衝區容量為 1024
    And 生產者持續寫入，當前 `write_cursor` 到達 1025 (已繞行一圈)
    # Index = (1025 - 1) % 1024 = 0
    And 生產者在 Slot[0] 覆蓋寫入了 `seq_num=1025` 的新數據
    When IPC Reader 請求讀取 `cursor=1025` 的數據
    Then Reader 應計算物理索引為 0
    And 讀取到的 `header.seq_num` 應為 1025
    And 讀取到的數據不應是舊的 `seq_num=1` 的數據

  # ---------------------------------------------------------------------------
  # 場景 4：慢速消費者檢測 (Slow Consumer / Overrun)
  # 驗證重點：如果 Python 處理太慢，數據被 Rust 覆蓋了，必須報錯，不能讀髒數據。
  # ---------------------------------------------------------------------------
  Scenario: 偵測數據覆蓋 (Overrun Detection)
    Given IPC Reader 正持有 `seq_num=100` 的數據指標 (位於 Slot[100])
    And Reader 進入了 200ms 的 GC 停頓 (模擬 Python 延遲)
    And 在此期間，生產者極速寫入，`write_cursor` 從 101 飆升至 2000
    # 2000 已經繞行覆蓋了 Slot[100] (2000 % 1024 != 100, 但 Slot[100] 可能被 seq=1124 覆蓋)
    # 讓我們精確一點：Slot[100] 會被 seq=1124, 2148... 覆蓋
    And Slot[100] 的內容已被更新為 `seq_num=1124`
    When Reader 從停頓中恢復並檢查手上的數據
    Then Reader 比較 `header.seq_num` (1124) 與 `expected_seq_num` (100)
    And 由於兩者不匹配，Reader 應拋出 `StaleReferenceError` 或 `OverrunWarning`
    And Reader 應自動捨棄當前操作，並將內部指標跳轉至最新的 `write_cursor`
