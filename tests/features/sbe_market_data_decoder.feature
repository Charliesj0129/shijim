Feature: SBE 市場數據解碼器 (SBE Market Data Decoder)
  為了 將 Ring Buffer 中的二進制數據轉換為策略可用的結構化信號
  作為 數據工程師
  我要求 解碼器必須能正確處理 SBE 的 Headers、複合類型 (Composite) 與重複組 (Repeating Groups)。

  # ---------------------------------------------------------------------------
  # 背景：定義 SBE Schema 的上下文
  # 假設我們正在處理 CME 風格的 MDIncrementalRefreshBook (TemplateID=2)
  # ---------------------------------------------------------------------------
  Background: SBE 協議環境
    Given 系統加載了 "MicroAlpha_v1.xml" Schema 定義
    And Schema 定義了 TemplateID 2 為 "MDIncrementalRefreshBook"
    And TemplateID 2 的結構包含:
      | Section | Field Name | Type | Offset |
      | Header  | BlockLength | u16 | 0 |
      | Header  | TemplateID  | u16 | 2 |
      | Body    | TransactTime| u64 | 0 |
      | Group   | NoMDEntries | Group | Varies |

  # ---------------------------------------------------------------------------
  # 場景 1: 消息頭識別 (Header Identification)
  # 驗證重點：在讀取 Payload 之前，必須先讀 Header 確定怎麼解碼。
  # ---------------------------------------------------------------------------
  Scenario: 正確解析 SBE 消息頭
    Given 一個二進制緩衝區 (Buffer)，前 8 字節為:
      # BlockLength=16, TemplateID=2, SchemaID=1, Version=0 (Little Endian)
      | Byte 0-1 | Byte 2-3 | Byte 4-5 | Byte 6-7 |
      | 16 (0x1000) | 2 (0x0200) | 1 (0x0100) | 0 (0x0000) |
    When 解碼器讀取消息頭 (Header)
    Then 解碼器應識別 TemplateID 為 2
    And 解碼器應知道 Root Block 長度為 16 bytes
    And 解碼器指標應前進 8 bytes (Header Size)

  # ---------------------------------------------------------------------------
  # 場景 2: 複合類型解析 (Composite Decimal)
  # 驗證重點：價格不應丟失精度。
  # SBE Price: Mantissa(i64) + Exponent(i8)
  # ---------------------------------------------------------------------------
  Scenario: 解析複合價格欄位 (Decimal64)
    Given Root Block 中包含一個 "Price" 欄位
    # Mantissa = 23305 (0x5B09...), Exponent = -1 (0xFF) -> 代表 2330.5
    # Note: Correct Little Endian for 23305 is 09 5B. 
    # We will use 09 5B 00 00 00 00 00 00 FF in implementation to match 23305.
    And 緩衝區對應位置的數據為 Hex: "09 5B 00 00 00 00 00 00 FF" (Little Endian i64 + i8)
    When 解碼器解析該 Price 欄位
    Then 解碼器應提取 Mantissa 為 23305
    And 解碼器應提取 Exponent 為 -1
    And 若請求轉換為 Float，結果應嚴格等於 2330.5

  # ---------------------------------------------------------------------------
  # 場景 3: 重複組迭代 (Repeating Group Iteration) - 核心難點
  # 驗證重點：SBE 的 Group 是嵌套的，必須先讀 Size 再讀 N 個 Block。
  # ---------------------------------------------------------------------------
  Scenario: 解析包含 2 筆更新的重複組 (Repeating Group)
    Given 消息體之後緊接著 "NoMDEntries" Group 的頭部數據:
      # BlockSize=32 (每筆 Entry 32 bytes), NumInGroup=2 (共 2 筆)
      | Type | Value | Raw (Hex) |
      | u16  | 32    | 20 00 |
      | u16  | 2     | 02 00 |
    And 緊接著是 64 bytes (32 * 2) 的 Entry 數據
    And 第一筆 Entry 的 "MDEntryType" 為 0 (Bid)
    And 第二筆 Entry 的 "MDEntryType" 為 1 (Ask)
    When 解碼器開始迭代 "NoMDEntries" Group
    Then 解碼器應正確識別出 Group 包含 2 個項目
    And 第一個迭代物件的 EntryType 應為 "Bid"
    And 第二個迭代物件的 EntryType 應為 "Ask"
    And 迭代結束後，解碼器指標應前進了 4 + 64 = 68 bytes

  # ---------------------------------------------------------------------------
  # 場景 4: 處理 Null 值 (Optional Fields)
  # 驗證重點：SBE 使用特殊值 (MaxInt/MinInt) 來表示 Null，不能當作正常數字處理。
  # ---------------------------------------------------------------------------
  Scenario: 識別可選欄位的 Null 狀態
    Given Schema 定義 "Price" 的 Null 值為 INT64_MAX (0x7FFFFFFFFFFFFFFF)
    And 緩衝區中的 Price 數據全為 0xFF (除了最後一個 byte 是 0x7F)
    When 解碼器解析該 Price 欄位
    Then 解碼器應判定該欄位為 "Null" 或 "None"
    And 解碼器不應將其轉換為一個巨大的整數

  # ---------------------------------------------------------------------------
  # 場景 5: 邊界檢查與Malformed數據
  # 驗證重點：如果 Group 聲稱有 100 筆數據但 Buffer 長度不夠，必須報錯，防止讀取越界。
  # ---------------------------------------------------------------------------
  Scenario: 緩衝區不足以容納聲明的 Group 數量
    Given Group 頭部聲明 NumInGroup=50, BlockSize=100 (預期 5000 bytes)
    But 剩餘的緩衝區長度僅有 200 bytes
    When 解碼器嘗試解析 Group
    Then 系統應拋出 `SBEDecodeError` 或 `BufferUnderflow`
    And 解碼過程應立即中止
