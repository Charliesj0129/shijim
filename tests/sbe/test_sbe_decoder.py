import struct

import pytest

from shijim.sbe.decoder import (
    INT64_MAX,
    BufferUnderflow,
    SBEDecoder,
)


def test_scenario_1_header_identification():
    """
    Scenario 1: Header Identification
    """
    # Given Buffer with Header
    # BlockLength=16 (0x1000), TemplateID=2 (0x0200), SchemaID=1 (0x0100), Version=0 (0x0000)
    data = struct.pack('<HHHH', 16, 2, 1, 0)

    # When Decoder reads header
    decoder = SBEDecoder(data)
    header = decoder.decode_header()

    # Then
    assert header.template_id == 2
    assert header.block_length == 16
    assert decoder.offset == 8

def test_scenario_2_composite_decimal():
    """
    Scenario 2: Composite Decimal64
    """
    # Given Root Block with Price field
    # Mantissa = 23305, Exponent = -1
    # 23305 = 0x5B09 -> Little Endian: 09 5B 00 00 00 00 00 00
    # Exponent = -1 -> 0xFF
    mantissa = 23305
    exponent = -1
    data = struct.pack('<qb', mantissa, exponent)

    # When Decoder parses Price
    decoder = SBEDecoder(data)
    price = decoder.read_decimal64()

    # Then
    assert price.mantissa == 23305
    assert price.exponent == -1
    assert price.to_float() == 2330.5

def test_scenario_3_repeating_group():
    """
    Scenario 3: Repeating Group Iteration
    """
    # Given Group Header + 2 Entries
    # Group Header: BlockSize=32, NumInGroup=2
    group_header = struct.pack('<HH', 32, 2)

    # Entry 1: MDEntryType=0 (Bid), + padding to 32 bytes
    # Entry 2: MDEntryType=1 (Ask), + padding to 32 bytes
    # Assuming MDEntryType is the first byte (u8) for simplicity of test, or at some offset.
    # The decoder yields a sub-decoder. We can read u8 from it.
    entry1 = struct.pack('<B', 0) + b'\x00' * 31
    entry2 = struct.pack('<B', 1) + b'\x00' * 31

    data = group_header + entry1 + entry2

    # When Decoder iterates
    decoder = SBEDecoder(data)
    entries = list(decoder.groups())

    # Then
    assert len(entries) == 2

    # Check Entry 1
    assert entries[0].read_u8() == 0 # Bid

    # Check Entry 2
    assert entries[1].read_u8() == 1 # Ask

    # And decoder offset should be advanced
    # Header(4) + 2 * 32 = 68
    assert decoder.offset == 68

def test_scenario_4_null_values():
    """
    Scenario 4: Null Values
    """
    # Given Price Null = INT64_MAX
    mantissa = INT64_MAX
    exponent = 0 # Irrelevant
    data = struct.pack('<qb', mantissa, exponent)

    # When Decoder parses
    decoder = SBEDecoder(data)
    price = decoder.read_decimal64()

    # Then
    assert price is None

def test_scenario_5_buffer_underflow():
    """
    Scenario 5: Buffer Underflow
    """
    # Given Group Header declaring 50 entries of 100 bytes (5000 bytes needed)
    # But buffer is only 200 bytes
    group_header = struct.pack('<HH', 100, 50)
    data = group_header + b'\x00' * 196 # Total 200 bytes

    # When Decoder attempts to parse group
    decoder = SBEDecoder(data)

    # Then throws SBEDecodeError / BufferUnderflow
    with pytest.raises(BufferUnderflow):
        list(decoder.groups())
