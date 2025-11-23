import struct
import decimal
from typing import NamedTuple, Generator, Optional, Any
from dataclasses import dataclass

class SBEDecodeError(Exception):
    """Raised when SBE decoding fails."""
    pass

class BufferUnderflow(SBEDecodeError):
    """Raised when buffer is too short."""
    pass

# SBE Constants
SBE_HEADER_SIZE = 8
SBE_GROUP_HEADER_SIZE = 4 # BlockSize(u16) + NumInGroup(u16)
INT64_MAX = 0x7FFFFFFFFFFFFFFF
INT64_NULL = INT64_MAX # As per BDD Scenario 4

class SBEHeader(NamedTuple):
    block_length: int
    template_id: int
    schema_id: int
    version: int

@dataclass
class Decimal64:
    mantissa: int
    exponent: int
    
    def to_float(self) -> float:
        return float(self.mantissa) * (10 ** self.exponent)
    
    def to_decimal(self) -> decimal.Decimal:
        return decimal.Decimal(self.mantissa) * (decimal.Decimal(10) ** self.exponent)

class SBEDecoder:
    """
    A low-level SBE decoder that wraps a buffer and manages offsets.
    Designed for lazy decoding.
    """
    def __init__(self, buffer: bytes | bytearray | memoryview, offset: int = 0):
        self._buffer = memoryview(buffer)
        self._offset = offset
        self._limit = len(buffer)

    @property
    def offset(self) -> int:
        return self._offset

    def _check_bounds(self, size: int):
        if self._offset + size > self._limit:
            raise BufferUnderflow(f"Need {size} bytes, but only {self._limit - self._offset} left.")

    def decode_header(self) -> SBEHeader:
        """
        Reads the standard SBE header (8 bytes).
        Advances offset by 8.
        """
        self._check_bounds(SBE_HEADER_SIZE)
        # Little Endian: H(u16), H(u16), H(u16), H(u16)
        # BlockLength, TemplateID, SchemaID, Version
        vals = struct.unpack_from('<HHHH', self._buffer, self._offset)
        self._offset += SBE_HEADER_SIZE
        return SBEHeader(*vals)

    def skip(self, bytes_count: int):
        self._check_bounds(bytes_count)
        self._offset += bytes_count

    def read_u16(self) -> int:
        self._check_bounds(2)
        val = struct.unpack_from('<H', self._buffer, self._offset)[0]
        self._offset += 2
        return val

    def read_decimal64(self) -> Optional[Decimal64]:
        """
        Reads composite Decimal64 (i64 mantissa + i8 exponent).
        Advances offset by 9.
        """
        self._check_bounds(9)
        mantissa, exponent = struct.unpack_from('<qb', self._buffer, self._offset)
        self._offset += 9
        
        if mantissa == INT64_NULL:
            return None
            
        return Decimal64(mantissa, exponent)

    def groups(self) -> Generator['SBEDecoder', None, None]:
        """
        Reads a repeating group header and yields a decoder for each entry.
        Advances offset past the entire group.
        """
        self._check_bounds(SBE_GROUP_HEADER_SIZE)
        block_size, num_in_group = struct.unpack_from('<HH', self._buffer, self._offset)
        self._offset += SBE_GROUP_HEADER_SIZE
        
        # Check total size required
        # WARNING: This assumes fixed-size entries (no nested groups or var data).
        # If the schema contains nested groups or variable length data, 
        # simply multiplying block_size * num_in_group is INCORRECT.
        # The decoder must iterate and parse each entry dynamically.
        # For MDIncrementalRefreshBook (Template 2), entries are usually fixed.
        total_group_size = block_size * num_in_group
        self._check_bounds(total_group_size)
        
        for _ in range(num_in_group):
            # Yield a view/decoder for this entry
            # Note: We could yield a new SBEDecoder instance, 
            # but to keep it efficient and sequential, we can just yield 'self' 
            # assuming the caller consumes it sequentially.
            # However, to support random access within the block, a sub-decoder is safer.
            # But for "Lazy Decoding" of fields, we usually just want the base offset.
            
            # Implementation choice: Yield a lightweight object or just yield the decoder itself positioned at start?
            # If we yield the decoder, the user must read exactly 'block_size' bytes?
            # Or we yield a new decoder restricted to the block?
            
            # Let's yield a new SBEDecoder restricted to the block size for safety,
            # OR just yield the current decoder and manually advance if the user doesn't read?
            # SBE is strictly sequential. If the user skips a field, they must skip bytes.
            # But here we know the block size.
            
            # Let's yield a sub-decoder for the entry.
            entry_start = self._offset
            entry_end = entry_start + block_size
            
            # Create a view for the entry
            entry_view = self._buffer[entry_start:entry_end]
            yield SBEDecoder(entry_view, offset=0)
            
            # Advance main decoder
            self._offset += block_size

    # Helper for specific fields mentioned in BDD
    def read_u8(self) -> int:
        self._check_bounds(1)
        val = struct.unpack_from('<B', self._buffer, self._offset)[0]
        self._offset += 1
        return val
        
