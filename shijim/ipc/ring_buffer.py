import mmap
import numpy as np
import struct
import os
from typing import Optional, Tuple
from dataclasses import dataclass

class DataIntegrityError(Exception):
    """Raised when sequence number validation fails."""
    pass

class StaleReferenceError(Exception):
    """Raised when the reader is too slow and data is overwritten."""
    pass

import sys
import os

# Configuration Constants
HEADER_SIZE = 128
SLOT_COUNT = 1024
SLOT_SIZE = 256

# Schema Definition
# Slot Layout:
# [0-7]: seq_num (u64)
# [8-255]: SBE Payload (248 bytes)
MARKET_DATA_DTYPE = np.dtype([
    ('seq_num', 'u8'),
    ('payload', 'V248')
])

assert MARKET_DATA_DTYPE.itemsize == SLOT_SIZE, f"Schema size mismatch: {MARKET_DATA_DTYPE.itemsize} != {SLOT_SIZE}"

class RingBufferReader:
    """
    Zero-copy reader for a shared memory ring buffer produced by a Rust core.
    """
    def __init__(self, shm_name: str):
        self.shm_name = shm_name
        self._shm = None
        self._buffer = None
        self._header_view = None
        self._data_view = None
        
    def attach(self):
        """
        Connects to the shared memory segment.
        Supports Windows (Named Shared Memory) and Linux (/dev/shm).
        """
        total_size = HEADER_SIZE + (SLOT_COUNT * SLOT_SIZE)
        
        try:
            if sys.platform == "win32":
                # Windows: tagname maps to Named Shared Memory
                self._shm = mmap.mmap(-1, total_size, tagname=self.shm_name, access=mmap.ACCESS_WRITE)
            else:
                # Linux: Use /dev/shm/
                # We assume the file exists in /dev/shm/ with the given name.
                shm_path = f"/dev/shm/{self.shm_name}"
                if not os.path.exists(shm_path):
                    raise FileNotFoundError(f"Shared memory file not found at {shm_path}")
                    
                # Open file descriptor
                with open(shm_path, "r+b") as f:
                    self._shm = mmap.mmap(f.fileno(), total_size, access=mmap.ACCESS_WRITE)
                    
        except OSError as e:
            raise ConnectionError(f"Could not attach to shared memory '{self.shm_name}': {e}")

        # Create numpy views
        # 1. Header View (assuming write_cursor is the first u64)
        self._buffer = memoryview(self._shm)
        self._header_view = np.frombuffer(self._buffer, dtype='u8', count=1, offset=0)
        
        # 2. Data View (Structured Array)
        # Offset by HEADER_SIZE
        self._data_view = np.frombuffer(
            self._buffer, 
            dtype=MARKET_DATA_DTYPE, 
            count=SLOT_COUNT, 
            offset=HEADER_SIZE
        )

    @property
    def write_cursor(self) -> int:
        """Reads the current write cursor from the header."""
        # Volatile read is not strictly guaranteed by numpy, but usually works for mmap.
        return int(self._header_view[0])

    def _get_slot_index(self, cursor: int) -> int:
        """Calculates the physical slot index for a given cursor."""
        # Cursor is 1-based (usually), or 0-based? 
        # BDD says: "write_cursor set to 0" initially.
        # Scenario 2: "write_cursor to 500", "Slot[499] has seq_num 500".
        # This implies cursor is a monotonically increasing counter (sequence number of next write? or last write?).
        # If cursor=500 and data is at 499 (0-indexed), it implies:
        #   write_cursor points to the *next* available slot? 
        #   Or write_cursor is the *last written* seq_num?
        # Let's look at Scenario 2 again:
        # "Producer written 500 items, current write_cursor is 500"
        # "Slot[499] seq_num is 500".
        # This implies:
        #   Item 1 -> Slot 0
        #   Item 500 -> Slot 499
        #   write_cursor = 500 (Total items written).
        #   So to read item 500, we look at index (500-1) % 1024.
        
        if cursor == 0:
            raise ValueError("Cursor is 0, no data written yet.")
            
        return (cursor - 1) % SLOT_COUNT
        
        # 4. Verify Integrity
        # The row is a view. If we read fields, we read from shm.
        # We should check seq_num matches the cursor.
        if row['seq_num'] != cursor:
            # If the seq_num in the slot doesn't match the cursor we used to find it,
            # it means the producer overwrote it *before* or *during* our read setup.
            # Or we are reading a slot that hasn't been updated yet (unlikely if cursor moved).
            # Actually, if cursor=500, we expect seq_num=500.
            # If we see seq_num=1124 (Scenario 4), it's an overrun.
            raise StaleReferenceError(f"Overrun detected: Expected seq={cursor}, found {row['seq_num']}")
            
        return row

    def read_at(self, cursor: int) -> np.void:
        """
        Reads data for a specific cursor/sequence number.
        """
        idx = self._get_slot_index(cursor)
        row = self._data_view[idx]
        
        if row['seq_num'] != cursor:
             # If we asked for 100, and got 1124, that's an overrun/stale.
             # If we asked for 100, and got 0, that's data not ready?
             raise DataIntegrityError(f"Data integrity check failed: Expected seq={cursor}, found {row['seq_num']}")
             
        return row


    def latest(self) -> np.void:
        """
        Reads the most recently written data.
        """
        cursor = self.write_cursor
        if cursor == 0:
            raise ValueError("No data written yet (cursor is 0)")
        return self.read_at(cursor)
    
    def latest_bytes(self) -> bytes:
        """
        Returns the raw payload bytes of the latest slot.
        """
        cursor = self.write_cursor
        if cursor == 0:
            raise ValueError("No data written yet (cursor is 0)")
        idx = self._get_slot_index(cursor)
        row = self._data_view[idx]
        return bytes(row['payload'])
    
    def check_update(self) -> bool:
        """
        Checks if write_cursor has changed. Returns True if there's new data.
        This doesn't cache the previous cursor, so it just returns True if cursor > 0.
        For more sophisticated tracking, caller should cache the previous cursor.
        """
        return self.write_cursor > 0

    def close(self):
        # Explicitly release numpy views to decrement reference counts on the buffer
        self._header_view = None
        self._data_view = None
        
        if self._buffer:
            self._buffer.release()
            self._buffer = None
            
        if self._shm:
            self._shm.close()
            self._shm = None

