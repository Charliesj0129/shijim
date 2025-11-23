import pytest
import numpy as np
import time
import gc
from multiprocessing import shared_memory
from shijim.ipc.ring_buffer import RingBufferReader, DataIntegrityError, StaleReferenceError, MARKET_DATA_DTYPE, HEADER_SIZE, SLOT_COUNT, SLOT_SIZE

SHM_NAME = "shijim_market_data_l2"
TOTAL_SIZE = HEADER_SIZE + (SLOT_COUNT * SLOT_SIZE)

@pytest.fixture
def mock_producer():
    """
    Creates the shared memory segment and acts as the producer.
    Yields a tuple of (shm, header_view, data_view).
    """
    # Clean up if exists
    try:
        existing = shared_memory.SharedMemory(name=SHM_NAME)
        existing.close()
        existing.unlink()
    except (FileNotFoundError, OSError):
        pass

    # Create new
    try:
        shm = shared_memory.SharedMemory(name=SHM_NAME, create=True, size=TOTAL_SIZE)
    except FileExistsError:
        # Fallback if unlink failed previously
        shm = shared_memory.SharedMemory(name=SHM_NAME)
    
    # Create views for easy writing
    buffer = shm.buf
    header_view = np.frombuffer(buffer, dtype='u8', count=1, offset=0)
    data_view = np.frombuffer(buffer, dtype=MARKET_DATA_DTYPE, count=SLOT_COUNT, offset=HEADER_SIZE)
    
    # Initialize cursor to 0
    header_view[0] = 0
    
    yield shm, header_view, data_view
    
    # Teardown
    # Release views created in the fixture
    del header_view
    del data_view
    buffer.release()
    
    # Force GC to ensure numpy views are collected
    # Loop to ensure cyclic references are broken
    for _ in range(5):
        gc.collect()
    
    try:
        shm.close()
    except BufferError:
        # Known issue with multiprocessing.shared_memory on Windows where views might linger
        pass
        
    try:
        shm.unlink()
    except FileNotFoundError:
        pass

def test_scenario_1_attach_and_view(mock_producer):
    """
    Scenario 1: Connect and Structure Mapping (Happy Path)
    """
    shm, header, data = mock_producer
    
    # Given Producer init and write slot 0
    header[0] = 1  # Cursor at 1 (wrote 1 item)
    
    # Write to Slot 0 (index 0)
    data[0]['seq_num'] = 100
    data[0]['best_bid_price'] = 2330.0
    
    # When Reader attaches
    reader = RingBufferReader(SHM_NAME)
    try:
        reader.attach()
        
        # Then valid object
        assert reader._shm is not None
        
        # And read specific value
        row = reader._data_view[0]
        assert row['best_bid_price'] == 2330.0
        assert row['seq_num'] == 100
        
        del row
        
    finally:
        reader.close()

def test_scenario_2_catch_up(mock_producer):
    """
    Scenario 2: Catch-up Logic
    """
    shm, header, data = mock_producer
    
    # Given Producer wrote 500 items
    header[0] = 500
    # Slot[499] is the 500th item
    data[499]['seq_num'] = 500
    data[499]['best_bid_price'] = 123.45
    
    # When Reader reads latest
    reader = RingBufferReader(SHM_NAME)
    try:
        reader.attach()
        row = reader.latest()
        
        # Then
        assert row['seq_num'] == 500
        assert row['best_bid_price'] == 123.45
        
        del row
    finally:
        reader.close()

def test_scenario_3_wrap_around(mock_producer):
    """
    Scenario 3: Wrap-Around Handling
    """
    shm, header, data = mock_producer
    
    # Given Capacity 1024
    # Producer cursor 1025 (wrapped once, wrote 1 item in next lap)
    header[0] = 1025
    
    # Index = (1025 - 1) % 1024 = 0
    data[0]['seq_num'] = 1025
    data[0]['best_bid_price'] = 999.0
    
    # When Reader reads cursor 1025
    reader = RingBufferReader(SHM_NAME)
    try:
        reader.attach()
        
        # We can use read_at or latest. Scenario says "Request read cursor=1025".
        row = reader.read_at(1025)
        
        # Then
        assert row['seq_num'] == 1025
        assert row['best_bid_price'] == 999.0
        
        del row
    finally:
        reader.close()

def test_scenario_4_overrun(mock_producer):
    """
    Scenario 4: Slow Consumer / Overrun
    """
    shm, header, data = mock_producer
    
    # Given Reader holds seq=100
    # Let's simulate the state.
    header[0] = 100
    data[99]['seq_num'] = 100
    
    reader = RingBufferReader(SHM_NAME)
    try:
        reader.attach()
        
        # Producer updates to 2000
        header[0] = 2000
        # Slot[99] (where 100 was) is now overwritten by 1124 (100 + 1024)
        data[99]['seq_num'] = 1124
        
        # When Reader checks
        # We simulate the check by calling read_at(100)
        with pytest.raises((StaleReferenceError, DataIntegrityError)):
            reader.read_at(100)
    finally:
        reader.close()
