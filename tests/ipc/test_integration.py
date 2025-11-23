import pytest
import os
import time
import sys
import mmap
import struct
import numpy as np
from shijim.ipc.ring_buffer import RingBufferReader, SLOT_COUNT, SLOT_SIZE, HEADER_SIZE

# Add project root to path to find shijim_core.pyd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

try:
    import shijim_core
except ImportError:
    pytest.skip("shijim_core extension not built", allow_module_level=True)

SHM_NAME = "test_integration_shm"

@pytest.fixture
def clean_shm():
    # Clean up any existing shm file
    if sys.platform == "win32":
        # Windows named shm cleanup is tricky if persisted, but usually it's process lifetime.
        # However, our Rust implementation uses a file-backed mmap for now to simulate.
        # But wait, the Rust implementation for Windows in `lib.rs` returned Error!
        # "Windows Named Shared Memory not fully implemented..."
        # So we expect this to fail on Windows unless we implemented the file fallback in the `else` block?
        # Looking at `lib.rs`:
        # if cfg!(target_os = "windows") { return Err(...) }
        # So this test will fail on Windows.
        pass
    else:
        path = f"/dev/shm/{SHM_NAME}"
        if os.path.exists(path):
            os.remove(path)
    
    yield
    
    if sys.platform != "win32":
        path = f"/dev/shm/{SHM_NAME}"
        if os.path.exists(path):
            os.remove(path)

def test_rust_writer_python_reader(clean_shm):
    """
    Integration Test: Rust Writer -> Python Reader
    """
    if sys.platform == "win32":
        pytest.skip("Rust Writer not implemented for Windows yet")

    # 1. Initialize Rust Writer
    writer = shijim_core.RingBufferWriter(SHM_NAME)
    
    # 2. Initialize Python Reader
    reader = RingBufferReader(SHM_NAME)
    reader.attach()
    
    # 3. Publish Data from Rust
    # Seq 1
    writer.publish(100.5)
    
    # 4. Read from Python
    # Wait a tiny bit for propagation? (Should be instant via SHM)
    row = reader.latest()
    
    assert row['seq_num'] == 1
    assert row['best_bid_price'] == 100.5
    
    # 5. Publish more
    writer.publish(101.0)
    writer.publish(102.0)
    
    # Check cursor
    assert writer.current_cursor() == 3
    assert reader.write_cursor == 3
    
    # Read latest
    row = reader.latest()
    assert row['seq_num'] == 3
    assert row['best_bid_price'] == 102.0
    
    # Read specific
    row_1 = reader.read_at(1)
    assert row_1['best_bid_price'] == 100.5
    
    reader.close()
