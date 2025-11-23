import pytest
import os
import time
import sys
import mmap
import struct
import numpy as np
from shijim.ipc.ring_buffer import RingBufferReader, SLOT_COUNT, SLOT_SIZE, HEADER_SIZE
from shijim.sbe.decoder import SBEDecoder

# Add project root to path to find shijim_core.pyd
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

try:
    import shijim_core
except ImportError:
    pytest.skip("shijim_core extension not built", allow_module_level=True)

SHM_NAME = "test_integration_shm_sbe"

@pytest.fixture
def clean_shm():
    # Clean up any existing shm file
    if sys.platform == "win32":
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

def test_rust_sbe_writer_python_reader(clean_shm):
    """
    Integration Test: Rust SBE Writer -> Python Reader -> SBE Decoder
    """
    if sys.platform == "win32":
        pytest.skip("Rust Writer not implemented for Windows yet")

    # 1. Initialize Rust Writer
    writer = shijim_core.RingBufferWriter(SHM_NAME)
    
    # 2. Initialize Python Reader
    reader = RingBufferReader(SHM_NAME)
    reader.attach()
    
    # 3. Publish Data from Rust (SBE Encoded)
    # This encodes: Header + TransactTime + Price(2330.5)
    writer.publish(2330.5)
    
    # 4. Read from Python
    # Get raw bytes payload
    raw_payload = reader.latest_bytes()
    assert raw_payload is not None
    
    # 5. Decode SBE
    decoder = SBEDecoder(raw_payload)
    
    # Decode Header
    header = decoder.decode_header()
    assert header.template_id == 2
    assert header.block_length == 16
    
    # Decode Body
    # We encoded TransactTime (u64) then Price (Decimal64)
    transact_time = decoder.read_u64()
    assert transact_time == 123456789
    
    price = decoder.read_decimal64()
    assert price.to_float() == 2330.5
    
    reader.close()
