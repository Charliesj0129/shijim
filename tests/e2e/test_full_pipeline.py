import pytest
import os
import time
import sys
import threading
import struct
import socket
import numpy as np
from shijim.ipc.ring_buffer import RingBufferReader
from shijim.sbe.decoder import SBEDecoder

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

try:
    import shijim_core
except ImportError:
    pytest.skip("shijim_core extension not built", allow_module_level=True)

SHM_NAME = "test_e2e_shm"
MULTICAST_ADDR = "239.0.0.1"
PORT = 5000
INTERFACE = "0.0.0.0" # Or 127.0.0.1 depending on OS

# Helper to create SBE packet
def create_sbe_packet(template_id=2, price=0.0, transact_time=0):
    # Header: BlockLength(u16), TemplateID(u16), SchemaID(u16), Version(u16)
    # BlockLength=16 (Body size: u64 + Decimal64 = 8 + 9 = 17? No, wait.)
    # In Phase 2, we encoded TransactTime(u64) + Price(Decimal64).
    # u64 = 8 bytes. Decimal64 = 9 bytes. Total 17 bytes.
    # But Header BlockLength is usually the size of the Root Block.
    # Let's say BlockLength=17.
    
    header = struct.pack('<HHHH', 17, template_id, 1, 0)
    
    # Body
    # TransactTime (u64)
    body_time = struct.pack('<Q', transact_time)
    
    # Price (Decimal64) -> Mantissa(i64) + Exponent(i8)
    # 2330.5 -> 23305, -1
    # Simple converter for test
    mantissa = int(price * 10)
    exponent = -1
    body_price = struct.pack('<qb', mantissa, exponent)
    
    return header + body_time + body_price

def send_udp_packet(packet):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    sock.sendto(packet, (MULTICAST_ADDR, PORT))
    sock.close()

@pytest.fixture
def clean_shm():
    if sys.platform != "win32":
        path = f"/dev/shm/{SHM_NAME}"
        if os.path.exists(path):
            os.remove(path)
    yield
    if sys.platform != "win32":
        path = f"/dev/shm/{SHM_NAME}"
        if os.path.exists(path):
            os.remove(path)

def run_ingestor(writer):
    # This runs the blocking ingestion loop (100 cycles)
    try:
        writer.start_ingestion(f"{MULTICAST_ADDR}:{PORT}", INTERFACE)
    except Exception as e:
        print(f"Ingestor thread error: {e}")

def test_full_pipeline(clean_shm):
    """
    E2E Test: UDP -> Rust Ingestor -> Ring Buffer -> Python Reader
    """
    if sys.platform == "win32":
        pytest.skip("Rust Writer/Ingestor not fully supported on Windows")

    # 1. Init Writer & Reader
    writer = shijim_core.RingBufferWriter(SHM_NAME)
    reader = RingBufferReader(SHM_NAME)
    reader.attach()
    
    # 2. Start Ingestor in Background Thread
    ingest_thread = threading.Thread(target=run_ingestor, args=(writer,), daemon=True)
    ingest_thread.start()
    
    # Give it a moment to bind socket
    time.sleep(0.1)
    
    # ---------------------------------------------------------
    # Scenario 1: Happy Path
    # ---------------------------------------------------------
    initial_cursor = reader.write_cursor
    
    # Send Packet
    pkt = create_sbe_packet(template_id=2, price=2330.5, transact_time=123456)
    send_udp_packet(pkt)
    
    # Wait for processing
    time.sleep(0.1)
    
    # Check Cursor
    assert reader.write_cursor == initial_cursor + 1
    
    # Read Data
    payload = reader.latest_bytes()
    decoder = SBEDecoder(payload)
    header = decoder.decode_header()
    assert header.template_id == 2
    
    t_time = decoder.read_u64()
    assert t_time == 123456
    
    price = decoder.read_decimal64()
    assert price.to_float() == 2330.5
    
    # ---------------------------------------------------------
    # Scenario 2: Filtering (Heartbeat)
    # ---------------------------------------------------------
    current_cursor = reader.write_cursor
    
    # Send Heartbeat (Template ID 0)
    pkt_hb = create_sbe_packet(template_id=0, price=0, transact_time=0)
    send_udp_packet(pkt_hb)
    
    time.sleep(0.1)
    
    # Cursor should NOT move
    assert reader.write_cursor == current_cursor
    
    # ---------------------------------------------------------
    # Scenario 3: Stress / Continuity
    # ---------------------------------------------------------
    start_price = 100.0
    count = 10
    
    for i in range(count):
        p = start_price + i
        pkt = create_sbe_packet(template_id=2, price=p, transact_time=i)
        send_udp_packet(pkt)
        time.sleep(0.001) # Tiny delay to avoid UDP buffer overflow in this simple test
        
    time.sleep(0.2)
    
    # Check final cursor
    assert reader.write_cursor == current_cursor + count
    
    # Verify latest is the last one
    payload = reader.latest_bytes()
    decoder = SBEDecoder(payload)
    decoder.decode_header()
    decoder.read_u64()
    last_price = decoder.read_decimal64()
    assert last_price.to_float() == start_price + count - 1
    
    reader.close()
