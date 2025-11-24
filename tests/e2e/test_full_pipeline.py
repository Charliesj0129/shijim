import os
import sys
import time
import threading
import socket
import struct
import unittest
from decimal import Decimal
from typing import Iterable
from unittest.mock import MagicMock

# Mock external dependencies BEFORE importing shijim
sys.modules['shioaji'] = MagicMock()
sys.modules['orjson'] = MagicMock()

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from shijim.ipc.ring_buffer import RingBufferReader
from shijim.sbe.decoder import SBEDecoder


try:
    import shijim_core
except ImportError:
    print("shijim_core extension not built")
    sys.exit(1)

MARKET_DATA_TEMPLATE_ID = 2
POLL_INTERVAL = 0.001
SBE_HEADER_STRUCT = struct.Struct('<HHHH')
SHM_NAME = "e2e_test_shm"

TEST_MODE = os.getenv("SHIJIM_TEST_MODE", "UNICAST").upper()
USE_UNICAST = TEST_MODE == "UNICAST"
TEST_ADDR = os.getenv(
    "SHIJIM_TEST_ADDR",
    "127.0.0.1" if USE_UNICAST else "239.0.0.1",
)
TEST_PORT = int(os.getenv("SHIJIM_TEST_PORT", "5000"))
TEST_INTERFACE = os.getenv(
    "SHIJIM_TEST_INTERFACE",
    "127.0.0.1" if USE_UNICAST else "0.0.0.0",
)


def encode_decimal64(value: float) -> bytes:
    """
    Encodes a floating point value into SBE Decimal64 (mantissa + exponent).
    """
    dec = Decimal(str(value)).normalize()
    digits = ''.join(str(d) for d in dec.as_tuple().digits) or '0'
    mantissa = int(digits)
    if dec.as_tuple().sign:
        mantissa = -mantissa
    exponent = dec.as_tuple().exponent
    return struct.pack('<qb', mantissa, exponent)


def build_market_data_packet(price: float, transact_time: int) -> bytes:
    header = SBE_HEADER_STRUCT.pack(16, MARKET_DATA_TEMPLATE_ID, 1, 0)
    body = struct.pack('<Q', transact_time) + encode_decimal64(price)
    return header + body


def build_heartbeat_packet() -> bytes:
    return SBE_HEADER_STRUCT.pack(0, 0, 1, 0)


class UDPSender:
    def __init__(self, address: str, port: int, interface: str):
        self.address = address
        self.port = port
        self.interface = interface
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        if not USE_UNICAST:
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
            self.sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            self.sock.setsockopt(
                socket.IPPROTO_IP,
                socket.IP_MULTICAST_IF,
                socket.inet_aton(self.interface),
            )

    def send(self, data: bytes):
        self.sock.sendto(data, (self.address, self.port))

    def close(self):
        self.sock.close()


def run_ingestor(stop_event: threading.Event):
    """
    Runs the Rust ingestor in a dedicated thread to satisfy the BDD Background.
    """
    writer = shijim_core.RingBufferWriter(SHM_NAME)

    while not stop_event.is_set():
        try:
            writer.start_ingestion(f"{TEST_ADDR}:{TEST_PORT}", TEST_INTERFACE)
        except Exception as exc:
            print(f"Ingestor error: {exc}")
            time.sleep(0.1)


class TestE2ESystemValidation(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if sys.platform != "win32":
            path = f"/dev/shm/{SHM_NAME}"
            if os.path.exists(path):
                os.remove(path)

        cls.stop_event = threading.Event()
        cls.ingestor_thread = threading.Thread(target=run_ingestor, args=(cls.stop_event,), daemon=True)
        cls.ingestor_thread.start()
        time.sleep(0.5)

    @classmethod
    def tearDownClass(cls):
        cls.stop_event.set()
        cls.ingestor_thread.join(timeout=2.0)
        if sys.platform != "win32":
            path = f"/dev/shm/{SHM_NAME}"
            if os.path.exists(path):
                os.remove(path)

    def setUp(self):
        self.sender = UDPSender(TEST_ADDR, TEST_PORT, TEST_INTERFACE)
        self.reader = RingBufferReader(SHM_NAME)
        self.reader.attach()

    def tearDown(self):
        self.sender.close()
        self.reader.close()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _wait_for_cursor(self, target: int, timeout: float = 2.0) -> int:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.reader.check_update()
            current = self.reader.write_cursor
            if current >= target:
                return current
            time.sleep(POLL_INTERVAL)
        self.fail(f"Timeout waiting for cursor {target}, current={self.reader.write_cursor}")

    def _wait_without_cursor_change(self, expected: int, duration: float = 0.05):
        deadline = time.monotonic() + duration
        while time.monotonic() < deadline:
            self.reader.check_update()
            if self.reader.write_cursor != expected:
                self.fail(f"Cursor moved unexpectedly: expected {expected}, got {self.reader.write_cursor}")
            time.sleep(POLL_INTERVAL)

    def _ensure_baseline_payload(self) -> bytes:
        if self.reader.write_cursor == 0:
            self.sender.send(build_market_data_packet(1111.1, 1))
            self._wait_for_cursor(1)
        return self.reader.latest_bytes()

    def _decode_payload(self, payload: bytes):
        decoder = SBEDecoder(payload)
        header = decoder.decode_header()
        transact_time = decoder.read_u64()
        price = decoder.read_decimal64()
        price_value = price.to_float() if price else None
        return header, transact_time, price_value

    def _send_market_data(self, price: float, transact_time: int):
        packet = build_market_data_packet(price, transact_time)
        self.sender.send(packet)

    def _burst_send(self, prices: Iterable[float], transact_start: int):
        for idx, price in enumerate(prices):
            self._send_market_data(price, transact_start + idx)
            time.sleep(0.0005)

    # ------------------------------------------------------------------
    # Scenario 1: UDP SBE 封包的完整生命週期
    # ------------------------------------------------------------------
    def test_udp_sbe_packet_lifecycle(self):
        initial_cursor = self.reader.write_cursor
        self._send_market_data(2330.5, 123456)
        self._wait_for_cursor(initial_cursor + 1)

        payload = self.reader.latest_bytes()
        header, transact_time, price = self._decode_payload(payload)

        self.assertEqual(header.template_id, MARKET_DATA_TEMPLATE_ID)
        self.assertEqual(transact_time, 123456)
        self.assertAlmostEqual(price, 2330.5)
        self.assertEqual(self.reader.write_cursor, initial_cursor + 1)

    # ------------------------------------------------------------------
    # Scenario 2: 攔截 Heartbeat 封包
    # ------------------------------------------------------------------
    def test_kernel_filters_heartbeat_packets(self):
        baseline_payload = self._ensure_baseline_payload()
        baseline_cursor = self.reader.write_cursor

        self.sender.send(build_heartbeat_packet())
        self._wait_without_cursor_change(baseline_cursor, duration=0.05)

        self.assertEqual(self.reader.write_cursor, baseline_cursor)
        self.assertEqual(self.reader.latest_bytes(), baseline_payload)

    # ------------------------------------------------------------------
    # Scenario 3: 連續數據流處理
    # ------------------------------------------------------------------
    def test_burst_stream_integrity(self):
        initial_cursor = self.reader.write_cursor
        prices = [100.0 + float(i) for i in range(100)]

        self._burst_send(prices, transact_start=1_000_000)
        final_cursor = self._wait_for_cursor(initial_cursor + len(prices), timeout=5.0)

        observed_prices = []
        for seq in range(initial_cursor + 1, initial_cursor + len(prices) + 1):
            row = self.reader.read_at(seq)
            self.assertEqual(row['seq_num'], seq, "SeqNum should be contiguous")
            payload = bytes(row['payload'])
            header, _, price = self._decode_payload(payload)
            self.assertEqual(header.template_id, MARKET_DATA_TEMPLATE_ID)
            observed_prices.append(price)

        self.assertEqual(observed_prices, prices)
        self.assertEqual(final_cursor, initial_cursor + len(prices))

    # ------------------------------------------------------------------
    # Scenario 4: 超大封包 (Jumbo Frame) 處理
    # ------------------------------------------------------------------
    def test_jumbo_frame_truncation(self):
        initial_cursor = self.reader.write_cursor
        header = SBE_HEADER_STRUCT.pack(16, MARKET_DATA_TEMPLATE_ID, 1, 0)
        payload = b'J' * 292
        packet = header + payload

        self.sender.send(packet)
        self._wait_for_cursor(initial_cursor + 1)

        data = self.reader.latest_bytes()
        self.assertEqual(self.reader.write_cursor, initial_cursor + 1)
        self.assertEqual(len(data), 248)
        self.assertEqual(data[:8], header)
        self.assertEqual(data[8:], b'J' * 240)


if __name__ == '__main__':
    unittest.main()
