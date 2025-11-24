from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Optional, Protocol, Sequence

from shijim.dashboard.app import SystemSnapshot
from shijim.sbe.decoder import SBEDecodeError
from shijim.strategy.engine import SmartChasingEngine
from shijim.strategy.ofi import BboState, OfiCalculator


class RingBufferReaderProtocol(Protocol):
    def latest_bytes(self) -> bytes: ...


class SbeDecoderProtocol(Protocol):
    def decode(self, payload: bytes) -> BboState: ...


class GatewayProtocol(Protocol):
    def send(self, actions: Sequence) -> None: ...


@dataclass
class StrategyRunner:
    reader: RingBufferReaderProtocol
    decoder: SbeDecoderProtocol
    ofi: OfiCalculator
    strategy: SmartChasingEngine
    gateway: GatewayProtocol
    metrics_enabled: bool = False
    logger: Optional[callable] = None
    metrics: dict[str, float] | None = None
    metrics_callback: Optional[Callable[[SystemSnapshot], None]] = None

    def __post_init__(self) -> None:
        if self.metrics is None:
            self.metrics = {}

    def tick(self) -> None:
        start_ts = time.perf_counter() if self.metrics_enabled else 0.0
        snapshot: Optional[SystemSnapshot] = None
        try:
            payload = self.reader.latest_bytes()
            bbo = self.decoder.decode(payload)
            ofi_result = self.ofi.process_tick(
                bbo.bid_price, bbo.bid_size, bbo.ask_price, bbo.ask_size
            )
            actions = self.strategy.on_tick(bbo, ofi_override=ofi_result.net_ofi)
            if actions:
                self.gateway.send(actions)
            if self.metrics_callback:
                snapshot = self._build_snapshot(bbo, ofi_result.net_ofi)
        except SBEDecodeError:
            if self.logger:
                self.logger("Invalid Market Data")
        finally:
            if snapshot and self.metrics_callback:
                self.metrics_callback(snapshot)
            if self.metrics_enabled:
                elapsed = time.perf_counter() - start_ts
                self.metrics["TickProcessingTime"] = elapsed

    def _build_snapshot(self, bbo: BboState, ofi_value: float) -> SystemSnapshot:
        lag = self._ring_lag()
        state = getattr(self.strategy.order_manager, "state", None)
        strategy_state = state.name if state else "UNKNOWN"
        active_orders = getattr(self.gateway, "orders", [])
        if isinstance(active_orders, list):
            order_repr = [getattr(o, "reason", str(o)) for o in active_orders]
        else:
            order_repr = [str(active_orders)]
        position = getattr(self.strategy, "position", 0.0)
        kill_switch = getattr(self.gateway, "kill_switch_active", False)
        reject_count = getattr(self.gateway, "reject_count", 0)
        logs = list(getattr(self.strategy, "logs", []))
        return SystemSnapshot(
            timestamp=time.time(),
            ring_buffer_lag=lag,
            bid=bbo.bid_price,
            ask=bbo.ask_price,
            last_price=getattr(bbo, "last_price", bbo.bid_price),
            ofi=ofi_value,
            strategy_state=strategy_state,
            active_orders=order_repr,
            position=position,
            kill_switch=kill_switch,
            reject_count=reject_count,
            logs=logs,
        )

    def _ring_lag(self) -> float:
        lag_attr = getattr(self.reader, "lag", None)
        if callable(lag_attr):
            try:
                return float(lag_attr())
            except Exception:  # pragma: no cover
                return 0.0
        write_cursor = getattr(self.reader, "write_cursor", None)
        read_cursor = getattr(self.reader, "read_cursor", None)
        if isinstance(write_cursor, (int, float)) and isinstance(read_cursor, (int, float)):
            return float(write_cursor) - float(read_cursor)
        return 0.0
