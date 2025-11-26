from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from queue import SimpleQueue
from typing import Callable, List, Optional

try:  # pragma: no cover - textual is optional in CI
    from textual.app import App
    from textual.widgets import Static

    TEXTUAL_AVAILABLE = True
except Exception:  # pragma: no cover
    TEXTUAL_AVAILABLE = False
    App = object  # type: ignore
    Static = object  # type: ignore


@dataclass
class SystemSnapshot:
    timestamp: float
    ingestion_lag: float
    bid: float
    ask: float
    last_price: float
    ofi: float
    strategy_state: str
    active_orders: List[str] = field(default_factory=list)
    position: float = 0.0
    kill_switch: bool = False
    reject_count: int = 0
    logs: List[str] = field(default_factory=list)


class SnapshotFormatter:
    def render(self, snapshot: SystemSnapshot) -> str:
        ofi_color = "green" if snapshot.ofi >= 0 else "red"
        kill_text = "STOPPED" if snapshot.kill_switch else "ACTIVE"
        return (
            f"[Market] Price={snapshot.last_price:.2f} "
            f"Bid={snapshot.bid:.2f} Ask={snapshot.ask:.2f} "
            f"[Signal] OFI={snapshot.ofi:+.2f} ({ofi_color})\n"
            f"[System] Lag={snapshot.ingestion_lag:.2f} "
            f"Rejects={snapshot.reject_count} Kill={kill_text}\n"
            f"[Strategy] State={snapshot.strategy_state} "
            f"Position={snapshot.position} Orders={snapshot.active_orders}\n"
            f"[Logs] {' | '.join(snapshot.logs[-5:])}"
        )


class DashboardApp(App):  # pragma: no cover - heavy UI
    CSS_PATH = None

    def __init__(self, queue: SimpleQueue, kill_switch_callback: Optional[Callable[[], None]] = None):
        super().__init__()
        self.queue = queue
        self.kill_switch_callback = kill_switch_callback
        self.formatter = SnapshotFormatter()
        self.output = Static("") if TEXTUAL_AVAILABLE else None

    def compose(self):
        if TEXTUAL_AVAILABLE:
            yield self.output  # type: ignore[misc]

    def on_mount(self):
        if not TEXTUAL_AVAILABLE:
            return
        self.set_interval(0.1, self.refresh_snapshot)

    def refresh_snapshot(self):
        if not TEXTUAL_AVAILABLE:
            return
        if self.queue.empty():
            return
        snapshot = self.queue.get()
        if isinstance(snapshot, SystemSnapshot):
            self.output.update(self.formatter.render(snapshot))  # type: ignore[union-attr]

    def action_kill(self):
        if self.kill_switch_callback:
            self.kill_switch_callback()

    def run_headless_once(self) -> Optional[str]:
        if self.queue.empty():
            return None
        snapshot = self.queue.get()
        if isinstance(snapshot, SystemSnapshot):
            return self.formatter.render(snapshot)
        if isinstance(snapshot, dict):
            return json.dumps(snapshot)
        return str(snapshot)
