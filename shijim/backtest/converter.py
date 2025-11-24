from __future__ import annotations

import numpy as np
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Sequence


@dataclass
class SbeEvent:
    event_id: int
    exch_ts: int
    local_ts: int
    side: int
    price: float
    quantity: float


HBT_DTYPE = np.dtype(
    [
        ("ev", "u1"),
        ("exch_ts", "i8"),
        ("local_ts", "i8"),
        ("side", "i1"),
        ("px", "f8"),
        ("qty", "f8"),
    ]
)


def convert_events_to_np(events: Iterable[SbeEvent]) -> np.ndarray:
    event_list = list(events)
    arr = np.zeros(len(event_list), dtype=HBT_DTYPE)
    for idx, evt in enumerate(event_list):
        arr[idx] = (
            evt.event_id,
            evt.exch_ts,
            evt.local_ts,
            evt.side,
            evt.price,
            evt.quantity,
        )
    return arr


def save_events_as_npz(events: Iterable[SbeEvent], output_path: str | Path) -> Path:
    arr = convert_events_to_np(events)
    output = Path(output_path)
    np.savez_compressed(output, data=arr)
    return output
