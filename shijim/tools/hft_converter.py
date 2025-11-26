from __future__ import annotations

import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, List, Sequence

import numpy as np

from shijim.backtest.converter import SbeEvent, save_events_as_npz

NORMAL_EVENT_FLAG = 1
GAP_FLAG = 2


@dataclass
class RawEvent:
    """Represents a normalized market-data event ahead of conversion."""

    exchange_ts: int
    receive_ts: int
    event_type: int
    side: int
    price: float
    qty: float
    order_id: str
    seq_id: int


class BaseEventSource:
    """Abstract base class for replay sources used by HftBacktestConverter."""

    source_type: str = "generic"

    def iter_events(self, symbol: str, trading_day: str) -> Iterable[RawEvent]:  # pragma: no cover - abstract
        raise NotImplementedError


@dataclass
class LatencyModelConfig:
    """Describes the latency distribution parameters exported alongside data."""

    mode: str = "gaussian"
    mean_ms: float = 5.0
    std_ms: float = 1.0

    def to_dict(self) -> dict[str, float | str]:
        return {"mode": self.mode, "mean_ms": self.mean_ms, "std_ms": self.std_ms}


@dataclass
class HftBacktestConverter:
    """Converts RawEvent streams into numpy datasets for downstream simulators."""

    event_source: BaseEventSource
    latency_model: LatencyModelConfig = field(default_factory=LatencyModelConfig)
    min_latency_ns: int = 0

    def convert(self, symbol: str, trading_day: str, output_dir: str | Path) -> dict[str, str]:
        """Convert events for a symbol/day to structured npz + metadata files."""
        output_base = Path(output_dir)
        output_base.mkdir(parents=True, exist_ok=True)
        events = list(self.event_source.iter_events(symbol, trading_day))
        records, stats = self._build_records(events)

        arrays = self._records_to_arrays(records)
        npz_path = output_base / f"{symbol}_{trading_day}.npz"
        np.savez(npz_path, **arrays)

        meta = {"symbol": symbol, "trading_day": trading_day, "stats": stats}
        meta_path = output_base / f"{symbol}_{trading_day}_meta.json"
        meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")

        latency_path = output_base / f"{symbol}_{trading_day}_latency_model.json"
        latency_path.write_text(json.dumps(self.latency_model.to_dict(), indent=2), encoding="utf-8")

        return {"npz": str(npz_path), "meta": str(meta_path), "latency_model": str(latency_path)}

    # ------------------------------------------------------------------ #
    def _build_records(self, events: Sequence[RawEvent]) -> tuple[list[dict[str, float | int]], dict[str, int]]:
        records: list[dict[str, float | int]] = []
        stats = {"negative_latency": 0, "gap_events": 0}
        prev_seq: int | None = None

        for event in events:
            if prev_seq is not None:
                gap = event.seq_id - prev_seq - 1
                if gap > 0:
                    records.append(
                        {
                            "event_flag": GAP_FLAG,
                            "exchange_ts": event.exchange_ts,
                            "receive_ts": event.receive_ts,
                            "latency_ns": 0,
                            "price": event.price,
                            "qty": float(gap),
                            "side": 0,
                            "seq_id": prev_seq,
                        }
                    )
                    stats["gap_events"] += 1
            latency = event.receive_ts - event.exchange_ts
            if latency < 0:
                stats["negative_latency"] += 1
            latency = max(latency, self.min_latency_ns)
            records.append(
                {
                    "event_flag": NORMAL_EVENT_FLAG,
                    "exchange_ts": event.exchange_ts,
                    "receive_ts": event.receive_ts,
                    "latency_ns": latency,
                    "price": event.price,
                    "qty": event.qty,
                    "side": event.side,
                    "seq_id": event.seq_id,
                }
            )
            prev_seq = event.seq_id

        return records, stats

    def _records_to_arrays(self, records: Sequence[dict[str, float | int]]) -> dict[str, np.ndarray]:
        if not records:
            return {
                "event_flag": np.array([], dtype=np.int32),
                "exchange_ts": np.array([], dtype=np.int64),
                "receive_ts": np.array([], dtype=np.int64),
                "latency_ns": np.array([], dtype=np.int64),
                "price": np.array([], dtype=np.float64),
                "qty": np.array([], dtype=np.float64),
                "side": np.array([], dtype=np.int16),
                "seq_id": np.array([], dtype=np.int64),
            }

        def to_array(key: str, dtype) -> np.ndarray:  # noqa: ANN001
            return np.asarray([record[key] for record in records], dtype=dtype)

        return {
            "event_flag": to_array("event_flag", np.int32),
            "exchange_ts": to_array("exchange_ts", np.int64),
            "receive_ts": to_array("receive_ts", np.int64),
            "latency_ns": to_array("latency_ns", np.int64),
            "price": to_array("price", np.float64),
            "qty": to_array("qty", np.float64),
            "side": to_array("side", np.int16),
            "seq_id": to_array("seq_id", np.int64),
        }


# --------------------------------------------------------------------------- #
# Legacy CLI utilities for jsonl -> NPZ conversion
# --------------------------------------------------------------------------- #
from concurrent.futures import ProcessPoolExecutor
import os

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert SBE json lines to hftbacktest npz")
    parser.add_argument("input", help="Input jsonl file or directory")
    parser.add_argument("output", help="Output npz path or directory")
    parser.add_argument("--workers", type=int, default=1, help="Number of parallel workers")
    return parser.parse_args(argv)


def load_events(path: str | Path) -> list[SbeEvent]:
    events: list[SbeEvent] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            try:
                data = json.loads(line)
                events.append(
                    SbeEvent(
                        event_id=int(data["event_id"]),
                        exch_ts=int(data["exch_ts"]),
                        local_ts=int(data["local_ts"]),
                        side=int(data["side"]),
                        price=float(data["price"]),
                        quantity=float(data["quantity"]),
                    )
                )
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
    return events


def convert_file(input_path: Path, output_path: Path) -> Path:
    print(f"Converting {input_path} -> {output_path}")
    events = load_events(input_path)
    return save_events_as_npz(events, str(output_path))


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    input_path = Path(args.input)
    output_path = Path(args.output)
    
    tasks = []
    
    if input_path.is_dir():
        output_path.mkdir(parents=True, exist_ok=True)
        for file in input_path.glob("*.jsonl"):
            # Output filename: same basename but .npz
            out_file = output_path / file.with_suffix(".npz").name
            tasks.append((file, out_file))
    else:
        # Single file
        if output_path.is_dir():
             output_path = output_path / input_path.with_suffix(".npz").name
        tasks.append((input_path, output_path))
        
    if not tasks:
        print("No input files found.")
        return

    if args.workers > 1 and len(tasks) > 1:
        print(f"Processing {len(tasks)} files with {args.workers} workers...")
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            futures = [executor.submit(convert_file, inp, out) for inp, out in tasks]
            for f in futures:
                f.result()
    else:
        print(f"Processing {len(tasks)} files sequentially...")
        for inp, out in tasks:
            convert_file(inp, out)

if __name__ == "__main__":
    main()
