from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

import numpy as np
import orjson

logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
# Data models
# --------------------------------------------------------------------------- #


@dataclass(slots=True)
class RawEvent:
    """Internal representation of a raw MBO/L5 event."""

    exchange_ts: int
    receive_ts: int
    event_type: int
    side: int
    price: float
    qty: float
    order_id: str
    seq_id: int | None = None


@dataclass(slots=True)
class LatencyModelConfig:
    """Config describing order latency modelling."""

    mode: str = "fixed"  # fixed | uniform | normal | lognormal | dynamic
    mean_ms: float = 0.0
    std_ms: float = 0.0
    min_ms: float = 0.0
    max_ms: float = 0.0
    alpha: float = 0.0  # coupling to feed latency spikes

    def to_dict(self) -> dict[str, Any]:
        return {
            "mode": self.mode,
            "mean_ms": self.mean_ms,
            "std_ms": self.std_ms,
            "min_ms": self.min_ms,
            "max_ms": self.max_ms,
            "alpha": self.alpha,
        }


# --------------------------------------------------------------------------- #
# Sources
# --------------------------------------------------------------------------- #


class BaseEventSource:
    """Abstract reader for raw events."""

    source_type: str = "unknown"

    def iter_events(self, symbol: str, trading_day: str) -> Iterator[RawEvent]:  # pragma: no cover - interface
        raise NotImplementedError


class JsonlEventSource(BaseEventSource):
    """Reads events from a JSONL file or directory."""

    source_type = "jsonl"

    def __init__(self, path: str | Path):
        self.path = Path(path)

    def _resolve_file(self, symbol: str, trading_day: str) -> Path:
        if self.path.is_file():
            return self.path
        pattern = f"{symbol}_{trading_day}.jsonl"
        candidate = self.path / pattern
        if candidate.exists():
            return candidate
        raise FileNotFoundError(f"No JSONL file found for {symbol} {trading_day} at {self.path}")

    def iter_events(self, symbol: str, trading_day: str) -> Iterator[RawEvent]:
        file_path = self._resolve_file(symbol, trading_day)
        with file_path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    payload = orjson.loads(line)
                    yield RawEvent(
                        exchange_ts=int(payload["exchange_ts"]),
                        receive_ts=int(payload.get("receive_ts", payload.get("ts", 0))),
                        event_type=int(payload["event_type"]),
                        side=int(payload.get("side", 0) or 0),
                        price=float(payload.get("price", 0.0)),
                        qty=float(payload.get("qty", 0.0)),
                        order_id=str(payload.get("order_id", "")),
                        seq_id=int(payload["seq_id"]) if "seq_id" in payload else None,
                    )
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Skipping malformed JSONL line: %s (error=%s)", line[:128], exc)


class ClickHouseEventSource(BaseEventSource):
    """Reads events from ClickHouse via clickhouse-driver."""

    source_type = "clickhouse"

    def __init__(self, dsn: str, table: str = "mbo_events", client: Any | None = None):
        self.table = table
        if client is None:
            try:
                from clickhouse_driver import Client  # type: ignore
            except Exception as exc:  # pragma: no cover - optional
                raise RuntimeError("clickhouse-driver is required for ClickHouseEventSource") from exc
            client = Client.from_url(dsn)
        self.client = client

    def iter_events(self, symbol: str, trading_day: str) -> Iterator[RawEvent]:
        sql = f"""
            SELECT
                exchange_ts,
                receive_ts,
                event_type,
                side,
                price,
                qty,
                order_id,
                seq_id
            FROM {self.table}
            WHERE symbol = %(symbol)s AND trading_day = %(trading_day)s
            ORDER BY exchange_ts, seq_id
        """
        params = {"symbol": symbol, "trading_day": trading_day}
        rows = self.client.execute(sql, params)
        for row in rows:
            yield RawEvent(
                exchange_ts=int(row[0]),
                receive_ts=int(row[1]),
                event_type=int(row[2]),
                side=int(row[3]),
                price=float(row[4]),
                qty=float(row[5]),
                order_id=str(row[6]),
                seq_id=int(row[7]) if row[7] is not None else None,
            )


# --------------------------------------------------------------------------- #
# Converter
# --------------------------------------------------------------------------- #


EVENT_FLAG_MAP = {
    0: 1,  # Add -> EXCH_ADD_ORDER
    1: 2,  # Cancel -> EXCH_CANC_ORDER
    2: 3,  # Trade/Match -> EXCH_MATCH
}
GAP_FLAG = 9  # synthetic marker for gaps / reload


@dataclass
class ConversionStats:
    total: int = 0
    gaps: int = 0
    negative_latency: int = 0
    clamped_latency_ns: int = 0


@dataclass
class HftBacktestConverter:
    source: BaseEventSource
    latency_model: LatencyModelConfig
    min_latency_ns: int = 1_000_000  # 1ms clamp for skew
    output_compressed: bool = True
    random_seed: int | None = None
    stats: ConversionStats = field(default_factory=ConversionStats, init=False)

    def convert(self, symbol: str, trading_day: str, output_dir: str | Path) -> dict[str, Path]:
        events = list(self._sorted_events(symbol, trading_day))
        arrays = self._build_arrays(events)

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        base_name = f"hft_{symbol}_{trading_day}_{self.source.source_type}"
        npz_path = output_dir / f"{base_name}.npz"
        meta_path = output_dir / f"{base_name}.meta.json"
        latency_path = output_dir / f"{base_name}.latency_model.json"

        np.savez_compressed(npz_path, **arrays)
        self._write_json(meta_path, self._metadata(symbol, trading_day))
        self._write_json(latency_path, self.latency_model.to_dict())

        logger.info(
            "Converted %s events for %s %s (gaps=%s, negative_latency=%s) -> %s",
            self.stats.total,
            symbol,
            trading_day,
            self.stats.gaps,
            self.stats.negative_latency,
            npz_path,
        )
        return {"npz": npz_path, "meta": meta_path, "latency_model": latency_path}

    # ------------------------------------------------------------------ #
    # Internal
    # ------------------------------------------------------------------ #
    def _sorted_events(self, symbol: str, trading_day: str) -> Iterator[RawEvent]:
        raw = list(self.source.iter_events(symbol, trading_day))
        raw.sort(key=lambda evt: (evt.exchange_ts, evt.seq_id or 0))
        last_seq: int | None = None
        for evt in raw:
            if evt.seq_id is not None and last_seq is not None and evt.seq_id - last_seq > 1:
                gap_size = evt.seq_id - last_seq - 1
                self.stats.gaps += 1
                yield RawEvent(
                    exchange_ts=evt.exchange_ts,
                    receive_ts=evt.receive_ts,
                    event_type=GAP_FLAG,
                    side=0,
                    price=0.0,
                    qty=float(gap_size),
                    order_id="GAP",
                    seq_id=evt.seq_id,
                )
            last_seq = evt.seq_id if evt.seq_id is not None else last_seq
            yield evt

    def _build_arrays(self, events: Sequence[RawEvent]) -> dict[str, np.ndarray]:
        n = len(events)
        flags = np.zeros(n, dtype=np.int8)
        exchange_ts = np.zeros(n, dtype=np.int64)
        receive_ts = np.zeros(n, dtype=np.int64)
        latency_ns = np.zeros(n, dtype=np.int64)
        price = np.zeros(n, dtype=np.float64)
        qty = np.zeros(n, dtype=np.float64)
        side = np.zeros(n, dtype=np.int8)
        order_id = np.zeros(n, dtype=np.uint64)
        seq_id = np.zeros(n, dtype=np.int64)

        for idx, evt in enumerate(events):
            flags[idx] = EVENT_FLAG_MAP.get(evt.event_type, GAP_FLAG if evt.event_type == GAP_FLAG else 0)
            exchange_ts[idx] = evt.exchange_ts
            receive_ts[idx] = evt.receive_ts
            computed_latency = evt.receive_ts - evt.exchange_ts
            if computed_latency < 0:
                self.stats.negative_latency += 1
                latency_ns[idx] = self.min_latency_ns
                self.stats.clamped_latency_ns += abs(computed_latency)
            else:
                latency_ns[idx] = max(computed_latency, self.min_latency_ns)
            price[idx] = evt.price
            qty[idx] = evt.qty
            side[idx] = int(evt.side or 0)
            order_id[idx] = self._order_id_to_uint64(evt.order_id)
            seq_id[idx] = int(evt.seq_id or 0)
        self.stats.total = n
        return {
            "event_flag": flags,
            "exchange_ts": exchange_ts,
            "receive_ts": receive_ts,
            "latency_ns": latency_ns,
            "price": price,
            "qty": qty,
            "side": side,
            "order_id": order_id,
            "seq_id": seq_id,
        }

    def _order_id_to_uint64(self, value: str) -> np.uint64:
        if not value:
            return np.uint64(0)
        digest = hashlib.sha1(value.encode("utf-8")).digest()
        return np.frombuffer(digest[:8], dtype=np.uint64)[0]

    def _metadata(self, symbol: str, trading_day: str) -> dict[str, Any]:
        src_ref = os.getenv("GIT_COMMIT") or os.getenv("SOURCE_REF") or "unknown"
        return {
            "symbol": symbol,
            "trading_day": trading_day,
            "source_type": self.source.source_type,
            "source_ref": src_ref,
            "navigator_version": os.getenv("NAVIGATOR_VERSION", ""),
            "stats": {
                "total": self.stats.total,
                "gaps": self.stats.gaps,
                "negative_latency": self.stats.negative_latency,
                "clamped_latency_ns": self.stats.clamped_latency_ns,
            },
        }

    def _write_json(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert Shijim MBO/L5 data to HftBacktest NPZ.")
    parser.add_argument("--source-type", choices=["clickhouse", "jsonl"], required=True)
    parser.add_argument("--source", required=True, help="ClickHouse DSN or JSONL path")
    parser.add_argument("--table", default="mbo_events", help="ClickHouse table (for clickhouse source)")
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--trading-day", required=True, help="YYYY-MM-DD")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--latency-mode", default="fixed", choices=["fixed", "uniform", "normal", "lognormal", "dynamic"])
    parser.add_argument("--latency-mean-ms", type=float, default=0.0)
    parser.add_argument("--latency-std-ms", type=float, default=0.0)
    parser.add_argument("--latency-min-ms", type=float, default=0.0)
    parser.add_argument("--latency-max-ms", type=float, default=0.0)
    parser.add_argument("--latency-alpha", type=float, default=0.0, help="Coupling to feed latency for dynamic model")
    parser.add_argument("--min-latency-ns", type=int, default=1_000_000, help="Minimum feed latency clamp (ns)")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")

    if args.source_type == "clickhouse":
        source: BaseEventSource = ClickHouseEventSource(dsn=args.source, table=args.table)
    else:
        source = JsonlEventSource(path=args.source)

    latency_model = LatencyModelConfig(
        mode=args.latency_mode,
        mean_ms=args.latency_mean_ms,
        std_ms=args.latency_std_ms,
        min_ms=args.latency_min_ms,
        max_ms=args.latency_max_ms,
        alpha=args.latency_alpha,
    )
    converter = HftBacktestConverter(
        source=source,
        latency_model=latency_model,
        min_latency_ns=args.min_latency_ns,
    )
    converter.convert(args.symbol, args.trading_day, args.output_dir)
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
