from __future__ import annotations

import argparse
import json
from pathlib import Path

from shijim.backtest.converter import SbeEvent, save_events_as_npz


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert SBE json lines to hftbacktest npz")
    parser.add_argument("input", help="Input jsonl file with SBE fields")
    parser.add_argument("output", help="Output npz path")
    return parser.parse_args()


def load_events(path: str | Path) -> list[SbeEvent]:
    events: list[SbeEvent] = []
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
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
    return events


def main(argv: list[str] | None = None) -> Path:
    args = parse_args() if argv is None else parse_args()
    events = load_events(args.input)
    return save_events_as_npz(events, args.output)


if __name__ == "__main__":  # pragma: no cover - CLI
    main()
