"""Replay ClickHouse fallback JSONL batches into the analytical store."""

from __future__ import annotations

import argparse
import importlib
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence

import orjson

from shijim.events.schema import MDBookEvent, MDTickEvent
from shijim.recorder.clickhouse_writer import ClickHouseWriter

logger = logging.getLogger(__name__)


@dataclass
class RestoreStats:
    """Summary from a restore run."""

    files_processed: int = 0
    tick_events: int = 0
    book_events: int = 0
    skipped_lines: int = 0
    applied_ticks: int = 0
    applied_books: int = 0


def run_restore(
    *,
    fallback_dir: Path,
    mode: str,
    writer: ClickHouseWriter | None,
    batch_size: int = 1_000,
) -> RestoreStats:
    """Core restore routine shared by CLI + tests."""
    if not fallback_dir.exists():
        raise FileNotFoundError(f"Fallback directory {fallback_dir} does not exist.")
    if mode not in {"dry-run", "apply"}:
        raise ValueError(f"Unsupported mode {mode}.")
    if mode == "apply" and writer is None:
        raise ValueError("Apply mode requires a ClickHouseWriter instance.")

    stats = RestoreStats()
    tick_batch: list[MDTickEvent] = []
    book_batch: list[MDBookEvent] = []

    files = list(_iter_fallback_files(fallback_dir))
    if not files:
        logger.info("No fallback files found under %s.", fallback_dir)
        return stats

    for kind, path in files:
        stats.files_processed += 1
        logger.info("Processing fallback file %s (%s).", path, kind)
        for event in _stream_events(path, stats):
            if isinstance(event, MDTickEvent):
                stats.tick_events += 1
                if mode == "apply":
                    tick_batch.append(event)
                    if len(tick_batch) >= batch_size:
                        inserted_ticks, _ = _apply_batch(writer, tick_batch, [])
                        stats.applied_ticks += inserted_ticks
                        tick_batch.clear()
            elif isinstance(event, MDBookEvent):
                stats.book_events += 1
                if mode == "apply":
                    book_batch.append(event)
                    if len(book_batch) >= batch_size:
                        _, inserted_books = _apply_batch(writer, [], book_batch)
                        stats.applied_books += inserted_books
                        book_batch.clear()

    if mode == "apply" and (tick_batch or book_batch):
        inserted_ticks, inserted_books = _apply_batch(writer, tick_batch, book_batch)
        stats.applied_ticks += inserted_ticks
        stats.applied_books += inserted_books
        tick_batch.clear()
        book_batch.clear()

    return stats


def _iter_fallback_files(fallback_dir: Path) -> Iterator[tuple[str, Path]]:
    for kind in ("ticks", "books"):
        root = fallback_dir / kind
        if not root.exists():
            continue
        for path in sorted(root.glob("*.jsonl")):
            yield kind, path


def _stream_events(path: Path, stats: RestoreStats) -> Iterator[MDTickEvent | MDBookEvent]:
    with path.open("rb") as fh:
        for lineno, raw in enumerate(fh, 1):
            line = raw.strip()
            if not line:
                continue
            try:
                payload = orjson.loads(line)
            except orjson.JSONDecodeError as exc:
                logger.error("Failed to parse %s line %s: %s", path, lineno, exc, exc_info=True)
                stats.skipped_lines += 1
                continue
            event = _deserialize_event(payload)
            if event is None:
                stats.skipped_lines += 1
                logger.error("Unsupported event payload in %s line %s", path, lineno)
                continue
            yield event


def _deserialize_event(payload: dict) -> MDTickEvent | MDBookEvent | None:
    event_type = payload.get("type")
    data = dict(payload)
    data.pop("type", None)
    try:
        if event_type == "MD_TICK":
            return MDTickEvent(**data)
        if event_type == "MD_BOOK":
            return MDBookEvent(**data)
    except Exception:  # noqa: BLE001
        logger.exception("Failed to deserialize fallback event.")
        return None
    return None


def _apply_batch(
    writer: ClickHouseWriter | None,
    ticks: Sequence[MDTickEvent],
    books: Sequence[MDBookEvent],
) -> tuple[int, int]:
    if writer is None or (not ticks and not books):
        return 0, 0
    inserted_ticks, inserted_books = writer.insert_events(ticks, books)
    return inserted_ticks, inserted_books


def _load_client(factory_path: str | None):
    if not factory_path:
        return None
    module_name, _, attr = factory_path.partition(":")
    if not module_name or not attr:
        raise ValueError(f"Invalid client factory '{factory_path}', expected module:callable.")
    module = importlib.import_module(module_name)
    factory = getattr(module, attr)
    return factory()


def _build_writer(dsn: str, client_factory: str | None) -> ClickHouseWriter:
    client = _load_client(client_factory)
    if client is None:
        raise ValueError("Client factory must be provided for apply mode.")
    return ClickHouseWriter(dsn=dsn, client=client, fallback_dir=None)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay fallback JSONL batches into ClickHouse.")
    parser.add_argument("--fallback-dir", required=True, help="Path to ClickHouseWriter fallback directory.")
    parser.add_argument("--mode", choices=("dry-run", "apply"), default="dry-run")
    parser.add_argument("--dsn", default="clickhouse://localhost", help="ClickHouse DSN used for logging purposes.")
    parser.add_argument(
        "--client-factory",
        help="module:function that returns a ClickHouse client when mode=apply.",
    )
    parser.add_argument("--batch-size", type=int, default=1_000, help="Rows per insert in apply mode.")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(message)s")

    fallback_dir = Path(args.fallback_dir)
    writer: ClickHouseWriter | None = None
    if args.mode == "apply":
        writer = _build_writer(args.dsn, args.client_factory)

    try:
        stats = run_restore(
            fallback_dir=fallback_dir,
            mode=args.mode,
            writer=writer,
            batch_size=max(1, args.batch_size),
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("Restore failed: %s", exc, exc_info=True)
        return 1

    logger.info(
        "Processed %s files. ticks=%s, books=%s, skipped=%s",
        stats.files_processed,
        stats.tick_events,
        stats.book_events,
        stats.skipped_lines,
    )
    if args.mode == "apply":
        logger.info("Applied %s tick rows and %s book rows.", stats.applied_ticks, stats.applied_books)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
