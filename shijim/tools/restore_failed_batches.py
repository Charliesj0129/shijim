"""Replay ClickHouse fallback JSONL batches into the analytical store."""

from __future__ import annotations

import argparse
import importlib
import logging
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Sequence
from urllib.parse import parse_qsl, unquote, urlsplit

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
    archive_dir: Path | None = None,
) -> RestoreStats:
    """Core restore routine shared by CLI + tests."""
    if not fallback_dir.exists():
        raise FileNotFoundError(f"Fallback directory {fallback_dir} does not exist.")
    if mode not in {"dry-run", "apply"}:
        raise ValueError(f"Unsupported mode {mode}.")
    if mode == "apply" and writer is None:
        raise ValueError("Apply mode requires a ClickHouseWriter instance.")

    archive_root = Path(archive_dir) if archive_dir else None
    if archive_root is not None:
        archive_root.mkdir(parents=True, exist_ok=True)

    stats = RestoreStats()

    files = list(_iter_fallback_files(fallback_dir))
    if not files:
        logger.info("No fallback files found under %s.", fallback_dir)
        return stats

    for kind, path in files:
        stats.files_processed += 1
        logger.info("Processing fallback file %s (%s).", path, kind)
        tick_batch: list[MDTickEvent] = []
        book_batch: list[MDBookEvent] = []

        def flush_tick_batch() -> None:
            if not tick_batch:
                return
            inserted_ticks, _ = _apply_batch(writer, tick_batch, [])
            stats.applied_ticks += inserted_ticks
            tick_batch.clear()

        def flush_book_batch() -> None:
            if not book_batch:
                return
            _, inserted_books = _apply_batch(writer, [], book_batch)
            stats.applied_books += inserted_books
            book_batch.clear()

        try:
            for event in _stream_events(path, stats):
                if isinstance(event, MDTickEvent):
                    stats.tick_events += 1
                    if mode == "apply":
                        tick_batch.append(event)
                        if len(tick_batch) >= batch_size:
                            flush_tick_batch()
                elif isinstance(event, MDBookEvent):
                    stats.book_events += 1
                    if mode == "apply":
                        book_batch.append(event)
                        if len(book_batch) >= batch_size:
                            flush_book_batch()
            if mode == "apply":
                flush_tick_batch()
                flush_book_batch()
        except Exception:
            logger.error("Failed to process fallback file %s.", path, exc_info=True)
            raise
        else:
            if mode == "apply" and archive_root is not None:
                _archive_file(path, fallback_dir, archive_root)

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
                logger.error(
                    "Failed to parse %s line %s: %s", path, lineno, exc, exc_info=True
                )
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


def _create_client_from_dsn(dsn: str):
    try:
        driver = importlib.import_module("clickhouse_driver")
        Client = getattr(driver, "Client")
    except Exception as exc:  # pragma: no cover - import failure
        raise RuntimeError(
            "ClickHouse support requires 'pip install shijim[clickhouse]'."
        ) from exc

    parts = urlsplit(dsn)
    if parts.scheme not in {"clickhouse", "clickhouses"}:
        raise ValueError(f"Unsupported DSN scheme '{parts.scheme}'.")
    kwargs: dict[str, str | int | bool] = {}
    if parts.hostname:
        kwargs["host"] = parts.hostname
    if parts.port:
        kwargs["port"] = parts.port
    if parts.username:
        kwargs["user"] = unquote(parts.username)
    if parts.password:
        kwargs["password"] = unquote(parts.password)
    if parts.path and parts.path != "/":
        kwargs["database"] = parts.path.lstrip("/")
    if parts.query:
        for key, value in parse_qsl(parts.query, keep_blank_values=True):
            kwargs[key] = value
    if parts.scheme == "clickhouses":
        kwargs["secure"] = True
    return Client(**kwargs)


def _resolve_client(dsn: str | None, client_factory: str | None):
    if client_factory and dsn:
        raise ValueError("Provide either --client-factory or --dsn, not both.")
    if client_factory:
        return _load_client(client_factory)
    if dsn:
        return _create_client_from_dsn(dsn)
    raise ValueError("Either --dsn or --client-factory must be provided in apply mode.")


def _build_writer(dsn: str | None, client_factory: str | None) -> ClickHouseWriter:
    client = _resolve_client(dsn, client_factory)
    writer_dsn = dsn or "clickhouse://client-factory"
    return ClickHouseWriter(dsn=writer_dsn, client=client, fallback_dir=None)


def _archive_file(path: Path, fallback_dir: Path, archive_dir: Path) -> None:
    try:
        relative = path.relative_to(fallback_dir)
    except ValueError:  # pragma: no cover - should not happen
        relative = path.name
    target = archive_dir / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(path), target)
    logger.info("Archived fallback file %s -> %s", path, target)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Replay fallback JSONL batches into ClickHouse.")
    parser.add_argument(
        "--fallback-dir",
        required=True,
        help=(
            "Directory containing fallback JSONL files produced by ClickHouseWriter "
            "(e.g., raw/fallback)."
        ),
    )
    parser.add_argument(
        "--mode",
        choices=("dry-run", "apply"),
        default="dry-run",
        help=(
            "dry-run parses and counts events without inserts; "
            "apply writes batches into ClickHouse."
        ),
    )
    parser.add_argument(
        "--dsn",
        help=(
            "ClickHouse DSN (clickhouse://user:pass@host:port/db) used to build a client "
            "when --client-factory is absent (requires shijim[clickhouse])."
        ),
    )
    parser.add_argument(
        "--client-factory",
        help=(
            "module:function that returns a ClickHouse client for apply mode "
            "(takes precedence over --dsn)."
        ),
    )
    parser.add_argument(
        "--archive-dir",
        help=(
            "Optional directory where successfully processed JSONL files are moved "
            "to avoid replays."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1_000,
        help="Maximum rows per ClickHouse insert during apply mode.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level for the restore run (e.g., INFO, DEBUG).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(), format="%(asctime)s %(levelname)s %(message)s"
    )

    fallback_dir = Path(args.fallback_dir)
    archive_dir = Path(args.archive_dir) if args.archive_dir else None
    writer: ClickHouseWriter | None = None
    if args.mode == "apply":
        writer = _build_writer(args.dsn, args.client_factory)

    try:
        stats = run_restore(
            fallback_dir=fallback_dir,
            mode=args.mode,
            writer=writer,
            batch_size=max(1, args.batch_size),
            archive_dir=archive_dir,
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
        logger.info(
            "Applied %s tick rows and %s book rows.",
            stats.applied_ticks,
            stats.applied_books,
        )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
