# Shijim

Shijim is a Shioaji-based market data gateway that targets three core goals:

1. **High-quality market data ingestion** – subscribe to stocks/futures (tick + L2) via Shioaji.
2. **Durable recording pipeline** – append-only raw archives plus analytical backends (e.g., ClickHouse).
3. **CTA/backtest sandbox** – downstream engines consume the same broker-neutral events for replay and strategy R&D.

## Getting Started

1. Install dependencies into a virtual environment (placeholders are defined in `pyproject.toml`).  
2. Export `SHIOAJI_API_KEY` / `SHIOAJI_SECRET_KEY` and optional `SHIOAJI_SIMULATION` env vars.
3. Run the CLI:

```bash
python -m shijim
```

This boots a minimal Shioaji session, wires callbacks into the in-process event bus, and streams normalized events into the recorder writers. Press `Ctrl+C` to stop.

## Project Layout

- `shijim/gateway/`: Shioaji session management, subscriptions, callbacks, and historical replay.
- `shijim/bus/`: Simple queue-based PUB/SUB primitives.
- `shijim/events/`: Broker-neutral event schema + normalization helpers.
- `shijim/recorder/`: Ingestion workers and raw/ClickHouse writers.
- `tests/`: Pytest suite covering the core components.

The GitHub Actions workflow (`.github/workflows/main.yml`) runs `pytest` to keep the package healthy.

## Fallback & Recovery Lifecycle

Shijim’s recorder writes every event twice: once to the append-only `RawWriter` (ground truth) and once to the analytical store (ClickHouse). If ClickHouse becomes slow or unavailable, the `ClickHouseWriter` stops clearing its buffers and persists every failed batch as JSONL under a configured `fallback_dir` (e.g. `raw/fallback/ticks/1970-01-01.jsonl`). These JSONL files are the at-least-once safety net and are meant to be re-ingested later.

The recovery script `python -m shijim.tools.restore_failed_batches` streams these JSONL lines back into ClickHouse using the same normalization logic as the live pipeline. It:

- Defaults to `--mode dry-run`, which parses and counts events without writing to ClickHouse.
- Accepts `--mode apply` to insert the events in batches via `ClickHouseWriter.insert_events(...)`.
- Can use either a custom `--client-factory "module:function"` (the callable must return a ClickHouse client instance) or a DSN such as `--dsn "clickhouse://user:pass@host:9000/db"`.
- Requires a `--fallback-dir` pointing at the JSONL root (usually the same path you configured for `ClickHouseWriter.fallback_dir`).
- Supports `--archive-dir /path/to/archive` to automatically move each processed JSONL file after a successful replay, preventing accidental replays.

Example dry-run:

```bash
python -m shijim.tools.restore_failed_batches \
  --fallback-dir raw/fallback \
  --mode dry-run
```

Example apply mode using a client factory:

```bash
python -m shijim.tools.restore_failed_batches \
  --fallback-dir raw/fallback \
  --mode apply \
  --client-factory "shijim.clickhouse:make_client" \
  --batch-size 500 \
  --archive-dir raw/fallback/archive
```

Example apply mode using a DSN (requires `clickhouse-driver`):

```bash
python -m shijim.tools.restore_failed_batches \
  --fallback-dir raw/fallback \
  --mode apply \
  --dsn "clickhouse://user:pass@host:9000/dbname" \
  --archive-dir raw/fallback/archive
```

### Duplication / idempotency warning

The restore tool simply issues `INSERT` statements. Depending on your ClickHouse engine and primary key, reprocessing the same JSONL files may duplicate rows. Mitigate this by:

- Using ReplacingMergeTree or a primary-key schema that deduplicates on ingestion, **and/or**
- Enabling `--archive-dir` so processed files are automatically moved away after a successful run, **and/or**
- Manually archiving or deleting replayed JSONL files once they are safely written.

## Operations Guide

- **ClickHouse outage during trading hours:** RawWriter continues writing, and ClickHouseWriter falls back to JSONL. Monitor logs for “Fallback file … appended…” messages to confirm fallback is active.
- **Check for backlog:** List the `fallback_dir` (e.g. `ls raw/fallback/ticks`). Non-empty folders indicate batches waiting to be replayed.
- **Restore yesterday’s failures:** Run the restore CLI in `dry-run` first to verify counts, then rerun with `--mode apply` plus either `--client-factory` or `--dsn`, optionally with `--archive-dir` to relocate processed files.
- **Avoid double inserts:** After a successful apply, confirm the archive directory contains the moved files (or manually rename/delete them). If duplicates slipped through, rely on your ClickHouse table’s deduplication strategy or manually clean the affected partitions.
