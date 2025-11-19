# Shijim

Shijim is a Shioaji-based market data gateway that targets three core goals:

1. **High-quality market data ingestion** – subscribe to stocks/futures (tick + L2) via Shioaji.
2. **Durable recording pipeline** – append-only raw archives plus analytical backends (e.g., ClickHouse).
3. **CTA/backtest sandbox** – downstream engines consume the same broker-neutral events for replay and strategy R&D.

## Getting Started

- Install the core package (gateway + recorder) with minimal dependencies:

```bash
pip install shijim
```

- Optionally add ClickHouse tooling (writer + restore CLI) via the extra:

```bash
pip install shijim[clickhouse]
```

- Export `SHIOAJI_API_KEY` / `SHIOAJI_SECRET_KEY` and optional `SHIOAJI_SIMULATION` env vars.
- Run the CLI:

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

Example apply mode using a DSN (requires `pip install shijim[clickhouse]`):

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

- **ClickHouse outage during trading hours:** RawWriter continues writing, and ClickHouseWriter falls back to JSONL. Monitor logs for "Fallback file ... appended ..." messages to confirm fallback is active.
- **Check for backlog:** List the `fallback_dir` (e.g. `ls raw/fallback/ticks`). Non-empty folders indicate batches waiting to be replayed.
- **Restore yesterday’s failures:** Run the restore CLI in `dry-run` first to verify counts, then rerun with `--mode apply` plus either `--client-factory` or `--dsn`, optionally with `--archive-dir` to relocate processed files.
- **Avoid double inserts:** After a successful apply, confirm the archive directory contains the moved files (or manually rename/delete them). If duplicates slipped through, rely on your ClickHouse table’s deduplication strategy or manually clean the affected partitions.

## Docker / Cloud Deployment

Build a container image straight from the repo:

```bash
docker build -t shijim:latest .
```

Run it with durable volumes for raw data and fallback batches:

```bash
docker run --rm \
  -e SHIOAJI_API_KEY=xxx \
  -e SHIOAJI_SECRET_KEY=yyy \
  -v /var/lib/shijim/raw:/data/raw \
  -v /var/lib/shijim/fallback:/data/fallback \
  shijim:latest \
  python -m shijim
```

### Persistent fallback storage

- Map `fallback_dir` (and any raw archive directory) to persistent storage.
  - Kubernetes: mount a PersistentVolumeClaim (PVC) or network filesystem (EFS, Filestore) at `/var/lib/shijim/fallback`.
  - ECS/Fargate: attach an ephemeral volume that is backed by EFS or bind a host path to the container.
- If the fallback directory lives on the container’s writable layer, a restart will delete un-restored JSONL files.

### Restoring from fallback in containers

You can run the restore CLI from the same image as an ad-hoc Kubernetes Job / ECS task:

```bash
docker run --rm \
  -v /var/lib/shijim/fallback:/data/fallback \
  shijim:latest \
  python -m shijim.tools.restore_failed_batches \
    --fallback-dir /data/fallback \
    --mode apply \
    --dsn "clickhouse://user:pass@host:9000/dbname" \
    --archive-dir /data/fallback/archive
```

- Each restore run should either:
  - Use ClickHouse table engines (ReplacingMergeTree, primary keys) that tolerate duplicates, or
  - Move processed JSONL files to an archive directory (`--archive-dir`) so they cannot be replayed twice.
- In Kubernetes, run the restore job as a CronJob/Job that mounts the same PVC as the recorder.
- In ECS/Fargate, run a one-off task with the same task definition but override the command to the restore CLI.

Always keep an eye on the fallback directory size, as highly bursty outages can accumulate many JSONL files. In cloud deployments, monitor the dedicated volume (PVC/EFS) for capacity and IOPS.
