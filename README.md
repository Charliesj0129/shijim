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
