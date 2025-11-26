.PHONY: install test lint build clean

install:
	pip install -e ".[dev,clickhouse,backtest,dashboard]"

test:
	pytest --maxfail=1 --disable-warnings

lint:
	ruff check shijim tests

build:
	maturin develop

clean:
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf build
	rm -rf dist
	find . -type d -name "__pycache__" -exec rm -rf {} +
