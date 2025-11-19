from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class SmokeTestUniverse:
    """Small universe definition for smoke-test subscriptions."""

    futures: Sequence[str]
    stocks: Sequence[str]


def _parse_codes(value: str | None, default: Sequence[str]) -> list[str]:
    if not value:
        return list(default)
    parts = [item.strip() for item in value.split(",")]
    return [part for part in parts if part]


def get_smoke_test_universe() -> SmokeTestUniverse:
    """Return the default futures/stocks used in smoke tests.

    Override via env vars:
        * SHIJIM_SMOKE_FUTURES
        * SHIJIM_SMOKE_STOCKS
    """

    futures = _parse_codes(os.getenv("SHIJIM_SMOKE_FUTURES"), ["TXFR1"])
    stocks = _parse_codes(os.getenv("SHIJIM_SMOKE_STOCKS"), ["2330", "0050"])
    return SmokeTestUniverse(futures=futures, stocks=stocks)
