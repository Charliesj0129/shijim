"""Historical tick replayer leveraging ``api.ticks``.

References:
    * ``sinotrade_tutor_md/market_data/historical.md`` for ``api.ticks`` usage
    * Examples under ``advanced/quote_manager_basic.md`` for converting tick data
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Iterable, List, Sequence

from shijim.events import MDTickEvent, normalize_tick

logger = logging.getLogger(__name__)

try:  # pragma: no cover - used only when shioaji is installed
    from shioaji.constant import TicksQueryType
except Exception:  # pragma: no cover - fallback for test env w/o shioaji

    class TicksQueryType:  # type: ignore[override]
        AllDay = "AllDay"


def replay_ticks(
    contract,
    start_ts: int,
    end_ts: int,
    api,
    *,
    asset_type: str | None = None,
    sink=None,
    throttle: float = 0.25,
) -> List[MDTickEvent]:
    """Fetch historical ticks and emit broker-neutral events."""
    if start_ts > end_ts:
        raise ValueError("start_ts must be <= end_ts")

    asset_type = asset_type or _infer_asset_type(contract)
    exchange = getattr(contract, "exchange", None)
    events: List[MDTickEvent] = []

    for date_str in _date_strings(start_ts, end_ts):
        try:
            ticks = api.ticks(contract=contract, date=date_str, query_type=TicksQueryType.AllDay)
        except Exception as exc:  # noqa: BLE001
            logger.warning("api.ticks failed for %s on %s: %s", contract, date_str, exc)
            continue

        converted = _ticks_to_events(
            contract=contract,
            ticks=ticks,
            asset_type=asset_type,
            exchange=exchange,
            start_ts=start_ts,
            end_ts=end_ts,
        )

        for event in converted:
            events.append(event)
            if sink:
                sink.append(event)

        if throttle > 0:
            time.sleep(throttle)

    return events


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def _date_strings(start_ts: int, end_ts: int) -> Iterable[str]:
    start_dt = datetime.fromtimestamp(start_ts / 1_000_000_000, tz=timezone.utc).date()
    end_dt = datetime.fromtimestamp(end_ts / 1_000_000_000, tz=timezone.utc).date()
    delta = timedelta(days=1)
    current = start_dt
    while current <= end_dt:
        yield current.isoformat()
        current += delta


def _ticks_to_events(
    *,
    contract,
    ticks,
    asset_type: str,
    exchange,
    start_ts: int,
    end_ts: int,
) -> List[MDTickEvent]:
    if ticks is None:
        return []

    ts_list = getattr(ticks, "ts", []) or []
    close_list = getattr(ticks, "close", []) or []
    volume_list = getattr(ticks, "volume", []) or []
    bid_price_list = getattr(ticks, "bid_price", []) or []
    bid_volume_list = getattr(ticks, "bid_volume", []) or []
    ask_price_list = getattr(ticks, "ask_price", []) or []
    ask_volume_list = getattr(ticks, "ask_volume", []) or []
    tick_type_list = getattr(ticks, "tick_type", []) or []

    count = len(ts_list)
    events: List[MDTickEvent] = []
    symbol = getattr(contract, "code", getattr(contract, "symbol", ""))

    for idx in range(count):
        ts_ns = ts_list[idx]
        if ts_ns is None:
            continue
        if ts_ns < start_ts or ts_ns > end_ts:
            continue

        tick_obj = SimpleNamespace(
            code=symbol,
            datetime=_ns_to_datetime(ts_ns),
            close=_safe_get(close_list, idx),
            volume=_safe_get(volume_list, idx),
            bid_price=_safe_get(bid_price_list, idx),
            bid_volume=_safe_get(bid_volume_list, idx),
            ask_price=_safe_get(ask_price_list, idx),
            ask_volume=_safe_get(ask_volume_list, idx),
            tick_type=_safe_get(tick_type_list, idx),
        )
        event = normalize_tick(tick_obj, asset_type=asset_type, exchange=exchange)
        events.append(event)

    return events


def _ns_to_datetime(ts_ns: int) -> datetime:
    return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)


def _safe_get(seq: Sequence, idx: int):
    try:
        return seq[idx]
    except Exception:
        return None


def _infer_asset_type(contract) -> str:
    name = contract.__class__.__name__.lower()
    if "future" in name or "fop" in name:
        return "futures"
    if "option" in name:
        return "options"
    return "stock"
