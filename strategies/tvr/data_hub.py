from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable, Mapping

from filelock import FileLock

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core.live.binance_rest_gateway import (
    REQUEST_PRIORITY_LOW,
    REQUEST_PRIORITY_NORMAL,
    call_client_method,
    call_futures_public,
)
from core.runtime_state import get_state_dir

BJ = timezone(timedelta(hours=8))
STRATEGY_NAME = "tvr"


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _now_utc_ms() -> int:
    return int(time.time() * 1000)


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")


def _bj_day_from_ms(ts_ms: int | None) -> str:
    value = int(ts_ms) if ts_ms is not None else _now_utc_ms()
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%Y-%m-%d")


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, tuple):
        return list(value)
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, default=_json_default, separators=(",", ":"))


def _append_jsonl(path: Path, record: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(path) + ".lock")
    with lock:
        with path.open("a", encoding="utf-8") as f:
            f.write(_json_dumps(dict(record)) + "\n")
            f.flush()
            os.fsync(f.fileno())
    return path


def _stream_path(stream: str, *, day_bj: str | None = None) -> Path:
    stream_key = str(stream).strip()
    if not stream_key:
        raise ValueError("stream must not be empty")
    day_key = str(day_bj or "").strip() or _bj_day_from_ms(None)
    return get_state_dir() / "live_audit" / "tvr" / "data_hub" / stream_key / day_key / f"tradfi_{stream_key}.jsonl"


def _base_record(run_id: str, event: str) -> dict[str, Any]:
    now_ms = _now_utc_ms()
    return {
        "schema_version": 1,
        "strategy_name": STRATEGY_NAME,
        "run_mode": "live",
        "run_id": str(run_id),
        "event": str(event),
        "collected_utc_ms": int(now_ms),
        "collected_bj": _fmt_bj_from_ms(now_ms),
    }


def _load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"TVR data_hub config missing: {path}")
    with p.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise TypeError(f"TVR data_hub config must be JSON object: {path}")
    return payload


def _require_mapping(cfg: Mapping[str, Any], path: str, key: str) -> dict[str, Any]:
    if key not in cfg:
        raise KeyError(f"TVR data_hub config missing required section: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, dict):
        raise TypeError(f"TVR data_hub config section must be object: {key} | {path}")
    return dict(value)


def _require_bool(cfg: Mapping[str, Any], path: str, key: str) -> bool:
    if key not in cfg:
        raise KeyError(f"TVR data_hub config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, bool):
        raise TypeError(f"TVR data_hub config field must be bool: {key} | {path}")
    return bool(value)


def _require_non_empty_str(cfg: Mapping[str, Any], path: str, key: str) -> str:
    if key not in cfg:
        raise KeyError(f"TVR data_hub config missing required field: {key} | {path}")
    value = str(cfg[key]).strip()
    if not value:
        raise ValueError(f"TVR data_hub config field must not be empty: {key} | {path}")
    return value


def _require_positive_int(cfg: Mapping[str, Any], path: str, key: str) -> int:
    if key not in cfg:
        raise KeyError(f"TVR data_hub config missing required field: {key} | {path}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"TVR data_hub config field must be int: {key} | {path}")
    try:
        out = int(value)
    except Exception as exc:
        raise TypeError(f"TVR data_hub config field must be int: {key} | {path}") from exc
    if out <= 0:
        raise ValueError(f"TVR data_hub config field must be > 0: {key} | {path}")
    return out


def _require_non_negative_int(cfg: Mapping[str, Any], path: str, key: str) -> int:
    if key not in cfg:
        raise KeyError(f"TVR data_hub config missing required field: {key} | {path}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"TVR data_hub config field must be int: {key} | {path}")
    try:
        out = int(value)
    except Exception as exc:
        raise TypeError(f"TVR data_hub config field must be int: {key} | {path}") from exc
    if out < 0:
        raise ValueError(f"TVR data_hub config field must be >= 0: {key} | {path}")
    return out


def load_config(path: str) -> dict[str, Any]:
    cfg = _load_json(path)
    if int(cfg.get("schema_version", 0)) != 1:
        raise ValueError(f"TVR data_hub config schema_version must be 1 | {path}")
    universe = _require_mapping(cfg, path, "universe")
    collection = _require_mapping(cfg, path, "collection")
    funding_history = _require_mapping(cfg, path, "funding_history")
    price_history = _require_mapping(cfg, path, "price_history")
    out = {
        "schema_version": 1,
        "enabled": _require_bool(cfg, path, "enabled"),
        "account": _require_non_empty_str(cfg, path, "account"),
        "audit_enabled": _require_bool(cfg, path, "audit_enabled"),
        "universe": {
            "underlying_subtype": _require_non_empty_str(universe, path, "underlying_subtype"),
            "quote_asset": _require_non_empty_str(universe, path, "quote_asset").upper(),
            "contract_type": _require_non_empty_str(universe, path, "contract_type").upper(),
            "status": _require_non_empty_str(universe, path, "status").upper(),
        },
        "collection": {
            "interval_secs": _require_positive_int(collection, path, "interval_secs"),
            "funding_history_bootstrap_enabled": _require_bool(collection, path, "funding_history_bootstrap_enabled"),
            "price_history_bootstrap_enabled": _require_bool(collection, path, "price_history_bootstrap_enabled"),
        },
        "funding_history": {
            "lookback_days": _require_positive_int(funding_history, path, "lookback_days"),
            "limit": _require_positive_int(funding_history, path, "limit"),
        },
        "price_history": {
            "interval": _require_non_empty_str(price_history, path, "interval"),
            "lookback_days": _require_positive_int(price_history, path, "lookback_days"),
            "minimum_history_days": _require_positive_int(price_history, path, "minimum_history_days"),
            "rolling_window_hours": _require_positive_int(price_history, path, "rolling_window_hours"),
            "kline_limit": _require_positive_int(price_history, path, "kline_limit"),
            "max_symbols_per_run": _require_non_negative_int(price_history, path, "max_symbols_per_run"),
        },
    }
    if int(out["price_history"]["lookback_days"]) < int(out["price_history"]["minimum_history_days"]):
        raise ValueError(f"TVR price_history lookback_days must cover minimum_history_days | {path}")
    if int(out["funding_history"]["limit"]) > 1000:
        raise ValueError(f"TVR funding_history limit must be <= 1000 | {path}")
    if int(out["price_history"]["kline_limit"]) > 1500:
        raise ValueError(f"TVR price_history kline_limit must be <= 1500 | {path}")
    return out


def _call_client(
    account: str,
    source: str,
    method_name: str,
    *,
    priority: str = REQUEST_PRIORITY_NORMAL,
    **params: Any,
) -> Any:
    return call_client_method(
        account,
        source=source,
        method_name=method_name,
        priority=priority,
        **params,
    )


def _futures_public_get(
    account: str,
    source: str,
    endpoint: str,
    params: Mapping[str, Any] | None = None,
    *,
    priority: str = REQUEST_PRIORITY_NORMAL,
) -> Any:
    return call_futures_public(
        account,
        source=source,
        endpoint=endpoint,
        params=params,
        priority=priority,
    )


def _as_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        out = float(value)
    except Exception:
        return None
    if math.isnan(out) or math.isinf(out):
        return None
    return out


def _as_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _normalize_subtypes(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return [str(x).strip() for x in raw if str(x).strip()]
    text = str(raw).strip()
    return [text] if text else []


def _tradfi_symbols(exchange_info: Mapping[str, Any], cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    universe_cfg = cfg["universe"]
    target_subtype = str(universe_cfg["underlying_subtype"]).casefold()
    quote_asset = str(universe_cfg["quote_asset"]).upper()
    contract_type = str(universe_cfg["contract_type"]).upper()
    status = str(universe_cfg["status"]).upper()
    raw_symbols = exchange_info.get("symbols")
    if not isinstance(raw_symbols, list):
        raise TypeError("futures_exchange_info symbols must be list")

    out: list[dict[str, Any]] = []
    for item in raw_symbols:
        if not isinstance(item, dict):
            continue
        subtypes = _normalize_subtypes(item.get("underlyingSubType"))
        if target_subtype not in {x.casefold() for x in subtypes}:
            continue
        if str(item.get("quoteAsset") or "").upper() != quote_asset:
            continue
        if str(item.get("contractType") or "").upper() != contract_type:
            continue
        if str(item.get("status") or "").upper() != status:
            continue
        symbol = str(item.get("symbol") or "").upper().strip()
        if not symbol:
            raise ValueError("TradFi exchangeInfo matched empty symbol")
        out.append({
            "symbol": symbol,
            "pair": item.get("pair"),
            "status": item.get("status"),
            "contract_type": item.get("contractType"),
            "quote_asset": item.get("quoteAsset"),
            "margin_asset": item.get("marginAsset"),
            "underlying_type": item.get("underlyingType"),
            "underlying_subtype": subtypes,
            "onboard_date": item.get("onboardDate"),
            "delivery_date": item.get("deliveryDate"),
            "price_precision": item.get("pricePrecision"),
            "quantity_precision": item.get("quantityPrecision"),
        })
    out.sort(key=lambda x: str(x["symbol"]))
    if not out:
        raise RuntimeError(
            "No TradFi symbols matched exchangeInfo filters: "
            f"underlying_subtype={universe_cfg['underlying_subtype']} "
            f"quote_asset={quote_asset} contract_type={contract_type} status={status}"
        )
    return out


def _ticker_map(ticker_payload: Any) -> dict[str, dict[str, Any]]:
    if not isinstance(ticker_payload, list):
        raise TypeError("futures_ticker payload must be list")
    out: dict[str, dict[str, Any]] = {}
    for item in ticker_payload:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper().strip()
        if symbol:
            out[symbol] = dict(item)
    return out


def _premium_map(premium_payload: Any) -> dict[str, dict[str, Any]]:
    if isinstance(premium_payload, dict):
        symbol = str(premium_payload.get("symbol") or "").upper().strip()
        return {symbol: dict(premium_payload)} if symbol else {}
    if not isinstance(premium_payload, list):
        raise TypeError("premiumIndex payload must be list or object")
    out: dict[str, dict[str, Any]] = {}
    for item in premium_payload:
        if not isinstance(item, dict):
            continue
        symbol = str(item.get("symbol") or "").upper().strip()
        if symbol:
            out[symbol] = dict(item)
    return out


def _write_universe_snapshot(
    *,
    account: str,
    run_id: str,
    symbols: list[dict[str, Any]],
    ticker_by_symbol: Mapping[str, Mapping[str, Any]],
) -> Path:
    records: list[dict[str, Any]] = []
    for meta in symbols:
        symbol = str(meta["symbol"])
        ticker = dict(ticker_by_symbol.get(symbol) or {})
        records.append({
            **meta,
            "last_price": _as_float(ticker.get("lastPrice")),
            "price_change_pct_24h": _as_float(ticker.get("priceChangePercent")),
            "quote_volume_24h": _as_float(ticker.get("quoteVolume")),
            "volume_24h": _as_float(ticker.get("volume")),
            "ticker_close_time": _as_int(ticker.get("closeTime")),
            "ticker_close_time_bj": _fmt_bj_from_ms(_as_int(ticker.get("closeTime"))),
        })
    payload = {
        **_base_record(run_id, "tradfi_universe_snapshot"),
        "account": account,
        "symbol_count": len(records),
        "symbols": records,
    }
    return _append_jsonl(_stream_path("universe"), payload)


def _write_funding_snapshot(
    *,
    account: str,
    run_id: str,
    symbols: Iterable[str],
    premium_by_symbol: Mapping[str, Mapping[str, Any]],
) -> Path:
    rows: list[dict[str, Any]] = []
    for symbol in sorted({str(x).upper().strip() for x in symbols if str(x).strip()}):
        item = dict(premium_by_symbol.get(symbol) or {})
        if not item:
            raise KeyError(f"premiumIndex missing TradFi symbol: {symbol}")
        rows.append({
            "symbol": symbol,
            "mark_price": _as_float(item.get("markPrice")),
            "index_price": _as_float(item.get("indexPrice")),
            "estimated_settle_price": _as_float(item.get("estimatedSettlePrice")),
            "last_funding_rate": _as_float(item.get("lastFundingRate")),
            "interest_rate": _as_float(item.get("interestRate")),
            "next_funding_time": _as_int(item.get("nextFundingTime")),
            "next_funding_time_bj": _fmt_bj_from_ms(_as_int(item.get("nextFundingTime"))),
            "source_time": _as_int(item.get("time")),
            "source_time_bj": _fmt_bj_from_ms(_as_int(item.get("time"))),
        })
    payload = {
        **_base_record(run_id, "tradfi_funding_snapshot"),
        "account": account,
        "symbol_count": len(rows),
        "source": "fapi/v1/premiumIndex",
        "rows": rows,
    }
    return _append_jsonl(_stream_path("funding"), payload)


def _write_price_24h_snapshot(
    *,
    account: str,
    run_id: str,
    symbols: Iterable[str],
    ticker_by_symbol: Mapping[str, Mapping[str, Any]],
) -> Path:
    rows: list[dict[str, Any]] = []
    for symbol in sorted({str(x).upper().strip() for x in symbols if str(x).strip()}):
        item = dict(ticker_by_symbol.get(symbol) or {})
        if not item:
            raise KeyError(f"futures_ticker missing TradFi symbol: {symbol}")
        rows.append({
            "symbol": symbol,
            "last_price": _as_float(item.get("lastPrice")),
            "open_price_24h": _as_float(item.get("openPrice")),
            "high_price_24h": _as_float(item.get("highPrice")),
            "low_price_24h": _as_float(item.get("lowPrice")),
            "price_change_24h": _as_float(item.get("priceChange")),
            "price_change_pct_24h": _as_float(item.get("priceChangePercent")),
            "weighted_avg_price_24h": _as_float(item.get("weightedAvgPrice")),
            "quote_volume_24h": _as_float(item.get("quoteVolume")),
            "volume_24h": _as_float(item.get("volume")),
            "open_time": _as_int(item.get("openTime")),
            "open_time_bj": _fmt_bj_from_ms(_as_int(item.get("openTime"))),
            "close_time": _as_int(item.get("closeTime")),
            "close_time_bj": _fmt_bj_from_ms(_as_int(item.get("closeTime"))),
        })
    payload = {
        **_base_record(run_id, "tradfi_price_24h_snapshot"),
        "account": account,
        "symbol_count": len(rows),
        "source": "fapi/v1/ticker/24hr",
        "rows": rows,
    }
    return _append_jsonl(_stream_path("price_24h"), payload)


def _interval_ms(interval: str) -> int:
    text = str(interval).strip().lower()
    if not text:
        raise ValueError("interval must not be empty")
    unit = text[-1]
    try:
        count = int(text[:-1])
    except Exception as exc:
        raise ValueError(f"unsupported interval: {interval}") from exc
    if count <= 0:
        raise ValueError(f"unsupported interval: {interval}")
    if unit == "m":
        return count * 60_000
    if unit == "h":
        return count * 60 * 60_000
    if unit == "d":
        return count * 24 * 60 * 60_000
    raise ValueError(f"unsupported interval: {interval}")


def _fetch_klines_history(
    account: str,
    symbol: str,
    *,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int,
) -> list[list[Any]]:
    step_ms = _interval_ms(interval)
    rows: list[list[Any]] = []
    cursor = int(start_ms)
    while cursor <= int(end_ms):
        batch = _call_client(
            account,
            "tvr_data_hub.futures_klines",
            "futures_klines",
            priority=REQUEST_PRIORITY_LOW,
            symbol=symbol,
            interval=interval,
            startTime=cursor,
            endTime=end_ms,
            limit=limit,
        )
        if not isinstance(batch, list):
            raise TypeError(f"futures_klines payload must be list: {symbol}")
        if not batch:
            break
        clean_batch = [list(x) for x in batch if isinstance(x, (list, tuple)) and len(x) >= 5]
        if not clean_batch:
            break
        rows.extend(clean_batch)
        last_open = _as_int(clean_batch[-1][0])
        if last_open is None:
            raise ValueError(f"futures_klines missing open_time: {symbol}")
        next_cursor = int(last_open) + step_ms
        if next_cursor <= cursor:
            raise RuntimeError(f"futures_klines cursor did not advance: {symbol}")
        cursor = next_cursor
        if len(clean_batch) < int(limit):
            break
    deduped: dict[int, list[Any]] = {}
    for row in rows:
        open_time = _as_int(row[0])
        if open_time is not None and int(start_ms) <= open_time <= int(end_ms):
            deduped[int(open_time)] = row
    return [deduped[k] for k in sorted(deduped)]


def _percentile(sorted_values: list[float], pct: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    p = min(100.0, max(0.0, float(pct))) / 100.0
    pos = p * (len(sorted_values) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return float(sorted_values[lo])
    weight = pos - lo
    return float(sorted_values[lo] * (1.0 - weight) + sorted_values[hi] * weight)


def _rolling_24h_stats(rows: list[list[Any]], *, interval: str, rolling_window_hours: int) -> dict[str, Any]:
    step_ms = _interval_ms(interval)
    window_bars = int((rolling_window_hours * 60 * 60_000) // step_ms)
    if window_bars <= 0:
        raise ValueError("rolling_window_hours does not cover one interval")
    close_by_open: dict[int, float] = {}
    for row in rows:
        open_time = _as_int(row[0])
        close_price = _as_float(row[4])
        if open_time is None or close_price is None or close_price <= 0:
            continue
        close_by_open[int(open_time)] = float(close_price)
    returns: list[float] = []
    for open_time in sorted(close_by_open):
        past_time = int(open_time) - window_bars * step_ms
        past_close = close_by_open.get(past_time)
        current_close = close_by_open[open_time]
        if past_close is None or past_close <= 0:
            continue
        returns.append(current_close / past_close - 1.0)
    ordered = sorted(returns)
    if not ordered:
        return {
            "sample_count": 0,
            "window_bars": int(window_bars),
            "min": None,
            "max": None,
            "mean": None,
            "median": None,
            "p1": None,
            "p5": None,
            "p10": None,
            "p20": None,
            "latest": None,
        }
    return {
        "sample_count": len(ordered),
        "window_bars": int(window_bars),
        "min": float(ordered[0]),
        "max": float(ordered[-1]),
        "mean": float(mean(ordered)),
        "median": float(median(ordered)),
        "p1": _percentile(ordered, 1),
        "p5": _percentile(ordered, 5),
        "p10": _percentile(ordered, 10),
        "p20": _percentile(ordered, 20),
        "latest": float(returns[-1]),
    }


def _bootstrap_price_history(
    *,
    account: str,
    run_id: str,
    symbols: Iterable[str],
    cfg: Mapping[str, Any],
) -> Path | None:
    price_cfg = cfg["price_history"]
    symbol_list = sorted({str(x).upper().strip() for x in symbols if str(x).strip()})
    max_symbols = int(price_cfg["max_symbols_per_run"])
    if max_symbols > 0:
        symbol_list = symbol_list[:max_symbols]
    if not symbol_list:
        return None

    now_ms = _now_utc_ms()
    interval = str(price_cfg["interval"])
    step_ms = _interval_ms(interval)
    end_ms = (now_ms // step_ms) * step_ms - step_ms
    start_ms = end_ms - int(price_cfg["lookback_days"]) * 24 * 60 * 60_000
    min_required_bars = int(price_cfg["minimum_history_days"]) * 24 * 60 * 60_000 // step_ms

    rows_out: list[dict[str, Any]] = []
    for symbol in symbol_list:
        rows = _fetch_klines_history(
            account,
            symbol,
            interval=interval,
            start_ms=start_ms,
            end_ms=end_ms,
            limit=int(price_cfg["kline_limit"]),
        )
        if len(rows) < min_required_bars:
            stats = {
                "sample_count": 0,
                "window_bars": int((int(price_cfg["rolling_window_hours"]) * 60 * 60_000) // step_ms),
                "min": None,
                "max": None,
                "mean": None,
                "median": None,
                "p1": None,
                "p5": None,
                "p10": None,
                "p20": None,
                "latest": None,
            }
            history_sufficient = False
            insufficiency_reason = f"history_rows_below_minimum({len(rows)}/{min_required_bars})"
        else:
            stats = _rolling_24h_stats(
                rows,
                interval=interval,
                rolling_window_hours=int(price_cfg["rolling_window_hours"]),
            )
            history_sufficient = True
            insufficiency_reason = ""
        rows_out.append({
            "symbol": symbol,
            "interval": interval,
            "lookback_days": int(price_cfg["lookback_days"]),
            "minimum_history_days": int(price_cfg["minimum_history_days"]),
            "history_sufficient": bool(history_sufficient),
            "insufficiency_reason": insufficiency_reason,
            "first_open_time": _as_int(rows[0][0]) if rows else None,
            "first_open_time_bj": _fmt_bj_from_ms(_as_int(rows[0][0]) if rows else None),
            "last_open_time": _as_int(rows[-1][0]) if rows else None,
            "last_open_time_bj": _fmt_bj_from_ms(_as_int(rows[-1][0]) if rows else None),
            "kline_count": len(rows),
            "rolling_24h": stats,
        })
        logging.info(
            "TVR rolling_24h stats | symbol=%s | rows=%s | sufficient=%s | samples=%s",
            symbol,
            len(rows),
            history_sufficient,
            stats["sample_count"],
        )

    payload = {
        **_base_record(run_id, "tradfi_rolling_24h_stats"),
        "account": account,
        "source": "fapi/v1/klines",
        "symbol_count": len(rows_out),
        "rows": rows_out,
    }
    return _append_jsonl(_stream_path("rolling_24h_stats"), payload)


def _bootstrap_funding_history(
    *,
    account: str,
    run_id: str,
    symbols: Iterable[str],
    cfg: Mapping[str, Any],
) -> list[Path]:
    funding_cfg = cfg["funding_history"]
    now_ms = _now_utc_ms()
    start_ms = now_ms - int(funding_cfg["lookback_days"]) * 24 * 60 * 60_000
    paths: list[Path] = []
    for symbol in sorted({str(x).upper().strip() for x in symbols if str(x).strip()}):
        payload = _futures_public_get(
            account,
            "tvr_data_hub.funding_rate_history",
            "fundingRate",
            {
                "symbol": symbol,
                "startTime": start_ms,
                "endTime": now_ms,
                "limit": int(funding_cfg["limit"]),
            },
            priority=REQUEST_PRIORITY_LOW,
        )
        if not isinstance(payload, list):
            raise TypeError(f"fundingRate payload must be list: {symbol}")
        rows: list[dict[str, Any]] = []
        for item in payload:
            if not isinstance(item, dict):
                continue
            funding_time = _as_int(item.get("fundingTime"))
            rows.append({
                "symbol": symbol,
                "funding_rate": _as_float(item.get("fundingRate")),
                "funding_time": funding_time,
                "funding_time_bj": _fmt_bj_from_ms(funding_time),
                "mark_price": _as_float(item.get("markPrice")),
            })
        record = {
            **_base_record(run_id, "tradfi_funding_history_bootstrap"),
            "account": account,
            "source": "fapi/v1/fundingRate",
            "lookback_days": int(funding_cfg["lookback_days"]),
            "symbol": symbol,
            "row_count": len(rows),
            "rows": rows,
        }
        paths.append(_append_jsonl(_stream_path("funding_history"), record))
        logging.info("TVR funding history | symbol=%s | rows=%s", symbol, len(rows))
    return paths


def run_once(
    cfg: Mapping[str, Any],
    *,
    run_id: str,
    include_funding_history: bool,
    include_price_history: bool,
) -> dict[str, Any]:
    if not bool(cfg["enabled"]):
        raise RuntimeError("TVR data_hub config enabled=false")
    account = str(cfg["account"]).strip()
    exchange_info = _call_client(account, "tvr_data_hub.futures_exchange_info", "futures_exchange_info")
    if not isinstance(exchange_info, dict):
        raise TypeError("futures_exchange_info payload must be object")
    tradfi = _tradfi_symbols(exchange_info, cfg)
    symbols = [str(x["symbol"]) for x in tradfi]

    ticker_payload = _call_client(account, "tvr_data_hub.futures_ticker", "futures_ticker")
    ticker_by_symbol = _ticker_map(ticker_payload)
    premium_payload = _futures_public_get(account, "tvr_data_hub.premium_index", "premiumIndex")
    premium_by_symbol = _premium_map(premium_payload)

    paths: list[str] = []
    if bool(cfg["audit_enabled"]):
        paths.append(str(_write_universe_snapshot(
            account=account,
            run_id=run_id,
            symbols=tradfi,
            ticker_by_symbol=ticker_by_symbol,
        )))
        paths.append(str(_write_funding_snapshot(
            account=account,
            run_id=run_id,
            symbols=symbols,
            premium_by_symbol=premium_by_symbol,
        )))
        paths.append(str(_write_price_24h_snapshot(
            account=account,
            run_id=run_id,
            symbols=symbols,
            ticker_by_symbol=ticker_by_symbol,
        )))

    if include_funding_history:
        paths.extend(str(x) for x in _bootstrap_funding_history(
            account=account,
            run_id=run_id,
            symbols=symbols,
            cfg=cfg,
        ))
    if include_price_history:
        price_path = _bootstrap_price_history(
            account=account,
            run_id=run_id,
            symbols=symbols,
            cfg=cfg,
        )
        if price_path is not None:
            paths.append(str(price_path))

    return {
        "run_id": run_id,
        "account": account,
        "symbol_count": len(symbols),
        "symbols": symbols,
        "paths": paths,
    }


def _build_run_id(account: str) -> str:
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    account_key = str(account).upper().strip()
    if not account_key:
        raise ValueError("account must not be empty")
    return f"TVR_DATA_HUB_{account_key}_{ts}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TVR data_hub: collect TradFi live facts and bootstrap rolling 24h stats")
    parser.add_argument("--config", default="strategies/tvr/config.data_hub.json")
    parser.add_argument("--once", action="store_true", help="run one collection iteration")
    parser.add_argument("--loop", action="store_true", help="run collection loop")
    parser.add_argument("--max-iterations", type=int, default=0, help="loop iteration cap; 0 means unlimited")
    parser.add_argument("--skip-funding-history-bootstrap", action="store_true")
    parser.add_argument("--skip-price-history-bootstrap", action="store_true")
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()
    if args.once == args.loop:
        raise ValueError("exactly one of --once or --loop is required")
    cfg = load_config(args.config)
    run_id = _build_run_id(str(cfg["account"]))
    include_funding_history = bool(cfg["collection"]["funding_history_bootstrap_enabled"]) and not args.skip_funding_history_bootstrap
    include_price_history = bool(cfg["collection"]["price_history_bootstrap_enabled"]) and not args.skip_price_history_bootstrap

    iteration = 0
    while True:
        iteration += 1
        logging.info(
            "TVR data_hub iteration started | run_id=%s | iteration=%s | funding_history=%s | price_history=%s",
            run_id,
            iteration,
            include_funding_history,
            include_price_history,
        )
        summary = run_once(
            cfg,
            run_id=run_id,
            include_funding_history=include_funding_history,
            include_price_history=include_price_history,
        )
        logging.info(
            "TVR data_hub iteration finished | run_id=%s | iteration=%s | symbol_count=%s | paths=%s",
            run_id,
            iteration,
            summary["symbol_count"],
            len(summary["paths"]),
        )
        include_funding_history = False
        include_price_history = False
        if args.once:
            break
        if int(args.max_iterations) > 0 and iteration >= int(args.max_iterations):
            break
        time.sleep(int(cfg["collection"]["interval_secs"]))


if __name__ == "__main__":
    main()
