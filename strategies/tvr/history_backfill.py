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
from typing import Any, Iterable, Mapping

from filelock import FileLock

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as e:
    raise SystemExit("Missing dependency: pyarrow. Install with: pip install -U pyarrow") from e

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core.live.binance_rest_gateway import (
    BinanceRestGatewayRejected,
    REQUEST_PRIORITY_LOW,
    REQUEST_PRIORITY_NORMAL,
    call_client_method,
)
from core.runtime_state import get_state_dir

BJ = timezone(timedelta(hours=8))
STRATEGY_NAME = "tvr"
DAY_MS = 24 * 60 * 60_000

RESEARCH_KLINES_SCHEMA = pa.schema(
    [
        ("open_time_ms", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("quote_asset_volume", pa.float64()),
        ("close_time_ms", pa.int64()),
    ]
)


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
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")


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


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    tmp_path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2, default=_json_default) + "\n", encoding="utf-8")
    os.replace(tmp_path, path)


def _research_dir() -> Path:
    return get_state_dir() / "research" / "tvr"


def _state_path() -> Path:
    return _research_dir() / "history_backfill_state.json"


def _audit_path(day_bj: str | None = None) -> Path:
    day_key = str(day_bj or "").strip() or _bj_day_from_ms(None)
    return _research_dir() / "audit" / "history_backfill" / day_key / "tvr_history_backfill.jsonl"


def _raw_path(symbol: str, month_key: str) -> Path:
    symbol_key = str(symbol).upper().strip()
    if not symbol_key:
        raise ValueError("symbol must not be empty")
    month = str(month_key).strip()
    if not month:
        raise ValueError("month_key must not be empty")
    return _research_dir() / "klines_1m" / symbol_key / f"{month}.parquet"


def _base_record(run_id: str, event: str) -> dict[str, Any]:
    now_ms = _now_utc_ms()
    return {
        "schema_version": 1,
        "strategy_name": STRATEGY_NAME,
        "run_mode": "research",
        "run_id": str(run_id),
        "event": str(event),
        "collected_utc_ms": int(now_ms),
        "collected_bj": _fmt_bj_from_ms(now_ms),
    }


def _load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"TVR history backfill config missing: {path}")
    with p.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise TypeError(f"TVR history backfill config must be JSON object: {path}")
    return payload


def _require_mapping(cfg: Mapping[str, Any], path: str, key: str) -> dict[str, Any]:
    if key not in cfg:
        raise KeyError(f"TVR history backfill config missing required section: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, dict):
        raise TypeError(f"TVR history backfill config section must be object: {key} | {path}")
    return dict(value)


def _require_bool(cfg: Mapping[str, Any], path: str, key: str) -> bool:
    if key not in cfg:
        raise KeyError(f"TVR history backfill config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, bool):
        raise TypeError(f"TVR history backfill config field must be bool: {key} | {path}")
    return bool(value)


def _require_non_empty_str(cfg: Mapping[str, Any], path: str, key: str) -> str:
    if key not in cfg:
        raise KeyError(f"TVR history backfill config missing required field: {key} | {path}")
    value = str(cfg[key]).strip()
    if not value:
        raise ValueError(f"TVR history backfill config field must not be empty: {key} | {path}")
    return value


def _require_int(cfg: Mapping[str, Any], path: str, key: str, *, positive: bool) -> int:
    if key not in cfg:
        raise KeyError(f"TVR history backfill config missing required field: {key} | {path}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"TVR history backfill config field must be int: {key} | {path}")
    try:
        out = int(value)
    except Exception as exc:
        raise TypeError(f"TVR history backfill config field must be int: {key} | {path}") from exc
    if positive and out <= 0:
        raise ValueError(f"TVR history backfill config field must be > 0: {key} | {path}")
    if not positive and out < 0:
        raise ValueError(f"TVR history backfill config field must be >= 0: {key} | {path}")
    return out


def _require_float(cfg: Mapping[str, Any], path: str, key: str, *, positive: bool) -> float:
    if key not in cfg:
        raise KeyError(f"TVR history backfill config missing required field: {key} | {path}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"TVR history backfill config field must be number: {key} | {path}")
    try:
        out = float(value)
    except Exception as exc:
        raise TypeError(f"TVR history backfill config field must be number: {key} | {path}") from exc
    if math.isnan(out) or math.isinf(out):
        raise ValueError(f"TVR history backfill config field must be finite: {key} | {path}")
    if positive and out <= 0:
        raise ValueError(f"TVR history backfill config field must be > 0: {key} | {path}")
    if not positive and out < 0:
        raise ValueError(f"TVR history backfill config field must be >= 0: {key} | {path}")
    return out


def _require_symbol_list(cfg: Mapping[str, Any], path: str, key: str) -> list[str]:
    if key not in cfg:
        raise KeyError(f"TVR history backfill config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, list):
        raise TypeError(f"TVR history backfill config field must be list: {key} | {path}")
    out = [str(x).upper().strip() for x in value if str(x).strip()]
    if len(out) != len(set(out)):
        raise ValueError(f"TVR history backfill config symbols contain duplicates | {path}")
    return out


def load_config(path: str) -> dict[str, Any]:
    cfg = _load_json(path)
    if "schema_version" not in cfg:
        raise KeyError(f"TVR history backfill config missing required field: schema_version | {path}")
    if int(cfg["schema_version"]) != 1:
        raise ValueError(f"TVR history backfill config schema_version must be 1 | {path}")
    universe = _require_mapping(cfg, path, "universe")
    collection = _require_mapping(cfg, path, "collection")
    history = _require_mapping(cfg, path, "history")
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
            "symbols": _require_symbol_list(universe, path, "symbols"),
        },
        "collection": {
            "interval_secs": _require_int(collection, path, "interval_secs", positive=True),
        },
        "history": {
            "interval": _require_non_empty_str(history, path, "interval"),
            "stable_lag_minutes": _require_int(history, path, "stable_lag_minutes", positive=True),
            "kline_limit": _require_int(history, path, "kline_limit", positive=True),
            "max_symbols_per_run": _require_int(history, path, "max_symbols_per_run", positive=False),
            "max_requests_per_run": _require_int(history, path, "max_requests_per_run", positive=True),
            "max_batches_per_symbol_per_run": _require_int(history, path, "max_batches_per_symbol_per_run", positive=True),
            "request_sleep_secs": _require_float(history, path, "request_sleep_secs", positive=False),
            "dry_run_preview_days": _require_int(history, path, "dry_run_preview_days", positive=True),
        },
    }
    if int(out["history"]["kline_limit"]) > 1500:
        raise ValueError(f"TVR history kline_limit must be <= 1500 | {path}")
    return out


def _sleep_after_gateway_reject(exc: BinanceRestGatewayRejected, attempt: int) -> float:
    if str(exc.code).endswith("_QUOTA_CLOSED"):
        now_ms = _now_utc_ms()
        next_minute_ms = int((now_ms // 60_000 + 1) * 60_000)
        return max(1.0, (next_minute_ms - now_ms) / 1000.0 + 1.0)
    return min(60.0, float(2 ** max(0, int(attempt) - 1)))


def _call_with_gateway_retry(callable_obj, *, source: str, max_attempts: int = 8) -> Any:
    for attempt in range(1, int(max_attempts) + 1):
        try:
            return callable_obj()
        except BinanceRestGatewayRejected as exc:
            if attempt >= int(max_attempts):
                raise
            sleep_s = _sleep_after_gateway_reject(exc, attempt)
            logging.warning(
                "TVR history REST Gateway rejected | source=%s | attempt=%s/%s | code=%s | used=%s | threshold=%s | sleep=%.1fs",
                source,
                attempt,
                max_attempts,
                exc.code,
                exc.used_weight_1m,
                exc.threshold,
                sleep_s,
            )
            time.sleep(float(sleep_s))


def _call_client(account: str, source: str, method_name: str, *, priority: str, **params: Any) -> Any:
    return _call_with_gateway_retry(
        lambda: call_client_method(
            account,
            source=source,
            method_name=method_name,
            priority=priority,
            **params,
        ),
        source=source,
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
        return count * DAY_MS
    raise ValueError(f"unsupported interval: {interval}")


def _align_up_to_interval(ts_ms: int, step_ms: int) -> int:
    value = int(ts_ms)
    step = int(step_ms)
    return int(((value + step - 1) // step) * step)


def _stable_end_ms(cfg: Mapping[str, Any]) -> int:
    step_ms = _interval_ms(str(cfg["history"]["interval"]))
    now_ms = _now_utc_ms()
    return int((now_ms // step_ms) * step_ms - int(cfg["history"]["stable_lag_minutes"]) * step_ms)


def _month_key_from_ms(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc)
    return f"{dt.year:04d}-{dt.month:02d}"


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

    selected_symbols = set(universe_cfg["symbols"])
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
        if selected_symbols and symbol not in selected_symbols:
            continue
        onboard_date = _as_int(item.get("onboardDate"))
        if onboard_date is None or onboard_date <= 0:
            raise ValueError(f"TradFi exchangeInfo missing onboardDate: {symbol}")
        out.append({
            "symbol": symbol,
            "onboard_date": int(onboard_date),
            "onboard_date_bj": _fmt_bj_from_ms(int(onboard_date)),
            "pair": item.get("pair"),
            "status": item.get("status"),
            "contract_type": item.get("contractType"),
            "quote_asset": item.get("quoteAsset"),
            "underlying_subtype": subtypes,
        })
    out.sort(key=lambda x: str(x["symbol"]))
    if selected_symbols:
        found = {str(x["symbol"]) for x in out}
        missing = sorted(selected_symbols - found)
        if missing:
            raise RuntimeError(f"TVR history configured symbols not found in TradFi exchangeInfo: {missing}")
    if not out:
        raise RuntimeError("No TradFi symbols matched history backfill universe filters")
    return out


def _dry_run_symbols(cfg: Mapping[str, Any], symbols_override: list[str]) -> list[dict[str, Any]]:
    symbols = symbols_override or list(cfg["universe"]["symbols"])
    if not symbols:
        symbols = ["XAUUSDT"]
    now_ms = _now_utc_ms()
    onboard = now_ms - int(cfg["history"]["dry_run_preview_days"]) * DAY_MS
    return [
        {
            "symbol": str(symbol).upper().strip(),
            "onboard_date": int(onboard),
            "onboard_date_bj": _fmt_bj_from_ms(int(onboard)),
            "source": "dry_run_symbols",
        }
        for symbol in symbols
        if str(symbol).strip()
    ]


def _load_symbols(cfg: Mapping[str, Any], *, dry_run: bool, symbols_override: list[str]) -> list[dict[str, Any]]:
    if dry_run:
        return _dry_run_symbols(cfg, symbols_override)
    exchange_info = _call_client(
        str(cfg["account"]),
        "tvr_history_backfill.futures_exchange_info",
        "futures_exchange_info",
        priority=REQUEST_PRIORITY_NORMAL,
    )
    symbols = _tradfi_symbols(exchange_info, cfg)
    if symbols_override:
        wanted = set(symbols_override)
        symbols = [row for row in symbols if str(row["symbol"]) in wanted]
        found = {str(row["symbol"]) for row in symbols}
        missing = sorted(wanted - found)
        if missing:
            raise RuntimeError(f"TVR history CLI symbols not found in TradFi exchangeInfo: {missing}")
    return symbols


def _rows_to_table(rows: Iterable[Mapping[str, Any]]) -> pa.Table:
    ordered = sorted((dict(row) for row in rows), key=lambda x: int(x["open_time_ms"]))
    return pa.Table.from_arrays(
        [
            pa.array([int(row["open_time_ms"]) for row in ordered], type=pa.int64()),
            pa.array([float(row["open"]) for row in ordered], type=pa.float64()),
            pa.array([float(row["high"]) for row in ordered], type=pa.float64()),
            pa.array([float(row["low"]) for row in ordered], type=pa.float64()),
            pa.array([float(row["close"]) for row in ordered], type=pa.float64()),
            pa.array([float(row["quote_asset_volume"]) for row in ordered], type=pa.float64()),
            pa.array([int(row["close_time_ms"]) for row in ordered], type=pa.int64()),
        ],
        schema=RESEARCH_KLINES_SCHEMA,
    )


def _kline_to_row(row: list[Any]) -> dict[str, Any] | None:
    open_time = _as_int(row[0] if len(row) > 0 else None)
    open_price = _as_float(row[1] if len(row) > 1 else None)
    high = _as_float(row[2] if len(row) > 2 else None)
    low = _as_float(row[3] if len(row) > 3 else None)
    close = _as_float(row[4] if len(row) > 4 else None)
    close_time = _as_int(row[6] if len(row) > 6 else None)
    quote_volume = _as_float(row[7] if len(row) > 7 else 0.0)
    if open_time is None or open_price is None or high is None or low is None or close is None:
        return None
    return {
        "open_time_ms": int(open_time),
        "open": float(open_price),
        "high": float(high),
        "low": float(low),
        "close": float(close),
        "quote_asset_volume": float(quote_volume or 0.0),
        "close_time_ms": int(close_time if close_time is not None else open_time + 59_999),
    }


def _read_parquet_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    table = pq.read_table(path, columns=RESEARCH_KLINES_SCHEMA.names)
    cols = {name: table.column(name).to_pylist() for name in RESEARCH_KLINES_SCHEMA.names}
    return [{name: cols[name][i] for name in RESEARCH_KLINES_SCHEMA.names} for i in range(table.num_rows)]


def _merge_write_parquet(path: Path, rows: Iterable[Mapping[str, Any]]) -> int:
    incoming = {int(row["open_time_ms"]): dict(row) for row in rows}
    if not incoming and not path.exists():
        return 0
    merged = {int(row["open_time_ms"]): row for row in _read_parquet_rows(path)}
    merged.update(incoming)
    path.parent.mkdir(parents=True, exist_ok=True)
    table = _rows_to_table(merged.values())
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    pq.write_table(table, tmp_path, compression="zstd")
    os.replace(tmp_path, path)
    return int(table.num_rows)


def _write_rows(symbol: str, raw_rows: Iterable[list[Any]]) -> int:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for raw in raw_rows:
        row = _kline_to_row(list(raw))
        if row is None:
            continue
        grouped.setdefault(_month_key_from_ms(int(row["open_time_ms"])), []).append(row)
    written = 0
    for month_key, rows in grouped.items():
        _merge_write_parquet(_raw_path(symbol, month_key), rows)
        written += len(rows)
    return int(written)


def _infer_last_open_from_local(symbol: str) -> int | None:
    raw_dir = _research_dir() / "klines_1m" / str(symbol).upper().strip()
    if not raw_dir.exists():
        return None
    last_open: int | None = None
    for path in sorted(raw_dir.glob("*.parquet")):
        table = pq.read_table(path, columns=["open_time_ms"])
        values = table.column("open_time_ms").to_pylist()
        if not values:
            continue
        month_last = int(max(values))
        if last_open is None or month_last > last_open:
            last_open = month_last
    return last_open


def _load_state() -> dict[str, Any]:
    path = _state_path()
    if not path.exists():
        return {"schema_version": 1, "strategy_name": STRATEGY_NAME, "store": "research_history", "per_symbol": {}}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"TVR history state must be object: {path}")
    if int(payload.get("schema_version", 0)) != 1:
        raise ValueError(f"TVR history state schema_version must be 1: {path}")
    per_symbol = payload.get("per_symbol")
    if not isinstance(per_symbol, dict):
        raise TypeError(f"TVR history state missing per_symbol object: {path}")
    return payload


def _save_state(state: Mapping[str, Any]) -> None:
    _atomic_write_json(_state_path(), dict(state))


def _update_state(
    state: dict[str, Any],
    symbol: str,
    *,
    onboard_date_ms: int,
    last_open_time_ms: int,
    row_count: int,
    complete_through_ms: int,
) -> None:
    symbol_key = str(symbol).upper().strip()
    state.setdefault("per_symbol", {})
    state["per_symbol"][symbol_key] = {
        "onboard_date_ms": int(onboard_date_ms),
        "onboard_date_bj": _fmt_bj_from_ms(int(onboard_date_ms)),
        "last_open_time_ms": int(last_open_time_ms),
        "last_open_time_bj": _fmt_bj_from_ms(int(last_open_time_ms)),
        "row_count_last_increment": int(row_count),
        "complete_through_ms": int(complete_through_ms),
        "complete_through_bj": _fmt_bj_from_ms(int(complete_through_ms)),
        "updated_utc_ms": _now_utc_ms(),
        "updated_bj": _fmt_bj_from_ms(_now_utc_ms()),
    }
    state["updated_utc_ms"] = _now_utc_ms()
    state["updated_bj"] = _fmt_bj_from_ms(_now_utc_ms())


def _fetch_klines_batch(
    account: str,
    symbol: str,
    *,
    interval: str,
    start_ms: int,
    end_ms: int,
    limit: int,
) -> list[list[Any]]:
    batch = _call_client(
        account,
        "tvr_history_backfill.futures_klines",
        "futures_klines",
        priority=REQUEST_PRIORITY_LOW,
        symbol=symbol,
        interval=interval,
        startTime=int(start_ms),
        endTime=int(end_ms),
        limit=int(limit),
    )
    if not isinstance(batch, list):
        raise TypeError(f"futures_klines payload must be list: {symbol}")
    clean = [list(row) for row in batch if isinstance(row, (list, tuple)) and len(row) >= 5]
    clean.sort(key=lambda row: int(row[0]))
    return clean


def _sync_symbol(
    *,
    cfg: Mapping[str, Any],
    symbol_meta: Mapping[str, Any],
    state: dict[str, Any],
    end_ms: int,
    request_budget: int,
    dry_run: bool,
) -> dict[str, Any]:
    history_cfg = cfg["history"]
    account = str(cfg["account"])
    interval = str(history_cfg["interval"])
    step_ms = _interval_ms(interval)
    symbol = str(symbol_meta["symbol"]).upper().strip()
    onboard_ms = int(symbol_meta["onboard_date"])
    aligned_onboard_ms = _align_up_to_interval(onboard_ms, step_ms)
    state_row = dict((state.get("per_symbol") or {}).get(symbol) or {})
    state_last_open = _as_int(state_row.get("last_open_time_ms"))
    local_last_open = None if state_last_open is not None else _infer_last_open_from_local(symbol)
    if state_last_open is not None:
        fetch_start_ms = int(state_last_open) + step_ms
        fetch_mode = "incremental_state"
    elif local_last_open is not None:
        fetch_start_ms = int(local_last_open) + step_ms
        fetch_mode = "incremental_local_recovered"
    else:
        fetch_start_ms = int(aligned_onboard_ms)
        fetch_mode = "from_onboard_date"

    if fetch_start_ms > int(end_ms):
        return {
            "symbol": symbol,
            "status": "up_to_date",
            "fetch_mode": fetch_mode,
            "onboard_date": int(onboard_ms),
            "onboard_date_bj": _fmt_bj_from_ms(int(onboard_ms)),
            "start_time": None,
            "end_time": int(end_ms),
            "request_count": 0,
            "fetched_kline_count": 0,
            "written_row_count": 0,
            "last_open_time": state_last_open or local_last_open,
            "last_open_time_bj": _fmt_bj_from_ms(state_last_open or local_last_open),
        }

    max_batches = min(int(history_cfg["max_batches_per_symbol_per_run"]), int(request_budget))
    if dry_run:
        preview_end = min(int(end_ms), int(fetch_start_ms) + max_batches * int(history_cfg["kline_limit"]) * step_ms - step_ms)
        return {
            "symbol": symbol,
            "status": "dry_run_planned",
            "fetch_mode": fetch_mode,
            "onboard_date": int(onboard_ms),
            "onboard_date_bj": _fmt_bj_from_ms(int(onboard_ms)),
            "start_time": int(fetch_start_ms),
            "start_time_bj": _fmt_bj_from_ms(int(fetch_start_ms)),
            "end_time": int(end_ms),
            "end_time_bj": _fmt_bj_from_ms(int(end_ms)),
            "preview_end_time": int(preview_end),
            "preview_end_time_bj": _fmt_bj_from_ms(int(preview_end)),
            "request_count": 0,
            "planned_request_count": int(max_batches),
            "fetched_kline_count": 0,
            "written_row_count": 0,
        }

    cursor = int(fetch_start_ms)
    request_count = 0
    fetched_count = 0
    written_count = 0
    last_open: int | None = None
    status = "partial"
    while cursor <= int(end_ms) and request_count < max_batches:
        batch = _fetch_klines_batch(
            account,
            symbol,
            interval=interval,
            start_ms=cursor,
            end_ms=int(end_ms),
            limit=int(history_cfg["kline_limit"]),
        )
        request_count += 1
        if not batch:
            status = "no_data"
            break
        written = _write_rows(symbol, batch)
        fetched_count += len(batch)
        written_count += int(written)
        last_open = _as_int(batch[-1][0])
        if last_open is None:
            raise ValueError(f"TVR history fetched row missing open_time: {symbol}")
        _update_state(
            state,
            symbol,
            onboard_date_ms=int(onboard_ms),
            last_open_time_ms=int(last_open),
            row_count=int(written),
            complete_through_ms=int(end_ms),
        )
        _save_state(state)
        cursor = int(last_open) + step_ms
        if cursor > int(end_ms):
            status = "up_to_date"
            break
        if len(batch) < int(history_cfg["kline_limit"]):
            status = "exchange_returned_short_batch"
            break
        sleep_s = float(history_cfg["request_sleep_secs"])
        if sleep_s > 0:
            time.sleep(sleep_s)

    return {
        "symbol": symbol,
        "status": status,
        "fetch_mode": fetch_mode,
        "onboard_date": int(onboard_ms),
        "onboard_date_bj": _fmt_bj_from_ms(int(onboard_ms)),
        "start_time": int(fetch_start_ms),
        "start_time_bj": _fmt_bj_from_ms(int(fetch_start_ms)),
        "end_time": int(end_ms),
        "end_time_bj": _fmt_bj_from_ms(int(end_ms)),
        "request_count": int(request_count),
        "fetched_kline_count": int(fetched_count),
        "written_row_count": int(written_count),
        "last_open_time": int(last_open) if last_open is not None else state_last_open or local_last_open,
        "last_open_time_bj": _fmt_bj_from_ms(int(last_open) if last_open is not None else state_last_open or local_last_open),
    }


def run_once(
    cfg: Mapping[str, Any],
    *,
    run_id: str,
    dry_run: bool,
    symbols_override: list[str],
) -> Path | None:
    if not bool(cfg["enabled"]):
        raise RuntimeError("TVR history backfill config enabled=false")
    symbols = _load_symbols(cfg, dry_run=dry_run, symbols_override=symbols_override)
    max_symbols = int(cfg["history"]["max_symbols_per_run"])
    if max_symbols > 0:
        symbols = symbols[:max_symbols]
    end_ms = _stable_end_ms(cfg)
    state = _load_state()
    rows: list[dict[str, Any]] = []
    request_budget = int(cfg["history"]["max_requests_per_run"])
    for symbol_meta in symbols:
        if request_budget <= 0:
            rows.append({"symbol": str(symbol_meta["symbol"]), "status": "skipped_request_budget_exhausted"})
            continue
        row = _sync_symbol(
            cfg=cfg,
            symbol_meta=symbol_meta,
            state=state,
            end_ms=end_ms,
            request_budget=request_budget,
            dry_run=dry_run,
        )
        rows.append(row)
        request_budget -= int(row.get("request_count") or 0)

    record = {
        **_base_record(run_id, "tvr_history_backfill"),
        "account": str(cfg["account"]),
        "dry_run": bool(dry_run),
        "source": "futures_exchange_info + futures_klines",
        "store_root": str(_research_dir() / "klines_1m"),
        "state_path": str(_state_path()),
        "symbol_count": len(symbols),
        "request_budget_initial": int(cfg["history"]["max_requests_per_run"]),
        "request_budget_remaining": int(request_budget),
        "end_time": int(end_ms),
        "end_time_bj": _fmt_bj_from_ms(int(end_ms)),
        "config": {
            "universe": dict(cfg["universe"]),
            "history": dict(cfg["history"]),
        },
        "rows": rows,
    }
    logging.info(
        "TVR history backfill | dry_run=%s | symbols=%s | requests=%s | remaining=%s",
        dry_run,
        len(symbols),
        int(cfg["history"]["max_requests_per_run"]) - int(request_budget),
        int(request_budget),
    )
    if not bool(cfg["audit_enabled"]):
        return None
    return _append_jsonl(_audit_path(), record)


def _build_run_id(account: str) -> str:
    account_key = str(account).upper().strip()
    if not account_key:
        raise ValueError("account must not be empty")
    ts_utc = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"TVR_HISTORY_BACKFILL_{account_key}_{ts_utc}"


def _parse_symbols(value: str) -> list[str]:
    return [x.strip().upper() for x in str(value or "").split(",") if x.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TVR research history backfill: TradFi 1m contract klines")
    parser.add_argument("--config", default="strategies/tvr/config.history_backfill.json")
    parser.add_argument("--once", action="store_true", help="run one backfill iteration")
    parser.add_argument("--loop", action="store_true", help="run backfill loop")
    parser.add_argument("--max-iterations", type=int, default=0, help="loop iteration cap; 0 means unlimited")
    parser.add_argument("--dry-run", action="store_true", help="plan work without Binance requests, parquet writes, or state writes")
    parser.add_argument("--symbols", default="", help="optional comma-separated symbols")
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()
    if args.once == args.loop:
        raise ValueError("exactly one of --once or --loop is required")
    cfg = load_config(args.config)
    run_id = _build_run_id(str(cfg["account"]))
    symbols_override = _parse_symbols(str(args.symbols or ""))
    iteration = 0
    while True:
        iteration += 1
        logging.info("TVR history backfill iteration started | run_id=%s | iteration=%s | dry_run=%s", run_id, iteration, args.dry_run)
        path = run_once(cfg, run_id=run_id, dry_run=bool(args.dry_run), symbols_override=symbols_override)
        logging.info("TVR history backfill iteration finished | run_id=%s | iteration=%s | path=%s", run_id, iteration, path)
        if args.once:
            break
        if int(args.max_iterations) > 0 and iteration >= int(args.max_iterations):
            break
        time.sleep(int(cfg["collection"]["interval_secs"]))


if __name__ == "__main__":
    main()
