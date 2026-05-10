from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import sys
import time
from bisect import bisect_right
from datetime import datetime, timedelta, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Iterable, Mapping

from filelock import FileLock

try:
    import pandas as pd
    import pyarrow.parquet as pq
except Exception as e:
    raise SystemExit("Missing dependency: pandas/pyarrow. Install with: pip install -U pandas pyarrow") from e

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core.runtime_state import get_state_dir

BJ = timezone(timedelta(hours=8))
STRATEGY_NAME = "tvr"
DAY_MS = 24 * 60 * 60_000
REQUIRED_KLINE_COLUMNS = [
    "open_time_ms",
    "open",
    "high",
    "low",
    "close",
    "quote_asset_volume",
    "close_time_ms",
]


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


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    with tmp_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)
    return path


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


def _state_relative_path(path_text: str) -> Path:
    text = str(path_text).strip()
    if not text:
        raise ValueError("state relative path must not be empty")
    path = Path(text)
    if path.is_absolute():
        return path
    return get_state_dir() / path


def _audit_path(output_root: Path, day_bj: str | None = None) -> Path:
    day_key = str(day_bj or "").strip() or _bj_day_from_ms(None)
    return output_root / day_key / "tvr_percentile_reclaim_backtest.jsonl"


def _run_output_path(output_root: Path, run_id: str, suffix: str) -> Path:
    day_key = _bj_day_from_ms(None)
    return output_root / day_key / f"{run_id}.{suffix}"


def _load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"TVR percentile backtest config missing: {path}")
    with p.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise TypeError(f"TVR percentile backtest config must be JSON object: {path}")
    return payload


def _require_mapping(cfg: Mapping[str, Any], path: str, key: str) -> dict[str, Any]:
    if key not in cfg:
        raise KeyError(f"TVR percentile backtest config missing required section: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, dict):
        raise TypeError(f"TVR percentile backtest config section must be object: {key} | {path}")
    return dict(value)


def _require_bool(cfg: Mapping[str, Any], path: str, key: str) -> bool:
    if key not in cfg:
        raise KeyError(f"TVR percentile backtest config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, bool):
        raise TypeError(f"TVR percentile backtest config field must be bool: {key} | {path}")
    return bool(value)


def _require_non_empty_str(cfg: Mapping[str, Any], path: str, key: str) -> str:
    if key not in cfg:
        raise KeyError(f"TVR percentile backtest config missing required field: {key} | {path}")
    value = str(cfg[key]).strip()
    if not value:
        raise ValueError(f"TVR percentile backtest config field must not be empty: {key} | {path}")
    return value


def _require_int(cfg: Mapping[str, Any], path: str, key: str, *, positive: bool) -> int:
    if key not in cfg:
        raise KeyError(f"TVR percentile backtest config missing required field: {key} | {path}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"TVR percentile backtest config field must be int: {key} | {path}")
    try:
        out = int(value)
    except Exception as exc:
        raise TypeError(f"TVR percentile backtest config field must be int: {key} | {path}") from exc
    if positive and out <= 0:
        raise ValueError(f"TVR percentile backtest config field must be > 0: {key} | {path}")
    if not positive and out < 0:
        raise ValueError(f"TVR percentile backtest config field must be >= 0: {key} | {path}")
    return out


def _require_symbol_list(cfg: Mapping[str, Any], path: str, key: str) -> list[str]:
    if key not in cfg:
        raise KeyError(f"TVR percentile backtest config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, list):
        raise TypeError(f"TVR percentile backtest config field must be list: {key} | {path}")
    out = [str(x).upper().strip() for x in value if str(x).strip()]
    if len(out) != len(set(out)):
        raise ValueError(f"TVR percentile backtest config symbols contain duplicates | {path}")
    return out


def _require_float_list(cfg: Mapping[str, Any], path: str, key: str) -> list[float]:
    if key not in cfg:
        raise KeyError(f"TVR percentile backtest config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, list) or not value:
        raise TypeError(f"TVR percentile backtest config field must be non-empty list: {key} | {path}")
    out: list[float] = []
    for item in value:
        if isinstance(item, bool):
            raise TypeError(f"TVR percentile backtest config list item must be number: {key} | {path}")
        num = float(item)
        if math.isnan(num) or math.isinf(num) or num <= 0:
            raise ValueError(f"TVR percentile backtest config list item must be positive finite number: {key} | {path}")
        out.append(float(num))
    if len(out) != len(set(out)):
        raise ValueError(f"TVR percentile backtest config list contains duplicates: {key} | {path}")
    return out


def _require_percentiles(cfg: Mapping[str, Any], path: str, key: str) -> list[str]:
    if key not in cfg:
        raise KeyError(f"TVR percentile backtest config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, list) or not value:
        raise TypeError(f"TVR percentile backtest config field must be non-empty list: {key} | {path}")
    out = [str(x).lower().strip() for x in value if str(x).strip()]
    allowed = {"p1", "p5", "p10", "p20", "p50"}
    bad = [x for x in out if x not in allowed]
    if bad:
        raise ValueError(f"TVR percentile backtest unsupported entry_percentiles: {bad} | {path}")
    if len(out) != len(set(out)):
        raise ValueError(f"TVR percentile backtest entry_percentiles contain duplicates | {path}")
    return out


def load_config(path: str) -> dict[str, Any]:
    cfg = _load_json(path)
    if "schema_version" not in cfg:
        raise KeyError(f"TVR percentile backtest config missing required field: schema_version | {path}")
    if int(cfg["schema_version"]) != 1:
        raise ValueError(f"TVR percentile backtest config schema_version must be 1 | {path}")
    universe = _require_mapping(cfg, path, "universe")
    history_store = _require_mapping(cfg, path, "history_store")
    output = _require_mapping(cfg, path, "output")
    backtest = _require_mapping(cfg, path, "backtest")
    out = {
        "schema_version": 1,
        "enabled": _require_bool(cfg, path, "enabled"),
        "audit_enabled": _require_bool(cfg, path, "audit_enabled"),
        "universe": {
            "symbols": _require_symbol_list(universe, path, "symbols"),
        },
        "history_store": {
            "root": _require_non_empty_str(history_store, path, "root"),
        },
        "output": {
            "root": _require_non_empty_str(output, path, "root"),
            "write_samples": _require_bool(output, path, "write_samples"),
        },
        "backtest": {
            "interval": _require_non_empty_str(backtest, path, "interval"),
            "rolling_window_hours": _require_int(backtest, path, "rolling_window_hours", positive=True),
            "lookback_days": _require_int(backtest, path, "lookback_days", positive=True),
            "minimum_history_days": _require_int(backtest, path, "minimum_history_days", positive=True),
            "entry_percentiles": _require_percentiles(backtest, path, "entry_percentiles"),
            "take_profit_pcts": _require_float_list(backtest, path, "take_profit_pcts"),
            "max_hold_hours": _require_int(backtest, path, "max_hold_hours", positive=True),
            "entry_price_mode": _require_non_empty_str(backtest, path, "entry_price_mode").lower(),
            "max_symbols_per_run": _require_int(backtest, path, "max_symbols_per_run", positive=False),
        },
    }
    if out["backtest"]["entry_price_mode"] != "close":
        raise ValueError(f"TVR percentile backtest entry_price_mode must be close | {path}")
    if int(out["backtest"]["lookback_days"]) < int(out["backtest"]["minimum_history_days"]):
        raise ValueError(f"TVR percentile backtest lookback_days must cover minimum_history_days | {path}")
    return out


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


def _percentile_number(name: str) -> float:
    text = str(name).lower().strip()
    if not text.startswith("p"):
        raise ValueError(f"unsupported percentile: {name}")
    return float(text[1:])


def _symbol_dirs(root: Path, configured_symbols: list[str], override_symbols: list[str]) -> list[Path]:
    wanted = override_symbols or configured_symbols
    if wanted:
        dirs = [root / symbol for symbol in wanted]
        missing = [str(path) for path in dirs if not path.exists()]
        if missing:
            raise FileNotFoundError(f"TVR percentile backtest missing symbol dirs: {missing}")
        return dirs
    if not root.exists():
        raise FileNotFoundError(f"TVR percentile backtest history store missing: {root}")
    dirs = sorted(path for path in root.iterdir() if path.is_dir())
    if not dirs:
        raise FileNotFoundError(f"TVR percentile backtest history store has no symbol dirs: {root}")
    return dirs


def _load_symbol_rows(root: Path, symbol: str) -> list[dict[str, Any]]:
    symbol_dir = root / str(symbol).upper().strip()
    if not symbol_dir.exists():
        raise FileNotFoundError(f"TVR percentile backtest symbol dir missing: {symbol_dir}")
    parquet_files = sorted(symbol_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"TVR percentile backtest symbol has no parquet files: {symbol}")
    rows_by_time: dict[int, dict[str, Any]] = {}
    for path in parquet_files:
        schema_names = set(pq.read_schema(path).names)
        missing = [name for name in REQUIRED_KLINE_COLUMNS if name not in schema_names]
        if missing:
            raise ValueError(f"TVR percentile backtest parquet missing columns: {path} {missing}")
        table = pq.read_table(path, columns=REQUIRED_KLINE_COLUMNS)
        cols = {name: table.column(name).to_pylist() for name in REQUIRED_KLINE_COLUMNS}
        for i in range(table.num_rows):
            open_time = int(cols["open_time_ms"][i])
            rows_by_time[open_time] = {
                "open_time_ms": open_time,
                "open": float(cols["open"][i]),
                "high": float(cols["high"][i]),
                "low": float(cols["low"][i]),
                "close": float(cols["close"][i]),
                "quote_asset_volume": float(cols["quote_asset_volume"][i] or 0.0),
                "close_time_ms": int(cols["close_time_ms"][i]),
            }
    rows = [rows_by_time[k] for k in sorted(rows_by_time)]
    if not rows:
        raise RuntimeError(f"TVR percentile backtest symbol rows empty: {symbol}")
    return rows


def _rolling_returns(rows: list[dict[str, Any]], *, interval: str, rolling_window_hours: int) -> list[float | None]:
    step_ms = _interval_ms(interval)
    window_ms = int(rolling_window_hours) * 60 * 60_000
    close_by_time = {int(row["open_time_ms"]): float(row["close"]) for row in rows}
    out: list[float | None] = []
    for row in rows:
        open_time = int(row["open_time_ms"])
        current_close = float(row["close"])
        past_close = close_by_time.get(open_time - window_ms)
        if past_close is None or past_close <= 0 or current_close <= 0:
            out.append(None)
            continue
        out.append(float(current_close / past_close - 1.0))
    if window_ms % step_ms != 0:
        raise ValueError("rolling_window_hours must align to interval")
    return out


def _percentile_thresholds(
    returns: list[float | None],
    *,
    cfg: Mapping[str, Any],
) -> dict[str, list[float | None]]:
    bt = cfg["backtest"]
    step_ms = _interval_ms(str(bt["interval"]))
    lookback_bars = int(bt["lookback_days"]) * DAY_MS // step_ms
    min_samples = int(bt["minimum_history_days"]) * DAY_MS // step_ms
    values = [float("nan") if value is None else float(value) for value in returns]
    series = pd.Series(values, dtype="float64").shift(1)
    out: dict[str, list[float | None]] = {}
    for percentile_name in bt["entry_percentiles"]:
        quantile = _percentile_number(str(percentile_name)) / 100.0
        rolled = series.rolling(window=int(lookback_bars), min_periods=int(min_samples)).quantile(float(quantile))
        clean: list[float | None] = []
        for value in rolled.tolist():
            if value is None or math.isnan(float(value)) or math.isinf(float(value)):
                clean.append(None)
            else:
                clean.append(float(value))
        out[str(percentile_name)] = clean
    return out


def _future_tp_hit(
    rows: list[dict[str, Any]],
    times: list[int],
    start_index: int,
    *,
    target_price: float,
    max_hold_ms: int,
) -> tuple[bool, int | None, int | None, float | None]:
    entry_time = int(times[start_index])
    end_time = entry_time + int(max_hold_ms)
    end_index = bisect_right(times, end_time) - 1
    if end_index <= start_index:
        return False, None, end_index, None
    max_seen_high: float | None = None
    for j in range(start_index + 1, end_index + 1):
        high = float(rows[j]["high"])
        max_seen_high = high if max_seen_high is None else max(max_seen_high, high)
        if high >= float(target_price):
            return True, int(j), end_index, high
    return False, None, end_index, max_seen_high


def _duration_stats(values_ms: list[int]) -> dict[str, float | None]:
    if not values_ms:
        return {
            "tp_time_min_minutes": None,
            "tp_time_max_minutes": None,
            "tp_time_avg_minutes": None,
            "tp_time_median_minutes": None,
        }
    minutes = [float(x) / 60_000.0 for x in values_ms]
    return {
        "tp_time_min_minutes": float(min(minutes)),
        "tp_time_max_minutes": float(max(minutes)),
        "tp_time_avg_minutes": float(mean(minutes)),
        "tp_time_median_minutes": float(median(minutes)),
    }


def _summary_row(
    *,
    symbol: str,
    percentile_name: str,
    take_profit_pct: float,
    samples: list[dict[str, Any]],
    evaluated_bars: int,
) -> dict[str, Any]:
    sample_count = len(samples)
    hit_samples = [row for row in samples if bool(row["tp_hit"])]
    hit_count = len(hit_samples)
    hit_durations = [int(row["tp_time_ms"]) for row in hit_samples if row.get("tp_time_ms") is not None]
    stats = _duration_stats(hit_durations)
    return {
        "symbol": symbol,
        "entry_percentile": percentile_name,
        "take_profit_pct": float(take_profit_pct),
        "evaluated_bars": int(evaluated_bars),
        "sample_count": int(sample_count),
        "tp_hit_count": int(hit_count),
        "not_hit_count": int(sample_count - hit_count),
        "tp_hit_rate": float(hit_count / sample_count) if sample_count else None,
        **stats,
    }


def _backtest_combo(
    *,
    symbol: str,
    rows: list[dict[str, Any]],
    returns: list[float | None],
    thresholds: list[float | None],
    cfg: Mapping[str, Any],
    percentile_name: str,
    take_profit_pct: float,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    bt = cfg["backtest"]
    max_hold_ms = int(bt["max_hold_hours"]) * 60 * 60_000
    times = [int(row["open_time_ms"]) for row in rows]

    samples: list[dict[str, Any]] = []
    i = 0
    evaluated_bars = 0
    while i < len(rows):
        current_return = returns[i]
        selected_return = thresholds[i]
        if current_return is None:
            i += 1
            continue
        if selected_return is None:
            i += 1
            continue
        open_time = int(times[i])
        evaluated_bars += 1
        if selected_return is None or float(current_return) > float(selected_return):
            i += 1
            continue
        entry_price = float(rows[i]["close"])
        target_price = entry_price * (1.0 + float(take_profit_pct))
        hit, hit_index, end_index, max_seen_high = _future_tp_hit(
            rows,
            times,
            i,
            target_price=target_price,
            max_hold_ms=max_hold_ms,
        )
        exit_time = int(times[hit_index]) if hit_index is not None else None
        end_time = int(times[end_index]) if end_index is not None and end_index >= i else None
        tp_time_ms = int(exit_time - open_time) if exit_time is not None else None
        sample = {
            "symbol": symbol,
            "entry_percentile": percentile_name,
            "take_profit_pct": float(take_profit_pct),
            "entry_time_ms": int(open_time),
            "entry_time_bj": _fmt_bj_from_ms(int(open_time)),
            "entry_price": float(entry_price),
            "target_price": float(target_price),
            "current_24h_return": float(current_return),
            "selected_percentile_return": float(selected_return),
            "tp_hit": bool(hit),
            "tp_time_ms": tp_time_ms,
            "tp_time_minutes": float(tp_time_ms / 60_000.0) if tp_time_ms is not None else None,
            "tp_time_bj": _fmt_bj_from_ms(exit_time),
            "sample_end_time_ms": end_time,
            "sample_end_time_bj": _fmt_bj_from_ms(end_time),
            "max_seen_high": max_seen_high,
        }
        samples.append(sample)
        if hit_index is not None:
            i = int(hit_index) + 1
        elif end_time is not None:
            i = bisect_right(times, int(end_time))
        else:
            i += 1

    summary = _summary_row(
        symbol=symbol,
        percentile_name=percentile_name,
        take_profit_pct=float(take_profit_pct),
        samples=samples,
        evaluated_bars=evaluated_bars,
    )
    return summary, samples


def _backtest_symbol(symbol: str, rows: list[dict[str, Any]], cfg: Mapping[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    bt = cfg["backtest"]
    returns = _rolling_returns(
        rows,
        interval=str(bt["interval"]),
        rolling_window_hours=int(bt["rolling_window_hours"]),
    )
    thresholds_by_percentile = _percentile_thresholds(returns, cfg=cfg)
    summary_rows: list[dict[str, Any]] = []
    sample_rows: list[dict[str, Any]] = []
    for percentile_name in bt["entry_percentiles"]:
        for take_profit_pct in bt["take_profit_pcts"]:
            summary, samples = _backtest_combo(
                symbol=symbol,
                rows=rows,
                returns=returns,
                thresholds=thresholds_by_percentile[str(percentile_name)],
                cfg=cfg,
                percentile_name=str(percentile_name),
                take_profit_pct=float(take_profit_pct),
            )
            summary_rows.append(summary)
            sample_rows.extend(samples)
    meta = {
        "symbol": symbol,
        "kline_count": len(rows),
        "first_open_time": int(rows[0]["open_time_ms"]),
        "first_open_time_bj": _fmt_bj_from_ms(int(rows[0]["open_time_ms"])),
        "last_open_time": int(rows[-1]["open_time_ms"]),
        "last_open_time_bj": _fmt_bj_from_ms(int(rows[-1]["open_time_ms"])),
        "rolling_return_count": sum(1 for value in returns if value is not None),
    }
    return summary_rows, sample_rows, meta


SUMMARY_FIELDS = [
    "symbol",
    "entry_percentile",
    "take_profit_pct",
    "evaluated_bars",
    "sample_count",
    "tp_hit_count",
    "not_hit_count",
    "tp_hit_rate",
    "tp_time_min_minutes",
    "tp_time_max_minutes",
    "tp_time_avg_minutes",
    "tp_time_median_minutes",
]

SAMPLE_FIELDS = [
    "symbol",
    "entry_percentile",
    "take_profit_pct",
    "entry_time_ms",
    "entry_time_bj",
    "entry_price",
    "target_price",
    "current_24h_return",
    "selected_percentile_return",
    "tp_hit",
    "tp_time_ms",
    "tp_time_minutes",
    "tp_time_bj",
    "sample_end_time_ms",
    "sample_end_time_bj",
    "max_seen_high",
]


def run_once(cfg: Mapping[str, Any], *, run_id: str, symbols_override: list[str]) -> dict[str, Any]:
    if not bool(cfg["enabled"]):
        raise RuntimeError("TVR percentile backtest config enabled=false")
    history_root = _state_relative_path(str(cfg["history_store"]["root"]))
    output_root = _state_relative_path(str(cfg["output"]["root"]))
    symbol_dirs = _symbol_dirs(history_root, list(cfg["universe"]["symbols"]), symbols_override)
    max_symbols = int(cfg["backtest"]["max_symbols_per_run"])
    if max_symbols > 0:
        symbol_dirs = symbol_dirs[:max_symbols]

    all_summary: list[dict[str, Any]] = []
    all_samples: list[dict[str, Any]] = []
    symbol_meta: list[dict[str, Any]] = []
    for symbol_dir in symbol_dirs:
        symbol = symbol_dir.name.upper().strip()
        rows = _load_symbol_rows(history_root, symbol)
        summary_rows, sample_rows, meta = _backtest_symbol(symbol, rows, cfg)
        all_summary.extend(summary_rows)
        all_samples.extend(sample_rows)
        symbol_meta.append(meta)
        logging.info("TVR percentile backtest | symbol=%s | summary=%s | samples=%s", symbol, len(summary_rows), len(sample_rows))

    summary_path = _run_output_path(output_root, run_id, "summary.csv")
    samples_path = _run_output_path(output_root, run_id, "samples.csv")
    _write_csv(summary_path, all_summary, SUMMARY_FIELDS)
    if bool(cfg["output"]["write_samples"]):
        _write_csv(samples_path, all_samples, SAMPLE_FIELDS)
    else:
        samples_path = None

    record = {
        **_base_record(run_id, "tvr_percentile_reclaim_backtest"),
        "history_root": str(history_root),
        "output_root": str(output_root),
        "summary_path": str(summary_path),
        "samples_path": str(samples_path) if samples_path is not None else None,
        "symbol_count": len(symbol_dirs),
        "summary_count": len(all_summary),
        "sample_count": len(all_samples),
        "config": {
            "universe": dict(cfg["universe"]),
            "history_store": dict(cfg["history_store"]),
            "output": dict(cfg["output"]),
            "backtest": dict(cfg["backtest"]),
        },
        "symbols": symbol_meta,
        "summary": all_summary,
    }
    audit_path = None
    if bool(cfg["audit_enabled"]):
        audit_path = _append_jsonl(_audit_path(output_root), record)
    return {
        "audit_path": str(audit_path) if audit_path else None,
        "summary_path": str(summary_path),
        "samples_path": str(samples_path) if samples_path is not None else None,
        "symbol_count": len(symbol_dirs),
        "summary_count": len(all_summary),
        "sample_count": len(all_samples),
    }


def _build_run_id() -> str:
    ts_utc = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"TVR_PERCENTILE_BACKTEST_{ts_utc}"


def _parse_symbols(value: str) -> list[str]:
    return [x.strip().upper() for x in str(value or "").split(",") if x.strip()]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TVR percentile reclaim backtest")
    parser.add_argument("--config", default="strategies/tvr/config.percentile_backtest.json")
    parser.add_argument("--symbols", default="", help="optional comma-separated symbols")
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()
    cfg = load_config(args.config)
    run_id = _build_run_id()
    logging.info("TVR percentile backtest started | run_id=%s", run_id)
    summary = run_once(cfg, run_id=run_id, symbols_override=_parse_symbols(args.symbols))
    logging.info(
        "TVR percentile backtest finished | run_id=%s | symbols=%s | summary=%s | samples=%s | audit=%s",
        run_id,
        summary["symbol_count"],
        summary["summary_count"],
        summary["sample_count"],
        summary["audit_path"],
    )


if __name__ == "__main__":
    main()
