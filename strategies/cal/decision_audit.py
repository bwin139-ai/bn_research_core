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
from typing import Any, Mapping

from filelock import FileLock

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core.live.binance_exec import get_open_orders, get_position
from core.live.binance_rest_gateway import REQUEST_PRIORITY_LOW, call_client_method
from core.live.custom_id import parse_client_order_id
from core.runtime_state import get_state_dir

BJ = timezone(timedelta(hours=8))
STRATEGY_NAME = "cal"
STRATEGY_CODE = "CAL"
POSITION_SIDE_LONG = "LONG"
KLINE_OPEN_TIME_INDEX = 0
KLINE_HIGH_INDEX = 2
KLINE_CLOSE_INDEX = 4
INTERVAL_1M_MS = 60_000
LEVEL_ORDER = {"P1": 1, "P2": 2, "P3": 3}
REPEAT_SHIFT_LEVELS = {"P2", "P3"}


def _interval_ms(interval: str) -> int:
    text = str(interval or "").strip().lower()
    if len(text) < 2:
        raise ValueError(f"CAL unsupported interval: {interval}")
    unit = text[-1]
    try:
        count = int(text[:-1])
    except Exception as exc:
        raise ValueError(f"CAL unsupported interval: {interval}") from exc
    if count <= 0:
        raise ValueError(f"CAL unsupported interval: {interval}")
    if unit == "m":
        return count * 60_000
    if unit == "h":
        return count * 60 * 60_000
    if unit == "d":
        return count * 24 * 60 * 60_000
    raise ValueError(f"CAL unsupported interval: {interval}")


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


def _audit_path(*, day_bj: str | None = None) -> Path:
    day_key = str(day_bj or "").strip() or _bj_day_from_ms(None)
    return get_state_dir() / "live_audit" / "cal" / "decision" / day_key / "cal_decision_audit.jsonl"


def _state_path(account: str) -> Path:
    account_key = str(account).strip()
    if not account_key:
        raise ValueError("CAL account must not be empty")
    return get_state_dir() / "live" / f"cal_{account_key}.state.json"


def _anchor_cache_path() -> Path:
    return get_state_dir() / "live_audit" / "cal" / "decision" / "h_anchor_cache.json"


def _base_record(run_id: str, event: str) -> dict[str, Any]:
    now_ms = _now_utc_ms()
    return {
        "schema_version": 1,
        "strategy_name": STRATEGY_NAME,
        "run_mode": "dry_run",
        "run_id": str(run_id),
        "event": str(event),
        "collected_utc_ms": int(now_ms),
        "collected_bj": _fmt_bj_from_ms(now_ms),
    }


def _load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"CAL decision audit config missing: {path}")
    payload = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"CAL decision audit config must be JSON object: {path}")
    return payload


def _require_mapping(cfg: Mapping[str, Any], path: str, key: str) -> dict[str, Any]:
    if key not in cfg:
        raise KeyError(f"CAL decision audit config missing required section: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, dict):
        raise TypeError(f"CAL decision audit config section must be object: {key} | {path}")
    return dict(value)


def _require_bool(cfg: Mapping[str, Any], path: str, key: str) -> bool:
    if key not in cfg:
        raise KeyError(f"CAL decision audit config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, bool):
        raise TypeError(f"CAL decision audit config field must be bool: {key} | {path}")
    return bool(value)


def _require_non_empty_str(cfg: Mapping[str, Any], path: str, key: str) -> str:
    if key not in cfg:
        raise KeyError(f"CAL decision audit config missing required field: {key} | {path}")
    value = str(cfg[key]).strip()
    if not value:
        raise ValueError(f"CAL decision audit config field must not be empty: {key} | {path}")
    return value


def _require_int(cfg: Mapping[str, Any], path: str, key: str, *, positive: bool) -> int:
    if key not in cfg:
        raise KeyError(f"CAL decision audit config missing required field: {key} | {path}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"CAL decision audit config field must be int: {key} | {path}")
    try:
        out = int(value)
    except Exception as exc:
        raise TypeError(f"CAL decision audit config field must be int: {key} | {path}") from exc
    if positive and out <= 0:
        raise ValueError(f"CAL decision audit config field must be > 0: {key} | {path}")
    if not positive and out < 0:
        raise ValueError(f"CAL decision audit config field must be >= 0: {key} | {path}")
    return out


def _require_float(cfg: Mapping[str, Any], path: str, key: str, *, positive: bool) -> float:
    if key not in cfg:
        raise KeyError(f"CAL decision audit config missing required field: {key} | {path}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"CAL decision audit config field must be number: {key} | {path}")
    try:
        out = float(value)
    except Exception as exc:
        raise TypeError(f"CAL decision audit config field must be number: {key} | {path}") from exc
    if math.isnan(out) or math.isinf(out):
        raise ValueError(f"CAL decision audit config field must be finite: {key} | {path}")
    if positive and out <= 0:
        raise ValueError(f"CAL decision audit config field must be > 0: {key} | {path}")
    if not positive and out < 0:
        raise ValueError(f"CAL decision audit config field must be >= 0: {key} | {path}")
    return out


def _require_symbol_list(cfg: Mapping[str, Any], path: str, key: str) -> list[str]:
    if key not in cfg:
        raise KeyError(f"CAL decision audit config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, list):
        raise TypeError(f"CAL decision audit config field must be list: {key} | {path}")
    out: list[str] = []
    seen: set[str] = set()
    for raw in value:
        symbol = str(raw).upper().strip()
        if not symbol:
            raise ValueError(f"CAL decision audit config contains empty symbol: {key} | {path}")
        if symbol in seen:
            raise ValueError(f"CAL decision audit config duplicated symbol: {symbol} | {path}")
        seen.add(symbol)
        out.append(symbol)
    if not out:
        raise ValueError(f"CAL decision audit config field must not be empty: {key} | {path}")
    return out


def _require_symbol_float_map(cfg: Mapping[str, Any], path: str, key: str) -> dict[str, float]:
    if key not in cfg:
        raise KeyError(f"CAL decision audit config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, dict) or not value:
        raise TypeError(f"CAL decision audit config field must be non-empty object: {key} | {path}")
    out: dict[str, float] = {}
    for raw_symbol, raw_value in value.items():
        symbol = str(raw_symbol).upper().strip()
        if not symbol:
            raise ValueError(f"CAL decision audit config contains empty symbol key: {key} | {path}")
        if symbol in out:
            raise ValueError(f"CAL decision audit config duplicated symbol key: {symbol} | {path}")
        if isinstance(raw_value, bool):
            raise TypeError(f"CAL decision audit config symbol value must be number: {key}.{symbol} | {path}")
        try:
            num = float(raw_value)
        except Exception as exc:
            raise TypeError(f"CAL decision audit config symbol value must be number: {key}.{symbol} | {path}") from exc
        if math.isnan(num) or math.isinf(num) or num <= 0:
            raise ValueError(f"CAL decision audit config symbol value must be positive finite: {key}.{symbol} | {path}")
        out[symbol] = float(num)
    return out


def _require_symbol_int_map(cfg: Mapping[str, Any], path: str, key: str, *, positive: bool) -> dict[str, int]:
    if key not in cfg:
        raise KeyError(f"CAL decision audit config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, dict) or not value:
        raise TypeError(f"CAL decision audit config field must be non-empty object: {key} | {path}")
    out: dict[str, int] = {}
    for raw_symbol, raw_value in value.items():
        symbol = str(raw_symbol).upper().strip()
        if not symbol:
            raise ValueError(f"CAL decision audit config contains empty symbol key: {key} | {path}")
        if symbol in out:
            raise ValueError(f"CAL decision audit config duplicated symbol key: {symbol} | {path}")
        if isinstance(raw_value, bool):
            raise TypeError(f"CAL decision audit config symbol value must be integer: {key}.{symbol} | {path}")
        try:
            num = int(raw_value)
        except Exception as exc:
            raise TypeError(f"CAL decision audit config symbol value must be integer: {key}.{symbol} | {path}") from exc
        if positive and num <= 0:
            raise ValueError(f"CAL decision audit config symbol value must be positive: {key}.{symbol} | {path}")
        out[symbol] = int(num)
    return out


def _load_levels(raw_levels: Any, path: str, *, label: str = "ladder.levels") -> list[dict[str, Any]]:
    if not isinstance(raw_levels, list) or not raw_levels:
        raise TypeError(f"CAL {label} must be non-empty list | {path}")
    levels: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in raw_levels:
        if not isinstance(item, dict):
            raise TypeError(f"CAL {label} item must be object | {path}")
        level = str(item.get("level") or "").upper().strip()
        if level not in {"P1", "P2", "P3"}:
            raise ValueError(f"CAL unsupported ladder level: {level!r} | {label} | {path}")
        if level in seen:
            raise ValueError(f"CAL duplicated ladder level: {level} | {label} | {path}")
        seen.add(level)
        drop_pct = _require_float(item, path, "drop_pct", positive=True)
        notional = _require_float(item, path, "notional_usdt", positive=True)
        row = {"level": level, "drop_pct": float(drop_pct), "notional_usdt": float(notional)}
        if level in REPEAT_SHIFT_LEVELS:
            row["repeat_drop_step_pct"] = float(_require_float(item, path, "repeat_drop_step_pct", positive=True))
        elif "repeat_drop_step_pct" in item:
            raise ValueError(f"CAL repeat_drop_step_pct is only supported for P2/P3 | {label}.{level} | {path}")
        levels.append(row)
    if "P1" not in seen:
        raise ValueError(f"CAL {label} must include P1 | {path}")
    levels.sort(key=lambda row: LEVEL_ORDER[str(row["level"])])
    p2 = next((row for row in levels if row["level"] == "P2"), None)
    p3 = next((row for row in levels if row["level"] == "P3"), None)
    if p2 is not None and p3 is not None and float(p3["drop_pct"]) <= float(p2["drop_pct"]):
        raise ValueError(f"CAL P3 drop_pct must be greater than P2 drop_pct | {label} | {path}")
    return levels


def _load_symbol_levels(ladder: Mapping[str, Any], path: str) -> dict[str, list[dict[str, Any]]]:
    raw = ladder.get("symbol_levels", {})
    if raw in (None, ""):
        return {}
    if not isinstance(raw, dict):
        raise TypeError(f"CAL ladder.symbol_levels must be object | {path}")
    out: dict[str, list[dict[str, Any]]] = {}
    for raw_symbol, raw_levels in raw.items():
        symbol = str(raw_symbol).upper().strip()
        if not symbol:
            raise ValueError(f"CAL ladder.symbol_levels contains empty symbol | {path}")
        if symbol in out:
            raise ValueError(f"CAL ladder.symbol_levels duplicated symbol: {symbol} | {path}")
        out[symbol] = _load_levels(raw_levels, path, label=f"ladder.symbol_levels.{symbol}")
    return out


def load_config(path: str) -> dict[str, Any]:
    cfg = _load_json(path)
    if int(cfg.get("schema_version", 0)) != 1:
        raise ValueError(f"CAL decision audit config schema_version must be 1 | {path}")
    universe = _require_mapping(cfg, path, "universe")
    collection = _require_mapping(cfg, path, "collection")
    data = _require_mapping(cfg, path, "data")
    ladder = _require_mapping(cfg, path, "ladder")
    exit_policy = _require_mapping(cfg, path, "exit_policy")
    risk = _require_mapping(cfg, path, "risk")
    execution = _require_mapping(cfg, path, "execution")
    decision = _require_mapping(cfg, path, "decision")

    out = {
        "schema_version": 1,
        "enabled": _require_bool(cfg, path, "enabled"),
        "account": _require_non_empty_str(cfg, path, "account"),
        "audit_enabled": _require_bool(cfg, path, "audit_enabled"),
        "universe": {"tradable_symbols": _require_symbol_list(universe, path, "tradable_symbols")},
        "collection": {"interval_secs": _require_int(collection, path, "interval_secs", positive=True)},
        "data": {
            "klines_interval": _require_non_empty_str(data, path, "klines_interval"),
            "lookback_hours": _require_int(data, path, "lookback_hours", positive=True),
            "include_current_bar": _require_bool(data, path, "include_current_bar"),
            "h_anchor_refresh_secs": _require_int(data, path, "h_anchor_refresh_secs", positive=True),
            "kline_limit": _require_int(data, path, "kline_limit", positive=True),
        },
        "ladder": {
            "levels": _load_levels(ladder.get("levels"), path),
            "symbol_levels": _load_symbol_levels(ladder, path),
        },
        "exit_policy": {
            "symbol_take_profit_pct": _require_symbol_float_map(
                exit_policy, path, "symbol_take_profit_pct"
            ),
        },
        "risk": {
            "max_symbol_strategy_notional_usdt": _require_symbol_float_map(
                risk, path, "max_symbol_strategy_notional_usdt"
            ),
            "max_total_strategy_notional_usdt": _require_float(
                risk, path, "max_total_strategy_notional_usdt", positive=True
            ),
        },
        "execution": {
            "position_side": _require_non_empty_str(execution, path, "position_side").upper(),
            "position_mode": _require_non_empty_str(execution, path, "position_mode").upper(),
            "margin_type": _require_non_empty_str(execution, path, "margin_type").upper(),
            "symbol_leverage": _require_symbol_int_map(execution, path, "symbol_leverage", positive=True),
            "post_only_time_in_force": _require_non_empty_str(execution, path, "post_only_time_in_force").upper(),
            "current_price_source": _require_non_empty_str(execution, path, "current_price_source").upper(),
            "order_book_limit": _require_int(execution, path, "order_book_limit", positive=True),
        },
        "decision": {"max_candidates": _require_int(decision, path, "max_candidates", positive=True)},
    }

    if out["data"]["klines_interval"] != "1h":
        raise ValueError(f"CAL first version only supports 1h klines_interval | {path}")
    if int(out["data"]["lookback_hours"]) % 1 != 0:
        raise ValueError(f"CAL data.lookback_hours must align to 1h bars | {path}")
    if int(out["data"]["kline_limit"]) > 1500:
        raise ValueError(f"CAL data.kline_limit must be <= 1500 | {path}")
    if out["execution"]["position_side"] != POSITION_SIDE_LONG:
        raise ValueError(f"CAL only supports LONG position_side | {path}")
    if out["execution"]["position_mode"] != "HEDGE":
        raise ValueError(f"CAL position_mode must be HEDGE | {path}")
    if out["execution"]["margin_type"] != "CROSSED":
        raise ValueError(f"CAL margin_type must be CROSSED | {path}")
    if out["execution"]["post_only_time_in_force"] != "GTX":
        raise ValueError(f"CAL post_only_time_in_force must be GTX | {path}")
    if out["execution"]["current_price_source"] != "BEST_BID":
        raise ValueError(f"CAL current_price_source must be BEST_BID | {path}")

    tradable_set = set(out["universe"]["tradable_symbols"])
    cap_set = set(out["risk"]["max_symbol_strategy_notional_usdt"])
    if cap_set != tradable_set:
        raise ValueError(f"CAL max_symbol_strategy_notional_usdt keys must match tradable_symbols | {path}")
    override_symbols = set(out["ladder"]["symbol_levels"])
    if not override_symbols.issubset(tradable_set):
        raise ValueError(f"CAL ladder.symbol_levels keys must be subset of tradable_symbols: {sorted(override_symbols - tradable_set)} | {path}")
    tp_override_symbols = set(out["exit_policy"]["symbol_take_profit_pct"])
    if tp_override_symbols != tradable_set:
        raise ValueError(f"CAL symbol_take_profit_pct keys must match tradable_symbols | {path}")
    leverage_symbols = set(out["execution"]["symbol_leverage"])
    if leverage_symbols != tradable_set:
        raise ValueError(f"CAL symbol_leverage keys must match tradable_symbols | {path}")
    for symbol in sorted(tradable_set):
        total_level_notional = sum(float(row["notional_usdt"]) for row in _levels_for_symbol(out, symbol))
        symbol_cap = float(out["risk"]["max_symbol_strategy_notional_usdt"][symbol])
        if symbol_cap < total_level_notional:
            raise ValueError(
                f"CAL max_symbol_strategy_notional_usdt must cover all configured levels: {symbol} | {path}"
            )
        if symbol_cap > float(out["risk"]["max_total_strategy_notional_usdt"]):
            raise ValueError(
                f"CAL max_symbol_strategy_notional_usdt must be <= max_total_strategy_notional_usdt: {symbol} | {path}"
            )
    return out


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


def _load_state(account: str) -> dict[str, Any]:
    path = _state_path(account)
    if not path.exists():
        return {
            "schema_version": 1,
            "strategy_name": STRATEGY_NAME,
            "account": str(account),
            "state_exists": False,
            "symbols": {},
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"CAL state must be JSON object: {path}")
    if int(payload.get("schema_version", 0)) != 1:
        raise ValueError(f"CAL state schema_version must be 1: {path}")
    if str(payload.get("strategy_name") or "").lower() != STRATEGY_NAME:
        raise ValueError(f"CAL state strategy_name mismatch: {path}")
    if str(payload.get("account") or "").strip() != str(account).strip():
        raise ValueError(f"CAL state account mismatch: {path}")
    symbols = payload.get("symbols")
    if not isinstance(symbols, dict):
        raise TypeError(f"CAL state symbols must be object: {path}")
    out = dict(payload)
    out["state_exists"] = True
    return out


def _state_symbol(state: Mapping[str, Any], symbol: str) -> dict[str, Any]:
    symbols = state.get("symbols")
    if not isinstance(symbols, Mapping):
        return {}
    value = symbols.get(str(symbol).upper().strip())
    return dict(value) if isinstance(value, Mapping) else {}


def _state_open_lots(symbol_state: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = symbol_state.get("open_lots")
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise TypeError("CAL symbol state open_lots must be list")
    return [dict(item) for item in raw if isinstance(item, Mapping)]


def _state_closed_lots(symbol_state: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = symbol_state.get("closed_lots")
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise TypeError("CAL symbol state closed_lots must be list")
    return [dict(item) for item in raw if isinstance(item, Mapping)]


def _levels_for_symbol(cfg: Mapping[str, Any], symbol: str) -> list[dict[str, Any]]:
    symbol_key = str(symbol).upper().strip()
    symbol_levels = cfg.get("ladder", {}).get("symbol_levels", {})
    if isinstance(symbol_levels, Mapping) and symbol_key in symbol_levels:
        return [dict(row) for row in symbol_levels[symbol_key]]
    return [dict(row) for row in cfg["ladder"]["levels"]]


def _repeat_counts(symbol_state: Mapping[str, Any]) -> dict[str, int]:
    raw = symbol_state.get("repeat_counts", {})
    if raw in (None, ""):
        return _derive_repeat_counts_from_lots(symbol_state)
    if not isinstance(raw, Mapping):
        raise TypeError("CAL symbol state repeat_counts must be object")
    counts: dict[str, int] = {}
    for raw_level, raw_count in raw.items():
        level = str(raw_level or "").upper().strip()
        if level not in REPEAT_SHIFT_LEVELS:
            raise ValueError(f"CAL symbol state repeat_counts unsupported level: {level!r}")
        if isinstance(raw_count, bool):
            raise TypeError(f"CAL symbol state repeat_counts value must be int: {level}")
        try:
            count = int(raw_count)
        except Exception as exc:
            raise TypeError(f"CAL symbol state repeat_counts value must be int: {level}") from exc
        if count < 0:
            raise ValueError(f"CAL symbol state repeat_counts value must be >= 0: {level}")
        counts[level] = int(count)
    if not counts:
        return _derive_repeat_counts_from_lots(symbol_state)
    return counts


def _derive_repeat_counts_from_lots(symbol_state: Mapping[str, Any]) -> dict[str, int]:
    open_lots = _state_open_lots(symbol_state)
    p1_lot = next((lot for lot in open_lots if str(lot.get("level") or "").upper().strip() == "P1"), None)
    if not isinstance(p1_lot, Mapping):
        return {}
    p1_opened = str(p1_lot.get("opened_bj") or "").strip()
    if not p1_opened:
        return {}
    counts: dict[str, int] = {}
    for lot in _state_closed_lots(symbol_state):
        level = str(lot.get("level") or "").upper().strip()
        if level not in REPEAT_SHIFT_LEVELS:
            continue
        if str(lot.get("exit_reason") or "").upper().strip() != "TAKE_PROFIT":
            continue
        opened = str(lot.get("opened_bj") or "").strip()
        if opened and opened >= p1_opened:
            counts[level] = counts.get(level, 0) + 1
    return counts


def _effective_drop_info(levels: list[dict[str, Any]], target_level: str, repeat_counts: Mapping[str, int]) -> dict[str, Any]:
    target = str(target_level or "").upper().strip()
    if target not in LEVEL_ORDER:
        raise ValueError(f"CAL unsupported target level for repeat shift: {target!r}")
    target_row = next((row for row in levels if str(row.get("level") or "").upper() == target), None)
    if target_row is None:
        raise ValueError(f"CAL target level not configured for repeat shift: {target}")
    base_drop_pct = float(target_row["drop_pct"])
    shift_components: list[dict[str, Any]] = []
    shift_pct = 0.0
    for row in levels:
        level = str(row.get("level") or "").upper().strip()
        if level not in REPEAT_SHIFT_LEVELS:
            continue
        if LEVEL_ORDER[level] > LEVEL_ORDER[target]:
            continue
        count = int(repeat_counts.get(level, 0) or 0)
        step = float(row["repeat_drop_step_pct"])
        component = float(count) * step
        shift_pct += component
        shift_components.append({
            "level": level,
            "repeat_count": count,
            "repeat_drop_step_pct": step,
            "shift_pct": component,
        })
    return {
        "base_drop_pct": base_drop_pct,
        "repeat_shift_pct": float(shift_pct),
        "effective_drop_pct": float(base_drop_pct + shift_pct),
        "repeat_shift_components": shift_components,
    }


def _take_profit_pct_for_symbol(cfg: Mapping[str, Any], symbol: str) -> float:
    symbol_key = str(symbol).upper().strip()
    return float(cfg["exit_policy"]["symbol_take_profit_pct"][symbol_key])


def _level_map(cfg: Mapping[str, Any], symbol: str) -> dict[str, dict[str, Any]]:
    return {str(row["level"]): dict(row) for row in _levels_for_symbol(cfg, symbol)}


def _next_level(cfg: Mapping[str, Any], symbol: str, existing_levels: set[str]) -> dict[str, Any] | None:
    for row in _levels_for_symbol(cfg, symbol):
        if str(row["level"]) not in existing_levels:
            return dict(row)
    return None


def _validate_open_lots(
    *,
    cfg: Mapping[str, Any],
    symbol: str,
    lots: list[dict[str, Any]],
) -> tuple[list[str], dict[str, Any]]:
    reasons: list[str] = []
    detail: dict[str, Any] = {}
    seen_ids: set[str] = set()
    seen_levels: set[str] = set()
    tp_by_level: dict[str, float] = {}
    level_cfg = _level_map(cfg, symbol)

    for lot in lots:
        lot_id = str(lot.get("lot_id") or "").strip()
        level = str(lot.get("level") or "").upper().strip()
        if not lot_id:
            reasons.append("open_lot_missing_lot_id")
        elif lot_id in seen_ids:
            reasons.append("duplicate_lot_id")
        seen_ids.add(lot_id)
        if level not in level_cfg:
            reasons.append("open_lot_level_not_configured")
        elif level in seen_levels:
            reasons.append("duplicate_active_level")
        seen_levels.add(level)
        entry_price = _as_float(lot.get("entry_price"))
        tp_price = _as_float(lot.get("tp_price"))
        qty = _as_float(lot.get("entry_qty"))
        if entry_price is None or entry_price <= 0:
            reasons.append("open_lot_invalid_entry_price")
        if qty is None or qty <= 0:
            reasons.append("open_lot_invalid_entry_qty")
        if tp_price is None or tp_price <= 0:
            reasons.append("open_lot_invalid_tp_price")
        elif level:
            tp_by_level[level] = float(tp_price)

    ordered_tp = [(level, tp_by_level[level]) for level in ("P3", "P2", "P1") if level in tp_by_level]
    for idx in range(1, len(ordered_tp)):
        prev_level, prev_tp = ordered_tp[idx - 1]
        level, tp = ordered_tp[idx]
        if not prev_tp < tp:
            reasons.append("tp_price_monotonicity_violation")
            detail["tp_monotonicity_violation"] = {
                "lower_level": prev_level,
                "lower_tp": prev_tp,
                "higher_level": level,
                "higher_tp": tp,
            }
            break

    levels = set(tp_by_level)
    if "P2" in levels and "P1" not in levels:
        reasons.append("p2_without_active_p1")
    if "P3" in levels and "P1" not in levels:
        reasons.append("p3_without_active_p1")
    detail["symbol"] = symbol
    detail["active_levels"] = sorted(seen_levels)
    detail["tp_by_level"] = tp_by_level
    return sorted(set(reasons)), detail


def _cal_open_notional(lots: list[Mapping[str, Any]]) -> float:
    total = 0.0
    for lot in lots:
        value = _as_float(lot.get("entry_notional_usdt"))
        if value is not None and value > 0:
            total += float(value)
    return float(total)


def _cal_open_qty(lots: list[Mapping[str, Any]]) -> float:
    total = 0.0
    for lot in lots:
        value = _as_float(lot.get("entry_qty"))
        if value is not None and value > 0:
            total += float(value)
    return float(total)


def _total_open_notional(state: Mapping[str, Any]) -> float:
    symbols = state.get("symbols")
    if not isinstance(symbols, Mapping):
        return 0.0
    total = 0.0
    for symbol_state in symbols.values():
        if isinstance(symbol_state, Mapping):
            total += _cal_open_notional(_state_open_lots(symbol_state))
    return float(total)


def _is_cal_client_order_id(client_order_id: Any) -> bool:
    parsed = parse_client_order_id(str(client_order_id or ""))
    return bool(parsed.get("recognized")) and str(parsed.get("strat") or "").upper() == STRATEGY_CODE


def _order_client_id(order: Mapping[str, Any]) -> str:
    return str(order.get("client_order_id") or order.get("clientOrderId") or order.get("origClientOrderId") or "").strip()


def _fetch_best_bid(account: str, symbol: str, limit: int) -> dict[str, Any]:
    raw = call_client_method(
        account,
        source="cal_decision_audit.futures_order_book",
        method_name="futures_order_book",
        priority=REQUEST_PRIORITY_LOW,
        symbol=str(symbol).upper().strip(),
        limit=int(limit),
    )
    if not isinstance(raw, dict):
        raise RuntimeError(f"CAL futures_order_book payload must be object: {symbol}")
    bids = raw.get("bids")
    if not isinstance(bids, list) or not bids:
        raise RuntimeError(f"CAL futures_order_book bids missing: {symbol}")
    first = bids[0]
    if not isinstance(first, (list, tuple)) or len(first) < 2:
        raise RuntimeError(f"CAL futures_order_book best bid malformed: {symbol}")
    price = _as_float(first[0])
    qty = _as_float(first[1])
    if price is None or price <= 0:
        raise RuntimeError(f"CAL futures_order_book best bid price invalid: {symbol}")
    return {"price": float(price), "qty": qty, "raw": raw}


def _fetch_klines(
    account: str,
    symbol: str,
    *,
    start_ms: int,
    end_ms: int,
    interval: str,
    limit: int,
) -> list[list[Any]]:
    rows: list[list[Any]] = []
    cursor = int(start_ms)
    while cursor <= int(end_ms):
        batch = call_client_method(
            account,
            source="cal_decision_audit.futures_klines",
            method_name="futures_klines",
            priority=REQUEST_PRIORITY_LOW,
            symbol=str(symbol).upper().strip(),
            interval=str(interval),
            startTime=int(cursor),
            endTime=int(end_ms),
            limit=int(limit),
        )
        if not isinstance(batch, list):
            raise TypeError(f"CAL futures_klines payload must be list: {symbol}")
        clean = [list(row) for row in batch if isinstance(row, (list, tuple)) and len(row) > KLINE_CLOSE_INDEX]
        clean.sort(key=lambda row: int(row[KLINE_OPEN_TIME_INDEX]))
        if not clean:
            break
        rows.extend(clean)
        last_open = _as_int(clean[-1][KLINE_OPEN_TIME_INDEX])
        if last_open is None:
            raise ValueError(f"CAL futures_klines row missing open_time: {symbol}")
        next_cursor = int(last_open) + _interval_ms(interval)
        if next_cursor <= int(cursor):
            raise RuntimeError(f"CAL futures_klines cursor did not advance: {symbol}")
        cursor = next_cursor
        if len(clean) < int(limit):
            break

    deduped: dict[int, list[Any]] = {}
    for row in rows:
        open_time = _as_int(row[KLINE_OPEN_TIME_INDEX])
        if open_time is not None and int(start_ms) <= int(open_time) <= int(end_ms):
            deduped[int(open_time)] = row
    return [deduped[k] for k in sorted(deduped)]


def _load_anchor_cache() -> dict[str, Any]:
    path = _anchor_cache_path()
    if not path.exists():
        return {"schema_version": 1, "anchors": {}}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"CAL H anchor cache must be object: {path}")
    if int(payload.get("schema_version", 0)) != 1:
        raise ValueError(f"CAL H anchor cache schema_version must be 1: {path}")
    anchors = payload.get("anchors")
    if not isinstance(anchors, dict):
        raise TypeError(f"CAL H anchor cache anchors must be object: {path}")
    return dict(payload)


def _save_anchor_cache(cache: Mapping[str, Any]) -> None:
    path = _anchor_cache_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(path) + ".lock")
    with lock:
        tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
        tmp.write_text(json.dumps(dict(cache), ensure_ascii=False, indent=2, default=_json_default) + "\n", encoding="utf-8")
        os.replace(tmp, path)


def _anchor_cache_key(account: str, symbol: str) -> str:
    return f"{str(account).strip()}:{str(symbol).upper().strip()}"


def _cached_h_anchor(
    *,
    cfg: Mapping[str, Any],
    account: str,
    symbol: str,
    anchor_hour_open_ms: int,
    now_ms: int,
) -> dict[str, Any] | None:
    cache = _load_anchor_cache()
    anchors = cache.get("anchors")
    if not isinstance(anchors, Mapping):
        return None
    row = anchors.get(_anchor_cache_key(account, symbol))
    if not isinstance(row, Mapping):
        return None
    if str(row.get("klines_interval") or "") != str(cfg["data"]["klines_interval"]):
        return None
    if int(row.get("lookback_hours") or 0) != int(cfg["data"]["lookback_hours"]):
        return None
    if bool(row.get("include_current_bar")) != bool(cfg["data"]["include_current_bar"]):
        return None
    if int(row.get("anchor_hour_open_time_ms") or 0) != int(anchor_hour_open_ms):
        return None
    refreshed = _as_int(row.get("refreshed_utc_ms"))
    if refreshed is None or int(now_ms) - int(refreshed) > int(cfg["data"]["h_anchor_refresh_secs"]) * 1000:
        return None
    out = dict(row)
    out["cache_hit"] = True
    return out


def _store_h_anchor(account: str, symbol: str, anchor: Mapping[str, Any]) -> None:
    cache = _load_anchor_cache()
    anchors = cache.get("anchors")
    if not isinstance(anchors, dict):
        anchors = {}
    anchors[_anchor_cache_key(account, symbol)] = dict(anchor)
    cache["schema_version"] = 1
    cache["anchors"] = anchors
    _save_anchor_cache(cache)


def _calc_h_anchor(
    account: str,
    symbol: str,
    cfg: Mapping[str, Any],
    *,
    now_ms: int,
    force_refresh: bool,
) -> dict[str, Any]:
    interval = str(cfg["data"]["klines_interval"])
    step_ms = _interval_ms(interval)
    lookback_bars = int(cfg["data"]["lookback_hours"])
    current_bar_open = (int(now_ms) // step_ms) * step_ms
    include_current_bar = bool(cfg["data"]["include_current_bar"])
    latest_anchor_open = int(current_bar_open if include_current_bar else current_bar_open - step_ms)
    cached = None if force_refresh else _cached_h_anchor(
        cfg=cfg,
        account=account,
        symbol=symbol,
        anchor_hour_open_ms=int(latest_anchor_open),
        now_ms=int(now_ms),
    )
    if cached is not None:
        return cached
    start_ms = int(latest_anchor_open) - (lookback_bars - 1) * step_ms
    fetch_start_ms = int(start_ms - step_ms if include_current_bar else start_ms)
    rows = _fetch_klines(
        account,
        symbol,
        start_ms=fetch_start_ms,
        end_ms=int(now_ms if include_current_bar else latest_anchor_open + step_ms - 1),
        interval=interval,
        limit=int(cfg["data"]["kline_limit"]),
    )
    if len(rows) < lookback_bars:
        raise RuntimeError(f"CAL insufficient 48h 1h bars: {symbol} | {len(rows)} < {lookback_bars}")
    rows = rows[-lookback_bars:]
    latest_anchor_open = int(rows[-1][KLINE_OPEN_TIME_INDEX])
    start_ms = int(latest_anchor_open) - (lookback_bars - 1) * step_ms
    expected = start_ms
    highs: list[float] = []
    h_time: int | None = None
    h_value: float | None = None
    for row in rows:
        open_time = _as_int(row[KLINE_OPEN_TIME_INDEX])
        if open_time != expected:
            raise RuntimeError(f"CAL non-contiguous 1h bars: {symbol} | expected={expected} got={open_time}")
        high = _as_float(row[KLINE_HIGH_INDEX])
        close = _as_float(row[KLINE_CLOSE_INDEX])
        if high is None or high <= 0 or close is None or close <= 0:
            raise RuntimeError(f"CAL invalid kline OHLC: {symbol} | open_time={open_time}")
        highs.append(float(high))
        if h_value is None or float(high) > h_value:
            h_value = float(high)
            h_time = int(open_time)
        expected += step_ms
    anchor = {
        "lookback_hours": int(cfg["data"]["lookback_hours"]),
        "klines_interval": interval,
        "include_current_bar": include_current_bar,
        "cache_hit": False,
        "refreshed_utc_ms": int(now_ms),
        "refreshed_bj": _fmt_bj_from_ms(now_ms),
        "anchor_hour_open_time_ms": int(latest_anchor_open),
        "anchor_hour_open_bj": _fmt_bj_from_ms(latest_anchor_open),
        "bar_count": len(rows),
        "start_open_time_ms": int(start_ms),
        "start_open_bj": _fmt_bj_from_ms(start_ms),
        "latest_anchor_open_time_ms": int(latest_anchor_open),
        "latest_anchor_open_bj": _fmt_bj_from_ms(latest_anchor_open),
        "h_price": float(h_value or max(highs)),
        "h_open_time_ms": int(h_time) if h_time is not None else None,
        "h_open_bj": _fmt_bj_from_ms(h_time),
    }
    _store_h_anchor(account, symbol, anchor)
    return anchor


def _account_facts(account: str, symbol: str) -> dict[str, Any]:
    position_res = get_position(account, symbol, POSITION_SIDE_LONG)
    if not position_res.get("ok"):
        raise RuntimeError(f"CAL position query failed: {symbol} | {position_res.get('reason')}")
    orders_res = get_open_orders(account, symbol)
    if not orders_res.get("ok"):
        raise RuntimeError(f"CAL open orders query failed: {symbol} | {orders_res.get('reason')}")
    position = position_res.get("data") if isinstance(position_res.get("data"), Mapping) else None
    orders = [dict(row) for row in (orders_res.get("data") or []) if isinstance(row, Mapping)]
    return {"position": position, "open_orders": orders}


def _position_qty(position: Any) -> float:
    if not isinstance(position, Mapping):
        return 0.0
    return float(position.get("qty") or 0.0)


def _build_symbol_decision(
    *,
    cfg: Mapping[str, Any],
    state: Mapping[str, Any],
    symbol: str,
    now_ms: int,
    force_anchor_refresh: bool,
) -> dict[str, Any]:
    account = str(cfg["account"])
    symbol_state = _state_symbol(state, symbol)
    open_lots = _state_open_lots(symbol_state)
    repeat_counts = _repeat_counts(symbol_state)
    invariant_reasons, invariant_detail = _validate_open_lots(cfg=cfg, symbol=symbol, lots=open_lots)
    paused_status = str(symbol_state.get("status") or "").upper().strip()
    paused_by_state = paused_status == "PAUSED_BY_INVARIANT_VIOLATION"
    account_facts = _account_facts(account, symbol)
    position = account_facts["position"]
    open_orders = account_facts["open_orders"]
    open_order_client_ids = [_order_client_id(order) for order in open_orders]
    cal_open_orders = [order for order in open_orders if _is_cal_client_order_id(_order_client_id(order))]
    non_cal_open_orders = [order for order in open_orders if not _is_cal_client_order_id(_order_client_id(order))]

    cal_qty = _cal_open_qty(open_lots)
    position_qty = _position_qty(position)
    external_p0_qty = max(0.0, position_qty - cal_qty)
    if position_qty + 1e-12 < cal_qty:
        invariant_reasons.append("position_qty_below_cal_open_lot_qty")
    if cal_open_orders:
        known_cal_cids = {
            str(lot.get("entry_client_order_id") or "").strip()
            for lot in open_lots
        } | {
            str(lot.get("tp_client_order_id") or "").strip()
            for lot in open_lots
        }
        unknown_cal_orders = [
            order for order in cal_open_orders if _order_client_id(order) and _order_client_id(order) not in known_cal_cids
        ]
        if unknown_cal_orders:
            invariant_reasons.append("cal_open_order_missing_from_state")
    else:
        unknown_cal_orders = []

    if invariant_reasons or paused_by_state:
        return {
            "symbol": symbol,
            "status": "paused_by_invariant_violation" if (invariant_reasons or paused_by_state) else "blocked",
            "ready": False,
            "block_reasons": sorted(set(invariant_reasons + (["state_paused_by_invariant_violation"] if paused_by_state else []))),
            "invariant_detail": invariant_detail,
            "account_facts": {
                "long_position_qty": position_qty,
                "external_p0_qty_estimated": external_p0_qty,
                "open_order_client_ids": open_order_client_ids,
                "unknown_cal_order_count": len(unknown_cal_orders),
                "non_cal_open_order_count": len(non_cal_open_orders),
            },
            "open_lots": open_lots,
            "repeat_counts": repeat_counts,
        }

    h_anchor = _calc_h_anchor(account, symbol, cfg, now_ms=now_ms, force_refresh=force_anchor_refresh)
    best_bid = _fetch_best_bid(account, symbol, int(cfg["execution"]["order_book_limit"]))
    current_price = float(best_bid["price"])
    existing_levels = {str(lot.get("level") or "").upper().strip() for lot in open_lots}
    next_level = _next_level(cfg, symbol, existing_levels)
    p1_lot = next((lot for lot in open_lots if str(lot.get("level") or "").upper() == "P1"), None)

    if non_cal_open_orders:
        block_reason = "non_cal_open_orders_present"
    elif next_level is None:
        block_reason = "all_configured_levels_open"
    else:
        block_reason = ""

    anchor_type = "H_48H"
    anchor_price = float(h_anchor["h_price"])
    if open_lots:
        if not isinstance(p1_lot, Mapping):
            block_reason = block_reason or "active_ladder_missing_p1"
            anchor_type = "INVALID"
            anchor_price = 0.0
        else:
            p1_entry = _as_float(p1_lot.get("entry_price"))
            if p1_entry is None or p1_entry <= 0:
                block_reason = block_reason or "active_p1_entry_price_invalid"
                anchor_type = "INVALID"
                anchor_price = 0.0
            else:
                anchor_type = "P1_ENTRY"
                anchor_price = float(p1_entry)

    trigger_price = None
    next_notional = None
    tp_price_estimate = None
    drop_info = None
    if next_level is not None and anchor_price > 0:
        drop_info = _effective_drop_info(_levels_for_symbol(cfg, symbol), str(next_level["level"]), repeat_counts)
        trigger_price = anchor_price * (1.0 - float(drop_info["effective_drop_pct"]))
        next_notional = float(next_level["notional_usdt"])
        tp_price_estimate = current_price * (1.0 + _take_profit_pct_for_symbol(cfg, symbol))

    open_symbol_notional = _cal_open_notional(open_lots)
    open_total_notional = _total_open_notional(state)
    if next_notional is not None:
        symbol_cap = float(cfg["risk"]["max_symbol_strategy_notional_usdt"][symbol])
        total_cap = float(cfg["risk"]["max_total_strategy_notional_usdt"])
        if open_symbol_notional + next_notional > symbol_cap + 1e-9:
            block_reason = block_reason or "max_symbol_strategy_notional_exceeded"
        if open_total_notional + next_notional > total_cap + 1e-9:
            block_reason = block_reason or "max_total_strategy_notional_exceeded"

    ready = bool(
        not block_reason
        and next_level is not None
        and trigger_price is not None
        and current_price <= float(trigger_price)
    )
    status = "entry_ready" if ready else "blocked"
    block_reasons = [] if ready else [block_reason or "price_above_trigger"]
    intent = None
    if ready and next_level is not None and trigger_price is not None and next_notional is not None:
        intent = {
            "intent_type": "POST_ONLY_MAKER_BUY_AUDIT_ONLY",
            "symbol": symbol,
            "level": str(next_level["level"]),
            "anchor_type": anchor_type,
            "anchor_price": float(anchor_price),
            "trigger_price": float(trigger_price),
            "base_drop_pct": float(drop_info["base_drop_pct"]) if isinstance(drop_info, Mapping) else float(next_level["drop_pct"]),
            "repeat_shift_pct": float(drop_info["repeat_shift_pct"]) if isinstance(drop_info, Mapping) else 0.0,
            "effective_drop_pct": float(drop_info["effective_drop_pct"]) if isinstance(drop_info, Mapping) else float(next_level["drop_pct"]),
            "repeat_counts": dict(repeat_counts),
            "repeat_shift_components": list(drop_info["repeat_shift_components"]) if isinstance(drop_info, Mapping) else [],
            "current_price_source": "BEST_BID",
            "current_price": float(current_price),
            "proposed_order_notional_usdt": float(next_notional),
            "take_profit_pct": _take_profit_pct_for_symbol(cfg, symbol),
            "estimated_tp_price": float(tp_price_estimate),
        }

    return {
        "symbol": symbol,
        "status": status,
        "ready": ready,
        "block_reasons": block_reasons,
        "next_level": dict(next_level) if next_level is not None else None,
        "anchor_type": anchor_type,
        "anchor_price": anchor_price if anchor_price > 0 else None,
        "trigger_price": trigger_price,
        "base_drop_pct": float(drop_info["base_drop_pct"]) if isinstance(drop_info, Mapping) else None,
        "repeat_shift_pct": float(drop_info["repeat_shift_pct"]) if isinstance(drop_info, Mapping) else None,
        "effective_drop_pct": float(drop_info["effective_drop_pct"]) if isinstance(drop_info, Mapping) else None,
        "repeat_counts": repeat_counts,
        "repeat_shift_components": list(drop_info["repeat_shift_components"]) if isinstance(drop_info, Mapping) else [],
        "current_price_source": "BEST_BID",
        "current_price": current_price,
        "estimated_tp_price": tp_price_estimate,
        "h_anchor": h_anchor,
        "risk": {
            "open_symbol_strategy_notional_usdt": float(open_symbol_notional),
            "open_total_strategy_notional_usdt": float(open_total_notional),
            "next_notional_usdt": next_notional,
            "max_symbol_strategy_notional_usdt": float(cfg["risk"]["max_symbol_strategy_notional_usdt"][symbol]),
            "max_total_strategy_notional_usdt": float(cfg["risk"]["max_total_strategy_notional_usdt"]),
        },
        "account_facts": {
            "long_position_qty": position_qty,
            "external_p0_qty_estimated": external_p0_qty,
            "open_order_client_ids": open_order_client_ids,
            "cal_open_order_count": len(cal_open_orders),
            "non_cal_open_order_count": len(non_cal_open_orders),
        },
        "open_lots": open_lots,
        "intent": intent,
    }


def build_decision_audit(
    *,
    cfg: Mapping[str, Any],
    run_id: str,
    write_audit: bool = True,
    force_anchor_refresh: bool = False,
) -> dict[str, Any]:
    if not bool(cfg["enabled"]):
        raise RuntimeError("CAL decision audit config enabled=false")
    now_ms = _now_utc_ms()
    state = _load_state(str(cfg["account"]))
    decisions: list[dict[str, Any]] = []
    selected_intents: list[dict[str, Any]] = []
    for symbol in cfg["universe"]["tradable_symbols"]:
        decision = _build_symbol_decision(
            cfg=cfg,
            state=state,
            symbol=str(symbol),
            now_ms=now_ms,
            force_anchor_refresh=force_anchor_refresh,
        )
        decisions.append(decision)
        intent = decision.get("intent")
        if isinstance(intent, Mapping):
            selected_intents.append(dict(intent))

    max_candidates = int(cfg["decision"]["max_candidates"])
    selected_intents = selected_intents[:max_candidates]
    record = _base_record(run_id, "cal_decision_audit")
    record.update(
        {
            "account": str(cfg["account"]),
            "state_path": str(_state_path(str(cfg["account"]))),
            "state_exists": bool(state.get("state_exists")),
            "symbols": list(cfg["universe"]["tradable_symbols"]),
            "decision_count": len(decisions),
            "selected_count": len(selected_intents),
            "decisions": decisions,
            "selected_intents": selected_intents,
        }
    )
    if write_audit and bool(cfg["audit_enabled"]):
        path = _append_jsonl(_audit_path(day_bj=str(record["collected_bj"])[:10]), record)
        record["audit_path"] = str(path)
    return record


def _make_run_id() -> str:
    return f"CAL_DECISION_AUDIT_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def main() -> None:
    parser = argparse.ArgumentParser(description="Core Anchor Ladder dry-run decision audit")
    parser.add_argument("--config", default="strategies/cal/config.decision_audit.json")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--no-audit-write", action="store_true")
    parser.add_argument("--force-anchor-refresh", action="store_true")
    args = parser.parse_args()
    setup_logging()
    if bool(args.once) == bool(args.loop):
        raise SystemExit("Specify exactly one of --once or --loop")
    cfg = load_config(str(args.config))
    run_id = str(args.run_id or _make_run_id())
    logging.info("CAL decision audit started | run_id=%s | config=%s", run_id, args.config)
    while True:
        record = build_decision_audit(
            cfg=cfg,
            run_id=run_id,
            write_audit=not bool(args.no_audit_write),
            force_anchor_refresh=bool(args.force_anchor_refresh),
        )
        logging.info(
            "CAL decision audit iteration | run_id=%s | decisions=%s | selected=%s | audit=%s",
            run_id,
            record.get("decision_count"),
            record.get("selected_count"),
            record.get("audit_path"),
        )
        if args.once:
            break
        time.sleep(int(cfg["collection"]["interval_secs"]))


if __name__ == "__main__":
    main()
