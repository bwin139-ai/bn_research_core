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

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core.runtime_state import get_state_dir
from core.live.binance_rest_gateway import (
    BinanceRestGatewayRejected,
    REQUEST_PRIORITY_LOW,
    call_client_method,
)

BJ = timezone(timedelta(hours=8))
STRATEGY_NAME = "tvr"
DATA_HUB_PRODUCER = "tvr_data_hub"


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


def _audit_path(*, day_bj: str | None = None) -> Path:
    day_key = str(day_bj or "").strip() or _bj_day_from_ms(None)
    return get_state_dir() / "live_audit" / "tvr" / "decision" / day_key / "tvr_decision_audit.jsonl"


def _percentile_state_path() -> Path:
    return get_state_dir() / "live_audit" / "tvr" / "decision" / "percentile_threshold_state.json"


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
        raise FileNotFoundError(f"TVR decision audit config missing: {path}")
    with p.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise TypeError(f"TVR decision audit config must be JSON object: {path}")
    return payload


def _require_mapping(cfg: Mapping[str, Any], path: str, key: str) -> dict[str, Any]:
    if key not in cfg:
        raise KeyError(f"TVR decision audit config missing required section: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, dict):
        raise TypeError(f"TVR decision audit config section must be object: {key} | {path}")
    return dict(value)


def _require_bool(cfg: Mapping[str, Any], path: str, key: str) -> bool:
    if key not in cfg:
        raise KeyError(f"TVR decision audit config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, bool):
        raise TypeError(f"TVR decision audit config field must be bool: {key} | {path}")
    return bool(value)


def _require_non_empty_str(cfg: Mapping[str, Any], path: str, key: str) -> str:
    if key not in cfg:
        raise KeyError(f"TVR decision audit config missing required field: {key} | {path}")
    value = str(cfg[key]).strip()
    if not value:
        raise ValueError(f"TVR decision audit config field must not be empty: {key} | {path}")
    return value


def _require_int(cfg: Mapping[str, Any], path: str, key: str, *, positive: bool) -> int:
    if key not in cfg:
        raise KeyError(f"TVR decision audit config missing required field: {key} | {path}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"TVR decision audit config field must be int: {key} | {path}")
    try:
        out = int(value)
    except Exception as exc:
        raise TypeError(f"TVR decision audit config field must be int: {key} | {path}") from exc
    if positive and out <= 0:
        raise ValueError(f"TVR decision audit config field must be > 0: {key} | {path}")
    if not positive and out < 0:
        raise ValueError(f"TVR decision audit config field must be >= 0: {key} | {path}")
    return out


def _require_float(cfg: Mapping[str, Any], path: str, key: str, *, positive: bool) -> float:
    if key not in cfg:
        raise KeyError(f"TVR decision audit config missing required field: {key} | {path}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"TVR decision audit config field must be number: {key} | {path}")
    try:
        out = float(value)
    except Exception as exc:
        raise TypeError(f"TVR decision audit config field must be number: {key} | {path}") from exc
    if math.isnan(out) or math.isinf(out):
        raise ValueError(f"TVR decision audit config field must be finite: {key} | {path}")
    if positive and out <= 0:
        raise ValueError(f"TVR decision audit config field must be > 0: {key} | {path}")
    if not positive and out < 0:
        raise ValueError(f"TVR decision audit config field must be >= 0: {key} | {path}")
    return out


def _require_percentile(cfg: Mapping[str, Any], path: str, key: str) -> str:
    value = _require_non_empty_str(cfg, path, key).lower()
    if value not in {"p1", "p5", "p10", "p20"}:
        raise ValueError(f"TVR decision audit unsupported percentile: {value} | {path}")
    return value


def _require_symbol_list(cfg: Mapping[str, Any], path: str, key: str) -> list[str]:
    if key not in cfg:
        raise KeyError(f"TVR decision audit config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, list):
        raise TypeError(f"TVR decision audit config field must be list: {key} | {path}")
    out: list[str] = []
    seen: set[str] = set()
    for raw in value:
        symbol = str(raw).upper().strip()
        if not symbol:
            raise ValueError(f"TVR decision audit config contains empty symbol: {key} | {path}")
        if symbol in seen:
            raise ValueError(f"TVR decision audit config duplicated symbol: {symbol} | {path}")
        seen.add(symbol)
        out.append(symbol)
    if not out:
        raise ValueError(f"TVR decision audit config field must not be empty: {key} | {path}")
    return out


def load_config(path: str) -> dict[str, Any]:
    cfg = _load_json(path)
    if int(cfg.get("schema_version", 0)) != 1:
        raise ValueError(f"TVR decision audit config schema_version must be 1 | {path}")
    data_hub = _require_mapping(cfg, path, "data_hub")
    universe = _require_mapping(cfg, path, "universe")
    collection = _require_mapping(cfg, path, "collection")
    decision = _require_mapping(cfg, path, "decision")
    risk = _require_mapping(cfg, path, "risk")
    out = {
        "schema_version": 1,
        "enabled": _require_bool(cfg, path, "enabled"),
        "account": _require_non_empty_str(cfg, path, "account"),
        "audit_enabled": _require_bool(cfg, path, "audit_enabled"),
        "data_hub": {
            "max_age_secs": _require_int(data_hub, path, "max_age_secs", positive=True),
            "min_symbol_count": _require_int(data_hub, path, "min_symbol_count", positive=True),
        },
        "universe": {
            "tradable_symbols": _require_symbol_list(universe, path, "tradable_symbols"),
        },
        "collection": {
            "interval_secs": _require_int(collection, path, "interval_secs", positive=True),
        },
        "decision": {
            "entry_percentile": _require_percentile(decision, path, "entry_percentile"),
            "percentile_refresh_secs": _require_int(decision, path, "percentile_refresh_secs", positive=True),
            "funding_rate_entry_max": _require_float(decision, path, "funding_rate_entry_max", positive=True),
            "take_profit_pct": _require_float(decision, path, "take_profit_pct", positive=True),
            "max_candidates": _require_int(decision, path, "max_candidates", positive=True),
            "min_quote_volume_24h": _require_float(decision, path, "min_quote_volume_24h", positive=False),
        },
        "risk": {
            "proposed_order_notional_usdt": _require_float(risk, path, "proposed_order_notional_usdt", positive=True),
            "max_symbol_notional_usdt": _require_float(risk, path, "max_symbol_notional_usdt", positive=True),
            "max_total_notional_usdt": _require_float(risk, path, "max_total_notional_usdt", positive=True),
        },
    }
    proposed = float(out["risk"]["proposed_order_notional_usdt"])
    symbol_cap = float(out["risk"]["max_symbol_notional_usdt"])
    total_cap = float(out["risk"]["max_total_notional_usdt"])
    if proposed > symbol_cap:
        raise ValueError(f"TVR proposed_order_notional_usdt must be <= max_symbol_notional_usdt | {path}")
    if symbol_cap > total_cap:
        raise ValueError(f"TVR max_symbol_notional_usdt must be <= max_total_notional_usdt | {path}")
    return out


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    tmp_path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2, default=_json_default) + "\n", encoding="utf-8")
    os.replace(tmp_path, path)


def _data_hub_stream_path(stream: str, *, day_bj: str) -> Path:
    stream_key = str(stream).strip()
    if not stream_key:
        raise ValueError("stream must not be empty")
    return get_state_dir() / "live_audit" / "tvr" / "data_hub" / stream_key / day_bj / f"tradfi_{stream_key}.jsonl"


def _iter_recent_stream_files(stream: str) -> list[Path]:
    root = get_state_dir() / "live_audit" / "tvr" / "data_hub" / str(stream).strip()
    if not root.exists():
        return []
    return sorted(root.glob(f"*/tradfi_{stream}.jsonl"), key=lambda p: (p.stat().st_mtime, str(p)))


def _load_latest_jsonl_record(stream: str) -> tuple[dict[str, Any], Path]:
    files = _iter_recent_stream_files(stream)
    if not files:
        raise FileNotFoundError(f"TVR data_hub stream file missing: {stream}")
    for path in reversed(files):
        lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
        if not lines:
            continue
        payload = json.loads(lines[-1])
        if not isinstance(payload, dict):
            raise TypeError(f"TVR data_hub latest record must be object: {path}")
        return payload, path
    raise RuntimeError(f"TVR data_hub stream has no records: {stream}")


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


def _require_record_age(record: Mapping[str, Any], *, stream: str, max_age_secs: int) -> int:
    collected = _as_int(record.get("collected_utc_ms"))
    if collected is None or collected <= 0:
        raise ValueError(f"TVR data_hub {stream} latest record missing collected_utc_ms")
    age_ms = _now_utc_ms() - int(collected)
    if age_ms < 0:
        raise ValueError(f"TVR data_hub {stream} latest record collected_utc_ms is in the future")
    if age_ms > int(max_age_secs) * 1000:
        raise RuntimeError(
            f"TVR data_hub {stream} latest record stale: age_secs={age_ms / 1000.0:.1f} "
            f"max_age_secs={max_age_secs}"
        )
    return int(age_ms)


def _require_global_data_hub(record: Mapping[str, Any], *, stream: str) -> None:
    data_scope = str(record.get("data_scope") or "").strip().lower()
    if data_scope != "global":
        raise ValueError(f"TVR data_hub {stream} data_scope must be global: {data_scope!r}")
    producer = str(record.get("producer") or "").strip()
    if producer != DATA_HUB_PRODUCER:
        raise ValueError(f"TVR data_hub {stream} producer mismatch: {producer!r}")


def _map_rows(rows: Any, *, stream: str) -> dict[str, dict[str, Any]]:
    if not isinstance(rows, list):
        raise TypeError(f"TVR data_hub {stream} rows must be list")
    out: dict[str, dict[str, Any]] = {}
    for item in rows:
        if not isinstance(item, dict):
            raise TypeError(f"TVR data_hub {stream} row must be object")
        symbol = str(item.get("symbol") or "").upper().strip()
        if not symbol:
            raise ValueError(f"TVR data_hub {stream} row missing symbol")
        if symbol in out:
            raise ValueError(f"TVR data_hub {stream} duplicated symbol: {symbol}")
        out[symbol] = dict(item)
    return out


def _load_inputs(cfg: Mapping[str, Any]) -> dict[str, Any]:
    max_age_secs = int(cfg["data_hub"]["max_age_secs"])
    streams = {}
    sources = {}
    ages = {}
    for stream in ("universe", "funding", "rolling_24h_stats"):
        record, path = _load_latest_jsonl_record(stream)
        _require_global_data_hub(record, stream=stream)
        ages[stream] = _require_record_age(record, stream=stream, max_age_secs=max_age_secs)
        streams[stream] = record
        sources[stream] = str(path)
    return {"records": streams, "paths": sources, "age_ms": ages}


def _universe_symbols(universe_record: Mapping[str, Any], *, min_symbol_count: int) -> list[str]:
    symbols = universe_record.get("symbols")
    if not isinstance(symbols, list):
        raise TypeError("TVR data_hub universe symbols must be list")
    out: list[str] = []
    seen: set[str] = set()
    for item in symbols:
        if not isinstance(item, dict):
            raise TypeError("TVR data_hub universe symbol item must be object")
        symbol = str(item.get("symbol") or "").upper().strip()
        if not symbol:
            raise ValueError("TVR data_hub universe item missing symbol")
        if symbol in seen:
            raise ValueError(f"TVR data_hub universe duplicated symbol: {symbol}")
        seen.add(symbol)
        out.append(symbol)
    if len(out) < int(min_symbol_count):
        raise RuntimeError(f"TVR universe symbol_count below min_symbol_count: {len(out)} < {min_symbol_count}")
    return sorted(out)


def _reject(reasons: list[str], reason: str) -> None:
    if reason not in reasons:
        reasons.append(reason)


def _call_client(account: str, source: str, method_name: str, **params: Any) -> Any:
    for attempt in range(1, 5):
        try:
            return call_client_method(
                account,
                source=source,
                method_name=method_name,
                priority=REQUEST_PRIORITY_LOW,
                **params,
            )
        except BinanceRestGatewayRejected as exc:
            if attempt >= 4:
                raise
            if str(exc.code).endswith("_QUOTA_CLOSED"):
                now_ms = _now_utc_ms()
                next_minute_ms = int((now_ms // 60_000 + 1) * 60_000)
                sleep_s = max(1.0, (next_minute_ms - now_ms) / 1000.0 + 1.0)
            else:
                sleep_s = min(30.0, float(2 ** (attempt - 1)))
            logging.warning(
                "TVR decision REST Gateway rejected | source=%s | attempt=%s/4 | code=%s | sleep=%.1fs",
                source,
                attempt,
                exc.code,
                sleep_s,
            )
            time.sleep(float(sleep_s))


def _load_percentile_state() -> dict[str, Any] | None:
    path = _percentile_state_path()
    if not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"TVR percentile threshold state must be object: {path}")
    if int(payload.get("schema_version", 0)) != 1:
        raise ValueError(f"TVR percentile threshold state schema_version must be 1: {path}")
    thresholds = payload.get("thresholds")
    if not isinstance(thresholds, dict):
        raise TypeError(f"TVR percentile threshold state missing thresholds object: {path}")
    return payload


def _save_percentile_state(state: Mapping[str, Any]) -> None:
    _atomic_write_json(_percentile_state_path(), dict(state))


def _build_percentile_state(
    *,
    cfg: Mapping[str, Any],
    rolling_by_symbol: Mapping[str, Mapping[str, Any]],
    force_refresh: bool,
) -> dict[str, Any]:
    now_ms = _now_utc_ms()
    refresh_secs = int(cfg["decision"]["percentile_refresh_secs"])
    entry_percentile = str(cfg["decision"]["entry_percentile"])
    existing = _load_percentile_state()
    if not force_refresh and isinstance(existing, dict):
        updated = _as_int(existing.get("updated_utc_ms"))
        if updated is not None and now_ms - int(updated) <= refresh_secs * 1000:
            existing_thresholds = existing.get("thresholds")
            existing_symbols = set(existing_thresholds.keys()) if isinstance(existing_thresholds, dict) else set()
            tradable_symbols = set(str(x).upper().strip() for x in cfg["universe"]["tradable_symbols"])
            if str(existing.get("entry_percentile") or "") == entry_percentile and tradable_symbols.issubset(existing_symbols):
                return existing

    thresholds: dict[str, dict[str, Any]] = {}
    for symbol in cfg["universe"]["tradable_symbols"]:
        rolling = dict(rolling_by_symbol.get(symbol) or {})
        if not rolling:
            raise KeyError(f"TVR rolling_24h_stats missing tradable symbol: {symbol}")
        rolling_stats = rolling.get("rolling_24h")
        if not isinstance(rolling_stats, dict):
            raise TypeError(f"TVR rolling_24h stats missing object: {symbol}")
        if not bool(rolling.get("history_sufficient")):
            thresholds[symbol] = {
                "history_sufficient": False,
                "insufficiency_reason": str(rolling.get("insufficiency_reason") or ""),
                "selected_percentile_return": None,
                "rolling_24h_sample_count": _as_int(rolling_stats.get("sample_count")),
            }
            continue
        selected = _as_float(rolling_stats.get(entry_percentile))
        if selected is None:
            raise ValueError(f"TVR rolling_24h {entry_percentile} missing/unreadable: {symbol}")
        thresholds[symbol] = {
            "history_sufficient": True,
            "insufficiency_reason": "",
            "entry_percentile": entry_percentile,
            "selected_percentile_return": float(selected),
            "rolling_24h_p1": _as_float(rolling_stats.get("p1")),
            "rolling_24h_p5": _as_float(rolling_stats.get("p5")),
            "rolling_24h_p10": _as_float(rolling_stats.get("p10")),
            "rolling_24h_p20": _as_float(rolling_stats.get("p20")),
            "rolling_24h_sample_count": _as_int(rolling_stats.get("sample_count")),
        }
    state = {
        "schema_version": 1,
        "strategy_name": STRATEGY_NAME,
        "entry_percentile": entry_percentile,
        "percentile_refresh_secs": int(refresh_secs),
        "updated_utc_ms": int(now_ms),
        "updated_bj": _fmt_bj_from_ms(now_ms),
        "thresholds": thresholds,
    }
    _save_percentile_state(state)
    return state


def _fetch_live_ticker(account: str, symbol: str) -> dict[str, Any]:
    payload = _call_client(
        account,
        "tvr_decision_audit.futures_ticker",
        "futures_ticker",
        symbol=str(symbol).upper().strip(),
    )
    if not isinstance(payload, dict):
        raise TypeError(f"TVR futures_ticker payload must be object: {symbol}")
    return dict(payload)


def _non_tradable_symbol_row(symbol: str) -> dict[str, Any]:
    return {
        "symbol": str(symbol).upper().strip(),
        "eligible": False,
        "reject_reasons": ["symbol_not_in_tradable_symbols"],
        "intent": None,
        "last_price": None,
        "quote_volume_24h": None,
        "price_change_pct_24h_percent": None,
        "current_24h_return": None,
        "entry_percentile": None,
        "selected_percentile_return": None,
        "funding_rate": None,
        "funding_rate_entry_max": None,
        "history_sufficient": None,
        "insufficiency_reason": "",
        "rolling_24h_p1": None,
        "rolling_24h_p5": None,
        "rolling_24h_p10": None,
        "rolling_24h_p20": None,
        "rolling_24h_sample_count": None,
    }


def _evaluate_symbol(
    symbol: str,
    *,
    funding: Mapping[str, Any],
    ticker: Mapping[str, Any],
    threshold: Mapping[str, Any],
    cfg: Mapping[str, Any],
) -> dict[str, Any]:
    decision_cfg = cfg["decision"]
    risk_cfg = cfg["risk"]
    tradable_symbols = set(cfg["universe"]["tradable_symbols"])
    reasons: list[str] = []

    if symbol not in tradable_symbols:
        _reject(reasons, "symbol_not_in_tradable_symbols")

    funding_rate = _as_float(funding.get("last_funding_rate"))
    if funding_rate is None:
        raise ValueError(f"TVR funding last_funding_rate missing/unreadable: {symbol}")
    if funding_rate > float(decision_cfg["funding_rate_entry_max"]):
        _reject(reasons, "funding_rate_above_entry_max")

    last_price = _as_float(ticker.get("lastPrice"))
    if last_price is None or last_price <= 0:
        raise ValueError(f"TVR live ticker lastPrice missing/unreadable: {symbol}")
    quote_volume_24h = _as_float(ticker.get("quoteVolume"))
    if quote_volume_24h is None:
        raise ValueError(f"TVR live ticker quoteVolume missing/unreadable: {symbol}")
    if quote_volume_24h < float(decision_cfg["min_quote_volume_24h"]):
        _reject(reasons, "quote_volume_24h_below_min")

    history_sufficient = bool(threshold.get("history_sufficient"))
    insufficiency_reason = str(threshold.get("insufficiency_reason") or "")
    selected_percentile_return = _as_float(threshold.get("selected_percentile_return"))
    if not history_sufficient:
        _reject(reasons, "history_not_sufficient")
    elif selected_percentile_return is None:
        raise ValueError(f"TVR selected_percentile_return missing/unreadable: {symbol}")

    price_change_pct_24h_percent = _as_float(ticker.get("priceChangePercent"))
    if price_change_pct_24h_percent is None:
        raise ValueError(f"TVR live ticker priceChangePercent missing/unreadable: {symbol}")
    current_24h_return = float(price_change_pct_24h_percent) / 100.0
    if history_sufficient and selected_percentile_return is not None and current_24h_return > float(selected_percentile_return):
        _reject(reasons, "current_24h_return_above_selected_percentile")

    proposed_notional = float(risk_cfg["proposed_order_notional_usdt"])
    max_symbol_notional = float(risk_cfg["max_symbol_notional_usdt"])
    max_total_notional = float(risk_cfg["max_total_notional_usdt"])
    if proposed_notional > max_symbol_notional:
        _reject(reasons, "proposed_notional_above_symbol_cap")
    if proposed_notional > max_total_notional:
        _reject(reasons, "proposed_notional_above_total_cap")

    estimated_qty = proposed_notional / float(last_price)
    eligible = not reasons
    intent = None
    if eligible:
        intent = {
            "strategy_name": STRATEGY_NAME,
            "symbol": symbol,
            "side": "LONG",
            "order_intent": "POST_ONLY_MAKER_BUY_AUDIT_ONLY",
            "order_submission_enabled": False,
            "proposed_order_notional_usdt": proposed_notional,
            "estimated_entry_price": float(last_price),
            "estimated_order_qty": float(estimated_qty),
            "take_profit_pct": float(decision_cfg["take_profit_pct"]),
            "entry_percentile": str(decision_cfg["entry_percentile"]),
            "selected_percentile_return": float(selected_percentile_return),
            "current_24h_return": float(current_24h_return),
            "take_profit_order_intent": "POST_ONLY_MAKER_SELL_AUDIT_ONLY",
        }

    return {
        "symbol": symbol,
        "eligible": bool(eligible),
        "reject_reasons": reasons,
        "intent": intent,
        "last_price": float(last_price),
        "quote_volume_24h": float(quote_volume_24h),
        "price_change_pct_24h_percent": price_change_pct_24h_percent,
        "current_24h_return": float(current_24h_return),
        "entry_percentile": str(decision_cfg["entry_percentile"]),
        "selected_percentile_return": selected_percentile_return,
        "funding_rate": float(funding_rate),
        "funding_rate_entry_max": float(decision_cfg["funding_rate_entry_max"]),
        "history_sufficient": bool(history_sufficient),
        "insufficiency_reason": insufficiency_reason,
        "rolling_24h_p1": _as_float(threshold.get("rolling_24h_p1")),
        "rolling_24h_p5": _as_float(threshold.get("rolling_24h_p5")),
        "rolling_24h_p10": _as_float(threshold.get("rolling_24h_p10")),
        "rolling_24h_p20": _as_float(threshold.get("rolling_24h_p20")),
        "rolling_24h_sample_count": _as_int(threshold.get("rolling_24h_sample_count")),
    }


def _selected_key(row: Mapping[str, Any]) -> tuple[float, float, float, str]:
    current_return = _as_float(row.get("current_24h_return"))
    funding = _as_float(row.get("funding_rate"))
    quote_volume = _as_float(row.get("quote_volume_24h"))
    return (
        float(current_return if current_return is not None else 0.0),
        float(funding if funding is not None else 0.0),
        -float(quote_volume if quote_volume is not None else 0.0),
        str(row.get("symbol") or ""),
    )


def build_decision_audit(cfg: Mapping[str, Any], *, run_id: str) -> dict[str, Any]:
    if not bool(cfg["enabled"]):
        raise RuntimeError("TVR decision audit config enabled=false")
    inputs = _load_inputs(cfg)
    records = inputs["records"]
    universe_symbols = _universe_symbols(records["universe"], min_symbol_count=int(cfg["data_hub"]["min_symbol_count"]))
    universe_symbol_set = set(universe_symbols)
    tradable_symbols = list(cfg["universe"]["tradable_symbols"])
    missing_tradable = [symbol for symbol in tradable_symbols if symbol not in universe_symbol_set]
    if missing_tradable:
        raise RuntimeError(f"TVR tradable_symbols not found in data_hub universe: {missing_tradable}")
    funding_by_symbol = _map_rows(records["funding"].get("rows"), stream="funding")
    rolling_by_symbol = _map_rows(records["rolling_24h_stats"].get("rows"), stream="rolling_24h_stats")
    percentile_state = _build_percentile_state(
        cfg=cfg,
        rolling_by_symbol=rolling_by_symbol,
        force_refresh=False,
    )
    thresholds_by_symbol = dict(percentile_state["thresholds"])

    rows: list[dict[str, Any]] = []
    tradable_symbol_set = set(tradable_symbols)
    for symbol in universe_symbols:
        if symbol not in tradable_symbol_set:
            rows.append(_non_tradable_symbol_row(symbol))
            continue
        if symbol not in funding_by_symbol:
            raise KeyError(f"TVR funding snapshot missing universe symbol: {symbol}")
        if symbol not in thresholds_by_symbol:
            raise KeyError(f"TVR percentile threshold missing tradable symbol: {symbol}")
        ticker = _fetch_live_ticker(str(cfg["account"]), symbol)
        rows.append(_evaluate_symbol(
            symbol,
            funding=funding_by_symbol[symbol],
            ticker=ticker,
            threshold=thresholds_by_symbol[symbol],
            cfg=cfg,
        ))

    eligible_rows = [row for row in rows if row["eligible"]]
    eligible_rows.sort(key=_selected_key)
    selected_rows = eligible_rows[: int(cfg["decision"]["max_candidates"])]
    selected_intents = [row["intent"] for row in selected_rows if isinstance(row.get("intent"), dict)]

    record = {
        **_base_record(run_id, "tvr_decision_audit"),
        "account": str(cfg["account"]).strip(),
        "trading_enabled": False,
        "order_submission_enabled": False,
        "data_hub_inputs": {
            "paths": dict(inputs["paths"]),
            "age_ms": dict(inputs["age_ms"]),
            "percentile_threshold_state_path": str(_percentile_state_path()),
            "percentile_threshold_updated_bj": percentile_state.get("updated_bj"),
            "collected_bj": {
                key: value.get("collected_bj")
                for key, value in dict(records).items()
                if isinstance(value, dict)
            },
        },
        "config": {
            "decision": dict(cfg["decision"]),
            "risk": dict(cfg["risk"]),
            "data_hub": dict(cfg["data_hub"]),
            "universe": dict(cfg["universe"]),
        },
        "symbol_count": len(rows),
        "data_hub_universe_symbol_count": len(universe_symbols),
        "tradable_symbol_count": len(tradable_symbols),
        "tradable_symbols": tradable_symbols,
        "eligible_count": len(eligible_rows),
        "selected_count": len(selected_intents),
        "selected_symbols": [str(row["symbol"]) for row in selected_rows],
        "selected_intents": selected_intents,
        "rows": rows,
    }
    return record


def run_once(cfg: Mapping[str, Any], *, run_id: str) -> Path | None:
    record = build_decision_audit(cfg, run_id=run_id)
    logging.info(
        "TVR decision audit | symbols=%s | eligible=%s | selected=%s | selected_symbols=%s",
        record["symbol_count"],
        record["eligible_count"],
        record["selected_count"],
        record["selected_symbols"],
    )
    if not bool(cfg["audit_enabled"]):
        return None
    return write_decision_audit_record(record)


def write_decision_audit_record(record: Mapping[str, Any]) -> Path:
    return _append_jsonl(_audit_path(), record)


def _build_run_id(account: str) -> str:
    account_key = str(account).upper().strip()
    if not account_key:
        raise ValueError("account must not be empty")
    ts_utc = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"TVR_DECISION_AUDIT_{account_key}_{ts_utc}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TVR decision audit: build audit-only LONG maker intents from TVR data_hub facts")
    parser.add_argument("--config", default="strategies/tvr/config.decision_audit.json")
    parser.add_argument("--once", action="store_true", help="run one decision audit iteration")
    parser.add_argument("--loop", action="store_true", help="run decision audit loop")
    parser.add_argument("--max-iterations", type=int, default=0, help="loop iteration cap; 0 means unlimited")
    return parser.parse_args()


def main() -> None:
    setup_logging()
    args = parse_args()
    if args.once == args.loop:
        raise ValueError("exactly one of --once or --loop is required")
    cfg = load_config(args.config)
    run_id = _build_run_id(str(cfg["account"]))
    iteration = 0
    while True:
        iteration += 1
        logging.info("TVR decision audit iteration started | run_id=%s | iteration=%s", run_id, iteration)
        path = run_once(cfg, run_id=run_id)
        logging.info("TVR decision audit iteration finished | run_id=%s | iteration=%s | path=%s", run_id, iteration, path)
        if args.once:
            break
        if int(args.max_iterations) > 0 and iteration >= int(args.max_iterations):
            break
        time.sleep(int(cfg["collection"]["interval_secs"]))


if __name__ == "__main__":
    main()
