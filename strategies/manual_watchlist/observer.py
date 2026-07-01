from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

BJ = timezone(timedelta(hours=8))
STRATEGY_NAME = "manual_watchlist_observer"
STRATEGY_CODE = "MWO"
ALERT_LABEL = "manual_watchlist"
BAR_MS = 60_000
REQUEST_PRIORITY_LOW = "LOW"
_VALID_REQUEST_PRIORITIES = {"LOW", "NORMAL", "HIGH", "CRITICAL"}


def _now_ms() -> int:
    return int(time.time() * 1000)


def _now_bj() -> str:
    return datetime.now(tz=BJ).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")


def _hhmm(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "NA"
    if " " in text:
        text = text.rsplit(" ", 1)[-1]
    return text[:5] if len(text) >= 5 else text


def _load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"manual watchlist config missing: {path}")
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError(f"manual watchlist config must be object: {path}")
    return data


def _require_mapping(cfg: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    if key not in cfg:
        raise KeyError(f"manual watchlist config missing section: {key}")
    value = cfg[key]
    if not isinstance(value, Mapping):
        raise TypeError(f"manual watchlist config section must be object: {key}")
    return value


def _require_bool(cfg: Mapping[str, Any], key: str) -> bool:
    if key not in cfg:
        raise KeyError(f"manual watchlist config missing bool: {key}")
    value = cfg[key]
    if not isinstance(value, bool):
        raise TypeError(f"manual watchlist config {key} must be bool")
    return value


def _require_str(cfg: Mapping[str, Any], key: str) -> str:
    if key not in cfg:
        raise KeyError(f"manual watchlist config missing string: {key}")
    value = str(cfg[key]).strip()
    if not value:
        raise ValueError(f"manual watchlist config string empty: {key}")
    return value


def _require_int(cfg: Mapping[str, Any], key: str, *, min_value: int | None = None, max_value: int | None = None) -> int:
    if key not in cfg:
        raise KeyError(f"manual watchlist config missing int: {key}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"manual watchlist config {key} must be int")
    try:
        out = int(value)
    except Exception as e:
        raise TypeError(f"manual watchlist config {key} must be int") from e
    if min_value is not None and out < int(min_value):
        raise ValueError(f"manual watchlist config {key} must be >= {min_value}")
    if max_value is not None and out > int(max_value):
        raise ValueError(f"manual watchlist config {key} must be <= {max_value}")
    return out


def _require_float(cfg: Mapping[str, Any], key: str, *, min_value: float | None = None) -> float:
    if key not in cfg:
        raise KeyError(f"manual watchlist config missing number: {key}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"manual watchlist config {key} must be number")
    try:
        out = float(value)
    except Exception as e:
        raise TypeError(f"manual watchlist config {key} must be number") from e
    if min_value is not None and out < float(min_value):
        raise ValueError(f"manual watchlist config {key} must be >= {min_value}")
    return out


def _load_return_signal(section: Mapping[str, Any], name: str) -> dict[str, Any]:
    raw = _require_mapping(section, name)
    enabled = _require_bool(raw, "enabled")
    threshold = _require_float(raw, "threshold", min_value=0.0)
    if enabled and threshold <= 0:
        raise ValueError(f"manual watchlist {name}.threshold must be > 0 when enabled")
    return {"enabled": enabled, "threshold": float(threshold)}


def _load_fixed_price_range(section: Mapping[str, Any]) -> dict[str, Any]:
    raw = _require_mapping(section, "fixed_price_range")
    enabled = _require_bool(raw, "enabled")
    out: dict[str, Any] = {"enabled": enabled}
    if enabled:
        min_price = _require_float(raw, "min_price", min_value=0.0)
        max_price = _require_float(raw, "max_price", min_value=0.0)
        if min_price <= 0 or max_price <= 0:
            raise ValueError("manual watchlist fixed_price_range min_price/max_price must be > 0")
        if max_price <= min_price:
            raise ValueError("manual watchlist fixed_price_range max_price must be > min_price")
        out["min_price"] = float(min_price)
        out["max_price"] = float(max_price)
    return out


def _load_rolling_ranges(section: Mapping[str, Any], *, max_lookback_mins: int) -> list[dict[str, Any]]:
    if "rolling_ranges" not in section:
        raise KeyError("manual watchlist config missing signals.rolling_ranges")
    raw = section["rolling_ranges"]
    if not isinstance(raw, list):
        raise TypeError("manual watchlist signals.rolling_ranges must be list")
    out: list[dict[str, Any]] = []
    seen: set[int] = set()
    for idx, item in enumerate(raw):
        if not isinstance(item, Mapping):
            raise TypeError(f"manual watchlist rolling_ranges[{idx}] must be object")
        enabled = _require_bool(item, "enabled")
        lookback = _require_int(item, "lookback_mins", min_value=2, max_value=max_lookback_mins)
        if lookback in seen:
            raise ValueError(f"manual watchlist duplicate rolling range lookback_mins: {lookback}")
        seen.add(lookback)
        out.append({"enabled": enabled, "lookback_mins": int(lookback)})
    return out


def _normalize_priority(value: str) -> str:
    out = str(value or "").upper().strip()
    if out not in _VALID_REQUEST_PRIORITIES:
        raise ValueError(f"manual watchlist unsupported request priority: {value!r}")
    return out


def _required_closed_bars(symbol_cfg: Mapping[str, Any]) -> int:
    signals = symbol_cfg["signals"]
    need = 2
    if bool(signals["return_5m_abs"]["enabled"]):
        need = max(need, 6)
    for item in signals["rolling_ranges"]:
        if bool(item["enabled"]):
            need = max(need, int(item["lookback_mins"]) + 1)
    return need


def load_config(path: str) -> dict[str, Any]:
    data = _load_json(path)
    runtime = _require_mapping(data, "runtime")
    data_cfg = _require_mapping(data, "data")
    max_lookback_mins = _require_int(data_cfg, "max_lookback_mins", min_value=5, max_value=990)
    priority = _normalize_priority(_require_str(data_cfg, "request_priority"))
    symbols_raw = data.get("symbols")
    if not isinstance(symbols_raw, list) or not symbols_raw:
        raise ValueError("manual watchlist config symbols must be non-empty list")

    symbols: list[dict[str, Any]] = []
    seen_symbols: set[str] = set()
    for idx, raw_item in enumerate(symbols_raw):
        if not isinstance(raw_item, Mapping):
            raise TypeError(f"manual watchlist symbols[{idx}] must be object")
        symbol = _require_str(raw_item, "symbol").upper()
        if symbol in seen_symbols:
            raise ValueError(f"manual watchlist duplicate symbol: {symbol}")
        seen_symbols.add(symbol)
        signals_raw = _require_mapping(raw_item, "signals")
        signals = {
            "return_1m_abs": _load_return_signal(signals_raw, "return_1m_abs"),
            "return_5m_abs": _load_return_signal(signals_raw, "return_5m_abs"),
            "fixed_price_range": _load_fixed_price_range(signals_raw),
            "rolling_ranges": _load_rolling_ranges(signals_raw, max_lookback_mins=max_lookback_mins),
        }
        item = {
            "enabled": _require_bool(raw_item, "enabled"),
            "symbol": symbol,
            "signals": signals,
        }
        enabled_signal_count = 0
        enabled_signal_count += int(bool(signals["return_1m_abs"]["enabled"]))
        enabled_signal_count += int(bool(signals["return_5m_abs"]["enabled"]))
        enabled_signal_count += int(bool(signals["fixed_price_range"]["enabled"]))
        enabled_signal_count += sum(1 for row in signals["rolling_ranges"] if bool(row["enabled"]))
        if bool(item["enabled"]) and enabled_signal_count <= 0:
            raise ValueError(f"manual watchlist enabled symbol has no enabled signals: {symbol}")
        if _required_closed_bars(item) > int(max_lookback_mins) + 1:
            raise ValueError(f"manual watchlist required lookback exceeds data.max_lookback_mins: {symbol}")
        symbols.append(item)

    return {
        "enabled": _require_bool(data, "enabled"),
        "account": _require_str(data, "account"),
        "notify_enabled": _require_bool(data, "notify_enabled"),
        "runtime": {
            "loop": _require_bool(runtime, "loop"),
            "scan_second": _require_int(runtime, "scan_second", min_value=0, max_value=59),
            "alert_cooldown_secs": _require_int(runtime, "alert_cooldown_secs", min_value=0),
            "config_error_alert_cooldown_secs": _require_int(runtime, "config_error_alert_cooldown_secs", min_value=0),
            "summary_log_interval_secs": _require_int(runtime, "summary_log_interval_secs", min_value=0),
        },
        "data": {
            "max_lookback_mins": int(max_lookback_mins),
            "request_priority": priority,
        },
        "symbols": symbols,
    }


def config_digest(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _expected_latest_closed_bar_ts(now_ms: int | None = None) -> int:
    value = int(now_ms) if now_ms is not None else _now_ms()
    return (value // BAR_MS) * BAR_MS - BAR_MS


def _fetch_symbol_klines(account: str, symbol: str, limit: int, priority: str) -> list[list[Any]]:
    from core.live.binance_rest_gateway import call_client_method

    payload = call_client_method(
        account,
        source="manual_watchlist_observer.futures_klines",
        method_name="futures_klines",
        priority=priority or REQUEST_PRIORITY_LOW,
        symbol=symbol,
        interval="1m",
        limit=int(limit),
    )
    if not isinstance(payload, list):
        raise TypeError(f"manual watchlist futures_klines payload must be list: {symbol}")
    return payload


def _as_float(value: Any, field: str, symbol: str) -> float:
    try:
        out = float(value)
    except Exception as e:
        raise ValueError(f"manual watchlist invalid {field}: {symbol}") from e
    if out <= 0:
        raise ValueError(f"manual watchlist non-positive {field}: {symbol}")
    return out


def _normalize_closed_bars(symbol: str, rows: list[list[Any]], *, latest_closed_bar_ts: int, keep: int) -> list[dict[str, Any]]:
    by_ts: dict[int, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, list) or len(row) < 8:
            raise ValueError(f"manual watchlist malformed kline row: {symbol}")
        open_time_ms = int(row[0])
        if open_time_ms > int(latest_closed_bar_ts):
            continue
        by_ts[open_time_ms] = {
            "open_time_ms": open_time_ms,
            "open_time_bj": _fmt_bj_from_ms(open_time_ms),
            "open": _as_float(row[1], "open", symbol),
            "high": _as_float(row[2], "high", symbol),
            "low": _as_float(row[3], "low", symbol),
            "close": _as_float(row[4], "close", symbol),
            "quote_asset_volume": float(row[7] or 0.0),
        }
    ordered = [by_ts[key] for key in sorted(by_ts)]
    return ordered[-int(keep):]


def _pct(value: float) -> str:
    return f"{float(value) * 100:.2f}%"


def _signal_row(
    *,
    symbol: str,
    signal_type: str,
    direction: str,
    c_bar: Mapping[str, Any],
    value: float,
    threshold: float | None = None,
    reference: float | None = None,
    lookback_mins: int | None = None,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "signal_type": signal_type,
        "cooldown_key": f"{symbol}:{signal_type}",
        "direction": direction,
        "c_bar_ts": int(c_bar["open_time_ms"]),
        "c_bar_bj": c_bar["open_time_bj"],
        "c_close": float(c_bar["close"]),
        "value": float(value),
        "threshold": float(threshold) if threshold is not None else None,
        "reference": float(reference) if reference is not None else None,
        "lookback_mins": int(lookback_mins) if lookback_mins is not None else None,
    }


def analyze_symbol(symbol_cfg: Mapping[str, Any], bars: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    symbol = str(symbol_cfg["symbol"])
    required = _required_closed_bars(symbol_cfg)
    if len(bars) < required:
        return [], {
            "symbol": symbol,
            "ok": False,
            "reason": f"history_insufficient:{len(bars)}<{required}",
        }
    c_bar = bars[-1]
    signals_cfg = symbol_cfg["signals"]
    out: list[dict[str, Any]] = []

    if bool(signals_cfg["return_1m_abs"]["enabled"]):
        prev = bars[-2]
        ret = float(c_bar["close"]) / float(prev["close"]) - 1.0
        threshold = float(signals_cfg["return_1m_abs"]["threshold"])
        if abs(ret) >= threshold:
            out.append(_signal_row(
                symbol=symbol,
                signal_type="return_1m_abs",
                direction="up" if ret > 0 else "down",
                c_bar=c_bar,
                value=ret,
                threshold=threshold,
                reference=float(prev["close"]),
                lookback_mins=1,
            ))

    if bool(signals_cfg["return_5m_abs"]["enabled"]):
        prev5 = bars[-6]
        ret5 = float(c_bar["close"]) / float(prev5["close"]) - 1.0
        threshold5 = float(signals_cfg["return_5m_abs"]["threshold"])
        if abs(ret5) >= threshold5:
            out.append(_signal_row(
                symbol=symbol,
                signal_type="return_5m_abs",
                direction="up" if ret5 > 0 else "down",
                c_bar=c_bar,
                value=ret5,
                threshold=threshold5,
                reference=float(prev5["close"]),
                lookback_mins=5,
            ))

    fixed_range = signals_cfg["fixed_price_range"]
    if bool(fixed_range["enabled"]):
        close = float(c_bar["close"])
        min_price = float(fixed_range["min_price"])
        max_price = float(fixed_range["max_price"])
        if close > max_price:
            out.append(_signal_row(
                symbol=symbol,
                signal_type="fixed_price_range",
                direction="above_max",
                c_bar=c_bar,
                value=close,
                threshold=max_price,
                reference=max_price,
            ))
        elif close < min_price:
            out.append(_signal_row(
                symbol=symbol,
                signal_type="fixed_price_range",
                direction="below_min",
                c_bar=c_bar,
                value=close,
                threshold=min_price,
                reference=min_price,
            ))

    for item in signals_cfg["rolling_ranges"]:
        if not bool(item["enabled"]):
            continue
        lookback = int(item["lookback_mins"])
        prior = bars[-lookback - 1:-1]
        if len(prior) != lookback:
            return out, {
                "symbol": symbol,
                "ok": False,
                "reason": f"rolling_history_insufficient:{lookback}",
            }
        high = max(float(row["high"]) for row in prior)
        low = min(float(row["low"]) for row in prior)
        close = float(c_bar["close"])
        signal_type = f"rolling_range_{lookback}m"
        if close > high:
            out.append(_signal_row(
                symbol=symbol,
                signal_type=signal_type,
                direction="above_high",
                c_bar=c_bar,
                value=close,
                threshold=high,
                reference=high,
                lookback_mins=lookback,
            ))
        elif close < low:
            out.append(_signal_row(
                symbol=symbol,
                signal_type=signal_type,
                direction="below_low",
                c_bar=c_bar,
                value=close,
                threshold=low,
                reference=low,
                lookback_mins=lookback,
            ))

    return out, {
        "symbol": symbol,
        "ok": True,
        "reason": "",
        "c_bar_ts": int(c_bar["open_time_ms"]),
        "c_bar_bj": c_bar["open_time_bj"],
        "c_close": float(c_bar["close"]),
    }


def _alert_state_path(account: str) -> Path:
    from core.runtime_state import get_state_dir

    return get_state_dir() / "live" / f"manual_watchlist_observer_alerts.{account}.json"


def _load_alert_state(account: str) -> dict[str, Any]:
    path = _alert_state_path(account)
    if not path.exists():
        return {"alerts": {}}
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict) or not isinstance(data.get("alerts"), dict):
        raise TypeError(f"manual watchlist alert state invalid: {path}")
    return data


def _save_alert_state(account: str, state: Mapping[str, Any]) -> None:
    path = _alert_state_path(account)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, sort_keys=True)
    os.replace(tmp, path)


def _apply_alert_cooldown(account: str, signals: list[dict[str, Any]], *, cooldown_secs: int, now_ms: int) -> tuple[list[dict[str, Any]], int]:
    if not signals or int(cooldown_secs) <= 0:
        return signals, 0
    state = _load_alert_state(account)
    alerts = state["alerts"]
    cooldown_ms = int(cooldown_secs) * 1000
    passed: list[dict[str, Any]] = []
    suppressed = 0
    for item in signals:
        key = str(item["cooldown_key"])
        last_ms = int(alerts.get(key, 0) or 0)
        if last_ms > 0 and now_ms - last_ms < cooldown_ms:
            suppressed += 1
            continue
        passed.append(item)
        alerts[key] = int(now_ms)
    state["updated_bj"] = _now_bj()
    _save_alert_state(account, state)
    return passed, suppressed


def _format_signal_line(item: Mapping[str, Any]) -> str:
    signal_type = str(item["signal_type"])
    direction = str(item["direction"])
    symbol = str(item["symbol"])
    close = float(item["c_close"])
    if signal_type in {"return_1m_abs", "return_5m_abs"}:
        return (
            f"{symbol} {signal_type} {direction} {_pct(float(item['value']))} "
            f">= {_pct(float(item['threshold']))} close={close:g}"
        )
    if signal_type == "fixed_price_range":
        op = ">" if direction == "above_max" else "<"
        return f"{symbol} fixed {direction} close={close:g} {op} {float(item['threshold']):g}"
    return f"{symbol} {signal_type} {direction} close={close:g} ref={float(item['reference']):g}"


def _notify_signals(enabled: bool, signals: list[dict[str, Any]], signal_bj: str) -> None:
    if not enabled or not signals:
        return
    from core.message_bridge import send_to_bot

    lines = [f"🔔 [{STRATEGY_CODE}] sig={_hhmm(signal_bj)}"]
    lines.extend(_format_signal_line(item) for item in signals)
    send_to_bot("\n".join(lines), label=ALERT_LABEL)


def _notify_config_error(account: str, error: str) -> None:
    from core.message_bridge import send_to_bot

    text = f"🔔 [{STRATEGY_CODE}] CONFIG_ERROR | account={account}\n{error}"
    send_to_bot(text, label=ALERT_LABEL)


def _write_strategy_event(account: str, event: str, payload: dict[str, Any]) -> None:
    from core.live.audit_log import write_strategy_event

    write_strategy_event(account, STRATEGY_NAME, event, payload)


def scan_once(cfg: Mapping[str, Any]) -> dict[str, Any]:
    account = str(cfg["account"]).strip()
    run_id = uuid.uuid4().hex[:12]
    now_ms = _now_ms()
    expected_latest_closed_bar_ts = _expected_latest_closed_bar_ts(now_ms)
    if not bool(cfg["enabled"]):
        summary = {
            "run_id": run_id,
            "strategy_name": STRATEGY_NAME,
            "strategy_code": STRATEGY_CODE,
            "account": account,
            "enabled": False,
            "scan_bj": _now_bj(),
            "reason": "disabled_by_config",
        }
        _write_strategy_event(account, "scan_disabled", summary)
        return summary

    all_signals: list[dict[str, Any]] = []
    symbol_results: list[dict[str, Any]] = []
    errors: dict[str, str] = {}
    priority = str(cfg["data"]["request_priority"])
    for symbol_cfg in cfg["symbols"]:
        if not bool(symbol_cfg["enabled"]):
            continue
        symbol = str(symbol_cfg["symbol"])
        required = _required_closed_bars(symbol_cfg)
        limit = required + 1
        if limit > 1000:
            raise ValueError(f"manual watchlist klines request limit exceeds 1000: {symbol} limit={limit}")
        try:
            raw_rows = _fetch_symbol_klines(account, symbol, limit, priority)
            bars = _normalize_closed_bars(
                symbol,
                raw_rows,
                latest_closed_bar_ts=expected_latest_closed_bar_ts,
                keep=required,
            )
            signals, result = analyze_symbol(symbol_cfg, bars)
            result["bars_loaded"] = int(len(bars))
            result["request_limit"] = int(limit)
            result["expected_latest_closed_bar_ts"] = int(expected_latest_closed_bar_ts)
            result["expected_latest_closed_bar_bj"] = _fmt_bj_from_ms(expected_latest_closed_bar_ts)
            symbol_results.append(result)
            all_signals.extend(signals)
        except Exception as exc:
            errors[symbol] = str(exc)
            symbol_results.append({
                "symbol": symbol,
                "ok": False,
                "reason": str(exc),
                "request_limit": int(limit),
                "expected_latest_closed_bar_ts": int(expected_latest_closed_bar_ts),
                "expected_latest_closed_bar_bj": _fmt_bj_from_ms(expected_latest_closed_bar_ts),
            })

    notify_enabled = bool(cfg["notify_enabled"])
    notify_signals, suppressed = (
        _apply_alert_cooldown(
            account,
            all_signals,
            cooldown_secs=int(cfg["runtime"]["alert_cooldown_secs"]),
            now_ms=now_ms,
        )
        if notify_enabled
        else (all_signals, 0)
    )
    signal_bj = _fmt_bj_from_ms(expected_latest_closed_bar_ts + BAR_MS) or _now_bj()
    summary = {
        "run_id": run_id,
        "strategy_name": STRATEGY_NAME,
        "strategy_code": STRATEGY_CODE,
        "account": account,
        "enabled": True,
        "scan_bj": _now_bj(),
        "signal_time_ts": int(expected_latest_closed_bar_ts + BAR_MS),
        "signal_time_bj": signal_bj,
        "expected_latest_closed_bar_ts": int(expected_latest_closed_bar_ts),
        "expected_latest_closed_bar_bj": _fmt_bj_from_ms(expected_latest_closed_bar_ts),
        "symbol_count": int(sum(1 for item in cfg["symbols"] if bool(item["enabled"]))),
        "signal_count": int(len(all_signals)),
        "notify_signal_count": int(len(notify_signals)),
        "alert_suppressed_count": int(suppressed),
        "error_count": int(len(errors)),
        "errors": errors,
        "signals": all_signals,
        "notify_signals": notify_signals,
        "symbol_results": symbol_results,
    }
    _write_strategy_event(account, "scan_finished", summary)
    _notify_signals(notify_enabled, notify_signals, signal_bj)
    return summary


def _sleep_until_scan_second(scan_second: int) -> None:
    now = time.time()
    current_minute = int(now // 60) * 60
    target = current_minute + int(scan_second)
    if now >= target:
        target += 60
    sleep_secs = max(0.0, target - now)
    if sleep_secs > 0:
        time.sleep(sleep_secs)


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manual watchlist observer: alert only, no trading")
    parser.add_argument("--config", required=True)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--notify", action="store_true")
    parser.add_argument("--max-iterations", type=int, default=0)
    return parser.parse_args()


def _record_config_error(account: str, error: str) -> None:
    payload = {
        "strategy_name": STRATEGY_NAME,
        "strategy_code": STRATEGY_CODE,
        "account": account,
        "scan_bj": _now_bj(),
        "error": str(error),
    }
    _write_strategy_event(account, "config_error", payload)


def main() -> None:
    args = parse_args()
    setup_logging()
    cfg: dict[str, Any] | None = None
    digest: str | None = None
    config_error_last_notify_ms = 0
    last_info_log_ts = 0.0
    iterations = 0

    while True:
        try:
            new_digest = config_digest(args.config)
            if new_digest != digest:
                cfg = load_config(args.config)
                if args.loop:
                    cfg["runtime"]["loop"] = True
                if args.notify:
                    cfg["notify_enabled"] = True
                digest = new_digest
                logging.info(
                    "manual watchlist config loaded | account=%s | enabled=%s | symbols=%s | digest=%s",
                    cfg["account"],
                    cfg["enabled"],
                    len(cfg["symbols"]),
                    digest[:12],
                )
            if cfg is None:
                raise RuntimeError("manual watchlist config not loaded")
        except Exception as exc:
            account = str((cfg or {}).get("account") or "unknown")
            error = str(exc)
            _record_config_error(account, error)
            now_ms = _now_ms()
            cooldown_secs = int((cfg or {}).get("runtime", {}).get("config_error_alert_cooldown_secs", 300))
            notify_config_error = bool(args.notify or (cfg or {}).get("notify_enabled"))
            if notify_config_error and now_ms - config_error_last_notify_ms >= cooldown_secs * 1000:
                _notify_config_error(account, error)
                config_error_last_notify_ms = now_ms
            logging.error("manual watchlist config error | %s", error)
            if not args.loop:
                raise
            time.sleep(5)
            continue

        if bool(cfg["runtime"]["loop"]):
            _sleep_until_scan_second(int(cfg["runtime"]["scan_second"]))
        summary = scan_once(cfg)
        iterations += 1
        now_ts = time.monotonic()
        loop_enabled = bool(cfg["runtime"]["loop"])
        notify_count = int(summary.get("notify_signal_count") or 0)
        summary_interval = int(cfg["runtime"]["summary_log_interval_secs"])
        should_info_log = (
            (not loop_enabled)
            or notify_count > 0
            or (
                summary_interval > 0
                and (last_info_log_ts <= 0.0 or now_ts - last_info_log_ts >= summary_interval)
            )
        )
        log_fn = logging.info if should_info_log else logging.debug
        log_fn(
            "manual watchlist scan finished | account=%s | enabled=%s | symbols=%s | signals=%s | notify=%s | suppressed=%s | errors=%s | signal_time=%s",
            summary.get("account"),
            summary.get("enabled"),
            summary.get("symbol_count"),
            summary.get("signal_count"),
            summary.get("notify_signal_count"),
            summary.get("alert_suppressed_count"),
            summary.get("error_count"),
            summary.get("signal_time_bj"),
        )
        if should_info_log:
            last_info_log_ts = now_ts
        if not loop_enabled:
            break
        if args.max_iterations > 0 and iterations >= int(args.max_iterations):
            break


if __name__ == "__main__":
    main()
