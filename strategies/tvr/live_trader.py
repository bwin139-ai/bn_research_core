from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Mapping

from filelock import FileLock

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core.live.binance_exec import (
    cancel_order,
    ensure_cross_margin,
    ensure_hedge_mode,
    ensure_leverage,
    get_open_orders,
    get_order,
    get_position,
    get_symbol_filters,
    place_limit_order,
)
from core.live.binance_rest_gateway import REQUEST_PRIORITY_HIGH, call_client_method
from core.live.custom_id import build_client_order_id, make_order_root
from core.live.live_state import (
    load_live_state,
    mark_error,
    mark_loop_heartbeat,
    mark_order_reconcile,
    mark_position_reconcile,
    set_open_trade,
    set_pending_entry_order,
)
from core.message_bridge import send_to_bot
from core.runtime_state import get_state_dir
from strategies.tvr.decision_audit import (
    build_decision_audit,
    load_config as load_decision_audit_config,
    write_decision_audit_record,
)

BJ = timezone(timedelta(hours=8))
STRATEGY_NAME = "tvr"
STRATEGY_CODE = "TVR"
STRATEGY_LOGO = "🏛"
POSITION_SIDE_LONG = "LONG"
TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED"}
REPRICE_RETRY_ORDER_STATUSES = {"EXPIRED", "REJECTED"}
QUIET_ENTRY_ACTIONS = {
    "entry_skipped_local_active_symbol",
    "entry_blocked_max_open_trades",
    "entry_skipped_no_candidate_symbols",
    "entry_skipped_active_decision_interval",
}
QUIET_RECONCILE_ACTIONS = {"open_trade_wait", "pending_entry_wait"}
IMPORTANT_ENTRY_ACTIONS = {
    "signal_locked",
    "dry_run_entry",
    "entry_submitted",
    "entry_terminal_without_pending",
    "entry_attempt_failed",
    "entry_blocked",
}
IMPORTANT_RECONCILE_ACTIONS = {
    "tp_submitted",
    "open_trade_exit_detected",
    "open_trade_transient_signed_query_failed",
    "pending_entry_query_failed",
    "dry_run_pending_entry_filled",
    "partial_entry_wait",
    "partial_entry_cancel_remaining",
    "pending_entry_terminal_cleared",
    "dry_run_pending_entry_ttl_reached",
    "pending_entry_ttl_cancel",
}
_ITERATION_LOG_STATE = {
    "last_summary_utc_ms": 0,
    "iterations": 0,
    "quiet_reconcile_events": 0,
    "quiet_entry_events": 0,
    "decision_selected_total": 0,
}
_DECISION_THROTTLE_STATE = {"last_decision_utc_ms": 0}
_TRANSIENT_SIGNED_QUERY_NOTIFY_STATE: dict[str, int] = {}
_TRANSIENT_SIGNED_QUERY_NOTIFY_INTERVAL_MS = 30 * 60 * 1000


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
    return get_state_dir() / "live_audit" / "tvr" / "live" / day_key / "tvr_live_trader.jsonl"


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
        raise FileNotFoundError(f"TVR live trader config missing: {path}")
    with p.open("r", encoding="utf-8") as f:
        payload = json.load(f)
    if not isinstance(payload, dict):
        raise TypeError(f"TVR live trader config must be JSON object: {path}")
    return payload


def _require_mapping(cfg: Mapping[str, Any], path: str, key: str) -> dict[str, Any]:
    if key not in cfg:
        raise KeyError(f"TVR live trader config missing required section: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, dict):
        raise TypeError(f"TVR live trader config section must be object: {key} | {path}")
    return dict(value)


def _require_bool(cfg: Mapping[str, Any], path: str, key: str) -> bool:
    if key not in cfg:
        raise KeyError(f"TVR live trader config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, bool):
        raise TypeError(f"TVR live trader config field must be bool: {key} | {path}")
    return bool(value)


def _require_non_empty_str(cfg: Mapping[str, Any], path: str, key: str) -> str:
    if key not in cfg:
        raise KeyError(f"TVR live trader config missing required field: {key} | {path}")
    value = str(cfg[key]).strip()
    if not value:
        raise ValueError(f"TVR live trader config field must not be empty: {key} | {path}")
    return value


def _require_int(cfg: Mapping[str, Any], path: str, key: str, *, positive: bool) -> int:
    if key not in cfg:
        raise KeyError(f"TVR live trader config missing required field: {key} | {path}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"TVR live trader config field must be int: {key} | {path}")
    try:
        out = int(value)
    except Exception as exc:
        raise TypeError(f"TVR live trader config field must be int: {key} | {path}") from exc
    if positive and out <= 0:
        raise ValueError(f"TVR live trader config field must be > 0: {key} | {path}")
    if not positive and out < 0:
        raise ValueError(f"TVR live trader config field must be >= 0: {key} | {path}")
    return out


def _require_float(cfg: Mapping[str, Any], path: str, key: str, *, positive: bool) -> float:
    if key not in cfg:
        raise KeyError(f"TVR live trader config missing required field: {key} | {path}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"TVR live trader config field must be number: {key} | {path}")
    try:
        out = float(value)
    except Exception as exc:
        raise TypeError(f"TVR live trader config field must be number: {key} | {path}") from exc
    if math.isnan(out) or math.isinf(out):
        raise ValueError(f"TVR live trader config field must be finite: {key} | {path}")
    if positive and out <= 0:
        raise ValueError(f"TVR live trader config field must be > 0: {key} | {path}")
    if not positive and out < 0:
        raise ValueError(f"TVR live trader config field must be >= 0: {key} | {path}")
    return out


def _require_symbol_float_map(cfg: Mapping[str, Any], path: str, key: str) -> dict[str, float]:
    if key not in cfg:
        raise KeyError(f"TVR live trader config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, dict) or not value:
        raise TypeError(f"TVR live trader config field must be non-empty object: {key} | {path}")
    out: dict[str, float] = {}
    for raw_symbol, raw_value in value.items():
        symbol = str(raw_symbol).upper().strip()
        if not symbol:
            raise ValueError(f"TVR live trader config contains empty symbol key: {key} | {path}")
        if symbol in out:
            raise ValueError(f"TVR live trader config duplicated symbol key: {symbol} | {path}")
        if isinstance(raw_value, bool):
            raise TypeError(f"TVR live trader config symbol value must be number: {key}.{symbol} | {path}")
        try:
            num = float(raw_value)
        except Exception as exc:
            raise TypeError(f"TVR live trader config symbol value must be number: {key}.{symbol} | {path}") from exc
        if math.isnan(num) or math.isinf(num) or num <= 0:
            raise ValueError(f"TVR live trader config symbol value must be positive finite: {key}.{symbol} | {path}")
        out[symbol] = float(num)
    return out


def load_config(path: str) -> dict[str, Any]:
    cfg = _load_json(path)
    if "schema_version" not in cfg:
        raise KeyError(f"TVR live trader config missing required field: schema_version | {path}")
    if int(cfg["schema_version"]) != 1:
        raise ValueError(f"TVR live trader config schema_version must be 1 | {path}")
    decision_audit = _require_mapping(cfg, path, "decision_audit")
    collection = _require_mapping(cfg, path, "collection")
    execution = _require_mapping(cfg, path, "execution")
    recovery = _require_mapping(cfg, path, "recovery")
    logging_cfg = _require_mapping(cfg, path, "logging")
    out = {
        "schema_version": 1,
        "enabled": _require_bool(cfg, path, "enabled"),
        "account": _require_non_empty_str(cfg, path, "account"),
        "allow_live_order": _require_bool(cfg, path, "allow_live_order"),
        "audit_enabled": _require_bool(cfg, path, "audit_enabled"),
        "decision_audit": {
            "config_path": _require_non_empty_str(decision_audit, path, "config_path"),
            "required_order_submission_enabled": _require_bool(decision_audit, path, "required_order_submission_enabled"),
        },
        "collection": {
            "interval_secs": _require_int(collection, path, "interval_secs", positive=True),
            "active_decision_interval_secs": _require_int(collection, path, "active_decision_interval_secs", positive=True),
        },
        "logging": {
            "summary_interval_secs": _require_int(logging_cfg, path, "summary_interval_secs", positive=True),
        },
        "execution": {
            "position_side": _require_non_empty_str(execution, path, "position_side").upper(),
            "position_mode": _require_non_empty_str(execution, path, "position_mode").upper(),
            "margin_type": _require_non_empty_str(execution, path, "margin_type").upper(),
            "leverage": _require_int(execution, path, "leverage", positive=True),
            "symbol_notional_usdt": _require_symbol_float_map(execution, path, "symbol_notional_usdt"),
            "max_entry_notional_usdt": _require_symbol_float_map(execution, path, "max_entry_notional_usdt"),
            "max_symbol_entry_notional_usdt": _require_symbol_float_map(
                execution, path, "max_symbol_entry_notional_usdt"
            ),
            "max_open_trades": _require_int(execution, path, "max_open_trades", positive=True),
            "max_new_entries_per_iteration": _require_int(execution, path, "max_new_entries_per_iteration", positive=True),
            "entry_price_mode": _require_non_empty_str(execution, path, "entry_price_mode").upper(),
            "entry_best_bid_offset_ticks": _require_int(execution, path, "entry_best_bid_offset_ticks", positive=False),
            "entry_attempt_window_secs": _require_float(execution, path, "entry_attempt_window_secs", positive=True),
            "entry_retry_sleep_secs": _require_float(execution, path, "entry_retry_sleep_secs", positive=True),
            "entry_max_attempts": _require_int(execution, path, "entry_max_attempts", positive=True),
            "entry_order_ttl_secs": _require_float(execution, path, "entry_order_ttl_secs", positive=True),
            "partial_fill_wait_secs": _require_float(execution, path, "partial_fill_wait_secs", positive=False),
            "take_profit_pct": _require_float(execution, path, "take_profit_pct", positive=True),
            "post_only_time_in_force": _require_non_empty_str(execution, path, "post_only_time_in_force").upper(),
            "order_retry_max": _require_int(execution, path, "order_retry_max", positive=False),
            "api_retry_delay_secs": _require_float(execution, path, "api_retry_delay_secs", positive=False),
            "require_symbol_flat": _require_bool(execution, path, "require_symbol_flat"),
            "require_no_symbol_open_orders": _require_bool(execution, path, "require_no_symbol_open_orders"),
        },
        "recovery": {
            "enabled": _require_bool(recovery, path, "enabled"),
            "anchor": _require_non_empty_str(recovery, path, "anchor").upper(),
            "grid_step_pct": _require_float(recovery, path, "grid_step_pct", positive=True),
            "min_spacing_hours": _require_float(recovery, path, "min_spacing_hours", positive=True),
        },
    }
    exec_cfg = out["execution"]
    recovery_cfg = out["recovery"]
    if exec_cfg["position_side"] != POSITION_SIDE_LONG:
        raise ValueError(f"TVR live trader only supports LONG position_side | {path}")
    if exec_cfg["position_mode"] != "HEDGE":
        raise ValueError(f"TVR live trader position_mode must be HEDGE | {path}")
    if exec_cfg["margin_type"] != "CROSSED":
        raise ValueError(f"TVR live trader margin_type must be CROSSED | {path}")
    if exec_cfg["post_only_time_in_force"] != "GTX":
        raise ValueError(f"TVR live trader post_only_time_in_force must be GTX | {path}")
    if exec_cfg["entry_price_mode"] != "BEST_BID":
        raise ValueError(f"TVR live trader entry_price_mode must be BEST_BID | {path}")
    if set(exec_cfg["symbol_notional_usdt"]) != set(exec_cfg["max_entry_notional_usdt"]):
        raise ValueError(f"TVR symbol_notional_usdt keys must match max_entry_notional_usdt keys | {path}")
    if set(exec_cfg["symbol_notional_usdt"]) != set(exec_cfg["max_symbol_entry_notional_usdt"]):
        raise ValueError(
            f"TVR symbol_notional_usdt keys must match max_symbol_entry_notional_usdt keys | {path}"
        )
    for symbol in sorted(exec_cfg["symbol_notional_usdt"]):
        if float(exec_cfg["symbol_notional_usdt"][symbol]) > float(exec_cfg["max_entry_notional_usdt"][symbol]):
            raise ValueError(f"TVR symbol_notional_usdt must be <= max_entry_notional_usdt: {symbol} | {path}")
        if float(exec_cfg["symbol_notional_usdt"][symbol]) > float(
            exec_cfg["max_symbol_entry_notional_usdt"][symbol]
        ):
            raise ValueError(
                f"TVR symbol_notional_usdt must be <= max_symbol_entry_notional_usdt: {symbol} | {path}"
            )
    if recovery_cfg["anchor"] != "HIGHEST_OPEN_ENTRY":
        raise ValueError(f"TVR recovery.anchor must be HIGHEST_OPEN_ENTRY | {path}")
    if recovery_cfg["enabled"]:
        raise ValueError(f"TVR recovery is configured but live multi-lot recovery is not implemented yet | {path}")
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


def _floor_to_step(value: float, step: float | None) -> float:
    if step is None or step <= 0:
        return float(value)
    dec_value = Decimal(str(value))
    dec_step = Decimal(str(step))
    return float((dec_value / dec_step).to_integral_value(rounding=ROUND_DOWN) * dec_step)


def _round_to_tick(value: float, tick: float | None) -> float:
    if tick is None or tick <= 0:
        return float(value)
    dec_value = Decimal(str(value))
    dec_tick = Decimal(str(tick))
    return float((dec_value / dec_tick).to_integral_value(rounding=ROUND_DOWN) * dec_tick)


def _fill_price(order: Mapping[str, Any], *, fallback_price: float) -> float:
    avg_price = _as_float(order.get("avg_price"))
    if avg_price is not None and avg_price > 0:
        return float(avg_price)
    executed_qty = _as_float(order.get("executed_qty"))
    cum_quote = _as_float(order.get("cum_quote"))
    if executed_qty is not None and cum_quote is not None and executed_qty > 0 and cum_quote > 0:
        return float(cum_quote / executed_qty)
    fallback = float(fallback_price or 0.0)
    if fallback <= 0:
        raise RuntimeError("TVR fill price unavailable")
    return fallback


def _fmt_num(value: Any, *, digits: int = 6) -> str:
    num = _as_float(value)
    if num is None:
        return "NA"
    text = f"{num:.{digits}f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


def _fmt_pct(value: Any, *, digits: int = 2) -> str:
    num = _as_float(value)
    if num is None:
        return "NA"
    return f"{num * 100:.{digits}f}%"


def _fmt_duration_ms(duration_ms: Any) -> str:
    value = _as_int(duration_ms)
    if value is None or value < 0:
        return "NA"
    total_minutes = int(round(value / 60_000))
    days, rem_minutes = divmod(total_minutes, 24 * 60)
    hours, minutes = divmod(rem_minutes, 60)
    if days > 0:
        return f"{days}d{hours}h{minutes}m"
    if hours > 0:
        return f"{hours}h{minutes}m"
    return f"{minutes}m"


def _notify_tvr(account: str, title: str, lines: list[str]) -> None:
    now_bj = datetime.now(BJ).strftime("%H:%M:%S")
    message = "\n".join([f"[{now_bj} {STRATEGY_LOGO} TVR] {account}", str(title), *lines])
    send_to_bot(message, label="tvr")


def _is_transient_signed_query_error(reason: Any) -> bool:
    text = str(reason or "")
    return (
        "code=-1021" in text
        or "outside of the recvWindow" in text
        or "Timestamp for this request" in text
    )


def _notify_transient_signed_query(
    account: str,
    symbol: str,
    *,
    operation: str,
    reason: str,
    attempts: int,
) -> bool:
    now_ms = _now_utc_ms()
    key = f"{account}:{symbol}:{operation}"
    last_ms = int(_TRANSIENT_SIGNED_QUERY_NOTIFY_STATE.get(key) or 0)
    if now_ms - last_ms < _TRANSIENT_SIGNED_QUERY_NOTIFY_INTERVAL_MS:
        return False
    _TRANSIENT_SIGNED_QUERY_NOTIFY_STATE[key] = now_ms
    _notify_tvr(
        account,
        "WARN transient signed API error",
        [
            f"symbol={symbol}",
            f"operation={operation}",
            f"attempts={attempts}",
            reason,
        ],
    )
    return True


def _transient_signed_query_event(
    account: str,
    symbol: str,
    *,
    operation: str,
    reason: str,
    attempts: int,
    now_bj: str,
) -> dict[str, Any]:
    mark_error(
        account,
        symbol,
        error_code="tvr_transient_signed_query_failed",
        error_message=reason,
        error_bj=now_bj,
        strategy_name=STRATEGY_NAME,
    )
    notified = _notify_transient_signed_query(
        account,
        symbol,
        operation=operation,
        reason=reason,
        attempts=attempts,
    )
    logging.warning(
        "TVR transient signed query failed | account=%s | symbol=%s | operation=%s | attempts=%s | notified=%s | reason=%s",
        account,
        symbol,
        operation,
        attempts,
        notified,
        reason,
    )
    return {
        "action": "open_trade_transient_signed_query_failed",
        "symbol": symbol,
        "operation": operation,
        "attempts": attempts,
        "reason": reason,
        "notified": notified,
    }


def _emit_signal_locked(cfg: Mapping[str, Any], decision: Mapping[str, Any], intent: Mapping[str, Any]) -> dict[str, Any]:
    account = str(cfg["account"]).strip()
    symbol = str(intent.get("symbol") or "").upper().strip()
    event = {
        "action": "signal_locked",
        "symbol": symbol,
        "entry_percentile": intent.get("entry_percentile"),
        "current_24h_return": intent.get("current_24h_return"),
        "selected_percentile_return": intent.get("selected_percentile_return"),
        "estimated_entry_price": intent.get("estimated_entry_price"),
        "estimated_order_qty": intent.get("estimated_order_qty"),
        "proposed_order_notional_usdt": intent.get("proposed_order_notional_usdt"),
        "take_profit_pct": intent.get("take_profit_pct"),
        "decision_run_id": decision.get("run_id"),
        "decision_collected_bj": decision.get("collected_bj"),
    }
    logging.info(
        "TVR signal locked | account=%s | symbol=%s | percentile=%s | current_24h_return=%s | threshold=%s | entry_est=%s | qty_est=%s | notional=%s | tp_pct=%s | decision_run_id=%s",
        account,
        symbol,
        intent.get("entry_percentile"),
        _fmt_pct(intent.get("current_24h_return")),
        _fmt_pct(intent.get("selected_percentile_return")),
        _fmt_num(intent.get("estimated_entry_price")),
        _fmt_num(intent.get("estimated_order_qty")),
        _fmt_num(intent.get("proposed_order_notional_usdt"), digits=2),
        _fmt_pct(intent.get("take_profit_pct")),
        decision.get("run_id"),
    )
    _notify_tvr(
        account,
        f"雷达锁定: {symbol}",
        [
            f"percentile={intent.get('entry_percentile')}",
            (
                "24h_return="
                f"{_fmt_pct(intent.get('current_24h_return'))} <= "
                f"threshold={_fmt_pct(intent.get('selected_percentile_return'))}"
            ),
            (
                f"entry≈{_fmt_num(intent.get('estimated_entry_price'))} | "
                f"qty≈{_fmt_num(intent.get('estimated_order_qty'))}"
            ),
            f"开仓金额: {_fmt_num(intent.get('proposed_order_notional_usdt'), digits=2)}U",
            f"TP: {_fmt_pct(intent.get('take_profit_pct'))}",
            f"decision={decision.get('run_id')}",
        ],
    )
    return event


def _client_order_ids() -> dict[str, str]:
    root = make_order_root()
    return {
        "order_root": root,
        "entry_client_order_id": build_client_order_id(strat=STRATEGY_CODE, leg="ENT", root=root),
        "tp_client_order_id": build_client_order_id(strat=STRATEGY_CODE, leg="TP", root=root),
    }


def _submit_post_only_limit(
    *,
    account: str,
    symbol: str,
    side: str,
    position_side: str,
    quantity: float,
    price: float,
    client_order_id: str,
    time_in_force: str,
    notify_on_error: bool = True,
    notify_on_success: bool = True,
) -> dict[str, Any]:
    role = "ENTRY" if str(side).upper().strip() == "BUY" else "TP"
    result = place_limit_order(
        account,
        symbol,
        position_side,
        side,
        float(quantity),
        float(price),
        order_role=role,
        time_in_force=time_in_force,
        client_order_id=client_order_id,
        notify_label="tvr",
        notify_on_error=notify_on_error,
        notify_on_success=notify_on_success,
        notify_order_statuses={"NEW", "PARTIALLY_FILLED", "FILLED"},
    )
    if not result.get("ok"):
        raise RuntimeError(str(result.get("reason") or "TVR post-only limit order failed"))
    return dict(result.get("data") or {})


def _is_post_only_reprice_exception(exc: Exception) -> bool:
    text = str(exc)
    if "APIError(code=-5022)" in text:
        return True
    upper = text.upper()
    return (
        "GTX" in upper
        or "POST ONLY" in upper
        or "POST-ONLY" in upper
        or "COULD NOT BE EXECUTED AS MAKER" in upper
        or "WOULD BE IMMEDIATELY MATCHED" in upper
    )


def _best_bid_price(account: str, symbol: str) -> dict[str, Any]:
    raw = call_client_method(
        account,
        source="tvr_live_trader.futures_order_book",
        method_name="futures_order_book",
        priority=REQUEST_PRIORITY_HIGH,
        symbol=str(symbol).upper().strip(),
        limit=5,
    )
    if not isinstance(raw, dict):
        raise RuntimeError(f"unexpected futures_order_book payload: {raw}")
    bids = raw.get("bids")
    if not isinstance(bids, list) or not bids:
        raise RuntimeError(f"futures_order_book bids missing: {symbol}")
    first = bids[0]
    if not isinstance(first, (list, tuple)) or len(first) < 2:
        raise RuntimeError(f"futures_order_book best bid malformed: {symbol}")
    price = _as_float(first[0])
    qty = _as_float(first[1])
    if price is None or price <= 0:
        raise RuntimeError(f"futures_order_book best bid price invalid: {symbol}")
    return {"price": float(price), "qty": qty, "raw": raw}


def _normalize_order_size(account: str, symbol: str, *, notional_usdt: float, limit_price: float) -> dict[str, Any]:
    filters_res = get_symbol_filters(account, symbol)
    if not filters_res.get("ok"):
        raise RuntimeError(f"symbol filters query failed: {filters_res.get('reason')}")
    filters = dict(filters_res["data"])
    price = _round_to_tick(float(limit_price), filters.get("tick_size"))
    if price <= 0:
        raise ValueError(f"TVR limit price invalid after tick rounding: {limit_price}")
    qty = _floor_to_step(float(notional_usdt) / price, filters.get("step_size"))
    notional = qty * price
    if qty <= 0:
        raise ValueError("TVR quantity non-positive after step rounding")
    if filters.get("min_qty") and qty < float(filters["min_qty"]):
        raise ValueError(f"TVR quantity below exchange min_qty: {qty} < {filters['min_qty']}")
    if filters.get("min_notional") and notional < float(filters["min_notional"]):
        raise ValueError(f"TVR notional below exchange min_notional: {notional} < {filters['min_notional']}")
    return {"qty": float(qty), "price": float(price), "notional_usdt": float(notional), "filters": filters}


def _entry_price_from_best_bid(account: str, symbol: str, cfg: Mapping[str, Any], *, order_notional_usdt: float) -> dict[str, Any]:
    bid = _best_bid_price(account, symbol)
    filters_res = get_symbol_filters(account, symbol)
    if not filters_res.get("ok"):
        raise RuntimeError(f"symbol filters query failed: {filters_res.get('reason')}")
    filters = dict(filters_res["data"])
    tick_size = _as_float(filters.get("tick_size")) or 0.0
    raw_price = float(bid["price"]) - int(cfg["execution"]["entry_best_bid_offset_ticks"]) * float(tick_size)
    price = _round_to_tick(raw_price, filters.get("tick_size"))
    if price <= 0:
        raise ValueError(f"TVR best-bid entry price invalid after rounding: {raw_price}")
    qty = _floor_to_step(float(order_notional_usdt) / price, filters.get("step_size"))
    notional = qty * price
    if qty <= 0:
        raise ValueError("TVR quantity non-positive after best-bid step rounding")
    if filters.get("min_qty") and qty < float(filters["min_qty"]):
        raise ValueError(f"TVR quantity below exchange min_qty: {qty} < {filters['min_qty']}")
    if filters.get("min_notional") and notional < float(filters["min_notional"]):
        raise ValueError(f"TVR notional below exchange min_notional: {notional} < {filters['min_notional']}")
    return {
        "qty": float(qty),
        "price": float(price),
        "notional_usdt": float(notional),
        "best_bid": bid,
        "filters": filters,
    }


def _symbol_order_notional(cfg: Mapping[str, Any], symbol: str) -> float:
    symbol_key = str(symbol).upper().strip()
    notional_by_symbol = dict(cfg["execution"]["symbol_notional_usdt"])
    if symbol_key not in notional_by_symbol:
        raise KeyError(f"TVR live trader symbol_notional_usdt missing symbol: {symbol_key}")
    return float(notional_by_symbol[symbol_key])


def _symbol_max_entry_notional(cfg: Mapping[str, Any], symbol: str) -> float:
    symbol_key = str(symbol).upper().strip()
    cap_by_symbol = dict(cfg["execution"]["max_entry_notional_usdt"])
    if symbol_key not in cap_by_symbol:
        raise KeyError(f"TVR live trader max_entry_notional_usdt missing symbol: {symbol_key}")
    return float(cap_by_symbol[symbol_key])


def _active_local_symbols(state: Mapping[str, Any]) -> list[str]:
    out: list[str] = []
    for symbol, payload in dict(state.get("symbols") or {}).items():
        if not isinstance(payload, Mapping):
            continue
        if isinstance(payload.get("pending_entry_order"), Mapping) or isinstance(payload.get("open_trade"), Mapping):
            out.append(str(symbol).upper().strip())
    return sorted(x for x in out if x)


def _ensure_symbol_is_flat(account: str, symbol: str, cfg: Mapping[str, Any]) -> dict[str, Any]:
    blockers: list[str] = []
    position_snapshot = None
    orders_snapshot: list[dict[str, Any]] = []
    if bool(cfg["execution"]["require_symbol_flat"]):
        position_res = get_position(account, symbol, POSITION_SIDE_LONG)
        if not position_res.get("ok"):
            raise RuntimeError(f"TVR position precheck failed: {position_res.get('reason')}")
        position_snapshot = position_res.get("data")
        if isinstance(position_snapshot, Mapping) and float(position_snapshot.get("qty") or 0.0) > 0:
            blockers.append("exchange_symbol_has_long_position")
    if bool(cfg["execution"]["require_no_symbol_open_orders"]):
        orders_res = get_open_orders(account, symbol)
        if not orders_res.get("ok"):
            raise RuntimeError(f"TVR open orders precheck failed: {orders_res.get('reason')}")
        orders_snapshot = [dict(x) for x in orders_res.get("data") or [] if isinstance(x, Mapping)]
        if orders_snapshot:
            blockers.append("exchange_symbol_has_open_orders")
    return {
        "ok": not blockers,
        "blockers": blockers,
        "position": position_snapshot,
        "open_orders": orders_snapshot,
    }


def _ensure_execution_env(account: str, symbol: str, cfg: Mapping[str, Any]) -> dict[str, Any]:
    hedge = ensure_hedge_mode(account)
    if not hedge.get("ok"):
        raise RuntimeError(f"ensure_hedge_mode failed: {hedge.get('reason')}")
    margin = ensure_cross_margin(account, symbol)
    if not margin.get("ok"):
        raise RuntimeError(f"ensure_cross_margin failed: {margin.get('reason')}")
    leverage = ensure_leverage(account, symbol, int(cfg["execution"]["leverage"]))
    if not leverage.get("ok"):
        raise RuntimeError(f"ensure_leverage failed: {leverage.get('reason')}")
    return {"hedge_mode": hedge, "margin": margin, "leverage": leverage}


def _place_entry_from_intent(
    *,
    cfg: Mapping[str, Any],
    decision: Mapping[str, Any],
    intent: Mapping[str, Any],
    dry_run: bool,
) -> dict[str, Any]:
    account = str(cfg["account"]).strip()
    symbol = str(intent.get("symbol") or "").upper().strip()
    if not symbol:
        raise ValueError("TVR selected intent missing symbol")
    proposed = _as_float(intent.get("proposed_order_notional_usdt"))
    order_notional = _symbol_order_notional(cfg, symbol)
    if proposed is None or not math.isclose(float(proposed), order_notional, rel_tol=0.0, abs_tol=1e-9):
        raise ValueError(f"TVR intent proposed notional mismatch: {proposed} vs config {order_notional}")
    if order_notional > _symbol_max_entry_notional(cfg, symbol):
        raise ValueError(f"TVR symbol_notional_usdt exceeds max_entry_notional_usdt: {symbol}")
    intent_tp = _as_float(intent.get("take_profit_pct"))
    config_tp = float(cfg["execution"]["take_profit_pct"])
    if intent_tp is None or not math.isclose(float(intent_tp), config_tp, rel_tol=0.0, abs_tol=1e-12):
        raise ValueError(f"TVR intent take_profit_pct mismatch: {intent_tp} vs config {config_tp}")

    order_ids = _client_order_ids()

    if dry_run or not bool(cfg["allow_live_order"]):
        estimated_price = _as_float(intent.get("estimated_entry_price"))
        if estimated_price is None or estimated_price <= 0:
            raise ValueError(f"TVR selected intent missing estimated_entry_price: {symbol}")
        entry_limit_price = estimated_price
        estimated_qty = order_notional / entry_limit_price
        return {
            "action": "dry_run_entry",
            "symbol": symbol,
            "current_price": estimated_price,
            "entry_price_mode": str(cfg["execution"]["entry_price_mode"]),
            "entry_limit_price": entry_limit_price,
            "qty": estimated_qty,
            "notional_usdt": order_notional,
            "client_order_id": order_ids["entry_client_order_id"],
            "decision_run_id": decision.get("run_id"),
        }

    precheck = _ensure_symbol_is_flat(account, symbol, cfg)
    if not precheck["ok"]:
        return {"action": "entry_blocked", "symbol": symbol, "precheck": precheck}
    env = _ensure_execution_env(account, symbol, cfg)
    attempt_started_ms = _now_utc_ms()
    attempt_deadline_ms = attempt_started_ms + int(float(cfg["execution"]["entry_attempt_window_secs"]) * 1000)
    attempts: list[dict[str, Any]] = []
    entry_order: dict[str, Any] | None = None
    size: dict[str, Any] | None = None
    status = ""
    for attempt_index in range(1, int(cfg["execution"]["entry_max_attempts"]) + 1):
        now_ms = _now_utc_ms()
        if now_ms > attempt_deadline_ms:
            break
        try:
            size = _entry_price_from_best_bid(account, symbol, cfg, order_notional_usdt=order_notional)
            attempt_client_order_id = build_client_order_id(
                strat=STRATEGY_CODE,
                leg=f"E{attempt_index}",
                root=order_ids["order_root"],
            )
            entry_order = _submit_post_only_limit(
                account=account,
                symbol=symbol,
                side="BUY",
                position_side=POSITION_SIDE_LONG,
                quantity=size["qty"],
                price=size["price"],
                client_order_id=attempt_client_order_id,
                time_in_force=str(cfg["execution"]["post_only_time_in_force"]),
                notify_on_error=False,
                notify_on_success=True,
            )
            status = str(entry_order.get("status") or "").upper()
            order_ids["entry_client_order_id"] = attempt_client_order_id
            attempts.append({
                "attempt": attempt_index,
                "entry_limit_price": float(size["price"]),
                "qty": float(size["qty"]),
                "status": status,
                "order_id": entry_order.get("order_id"),
                "client_order_id": entry_order.get("client_order_id"),
            })
            if status not in REPRICE_RETRY_ORDER_STATUSES:
                break
        except Exception as exc:
            if not _is_post_only_reprice_exception(exc):
                raise
            attempts.append({"attempt": attempt_index, "exception": str(exc), "status": "POST_ONLY_REPRICE_RETRY"})
            status = "EXCEPTION"
        if _now_utc_ms() >= attempt_deadline_ms:
            break
        time.sleep(float(cfg["execution"]["entry_retry_sleep_secs"]))

    if entry_order is None or size is None:
        return {
            "action": "entry_attempt_failed",
            "symbol": symbol,
            "env": env,
            "attempts": attempts,
            "attempt_window_secs": float(cfg["execution"]["entry_attempt_window_secs"]),
        }
    payload = {
        "symbol": symbol,
        "strategy_name": STRATEGY_NAME,
        "order_root": order_ids["order_root"],
        "entry_client_order_id": order_ids["entry_client_order_id"],
        "entry_exchange_order_id": entry_order.get("order_id"),
        "entry_limit_price": float(size["price"]),
        "entry_qty": float(size["qty"]),
        "entry_notional_usdt": float(size["notional_usdt"]),
        "take_profit_pct": float(cfg["execution"]["take_profit_pct"]),
        "tp_client_order_id": order_ids["tp_client_order_id"],
        "decision_run_id": decision.get("run_id"),
        "decision_collected_bj": decision.get("collected_bj"),
        "intent": dict(intent),
        "created_utc_ms": _now_utc_ms(),
        "created_bj": _fmt_bj_from_ms(_now_utc_ms()),
        "order_status": status,
        "raw_order": entry_order,
        "entry_attempts": attempts,
    }
    if status in TERMINAL_ORDER_STATUSES and status != "FILLED":
        return {"action": "entry_terminal_without_pending", "symbol": symbol, "entry_order": entry_order, "env": env, "attempts": attempts}
    if status not in TERMINAL_ORDER_STATUSES or status == "FILLED":
        set_pending_entry_order(account, symbol, payload, strategy_name=STRATEGY_NAME)
    return {"action": "entry_submitted", "symbol": symbol, "entry_order": entry_order, "env": env, "pending_entry_order": payload}


def _place_take_profit_for_pending(account: str, symbol: str, pending: Mapping[str, Any], order: Mapping[str, Any], cfg: Mapping[str, Any]) -> dict[str, Any]:
    executed_qty = _as_float(order.get("executed_qty")) or 0.0
    if executed_qty <= 0:
        raise RuntimeError("TVR pending entry has no executed quantity")
    entry_price = _fill_price(order, fallback_price=float(pending.get("entry_limit_price") or 0.0))
    tp_raw_price = entry_price * (1.0 + float(pending["take_profit_pct"]))
    size = _normalize_order_size(account, symbol, notional_usdt=executed_qty * tp_raw_price, limit_price=tp_raw_price)
    tp_order = _submit_post_only_limit(
        account=account,
        symbol=symbol,
        side="SELL",
        position_side=POSITION_SIDE_LONG,
        quantity=float(size["qty"]),
        price=float(size["price"]),
        client_order_id=str(pending["tp_client_order_id"]),
        time_in_force=str(cfg["execution"]["post_only_time_in_force"]),
        notify_on_error=True,
        notify_on_success=True,
    )
    tp_status = str(tp_order.get("status") or "").upper()
    if tp_status in TERMINAL_ORDER_STATUSES and tp_status != "FILLED":
        raise RuntimeError(f"TVR take-profit order terminal without maker TP: symbol={symbol} status={tp_status}")
    opened_ms = _now_utc_ms()
    open_trade = {
        "symbol": symbol,
        "strategy_name": STRATEGY_NAME,
        "side": POSITION_SIDE_LONG,
        "order_root": pending.get("order_root"),
        "entry_client_order_id": pending.get("entry_client_order_id"),
        "entry_exchange_order_id": pending.get("entry_exchange_order_id"),
        "entry_price": float(entry_price),
        "entry_qty": float(size["qty"]),
        "entry_notional_usdt": float(size["qty"] * entry_price),
        "tp_price": float(size["price"]),
        "tp_client_order_id": tp_order.get("client_order_id"),
        "tp_exchange_order_id": tp_order.get("order_id"),
        "tp_status": tp_status,
        "take_profit_pct": float(pending["take_profit_pct"]),
        "decision_run_id": pending.get("decision_run_id"),
        "opened_utc_ms": opened_ms,
        "opened_bj": _fmt_bj_from_ms(opened_ms),
        "opened_time_source": "tp_order_submitted_after_entry_fill",
        "entry_order_snapshot": dict(order),
        "tp_order_snapshot": dict(tp_order),
    }
    set_open_trade(account, symbol, open_trade, strategy_name=STRATEGY_NAME)
    set_pending_entry_order(account, symbol, None, strategy_name=STRATEGY_NAME)
    logging.info(
        "TVR OPEN established | account=%s | symbol=%s | entry=%s | tp=%s | qty=%s",
        account,
        symbol,
        _fmt_num(entry_price),
        _fmt_num(size["price"]),
        _fmt_num(size["qty"]),
    )
    _notify_tvr(
        account,
        "OPEN",
        [
            f"symbol={symbol}",
            f"entry={_fmt_num(entry_price)} | tp={_fmt_num(size['price'])} | qty={_fmt_num(size['qty'])}",
            f"entry_order={pending.get('entry_client_order_id')}",
            f"tp_order={tp_order.get('client_order_id')}",
        ],
    )
    return {"action": "tp_submitted", "symbol": symbol, "open_trade": open_trade}


def _query_open_trade_tp(account: str, symbol: str, open_trade: Mapping[str, Any], cfg: Mapping[str, Any]) -> dict[str, Any]:
    exchange_order_id = _as_int(open_trade.get("tp_exchange_order_id"))
    client_order_id = str(open_trade.get("tp_client_order_id") or "").strip()
    if exchange_order_id is None and not client_order_id:
        raise ValueError(f"TVR open_trade missing TP order identity: {symbol}")
    return get_order(
        account,
        symbol,
        exchange_order_id=exchange_order_id,
        client_order_id=client_order_id,
        retry_max=int(cfg["execution"]["order_retry_max"]),
        retry_delay_secs=float(cfg["execution"]["api_retry_delay_secs"]),
    )


def _query_long_position_with_retry(account: str, symbol: str, cfg: Mapping[str, Any]) -> dict[str, Any]:
    attempts = max(0, int(cfg["execution"]["order_retry_max"])) + 1
    retry_delay_secs = max(0.0, float(cfg["execution"]["api_retry_delay_secs"]))
    last_res: dict[str, Any] | None = None
    for attempt in range(1, attempts + 1):
        res = get_position(account, symbol, POSITION_SIDE_LONG)
        payload = dict(res)
        payload["attempts"] = attempt
        if payload.get("ok"):
            return payload
        last_res = payload
        if attempt >= attempts or not _is_transient_signed_query_error(payload.get("reason")):
            return payload
        time.sleep(retry_delay_secs)
    return last_res or {"ok": False, "reason": "TVR position query failed without response", "attempts": attempts, "data": None}


def _position_qty(position: Any) -> float:
    if not isinstance(position, Mapping):
        return 0.0
    return float(position.get("qty") or 0.0)


def _order_executed_qty(order: Mapping[str, Any] | None) -> float:
    if not isinstance(order, Mapping):
        return 0.0
    return float(order.get("executed_qty") or 0.0)


def _order_status(order: Mapping[str, Any] | None) -> str:
    if not isinstance(order, Mapping):
        return ""
    return str(order.get("status") or "").upper().strip()


def _finalize_open_trade_exit(
    *,
    account: str,
    symbol: str,
    open_trade: Mapping[str, Any],
    exit_reason: str,
    tp_order: Mapping[str, Any] | None,
    position_snapshot: Mapping[str, Any] | None,
    cleanup_cancel: Mapping[str, Any] | None,
) -> dict[str, Any]:
    exit_price = None
    if isinstance(tp_order, Mapping) and (_order_status(tp_order) == "FILLED" or _order_executed_qty(tp_order) > 0):
        exit_price = _as_float(tp_order.get("avg_price")) or _as_float(tp_order.get("price"))
    if exit_price is None and exit_reason == "TAKE_PROFIT":
        exit_price = _as_float(open_trade.get("tp_price"))
    entry_price = _as_float(open_trade.get("entry_price"))
    pnl_pct = None
    if entry_price is not None and entry_price > 0 and exit_price is not None and exit_price > 0:
        pnl_pct = exit_price / entry_price - 1.0

    closed_trade = dict(open_trade)
    now_ms = _now_utc_ms()
    opened_ms = _as_int(open_trade.get("opened_utc_ms"))
    holding_ms = int(now_ms - opened_ms) if opened_ms is not None and int(opened_ms) <= now_ms else None
    holding_minutes = round(holding_ms / 60_000, 3) if holding_ms is not None else None
    holding_text = _fmt_duration_ms(holding_ms)
    closed_trade.update({
        "status": "CLOSED",
        "exit_reason": exit_reason,
        "exit_price": exit_price,
        "exit_pnl_pct": pnl_pct,
        "closed_utc_ms": now_ms,
        "closed_bj": _fmt_bj_from_ms(now_ms),
        "holding_ms": holding_ms,
        "holding_minutes": holding_minutes,
        "holding_text": holding_text,
        "tp_exit_order_snapshot": dict(tp_order or {}),
        "position_close_snapshot": dict(position_snapshot or {}),
        "cleanup_cancel": dict(cleanup_cancel or {}),
    })
    set_open_trade(account, symbol, None, strategy_name=STRATEGY_NAME)
    mark_position_reconcile(account, symbol, reconcile_bj=_fmt_bj_from_ms(now_ms), strategy_name=STRATEGY_NAME)
    mark_order_reconcile(account, symbol, reconcile_bj=_fmt_bj_from_ms(now_ms), strategy_name=STRATEGY_NAME)
    logging.info(
        "TVR EXIT detected | account=%s | symbol=%s | reason=%s | entry=%s | exit=%s | pnl_pct=%s | holding=%s",
        account,
        symbol,
        exit_reason,
        _fmt_num(entry_price),
        _fmt_num(exit_price),
        _fmt_num(pnl_pct, digits=4),
        holding_text,
    )
    _notify_tvr(
        account,
        f"EXIT {exit_reason}",
        [
            f"symbol={symbol}",
            f"entry={_fmt_num(entry_price)} | exit={_fmt_num(exit_price)} | pnl={_fmt_num(pnl_pct, digits=4)}",
            f"持仓={holding_text}",
            f"tp_status={_order_status(tp_order)} | tp_executed={_fmt_num(_order_executed_qty(tp_order))}",
        ],
    )
    return {
        "action": "open_trade_exit_detected",
        "symbol": symbol,
        "exit_reason": exit_reason,
        "closed_trade": closed_trade,
    }


def _reconcile_open_trade(
    account: str,
    symbol: str,
    open_trade: Mapping[str, Any],
    cfg: Mapping[str, Any],
    *,
    dry_run: bool,
) -> dict[str, Any]:
    now_bj = _fmt_bj_from_ms(_now_utc_ms())
    tp_res = _query_open_trade_tp(account, symbol, open_trade, cfg)
    if not tp_res.get("ok") and _is_transient_signed_query_error(tp_res.get("reason")):
        reason = f"TVR open_trade TP query transient failed: {symbol} | {tp_res.get('reason')}"
        return _transient_signed_query_event(
            account,
            symbol,
            operation="query_tp_order",
            reason=reason,
            attempts=int(tp_res.get("attempts") or 1),
            now_bj=now_bj,
        )
    tp_order = dict(tp_res.get("data") or {}) if tp_res.get("ok") else None
    tp_status = _order_status(tp_order)
    position_res = _query_long_position_with_retry(account, symbol, cfg)
    if not position_res.get("ok"):
        if _is_transient_signed_query_error(position_res.get("reason")):
            reason = f"TVR open_trade position query transient failed: {symbol} | {position_res.get('reason')}"
            return _transient_signed_query_event(
                account,
                symbol,
                operation="query_long_position",
                reason=reason,
                attempts=int(position_res.get("attempts") or 1),
                now_bj=now_bj,
            )
        raise RuntimeError(f"TVR open_trade position query failed: {symbol} | {position_res.get('reason')}")
    position_snapshot = position_res.get("data") if isinstance(position_res.get("data"), Mapping) else None
    position_qty = _position_qty(position_snapshot)

    if dry_run:
        return {
            "action": "dry_run_open_trade_reconcile",
            "symbol": symbol,
            "tp_query_ok": bool(tp_res.get("ok")),
            "tp_status": tp_status,
            "position_qty": position_qty,
        }

    mark_position_reconcile(account, symbol, reconcile_bj=now_bj, strategy_name=STRATEGY_NAME)
    mark_order_reconcile(account, symbol, reconcile_bj=now_bj, strategy_name=STRATEGY_NAME)
    mark_error(account, symbol, error_code=None, error_message=None, error_bj=None, strategy_name=STRATEGY_NAME)

    if position_qty <= 0:
        cleanup_cancel = None
        exit_reason = "TAKE_PROFIT" if tp_status == "FILLED" else "POSITION_CLOSED"
        if tp_res.get("ok") and tp_status and tp_status not in TERMINAL_ORDER_STATUSES:
            cleanup_cancel = cancel_order(
                account,
                symbol,
                exchange_order_id=_as_int(open_trade.get("tp_exchange_order_id")),
                client_order_id=str(open_trade.get("tp_client_order_id") or ""),
                retry_max=int(cfg["execution"]["order_retry_max"]),
                retry_delay_secs=float(cfg["execution"]["api_retry_delay_secs"]),
                notify_label="tvr",
            )
        return _finalize_open_trade_exit(
            account=account,
            symbol=symbol,
            open_trade=open_trade,
            exit_reason=exit_reason,
            tp_order=tp_order,
            position_snapshot=position_snapshot,
            cleanup_cancel=cleanup_cancel,
        )

    if not tp_res.get("ok"):
        reason = f"TVR open position TP query failed: {symbol} | {tp_res.get('reason')}"
        mark_error(account, symbol, error_code="tvr_tp_query_failed", error_message=reason, error_bj=now_bj, strategy_name=STRATEGY_NAME)
        _notify_tvr(account, "CRITICAL", [f"symbol={symbol}", reason])
        raise RuntimeError(reason)

    if not tp_status:
        reason = f"TVR open position TP status missing: {symbol}"
        mark_error(account, symbol, error_code="tvr_tp_status_missing", error_message=reason, error_bj=now_bj, strategy_name=STRATEGY_NAME)
        _notify_tvr(account, "CRITICAL", [f"symbol={symbol}", reason])
        raise RuntimeError(reason)

    if tp_status == "FILLED":
        reason = f"TVR TP filled but LONG position still open: {symbol} | qty={position_qty}"
        mark_error(account, symbol, error_code="tvr_tp_filled_position_open", error_message=reason, error_bj=now_bj, strategy_name=STRATEGY_NAME)
        _notify_tvr(account, "CRITICAL", [f"symbol={symbol}", reason])
        raise RuntimeError(reason)

    if tp_status in TERMINAL_ORDER_STATUSES:
        reason = f"TVR open position lost active TP: {symbol} | tp_status={tp_status}"
        mark_error(account, symbol, error_code="tvr_open_position_tp_terminal", error_message=reason, error_bj=now_bj, strategy_name=STRATEGY_NAME)
        _notify_tvr(account, "CRITICAL", [f"symbol={symbol}", reason])
        raise RuntimeError(reason)

    return {
        "action": "open_trade_wait",
        "symbol": symbol,
        "position_qty": position_qty,
        "tp_status": tp_status,
        "tp_executed_qty": _order_executed_qty(tp_order),
    }


def _reconcile_local_state(cfg: Mapping[str, Any], *, dry_run: bool) -> list[dict[str, Any]]:
    account = str(cfg["account"]).strip()
    state = load_live_state(account, strategy_name=STRATEGY_NAME)
    events: list[dict[str, Any]] = []
    for symbol, symbol_state in dict(state.get("symbols") or {}).items():
        if not isinstance(symbol_state, Mapping):
            continue
        symbol_key = str(symbol).upper().strip()
        pending = symbol_state.get("pending_entry_order")
        if isinstance(pending, Mapping):
            order_res = get_order(
                account,
                symbol_key,
                exchange_order_id=_as_int(pending.get("entry_exchange_order_id")),
                client_order_id=str(pending.get("entry_client_order_id") or ""),
                retry_max=int(cfg["execution"]["order_retry_max"]),
                retry_delay_secs=float(cfg["execution"]["api_retry_delay_secs"]),
            )
            if not order_res.get("ok"):
                events.append({"action": "pending_entry_query_failed", "symbol": symbol_key, "reason": order_res.get("reason")})
                continue
            order = dict(order_res.get("data") or {})
            status = str(order.get("status") or "").upper()
            executed_qty = _as_float(order.get("executed_qty")) or 0.0
            created_ms = _as_int(pending.get("created_utc_ms"))
            now_ms = _now_utc_ms()
            ttl_ms = int(float(cfg["execution"]["entry_order_ttl_secs"]) * 1000)
            if status == "PARTIALLY_FILLED" and executed_qty > 0:
                first_partial_ms = _as_int(pending.get("first_partial_fill_utc_ms"))
                pending_update = dict(pending)
                if first_partial_ms is None:
                    first_partial_ms = now_ms
                    pending_update["first_partial_fill_utc_ms"] = int(first_partial_ms)
                    pending_update["first_partial_fill_bj"] = _fmt_bj_from_ms(first_partial_ms)
                pending_update["last_partial_fill_utc_ms"] = int(now_ms)
                pending_update["last_partial_fill_bj"] = _fmt_bj_from_ms(now_ms)
                pending_update["partial_executed_qty"] = float(executed_qty)
                pending_update["latest_entry_order_snapshot"] = order
                partial_wait_ms = int(float(cfg["execution"]["partial_fill_wait_secs"]) * 1000)
                partial_elapsed_ms = max(0, now_ms - int(first_partial_ms))
                ttl_elapsed = created_ms is not None and now_ms - int(created_ms) > ttl_ms
                if partial_elapsed_ms < partial_wait_ms and not ttl_elapsed:
                    if not dry_run:
                        set_pending_entry_order(account, symbol_key, pending_update, strategy_name=STRATEGY_NAME)
                    events.append({
                        "action": "partial_entry_wait",
                        "symbol": symbol_key,
                        "executed_qty": float(executed_qty),
                        "partial_elapsed_secs": round(partial_elapsed_ms / 1000.0, 3),
                        "partial_fill_wait_secs": float(cfg["execution"]["partial_fill_wait_secs"]),
                        "status": status,
                        "order": order,
                    })
                    continue
                if dry_run:
                    events.append({
                        "action": "dry_run_partial_entry_cancel_remaining",
                        "symbol": symbol_key,
                        "executed_qty": float(executed_qty),
                        "partial_elapsed_secs": round(partial_elapsed_ms / 1000.0, 3),
                        "partial_fill_wait_secs": float(cfg["execution"]["partial_fill_wait_secs"]),
                        "ttl_elapsed": bool(ttl_elapsed),
                        "status": status,
                        "order": order,
                    })
                    continue
                cancel_res = cancel_order(
                    account,
                    symbol_key,
                    exchange_order_id=_as_int(pending.get("entry_exchange_order_id")),
                    client_order_id=str(pending.get("entry_client_order_id") or ""),
                    retry_max=int(cfg["execution"]["order_retry_max"]),
                    retry_delay_secs=float(cfg["execution"]["api_retry_delay_secs"]),
                    notify_label="tvr",
                )
                events.append({
                    "action": "partial_entry_cancel_remaining",
                    "symbol": symbol_key,
                    "executed_qty": float(executed_qty),
                    "partial_elapsed_secs": round(partial_elapsed_ms / 1000.0, 3),
                    "partial_fill_wait_secs": float(cfg["execution"]["partial_fill_wait_secs"]),
                    "ttl_elapsed": bool(ttl_elapsed),
                    "cancel": cancel_res,
                })
                status = "FILLED"
            if status == "FILLED" or (executed_qty > 0 and status in TERMINAL_ORDER_STATUSES):
                if dry_run:
                    events.append({"action": "dry_run_pending_entry_filled", "symbol": symbol_key, "order": order})
                else:
                    events.append(_place_take_profit_for_pending(account, symbol_key, pending, order, cfg))
                continue
            if status in TERMINAL_ORDER_STATUSES:
                set_pending_entry_order(account, symbol_key, None, strategy_name=STRATEGY_NAME)
                events.append({"action": "pending_entry_terminal_cleared", "symbol": symbol_key, "status": status, "order": order})
                continue
            if created_ms is not None and now_ms - int(created_ms) > ttl_ms:
                if dry_run:
                    events.append({"action": "dry_run_pending_entry_ttl_reached", "symbol": symbol_key, "status": status, "order": order})
                else:
                    cancel_res = cancel_order(
                        account,
                        symbol_key,
                        exchange_order_id=_as_int(pending.get("entry_exchange_order_id")),
                        client_order_id=str(pending.get("entry_client_order_id") or ""),
                        retry_max=int(cfg["execution"]["order_retry_max"]),
                        retry_delay_secs=float(cfg["execution"]["api_retry_delay_secs"]),
                        notify_label="tvr",
                    )
                    if cancel_res.get("ok"):
                        set_pending_entry_order(account, symbol_key, None, strategy_name=STRATEGY_NAME)
                    events.append({"action": "pending_entry_ttl_cancel", "symbol": symbol_key, "status": status, "cancel": cancel_res})
                continue
            events.append({"action": "pending_entry_wait", "symbol": symbol_key, "status": status, "order": order})
        open_trade = symbol_state.get("open_trade")
        if isinstance(open_trade, Mapping):
            events.append(_reconcile_open_trade(account, symbol_key, open_trade, cfg, dry_run=dry_run))
    return events


def _selected_intents(decision: Mapping[str, Any]) -> list[dict[str, Any]]:
    if str(decision.get("strategy_name") or "").strip() != STRATEGY_NAME:
        raise ValueError("latest decision audit strategy_name mismatch")
    selected = decision.get("selected_intents")
    if not isinstance(selected, list):
        raise TypeError("latest decision audit selected_intents must be list")
    out: list[dict[str, Any]] = []
    for item in selected:
        if not isinstance(item, Mapping):
            raise TypeError("latest decision audit selected_intents item must be object")
        if bool(item.get("order_submission_enabled")):
            raise ValueError("TVR live trader expects audit-only selected intents before execution")
        if str(item.get("side") or "").upper().strip() != POSITION_SIDE_LONG:
            raise ValueError("TVR live trader only accepts LONG selected intents")
        out.append(dict(item))
    return out


def _decision_cfg_for_candidate_symbols(decision_cfg: Mapping[str, Any], candidate_symbols: list[str]) -> dict[str, Any]:
    if not candidate_symbols:
        raise ValueError("candidate_symbols must not be empty")
    out = dict(decision_cfg)
    universe = dict(out.get("universe") or {})
    risk = dict(out.get("risk") or {})
    symbol_notional = dict(risk.get("symbol_notional_usdt") or {})
    max_symbol_notional = dict(risk.get("max_symbol_notional_usdt") or {})
    for symbol in candidate_symbols:
        if symbol not in symbol_notional:
            raise KeyError(f"TVR decision risk symbol_notional_usdt missing candidate symbol: {symbol}")
        if symbol not in max_symbol_notional:
            raise KeyError(f"TVR decision risk max_symbol_notional_usdt missing candidate symbol: {symbol}")
    universe["tradable_symbols"] = list(candidate_symbols)
    risk["symbol_notional_usdt"] = {symbol: symbol_notional[symbol] for symbol in candidate_symbols}
    risk["max_symbol_notional_usdt"] = {symbol: max_symbol_notional[symbol] for symbol in candidate_symbols}
    out["universe"] = universe
    out["risk"] = risk
    return out


def _count_actions(events: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for event in events:
        if not isinstance(event, Mapping):
            continue
        action = str(event.get("action") or "").strip()
        if not action:
            action = "unknown"
        counts[action] = counts.get(action, 0) + 1
    return counts


def _has_interesting_actions(reconcile_events: list[dict[str, Any]], entry_events: list[dict[str, Any]]) -> bool:
    reconcile_actions = {str(event.get("action") or "") for event in reconcile_events if isinstance(event, Mapping)}
    entry_actions = {str(event.get("action") or "") for event in entry_events if isinstance(event, Mapping)}
    if reconcile_actions & IMPORTANT_RECONCILE_ACTIONS:
        return True
    if entry_actions & IMPORTANT_ENTRY_ACTIONS:
        return True
    noisy_reconcile = reconcile_actions - QUIET_RECONCILE_ACTIONS
    noisy_entry = entry_actions - QUIET_ENTRY_ACTIONS
    return bool(noisy_reconcile or noisy_entry)


def _log_iteration_summary(cfg: Mapping[str, Any], record: Mapping[str, Any]) -> None:
    reconcile_events = [dict(x) for x in list(record.get("reconcile_events") or []) if isinstance(x, Mapping)]
    entry_events = [dict(x) for x in list(record.get("entry_events") or []) if isinstance(x, Mapping)]
    signal_events = [dict(x) for x in list(record.get("signal_events") or []) if isinstance(x, Mapping)]
    reconcile_counts = _count_actions(reconcile_events)
    entry_counts = _count_actions(entry_events)
    signal_counts = _count_actions(signal_events)
    now_ms = _now_utc_ms()
    _ITERATION_LOG_STATE["iterations"] = int(_ITERATION_LOG_STATE["iterations"]) + 1
    _ITERATION_LOG_STATE["quiet_reconcile_events"] = int(_ITERATION_LOG_STATE["quiet_reconcile_events"]) + sum(
        count for action, count in reconcile_counts.items() if action in QUIET_RECONCILE_ACTIONS
    )
    _ITERATION_LOG_STATE["quiet_entry_events"] = int(_ITERATION_LOG_STATE["quiet_entry_events"]) + sum(
        count for action, count in entry_counts.items() if action in QUIET_ENTRY_ACTIONS
    )
    _ITERATION_LOG_STATE["decision_selected_total"] = int(_ITERATION_LOG_STATE["decision_selected_total"]) + int(record.get("decision_selected_count") or 0)

    interval_ms = int(cfg["logging"]["summary_interval_secs"]) * 1000
    elapsed = now_ms - int(_ITERATION_LOG_STATE["last_summary_utc_ms"] or 0)
    important = bool(signal_events) or _has_interesting_actions(reconcile_events, entry_events)
    if not important and elapsed < interval_ms:
        return

    logging.info(
        "TVR live heartbeat | dry_run=%s | selected=%s | active=%s | signals=%s | reconcile=%s | entry_events=%s | quiet_reconcile_events=%s | quiet_entry_events=%s | iterations=%s",
        bool(record.get("dry_run")),
        list(record.get("selected_symbols") or []),
        list(record.get("active_symbols") or []),
        signal_counts,
        reconcile_counts,
        entry_counts,
        int(_ITERATION_LOG_STATE["quiet_reconcile_events"]),
        int(_ITERATION_LOG_STATE["quiet_entry_events"]),
        int(_ITERATION_LOG_STATE["iterations"]),
    )
    _ITERATION_LOG_STATE["last_summary_utc_ms"] = now_ms
    _ITERATION_LOG_STATE["iterations"] = 0
    _ITERATION_LOG_STATE["quiet_reconcile_events"] = 0
    _ITERATION_LOG_STATE["quiet_entry_events"] = 0
    _ITERATION_LOG_STATE["decision_selected_total"] = 0


def run_once(cfg: Mapping[str, Any], *, run_id: str, dry_run: bool) -> Path | None:
    if not bool(cfg["enabled"]):
        raise RuntimeError("TVR live trader config enabled=false")
    account = str(cfg["account"]).strip()
    mark_loop_heartbeat(account, runner_pid=os.getpid(), strategy_name=STRATEGY_NAME)
    decision_cfg = load_decision_audit_config(str(cfg["decision_audit"]["config_path"]))
    if str(decision_cfg.get("account") or "").strip() != account:
        raise ValueError("TVR embedded decision config account mismatch")

    reconcile_events = _reconcile_local_state(cfg, dry_run=dry_run)
    state = load_live_state(account, strategy_name=STRATEGY_NAME)
    active_symbols = _active_local_symbols(state)
    entry_events: list[dict[str, Any]] = []
    signal_events: list[dict[str, Any]] = []

    decision: dict[str, Any] | None = None
    decision_path: Path | None = None
    decision_skip_reason: str | None = None
    selected_intents: list[dict[str, Any]] = []
    max_open_trades = int(cfg["execution"]["max_open_trades"])
    if len(active_symbols) >= max_open_trades:
        decision_skip_reason = "max_open_trades_reached"
        entry_events.append({"action": "entry_blocked_max_open_trades", "active_symbols": active_symbols})
    else:
        active_set = set(active_symbols)
        tradable_symbols = [str(symbol).upper().strip() for symbol in list(decision_cfg["universe"]["tradable_symbols"])]
        candidate_symbols = [symbol for symbol in tradable_symbols if symbol and symbol not in active_set]
        if not candidate_symbols:
            decision_skip_reason = "no_candidate_symbols_after_active_filter"
            entry_events.append({"action": "entry_skipped_no_candidate_symbols", "active_symbols": active_symbols})
        elif active_symbols and _now_utc_ms() - int(_DECISION_THROTTLE_STATE["last_decision_utc_ms"] or 0) < int(cfg["collection"]["active_decision_interval_secs"]) * 1000:
            decision_skip_reason = "active_decision_interval_wait"
            entry_events.append({
                "action": "entry_skipped_active_decision_interval",
                "active_symbols": active_symbols,
                "candidate_symbols": candidate_symbols,
            })
        else:
            filtered_decision_cfg = _decision_cfg_for_candidate_symbols(decision_cfg, candidate_symbols)
            decision_run_id = _build_decision_run_id(account)
            decision = build_decision_audit(filtered_decision_cfg, run_id=decision_run_id)
            _DECISION_THROTTLE_STATE["last_decision_utc_ms"] = _now_utc_ms()
            decision_path = write_decision_audit_record(decision) if bool(filtered_decision_cfg.get("audit_enabled")) else None
            if str(decision.get("account") or "").strip() != account:
                raise ValueError("TVR embedded decision account mismatch")
            if bool(decision.get("order_submission_enabled")) != bool(cfg["decision_audit"]["required_order_submission_enabled"]):
                raise ValueError("TVR decision audit order_submission_enabled does not match required value")
            selected_intents = _selected_intents(decision)

        for intent in selected_intents:
            if len(entry_events) >= int(cfg["execution"]["max_new_entries_per_iteration"]):
                break
            symbol = str(intent.get("symbol") or "").upper().strip()
            if symbol in active_symbols:
                entry_events.append({"action": "entry_skipped_local_active_symbol", "symbol": symbol})
                continue
            signal_events.append(_emit_signal_locked(cfg, decision, intent))
            event = _place_entry_from_intent(cfg=cfg, decision=decision, intent=intent, dry_run=dry_run)
            entry_events.append(event)
            if event.get("action") in {"entry_submitted", "dry_run_entry"}:
                active_symbols.append(symbol)

    record = {
        **_base_record(run_id, "tvr_live_trader_iteration"),
        "account": account,
        "dry_run": bool(dry_run),
        "allow_live_order": bool(cfg["allow_live_order"]),
        "decision_mode": "embedded",
        "decision_config_path": str(cfg["decision_audit"]["config_path"]),
        "decision_audit_path": str(decision_path) if decision_path is not None else None,
        "decision_skip_reason": decision_skip_reason,
        "decision_audit_run_id": decision.get("run_id") if decision is not None else None,
        "decision_collected_bj": decision.get("collected_bj") if decision is not None else None,
        "decision_data_hub_inputs": dict(decision.get("data_hub_inputs") or {}) if decision is not None else {},
        "decision_eligible_count": decision.get("eligible_count") if decision is not None else 0,
        "decision_selected_count": decision.get("selected_count") if decision is not None else 0,
        "selected_symbols": list(decision.get("selected_symbols") or []) if decision is not None else [],
        "active_symbols": list(active_symbols),
        "signal_events": signal_events,
        "reconcile_events": reconcile_events,
        "entry_events": entry_events,
    }
    _log_iteration_summary(cfg, record)
    if not bool(cfg["audit_enabled"]):
        return None
    return _append_jsonl(_audit_path(), record)


def _build_run_id(account: str) -> str:
    account_key = str(account).upper().strip()
    if not account_key:
        raise ValueError("account must not be empty")
    ts_utc = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"TVR_LIVE_TRADER_{account_key}_{ts_utc}"


def _build_decision_run_id(account: str) -> str:
    account_key = str(account).upper().strip()
    if not account_key:
        raise ValueError("account must not be empty")
    ts_utc = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"TVR_LIVE_DECISION_{account_key}_{ts_utc}_{_now_utc_ms()}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="TVR live trader: submit small maker-only LONG orders from TVR decision audit")
    parser.add_argument("--config", default="strategies/tvr/config.live_trader.smoke_10u.json")
    parser.add_argument("--once", action="store_true", help="run one live trader iteration")
    parser.add_argument("--loop", action="store_true", help="run live trader loop")
    parser.add_argument("--max-iterations", type=int, default=0, help="loop iteration cap; 0 means unlimited")
    parser.add_argument("--dry-run", action="store_true", help="do not submit orders or mutate open/pending state")
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
        logging.debug("TVR live trader iteration started | run_id=%s | iteration=%s | dry_run=%s", run_id, iteration, args.dry_run)
        path = run_once(cfg, run_id=run_id, dry_run=bool(args.dry_run))
        logging.debug("TVR live trader iteration finished | run_id=%s | iteration=%s | path=%s", run_id, iteration, path)
        if args.once:
            break
        if int(args.max_iterations) > 0 and iteration >= int(args.max_iterations):
            break
        time.sleep(int(cfg["collection"]["interval_secs"]))


if __name__ == "__main__":
    main()
