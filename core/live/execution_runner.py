from __future__ import annotations

import json
import logging
import os
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Mapping

from core.live.audit_log import write_strategy_event
from core.live.binance_exec import (
    cancel_order,
    ensure_cross_margin,
    ensure_hedge_mode,
    ensure_leverage,
    get_last_price,
    get_open_orders,
    get_order,
    get_position,
    get_symbol_filters,
    place_entry_order,
    place_sl_order,
    place_time_stop_order,
    place_tp_order,
    resolve_order_fill_price,
)
from core.live.custom_id import BROKER_ID, build_client_order_id
from core.live.execution_intent import ValidatedLiveExecutionIntent, validate_live_execution_intent
from core.live.live_state import (
    load_live_state,
    mark_error,
    mark_last_processed_bar,
    mark_order_reconcile,
    mark_position_reconcile,
    mark_signal,
    set_cooldown,
    set_open_trade,
    set_pending_entry_order,
)
from core.message_bridge import send_to_bot

BJ = timezone(timedelta(hours=8))
POSITION_SIDE_LONG = "LONG"
FILLED_ORDER_STATUSES = {"FILLED", "FINISHED"}
TERMINAL_ORDER_STATUSES = {"FILLED", "FINISHED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED"}
DEFAULT_STRATEGY_NAME = "spring-sabc"
DEFAULT_PROJECTION_DIR = "output/live_projection"
SPRING_NOTIFY_LABEL = "spring"
SWR_NOTIFY_LABEL = "swr"
EXIT_REASON_TIME_STOP = "TIME_STOP"
EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN = "SL_SUBMIT_FAILED_FLATTEN"
LEG_SL_FAIL_FLATTEN = "SF"


def _now_bj_str() -> str:
    return datetime.now(timezone.utc).astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_hms_from_ms(ts_ms: Any) -> str:
    try:
        value = int(ts_ms)
    except (TypeError, ValueError):
        return "UNKNOWN"
    if value <= 0:
        return "UNKNOWN"
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%H:%M:%S")


def _fmt_short_bj_from_ms(ts_ms: Any) -> str:
    try:
        value = int(ts_ms)
    except (TypeError, ValueError):
        return "NA"
    if value <= 0:
        return "NA"
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%m-%d %H:%M")


def _safe_float(value: Any, default: float | None = None) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _fmt_notify_price(value: Any) -> str:
    safe = _safe_float(value)
    if safe is None or safe <= 0:
        return "NA"
    return f"{safe:.6f}"


def _fmt_notify_pct(value: Any) -> str:
    safe = _safe_float(value)
    if safe is None:
        return "NA"
    return f"{safe * 100.0:.2f}%"


def _fmt_notify_float(value: Any) -> str:
    safe = _safe_float(value)
    if safe is None:
        return "NA"
    return f"{safe:.2f}"


def _extract_event_time_ms(*payloads: Any) -> int | None:
    for payload in payloads:
        if not isinstance(payload, Mapping):
            continue
        for key in ("update_time_ms", "time_ms", "updateTime", "time", "createTime", "transactTime"):
            try:
                value = int(payload.get(key))
            except (TypeError, ValueError):
                continue
            if value > 0:
                return value
    return None


def _spring_notify_header(account: str, event_time_ms: Any) -> str:
    return f"[{_fmt_hms_from_ms(event_time_ms)} 🌱 SPR] {account}"


def _strategy_notify_label(strategy_name: Any) -> str:
    if str(strategy_name or "").strip() == "sweep-reclaim":
        return SWR_NOTIFY_LABEL
    return SPRING_NOTIFY_LABEL


def _strategy_notify_header(account: str, event_time_ms: Any, *, strategy_name: Any, strategy_code: Any) -> str:
    if str(strategy_name or "").strip() == "sweep-reclaim":
        return f"[{_fmt_hms_from_ms(event_time_ms)} 📈 SWR] {account}"
    return _spring_notify_header(account, event_time_ms)


def _require(payload: Mapping[str, Any], field: str) -> Any:
    if field not in payload:
        raise KeyError(f"live execution config missing required field: {field}")
    value = payload[field]
    if value is None:
        raise ValueError(f"live execution config field is null: {field}")
    return value


def _require_mapping(payload: Mapping[str, Any], field: str) -> dict[str, Any]:
    value = _require(payload, field)
    if not isinstance(value, Mapping):
        raise TypeError(f"live execution config field must be object: {field}")
    return dict(value)


def _require_str(payload: Mapping[str, Any], field: str) -> str:
    value = str(_require(payload, field)).strip()
    if not value:
        raise ValueError(f"live execution config field is empty: {field}")
    return value


def _require_bool(payload: Mapping[str, Any], field: str) -> bool:
    value = _require(payload, field)
    if not isinstance(value, bool):
        raise TypeError(f"live execution config field must be bool: {field}")
    return bool(value)


def _require_int(payload: Mapping[str, Any], field: str, *, min_value: int | None = None) -> int:
    value = _require(payload, field)
    if isinstance(value, bool):
        raise TypeError(f"live execution config field must be int: {field}")
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise TypeError(f"live execution config field must be int: {field}") from None
    if min_value is not None and parsed < min_value:
        raise ValueError(f"live execution config field {field} must be >= {min_value}, got {parsed}")
    return parsed


def _require_float(payload: Mapping[str, Any], field: str, *, min_value: float | None = None) -> float:
    value = _require(payload, field)
    if isinstance(value, bool):
        raise TypeError(f"live execution config field must be float: {field}")
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        raise TypeError(f"live execution config field must be float: {field}") from None
    if min_value is not None and parsed < min_value:
        raise ValueError(f"live execution config field {field} must be >= {min_value}, got {parsed}")
    return parsed


def load_live_execution_config(path: str | Path) -> dict[str, Any]:
    config_path = Path(path)
    if not str(config_path).strip():
        raise ValueError("live execution config path must not be empty")
    if not config_path.exists():
        raise FileNotFoundError(f"live execution config not found: {config_path}")
    data = json.loads(config_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise TypeError(f"live execution config must be object, got {type(data).__name__}")
    return data


def _validate_order_contract(config: Mapping[str, Any]) -> None:
    entry = _require_mapping(config, "entry")
    stop_loss = _require_mapping(config, "stop_loss")
    take_profit = _require_mapping(config, "take_profit")
    time_stop = _require_mapping(config, "time_stop")
    if _require_str(entry, "order_type").upper() != "MARKET":
        raise ValueError("entry.order_type must be MARKET")
    if _require_str(stop_loss, "order_type").upper() != "STOP_MARKET":
        raise ValueError("stop_loss.order_type must be STOP_MARKET")
    if _require_str(stop_loss, "working_type").upper() != "CONTRACT_PRICE":
        raise ValueError("stop_loss.working_type must be CONTRACT_PRICE")
    if not _require_bool(stop_loss, "close_position"):
        raise ValueError("stop_loss.close_position must be true")
    if _require_str(take_profit, "order_type").upper() != "LIMIT":
        raise ValueError("take_profit.order_type must be LIMIT")
    if _require_str(take_profit, "time_in_force").upper() != "GTC":
        raise ValueError("take_profit.time_in_force must be GTC")
    if _require_str(take_profit, "working_type").upper() != "CONTRACT_PRICE":
        raise ValueError("take_profit.working_type must be CONTRACT_PRICE")
    if _require_str(time_stop, "order_type").upper() != "MARKET":
        raise ValueError("time_stop.order_type must be MARKET")


def validate_live_execution_config(
    config: Mapping[str, Any],
    *,
    intent: ValidatedLiveExecutionIntent | Mapping[str, Any],
) -> dict[str, Any]:
    intent_dict = intent.to_dict() if isinstance(intent, ValidatedLiveExecutionIntent) else validate_live_execution_intent(intent).to_dict()
    cfg = dict(config)
    if _require_int(cfg, "schema_version", min_value=1) != 1:
        raise ValueError("live execution config schema_version must be 1")
    if _require_str(cfg, "strategy_name") != intent_dict["strategy_name"]:
        raise ValueError("live execution config strategy_name does not match intent")
    if _require_str(cfg, "strategy_code").upper() != str(intent_dict["strategy_code"]).upper():
        raise ValueError("live execution config strategy_code does not match intent")
    if _require_str(cfg, "account") != intent_dict["account"]:
        raise ValueError("live execution config account does not match intent")
    if _require_str(cfg, "execution_mode") != "live_once":
        raise ValueError("live execution config execution_mode must be live_once")
    if not _require_bool(cfg, "allow_live_order"):
        raise ValueError("live execution config allow_live_order must be true")
    if _require_str(cfg, "position_side").upper() != POSITION_SIDE_LONG:
        raise ValueError("live execution config position_side must be LONG")
    if _require_str(cfg, "position_mode").upper() != "HEDGE":
        raise ValueError("live execution config position_mode must be HEDGE")
    if _require_str(cfg, "margin_type").upper() != "CROSSED":
        raise ValueError("live execution config margin_type must be CROSSED")
    if _require_str(cfg, "precheck_scope") not in {"symbol", "account_flat"}:
        raise ValueError("live execution config precheck_scope must be symbol or account_flat")
    if _require_str(cfg, "strategy_concurrency_scope") not in {"symbol", "account"}:
        raise ValueError("live execution config strategy_concurrency_scope must be symbol or account")
    if _require_str(cfg, "stop_loss_failure_action") != "submit_market_flatten":
        raise ValueError("live execution config stop_loss_failure_action must be submit_market_flatten")
    _require_bool(cfg, "audit_enabled")
    _require_bool(cfg, "notify_enabled")
    _require_bool(cfg, "notify_on_signal_locked")
    _require_bool(cfg, "notify_on_order_submit")
    _require_bool(cfg, "notify_on_exit_detected")
    _require_bool(cfg, "notify_on_order_error")
    _require_bool(cfg, "require_local_state_precheck")
    _require_bool(cfg, "require_exchange_precheck")
    _require_bool(cfg, "require_symbol_filters")
    _require_int(cfg, "leverage", min_value=1)
    _require_int(cfg, "order_retry_max", min_value=0)
    _require_int(cfg, "cooldown_mins", min_value=0)
    _require_float(cfg, "api_retry_delay_secs", min_value=0.0)
    _require_float(cfg, "pre_entry_min_sl_distance_pct", min_value=0.0)
    _require_float(cfg, "min_position_notional_usdt", min_value=0.0)
    _require_float(cfg, "max_position_notional_usdt", min_value=0.0)
    if _require_float(cfg, "max_position_notional_usdt", min_value=0.0) <= 0:
        raise ValueError("live execution config max_position_notional_usdt must be > 0")
    _validate_order_contract(cfg)
    return cfg


def _exchange_res_ok(snapshot: Mapping[str, Any], field: str) -> bool:
    value = snapshot.get(field)
    return isinstance(value, Mapping) and bool(value.get("ok"))


def _exchange_rows(snapshot: Mapping[str, Any], field: str) -> list[dict[str, Any]]:
    value = snapshot.get(field)
    if not isinstance(value, Mapping):
        return []
    return [dict(row) for row in (value.get("data") or []) if isinstance(row, Mapping)]


def _account_flat_precheck(snapshot: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(snapshot, Mapping):
        return {"ok": False, "blockers": ["exchange_snapshot_missing"]}
    blockers: list[str] = []
    if not _exchange_res_ok(snapshot, "positions"):
        blockers.append("precheck_positions_query_failed")
    if not _exchange_res_ok(snapshot, "orders"):
        blockers.append("precheck_orders_query_failed")
    positions = _exchange_rows(snapshot, "positions")
    orders = _exchange_rows(snapshot, "orders")
    if positions:
        blockers.append("exchange_account_has_position")
    if orders:
        blockers.append("exchange_account_has_open_orders")
    return {
        "ok": not blockers,
        "blockers": blockers,
        "positions_count": len(positions),
        "open_orders_count": len(orders),
    }


def _floor_to_step(value: float, step: float | None) -> float:
    if step is None or step <= 0:
        return float(value)
    dec_value = Decimal(str(value))
    dec_step = Decimal(str(step))
    return float((dec_value / dec_step).to_integral_value(rounding=ROUND_DOWN) * dec_step)


def _symbol_filter_precheck(account: str, symbol: str, *, quantity: float, current_price: float) -> dict[str, Any]:
    filters_res = get_symbol_filters(account, symbol)
    if not filters_res.get("ok"):
        return {"ok": False, "blockers": ["symbol_filters_query_failed"], "exchange_snapshot": filters_res}
    filters = dict(filters_res["data"])
    qty = _floor_to_step(float(quantity), filters.get("step_size"))
    notional = qty * float(current_price)
    blockers: list[str] = []
    if qty <= 0:
        blockers.append("quantity_after_step_floor_non_positive")
    min_qty = float(filters.get("min_qty") or 0.0)
    if min_qty > 0 and qty < min_qty:
        blockers.append("quantity_below_exchange_min_qty")
    min_notional = float(filters.get("min_notional") or 0.0)
    if min_notional > 0 and notional < min_notional:
        blockers.append("notional_below_exchange_min_notional")
    return {
        "ok": not blockers,
        "blockers": blockers,
        "quantity": qty,
        "notional_usdt": notional,
        "filters": filters,
    }


def _resolve_position_notional(intent: Mapping[str, Any], *, entry_reference_price: float) -> dict[str, float]:
    sl_price = float(intent["sl_price"])
    risk_pct = (float(entry_reference_price) - sl_price) / float(entry_reference_price)
    if risk_pct <= 0:
        raise ValueError(f"LONG entry reference price must be above sl_price, got {entry_reference_price} <= {sl_price}")
    base_notional = float(intent["base_order_notional_usdt"])
    full_risk_pct = float(intent["full_notional_risk_pct"])
    sizing_ratio = min(1.0, full_risk_pct / float(risk_pct))
    position_notional_usdt = base_notional * sizing_ratio
    return {
        "risk_pct": float(risk_pct),
        "sizing_ratio": float(sizing_ratio),
        "position_notional_usdt": float(position_notional_usdt),
        "planned_sl_loss_usdt": float(position_notional_usdt * risk_pct),
    }


def _resolve_tp_price(intent: Mapping[str, Any], *, entry_price: float) -> tuple[float, str]:
    sl_price = float(intent["sl_price"])
    take_profit_mode = str(intent["take_profit_mode"]).strip()
    if take_profit_mode == "risk_reward_1r":
        risk_distance = float(entry_price) - sl_price
        if risk_distance <= 0:
            raise ValueError(f"LONG entry_price must be above sl_price for risk_reward_1r, got {entry_price} <= {sl_price}")
        tp_price = float(entry_price) + risk_distance
        source = "entry_fill_rr_1r"
    elif take_profit_mode == "risk_reward_r_multiple":
        risk_distance = float(entry_price) - sl_price
        if risk_distance <= 0:
            raise ValueError(f"LONG entry_price must be above sl_price for risk_reward_r_multiple, got {entry_price} <= {sl_price}")
        r_multiple = float(intent["take_profit_r_multiple"])
        if r_multiple <= 0:
            raise ValueError(f"risk_reward_r_multiple TP requires positive take_profit_r_multiple, got {r_multiple}")
        tp_price = float(entry_price) + risk_distance * r_multiple
        source = f"entry_fill_rr_{r_multiple:g}r"
    elif take_profit_mode == "fixed_pct":
        take_profit_pct = float(intent["take_profit_pct"])
        if take_profit_pct <= 0:
            raise ValueError(f"fixed_pct TP requires positive take_profit_pct, got {take_profit_pct}")
        tp_price = float(entry_price) * (1.0 + take_profit_pct)
        source = "entry_fill_fixed_pct"
    else:
        raise ValueError(f"unsupported take_profit_mode: {take_profit_mode!r}")
    if tp_price <= float(entry_price):
        raise ValueError(f"LONG resolved TP must be above entry_price, got {tp_price} <= {entry_price}")
    return float(tp_price), source


def _signal_digest(signal: Mapping[str, Any]) -> str:
    base = {
        "symbol": signal.get("symbol"),
        "signal_time": signal.get("signal_time"),
        "action": signal.get("action"),
        "sl_price": signal.get("sl_price"),
    }
    return json.dumps(base, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _resolve_entry_fill_price(account: str, symbol: str, entry_data: Mapping[str, Any], fallback_price: float) -> tuple[float, str]:
    fill_res = resolve_order_fill_price(dict(entry_data), fallback_price=None)
    fill_payload = fill_res.get("data") if fill_res.get("ok") else None
    if isinstance(fill_payload, Mapping):
        fill_price = float(fill_payload.get("fill_price") or 0.0)
        if fill_price > 0:
            return fill_price, str(fill_payload.get("price_source") or "order_fill")
    position_res = get_position(account, symbol, POSITION_SIDE_LONG)
    if position_res.get("ok") and position_res.get("data"):
        position_entry_price = float((position_res.get("data") or {}).get("entry_price") or 0.0)
        if position_entry_price > 0:
            return position_entry_price, "position_entry_price"
    fallback = float(fallback_price or 0.0)
    if fallback > 0:
        return fallback, "fallback_pre_entry_price"
    raise RuntimeError("entry fill price unavailable")


def _pending_entry_payload(
    *,
    intent: Mapping[str, Any],
    order_ids: Mapping[str, Any],
    order_root: str,
    entry_res: Mapping[str, Any],
    entry_price_source: str,
    signal_digest: str,
    pre_entry_price: float,
    pre_entry_price_source: str,
    position_notional_usdt: float,
    sizing: Mapping[str, Any],
    resolved_tp_price: float,
    resolved_tp_price_source: str,
) -> dict[str, Any]:
    entry = dict(entry_res.get("data") or {})
    return {
        "symbol": intent["symbol"],
        "strategy_name": intent["strategy_name"],
        "strategy_code": intent["strategy_code"],
        "order_root": order_root,
        "client_order_id": entry.get("client_order_id", order_ids["entry_client_order_id"]),
        "exchange_order_id": entry.get("exchange_order_id"),
        "signal_time": int(intent["signal_time"]),
        "signal_time_bj": intent["signal_time_bj"],
        "pre_entry_price": float(pre_entry_price),
        "pre_entry_price_source": pre_entry_price_source,
        "current_price": float(pre_entry_price),
        "entry_notional_usdt": float(position_notional_usdt),
        "signal_digest": signal_digest,
        "signal_snapshot": deepcopy(intent["signal_snapshot"]),
        "entry_fill_price_source": entry_price_source,
        "sizing": dict(sizing),
        "tp_price": float(resolved_tp_price),
        "resolved_tp_price_source": resolved_tp_price_source,
        "sl_price": float(intent["sl_price"]),
        "tp_client_order_id": order_ids["tp_client_order_id"],
        "sl_client_order_id": order_ids["sl_client_order_id"],
        "time_stop_client_order_id": order_ids["time_stop_client_order_id"],
        "max_hold_mins": int(intent["max_hold_mins"]),
        "time_stop_min_profit_pct": float(intent["time_stop_min_profit_pct"]),
        "created_bj": _now_bj_str(),
    }


def _open_trade_payload(
    *,
    intent: Mapping[str, Any],
    order_ids: Mapping[str, Any],
    order_root: str,
    entry_res: Mapping[str, Any],
    sl_res: Mapping[str, Any],
    tp_res: Mapping[str, Any],
    entry_price: float,
    entry_price_source: str,
    signal_digest: str,
    pre_entry_price: float,
    pre_entry_price_source: str,
    position_notional_usdt: float,
    sizing: Mapping[str, Any],
    resolved_tp_price: float,
    resolved_tp_price_source: str,
    status: str,
) -> dict[str, Any]:
    entry = dict(entry_res.get("data") or {})
    sl = dict(sl_res.get("data") or {}) if sl_res.get("ok") else {}
    tp = dict(tp_res.get("data") or {}) if tp_res.get("ok") else {}
    now_bj = _now_bj_str()
    return {
        "symbol": intent["symbol"],
        "strategy_name": intent["strategy_name"],
        "strategy_code": intent["strategy_code"],
        "side": POSITION_SIDE_LONG,
        "order_root": order_root,
        "entry_client_order_id": entry.get("client_order_id", order_ids["entry_client_order_id"]),
        "entry_exchange_order_id": entry.get("exchange_order_id"),
        "entry_ts": int(intent["signal_time"]),
        "entry_bj": intent["signal_time_bj"],
        "entry_price": float(entry_price),
        "entry_price_source": entry_price_source,
        "pre_entry_price": float(pre_entry_price),
        "pre_entry_price_source": pre_entry_price_source,
        "entry_qty": float(entry.get("executed_qty") or entry.get("qty") or 0.0),
        "entry_notional_usdt": float(position_notional_usdt),
        "sizing": dict(sizing),
        "signal_digest": signal_digest,
        "signal_snapshot": deepcopy(intent["signal_snapshot"]),
        "tp_order_client_id": tp.get("client_order_id", order_ids["tp_client_order_id"]) if tp else None,
        "tp_order_exchange_id": tp.get("exchange_order_id") if tp else None,
        "sl_order_client_id": sl.get("client_order_id", order_ids["sl_client_order_id"]) if sl else None,
        "sl_order_exchange_id": sl.get("exchange_order_id") if sl else None,
        "time_stop_client_order_id": None,
        "time_stop_exchange_order_id": None,
        "tp_price": float(resolved_tp_price),
        "resolved_tp_price_source": resolved_tp_price_source,
        "sl_trigger_price": float(intent["sl_price"]),
        "max_hold_mins": int(intent["max_hold_mins"]),
        "time_stop_min_profit_pct": float(intent["time_stop_min_profit_pct"]),
        "status": status,
        "exit_submit_inflight": False,
        "last_status_bj": now_bj,
        "time_stop_last_check_bj": None,
    }


def _cooldown_until(current_time_ms: int, cooldown_mins: int) -> tuple[int, str | None]:
    cooldown_until_ts = int(current_time_ms) + int(cooldown_mins) * 60 * 1000
    return cooldown_until_ts, _fmt_bj_from_ms(cooldown_until_ts)


def _strategy_name_from_exec_payload(payload: Mapping[str, Any]) -> str:
    explicit = str(payload.get("strategy_name") or "").strip()
    if explicit:
        return explicit
    source = str(payload.get("source") or "").strip().lower()
    if source.startswith("sweep_reclaim") or source.startswith("swr"):
        return "sweep-reclaim"
    return DEFAULT_STRATEGY_NAME


def _strategy_event_name(event: str, *, strategy_name: str) -> str:
    if strategy_name == "sweep-reclaim" and event.startswith("spring_"):
        return "sweep_reclaim_" + event[len("spring_"):]
    return event


def _write_exec_event(enabled: bool, account: str, event: str, payload: dict[str, Any]) -> None:
    if enabled:
        enriched = dict(payload)
        enriched["execution_contract"] = "live_execution"
        strategy_name = _strategy_name_from_exec_payload(enriched)
        enriched["strategy_name"] = strategy_name
        write_strategy_event(
            account,
            strategy_name,
            _strategy_event_name(event, strategy_name=strategy_name),
            enriched,
        )


def _notify_enabled(cfg: Mapping[str, Any], field: str) -> bool:
    return bool(_require_bool(cfg, "notify_enabled")) and bool(_require_bool(cfg, field))


def _send_spring_notify(message: str, *, strategy_name: Any = DEFAULT_STRATEGY_NAME) -> None:
    send_to_bot(message, label=_strategy_notify_label(strategy_name))


def is_live_symbol_supported_for_signed_api(symbol: Any) -> bool:
    text = str(symbol or "").strip()
    return bool(text) and text.isascii()


def _unsupported_live_symbol_reason(symbol: Any) -> str:
    return f"unsupported_live_symbol_non_ascii: symbol={str(symbol or '').strip()!r}"


def _json_default(value: Any) -> Any:
    if hasattr(value, "item"):
        return value.item()
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, tuple):
        return list(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def _projection_schema_version(cfg: Mapping[str, Any]) -> int:
    try:
        return int(cfg.get("_projection_schema_version") or 1)
    except (TypeError, ValueError):
        return 1


def _projection_run_id(cfg: Mapping[str, Any]) -> str:
    return str(cfg.get("_projection_run_id") or "UNSET").strip() or "UNSET"


def _projection_dir(cfg: Mapping[str, Any]) -> Path:
    raw = str(cfg.get("_projection_output_dir") or DEFAULT_PROJECTION_DIR).strip() or DEFAULT_PROJECTION_DIR
    return Path(raw)


def _projection_path(cfg: Mapping[str, Any], kind: str) -> Path:
    return _projection_dir(cfg) / f"{kind}.{_projection_run_id(cfg)}.jsonl"


def _append_projection_row(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False, default=_json_default) + "\n")
        f.flush()
        os.fsync(f.fileno())


def _spring_signal_message(account: str, intent: Mapping[str, Any]) -> str:
    signal = dict(intent.get("signal_snapshot") or {})
    context = dict(signal.get("context") or {})
    symbol = str(intent.get("symbol") or signal.get("symbol") or "").upper().strip()
    lines = [
        _spring_notify_header(account, intent.get("signal_time")),
        f"雷达锁定: {symbol}",
        f"pre-entry价: {_fmt_notify_price(intent.get('pre_entry_price'))}",
        (
            f"A: {_fmt_short_bj_from_ms(context.get('a_time_ms'))} "
            f"high={_fmt_notify_price(context.get('a_high', context.get('a_close')))}"
        ),
        (
            f"B: {_fmt_short_bj_from_ms(context.get('b_time_ms'))} "
            f"low={_fmt_notify_price(context.get('b_low', context.get('b_close')))}"
        ),
        f"C: {_fmt_short_bj_from_ms(context.get('c_time_ms'))} close={_fmt_notify_price(context.get('c_close'))}",
        f"24h涨幅: {_fmt_notify_pct(context.get('chg_24h'))}",
        f"24h成交额: {_safe_float(context.get('vol_24h'), 0.0):.0f}",
        f"AB跌幅: {_fmt_notify_pct(context.get('ab_chg_pct'))}",
        f"爆量倍数: {_fmt_notify_float(context.get('vol_ratio'))}",
        f"AC/γA量比: {_fmt_notify_float(context.get('gamma_ac_vol_ratio'))}",
        f"反弹比例: {_fmt_notify_pct(context.get('rebound_ratio'))}",
        f"AB/BC: {int(_safe_float(context.get('ab_bars'), 0.0) or 0)}/{int(_safe_float(context.get('bc_bars'), 0.0) or 0)}",
        f"开仓金额: {_safe_float(intent.get('position_notional_usdt'), 0.0):.2f}U",
        f"评分: {int(_safe_float(context.get('score'), 0.0) or 0)} (#{int(_safe_float(context.get('score_order'), 0.0) or 0)})",
    ]
    return "\n".join(lines)


def _sweep_reclaim_signal_message(account: str, intent: Mapping[str, Any]) -> str:
    signal = dict(intent.get("signal_snapshot") or {})
    context = dict(signal.get("context") or {})
    symbol = str(intent.get("symbol") or signal.get("symbol") or "").upper().strip()
    lines = [
        _strategy_notify_header(
            account,
            intent.get("signal_time"),
            strategy_name="sweep-reclaim",
            strategy_code="SWR",
        ),
        f"雷达锁定: {symbol}",
        f"pre-entry价: {_fmt_notify_price(intent.get('pre_entry_price'))}",
        f"H: {_fmt_short_bj_from_ms(context.get('h_time_ms'))} close={_fmt_notify_price(context.get('h_close'))}",
        f"B: {_fmt_short_bj_from_ms(context.get('b_time_ms'))} low={_fmt_notify_price(context.get('b_low'))}",
        f"C: {_fmt_short_bj_from_ms(context.get('c_time_ms'))} close={_fmt_notify_price(context.get('c_close'))}",
        f"HB跌幅: {_fmt_notify_pct(context.get('hb_drop'))}",
        f"修复: {_fmt_notify_pct(context.get('bc_rebound'))}",
        f"速度: {_fmt_notify_float(context.get('bc_over_hb_bars'))}",
        f"爆量: {_fmt_notify_float(context.get('vol_climax'))}",
        f"开仓金额: {_safe_float(intent.get('position_notional_usdt'), 0.0):.2f}U",
        f"TP: {_fmt_notify_float(intent.get('take_profit_r_multiple'))}R",
        f"评分: {int(_safe_float(context.get('score'), 0.0) or 0)} (#{int(_safe_float(context.get('score_order'), 0.0) or 0)})",
    ]
    return "\n".join(lines)


def _strategy_signal_message(account: str, intent: Mapping[str, Any]) -> str:
    if str(intent.get("strategy_name") or "").strip() == "sweep-reclaim":
        return _sweep_reclaim_signal_message(account, intent)
    return _spring_signal_message(account, intent)


def _spring_entry_message(account: str, open_trade: Mapping[str, Any], event_time_ms: Any) -> str:
    symbol = str(open_trade.get("symbol") or "").upper().strip()
    return "\n".join([
        _strategy_notify_header(
            account,
            event_time_ms or open_trade.get("entry_ts"),
            strategy_name=open_trade.get("strategy_name"),
            strategy_code=open_trade.get("strategy_code"),
        ),
        f"开仓 {symbol}",
        f"entry≈{_fmt_notify_price(open_trade.get('entry_price'))}",
        f"TP={_fmt_notify_price(open_trade.get('tp_price'))}",
        f"SL={_fmt_notify_price(open_trade.get('sl_trigger_price'))}",
    ])


def _spring_exit_message(
    account: str,
    closed_trade: Mapping[str, Any],
    *,
    exit_reason: str,
    event_time_ms: Any,
) -> str:
    entry_ts = int(closed_trade.get("entry_ts") or 0)
    exit_ts = int(event_time_ms or 0)
    hold_mins = "NA"
    if entry_ts > 0 and exit_ts >= entry_ts:
        hold_mins = f"{(exit_ts - entry_ts) / 60000.0:.1f}m"
    entry_price = _safe_float(closed_trade.get("entry_price"))
    exit_price = _safe_float(closed_trade.get("exit_price"))
    pnl_pct = None
    if entry_price and exit_price:
        pnl_pct = exit_price / entry_price - 1.0
    symbol = str(closed_trade.get("symbol") or "").upper().strip()
    return "\n".join([
        _strategy_notify_header(
            account,
            event_time_ms,
            strategy_name=closed_trade.get("strategy_name"),
            strategy_code=closed_trade.get("strategy_code"),
        ),
        f"离场 {symbol}",
        f"reason={exit_reason}",
        f"entry≈{_fmt_notify_price(entry_price)}",
        f"exit≈{_fmt_notify_price(exit_price)}",
        f"持仓={hold_mins}",
        f"pnl={_fmt_notify_pct(pnl_pct)}",
    ])


def _is_order_not_exist_reason(reason: Any) -> bool:
    text = str(reason or "")
    return (
        "code=-2013" in text
        or "Order does not exist" in text
        or "Unknown order sent" in text
        or "algo order does not exist" in text
        or "order not exist" in text
    )


def _find_open_order(
    orders: list[dict[str, Any]],
    *,
    exchange_order_id: Any = None,
    client_order_id: Any = None,
) -> dict[str, Any] | None:
    exchange_key = str(exchange_order_id).strip() if exchange_order_id not in (None, "") else ""
    client_key = str(client_order_id).strip() if client_order_id not in (None, "") else ""
    if not exchange_key and not client_key:
        return None
    for row in orders:
        if not isinstance(row, Mapping):
            continue
        row_order_id = str(row.get("order_id") or row.get("exchange_order_id") or row.get("algo_id") or "").strip()
        row_client_id = str(row.get("client_order_id") or row.get("client_algo_id") or "").strip()
        if exchange_key and row_order_id == exchange_key:
            return dict(row)
        if client_key and row_client_id == client_key:
            return dict(row)
    return None


def _resolve_leg_order(
    account: str,
    symbol: str,
    *,
    known_open_orders: list[dict[str, Any]],
    exchange_order_id: Any = None,
    client_order_id: Any = None,
    retry_max: int,
    retry_delay_secs: float,
) -> dict[str, Any]:
    if exchange_order_id in (None, "") and not client_order_id:
        return {"ok": True, "reason": "", "data": None, "skipped": True, "missing_identity": True}
    matched_open_order = _find_open_order(
        known_open_orders,
        exchange_order_id=exchange_order_id,
        client_order_id=client_order_id,
    )
    if matched_open_order is not None:
        return {
            "ok": True,
            "reason": "",
            "data": matched_open_order,
            "skipped": True,
            "known_open_order_snapshot": True,
        }
    order_res = get_order(
        account,
        symbol,
        exchange_order_id=int(exchange_order_id) if exchange_order_id not in (None, "") else None,
        client_order_id=str(client_order_id).strip() if client_order_id else None,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if (not order_res.get("ok")) and _is_order_not_exist_reason(order_res.get("reason")):
        return {
            "ok": True,
            "reason": "",
            "data": None,
            "skipped": True,
            "missing_on_exchange": True,
            "missing_reason": order_res.get("reason"),
        }
    return order_res


def _cancel_order_if_present(
    account: str,
    symbol: str,
    *,
    known_open_orders: list[dict[str, Any]],
    exchange_order_id: Any = None,
    client_order_id: Any = None,
    prefetched_order_res: dict[str, Any] | None = None,
    retry_max: int,
    retry_delay_secs: float,
) -> dict[str, Any]:
    if exchange_order_id in (None, "") and not client_order_id:
        return {"ok": True, "reason": "", "data": None, "skipped": True, "missing_identity": True}
    matched_open_order = _find_open_order(
        known_open_orders,
        exchange_order_id=exchange_order_id,
        client_order_id=client_order_id,
    )
    if matched_open_order is None:
        return {"ok": True, "reason": "", "data": None, "skipped": True, "not_in_open_orders_snapshot": True}
    order_res = prefetched_order_res if isinstance(prefetched_order_res, dict) else None
    if order_res and order_res.get("ok") and order_res.get("data"):
        status = str((order_res.get("data") or {}).get("status") or "").upper()
        if status in TERMINAL_ORDER_STATUSES:
            return {"ok": True, "reason": "", "data": order_res.get("data"), "skipped": True, "already_terminal": True}
    cancel_res = cancel_order(
        account,
        symbol,
        exchange_order_id=int(exchange_order_id) if exchange_order_id not in (None, "") else None,
        client_order_id=str(client_order_id).strip() if client_order_id else None,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
        notify_label=SPRING_NOTIFY_LABEL,
    )
    if cancel_res.get("ok"):
        cancel_res = dict(cancel_res)
        cancel_res["matched_open_order_snapshot"] = matched_open_order
    return cancel_res


def _infer_exit_reason_from_orders(
    account: str,
    symbol: str,
    open_trade: Mapping[str, Any],
    *,
    known_open_orders: list[dict[str, Any]],
    retry_max: int,
    retry_delay_secs: float,
) -> tuple[str, dict[str, Any], str | None]:
    checks: dict[str, Any] = {}
    time_stop_exit_reason = str(
        open_trade.get("protective_flatten_exit_reason")
        or open_trade.get("time_stop_exit_reason")
        or EXIT_REASON_TIME_STOP
    ).upper().strip()
    if time_stop_exit_reason != EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN:
        time_stop_exit_reason = EXIT_REASON_TIME_STOP
    legs = [
        (
            "time_stop",
            time_stop_exit_reason,
            open_trade.get("time_stop_exchange_order_id"),
            open_trade.get("time_stop_client_order_id"),
        ),
        (
            "tp",
            "TAKE_PROFIT",
            open_trade.get("tp_order_exchange_id"),
            open_trade.get("tp_order_client_id"),
        ),
        (
            "sl",
            "STOP_LOSS",
            open_trade.get("sl_order_exchange_id"),
            open_trade.get("sl_order_client_id"),
        ),
    ]
    for leg_key, exit_reason, exchange_order_id, client_order_id in legs:
        leg_res = _resolve_leg_order(
            account,
            symbol,
            known_open_orders=known_open_orders,
            exchange_order_id=exchange_order_id,
            client_order_id=client_order_id,
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
        )
        checks[leg_key] = leg_res
        status = str(((leg_res.get("data") or {}) if leg_res.get("ok") else {}).get("status") or "").upper()
        if status in FILLED_ORDER_STATUSES:
            return exit_reason, checks, None

    blocking_reason = None
    for leg_key in ("time_stop", "tp", "sl"):
        leg_res = checks.get(leg_key) or {}
        if leg_res.get("ok"):
            continue
        blocking_reason = str(leg_res.get("reason") or f"{leg_key}_query_failed")
        break
    return "UNKNOWN_EXIT", checks, blocking_reason


def _exit_order_from_checks(exit_reason: str, checks: Mapping[str, Any]) -> dict[str, Any] | None:
    leg = "tp" if exit_reason == "TAKE_PROFIT" else "sl" if exit_reason == "STOP_LOSS" else "time_stop" if exit_reason in {EXIT_REASON_TIME_STOP, EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN} else ""
    if not leg:
        return None
    payload = checks.get(leg)
    if isinstance(payload, Mapping) and isinstance(payload.get("data"), Mapping):
        return dict(payload["data"])
    return None


def _resolve_exit_price(exit_reason: str, open_trade: Mapping[str, Any], exit_order: Mapping[str, Any] | None) -> tuple[float | None, str]:
    fallback_price = float(open_trade.get("entry_price") or 0.0)
    fallback_source = "fallback_entry_price"
    if exit_reason == "TAKE_PROFIT" and float(open_trade.get("tp_price") or 0.0) > 0:
        fallback_price = float(open_trade["tp_price"])
        fallback_source = "fallback_tp_price"
    elif exit_reason == "STOP_LOSS" and float(open_trade.get("sl_trigger_price") or 0.0) > 0:
        fallback_price = float(open_trade["sl_trigger_price"])
        fallback_source = "fallback_sl_price"
    fill_res = resolve_order_fill_price(dict(exit_order or {}), fallback_price=fallback_price if fallback_price > 0 else None)
    payload = dict(fill_res.get("data") or {}) if fill_res.get("ok") else {}
    price = payload.get("fill_price")
    try:
        parsed_price = float(price)
    except (TypeError, ValueError):
        parsed_price = None
    return parsed_price, str(payload.get("price_source") or fallback_source)


def _signal_context(signal_snapshot: Any) -> Mapping[str, Any]:
    if not isinstance(signal_snapshot, Mapping):
        return {}
    context = signal_snapshot.get("context")
    return context if isinstance(context, Mapping) else {}


def _live_trade_projection_row(
    *,
    account: str,
    cfg: Mapping[str, Any],
    closed_trade: Mapping[str, Any],
    source: str,
    current_time_ms: int,
    current_time_bj: str,
    order_checks: Mapping[str, Any],
    cleanup_checks: Mapping[str, Any],
) -> dict[str, Any]:
    entry_ts = int(closed_trade.get("entry_ts") or 0)
    exit_ts = int(closed_trade.get("exit_detected_bar_ts") or current_time_ms)
    entry_price = _safe_float(closed_trade.get("entry_price"))
    exit_price = _safe_float(closed_trade.get("exit_price"))
    pnl_pct = None
    if entry_price and exit_price:
        pnl_pct = exit_price / entry_price - 1.0
    return {
        "schema_version": 1,
        "run_mode": "live_execution",
        "projection_type": "live_trade",
        "projection_schema_version": _projection_schema_version(cfg),
        "strategy_name": str(closed_trade.get("strategy_name") or cfg.get("strategy_name") or DEFAULT_STRATEGY_NAME),
        "strategy_code": str(closed_trade.get("strategy_code") or cfg.get("strategy_code") or "").upper().strip(),
        "account": str(account),
        "run_id": _projection_run_id(cfg),
        "source": source,
        "symbol": str(closed_trade.get("symbol") or "").upper().strip(),
        "side": POSITION_SIDE_LONG,
        "order_root": closed_trade.get("order_root"),
        "signal_time": closed_trade.get("entry_ts"),
        "signal_time_bj": closed_trade.get("entry_bj"),
        "c_bar_ts": _signal_context(closed_trade.get("signal_snapshot")).get("c_time_ms"),
        "entry_time_ms": entry_ts or None,
        "entry_time_bj": closed_trade.get("entry_bj") or _fmt_bj_from_ms(entry_ts) if entry_ts else None,
        "entry_price": entry_price,
        "entry_price_source": closed_trade.get("entry_price_source"),
        "pre_entry_price": _safe_float(closed_trade.get("pre_entry_price")),
        "pre_entry_price_source": closed_trade.get("pre_entry_price_source"),
        "entry_qty": _safe_float(closed_trade.get("entry_qty")),
        "entry_notional_usdt": _safe_float(closed_trade.get("entry_notional_usdt")),
        "entry_client_order_id": closed_trade.get("entry_client_order_id"),
        "entry_exchange_order_id": closed_trade.get("entry_exchange_order_id"),
        "tp_price": _safe_float(closed_trade.get("tp_price")),
        "resolved_tp_price_source": closed_trade.get("resolved_tp_price_source"),
        "tp_client_order_id": closed_trade.get("tp_order_client_id"),
        "tp_exchange_order_id": closed_trade.get("tp_order_exchange_id"),
        "sl_trigger_price": _safe_float(closed_trade.get("sl_trigger_price")),
        "sl_client_order_id": closed_trade.get("sl_order_client_id"),
        "sl_exchange_order_id": closed_trade.get("sl_order_exchange_id"),
        "time_stop_client_order_id": closed_trade.get("time_stop_client_order_id"),
        "time_stop_exchange_order_id": closed_trade.get("time_stop_exchange_order_id"),
        "protective_flatten_client_order_id": closed_trade.get("protective_flatten_client_order_id")
        if closed_trade.get("exit_reason") == EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN
        else None,
        "protective_flatten_exchange_order_id": closed_trade.get("protective_flatten_exchange_order_id")
        if closed_trade.get("exit_reason") == EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN
        else None,
        "protective_flatten_exit_reason": closed_trade.get("protective_flatten_exit_reason")
        if closed_trade.get("exit_reason") == EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN
        else None,
        "exit_reason": closed_trade.get("exit_reason"),
        "exit_time_ms": exit_ts,
        "exit_time_bj": closed_trade.get("exit_detected_bar_bj") or current_time_bj,
        "exit_detected_bj": closed_trade.get("exit_bj"),
        "exit_price": exit_price,
        "exit_price_source": closed_trade.get("exit_price_source"),
        "exit_order_client_id": closed_trade.get("exit_order_client_id"),
        "exit_order_exchange_id": closed_trade.get("exit_order_exchange_id"),
        "exit_order_status": closed_trade.get("exit_order_status"),
        "pnl_pct": pnl_pct,
        "hold_mins": (exit_ts - entry_ts) / 60000.0 if entry_ts and exit_ts else None,
        "current_time_ms": current_time_ms,
        "current_time_bj": current_time_bj,
        "order_checks": dict(order_checks),
        "cleanup_checks": dict(cleanup_checks),
        "signal_snapshot": closed_trade.get("signal_snapshot"),
    }


def _append_live_trade_projection(
    *,
    account: str,
    cfg: Mapping[str, Any],
    closed_trade: Mapping[str, Any],
    source: str,
    current_time_ms: int,
    current_time_bj: str,
    order_checks: Mapping[str, Any],
    cleanup_checks: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        row = _live_trade_projection_row(
            account=account,
            cfg=cfg,
            closed_trade=closed_trade,
            source=source,
            current_time_ms=current_time_ms,
            current_time_bj=current_time_bj,
            order_checks=order_checks,
            cleanup_checks=cleanup_checks,
        )
        path = _projection_path(cfg, "live_trades")
        _append_projection_row(path, row)
        return {"ok": True, "path": str(path), "row": row}
    except Exception as exc:
        return {"ok": False, "reason": str(exc), "path": str(_projection_path(cfg, "live_trades"))}


def _recover_open_trade_from_pending(pending: Mapping[str, Any], position: Mapping[str, Any]) -> dict[str, Any]:
    signal_snapshot = pending.get("signal_snapshot") if isinstance(pending.get("signal_snapshot"), Mapping) else {}
    params = signal_snapshot.get("params") if isinstance(signal_snapshot.get("params"), Mapping) else {}
    return {
        "symbol": str(pending.get("symbol") or position.get("symbol") or "").upper().strip(),
        "strategy_name": str(pending.get("strategy_name") or DEFAULT_STRATEGY_NAME),
        "strategy_code": str(pending.get("strategy_code") or "").upper().strip(),
        "side": POSITION_SIDE_LONG,
        "order_root": pending.get("order_root"),
        "entry_client_order_id": pending.get("client_order_id"),
        "entry_exchange_order_id": pending.get("exchange_order_id"),
        "entry_ts": int(pending.get("signal_time") or 0),
        "entry_bj": pending.get("signal_time_bj"),
        "entry_price": float(position.get("entry_price") or pending.get("current_price") or 0.0),
        "entry_price_source": "position_entry_price" if float(position.get("entry_price") or 0.0) > 0 else pending.get("entry_fill_price_source"),
        "entry_qty": float(position.get("qty") or 0.0),
        "entry_notional_usdt": float(pending.get("entry_notional_usdt") or 0.0),
        "signal_digest": pending.get("signal_digest"),
        "signal_snapshot": dict(signal_snapshot),
        "tp_order_client_id": pending.get("tp_client_order_id"),
        "tp_order_exchange_id": None,
        "sl_order_client_id": pending.get("sl_client_order_id"),
        "sl_order_exchange_id": None,
        "time_stop_client_order_id": None,
        "time_stop_exchange_order_id": None,
        "tp_price": float(pending.get("tp_price") or signal_snapshot.get("tp_price") or 0.0),
        "sl_trigger_price": float(pending.get("sl_price") or signal_snapshot.get("sl_price") or 0.0),
        "max_hold_mins": int(pending.get("max_hold_mins") or params.get("max_hold_mins") or 0),
        "time_stop_min_profit_pct": float(pending.get("time_stop_min_profit_pct") or params.get("time_stop_min_profit_pct") or 0.0),
        "status": "OPEN",
        "exit_submit_inflight": False,
        "last_status_bj": _now_bj_str(),
        "time_stop_last_check_bj": None,
    }


def _pending_terminal_trade(pending: Mapping[str, Any], entry_order: Mapping[str, Any] | None) -> dict[str, Any]:
    signal_snapshot = pending.get("signal_snapshot") if isinstance(pending.get("signal_snapshot"), Mapping) else {}
    fill_res = resolve_order_fill_price(dict(entry_order or {}), fallback_price=float(pending.get("current_price") or 0.0) or None)
    fill_payload = dict(fill_res.get("data") or {}) if fill_res.get("ok") else {}
    return {
        "symbol": str(pending.get("symbol") or "").upper().strip(),
        "strategy_name": str(pending.get("strategy_name") or DEFAULT_STRATEGY_NAME),
        "strategy_code": str(pending.get("strategy_code") or "").upper().strip(),
        "side": POSITION_SIDE_LONG,
        "order_root": pending.get("order_root"),
        "entry_client_order_id": pending.get("client_order_id"),
        "entry_exchange_order_id": pending.get("exchange_order_id"),
        "entry_ts": int(pending.get("signal_time") or 0),
        "entry_bj": pending.get("signal_time_bj"),
        "entry_price": float(fill_payload.get("fill_price") or pending.get("current_price") or 0.0),
        "entry_price_source": str(fill_payload.get("price_source") or pending.get("entry_fill_price_source") or "pending_current_price"),
        "entry_qty": float((entry_order or {}).get("executed_qty") or (entry_order or {}).get("qty") or 0.0),
        "entry_notional_usdt": float(pending.get("entry_notional_usdt") or 0.0),
        "signal_digest": pending.get("signal_digest"),
        "signal_snapshot": dict(signal_snapshot),
        "tp_order_client_id": pending.get("tp_client_order_id"),
        "tp_order_exchange_id": None,
        "sl_order_client_id": pending.get("sl_client_order_id"),
        "sl_order_exchange_id": None,
        "time_stop_client_order_id": pending.get("time_stop_client_order_id"),
        "time_stop_exchange_order_id": None,
        "tp_price": float(pending.get("tp_price") or signal_snapshot.get("tp_price") or 0.0),
        "sl_trigger_price": float(pending.get("sl_price") or signal_snapshot.get("sl_price") or 0.0),
        "status": "CLOSED",
        "exit_submit_inflight": False,
        "last_status_bj": _now_bj_str(),
        "time_stop_last_check_bj": None,
    }


def _cancel_residual_exit_orders(
    *,
    account: str,
    symbol: str,
    open_trade: Mapping[str, Any],
    known_open_orders: list[dict[str, Any]],
    order_checks: Mapping[str, Any],
    retry_max: int,
    retry_delay_secs: float,
) -> dict[str, Any]:
    return {
        "tp": _cancel_order_if_present(
            account,
            symbol,
            known_open_orders=known_open_orders,
            exchange_order_id=open_trade.get("tp_order_exchange_id"),
            client_order_id=open_trade.get("tp_order_client_id"),
            prefetched_order_res=dict(order_checks.get("tp") or {}),
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
        ),
        "sl": _cancel_order_if_present(
            account,
            symbol,
            known_open_orders=known_open_orders,
            exchange_order_id=open_trade.get("sl_order_exchange_id"),
            client_order_id=open_trade.get("sl_order_client_id"),
            prefetched_order_res=dict(order_checks.get("sl") or {}),
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
        ),
        "time_stop": _cancel_order_if_present(
            account,
            symbol,
            known_open_orders=known_open_orders,
            exchange_order_id=open_trade.get("time_stop_exchange_order_id"),
            client_order_id=open_trade.get("time_stop_client_order_id"),
            prefetched_order_res=dict(order_checks.get("time_stop") or {}),
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
        ),
    }


def _cleanup_checks_ok(cleanup_checks: Mapping[str, Any]) -> bool:
    return all(bool((cleanup_checks.get(key) or {}).get("ok")) for key in ("tp", "sl", "time_stop"))


def _finalize_closed_trade(
    *,
    account: str,
    symbol: str,
    open_trade: Mapping[str, Any],
    exit_reason: str,
    order_checks: Mapping[str, Any],
    cleanup_checks: Mapping[str, Any],
    cfg: Mapping[str, Any],
    audit_enabled: bool,
    state_strategy_name: str,
    current_time_ms: int,
    current_time_bj: str,
    checked_bj: str,
    source: str,
) -> dict[str, Any]:
    exit_order = _exit_order_from_checks(exit_reason, order_checks)
    exit_event_time_ms = _extract_event_time_ms(exit_order) or current_time_ms
    exit_price, exit_price_source = _resolve_exit_price(exit_reason, open_trade, exit_order)
    closed_trade = deepcopy(dict(open_trade))
    closed_trade.update({
        "status": "CLOSED",
        "exit_reason": exit_reason,
        "exit_bj": checked_bj,
        "exit_detected_bar_ts": current_time_ms,
        "exit_detected_bar_bj": current_time_bj,
        "exit_price": exit_price,
        "exit_price_source": exit_price_source,
        "exit_order_client_id": (exit_order or {}).get("client_order_id"),
        "exit_order_exchange_id": (exit_order or {}).get("order_id"),
        "exit_order_status": (exit_order or {}).get("status"),
    })
    set_open_trade(account, symbol, None, strategy_name=state_strategy_name)
    set_pending_entry_order(account, symbol, None, strategy_name=state_strategy_name)
    cooldown_until_ts, cooldown_until_bj = _cooldown_until(current_time_ms, _require_int(cfg, "cooldown_mins", min_value=0))
    set_cooldown(account, symbol, cooldown_until_ts=cooldown_until_ts, cooldown_until_bj=cooldown_until_bj, strategy_name=state_strategy_name)
    mark_error(account, symbol, error_code=None, error_message=None, error_bj=None, strategy_name=state_strategy_name)
    projection_res = _append_live_trade_projection(
        account=account,
        cfg=cfg,
        closed_trade=closed_trade,
        source=source,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        order_checks=order_checks,
        cleanup_checks=cleanup_checks,
    )
    _write_exec_event(audit_enabled, account, "spring_position_closed_detected", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": open_trade.get("order_root"),
        "exit_reason": exit_reason,
        "exit_price": exit_price,
        "exit_price_source": exit_price_source,
        "order_checks": dict(order_checks),
        "cleanup_checks": dict(cleanup_checks),
        "live_trade_projection": projection_res,
    })
    if audit_enabled and not projection_res.get("ok"):
        _write_exec_event(True, account, "spring_live_trade_projection_write_failed", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "reason": projection_res.get("reason"),
            "projection_path": projection_res.get("path"),
        })
    if exit_reason == EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN:
        _write_exec_event(audit_enabled, account, "spring_sl_submit_failed_flatten_filled", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "exit_reason": exit_reason,
            "protective_flatten_client_order_id": open_trade.get("protective_flatten_client_order_id") or open_trade.get("time_stop_client_order_id"),
            "protective_flatten_exchange_order_id": open_trade.get("protective_flatten_exchange_order_id") or open_trade.get("time_stop_exchange_order_id"),
            "exit_price": exit_price,
            "exit_price_source": exit_price_source,
            "order_checks": dict(order_checks),
        })
    _write_exec_event(audit_enabled, account, "spring_state_cleared_after_exit", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": open_trade.get("order_root"),
        "exit_reason": exit_reason,
        "cooldown_until_ts": cooldown_until_ts,
        "cooldown_until_bj": cooldown_until_bj,
    })
    if _notify_enabled(cfg, "notify_on_exit_detected"):
        _send_spring_notify(
            _spring_exit_message(
                account,
                closed_trade,
                exit_reason=exit_reason,
                event_time_ms=exit_event_time_ms,
            ),
            strategy_name=state_strategy_name,
        )
    return {
        "ok": True,
        "outcome": "flat_state_cleared",
        "exit_reason": exit_reason,
        "closed_trade": closed_trade,
        "order_checks": dict(order_checks),
        "cleanup_checks": dict(cleanup_checks),
        "live_trade_projection": projection_res,
        "cooldown_until_ts": cooldown_until_ts,
        "cooldown_until_bj": cooldown_until_bj,
        "checked_bj": checked_bj,
    }


def _time_stop_client_order_id(open_trade: Mapping[str, Any]) -> str:
    existing = str(open_trade.get("time_stop_client_order_id") or "").strip()
    if existing:
        return existing
    strategy_code = str(open_trade.get("strategy_code") or "").upper().strip()
    order_root = str(open_trade.get("order_root") or "").strip()
    if not strategy_code:
        raise ValueError("open_trade.strategy_code is required for time-stop client order id")
    if not order_root:
        raise ValueError("open_trade.order_root is required for time-stop client order id")
    return build_client_order_id(broker_id=BROKER_ID, strat=strategy_code, leg="TS", root=order_root)


def _bracket_client_order_id(open_trade: Mapping[str, Any], leg: str) -> str:
    leg_key = str(leg).upper().strip()
    existing_field = "tp_order_client_id" if leg_key == "TP" else "sl_order_client_id" if leg_key == "SL" else ""
    if not existing_field:
        raise ValueError(f"unsupported bracket leg: {leg}")
    existing = str(open_trade.get(existing_field) or "").strip()
    if existing:
        return existing
    strategy_code = str(open_trade.get("strategy_code") or "").upper().strip()
    order_root = str(open_trade.get("order_root") or "").strip()
    if not strategy_code:
        raise ValueError("open_trade.strategy_code is required for bracket client order id")
    if not order_root:
        raise ValueError("open_trade.order_root is required for bracket client order id")
    return build_client_order_id(broker_id=BROKER_ID, strat=strategy_code, leg=leg_key, root=order_root)


def _verify_open_trade_brackets(
    open_trade: Mapping[str, Any],
    open_orders: list[dict[str, Any]],
) -> dict[str, Any]:
    tp_row = _find_open_order(
        open_orders,
        exchange_order_id=open_trade.get("tp_order_exchange_id"),
        client_order_id=open_trade.get("tp_order_client_id"),
    )
    sl_row = _find_open_order(
        open_orders,
        exchange_order_id=open_trade.get("sl_order_exchange_id"),
        client_order_id=open_trade.get("sl_order_client_id"),
    )
    return {
        "tp_bound": tp_row is not None,
        "sl_bound": sl_row is not None,
        "tp_row": tp_row,
        "sl_row": sl_row,
    }


def _verify_or_repair_open_trade_brackets(
    *,
    account: str,
    symbol: str,
    open_trade: dict[str, Any],
    position: Mapping[str, Any],
    open_orders: list[dict[str, Any]],
    cfg: Mapping[str, Any],
    audit_enabled: bool,
    state_strategy_name: str,
    current_time_ms: int,
    current_time_bj: str,
    checked_bj: str,
    source: str,
) -> dict[str, Any]:
    initial_verify = _verify_open_trade_brackets(open_trade, open_orders)
    if initial_verify["tp_bound"] and initial_verify["sl_bound"]:
        changed = False
        tp_row = initial_verify.get("tp_row")
        if isinstance(tp_row, Mapping):
            if open_trade.get("tp_order_exchange_id") != tp_row.get("order_id") or open_trade.get("tp_order_client_id") != tp_row.get("client_order_id"):
                open_trade["tp_order_exchange_id"] = tp_row.get("order_id")
                open_trade["tp_order_client_id"] = tp_row.get("client_order_id")
                changed = True
        sl_row = initial_verify.get("sl_row")
        if isinstance(sl_row, Mapping):
            if open_trade.get("sl_order_exchange_id") != sl_row.get("order_id") or open_trade.get("sl_order_client_id") != sl_row.get("client_order_id"):
                open_trade["sl_order_exchange_id"] = sl_row.get("order_id")
                open_trade["sl_order_client_id"] = sl_row.get("client_order_id")
                changed = True
        if changed:
            open_trade["last_status_bj"] = current_time_bj
            set_open_trade(account, symbol, open_trade, strategy_name=state_strategy_name)
        return {
            "ok": True,
            "outcome": "bracket_verified",
            "open_trade": open_trade,
            "open_orders": open_orders,
            "verify": initial_verify,
            "checked_bj": checked_bj,
        }

    qty = float(position.get("qty") or open_trade.get("entry_qty") or 0.0)
    if qty <= 0:
        reason = f"position qty must be positive for bracket repair, got {qty}"
        mark_error(
            account,
            symbol,
            error_code="open_trade_bracket_qty_invalid",
            error_message=reason,
            error_bj=checked_bj,
            strategy_name=state_strategy_name,
        )
        _write_exec_event(audit_enabled, account, "spring_open_trade_bracket_repair_blocked", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "reason": reason,
            "verify": initial_verify,
        })
        return {"ok": False, "outcome": "open_trade_bracket_qty_invalid", "reason": reason, "verify": initial_verify, "checked_bj": checked_bj}

    retry_max = _require_int(cfg, "order_retry_max", min_value=0)
    retry_delay_secs = _require_float(cfg, "api_retry_delay_secs", min_value=0.0)
    repaired = False
    repair_results: dict[str, Any] = {}

    if not initial_verify["tp_bound"]:
        tp_price = float(open_trade.get("tp_price") or 0.0)
        if tp_price <= 0:
            reason = f"tp_price must be positive for bracket repair, got {tp_price}"
            mark_error(account, symbol, error_code="tp_recreate_payload_invalid", error_message=reason, error_bj=checked_bj, strategy_name=state_strategy_name)
            _write_exec_event(audit_enabled, account, "spring_tp_recreate_payload_invalid", {
                "symbol": symbol,
                "source": source,
                "bar_ts": current_time_ms,
                "bar_bj": current_time_bj,
                "order_root": open_trade.get("order_root"),
                "reason": reason,
                "verify": initial_verify,
            })
            return {"ok": False, "outcome": "tp_recreate_payload_invalid", "reason": reason, "verify": initial_verify, "checked_bj": checked_bj}
        tp_client_order_id = _bracket_client_order_id(open_trade, "TP")
        tp_res = place_tp_order(
            account,
            symbol,
            POSITION_SIDE_LONG,
            qty,
            tp_price,
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
            client_order_id=tp_client_order_id,
            notify_label=SPRING_NOTIFY_LABEL,
        )
        repair_results["tp"] = tp_res
        if not tp_res.get("ok"):
            reason = str(tp_res.get("reason") or "tp_recreate_failed")
            mark_error(account, symbol, error_code="tp_recreate_failed", error_message=reason, error_bj=checked_bj, strategy_name=state_strategy_name)
            _write_exec_event(audit_enabled, account, "spring_tp_recreate_failed", {
                "symbol": symbol,
                "source": source,
                "bar_ts": current_time_ms,
                "bar_bj": current_time_bj,
                "order_root": open_trade.get("order_root"),
                "exchange_snapshot": tp_res,
                "verify": initial_verify,
            })
            return {"ok": False, "outcome": "tp_recreate_failed", "reason": reason, "repair_results": repair_results, "verify": initial_verify, "checked_bj": checked_bj}
        tp_data = dict(tp_res.get("data") or {})
        open_trade["tp_order_client_id"] = tp_data.get("client_order_id", tp_client_order_id)
        open_trade["tp_order_exchange_id"] = tp_data.get("exchange_order_id")
        repaired = True
        _write_exec_event(audit_enabled, account, "spring_tp_recreated", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "tp_client_order_id": open_trade.get("tp_order_client_id"),
            "exchange_snapshot": tp_res,
        })

    if not initial_verify["sl_bound"]:
        sl_trigger_price = float(open_trade.get("sl_trigger_price") or 0.0)
        if sl_trigger_price <= 0:
            reason = f"sl_trigger_price must be positive for bracket repair, got {sl_trigger_price}"
            mark_error(account, symbol, error_code="sl_recreate_payload_invalid", error_message=reason, error_bj=checked_bj, strategy_name=state_strategy_name)
            _write_exec_event(audit_enabled, account, "spring_sl_recreate_payload_invalid", {
                "symbol": symbol,
                "source": source,
                "bar_ts": current_time_ms,
                "bar_bj": current_time_bj,
                "order_root": open_trade.get("order_root"),
                "reason": reason,
                "verify": initial_verify,
            })
            if repaired:
                open_trade["last_status_bj"] = current_time_bj
                set_open_trade(account, symbol, open_trade, strategy_name=state_strategy_name)
            return {"ok": False, "outcome": "sl_recreate_payload_invalid", "reason": reason, "repair_results": repair_results, "verify": initial_verify, "checked_bj": checked_bj}
        sl_client_order_id = _bracket_client_order_id(open_trade, "SL")
        sl_res = place_sl_order(
            account,
            symbol,
            POSITION_SIDE_LONG,
            sl_trigger_price,
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
            client_order_id=sl_client_order_id,
            notify_label=SPRING_NOTIFY_LABEL,
        )
        repair_results["sl"] = sl_res
        if not sl_res.get("ok"):
            reason = str(sl_res.get("reason") or "sl_recreate_failed")
            mark_error(account, symbol, error_code="sl_recreate_failed", error_message=reason, error_bj=checked_bj, strategy_name=state_strategy_name)
            _write_exec_event(audit_enabled, account, "spring_sl_recreate_failed", {
                "symbol": symbol,
                "source": source,
                "bar_ts": current_time_ms,
                "bar_bj": current_time_bj,
                "order_root": open_trade.get("order_root"),
                "exchange_snapshot": sl_res,
                "verify": initial_verify,
            })
            if repaired:
                open_trade["last_status_bj"] = current_time_bj
                set_open_trade(account, symbol, open_trade, strategy_name=state_strategy_name)
            return {"ok": False, "outcome": "sl_recreate_failed", "reason": reason, "repair_results": repair_results, "verify": initial_verify, "checked_bj": checked_bj}
        sl_data = dict(sl_res.get("data") or {})
        open_trade["sl_order_client_id"] = sl_data.get("client_order_id", sl_client_order_id)
        open_trade["sl_order_exchange_id"] = sl_data.get("exchange_order_id")
        repaired = True
        _write_exec_event(audit_enabled, account, "spring_sl_recreated", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "sl_client_order_id": open_trade.get("sl_order_client_id"),
            "exchange_snapshot": sl_res,
        })

    if repaired:
        open_trade["last_status_bj"] = current_time_bj
        set_open_trade(account, symbol, open_trade, strategy_name=state_strategy_name)

    verify_orders_res = get_open_orders(account, symbol)
    if not verify_orders_res.get("ok"):
        reason = str(verify_orders_res.get("reason") or "bracket_repair_verify_query_failed")
        mark_error(account, symbol, error_code="open_trade_bracket_verify_query_failed", error_message=reason, error_bj=checked_bj, strategy_name=state_strategy_name)
        _write_exec_event(audit_enabled, account, "spring_open_trade_bracket_verify_query_failed", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "exchange_snapshot": verify_orders_res,
            "repair_results": repair_results,
        })
        return {"ok": False, "outcome": "open_trade_bracket_verify_query_failed", "reason": reason, "repair_results": repair_results, "checked_bj": checked_bj}

    verified_open_orders = [dict(row) for row in (verify_orders_res.get("data") or []) if isinstance(row, Mapping)]
    final_verify = _verify_open_trade_brackets(open_trade, verified_open_orders)
    if not (final_verify["tp_bound"] and final_verify["sl_bound"]):
        reason = f"tp_bound={final_verify['tp_bound']}, sl_bound={final_verify['sl_bound']}"
        mark_error(account, symbol, error_code="open_trade_bracket_incomplete", error_message=reason, error_bj=checked_bj, strategy_name=state_strategy_name)
        _write_exec_event(audit_enabled, account, "spring_open_trade_bracket_incomplete", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "reason": reason,
            "initial_verify": initial_verify,
            "final_verify": final_verify,
            "repair_results": repair_results,
            "open_orders_snapshot": verify_orders_res,
        })
        return {"ok": False, "outcome": "open_trade_bracket_incomplete", "reason": reason, "repair_results": repair_results, "verify": final_verify, "checked_bj": checked_bj}

    _write_exec_event(audit_enabled, account, "spring_open_trade_bracket_verified_after_repair", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": open_trade.get("order_root"),
        "initial_verify": initial_verify,
        "final_verify": final_verify,
        "repair_results": repair_results,
    })
    return {
        "ok": True,
        "outcome": "bracket_repaired",
        "open_trade": open_trade,
        "open_orders": verified_open_orders,
        "initial_verify": initial_verify,
        "verify": final_verify,
        "repair_results": repair_results,
        "checked_bj": checked_bj,
    }


def _repair_brackets_after_time_stop_issue(
    *,
    account: str,
    symbol: str,
    open_trade: dict[str, Any],
    cfg: Mapping[str, Any],
    audit_enabled: bool,
    state_strategy_name: str,
    current_time_ms: int,
    current_time_bj: str,
    checked_bj: str,
    source: str,
    event_prefix: str,
) -> dict[str, Any]:
    position_res = get_position(account, symbol, POSITION_SIDE_LONG)
    orders_res = get_open_orders(account, symbol)
    _write_exec_event(audit_enabled, account, f"spring_{event_prefix}_repair_attempted", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": open_trade.get("order_root"),
        "exchange_snapshot": {
            "position": position_res,
            "orders": orders_res,
        },
    })
    if not position_res.get("ok") or not orders_res.get("ok"):
        reason = str(orders_res.get("reason") or position_res.get("reason") or f"{event_prefix}_repair_query_failed")
        mark_error(
            account,
            symbol,
            error_code=f"{event_prefix}_repair_query_failed",
            error_message=reason,
            error_bj=checked_bj,
            strategy_name=state_strategy_name,
        )
        _write_exec_event(audit_enabled, account, f"spring_{event_prefix}_repair_query_failed", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "reason": reason,
            "exchange_snapshot": {
                "position": position_res,
                "orders": orders_res,
            },
        })
        set_open_trade(account, symbol, open_trade, strategy_name=state_strategy_name)
        return {"ok": False, "outcome": f"{event_prefix}_repair_query_failed", "reason": reason, "checked_bj": checked_bj}
    position = position_res.get("data")
    if not position:
        set_open_trade(account, symbol, open_trade, strategy_name=state_strategy_name)
        return {
            "ok": True,
            "outcome": f"{event_prefix}_repair_skipped_position_flat",
            "position_snapshot": position_res,
            "orders_snapshot": orders_res,
            "checked_bj": checked_bj,
        }
    repair_res = _verify_or_repair_open_trade_brackets(
        account=account,
        symbol=symbol,
        open_trade=open_trade,
        position=dict(position),
        open_orders=[dict(row) for row in (orders_res.get("data") or []) if isinstance(row, Mapping)],
        cfg=cfg,
        audit_enabled=audit_enabled,
        state_strategy_name=state_strategy_name,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        checked_bj=checked_bj,
        source=f"{source}_{event_prefix}_repair",
    )
    repair_res = dict(repair_res)
    repair_res["outcome"] = f"{event_prefix}_{repair_res.get('outcome')}"
    return repair_res


def _maybe_submit_time_stop(
    *,
    account: str,
    symbol: str,
    open_trade: dict[str, Any],
    position: Mapping[str, Any],
    open_orders: list[dict[str, Any]],
    latest_close: Any,
    cfg: Mapping[str, Any],
    audit_enabled: bool,
    state_strategy_name: str,
    current_time_ms: int,
    current_time_bj: str,
    checked_bj: str,
    source: str,
) -> dict[str, Any]:
    if bool(open_trade.get("exit_submit_inflight")):
        ts_exchange_order_id = open_trade.get("time_stop_exchange_order_id")
        ts_client_order_id = open_trade.get("time_stop_client_order_id")
        if ts_exchange_order_id is None and not ts_client_order_id:
            open_trade["exit_submit_inflight"] = False
            open_trade["status"] = "OPEN"
            open_trade["last_status_bj"] = current_time_bj
            mark_error(
                account,
                symbol,
                error_code="time_stop_inflight_missing_identity",
                error_message="missing time-stop order identity while exit_submit_inflight=true",
                error_bj=checked_bj,
                strategy_name=state_strategy_name,
            )
            repair_res = _repair_brackets_after_time_stop_issue(
                account=account,
                symbol=symbol,
                open_trade=open_trade,
                cfg=cfg,
                audit_enabled=audit_enabled,
                state_strategy_name=state_strategy_name,
                current_time_ms=current_time_ms,
                current_time_bj=current_time_bj,
                checked_bj=checked_bj,
                source=source,
                event_prefix="time_stop_inflight_missing_identity",
            )
            return {"ok": False, "outcome": "time_stop_inflight_missing_identity", "repair": repair_res, "checked_bj": checked_bj}

        matched_ts_open_order = _find_open_order(
            open_orders,
            exchange_order_id=ts_exchange_order_id,
            client_order_id=ts_client_order_id,
        )
        if matched_ts_open_order is not None:
            _write_exec_event(audit_enabled, account, "spring_time_stop_inflight_waiting", {
                "symbol": symbol,
                "source": source,
                "bar_ts": current_time_ms,
                "bar_bj": current_time_bj,
                "order_root": open_trade.get("order_root"),
                "time_stop_client_order_id": ts_client_order_id,
                "exchange_snapshot": {
                    "known_open_order": matched_ts_open_order,
                    "known_open_orders_count": len(open_orders),
                },
            })
            return {
                "ok": True,
                "outcome": "time_stop_inflight_waiting",
                "position_snapshot": dict(position),
                "open_orders_count": len(open_orders),
                "checked_bj": checked_bj,
            }

        retry_max = _require_int(cfg, "order_retry_max", min_value=0)
        retry_delay_secs = _require_float(cfg, "api_retry_delay_secs", min_value=0.0)
        ts_order_res = _resolve_leg_order(
            account,
            symbol,
            known_open_orders=open_orders,
            exchange_order_id=ts_exchange_order_id,
            client_order_id=ts_client_order_id,
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
        )
        if not ts_order_res.get("ok"):
            mark_error(
                account,
                symbol,
                error_code="time_stop_inflight_query_error",
                error_message=ts_order_res.get("reason"),
                error_bj=checked_bj,
                strategy_name=state_strategy_name,
            )
            _write_exec_event(audit_enabled, account, "spring_time_stop_inflight_query_error", {
                "symbol": symbol,
                "source": source,
                "bar_ts": current_time_ms,
                "bar_bj": current_time_bj,
                "order_root": open_trade.get("order_root"),
                "time_stop_client_order_id": ts_client_order_id,
                "exchange_snapshot": ts_order_res,
            })
            return {"ok": False, "outcome": "time_stop_inflight_query_error", "time_stop_res": ts_order_res, "checked_bj": checked_bj}
        ts_order = dict(ts_order_res.get("data") or {})
        if not ts_order:
            open_trade["exit_submit_inflight"] = False
            open_trade["status"] = "OPEN"
            open_trade["last_status_bj"] = current_time_bj
            mark_error(
                account,
                symbol,
                error_code="time_stop_inflight_missing_on_exchange",
                error_message="time-stop order missing on exchange while position still open",
                error_bj=checked_bj,
                strategy_name=state_strategy_name,
            )
            _write_exec_event(audit_enabled, account, "spring_time_stop_inflight_missing_on_exchange", {
                "symbol": symbol,
                "source": source,
                "bar_ts": current_time_ms,
                "bar_bj": current_time_bj,
                "order_root": open_trade.get("order_root"),
                "time_stop_client_order_id": ts_client_order_id,
                "exchange_snapshot": ts_order_res,
            })
            repair_res = _repair_brackets_after_time_stop_issue(
                account=account,
                symbol=symbol,
                open_trade=open_trade,
                cfg=cfg,
                audit_enabled=audit_enabled,
                state_strategy_name=state_strategy_name,
                current_time_ms=current_time_ms,
                current_time_bj=current_time_bj,
                checked_bj=checked_bj,
                source=source,
                event_prefix="time_stop_inflight_missing_on_exchange",
            )
            return {"ok": bool(repair_res.get("ok")), "outcome": "time_stop_inflight_missing_on_exchange_reset", "time_stop_res": ts_order_res, "repair": repair_res, "checked_bj": checked_bj}
        ts_status = str(ts_order.get("status") or "").upper()
        if ts_status in TERMINAL_ORDER_STATUSES:
            open_trade["exit_submit_inflight"] = False
            open_trade["status"] = "OPEN"
            open_trade["last_status_bj"] = current_time_bj
            error_code = "time_stop_filled_but_position_still_open" if ts_status in FILLED_ORDER_STATUSES else "time_stop_terminal_but_position_open"
            mark_error(
                account,
                symbol,
                error_code=error_code,
                error_message=f"time-stop status={ts_status} while position still open during inflight reconcile",
                error_bj=checked_bj,
                strategy_name=state_strategy_name,
            )
            _write_exec_event(audit_enabled, account, f"spring_{error_code}", {
                "symbol": symbol,
                "source": source,
                "bar_ts": current_time_ms,
                "bar_bj": current_time_bj,
                "order_root": open_trade.get("order_root"),
                "time_stop_client_order_id": ts_client_order_id,
                "exchange_snapshot": ts_order_res,
            })
            repair_res = _repair_brackets_after_time_stop_issue(
                account=account,
                symbol=symbol,
                open_trade=open_trade,
                cfg=cfg,
                audit_enabled=audit_enabled,
                state_strategy_name=state_strategy_name,
                current_time_ms=current_time_ms,
                current_time_bj=current_time_bj,
                checked_bj=checked_bj,
                source=source,
                event_prefix="time_stop_inflight_reset",
            )
            return {"ok": bool(repair_res.get("ok")), "outcome": f"{error_code}_reset", "time_stop_res": ts_order_res, "repair": repair_res, "checked_bj": checked_bj}

        _write_exec_event(audit_enabled, account, "spring_time_stop_inflight_waiting", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "time_stop_client_order_id": ts_client_order_id,
            "exchange_snapshot": ts_order_res,
        })
        return {
            "ok": True,
            "outcome": "time_stop_inflight_waiting",
            "position_snapshot": dict(position),
            "open_orders_count": len(open_orders),
            "checked_bj": checked_bj,
        }

    entry_ts = int(open_trade.get("entry_ts") or 0)
    entry_price = float(open_trade.get("entry_price") or 0.0)
    max_hold_mins = int(open_trade.get("max_hold_mins") or 0)
    min_profit_pct = float(open_trade.get("time_stop_min_profit_pct") or 0.0)
    if entry_ts <= 0 or entry_price <= 0 or max_hold_mins <= 0:
        reason = f"entry_ts={entry_ts}, entry_price={entry_price}, max_hold_mins={max_hold_mins}"
        mark_error(
            account,
            symbol,
            error_code="time_stop_payload_invalid",
            error_message=reason,
            error_bj=checked_bj,
            strategy_name=state_strategy_name,
        )
        _write_exec_event(audit_enabled, account, "spring_time_stop_payload_invalid", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "reason": reason,
        })
        return {"ok": False, "outcome": "time_stop_payload_invalid", "reason": reason, "checked_bj": checked_bj}

    held_mins = int((int(current_time_ms) - entry_ts) / 60000)
    if held_mins < max_hold_mins:
        return {
            "ok": True,
            "outcome": "position_still_open",
            "position_snapshot": dict(position),
            "open_orders_count": len(open_orders),
            "held_mins": held_mins,
            "max_hold_mins": max_hold_mins,
            "checked_bj": checked_bj,
        }

    try:
        latest_close_f = float(latest_close)
    except (TypeError, ValueError):
        latest_close_f = 0.0
    if latest_close_f <= 0:
        reason = "latest_close_missing_or_nonpositive"
        mark_error(
            account,
            symbol,
            error_code="time_stop_latest_close_missing",
            error_message=reason,
            error_bj=checked_bj,
            strategy_name=state_strategy_name,
        )
        _write_exec_event(audit_enabled, account, "spring_time_stop_latest_close_missing", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "held_mins": held_mins,
            "max_hold_mins": max_hold_mins,
        })
        return {"ok": False, "outcome": "time_stop_latest_close_missing", "reason": reason, "checked_bj": checked_bj}

    current_profit_pct = latest_close_f / entry_price - 1.0
    open_trade["time_stop_last_check_bj"] = current_time_bj
    open_trade["time_stop_last_check_bar_ts"] = int(current_time_ms)
    open_trade["time_stop_last_close"] = latest_close_f
    open_trade["time_stop_last_profit_pct"] = current_profit_pct
    if current_profit_pct >= min_profit_pct:
        set_open_trade(account, symbol, open_trade, strategy_name=state_strategy_name)
        _write_exec_event(audit_enabled, account, "spring_time_stop_skipped_profit_ok", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "held_mins": held_mins,
            "current_profit_pct": current_profit_pct,
            "min_profit_pct": min_profit_pct,
        })
        return {
            "ok": True,
            "outcome": "position_still_open_time_stop_profit_ok",
            "position_snapshot": dict(position),
            "open_orders_count": len(open_orders),
            "held_mins": held_mins,
            "current_profit_pct": current_profit_pct,
            "min_profit_pct": min_profit_pct,
            "checked_bj": checked_bj,
        }

    retry_max = _require_int(cfg, "order_retry_max", min_value=0)
    retry_delay_secs = _require_float(cfg, "api_retry_delay_secs", min_value=0.0)
    _write_exec_event(audit_enabled, account, "spring_time_stop_triggered", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": open_trade.get("order_root"),
        "held_mins": held_mins,
        "current_profit_pct": current_profit_pct,
        "min_profit_pct": min_profit_pct,
    })

    tp_check = _resolve_leg_order(
        account,
        symbol,
        known_open_orders=open_orders,
        exchange_order_id=open_trade.get("tp_order_exchange_id"),
        client_order_id=open_trade.get("tp_order_client_id"),
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    sl_check = _resolve_leg_order(
        account,
        symbol,
        known_open_orders=open_orders,
        exchange_order_id=open_trade.get("sl_order_exchange_id"),
        client_order_id=open_trade.get("sl_order_client_id"),
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    tp_cancel = _cancel_order_if_present(
        account,
        symbol,
        known_open_orders=open_orders,
        exchange_order_id=open_trade.get("tp_order_exchange_id"),
        client_order_id=open_trade.get("tp_order_client_id"),
        prefetched_order_res=tp_check,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    sl_cancel = _cancel_order_if_present(
        account,
        symbol,
        known_open_orders=open_orders,
        exchange_order_id=open_trade.get("sl_order_exchange_id"),
        client_order_id=open_trade.get("sl_order_client_id"),
        prefetched_order_res=sl_check,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    _write_exec_event(audit_enabled, account, "spring_time_stop_cancel_tp_ok" if tp_cancel.get("ok") else "spring_time_stop_cancel_tp_failed", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": open_trade.get("order_root"),
        "exchange_snapshot": tp_cancel,
    })
    _write_exec_event(audit_enabled, account, "spring_time_stop_cancel_sl_ok" if sl_cancel.get("ok") else "spring_time_stop_cancel_sl_failed", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": open_trade.get("order_root"),
        "exchange_snapshot": sl_cancel,
    })

    tp_status = str(((tp_cancel.get("data") or {}) if tp_cancel.get("ok") else {}).get("status") or "").upper()
    sl_status = str(((sl_cancel.get("data") or {}) if sl_cancel.get("ok") else {}).get("status") or "").upper()
    if tp_status in FILLED_ORDER_STATUSES or sl_status in FILLED_ORDER_STATUSES:
        reason = f"tp_status={tp_status or 'NA'}, sl_status={sl_status or 'NA'}"
        mark_error(
            account,
            symbol,
            error_code="time_stop_pre_submit_exit_already_filled",
            error_message=reason,
            error_bj=checked_bj,
            strategy_name=state_strategy_name,
        )
        _write_exec_event(audit_enabled, account, "spring_time_stop_pre_submit_exit_already_filled", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "exchange_snapshot": {"tp_cancel": tp_cancel, "sl_cancel": sl_cancel},
        })
        return {"ok": False, "outcome": "time_stop_pre_submit_exit_already_filled", "reason": reason, "checked_bj": checked_bj}

    if not tp_cancel.get("ok") or not sl_cancel.get("ok"):
        reason = str(tp_cancel.get("reason") or sl_cancel.get("reason") or "time_stop_pre_submit_cancel_failed")
        mark_error(
            account,
            symbol,
            error_code="time_stop_pre_submit_cancel_failed",
            error_message=reason,
            error_bj=checked_bj,
            strategy_name=state_strategy_name,
        )
        _write_exec_event(audit_enabled, account, "spring_time_stop_pre_submit_cancel_failed", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "exchange_snapshot": {"tp_cancel": tp_cancel, "sl_cancel": sl_cancel},
        })
        return {"ok": False, "outcome": "time_stop_pre_submit_cancel_failed", "reason": reason, "checked_bj": checked_bj}

    qty = float(position.get("qty") or open_trade.get("entry_qty") or 0.0)
    if qty <= 0:
        reason = f"position qty must be positive for time-stop, got {qty}"
        mark_error(
            account,
            symbol,
            error_code="time_stop_qty_invalid",
            error_message=reason,
            error_bj=checked_bj,
            strategy_name=state_strategy_name,
        )
        return {"ok": False, "outcome": "time_stop_qty_invalid", "reason": reason, "checked_bj": checked_bj}

    ts_client_order_id = _time_stop_client_order_id(open_trade)
    ts_res = place_time_stop_order(
        account,
        symbol,
        POSITION_SIDE_LONG,
        qty,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
        client_order_id=ts_client_order_id,
        notify_label=SPRING_NOTIFY_LABEL,
    )
    if not ts_res.get("ok"):
        reason = str(ts_res.get("reason") or "time_stop_submit_failed")
        mark_error(
            account,
            symbol,
            error_code="time_stop_submit_failed",
            error_message=reason,
            error_bj=checked_bj,
            strategy_name=state_strategy_name,
        )
        _write_exec_event(audit_enabled, account, "spring_time_stop_submit_failed", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "exchange_snapshot": ts_res,
        })
        repair_res = _repair_brackets_after_time_stop_issue(
            account=account,
            symbol=symbol,
            open_trade=open_trade,
            cfg=cfg,
            audit_enabled=audit_enabled,
            state_strategy_name=state_strategy_name,
            current_time_ms=current_time_ms,
            current_time_bj=current_time_bj,
            checked_bj=checked_bj,
            source=source,
            event_prefix="time_stop_submit_failed",
        )
        return {"ok": False, "outcome": "time_stop_submit_failed", "reason": reason, "time_stop_res": ts_res, "repair": repair_res, "checked_bj": checked_bj}

    ts_data = dict(ts_res.get("data") or {})
    open_trade["time_stop_client_order_id"] = ts_data.get("client_order_id", ts_client_order_id)
    open_trade["time_stop_exchange_order_id"] = ts_data.get("exchange_order_id")
    open_trade["exit_submit_inflight"] = True
    open_trade["status"] = "EXIT_SUBMITTED"
    open_trade["last_status_bj"] = current_time_bj
    set_open_trade(account, symbol, open_trade, strategy_name=state_strategy_name)
    _write_exec_event(audit_enabled, account, "spring_time_stop_submitted", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": open_trade.get("order_root"),
        "held_mins": held_mins,
        "current_profit_pct": current_profit_pct,
        "time_stop_client_order_id": open_trade.get("time_stop_client_order_id"),
        "exchange_snapshot": ts_res,
    })
    return {
        "ok": True,
        "outcome": "time_stop_submitted",
        "held_mins": held_mins,
        "current_profit_pct": current_profit_pct,
        "min_profit_pct": min_profit_pct,
        "time_stop_res": ts_res,
        "checked_bj": checked_bj,
    }


def _post_entry_reconcile(
    *,
    account: str,
    symbol: str,
    open_trade: dict[str, Any],
    cfg: Mapping[str, Any],
    audit_enabled: bool,
    state_strategy_name: str,
    current_time_ms: int,
    current_time_bj: str,
    source: str,
    active_time_stop: bool = False,
    latest_close: Any = None,
) -> dict[str, Any]:
    checked_bj = _now_bj_str()
    retry_max = _require_int(cfg, "order_retry_max", min_value=0)
    retry_delay_secs = _require_float(cfg, "api_retry_delay_secs", min_value=0.0)
    position_res = get_position(account, symbol, POSITION_SIDE_LONG)
    open_orders_res = get_open_orders(account, symbol)
    mark_position_reconcile(account, symbol, reconcile_bj=checked_bj, strategy_name=state_strategy_name)
    mark_order_reconcile(account, symbol, reconcile_bj=checked_bj, strategy_name=state_strategy_name)
    if not position_res.get("ok"):
        _write_exec_event(audit_enabled, account, "spring_post_entry_reconcile_blocked", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "reason": "position_query_failed",
            "position_snapshot": position_res,
        })
        return {"ok": False, "outcome": "post_entry_reconcile_blocked", "reason": "position_query_failed", "position_res": position_res}
    if not open_orders_res.get("ok"):
        _write_exec_event(audit_enabled, account, "spring_post_entry_reconcile_blocked", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "reason": "open_orders_query_failed",
            "orders_snapshot": open_orders_res,
        })
        return {"ok": False, "outcome": "post_entry_reconcile_blocked", "reason": "open_orders_query_failed", "orders_res": open_orders_res}

    position = position_res.get("data")
    open_orders = [dict(row) for row in (open_orders_res.get("data") or []) if isinstance(row, Mapping)]
    if position:
        bracket_reconcile = None
        if not bool(open_trade.get("exit_submit_inflight")):
            bracket_reconcile = _verify_or_repair_open_trade_brackets(
                account=account,
                symbol=symbol,
                open_trade=open_trade,
                position=dict(position),
                open_orders=open_orders,
                cfg=cfg,
                audit_enabled=audit_enabled,
                state_strategy_name=state_strategy_name,
                current_time_ms=current_time_ms,
                current_time_bj=current_time_bj,
                checked_bj=checked_bj,
                source=source,
            )
            if not bracket_reconcile.get("ok"):
                return bracket_reconcile
            open_trade = dict(bracket_reconcile.get("open_trade") or open_trade)
            open_orders = [dict(row) for row in (bracket_reconcile.get("open_orders") or open_orders) if isinstance(row, Mapping)]
        if active_time_stop:
            time_stop_res = _maybe_submit_time_stop(
                account=account,
                symbol=symbol,
                open_trade=open_trade,
                position=dict(position),
                open_orders=open_orders,
                latest_close=latest_close,
                cfg=cfg,
                audit_enabled=audit_enabled,
                state_strategy_name=state_strategy_name,
                current_time_ms=current_time_ms,
                current_time_bj=current_time_bj,
                checked_bj=checked_bj,
                source=source,
            )
            if bracket_reconcile is not None:
                time_stop_res = dict(time_stop_res)
                time_stop_res["bracket_reconcile"] = bracket_reconcile
            return time_stop_res
        return {
            "ok": True,
            "outcome": "position_still_open",
            "position_snapshot": position_res,
            "open_orders_count": len(open_orders),
            "bracket_reconcile": bracket_reconcile,
            "checked_bj": checked_bj,
        }
    exit_reason, order_checks, blocking_reason = _infer_exit_reason_from_orders(
        account,
        symbol,
        open_trade,
        known_open_orders=open_orders,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    cleanup_checks = _cancel_residual_exit_orders(
        account=account,
        symbol=symbol,
        open_trade=open_trade,
        known_open_orders=open_orders,
        order_checks=order_checks,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if blocking_reason or not _cleanup_checks_ok(cleanup_checks):
        reason = blocking_reason or str(
            (cleanup_checks.get("tp") or {}).get("reason")
            or (cleanup_checks.get("sl") or {}).get("reason")
            or (cleanup_checks.get("time_stop") or {}).get("reason")
            or "residual_exit_order_cleanup_failed"
        )
        mark_error(
            account,
            symbol,
            error_code="post_entry_exit_cleanup_failed" if not blocking_reason else "post_entry_exit_reason_infer_failed",
            error_message=reason,
            error_bj=checked_bj,
            strategy_name=state_strategy_name,
        )
        _write_exec_event(audit_enabled, account, "spring_position_closed_exit_cleanup_failed" if not blocking_reason else "spring_position_closed_exit_reason_infer_failed", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "reason": reason,
            "order_checks": order_checks,
            "cleanup_checks": cleanup_checks,
        })
        return {
            "ok": False,
            "outcome": "flat_exit_cleanup_failed" if not blocking_reason else "flat_exit_reason_unresolved",
            "reason": reason,
            "order_checks": order_checks,
            "cleanup_checks": cleanup_checks,
            "checked_bj": checked_bj,
        }
    return _finalize_closed_trade(
        account=account,
        symbol=symbol,
        open_trade=open_trade,
        exit_reason=exit_reason,
        order_checks=order_checks,
        cleanup_checks=cleanup_checks,
        cfg=cfg,
        audit_enabled=audit_enabled,
        state_strategy_name=state_strategy_name,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        checked_bj=checked_bj,
        source=source,
    )


def _strategy_ownership_blocker(open_trade: Mapping[str, Any], *, strategy_name: str, strategy_code: str) -> str | None:
    payload_strategy_name = str(open_trade.get("strategy_name") or "").strip()
    payload_strategy_code = str(open_trade.get("strategy_code") or "").upper().strip()
    if payload_strategy_name and payload_strategy_name != strategy_name:
        return f"foreign_strategy_name:{payload_strategy_name}"
    if payload_strategy_code and payload_strategy_code != strategy_code:
        return f"foreign_strategy_code:{payload_strategy_code}"
    if not payload_strategy_name and not payload_strategy_code:
        return "missing_strategy_ownership"
    return None


def _local_activity_summary(state: Mapping[str, Any]) -> dict[str, Any]:
    pending_symbols: list[str] = []
    open_symbols: list[str] = []
    symbols = state.get("symbols") if isinstance(state, Mapping) else {}
    if isinstance(symbols, Mapping):
        for raw_symbol, payload in symbols.items():
            if not isinstance(payload, Mapping):
                continue
            symbol = str(raw_symbol).upper().strip()
            if not symbol:
                continue
            if payload.get("pending_entry_order"):
                pending_symbols.append(symbol)
            if payload.get("open_trade"):
                open_symbols.append(symbol)
    blockers: list[str] = []
    if pending_symbols:
        blockers.append("account_local_pending_entry_order")
    if open_symbols:
        blockers.append("account_local_open_trade")
    return {
        "ok": not blockers,
        "blockers": blockers,
        "pending_symbols": sorted(pending_symbols),
        "open_symbols": sorted(open_symbols),
        "pending_count": len(pending_symbols),
        "open_count": len(open_symbols),
    }


def account_local_activity_precheck(
    account: str,
    *,
    strategy_name: str = DEFAULT_STRATEGY_NAME,
) -> dict[str, Any]:
    state = load_live_state(account, strategy_name=strategy_name)
    return _local_activity_summary(state)


def _reconcile_strategy_pending_entries(
    account: str,
    *,
    cfg: Mapping[str, Any],
    current_time_ms: int,
    current_time_bj: str,
    strategy_name: str,
    strategy_code: str,
    source: str,
) -> list[dict[str, Any]]:
    audit_enabled = _require_bool(cfg, "audit_enabled")
    retry_max = _require_int(cfg, "order_retry_max", min_value=0)
    retry_delay_secs = _require_float(cfg, "api_retry_delay_secs", min_value=0.0)
    state = load_live_state(account, strategy_name=strategy_name)
    symbols = state.get("symbols") or {}
    results: list[dict[str, Any]] = []
    for raw_symbol, payload in sorted(symbols.items()):
        if not isinstance(payload, Mapping):
            continue
        pending = payload.get("pending_entry_order")
        if not isinstance(pending, Mapping):
            continue
        symbol = str(raw_symbol).upper().strip()
        if not symbol:
            continue
        ownership_blocker = _strategy_ownership_blocker(
            pending,
            strategy_name=strategy_name,
            strategy_code=strategy_code,
        )
        if ownership_blocker:
            mark_error(
                account,
                symbol,
                error_code="foreign_pending_entry_blocked",
                error_message=ownership_blocker,
                error_bj=current_time_bj,
                strategy_name=strategy_name,
            )
            _write_exec_event(audit_enabled, account, "spring_foreign_pending_entry_blocked", {
                "symbol": symbol,
                "source": source,
                "bar_ts": current_time_ms,
                "bar_bj": current_time_bj,
                "reason": ownership_blocker,
                "pending_entry_order": dict(pending),
            })
            results.append({"symbol": symbol, "ok": False, "outcome": "foreign_pending_entry_blocked", "reason": ownership_blocker})
            continue
        if payload.get("open_trade"):
            set_pending_entry_order(account, symbol, None, strategy_name=strategy_name)
            results.append({"symbol": symbol, "ok": True, "outcome": "pending_cleared_open_trade_exists"})
            continue

        position_res = get_position(account, symbol, POSITION_SIDE_LONG)
        open_orders_res = get_open_orders(account, symbol)
        mark_position_reconcile(account, symbol, reconcile_bj=current_time_bj, strategy_name=strategy_name)
        mark_order_reconcile(account, symbol, reconcile_bj=current_time_bj, strategy_name=strategy_name)
        if not position_res.get("ok") or not open_orders_res.get("ok"):
            reason = str(open_orders_res.get("reason") or position_res.get("reason") or "pending_reconcile_query_failed")
            mark_error(
                account,
                symbol,
                error_code="pending_reconcile_query_failed",
                error_message=reason,
                error_bj=current_time_bj,
                strategy_name=strategy_name,
            )
            _write_exec_event(audit_enabled, account, "spring_pending_reconcile_query_failed", {
                "symbol": symbol,
                "source": source,
                "bar_ts": current_time_ms,
                "bar_bj": current_time_bj,
                "order_root": pending.get("order_root"),
                "reason": reason,
                "exchange_snapshot": {"position": position_res, "orders": open_orders_res},
            })
            results.append({"symbol": symbol, "ok": False, "outcome": "pending_reconcile_query_failed", "reason": reason})
            continue

        open_orders = [dict(row) for row in (open_orders_res.get("data") or []) if isinstance(row, Mapping)]
        position = position_res.get("data")
        if position:
            recovered_trade = _recover_open_trade_from_pending(pending, dict(position))
            set_open_trade(account, symbol, recovered_trade, strategy_name=strategy_name)
            set_pending_entry_order(account, symbol, None, strategy_name=strategy_name)
            repair_res = _verify_or_repair_open_trade_brackets(
                account=account,
                symbol=symbol,
                open_trade=recovered_trade,
                position=dict(position),
                open_orders=open_orders,
                cfg=cfg,
                audit_enabled=audit_enabled,
                state_strategy_name=strategy_name,
                current_time_ms=current_time_ms,
                current_time_bj=current_time_bj,
                checked_bj=_now_bj_str(),
                source=f"{source}_pending_recovery",
            )
            if repair_res.get("ok"):
                mark_error(account, symbol, error_code=None, error_message=None, error_bj=None, strategy_name=strategy_name)
                cooldown_until_ts, cooldown_until_bj = _cooldown_until(current_time_ms, _require_int(cfg, "cooldown_mins", min_value=0))
                set_cooldown(account, symbol, cooldown_until_ts=cooldown_until_ts, cooldown_until_bj=cooldown_until_bj, strategy_name=strategy_name)
                _write_exec_event(audit_enabled, account, "spring_entry_filled_recovered_to_open_trade", {
                    "symbol": symbol,
                    "source": source,
                    "bar_ts": current_time_ms,
                    "bar_bj": current_time_bj,
                    "order_root": recovered_trade.get("order_root"),
                    "repair": repair_res,
                })
            results.append({
                "symbol": symbol,
                "ok": bool(repair_res.get("ok")),
                "outcome": "entry_filled_recovered_to_open_trade" if repair_res.get("ok") else "entry_recovery_bracket_repair_failed",
                "repair": repair_res,
            })
            continue

        entry_res = _resolve_leg_order(
            account,
            symbol,
            known_open_orders=open_orders,
            exchange_order_id=pending.get("exchange_order_id"),
            client_order_id=pending.get("client_order_id"),
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
        )
        if not entry_res.get("ok"):
            reason = str(entry_res.get("reason") or "pending_entry_query_failed")
            mark_error(account, symbol, error_code="pending_reconcile_query_failed", error_message=reason, error_bj=current_time_bj, strategy_name=strategy_name)
            _write_exec_event(audit_enabled, account, "spring_pending_reconcile_query_failed", {
                "symbol": symbol,
                "source": source,
                "bar_ts": current_time_ms,
                "bar_bj": current_time_bj,
                "order_root": pending.get("order_root"),
                "exchange_snapshot": {"entry_order": entry_res, "position": position_res, "orders": open_orders_res},
            })
            results.append({"symbol": symbol, "ok": False, "outcome": "pending_entry_query_failed", "reason": reason})
            continue
        entry_order = dict(entry_res.get("data") or {})
        entry_status = str(entry_order.get("status") or "").upper()
        if entry_order and entry_status in TERMINAL_ORDER_STATUSES:
            terminal_trade = _pending_terminal_trade(pending, entry_order)
            if entry_status in FILLED_ORDER_STATUSES:
                exit_reason, order_checks, blocking_reason = _infer_exit_reason_from_orders(
                    account,
                    symbol,
                    terminal_trade,
                    known_open_orders=open_orders,
                    retry_max=retry_max,
                    retry_delay_secs=retry_delay_secs,
                )
                cleanup_checks = _cancel_residual_exit_orders(
                    account=account,
                    symbol=symbol,
                    open_trade=terminal_trade,
                    known_open_orders=open_orders,
                    order_checks=order_checks,
                    retry_max=retry_max,
                    retry_delay_secs=retry_delay_secs,
                )
                if blocking_reason or not _cleanup_checks_ok(cleanup_checks):
                    reason = blocking_reason or str(
                        (cleanup_checks.get("tp") or {}).get("reason")
                        or (cleanup_checks.get("sl") or {}).get("reason")
                        or (cleanup_checks.get("time_stop") or {}).get("reason")
                        or "pending_terminal_cleanup_failed"
                    )
                    mark_error(account, symbol, error_code="pending_terminal_cleanup_failed", error_message=reason, error_bj=current_time_bj, strategy_name=strategy_name)
                    _write_exec_event(audit_enabled, account, "spring_pending_terminal_cleanup_failed", {
                        "symbol": symbol,
                        "source": source,
                        "bar_ts": current_time_ms,
                        "bar_bj": current_time_bj,
                        "order_root": pending.get("order_root"),
                        "exit_reason": exit_reason,
                        "reason": reason,
                        "exchange_snapshot": {"entry_order": entry_res, "position": position_res, "orders": open_orders_res, "cleanup": cleanup_checks},
                    })
                    results.append({"symbol": symbol, "ok": False, "outcome": "pending_terminal_cleanup_failed", "reason": reason})
                    continue
                finalize_res = _finalize_closed_trade(
                    account=account,
                    symbol=symbol,
                    open_trade=terminal_trade,
                    exit_reason=exit_reason,
                    order_checks=order_checks,
                    cleanup_checks=cleanup_checks,
                    cfg=cfg,
                    audit_enabled=audit_enabled,
                    state_strategy_name=strategy_name,
                    current_time_ms=current_time_ms,
                    current_time_bj=current_time_bj,
                    checked_bj=_now_bj_str(),
                    source=f"{source}_pending_terminal",
                )
                results.append({"symbol": symbol, **finalize_res, "outcome": "pending_terminal_filled_flat_state_cleared"})
            else:
                cleanup_checks = _cancel_residual_exit_orders(
                    account=account,
                    symbol=symbol,
                    open_trade=terminal_trade,
                    known_open_orders=open_orders,
                    order_checks={},
                    retry_max=retry_max,
                    retry_delay_secs=retry_delay_secs,
                )
                if not _cleanup_checks_ok(cleanup_checks):
                    reason = str(
                        (cleanup_checks.get("tp") or {}).get("reason")
                        or (cleanup_checks.get("sl") or {}).get("reason")
                        or (cleanup_checks.get("time_stop") or {}).get("reason")
                        or "pending_terminal_cleanup_failed"
                    )
                    mark_error(account, symbol, error_code="pending_terminal_cleanup_failed", error_message=reason, error_bj=current_time_bj, strategy_name=strategy_name)
                    results.append({"symbol": symbol, "ok": False, "outcome": "pending_terminal_cleanup_failed", "reason": reason})
                    continue
                set_pending_entry_order(account, symbol, None, strategy_name=strategy_name)
                mark_error(account, symbol, error_code=None, error_message=None, error_bj=None, strategy_name=strategy_name)
                _write_exec_event(audit_enabled, account, "spring_state_cleared_after_entry_terminal_without_fill", {
                    "symbol": symbol,
                    "source": source,
                    "bar_ts": current_time_ms,
                    "bar_bj": current_time_bj,
                    "order_root": pending.get("order_root"),
                    "entry_status": entry_status,
                    "cleanup_checks": cleanup_checks,
                })
                results.append({"symbol": symbol, "ok": True, "outcome": "pending_terminal_without_fill_cleared", "entry_status": entry_status})
            continue
        results.append({"symbol": symbol, "ok": True, "outcome": "pending_entry_waiting", "entry_status": entry_status or None})
    return results


def reconcile_strategy_open_trades(
    account: str,
    *,
    execution_config: Mapping[str, Any],
    current_time_ms: int,
    current_time_bj: str,
    latest_closes: Mapping[str, Any] | None = None,
    strategy_name: str = DEFAULT_STRATEGY_NAME,
    source: str,
) -> dict[str, Any]:
    cfg = dict(execution_config)
    if _require_str(cfg, "strategy_name") != strategy_name:
        raise ValueError("live execution config strategy_name does not match reconcile strategy_name")
    strategy_code = _require_str(cfg, "strategy_code").upper()
    if _require_str(cfg, "account") != str(account):
        raise ValueError("live execution config account does not match reconcile account")
    audit_enabled = _require_bool(cfg, "audit_enabled")
    latest_close_map = {
        str(symbol).upper().strip(): value
        for symbol, value in dict(latest_closes or {}).items()
        if str(symbol).strip()
    }
    pending_results = _reconcile_strategy_pending_entries(
        account,
        cfg=cfg,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        strategy_name=strategy_name,
        strategy_code=strategy_code,
        source=source,
    )
    state = load_live_state(account, strategy_name=strategy_name)
    symbols = state.get("symbols") or {}
    results: list[dict[str, Any]] = []
    for raw_symbol, payload in sorted(symbols.items()):
        if not isinstance(payload, Mapping):
            continue
        open_trade = payload.get("open_trade")
        if not isinstance(open_trade, Mapping):
            continue
        symbol = str(raw_symbol).upper().strip()
        if not symbol:
            continue
        ownership_blocker = _strategy_ownership_blocker(
            open_trade,
            strategy_name=strategy_name,
            strategy_code=strategy_code,
        )
        if ownership_blocker:
            mark_error(
                account,
                symbol,
                error_code="foreign_open_trade_blocked",
                error_message=ownership_blocker,
                error_bj=current_time_bj,
                strategy_name=strategy_name,
            )
            _write_exec_event(audit_enabled, account, "spring_foreign_open_trade_blocked", {
                "symbol": symbol,
                "source": source,
                "bar_ts": current_time_ms,
                "bar_bj": current_time_bj,
                "reason": ownership_blocker,
                "open_trade": dict(open_trade),
            })
            results.append({
                "symbol": symbol,
                "ok": False,
                "outcome": "foreign_open_trade_blocked",
                "reason": ownership_blocker,
            })
            continue
        result = _post_entry_reconcile(
            account=account,
            symbol=symbol,
            open_trade=dict(open_trade),
            cfg=cfg,
            audit_enabled=audit_enabled,
            state_strategy_name=strategy_name,
            current_time_ms=current_time_ms,
            current_time_bj=current_time_bj,
            source=source,
            active_time_stop=True,
            latest_close=latest_close_map.get(symbol),
        )
        result = dict(result)
        result["symbol"] = symbol
        results.append(result)
    post_state = load_live_state(account, strategy_name=strategy_name)
    activity = _local_activity_summary(post_state)
    return {
        "ok": all(bool(row.get("ok")) for row in pending_results) and all(bool(row.get("ok")) for row in results) and bool(activity.get("ok")),
        "strategy_name": strategy_name,
        "strategy_code": strategy_code,
        "account": str(account),
        "checked_bj": _now_bj_str(),
        "pending_results": pending_results,
        "results": results,
        "remaining_local_activity": activity,
    }


def execute_live_execution_plan(
    intent: ValidatedLiveExecutionIntent | Mapping[str, Any],
    *,
    execution_plan: Mapping[str, Any],
    execution_config: Mapping[str, Any],
    exchange_snapshot: Mapping[str, Any] | None,
    source: str,
) -> dict[str, Any]:
    intent_dict = intent.to_dict() if isinstance(intent, ValidatedLiveExecutionIntent) else validate_live_execution_intent(intent).to_dict()
    cfg = validate_live_execution_config(execution_config, intent=intent_dict)
    audit_enabled = _require_bool(cfg, "audit_enabled")
    account = intent_dict["account"]
    symbol = intent_dict["symbol"]
    state_strategy_name = str(intent_dict["strategy_name"])
    notify_label = _strategy_notify_label(state_strategy_name)
    signal = deepcopy(intent_dict["signal_snapshot"])
    current_time_ms = int(intent_dict["signal_time"])
    current_time_bj = str(intent_dict["signal_time_bj"])
    order_ids = dict((execution_plan.get("order_ids") or {}))
    order_root = str(order_ids.get("order_root") or "").strip()
    if not order_root:
        raise ValueError("execution plan missing order_root")
    if not bool(execution_plan.get("ok_to_execute")):
        raise ValueError(f"execution plan is not executable: {execution_plan.get('executable_blockers')}")

    precheck = dict(execution_plan.get("precheck") or {})
    local_precheck = dict(precheck.get("local_state") or {})
    exchange_precheck = dict(precheck.get("exchange") or {})
    if _require_bool(cfg, "require_local_state_precheck") and local_precheck.get("status") != "verified":
        raise ValueError("local state precheck is not verified")
    if _require_bool(cfg, "require_exchange_precheck") and exchange_precheck.get("status") != "verified":
        raise ValueError("exchange precheck is not verified")
    if local_precheck.get("blockers") or exchange_precheck.get("blockers"):
        raise ValueError(f"execution precheck blockers present: {local_precheck.get('blockers')}, {exchange_precheck.get('blockers')}")

    if not is_live_symbol_supported_for_signed_api(symbol):
        reason = _unsupported_live_symbol_reason(symbol)
        signal_digest = _signal_digest(signal)
        mark_signal(
            account,
            symbol,
            signal_side=POSITION_SIDE_LONG,
            signal_time_ts=int(intent_dict["signal_time"]),
            signal_time_bj=str(intent_dict["signal_time_bj"]),
            c_bar_ts=int(intent_dict["c_bar_ts"]) if intent_dict.get("c_bar_ts") else None,
            c_bar_bj=intent_dict.get("c_bar_bj"),
            signal_digest=signal_digest,
            signal_snapshot=signal,
            strategy_name=state_strategy_name,
        )
        mark_error(
            account,
            symbol,
            error_code="unsupported_live_symbol_non_ascii",
            error_message=reason,
            error_bj=_now_bj_str(),
            strategy_name=state_strategy_name,
        )
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj, strategy_name=state_strategy_name)
        _write_exec_event(audit_enabled, account, "spring_unsupported_live_symbol_skip", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": order_root,
            "reason": reason,
            "signal_snapshot": signal,
        })
        logging.warning(
            "Live execution skipped unsupported non-ASCII symbol | account=%s | strategy=%s | symbol=%r | source=%s",
            account,
            state_strategy_name,
            symbol,
            source,
        )
        if _notify_enabled(cfg, "notify_on_order_error"):
            _send_spring_notify(
                f"[{_fmt_hms_from_ms(current_time_ms)} {state_strategy_name}] {account}\n"
                f"LIVE skipped unsupported symbol\n"
                f"symbol={symbol}\n"
                f"reason=non_ascii_symbol_signed_api_unsupported",
                strategy_name=state_strategy_name,
            )
        return {
            "ok": False,
            "outcome": "skipped_unsupported_live_symbol_non_ascii",
            "reason": reason,
            "symbol": symbol,
        }

    if _require_str(cfg, "precheck_scope") == "account_flat":
        account_flat = _account_flat_precheck(exchange_snapshot)
        if not account_flat["ok"]:
            raise ValueError(f"account_flat precheck blockers present: {account_flat['blockers']}")
    else:
        account_flat = None

    pre_entry_price_res = get_last_price(account, symbol)
    pre_entry_price = None
    if pre_entry_price_res.get("ok"):
        pre_entry_price = _safe_float((pre_entry_price_res.get("data") or {}).get("price"))
    pre_entry_price_source = "CONTRACT_PRICE:futures_symbol_ticker"
    if pre_entry_price is None or pre_entry_price <= 0:
        reason = pre_entry_price_res.get("reason") or f"pre_entry_price={pre_entry_price}"
        mark_error(account, symbol, error_code="pre_entry_price_query_failed", error_message=reason, error_bj=_now_bj_str(), strategy_name=state_strategy_name)
        _write_exec_event(audit_enabled, account, "spring_pre_entry_price_query_failed", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": order_root,
            "price_source": pre_entry_price_source,
            "sl_price": float(intent_dict["sl_price"]),
            "exchange_snapshot": pre_entry_price_res,
        })
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj, strategy_name=state_strategy_name)
        return {"ok": False, "outcome": "failed_pre_entry_price_query", "reason": reason, "pre_entry_price_res": pre_entry_price_res}

    min_sl_distance_pct = _require_float(cfg, "pre_entry_min_sl_distance_pct", min_value=0.0)
    sl_price = float(intent_dict["sl_price"])
    pre_entry_sl_distance_pct = (float(pre_entry_price) - sl_price) / sl_price if sl_price > 0 else None
    guard_payload = {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": order_root,
        "price_source": pre_entry_price_source,
        "pre_entry_price": float(pre_entry_price),
        "sl_price": sl_price,
        "sl_distance_pct": pre_entry_sl_distance_pct,
        "min_sl_distance_pct": min_sl_distance_pct,
        "exchange_snapshot": pre_entry_price_res,
    }
    if pre_entry_sl_distance_pct is None or pre_entry_sl_distance_pct < min_sl_distance_pct:
        reason = (
            f"pre_entry_sl_distance_pct={pre_entry_sl_distance_pct} "
            f"< min_sl_distance_pct={min_sl_distance_pct}; "
            f"pre_entry_price={pre_entry_price}; sl_price={sl_price}"
        )
        _write_exec_event(audit_enabled, account, "spring_pre_entry_price_guard_skip", {**guard_payload, "reason": reason})
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj, strategy_name=state_strategy_name)
        return {"ok": False, "outcome": "skipped_pre_entry_price_too_close_to_sl", "reason": reason, "pre_entry_price": pre_entry_price}
    _write_exec_event(audit_enabled, account, "spring_pre_entry_price_guard_pass", guard_payload)

    sizing = _resolve_position_notional(intent_dict, entry_reference_price=float(pre_entry_price))
    notional = float(sizing["position_notional_usdt"])
    min_notional = _require_float(cfg, "min_position_notional_usdt", min_value=0.0)
    max_notional = _require_float(cfg, "max_position_notional_usdt", min_value=0.0)
    if notional < min_notional or notional > max_notional:
        raise ValueError(f"resolved notional {notional} outside live execution bounds [{min_notional}, {max_notional}]")

    raw_quantity = notional / float(pre_entry_price)
    if _require_bool(cfg, "require_symbol_filters"):
        filters_precheck = _symbol_filter_precheck(
            account,
            symbol,
            quantity=raw_quantity,
            current_price=float(pre_entry_price),
        )
        if not filters_precheck["ok"]:
            raise ValueError(f"symbol filter precheck blockers present: {filters_precheck['blockers']}")
        quantity = float(filters_precheck["quantity"])
    else:
        filters_precheck = None
        quantity = raw_quantity

    signal_digest = _signal_digest(signal)
    mark_signal(
        account,
        symbol,
        signal_side=POSITION_SIDE_LONG,
        signal_time_ts=int(intent_dict["signal_time"]),
        signal_time_bj=str(intent_dict["signal_time_bj"]),
        c_bar_ts=int(intent_dict["c_bar_ts"]) if intent_dict.get("c_bar_ts") else None,
        c_bar_bj=intent_dict.get("c_bar_bj"),
        signal_digest=signal_digest,
        signal_snapshot=signal,
        strategy_name=state_strategy_name,
    )
    _write_exec_event(audit_enabled, account, "spring_live_execution_started", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": order_root,
        "position_notional_usdt": notional,
        "quantity": quantity,
        "pre_entry_price": float(pre_entry_price),
        "pre_entry_price_source": pre_entry_price_source,
        "sizing": sizing,
        "precheck": precheck,
        "account_flat_precheck": account_flat,
        "symbol_filter_precheck": filters_precheck,
    })
    if _notify_enabled(cfg, "notify_on_signal_locked"):
        notify_intent = {
            **intent_dict,
            "pre_entry_price": float(pre_entry_price),
            "position_notional_usdt": float(notional),
        }
        _send_spring_notify(
            _strategy_signal_message(account, notify_intent),
            strategy_name=state_strategy_name,
        )

    hedge_res = ensure_hedge_mode(account)
    if not hedge_res.get("ok"):
        mark_error(account, symbol, error_code="hedge_mode_ensure_failed", error_message=hedge_res.get("reason"), error_bj=_now_bj_str(), strategy_name=state_strategy_name)
        raise RuntimeError(f"hedge mode ensure failed: {hedge_res.get('reason')}")
    margin_res = ensure_cross_margin(account, symbol)
    if not margin_res.get("ok"):
        mark_error(account, symbol, error_code="cross_margin_ensure_failed", error_message=margin_res.get("reason"), error_bj=_now_bj_str(), strategy_name=state_strategy_name)
        raise RuntimeError(f"cross margin ensure failed: {margin_res.get('reason')}")
    leverage_res = ensure_leverage(account, symbol, _require_int(cfg, "leverage", min_value=1))
    if not leverage_res.get("ok"):
        mark_error(account, symbol, error_code="leverage_ensure_failed", error_message=leverage_res.get("reason"), error_bj=_now_bj_str(), strategy_name=state_strategy_name)
        raise RuntimeError(f"leverage ensure failed: {leverage_res.get('reason')}")

    retry_max = _require_int(cfg, "order_retry_max", min_value=0)
    retry_delay_secs = _require_float(cfg, "api_retry_delay_secs", min_value=0.0)
    entry_res = place_entry_order(
        account,
        symbol,
        POSITION_SIDE_LONG,
        quantity,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
        client_order_id=order_ids["entry_client_order_id"],
        notify_label=notify_label,
    )
    if not entry_res.get("ok"):
        mark_error(account, symbol, error_code="entry_submit_failed", error_message=entry_res.get("reason"), error_bj=_now_bj_str(), strategy_name=state_strategy_name)
        _write_exec_event(audit_enabled, account, "spring_entry_submit_failed", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": order_root,
            "exchange_snapshot": entry_res,
        })
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj, strategy_name=state_strategy_name)
        return {"ok": False, "outcome": "failed_entry_submit", "reason": entry_res.get("reason"), "entry_res": entry_res}

    entry_data = dict(entry_res.get("data") or {})
    entry_event_time_ms = _extract_event_time_ms(entry_data, entry_data.get("raw"))
    entry_price, entry_price_source = _resolve_entry_fill_price(
        account,
        symbol,
        entry_data,
        fallback_price=float(pre_entry_price),
    )
    qty_for_exit = float(entry_data.get("executed_qty") or entry_data.get("qty") or quantity)
    try:
        resolved_tp_price, resolved_tp_price_source = _resolve_tp_price(intent_dict, entry_price=entry_price)
    except Exception as exc:
        flatten_client_order_id = build_client_order_id(
            broker_id=BROKER_ID,
            strat=str(intent_dict["strategy_code"]).upper(),
            leg=LEG_SL_FAIL_FLATTEN,
            root=order_root,
        )
        flatten_res = place_time_stop_order(
            account,
            symbol,
            POSITION_SIDE_LONG,
            qty_for_exit,
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
            client_order_id=flatten_client_order_id,
            order_role=EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN,
            notify_label=notify_label,
        )
        open_trade = _open_trade_payload(
            intent=intent_dict,
            order_ids=order_ids,
            order_root=order_root,
            entry_res=entry_res,
            sl_res={"ok": False, "data": None},
            tp_res={"ok": False, "data": None},
            entry_price=entry_price,
            entry_price_source=entry_price_source,
            signal_digest=signal_digest,
            pre_entry_price=float(pre_entry_price),
            pre_entry_price_source=pre_entry_price_source,
            position_notional_usdt=notional,
            sizing=sizing,
            resolved_tp_price=0.0,
            resolved_tp_price_source="tp_resolution_failed",
            status="EXIT_SUBMITTED" if flatten_res.get("ok") else "BRACKET_GAP_CRITICAL",
        )
        open_trade["time_stop_exit_reason"] = EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN
        open_trade["protective_flatten_exit_reason"] = EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN
        open_trade["protective_flatten_client_order_id"] = flatten_client_order_id
        if flatten_res.get("ok"):
            flatten_data = dict(flatten_res.get("data") or {})
            open_trade["time_stop_client_order_id"] = flatten_data.get("client_order_id", flatten_client_order_id)
            open_trade["time_stop_exchange_order_id"] = flatten_data.get("exchange_order_id")
            open_trade["protective_flatten_client_order_id"] = flatten_data.get("client_order_id", flatten_client_order_id)
            open_trade["protective_flatten_exchange_order_id"] = flatten_data.get("exchange_order_id")
            open_trade["exit_submit_inflight"] = True
        set_open_trade(account, symbol, open_trade, strategy_name=state_strategy_name)
        set_pending_entry_order(account, symbol, None, strategy_name=state_strategy_name)
        mark_error(account, symbol, error_code="tp_resolution_failed", error_message=str(exc), error_bj=_now_bj_str(), strategy_name=state_strategy_name)
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj, strategy_name=state_strategy_name)
        _write_exec_event(audit_enabled, account, "spring_tp_resolution_failed_flatten_submitted" if flatten_res.get("ok") else "spring_tp_resolution_failed_flatten_submit_failed", {
            "symbol": symbol,
            "source": "entry_tp_resolution_failed_flatten",
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": order_root,
            "entry_price": entry_price,
            "entry_price_source": entry_price_source,
            "pre_entry_price": float(pre_entry_price),
            "sl_price": float(intent_dict["sl_price"]),
            "reason": str(exc),
            "exchange_snapshot": flatten_res,
        })
        return {
            "ok": False,
            "outcome": "failed_tp_resolution_flatten_submitted" if flatten_res.get("ok") else "failed_tp_resolution_flatten_submit_failed",
            "reason": str(exc),
            "entry_res": entry_res,
            "flatten_res": flatten_res,
        }
    pending_entry = _pending_entry_payload(
        intent=intent_dict,
        order_ids=order_ids,
        order_root=order_root,
        entry_res=entry_res,
        entry_price_source=entry_price_source,
        signal_digest=signal_digest,
        pre_entry_price=float(pre_entry_price),
        pre_entry_price_source=pre_entry_price_source,
        position_notional_usdt=notional,
        sizing=sizing,
        resolved_tp_price=resolved_tp_price,
        resolved_tp_price_source=resolved_tp_price_source,
    )
    set_pending_entry_order(account, symbol, pending_entry, strategy_name=state_strategy_name)
    _write_exec_event(audit_enabled, account, "spring_entry_submitted", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": order_root,
        "entry_price": entry_price,
        "entry_price_source": entry_price_source,
        "pre_entry_price": float(pre_entry_price),
        "resolved_tp_price": resolved_tp_price,
        "resolved_tp_price_source": resolved_tp_price_source,
        "exchange_snapshot": entry_res,
    })

    sl_res = place_sl_order(
        account,
        symbol,
        POSITION_SIDE_LONG,
        float(intent_dict["sl_price"]),
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
        client_order_id=order_ids["sl_client_order_id"],
        notify_label=notify_label,
    )
    _write_exec_event(audit_enabled, account, "spring_sl_submitted" if sl_res.get("ok") else "spring_sl_submit_failed", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": order_root,
        "exchange_snapshot": sl_res,
    })
    if not sl_res.get("ok"):
        flatten_client_order_id = build_client_order_id(
            broker_id=BROKER_ID,
            strat=str(intent_dict["strategy_code"]).upper(),
            leg=LEG_SL_FAIL_FLATTEN,
            root=order_root,
        )
        flatten_res = place_time_stop_order(
            account,
            symbol,
            POSITION_SIDE_LONG,
            qty_for_exit,
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
            client_order_id=flatten_client_order_id,
            order_role=EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN,
            notify_label=notify_label,
        )
        open_trade = _open_trade_payload(
            intent=intent_dict,
            order_ids=order_ids,
            order_root=order_root,
            entry_res=entry_res,
            sl_res=sl_res,
            tp_res={"ok": False, "data": None},
            entry_price=entry_price,
            entry_price_source=entry_price_source,
            signal_digest=signal_digest,
            pre_entry_price=float(pre_entry_price),
            pre_entry_price_source=pre_entry_price_source,
            position_notional_usdt=notional,
            sizing=sizing,
            resolved_tp_price=resolved_tp_price,
            resolved_tp_price_source=resolved_tp_price_source,
            status="EXIT_SUBMITTED" if flatten_res.get("ok") else "BRACKET_GAP_CRITICAL",
        )
        open_trade["time_stop_exit_reason"] = EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN
        open_trade["protective_flatten_exit_reason"] = EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN
        open_trade["protective_flatten_client_order_id"] = flatten_client_order_id
        if flatten_res.get("ok"):
            flatten_data = dict(flatten_res.get("data") or {})
            open_trade["time_stop_client_order_id"] = flatten_data.get("client_order_id", flatten_client_order_id)
            open_trade["time_stop_exchange_order_id"] = flatten_data.get("exchange_order_id")
            open_trade["protective_flatten_client_order_id"] = flatten_data.get("client_order_id", flatten_client_order_id)
            open_trade["protective_flatten_exchange_order_id"] = flatten_data.get("exchange_order_id")
            open_trade["exit_submit_inflight"] = True
        set_open_trade(account, symbol, open_trade, strategy_name=state_strategy_name)
        set_pending_entry_order(account, symbol, None, strategy_name=state_strategy_name)
        mark_error(account, symbol, error_code="entry_sl_submit_failed", error_message=sl_res.get("reason"), error_bj=_now_bj_str(), strategy_name=state_strategy_name)
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj, strategy_name=state_strategy_name)
        _write_exec_event(audit_enabled, account, "spring_entry_sl_submit_failed", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": order_root,
            "sl_snapshot": sl_res,
            "flatten_snapshot": flatten_res,
        })
        _write_exec_event(audit_enabled, account, "spring_sl_submit_failed_flatten_submitted" if flatten_res.get("ok") else "spring_sl_submit_failed_flatten_submit_failed", {
            "symbol": symbol,
            "source": "entry_sl_fail_flatten",
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": order_root,
            "exit_reason": EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN,
            "protective_flatten_client_order_id": flatten_client_order_id,
            "exchange_snapshot": flatten_res,
        })
        return {
            "ok": False,
            "outcome": "failed_entry_sl_submit",
            "reason": sl_res.get("reason"),
            "entry_res": entry_res,
            "sl_res": sl_res,
            "flatten_res": flatten_res,
            "open_trade": open_trade,
        }

    tp_res = place_tp_order(
        account,
        symbol,
        POSITION_SIDE_LONG,
        qty_for_exit,
        float(resolved_tp_price),
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
        client_order_id=order_ids["tp_client_order_id"],
        notify_label=notify_label,
    )
    _write_exec_event(audit_enabled, account, "spring_tp_submitted" if tp_res.get("ok") else "spring_tp_submit_failed", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": order_root,
        "resolved_tp_price": resolved_tp_price,
        "resolved_tp_price_source": resolved_tp_price_source,
        "exchange_snapshot": tp_res,
    })

    open_trade = _open_trade_payload(
        intent=intent_dict,
        order_ids=order_ids,
        order_root=order_root,
        entry_res=entry_res,
        sl_res=sl_res,
        tp_res=tp_res,
        entry_price=entry_price,
        entry_price_source=entry_price_source,
        signal_digest=signal_digest,
        pre_entry_price=float(pre_entry_price),
        pre_entry_price_source=pre_entry_price_source,
        position_notional_usdt=notional,
        sizing=sizing,
        resolved_tp_price=resolved_tp_price,
        resolved_tp_price_source=resolved_tp_price_source,
        status="OPEN" if tp_res.get("ok") else "OPEN_SL_ONLY",
    )
    set_open_trade(account, symbol, open_trade, strategy_name=state_strategy_name)
    set_pending_entry_order(account, symbol, None, strategy_name=state_strategy_name)
    if tp_res.get("ok"):
        mark_error(account, symbol, error_code=None, error_message=None, error_bj=None, strategy_name=state_strategy_name)
    else:
        mark_error(account, symbol, error_code="tp_submit_failed", error_message=tp_res.get("reason"), error_bj=_now_bj_str(), strategy_name=state_strategy_name)
    cooldown_until_ts, cooldown_until_bj = _cooldown_until(current_time_ms, _require_int(cfg, "cooldown_mins", min_value=0))
    set_cooldown(account, symbol, cooldown_until_ts=cooldown_until_ts, cooldown_until_bj=cooldown_until_bj, strategy_name=state_strategy_name)
    mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj, strategy_name=state_strategy_name)
    _write_exec_event(audit_enabled, account, "spring_live_execution_completed", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": order_root,
        "entry_client_order_id": open_trade["entry_client_order_id"],
        "sl_client_order_id": open_trade["sl_order_client_id"],
        "tp_client_order_id": open_trade["tp_order_client_id"],
        "status": open_trade["status"],
        "cooldown_until_ts": cooldown_until_ts,
        "cooldown_until_bj": cooldown_until_bj,
    })
    if _notify_enabled(cfg, "notify_on_order_submit"):
        _send_spring_notify(
            _spring_entry_message(account, open_trade, entry_event_time_ms),
            strategy_name=state_strategy_name,
        )
    post_entry_reconcile = _post_entry_reconcile(
        account=account,
        symbol=symbol,
        open_trade=open_trade,
        cfg=cfg,
        audit_enabled=audit_enabled,
        state_strategy_name=state_strategy_name,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        source=source,
    )
    return {
        "ok": bool(tp_res.get("ok")),
        "outcome": "consumed_open_confirmed" if tp_res.get("ok") else "consumed_open_sl_only",
        "reason": "" if tp_res.get("ok") else tp_res.get("reason"),
        "symbol": symbol,
        "order_root": order_root,
        "entry_res": entry_res,
        "sl_res": sl_res,
        "tp_res": tp_res,
        "open_trade": open_trade,
        "post_entry_reconcile": post_entry_reconcile,
        "cooldown_until_ts": cooldown_until_ts,
        "cooldown_until_bj": cooldown_until_bj,
    }
