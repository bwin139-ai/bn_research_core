from __future__ import annotations

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from pathlib import Path
from typing import Any, Mapping

from core.live.audit_log import write_strategy_event
from core.live.binance_exec import (
    ensure_cross_margin,
    ensure_hedge_mode,
    ensure_leverage,
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
from core.live.execution_intent import ValidatedLiveExecutionIntent, validate_live_execution_intent
from core.live.live_state import (
    mark_error,
    mark_last_processed_bar,
    mark_order_reconcile,
    mark_position_reconcile,
    mark_signal,
    set_cooldown,
    set_open_trade,
    set_pending_entry_order,
)

BJ = timezone(timedelta(hours=8))
POSITION_SIDE_LONG = "LONG"
FILLED_ORDER_STATUSES = {"FILLED", "FINISHED"}


def _now_bj_str() -> str:
    return datetime.now(timezone.utc).astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")


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
    if _require_str(cfg, "stop_loss_failure_action") != "submit_market_flatten":
        raise ValueError("live execution config stop_loss_failure_action must be submit_market_flatten")
    _require_bool(cfg, "audit_enabled")
    _require_bool(cfg, "require_local_state_precheck")
    _require_bool(cfg, "require_exchange_precheck")
    _require_bool(cfg, "require_symbol_filters")
    _require_int(cfg, "leverage", min_value=1)
    _require_int(cfg, "order_retry_max", min_value=0)
    _require_int(cfg, "cooldown_mins", min_value=0)
    _require_float(cfg, "api_retry_delay_secs", min_value=0.0)
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


def _signal_digest(signal: Mapping[str, Any]) -> str:
    base = {
        "symbol": signal.get("symbol"),
        "signal_time": signal.get("signal_time"),
        "action": signal.get("action"),
        "current_price": signal.get("current_price"),
        "tp_price": signal.get("tp_price"),
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
        return fallback, "fallback_current_price"
    raise RuntimeError("entry fill price unavailable")


def _pending_entry_payload(
    *,
    intent: Mapping[str, Any],
    order_ids: Mapping[str, Any],
    order_root: str,
    entry_res: Mapping[str, Any],
    entry_price_source: str,
    signal_digest: str,
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
        "current_price": float(intent["current_price"]),
        "entry_notional_usdt": float(intent["position_notional_usdt"]),
        "signal_digest": signal_digest,
        "signal_snapshot": deepcopy(intent["signal_snapshot"]),
        "entry_fill_price_source": entry_price_source,
        "tp_price": float(intent["tp_price"]),
        "sl_price": float(intent["sl_price"]),
        "tp_client_order_id": order_ids["tp_client_order_id"],
        "sl_client_order_id": order_ids["sl_client_order_id"],
        "time_stop_client_order_id": order_ids["time_stop_client_order_id"],
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
        "entry_qty": float(entry.get("executed_qty") or entry.get("qty") or 0.0),
        "entry_notional_usdt": float(intent["position_notional_usdt"]),
        "signal_digest": signal_digest,
        "signal_snapshot": deepcopy(intent["signal_snapshot"]),
        "tp_order_client_id": tp.get("client_order_id", order_ids["tp_client_order_id"]) if tp else None,
        "tp_order_exchange_id": tp.get("exchange_order_id") if tp else None,
        "sl_order_client_id": sl.get("client_order_id", order_ids["sl_client_order_id"]) if sl else None,
        "sl_order_exchange_id": sl.get("exchange_order_id") if sl else None,
        "time_stop_client_order_id": None,
        "time_stop_exchange_order_id": None,
        "tp_price": float(intent["tp_price"]),
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


def _write_exec_event(enabled: bool, account: str, event: str, payload: dict[str, Any]) -> None:
    if enabled:
        enriched = dict(payload)
        enriched["execution_contract"] = "live_execution"
        write_strategy_event(account, "spring-sabc", event, enriched)


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
    legs = [
        (
            "time_stop",
            "TIME_STOP",
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
    leg = "tp" if exit_reason == "TAKE_PROFIT" else "sl" if exit_reason == "STOP_LOSS" else "time_stop" if exit_reason == "TIME_STOP" else ""
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
        return {
            "ok": True,
            "outcome": "position_still_open",
            "position_snapshot": position_res,
            "open_orders_count": len(open_orders),
            "checked_bj": checked_bj,
        }
    if open_orders:
        return {
            "ok": True,
            "outcome": "flat_but_open_orders_remain",
            "position_snapshot": position_res,
            "open_orders_snapshot": open_orders_res,
            "open_orders_count": len(open_orders),
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
    if blocking_reason or exit_reason == "UNKNOWN_EXIT":
        reason = blocking_reason or "exit_reason_unknown"
        mark_error(
            account,
            symbol,
            error_code="post_entry_exit_reason_infer_failed",
            error_message=reason,
            error_bj=checked_bj,
            strategy_name=state_strategy_name,
        )
        _write_exec_event(audit_enabled, account, "spring_position_closed_exit_reason_infer_failed", {
            "symbol": symbol,
            "source": source,
            "bar_ts": current_time_ms,
            "bar_bj": current_time_bj,
            "order_root": open_trade.get("order_root"),
            "reason": reason,
            "order_checks": order_checks,
        })
        return {
            "ok": False,
            "outcome": "flat_exit_reason_unresolved",
            "reason": reason,
            "order_checks": order_checks,
            "checked_bj": checked_bj,
        }

    exit_order = _exit_order_from_checks(exit_reason, order_checks)
    exit_price, exit_price_source = _resolve_exit_price(exit_reason, open_trade, exit_order)
    closed_trade = deepcopy(open_trade)
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
    mark_error(account, symbol, error_code=None, error_message=None, error_bj=None, strategy_name=state_strategy_name)
    _write_exec_event(audit_enabled, account, "spring_position_closed_detected", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": open_trade.get("order_root"),
        "exit_reason": exit_reason,
        "exit_price": exit_price,
        "exit_price_source": exit_price_source,
        "order_checks": order_checks,
    })
    _write_exec_event(audit_enabled, account, "spring_state_cleared_after_exit", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": open_trade.get("order_root"),
        "exit_reason": exit_reason,
    })
    return {
        "ok": True,
        "outcome": "flat_state_cleared",
        "exit_reason": exit_reason,
        "closed_trade": closed_trade,
        "order_checks": order_checks,
        "checked_bj": checked_bj,
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
    signal = deepcopy(intent_dict["signal_snapshot"])
    current_time_ms = int(intent_dict["signal_time"])
    current_time_bj = str(intent_dict["signal_time_bj"])
    order_ids = dict((execution_plan.get("order_ids") or {}))
    order_root = str(order_ids.get("order_root") or "").strip()
    if not order_root:
        raise ValueError("execution plan missing order_root")
    if not bool(execution_plan.get("ok_to_execute")):
        raise ValueError(f"execution plan is not executable: {execution_plan.get('executable_blockers')}")

    notional = float(intent_dict["position_notional_usdt"])
    min_notional = _require_float(cfg, "min_position_notional_usdt", min_value=0.0)
    max_notional = _require_float(cfg, "max_position_notional_usdt", min_value=0.0)
    if notional < min_notional or notional > max_notional:
        raise ValueError(f"intent notional {notional} outside live execution bounds [{min_notional}, {max_notional}]")

    precheck = dict(execution_plan.get("precheck") or {})
    local_precheck = dict(precheck.get("local_state") or {})
    exchange_precheck = dict(precheck.get("exchange") or {})
    if _require_bool(cfg, "require_local_state_precheck") and local_precheck.get("status") != "verified":
        raise ValueError("local state precheck is not verified")
    if _require_bool(cfg, "require_exchange_precheck") and exchange_precheck.get("status") != "verified":
        raise ValueError("exchange precheck is not verified")
    if local_precheck.get("blockers") or exchange_precheck.get("blockers"):
        raise ValueError(f"execution precheck blockers present: {local_precheck.get('blockers')}, {exchange_precheck.get('blockers')}")

    if _require_str(cfg, "precheck_scope") == "account_flat":
        account_flat = _account_flat_precheck(exchange_snapshot)
        if not account_flat["ok"]:
            raise ValueError(f"account_flat precheck blockers present: {account_flat['blockers']}")
    else:
        account_flat = None

    raw_quantity = float(((execution_plan.get("sizing") or {}).get("quantity")))
    if _require_bool(cfg, "require_symbol_filters"):
        filters_precheck = _symbol_filter_precheck(
            account,
            symbol,
            quantity=raw_quantity,
            current_price=float(intent_dict["current_price"]),
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
        "precheck": precheck,
        "account_flat_precheck": account_flat,
        "symbol_filter_precheck": filters_precheck,
    })

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
    entry_price, entry_price_source = _resolve_entry_fill_price(
        account,
        symbol,
        entry_data,
        fallback_price=float(intent_dict["current_price"]),
    )
    pending_entry = _pending_entry_payload(
        intent=intent_dict,
        order_ids=order_ids,
        order_root=order_root,
        entry_res=entry_res,
        entry_price_source=entry_price_source,
        signal_digest=signal_digest,
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
        "exchange_snapshot": entry_res,
    })

    qty_for_exit = float(entry_data.get("executed_qty") or entry_data.get("qty") or quantity)
    sl_res = place_sl_order(
        account,
        symbol,
        POSITION_SIDE_LONG,
        float(intent_dict["sl_price"]),
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
        client_order_id=order_ids["sl_client_order_id"],
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
        flatten_res = place_time_stop_order(
            account,
            symbol,
            POSITION_SIDE_LONG,
            qty_for_exit,
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
            client_order_id=order_ids["time_stop_client_order_id"],
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
            status="EXIT_SUBMITTED" if flatten_res.get("ok") else "BRACKET_GAP_CRITICAL",
        )
        if flatten_res.get("ok"):
            flatten_data = dict(flatten_res.get("data") or {})
            open_trade["time_stop_client_order_id"] = flatten_data.get("client_order_id", order_ids["time_stop_client_order_id"])
            open_trade["time_stop_exchange_order_id"] = flatten_data.get("exchange_order_id")
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
        float(intent_dict["tp_price"]),
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
        client_order_id=order_ids["tp_client_order_id"],
    )
    _write_exec_event(audit_enabled, account, "spring_tp_submitted" if tp_res.get("ok") else "spring_tp_submit_failed", {
        "symbol": symbol,
        "source": source,
        "bar_ts": current_time_ms,
        "bar_bj": current_time_bj,
        "order_root": order_root,
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
