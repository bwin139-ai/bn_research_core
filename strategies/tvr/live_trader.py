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
)
from core.live.binance_rest_gateway import REQUEST_PRIORITY_CRITICAL, REQUEST_PRIORITY_HIGH, call_client_method
from core.live.custom_id import build_client_order_id, make_order_root
from core.live.live_state import (
    load_live_state,
    mark_loop_heartbeat,
    set_open_trade,
    set_pending_entry_order,
)
from core.runtime_state import get_state_dir
from strategies.tvr.decision_audit import (
    build_decision_audit,
    load_config as load_decision_audit_config,
    write_decision_audit_record,
)

BJ = timezone(timedelta(hours=8))
STRATEGY_NAME = "tvr"
STRATEGY_CODE = "TVR"
POSITION_SIDE_LONG = "LONG"
TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED"}
REPRICE_RETRY_ORDER_STATUSES = {"EXPIRED", "REJECTED"}


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


def load_config(path: str) -> dict[str, Any]:
    cfg = _load_json(path)
    if "schema_version" not in cfg:
        raise KeyError(f"TVR live trader config missing required field: schema_version | {path}")
    if int(cfg["schema_version"]) != 1:
        raise ValueError(f"TVR live trader config schema_version must be 1 | {path}")
    decision_audit = _require_mapping(cfg, path, "decision_audit")
    collection = _require_mapping(cfg, path, "collection")
    execution = _require_mapping(cfg, path, "execution")
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
        },
        "execution": {
            "position_side": _require_non_empty_str(execution, path, "position_side").upper(),
            "position_mode": _require_non_empty_str(execution, path, "position_mode").upper(),
            "margin_type": _require_non_empty_str(execution, path, "margin_type").upper(),
            "leverage": _require_int(execution, path, "leverage", positive=True),
            "order_notional_usdt": _require_float(execution, path, "order_notional_usdt", positive=True),
            "max_entry_notional_usdt": _require_float(execution, path, "max_entry_notional_usdt", positive=True),
            "max_open_trades": _require_int(execution, path, "max_open_trades", positive=True),
            "max_new_entries_per_iteration": _require_int(execution, path, "max_new_entries_per_iteration", positive=True),
            "entry_price_mode": _require_non_empty_str(execution, path, "entry_price_mode").upper(),
            "entry_best_bid_offset_ticks": _require_int(execution, path, "entry_best_bid_offset_ticks", positive=False),
            "entry_attempt_window_secs": _require_float(execution, path, "entry_attempt_window_secs", positive=True),
            "entry_retry_sleep_secs": _require_float(execution, path, "entry_retry_sleep_secs", positive=True),
            "entry_max_attempts": _require_int(execution, path, "entry_max_attempts", positive=True),
            "entry_order_ttl_secs": _require_float(execution, path, "entry_order_ttl_secs", positive=True),
            "take_profit_pct": _require_float(execution, path, "take_profit_pct", positive=True),
            "post_only_time_in_force": _require_non_empty_str(execution, path, "post_only_time_in_force").upper(),
            "order_retry_max": _require_int(execution, path, "order_retry_max", positive=False),
            "api_retry_delay_secs": _require_float(execution, path, "api_retry_delay_secs", positive=False),
            "require_symbol_flat": _require_bool(execution, path, "require_symbol_flat"),
            "require_no_symbol_open_orders": _require_bool(execution, path, "require_no_symbol_open_orders"),
        },
    }
    exec_cfg = out["execution"]
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
    if float(exec_cfg["order_notional_usdt"]) > float(exec_cfg["max_entry_notional_usdt"]):
        raise ValueError(f"TVR order_notional_usdt must be <= max_entry_notional_usdt | {path}")
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


def _extract_order_row(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "symbol": raw.get("symbol"),
        "order_id": raw.get("orderId"),
        "client_order_id": raw.get("clientOrderId"),
        "side": raw.get("side"),
        "position_side": raw.get("positionSide"),
        "type": raw.get("type"),
        "time_in_force": raw.get("timeInForce"),
        "status": str(raw.get("status") or "").upper(),
        "price": float(raw.get("price", 0.0) or 0.0),
        "orig_qty": float(raw.get("origQty", raw.get("quantity", 0.0)) or 0.0),
        "executed_qty": float(raw.get("executedQty", 0.0) or 0.0),
        "avg_price": float(raw.get("avgPrice", 0.0) or 0.0),
        "cum_quote": float(raw.get("cumQuote", 0.0) or 0.0),
        "update_time_ms": raw.get("updateTime"),
        "time_ms": raw.get("time"),
        "raw": dict(raw),
    }


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
) -> dict[str, Any]:
    payload = {
        "symbol": str(symbol).upper().strip(),
        "side": str(side).upper().strip(),
        "positionSide": str(position_side).upper().strip(),
        "type": "LIMIT",
        "timeInForce": str(time_in_force).upper().strip(),
        "quantity": float(quantity),
        "price": float(price),
        "newClientOrderId": str(client_order_id).strip(),
    }
    raw = call_client_method(
        account,
        source="tvr_live_trader.futures_create_order",
        method_name="futures_create_order",
        priority=REQUEST_PRIORITY_CRITICAL,
        **payload,
    )
    if not isinstance(raw, dict):
        raise RuntimeError(f"unexpected futures_create_order payload: {raw}")
    return _extract_order_row(raw)


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


def _entry_price_from_best_bid(account: str, symbol: str, cfg: Mapping[str, Any]) -> dict[str, Any]:
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
    qty = _floor_to_step(float(cfg["execution"]["order_notional_usdt"]) / price, filters.get("step_size"))
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
    order_notional = float(cfg["execution"]["order_notional_usdt"])
    if proposed is None or not math.isclose(float(proposed), order_notional, rel_tol=0.0, abs_tol=1e-9):
        raise ValueError(f"TVR intent proposed notional mismatch: {proposed} vs config {order_notional}")
    if order_notional > float(cfg["execution"]["max_entry_notional_usdt"]):
        raise ValueError("TVR order_notional_usdt exceeds max_entry_notional_usdt")

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
            size = _entry_price_from_best_bid(account, symbol, cfg)
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
    )
    tp_status = str(tp_order.get("status") or "").upper()
    if tp_status in TERMINAL_ORDER_STATUSES and tp_status != "FILLED":
        raise RuntimeError(f"TVR take-profit order terminal without maker TP: symbol={symbol} status={tp_status}")
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
        "opened_utc_ms": _now_utc_ms(),
        "opened_bj": _fmt_bj_from_ms(_now_utc_ms()),
        "entry_order_snapshot": dict(order),
        "tp_order_snapshot": dict(tp_order),
    }
    set_open_trade(account, symbol, open_trade, strategy_name=STRATEGY_NAME)
    set_pending_entry_order(account, symbol, None, strategy_name=STRATEGY_NAME)
    return {"action": "tp_submitted", "symbol": symbol, "open_trade": open_trade}


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
            if status == "PARTIALLY_FILLED" and executed_qty > 0:
                cancel_res = cancel_order(
                    account,
                    symbol_key,
                    exchange_order_id=_as_int(pending.get("entry_exchange_order_id")),
                    client_order_id=str(pending.get("entry_client_order_id") or ""),
                    retry_max=int(cfg["execution"]["order_retry_max"]),
                    retry_delay_secs=float(cfg["execution"]["api_retry_delay_secs"]),
                    notify_label="tvr",
                )
                events.append({"action": "partial_entry_cancel_remaining", "symbol": symbol_key, "cancel": cancel_res})
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
            created_ms = _as_int(pending.get("created_utc_ms"))
            if created_ms is not None and _now_utc_ms() - int(created_ms) > int(float(cfg["execution"]["entry_order_ttl_secs"]) * 1000):
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
            position_res = get_position(account, symbol_key, POSITION_SIDE_LONG)
            if position_res.get("ok") and not position_res.get("data"):
                set_open_trade(account, symbol_key, None, strategy_name=STRATEGY_NAME)
                events.append({"action": "open_trade_position_closed_cleared", "symbol": symbol_key, "trade": dict(open_trade)})
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


def run_once(cfg: Mapping[str, Any], *, run_id: str, dry_run: bool) -> Path | None:
    if not bool(cfg["enabled"]):
        raise RuntimeError("TVR live trader config enabled=false")
    account = str(cfg["account"]).strip()
    mark_loop_heartbeat(account, runner_pid=os.getpid(), strategy_name=STRATEGY_NAME)
    decision_cfg = load_decision_audit_config(str(cfg["decision_audit"]["config_path"]))
    if str(decision_cfg.get("account") or "").strip() != account:
        raise ValueError("TVR embedded decision config account mismatch")
    decision_run_id = _build_decision_run_id(account)
    decision = build_decision_audit(decision_cfg, run_id=decision_run_id)
    decision_path = write_decision_audit_record(decision) if bool(decision_cfg.get("audit_enabled")) else None
    if str(decision.get("account") or "").strip() != account:
        raise ValueError("TVR embedded decision account mismatch")
    if bool(decision.get("order_submission_enabled")) != bool(cfg["decision_audit"]["required_order_submission_enabled"]):
        raise ValueError("TVR decision audit order_submission_enabled does not match required value")

    reconcile_events = _reconcile_local_state(cfg, dry_run=dry_run)
    state = load_live_state(account, strategy_name=STRATEGY_NAME)
    active_symbols = _active_local_symbols(state)
    entry_events: list[dict[str, Any]] = []
    if len(active_symbols) < int(cfg["execution"]["max_open_trades"]):
        for intent in _selected_intents(decision):
            if len(entry_events) >= int(cfg["execution"]["max_new_entries_per_iteration"]):
                break
            symbol = str(intent.get("symbol") or "").upper().strip()
            if symbol in active_symbols:
                entry_events.append({"action": "entry_skipped_local_active_symbol", "symbol": symbol})
                continue
            event = _place_entry_from_intent(cfg=cfg, decision=decision, intent=intent, dry_run=dry_run)
            entry_events.append(event)
            if event.get("action") in {"entry_submitted", "dry_run_entry"}:
                active_symbols.append(symbol)
    else:
        entry_events.append({"action": "entry_blocked_max_open_trades", "active_symbols": active_symbols})

    record = {
        **_base_record(run_id, "tvr_live_trader_iteration"),
        "account": account,
        "dry_run": bool(dry_run),
        "allow_live_order": bool(cfg["allow_live_order"]),
        "decision_mode": "embedded",
        "decision_config_path": str(cfg["decision_audit"]["config_path"]),
        "decision_audit_path": str(decision_path) if decision_path is not None else None,
        "decision_audit_run_id": decision.get("run_id"),
        "decision_collected_bj": decision.get("collected_bj"),
        "decision_data_hub_inputs": dict(decision.get("data_hub_inputs") or {}),
        "decision_eligible_count": decision.get("eligible_count"),
        "decision_selected_count": decision.get("selected_count"),
        "selected_symbols": list(decision.get("selected_symbols") or []),
        "reconcile_events": reconcile_events,
        "entry_events": entry_events,
    }
    logging.info(
        "TVR live trader | dry_run=%s | selected=%s | reconcile=%s | entries=%s",
        dry_run,
        record["selected_symbols"],
        len(reconcile_events),
        len(entry_events),
    )
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
        logging.info("TVR live trader iteration started | run_id=%s | iteration=%s | dry_run=%s", run_id, iteration, args.dry_run)
        path = run_once(cfg, run_id=run_id, dry_run=bool(args.dry_run))
        logging.info("TVR live trader iteration finished | run_id=%s | iteration=%s | path=%s", run_id, iteration, path)
        if args.once:
            break
        if int(args.max_iterations) > 0 and iteration >= int(args.max_iterations):
            break
        time.sleep(int(cfg["collection"]["interval_secs"]))


if __name__ == "__main__":
    main()
