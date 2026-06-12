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
    get_order_book_top,
    get_position,
    get_symbol_filters,
    place_limit_order,
)
from core.live.custom_id import build_client_order_id, make_order_root
from core.message_bridge import send_to_bot
from core.runtime_state import get_state_dir
from strategies.cal.decision_audit import (
    STRATEGY_CODE,
    STRATEGY_NAME,
    build_decision_audit,
    load_config as load_decision_config,
)

BJ = timezone(timedelta(hours=8))
STRATEGY_LOGO = "⚓"
POSITION_SIDE_LONG = "LONG"
TERMINAL_ORDER_STATUSES = {"FILLED", "CANCELED", "CANCELLED", "EXPIRED", "REJECTED"}
ACTIVE_TP_STATUSES = {"NEW", "PARTIALLY_FILLED"}


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
    return get_state_dir() / "live_audit" / "cal" / "live" / day_key / "cal_live_trader.jsonl"


def _state_path(account: str) -> Path:
    return get_state_dir() / "live" / f"cal_{str(account).strip()}.state.json"


def _base_event(cfg: Mapping[str, Any], action: str) -> dict[str, Any]:
    now_ms = _now_utc_ms()
    return {
        "schema_version": 1,
        "strategy_name": STRATEGY_NAME,
        "run_mode": "live",
        "account": str(cfg["account"]),
        "action": str(action),
        "ts_utc_ms": int(now_ms),
        "ts_bj": _fmt_bj_from_ms(now_ms),
    }


def _write_event(cfg: Mapping[str, Any], action: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    event = _base_event(cfg, action)
    if payload:
        event.update(dict(payload))
    if bool(cfg.get("audit_enabled", True)):
        path = _append_jsonl(_audit_path(day_bj=str(event["ts_bj"])[:10]), event)
        event["audit_path"] = str(path)
    return event


def _notify_cal(account: str, title: str, lines: list[str]) -> bool:
    content = "\n".join([f"[{STRATEGY_LOGO} CAL] {account}", str(title), *[str(x) for x in lines]])
    return send_to_bot(content, label="cal")


def _load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"CAL live trader config missing: {path}")
    payload = json.loads(p.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"CAL live trader config must be JSON object: {path}")
    return payload


def _require_mapping(cfg: Mapping[str, Any], path: str, key: str) -> dict[str, Any]:
    if key not in cfg:
        raise KeyError(f"CAL live trader config missing required section: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, dict):
        raise TypeError(f"CAL live trader config section must be object: {key} | {path}")
    return dict(value)


def _require_bool(cfg: Mapping[str, Any], path: str, key: str) -> bool:
    if key not in cfg:
        raise KeyError(f"CAL live trader config missing required field: {key} | {path}")
    value = cfg[key]
    if not isinstance(value, bool):
        raise TypeError(f"CAL live trader config field must be bool: {key} | {path}")
    return bool(value)


def _require_str(cfg: Mapping[str, Any], path: str, key: str) -> str:
    if key not in cfg:
        raise KeyError(f"CAL live trader config missing required field: {key} | {path}")
    value = str(cfg[key]).strip()
    if not value:
        raise ValueError(f"CAL live trader config field must not be empty: {key} | {path}")
    return value


def _require_int(cfg: Mapping[str, Any], path: str, key: str, *, positive: bool) -> int:
    if key not in cfg:
        raise KeyError(f"CAL live trader config missing required field: {key} | {path}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"CAL live trader config field must be int: {key} | {path}")
    try:
        out = int(value)
    except Exception as exc:
        raise TypeError(f"CAL live trader config field must be int: {key} | {path}") from exc
    if positive and out <= 0:
        raise ValueError(f"CAL live trader config field must be > 0: {key} | {path}")
    if not positive and out < 0:
        raise ValueError(f"CAL live trader config field must be >= 0: {key} | {path}")
    return out


def _require_float(cfg: Mapping[str, Any], path: str, key: str, *, positive: bool) -> float:
    if key not in cfg:
        raise KeyError(f"CAL live trader config missing required field: {key} | {path}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"CAL live trader config field must be number: {key} | {path}")
    try:
        out = float(value)
    except Exception as exc:
        raise TypeError(f"CAL live trader config field must be number: {key} | {path}") from exc
    if math.isnan(out) or math.isinf(out):
        raise ValueError(f"CAL live trader config field must be finite: {key} | {path}")
    if positive and out <= 0:
        raise ValueError(f"CAL live trader config field must be > 0: {key} | {path}")
    if not positive and out < 0:
        raise ValueError(f"CAL live trader config field must be >= 0: {key} | {path}")
    return out


def load_config(path: str) -> dict[str, Any]:
    cfg = _load_json(path)
    if int(cfg.get("schema_version", 0)) != 1:
        raise ValueError(f"CAL live trader config schema_version must be 1 | {path}")
    collection = _require_mapping(cfg, path, "collection")
    execution = _require_mapping(cfg, path, "execution")
    logging_cfg = _require_mapping(cfg, path, "logging")
    out = {
        "schema_version": 1,
        "enabled": _require_bool(cfg, path, "enabled"),
        "account": _require_str(cfg, path, "account"),
        "allow_live_order": _require_bool(cfg, path, "allow_live_order"),
        "audit_enabled": _require_bool(cfg, path, "audit_enabled"),
        "decision_config_path": _require_str(cfg, path, "decision_config_path"),
        "collection": {
            "interval_secs": _require_int(collection, path, "interval_secs", positive=True),
        },
        "execution": {
            "entry_order_ttl_secs": _require_int(execution, path, "entry_order_ttl_secs", positive=True),
            "entry_retry_sleep_secs": _require_float(execution, path, "entry_retry_sleep_secs", positive=False),
            "partial_fill_wait_secs": _require_int(execution, path, "partial_fill_wait_secs", positive=False),
            "order_retry_max": _require_int(execution, path, "order_retry_max", positive=False),
            "api_retry_delay_secs": _require_float(execution, path, "api_retry_delay_secs", positive=False),
        },
        "logging": {
            "summary_interval_secs": _require_int(logging_cfg, path, "summary_interval_secs", positive=True),
        },
    }
    decision_cfg = load_decision_config(out["decision_config_path"])
    if str(decision_cfg["account"]) != str(out["account"]):
        raise ValueError(f"CAL live account must match decision config account | {path}")
    out["decision_cfg"] = decision_cfg
    return out


def _load_state(account: str) -> dict[str, Any]:
    path = _state_path(account)
    if not path.exists():
        return {
            "schema_version": 1,
            "strategy_name": STRATEGY_NAME,
            "account": str(account),
            "status": "RUNNING",
            "symbols": {},
        }
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError(f"CAL live state must be object: {path}")
    if int(payload.get("schema_version", 0)) != 1:
        raise ValueError(f"CAL live state schema_version must be 1: {path}")
    if str(payload.get("strategy_name") or "").lower() != STRATEGY_NAME:
        raise ValueError(f"CAL live state strategy_name mismatch: {path}")
    if str(payload.get("account") or "").strip() != str(account).strip():
        raise ValueError(f"CAL live state account mismatch: {path}")
    payload.setdefault("status", "RUNNING")
    payload.setdefault("symbols", {})
    if not isinstance(payload["symbols"], dict):
        raise TypeError(f"CAL live state symbols must be object: {path}")
    return payload


def _save_state(account: str, state: Mapping[str, Any]) -> None:
    path = _state_path(account)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.{time.time_ns()}.tmp")
    tmp.write_text(json.dumps(dict(state), ensure_ascii=False, indent=2, default=_json_default) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def _symbol_state(state: dict[str, Any], symbol: str) -> dict[str, Any]:
    symbols = state.setdefault("symbols", {})
    if not isinstance(symbols, dict):
        raise TypeError("CAL state symbols must be object")
    symbol_key = str(symbol).upper().strip()
    value = symbols.get(symbol_key)
    if not isinstance(value, dict):
        value = {"status": "RUNNING", "open_lots": [], "closed_lots": []}
        symbols[symbol_key] = value
    value.setdefault("status", "RUNNING")
    value.setdefault("open_lots", [])
    value.setdefault("closed_lots", [])
    return value


def _open_lots(symbol_state: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = symbol_state.get("open_lots")
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise TypeError("CAL open_lots must be list")
    return [dict(item) for item in raw if isinstance(item, Mapping)]


def _pending(symbol_state: Mapping[str, Any]) -> dict[str, Any] | None:
    raw = symbol_state.get("pending_entry_order")
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise TypeError("CAL pending_entry_order must be object")
    return dict(raw)


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


def _status(order: Mapping[str, Any] | None) -> str:
    if not isinstance(order, Mapping):
        return ""
    return str(order.get("status") or "").upper().strip()


def _executed_qty(order: Mapping[str, Any] | None) -> float:
    if not isinstance(order, Mapping):
        return 0.0
    return float(order.get("executed_qty") or 0.0)


def _avg_fill_price(order: Mapping[str, Any], fallback_price: float) -> float:
    avg = _as_float(order.get("avg_price"))
    if avg is not None and avg > 0:
        return float(avg)
    qty = _executed_qty(order)
    cum_quote = _as_float(order.get("cum_quote"))
    if qty > 0 and cum_quote is not None and cum_quote > 0:
        return float(cum_quote) / qty
    return float(fallback_price)


def _floor_to_step(value: float, step: Any) -> float:
    step_value = _as_float(step)
    if step_value is None or step_value <= 0:
        return float(value)
    return float((Decimal(str(value)) / Decimal(str(step_value))).to_integral_value(rounding=ROUND_DOWN) * Decimal(str(step_value)))


def _round_down_to_tick(value: float, tick: Any) -> float:
    tick_value = _as_float(tick)
    if tick_value is None or tick_value <= 0:
        return float(value)
    return float((Decimal(str(value)) / Decimal(str(tick_value))).to_integral_value(rounding=ROUND_DOWN) * Decimal(str(tick_value)))


def _normalize_size(account: str, symbol: str, *, notional_usdt: float, price: float) -> dict[str, Any]:
    filters_res = get_symbol_filters(account, symbol)
    if not filters_res.get("ok"):
        raise RuntimeError(f"CAL symbol filters query failed: {symbol} | {filters_res.get('reason')}")
    filters = dict(filters_res["data"])
    limit_price = _round_down_to_tick(float(price), filters.get("tick_size"))
    qty = _floor_to_step(float(notional_usdt) / limit_price, filters.get("step_size"))
    actual_notional = qty * limit_price
    if qty <= 0:
        raise RuntimeError(f"CAL normalized qty <= 0: {symbol}")
    if filters.get("min_qty") and qty < float(filters["min_qty"]):
        raise RuntimeError(f"CAL qty below min_qty: {symbol} | {qty} < {filters['min_qty']}")
    if filters.get("min_notional") and actual_notional < float(filters["min_notional"]):
        raise RuntimeError(f"CAL notional below min_notional: {symbol} | {actual_notional} < {filters['min_notional']}")
    return {"qty": float(qty), "price": float(limit_price), "notional_usdt": float(actual_notional), "filters": filters}


def _is_entry_size_config_error(exc: Exception) -> bool:
    text = str(exc)
    return (
        "CAL normalized qty <= 0:" in text
        or "CAL qty below min_qty:" in text
        or "CAL notional below min_notional:" in text
    )


def _is_post_only_reject(reason: Any) -> bool:
    text = str(reason or "")
    upper = text.upper()
    return (
        "APIERROR(CODE=-5022)" in upper
        or "GTX" in upper
        or "POST ONLY" in upper
        or "POST-ONLY" in upper
        or "COULD NOT BE EXECUTED AS MAKER" in upper
        or "WOULD BE IMMEDIATELY MATCHED" in upper
    )


def _prepare_symbol(cfg: Mapping[str, Any], symbol: str) -> None:
    account = str(cfg["account"])
    decision_cfg = cfg["decision_cfg"]
    hedge_res = ensure_hedge_mode(account)
    if not hedge_res.get("ok"):
        raise RuntimeError(f"CAL ensure hedge mode failed: {hedge_res.get('reason')}")
    margin_res = ensure_cross_margin(account, symbol)
    if not margin_res.get("ok"):
        raise RuntimeError(f"CAL ensure crossed margin failed: {symbol} | {margin_res.get('reason')}")
    symbol_key = str(symbol).upper().strip()
    lev_res = ensure_leverage(account, symbol, int(decision_cfg["execution"]["symbol_leverage"][symbol_key]))
    if not lev_res.get("ok"):
        raise RuntimeError(f"CAL ensure leverage failed: {symbol} | {lev_res.get('reason')}")


def _pause_symbol(
    *,
    cfg: Mapping[str, Any],
    state: dict[str, Any],
    symbol: str,
    reason: str,
    detail: Mapping[str, Any] | None = None,
) -> None:
    symbol_state = _symbol_state(state, symbol)
    symbol_state["status"] = "PAUSED_BY_INVARIANT_VIOLATION"
    symbol_state["paused_reason"] = str(reason)
    symbol_state["paused_detail"] = dict(detail or {})
    symbol_state["paused_utc_ms"] = _now_utc_ms()
    symbol_state["paused_bj"] = _fmt_bj_from_ms(symbol_state["paused_utc_ms"])
    state["status"] = "PAUSED_BY_INVARIANT_VIOLATION"
    _save_state(str(cfg["account"]), state)
    _write_event(cfg, "paused_by_invariant_violation", {"symbol": symbol, "reason": reason, "detail": dict(detail or {})})
    _notify_cal(str(cfg["account"]), "CRITICAL paused", [f"symbol={symbol}", f"reason={reason}"])


def _clear_resolved_position_qty_pause(account: str, state: dict[str, Any], symbol_state: dict[str, Any]) -> None:
    if str(symbol_state.get("paused_reason") or "") != "position_qty_below_cal_open_lot_qty":
        return
    symbol_state["status"] = "RUNNING"
    symbol_state.pop("paused_reason", None)
    symbol_state.pop("paused_detail", None)
    symbol_state.pop("paused_utc_ms", None)
    symbol_state.pop("paused_bj", None)
    if all(
        str(item.get("status") or "").upper().strip() != "PAUSED_BY_INVARIANT_VIOLATION"
        for item in (state.get("symbols") or {}).values()
        if isinstance(item, Mapping)
    ):
        state["status"] = "RUNNING"
    _save_state(account, state)


def _query_long_position(account: str, symbol: str) -> dict[str, Any] | None:
    res = get_position(account, symbol, POSITION_SIDE_LONG)
    if not res.get("ok"):
        raise RuntimeError(f"CAL position query failed: {symbol} | {res.get('reason')}")
    data = res.get("data")
    return dict(data) if isinstance(data, Mapping) else None


def _position_qty(position: Mapping[str, Any] | None) -> float:
    if not isinstance(position, Mapping):
        return 0.0
    return float(position.get("qty") or 0.0)


def _cal_qty(lots: list[Mapping[str, Any]]) -> float:
    total = 0.0
    for lot in lots:
        qty = _as_float(lot.get("entry_qty"))
        if qty is not None and qty > 0:
            total += qty
    return float(total)


def _append_closed_lot(symbol_state: dict[str, Any], lot: Mapping[str, Any], *, reason: str, order: Mapping[str, Any] | None) -> None:
    closed = list(symbol_state.get("closed_lots") or [])
    row = dict(lot)
    row["status"] = "CLOSED"
    row["exit_reason"] = str(reason)
    row["closed_utc_ms"] = _now_utc_ms()
    row["closed_bj"] = _fmt_bj_from_ms(row["closed_utc_ms"])
    row["exit_order_snapshot"] = dict(order or {})
    closed.append(row)
    symbol_state["closed_lots"] = closed[-200:]


def _emit_signal(cfg: Mapping[str, Any], intent: Mapping[str, Any]) -> None:
    account = str(cfg["account"])
    symbol = str(intent.get("symbol") or "").upper().strip()
    logging.info(
        "%s CAL SIGNAL | account=%s | symbol=%s | level=%s | current=%s | trigger=%s | anchor=%s:%s | notional=%s | tp_pct=%s",
        STRATEGY_LOGO,
        account,
        symbol,
        intent.get("level"),
        intent.get("current_price"),
        intent.get("trigger_price"),
        intent.get("anchor_type"),
        intent.get("anchor_price"),
        intent.get("proposed_order_notional_usdt"),
        intent.get("take_profit_pct"),
    )
    _notify_cal(
        account,
        f"SIGNAL {symbol}",
        [
            f"level={intent.get('level')}",
            f"current={intent.get('current_price')} <= trigger={intent.get('trigger_price')}",
            f"anchor={intent.get('anchor_type')} {intent.get('anchor_price')}",
            f"notional={intent.get('proposed_order_notional_usdt')}U",
            f"tp={intent.get('take_profit_pct')}",
        ],
    )


def _submit_tp_for_fill(
    *,
    cfg: Mapping[str, Any],
    state: dict[str, Any],
    symbol: str,
    pending: Mapping[str, Any],
    entry_order: Mapping[str, Any],
) -> None:
    account = str(cfg["account"])
    decision_cfg = cfg["decision_cfg"]
    symbol_state = _symbol_state(state, symbol)
    qty = _executed_qty(entry_order)
    if qty <= 0:
        raise RuntimeError(f"CAL cannot submit TP without executed qty: {symbol}")
    entry_price = _avg_fill_price(entry_order, float(pending["limit_price"]))
    filters_res = get_symbol_filters(account, symbol)
    if not filters_res.get("ok"):
        raise RuntimeError(f"CAL symbol filters query failed before TP: {symbol} | {filters_res.get('reason')}")
    filters = dict(filters_res["data"])
    intent = pending.get("intent") if isinstance(pending.get("intent"), Mapping) else {}
    take_profit_pct = _as_float(intent.get("take_profit_pct"))
    if take_profit_pct is None or take_profit_pct <= 0:
        raise RuntimeError(f"CAL pending intent missing valid take_profit_pct: {symbol}")
    tp_price = _round_down_to_tick(entry_price * (1.0 + float(take_profit_pct)), filters.get("tick_size"))
    tp_res = place_limit_order(
        account,
        symbol,
        POSITION_SIDE_LONG,
        "SELL",
        float(qty),
        float(tp_price),
        order_role="TP",
        time_in_force=str(decision_cfg["execution"]["post_only_time_in_force"]),
        client_order_id=str(pending["tp_client_order_id"]),
        retry_max=int(cfg["execution"]["order_retry_max"]),
        retry_delay_secs=float(cfg["execution"]["api_retry_delay_secs"]),
        notify_label="cal",
        notify_on_error=True,
        notify_on_success=True,
        notify_order_statuses={"NEW", "PARTIALLY_FILLED", "FILLED"},
    )
    if not tp_res.get("ok"):
        _pause_symbol(cfg=cfg, state=state, symbol=symbol, reason="tp_submit_failed", detail={"tp_res": tp_res})
        return
    tp_order = dict(tp_res.get("data") or {})
    lot = {
        "lot_id": str(pending["order_root"]),
        "ladder_id": str(pending["ladder_id"]),
        "level": str(pending["level"]),
        "status": "OPEN",
        "entry_client_order_id": entry_order.get("client_order_id") or pending.get("entry_client_order_id"),
        "entry_exchange_order_id": entry_order.get("exchange_order_id") or entry_order.get("order_id"),
        "entry_price": float(entry_price),
        "entry_qty": float(qty),
        "entry_notional_usdt": float(entry_price) * float(qty),
        "tp_price": float(tp_price),
        "tp_client_order_id": tp_order.get("client_order_id") or pending.get("tp_client_order_id"),
        "tp_exchange_order_id": tp_order.get("exchange_order_id") or tp_order.get("order_id"),
        "tp_order_snapshot": tp_order,
        "take_profit_pct": float(take_profit_pct),
        "opened_utc_ms": _now_utc_ms(),
        "opened_bj": _fmt_bj_from_ms(_now_utc_ms()),
    }
    lots = _open_lots(symbol_state)
    lots.append(lot)
    symbol_state["open_lots"] = lots
    symbol_state.pop("pending_entry_order", None)
    _save_state(account, state)
    _write_event(cfg, "tp_submitted", {"symbol": symbol, "lot": lot})
    logging.info(
        "%s CAL OPEN | account=%s | symbol=%s | level=%s | entry=%s | qty=%s | tp=%s | lot_id=%s",
        STRATEGY_LOGO,
        account,
        symbol,
        lot["level"],
        entry_price,
        qty,
        tp_price,
        lot["lot_id"],
    )
    _notify_cal(account, "OPEN", [f"symbol={symbol}", f"level={lot['level']}", f"entry={entry_price}", f"tp={tp_price}", f"qty={qty}"])


def _reconcile_pending(cfg: Mapping[str, Any], state: dict[str, Any], symbol: str) -> None:
    account = str(cfg["account"])
    symbol_state = _symbol_state(state, symbol)
    pending = _pending(symbol_state)
    if pending is None:
        return
    res = get_order(
        account,
        symbol,
        exchange_order_id=pending.get("entry_exchange_order_id"),
        client_order_id=str(pending.get("entry_client_order_id") or ""),
        retry_max=int(cfg["execution"]["order_retry_max"]),
        retry_delay_secs=float(cfg["execution"]["api_retry_delay_secs"]),
    )
    if not res.get("ok"):
        _write_event(cfg, "pending_entry_query_failed", {"symbol": symbol, "pending": pending, "reason": res.get("reason")})
        return
    order = dict(res.get("data") or {})
    status = _status(order)
    executed_qty = _executed_qty(order)
    now_ms = _now_utc_ms()
    created_ms = int(pending.get("created_utc_ms") or now_ms)
    ttl_ms = int(cfg["execution"]["entry_order_ttl_secs"]) * 1000
    should_cancel_empty = executed_qty <= 0 and now_ms - created_ms >= ttl_ms

    if status == "FILLED":
        _submit_tp_for_fill(cfg=cfg, state=state, symbol=symbol, pending=pending, entry_order=order)
        return

    if executed_qty > 0:
        _write_event(
            cfg,
            "pending_entry_partial_wait_full_fill",
            {"symbol": symbol, "status": status, "executed_qty": executed_qty, "pending": pending},
        )
        return

    if should_cancel_empty or status in {"EXPIRED", "REJECTED", "CANCELED", "CANCELLED"}:
        if status not in TERMINAL_ORDER_STATUSES:
            cancel_order(
                account,
                symbol,
                exchange_order_id=order.get("exchange_order_id"),
                client_order_id=order.get("client_order_id") or pending.get("entry_client_order_id"),
                retry_max=int(cfg["execution"]["order_retry_max"]),
                retry_delay_secs=float(cfg["execution"]["api_retry_delay_secs"]),
                notify_label="cal",
            )
        symbol_state.pop("pending_entry_order", None)
        _save_state(account, state)
        _write_event(cfg, "pending_entry_cleared_without_fill", {"symbol": symbol, "entry_order": order})
        return

    _write_event(cfg, "pending_entry_wait", {"symbol": symbol, "status": status, "executed_qty": executed_qty})


def _reconcile_open_lots(cfg: Mapping[str, Any], state: dict[str, Any], symbol: str) -> None:
    account = str(cfg["account"])
    symbol_state = _symbol_state(state, symbol)
    lots = _open_lots(symbol_state)
    if not lots:
        return

    rows: list[dict[str, Any]] = []
    for lot in lots:
        res = get_order(
            account,
            symbol,
            exchange_order_id=lot.get("tp_exchange_order_id"),
            client_order_id=str(lot.get("tp_client_order_id") or ""),
            retry_max=int(cfg["execution"]["order_retry_max"]),
            retry_delay_secs=float(cfg["execution"]["api_retry_delay_secs"]),
        )
        if not res.get("ok"):
            _pause_symbol(cfg=cfg, state=state, symbol=symbol, reason="tp_order_query_failed", detail={"lot": lot, "reason": res.get("reason")})
            return
        order = dict(res.get("data") or {})
        status = _status(order)
        if not status:
            _pause_symbol(cfg=cfg, state=state, symbol=symbol, reason="tp_order_status_missing", detail={"lot": lot, "order": order})
            return
        if status not in ACTIVE_TP_STATUSES and status != "FILLED":
            _pause_symbol(cfg=cfg, state=state, symbol=symbol, reason="tp_order_terminal_not_filled", detail={"lot": lot, "order": order})
            return
        rows.append({"lot": lot, "order": order, "status": status})

    filled_levels = {str(row["lot"].get("level") or "").upper() for row in rows if row["status"] == "FILLED"}
    active_levels = {str(row["lot"].get("level") or "").upper() for row in rows if row["status"] != "FILLED"}
    if "P1" in filled_levels and ({"P2", "P3"} & active_levels):
        _pause_symbol(cfg=cfg, state=state, symbol=symbol, reason="p1_tp_filled_while_lower_lots_open", detail={"active_levels": sorted(active_levels)})
        return
    if "P2" in filled_levels and "P3" in active_levels:
        _pause_symbol(cfg=cfg, state=state, symbol=symbol, reason="p2_tp_filled_while_p3_open", detail={"active_levels": sorted(active_levels)})
        return

    remaining: list[dict[str, Any]] = []
    closed_count = 0
    for row in rows:
        lot = row["lot"]
        if row["status"] == "FILLED":
            _append_closed_lot(symbol_state, lot, reason="TAKE_PROFIT", order=row["order"])
            logging.info(
                "%s CAL EXIT | account=%s | symbol=%s | level=%s | reason=TAKE_PROFIT | lot_id=%s",
                STRATEGY_LOGO,
                account,
                symbol,
                lot.get("level"),
                lot.get("lot_id"),
            )
            _notify_cal(
                account,
                "EXIT",
                [f"symbol={symbol}", f"level={lot.get('level')}", "reason=TAKE_PROFIT", f"lot_id={lot.get('lot_id')}"],
            )
            closed_count += 1
        else:
            remaining.append(lot)
    symbol_state["open_lots"] = remaining
    position = _query_long_position(account, symbol)
    position_qty = _position_qty(position)
    if remaining and position_qty <= 0:
        for lot in remaining:
            tp_cid = str(lot.get("tp_client_order_id") or "").strip()
            if tp_cid:
                cancel_order(account, symbol, client_order_id=tp_cid, notify_label="cal")
            _append_closed_lot(symbol_state, lot, reason="POSITION_CLOSED", order=None)
            logging.info(
                "%s CAL EXIT | account=%s | symbol=%s | level=%s | reason=POSITION_CLOSED | lot_id=%s",
                STRATEGY_LOGO,
                account,
                symbol,
                lot.get("level"),
                lot.get("lot_id"),
            )
            _notify_cal(
                account,
                "EXIT",
                [f"symbol={symbol}", f"level={lot.get('level')}", "reason=POSITION_CLOSED", f"lot_id={lot.get('lot_id')}"],
            )
        symbol_state["open_lots"] = []
        _save_state(account, state)
        _write_event(cfg, "open_lots_position_closed", {"symbol": symbol, "closed_count": len(remaining)})
        return
    if position_qty + 1e-12 < _cal_qty(remaining):
        _pause_symbol(
            cfg=cfg,
            state=state,
            symbol=symbol,
            reason="position_qty_below_cal_open_lot_qty",
            detail={"position_qty": position_qty, "cal_qty": _cal_qty(remaining)},
        )
        return
    _clear_resolved_position_qty_pause(account, state, symbol_state)
    _save_state(account, state)
    _write_event(
        cfg,
        "open_lots_reconcile",
        {"symbol": symbol, "open_count": len(remaining), "closed_count": closed_count, "position_qty": position_qty},
    )


def _place_entry(cfg: Mapping[str, Any], state: dict[str, Any], intent: Mapping[str, Any]) -> None:
    account = str(cfg["account"])
    decision_cfg = cfg["decision_cfg"]
    symbol = str(intent["symbol"]).upper().strip()
    level = str(intent.get("level") or "").upper().strip()
    if not bool(cfg["allow_live_order"]):
        _write_event(cfg, "entry_blocked_allow_live_order_false", {"symbol": symbol, "intent": dict(intent)})
        return
    symbol_state = _symbol_state(state, symbol)
    if str(symbol_state.get("status") or "").upper() == "PAUSED_BY_INVARIANT_VIOLATION":
        _write_event(cfg, "entry_blocked_symbol_paused", {"symbol": symbol})
        return
    if _pending(symbol_state) is not None:
        _write_event(cfg, "entry_blocked_pending_entry", {"symbol": symbol, "intent": dict(intent)})
        return
    existing_levels = {str(lot.get("level") or "").upper().strip() for lot in _open_lots(symbol_state)}
    if level in existing_levels:
        _write_event(cfg, "entry_blocked_existing_level", {"symbol": symbol, "level": level, "intent": dict(intent)})
        return
    _emit_signal(cfg, intent)
    _prepare_symbol(cfg, symbol)
    attempt = 0
    entry_order: dict[str, Any] | None = None
    order_size: dict[str, Any] | None = None
    accepted_root: str | None = None
    accepted_entry_cid: str | None = None
    accepted_tp_cid: str | None = None
    while True:
        attempt += 1
        attempt_root = make_order_root()
        attempt_cid = build_client_order_id(strat=STRATEGY_CODE, leg="ENT", root=attempt_root)
        attempt_tp_cid = build_client_order_id(strat=STRATEGY_CODE, leg="TP", root=attempt_root)
        book = get_order_book_top(account, symbol)
        if not book.get("ok"):
            raise RuntimeError(f"CAL order book query failed: {symbol} | {book.get('reason')}")
        best_bid = float(book["data"]["best_bid"])
        try:
            order_size = _normalize_size(
                account,
                symbol,
                notional_usdt=float(intent["proposed_order_notional_usdt"]),
                price=best_bid,
            )
        except RuntimeError as exc:
            if not _is_entry_size_config_error(exc):
                raise
            _pause_symbol(
                cfg=cfg,
                state=state,
                symbol=symbol,
                reason="entry_size_invalid",
                detail={"error": str(exc), "intent": dict(intent), "best_bid": float(best_bid)},
            )
            return
        entry_res = place_limit_order(
            account,
            symbol,
            POSITION_SIDE_LONG,
            "BUY",
            float(order_size["qty"]),
            float(order_size["price"]),
            order_role="ENTRY",
            time_in_force=str(decision_cfg["execution"]["post_only_time_in_force"]),
            client_order_id=attempt_cid,
            retry_max=int(cfg["execution"]["order_retry_max"]),
            retry_delay_secs=float(cfg["execution"]["api_retry_delay_secs"]),
            notify_label="cal",
            notify_on_error=True,
            notify_on_success=True,
            notify_order_statuses={"NEW", "PARTIALLY_FILLED", "FILLED"},
        )
        if not entry_res.get("ok"):
            if _is_post_only_reject(entry_res.get("reason")):
                _write_event(
                    cfg,
                    "entry_post_only_retry",
                    {"symbol": symbol, "attempt": attempt, "price": order_size["price"], "reason": entry_res.get("reason")},
                )
                time.sleep(float(cfg["execution"]["entry_retry_sleep_secs"]))
                continue
            _write_event(cfg, "entry_submit_failed", {"symbol": symbol, "intent": dict(intent), "reason": entry_res.get("reason")})
            return
        entry_order = dict(entry_res.get("data") or {})
        order_status = _status(entry_order)
        if order_status in {"EXPIRED", "REJECTED"}:
            _write_event(
                cfg,
                "entry_post_only_retry",
                {"symbol": symbol, "attempt": attempt, "price": order_size["price"], "status": order_status},
            )
            time.sleep(float(cfg["execution"]["entry_retry_sleep_secs"]))
            continue
        accepted_root = attempt_root
        accepted_entry_cid = str(entry_order.get("client_order_id") or attempt_cid)
        accepted_tp_cid = attempt_tp_cid
        break
    if entry_order is None or order_size is None or not accepted_root or not accepted_entry_cid or not accepted_tp_cid:
        raise RuntimeError(f"CAL entry order missing after retry loop: {symbol}")
    pending = {
        "order_root": accepted_root,
        "ladder_id": str(accepted_root),
        "level": str(intent["level"]),
        "intent": dict(intent),
        "entry_client_order_id": accepted_entry_cid,
        "entry_exchange_order_id": entry_order.get("exchange_order_id"),
        "tp_client_order_id": accepted_tp_cid,
        "limit_price": float(order_size["price"]),
        "qty": float(order_size["qty"]),
        "notional_usdt": float(order_size["notional_usdt"]),
        "created_utc_ms": _now_utc_ms(),
        "created_bj": _fmt_bj_from_ms(_now_utc_ms()),
        "entry_order_snapshot": entry_order,
    }
    if _status(entry_order) == "FILLED":
        _submit_tp_for_fill(cfg=cfg, state=state, symbol=symbol, pending=pending, entry_order=entry_order)
        return
    symbol_state["pending_entry_order"] = pending
    _save_state(account, state)
    _write_event(cfg, "entry_submitted", {"symbol": symbol, "pending": pending})
    logging.info(
        "%s CAL ENTRY SUBMITTED | account=%s | symbol=%s | level=%s | price=%s | qty=%s | cid=%s",
        STRATEGY_LOGO,
        account,
        symbol,
        pending["level"],
        pending["limit_price"],
        pending["qty"],
        pending["entry_client_order_id"],
    )
    _notify_cal(
        account,
        "ENTRY SUBMITTED",
        [
            f"symbol={symbol}",
            f"level={pending['level']}",
            f"price={pending['limit_price']}",
            f"qty={pending['qty']}",
            f"cid={pending['entry_client_order_id']}",
        ],
    )


def run_once(cfg: Mapping[str, Any]) -> dict[str, Any]:
    if not bool(cfg["enabled"]):
        raise RuntimeError("CAL live trader config enabled=false")
    account = str(cfg["account"])
    state = _load_state(account)
    for symbol in cfg["decision_cfg"]["universe"]["tradable_symbols"]:
        _reconcile_pending(cfg, state, str(symbol))
        _reconcile_open_lots(cfg, state, str(symbol))
    decision = build_decision_audit(cfg=cfg["decision_cfg"], run_id=f"CAL_LIVE_{account}", write_audit=True)
    selected = [dict(item) for item in decision.get("selected_intents") or [] if isinstance(item, Mapping)]
    for intent in selected:
        _place_entry(cfg, state, intent)
    state["last_loop_utc_ms"] = _now_utc_ms()
    state["last_loop_bj"] = _fmt_bj_from_ms(state["last_loop_utc_ms"])
    _save_state(account, state)
    return {"selected_count": len(selected), "decision_run_id": decision.get("run_id")}


def main() -> None:
    parser = argparse.ArgumentParser(description="Core Anchor Ladder live trader")
    parser.add_argument("--config", default="strategies/cal/config.live_trader.stark21.json")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()
    setup_logging()
    if bool(args.once) == bool(args.loop):
        raise SystemExit("Specify exactly one of --once or --loop")
    cfg = load_config(str(args.config))
    logging.info("%s CAL live trader started | account=%s | config=%s | allow_live_order=%s", STRATEGY_LOGO, cfg["account"], args.config, cfg["allow_live_order"])
    last_summary_ms = 0
    while True:
        try:
            event = run_once(cfg)
            now_ms = _now_utc_ms()
            should_log_summary = (
                bool(args.once)
                or int(event.get("selected_count") or 0) > 0
                or now_ms - int(last_summary_ms) >= int(cfg["logging"]["summary_interval_secs"]) * 1000
            )
            if should_log_summary:
                _write_event(cfg, "loop_summary", {"selected_count": event.get("selected_count"), "decision_run_id": event.get("decision_run_id")})
                logging.info("%s CAL live loop summary | account=%s | selected=%s", STRATEGY_LOGO, cfg["account"], event.get("selected_count"))
                last_summary_ms = now_ms
        except Exception as exc:
            logging.exception("CAL live loop failed: %s", exc)
            _write_event(cfg, "loop_exception", {"reason": str(exc)})
            _notify_cal(str(cfg["account"]), "CRITICAL loop exception", [str(exc)])
        if args.once:
            break
        time.sleep(int(cfg["collection"]["interval_secs"]))


if __name__ == "__main__":
    main()
