from __future__ import annotations

from dataclasses import asdict, is_dataclass
from typing import Any, Mapping

from core.live.custom_id import BROKER_ID, build_client_order_id, make_order_root
from core.live.execution_intent import ValidatedLiveExecutionIntent, validate_live_execution_intent

POSITION_SIDE_LONG = "LONG"
LEG_ENTRY = "EN"
LEG_TP = "TP"
LEG_SL = "SL"
LEG_TIME_STOP = "TS"


def _intent_to_dict(intent: ValidatedLiveExecutionIntent | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(intent, ValidatedLiveExecutionIntent):
        return intent.to_dict()
    if is_dataclass(intent):
        return asdict(intent)
    return validate_live_execution_intent(intent).to_dict()


def _symbol_state_from_snapshot(snapshot: Mapping[str, Any] | None, symbol: str) -> dict[str, Any]:
    if not isinstance(snapshot, Mapping):
        return {}
    symbols = snapshot.get("symbols")
    if isinstance(symbols, Mapping):
        value = symbols.get(symbol)
        return dict(value) if isinstance(value, Mapping) else {}
    return dict(snapshot)


def _local_state_precheck(
    *,
    local_state_snapshot: Mapping[str, Any] | None,
    symbol: str,
    current_time_ms: int,
) -> dict[str, Any]:
    symbol_state = _symbol_state_from_snapshot(local_state_snapshot, symbol)
    pending_entry_order = symbol_state.get("pending_entry_order")
    open_trade = symbol_state.get("open_trade")
    cooldown_until_ts = symbol_state.get("cooldown_until_ts")
    blockers: list[str] = []
    if pending_entry_order:
        blockers.append("local_pending_entry_order")
    if open_trade:
        blockers.append("local_open_trade")
    cooldown_active = False
    if cooldown_until_ts not in (None, ""):
        try:
            cooldown_active = int(cooldown_until_ts) > int(current_time_ms)
        except (TypeError, ValueError):
            blockers.append("local_cooldown_until_invalid")
        if cooldown_active:
            blockers.append("local_cooldown_active")
    return {
        "status": "verified" if local_state_snapshot is not None else "not_provided",
        "blocked": bool(blockers),
        "blockers": blockers,
        "pending_entry_order_present": bool(pending_entry_order),
        "open_trade_present": bool(open_trade),
        "cooldown_until_ts": cooldown_until_ts,
        "cooldown_active": bool(cooldown_active),
    }


def _exchange_symbol_rows(exchange_snapshot: Mapping[str, Any], symbol: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    positions_by_symbol = exchange_snapshot.get("positions_by_symbol")
    open_orders_by_symbol = exchange_snapshot.get("open_orders_by_symbol")
    positions = []
    orders = []
    if isinstance(positions_by_symbol, Mapping):
        positions = [dict(x) for x in (positions_by_symbol.get(symbol) or []) if isinstance(x, Mapping)]
    if isinstance(open_orders_by_symbol, Mapping):
        orders = [dict(x) for x in (open_orders_by_symbol.get(symbol) or []) if isinstance(x, Mapping)]
    return positions, orders


def _position_qty(row: Mapping[str, Any]) -> float:
    for key in ("qty", "positionAmt", "position_amt"):
        try:
            return abs(float(row.get(key) or 0.0))
        except (TypeError, ValueError):
            continue
    return 0.0


def _exchange_precheck(
    *,
    exchange_snapshot: Mapping[str, Any] | None,
    symbol: str,
) -> dict[str, Any]:
    if exchange_snapshot is None:
        return {
            "status": "not_verified",
            "blocked": False,
            "blockers": [],
            "reason": "exchange_snapshot_not_provided",
            "position_present": None,
            "nonlong_position_present": None,
            "open_orders_present": None,
        }
    positions_res = exchange_snapshot.get("positions")
    orders_res = exchange_snapshot.get("orders")
    blockers: list[str] = []
    if isinstance(positions_res, Mapping) and not positions_res.get("ok", False):
        blockers.append("precheck_positions_query_failed")
    if isinstance(orders_res, Mapping) and not orders_res.get("ok", False):
        blockers.append("precheck_orders_query_failed")

    positions, open_orders = _exchange_symbol_rows(exchange_snapshot, symbol)
    long_positions = [
        row for row in positions
        if str(row.get("position_side") or row.get("positionSide") or "").upper().strip() == POSITION_SIDE_LONG
        and _position_qty(row) > 0
    ]
    nonlong_positions = [
        row for row in positions
        if str(row.get("position_side") or row.get("positionSide") or "").upper().strip() != POSITION_SIDE_LONG
        and _position_qty(row) > 0
    ]
    if long_positions:
        blockers.append("exchange_has_position")
    if nonlong_positions:
        blockers.append("exchange_has_nonlong_position")
    if open_orders:
        blockers.append("exchange_has_open_orders")
    return {
        "status": "verified",
        "blocked": bool(blockers),
        "blockers": blockers,
        "position_present": bool(long_positions),
        "nonlong_position_present": bool(nonlong_positions),
        "open_orders_present": bool(open_orders),
        "positions_count": len(positions),
        "open_orders_count": len(open_orders),
    }


def _order_ids(strategy_code: str, order_root: str) -> dict[str, str]:
    return {
        "order_root": order_root,
        "entry_client_order_id": build_client_order_id(broker_id=BROKER_ID, strat=strategy_code, leg=LEG_ENTRY, root=order_root),
        "tp_client_order_id": build_client_order_id(broker_id=BROKER_ID, strat=strategy_code, leg=LEG_TP, root=order_root),
        "sl_client_order_id": build_client_order_id(broker_id=BROKER_ID, strat=strategy_code, leg=LEG_SL, root=order_root),
        "time_stop_client_order_id": build_client_order_id(broker_id=BROKER_ID, strat=strategy_code, leg=LEG_TIME_STOP, root=order_root),
    }


def build_dry_run_execution_plan(
    intent: ValidatedLiveExecutionIntent | Mapping[str, Any],
    *,
    exchange_snapshot: Mapping[str, Any] | None = None,
    local_state_snapshot: Mapping[str, Any] | None = None,
    order_root: str | None = None,
) -> dict[str, Any]:
    intent_dict = _intent_to_dict(intent)
    symbol = str(intent_dict["symbol"]).upper().strip()
    strategy_code = str(intent_dict["strategy_code"]).upper().strip()
    current_time_ms = int(intent_dict["signal_time"])
    base_order_notional_usdt = float(intent_dict["base_order_notional_usdt"])
    root = str(order_root).strip() if order_root else make_order_root()
    ids = _order_ids(strategy_code, root)
    local_precheck = _local_state_precheck(
        local_state_snapshot=local_state_snapshot,
        symbol=symbol,
        current_time_ms=current_time_ms,
    )
    exchange_precheck = _exchange_precheck(exchange_snapshot=exchange_snapshot, symbol=symbol)
    precheck_blockers = list(local_precheck["blockers"]) + list(exchange_precheck["blockers"])
    exchange_verified = exchange_precheck.get("status") == "verified"
    ok_to_execute = bool(not precheck_blockers and exchange_verified)
    executable_blockers = list(precheck_blockers)
    if not exchange_verified:
        executable_blockers.append("exchange_precheck_not_verified")

    order_plan = {
        "entry": {
            "role": "ENTRY",
            "order_type": "MARKET",
            "side": "BUY",
            "position_side": POSITION_SIDE_LONG,
            "quantity": None,
            "notional_usdt": None,
            "quantity_source": "resolved_after_live_pre_entry_price",
            "client_order_id": ids["entry_client_order_id"],
        },
        "stop_loss": {
            "role": "SL",
            "order_type": "STOP_MARKET",
            "side": "SELL",
            "position_side": POSITION_SIDE_LONG,
            "stop_price": float(intent_dict["sl_price"]),
            "close_position": True,
            "working_type": "CONTRACT_PRICE",
            "client_order_id": ids["sl_client_order_id"],
        },
        "take_profit": {
            "role": "TP",
            "order_type": "LIMIT",
            "side": "SELL",
            "position_side": POSITION_SIDE_LONG,
            "quantity": None,
            "price": None,
            "price_source": "resolved_after_entry_fill",
            "time_in_force": "GTC",
            "working_type": "CONTRACT_PRICE",
            "client_order_id": ids["tp_client_order_id"],
        },
        "time_stop": {
            "role": "TS",
            "order_type": "MARKET",
            "side": "SELL",
            "position_side": POSITION_SIDE_LONG,
            "quantity": None,
            "client_order_id": ids["time_stop_client_order_id"],
            "max_hold_mins": int(intent_dict["max_hold_mins"]),
            "min_profit_pct": float(intent_dict["time_stop_min_profit_pct"]),
        },
    }
    state_transition_plan = {
        "before_entry": [
            "record_signal_snapshot",
            "record_precheck_result",
            "create_order_root",
        ],
        "on_entry_submitted": [
            "set_pending_entry_order",
        ],
        "on_entry_filled": [
            "create_stop_loss_first",
            "create_take_profit_after_stop_loss_ok",
            "set_open_trade",
            "clear_pending_entry_order",
        ],
        "on_terminal_exit": [
            "cancel_residual_exit_orders",
            "write_live_trade_projection",
            "set_cooldown",
            "clear_open_trade",
        ],
    }
    return {
        "schema_version": 1,
        "run_mode": "dry_run_execution_plan",
        "dry_run_only": True,
        "strategy_name": intent_dict["strategy_name"],
        "strategy_code": strategy_code,
        "account": intent_dict["account"],
        "symbol": symbol,
        "side": POSITION_SIDE_LONG,
        "ok_to_execute": ok_to_execute,
        "executable_blockers": executable_blockers,
        "precheck": {
            "local_state": local_precheck,
            "exchange": exchange_precheck,
        },
        "sizing": {
            "base_order_notional_usdt": base_order_notional_usdt,
            "full_notional_risk_pct": float(intent_dict["full_notional_risk_pct"]),
            "position_notional_usdt": None,
            "quantity": None,
            "price_source": "resolved_after_live_pre_entry_price",
        },
        "order_ids": ids,
        "order_plan": order_plan,
        "state_transition_plan": state_transition_plan,
        "intent": intent_dict,
    }
