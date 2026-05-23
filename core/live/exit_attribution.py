from __future__ import annotations

from typing import Any, Mapping

FILLED_ORDER_STATUSES = {"FILLED", "FINISHED"}
OPEN_ORDER_STATUSES = {"NEW", "PARTIALLY_FILLED"}
UNKNOWN_EXIT = "UNKNOWN_EXIT"


def _as_mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _order_data(result: Any) -> Mapping[str, Any]:
    data = _as_mapping(result).get("data")
    return data if isinstance(data, Mapping) else {}


def _status(result: Any) -> str | None:
    text = str(_order_data(result).get("status") or "").upper().strip()
    return text or None


def _safe_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _position_flat(position_res: Any) -> bool | None:
    result = _as_mapping(position_res)
    if not result or not result.get("ok"):
        return None
    data = result.get("data")
    if not data:
        return True
    if not isinstance(data, Mapping):
        return None
    for key in ("position_amt", "positionAmt", "amount", "qty", "position_qty"):
        if key in data:
            qty = _safe_float(data.get(key))
            return None if qty is None else abs(qty) == 0
    return False


def _leg_summary(order_checks: Mapping[str, Any], leg: str) -> dict[str, Any]:
    result = _as_mapping(order_checks.get(leg))
    data = _order_data(result)
    return {
        "ok": result.get("ok"),
        "reason": result.get("reason"),
        "status": _status(result),
        "order_id": data.get("order_id"),
        "client_order_id": data.get("client_order_id"),
        "missing_identity": bool(result.get("missing_identity")),
        "missing_on_exchange": bool(result.get("missing_on_exchange")),
        "known_open_order_snapshot": bool(result.get("known_open_order_snapshot")),
        "not_in_open_orders_snapshot": bool(result.get("not_in_open_orders_snapshot")),
    }


def _cleanup_summary(cleanup_checks: Mapping[str, Any], leg: str) -> dict[str, Any]:
    result = _as_mapping(cleanup_checks.get(leg))
    data = _order_data(result)
    return {
        "ok": result.get("ok"),
        "reason": result.get("reason"),
        "status": _status(result),
        "order_id": data.get("order_id"),
        "client_order_id": data.get("client_order_id"),
        "skipped": bool(result.get("skipped")),
        "no_open_orders_snapshot": bool(result.get("no_open_orders_snapshot")),
        "not_in_open_orders_snapshot": bool(result.get("not_in_open_orders_snapshot")),
        "already_terminal": bool(result.get("already_terminal")),
    }


def build_unknown_exit_attribution_detail(
    *,
    exit_reason: str,
    position_res: Any = None,
    known_open_orders: list[dict[str, Any]] | None = None,
    order_checks: Mapping[str, Any] | None = None,
    cleanup_checks: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    if str(exit_reason or "").upper().strip() != UNKNOWN_EXIT:
        return None

    order_checks = _as_mapping(order_checks)
    cleanup_checks = _as_mapping(cleanup_checks)
    legs = ("time_stop", "tp", "sl")
    leg_checks = {leg: _leg_summary(order_checks, leg) for leg in legs}
    leg_cleanup = {leg: _cleanup_summary(cleanup_checks, leg) for leg in legs}
    statuses = {leg: payload.get("status") for leg, payload in leg_checks.items()}
    filled_legs = [leg for leg, status in statuses.items() if status in FILLED_ORDER_STATUSES]
    open_status_legs = [leg for leg, status in statuses.items() if status in OPEN_ORDER_STATUSES]
    open_orders_snapshot_available = known_open_orders is not None
    open_orders_snapshot_count = len(known_open_orders or []) if open_orders_snapshot_available else None
    open_orders_snapshot_empty = open_orders_snapshot_count == 0 if open_orders_snapshot_available else None
    open_query_but_absent_from_snapshot = bool(open_orders_snapshot_empty and open_status_legs)
    flat = _position_flat(position_res)

    attribution = "unknown"
    attribution_reason = "own_exit_order_not_filled_or_unresolved"
    if flat is True and not filled_legs:
        attribution = "external_or_exchange_inconsistent"
        attribution_reason = "position_flat_without_filled_own_exit_order"
        if open_query_but_absent_from_snapshot:
            attribution_reason = "position_flat_with_open_order_query_but_empty_open_orders_snapshot"
    elif filled_legs:
        attribution = "own_exit_order_filled_after_all"
        attribution_reason = "filled_own_exit_order_seen_in_checks"

    return {
        "attribution": attribution,
        "attribution_reason": attribution_reason,
        "position_flat": flat,
        "open_orders_snapshot_available": open_orders_snapshot_available,
        "open_orders_snapshot_empty": open_orders_snapshot_empty,
        "open_orders_snapshot_count": open_orders_snapshot_count,
        "own_filled_legs": filled_legs,
        "own_open_status_legs": open_status_legs,
        "open_query_but_absent_from_snapshot": open_query_but_absent_from_snapshot,
        "own_time_stop_status": statuses.get("time_stop"),
        "own_tp_status": statuses.get("tp"),
        "own_sl_status": statuses.get("sl"),
        "time_stop_query_status": statuses.get("time_stop"),
        "tp_query_status": statuses.get("tp"),
        "sl_query_status": statuses.get("sl"),
        "leg_checks": leg_checks,
        "cleanup_checks": leg_cleanup,
    }
