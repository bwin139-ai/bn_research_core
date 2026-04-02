from __future__ import annotations

from typing import Any

from core.live.binance_exec import get_open_orders, get_positions
from core.live.live_state import load_live_state


def collect_consumer_exchange_activity_snapshot(account: str) -> dict[str, Any]:
    symbols: set[str] = set()
    positions_by_symbol: dict[str, list[dict[str, Any]]] = {}
    open_orders_by_symbol: dict[str, list[dict[str, Any]]] = {}

    pos_res = get_positions(account)
    if pos_res.get('ok'):
        for row in pos_res.get('data') or []:
            symbol = str(row.get('symbol') or '').upper().strip()
            if not symbol:
                continue
            symbols.add(symbol)
            positions_by_symbol.setdefault(symbol, []).append(row)

    ord_res = get_open_orders(account)
    if ord_res.get('ok'):
        for row in ord_res.get('data') or []:
            symbol = str(row.get('symbol') or '').upper().strip()
            if not symbol:
                continue
            symbols.add(symbol)
            open_orders_by_symbol.setdefault(symbol, []).append(row)

    return {
        'ok': bool(pos_res.get('ok') and ord_res.get('ok')),
        'symbols': symbols,
        'positions': pos_res,
        'orders': ord_res,
        'positions_by_symbol': positions_by_symbol,
        'open_orders_by_symbol': open_orders_by_symbol,
    }


def collect_consumer_local_activity_symbols(account: str) -> set[str]:
    state = load_live_state(account)
    out: set[str] = set()
    for symbol, payload in (state.get('symbols') or {}).items():
        if not isinstance(payload, dict):
            continue
        if payload.get('pending_entry_order') or payload.get('open_trade'):
            out.add(str(symbol).upper().strip())
    return out


def build_consumer_reconcile_plan(
    account: str,
    candidate_symbols: list[str],
    *,
    exchange_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    snapshot = dict(exchange_snapshot) if exchange_snapshot is not None else collect_consumer_exchange_activity_snapshot(account)
    exchange_activity_symbols = {
        str(symbol).upper().strip()
        for symbol in (snapshot.get('symbols') or set())
        if str(symbol).strip()
    }
    local_active_symbols = {
        str(symbol).upper().strip()
        for symbol in collect_consumer_local_activity_symbols(account)
        if str(symbol).strip()
    }
    candidate_symbol_set = {
        str(symbol).upper().strip()
        for symbol in (candidate_symbols or [])
        if str(symbol).strip()
    }
    snapshot['symbols'] = exchange_activity_symbols
    snapshot['local_active_symbols'] = sorted(local_active_symbols)
    return {
        'exchange_snapshot': snapshot,
        'exchange_activity_symbols': sorted(exchange_activity_symbols),
        'local_active_symbols': sorted(local_active_symbols),
        'extra_reconcile_symbols': sorted((exchange_activity_symbols | local_active_symbols) - candidate_symbol_set),
    }


def build_consumer_active_symbols(scan_gate: dict[str, Any]) -> set[str]:
    return {
        str(symbol).upper().strip()
        for symbol in (
            list(scan_gate.get('local_active_symbols') or [])
            + list(scan_gate.get('exchange_activity_symbols') or [])
        )
        if str(symbol).strip()
    }
