from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

BJ = timezone(timedelta(hours=8))

from core.live.audit_log import write_event
from core.live.binance_exec import get_open_orders, get_positions
from core.live.live_state import collect_account_symbol_strategy_activity, load_live_state, mark_order_reconcile, mark_position_reconcile



FIXED_POSITION_SIDE = 'LONG'
VALID_PRECHECK_SCOPES = {'symbol', 'account_flat'}
VALID_STRATEGY_CONCURRENCY_SCOPES = {'symbol', 'account'}


def require_consumer_precheck_scope(live_cfg: dict[str, Any]) -> str:
    scope = str(live_cfg.get('precheck_scope') or '').strip()
    if scope not in VALID_PRECHECK_SCOPES:
        raise ValueError('live_config.precheck_scope must be symbol or account_flat')
    return scope


def require_consumer_strategy_concurrency_scope(live_cfg: dict[str, Any]) -> str:
    scope = str(live_cfg.get('strategy_concurrency_scope') or '').strip()
    if scope not in VALID_STRATEGY_CONCURRENCY_SCOPES:
        raise ValueError('live_config.strategy_concurrency_scope must be symbol or account')
    return scope


def _normalize_scalar(value: Any) -> Any:
    if hasattr(value, 'item'):
        value = value.item()
    try:
        import pandas as _pd  # type: ignore
        if _pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, float):
        return float(value)
    if isinstance(value, int):
        return int(value)
    return value


def _json_default(v: Any) -> Any:
    if hasattr(v, 'item'):
        return v.item()
    if isinstance(v, set):
        return sorted(v)
    if isinstance(v, tuple):
        return list(v)
    raise TypeError(f"Object of type {type(v).__name__} is not JSON serializable")


def _json_safe_dumps(data: Any, *, sort_keys: bool = False, indent: int | None = None, separators: tuple[str, str] | None = None) -> str:
    kwargs: dict[str, Any] = {
        'ensure_ascii': False,
        'default': _json_default,
    }
    if sort_keys:
        kwargs['sort_keys'] = True
    if indent is not None:
        kwargs['indent'] = indent
    if separators is not None:
        kwargs['separators'] = separators
    return json.dumps(data, **kwargs)


def _perf_elapsed_ms(start_perf: float) -> int:
    return int((time.perf_counter() - start_perf) * 1000)


def _should_log_perf_stage(stage: str, payload: dict[str, Any]) -> bool:
    if stage == 'audit_consumer_orphan_exchange_activity':
        return bool(
            payload.get('findings_count')
            or payload.get('input_symbols_count')
            or payload.get('seen_symbols_count')
            or payload.get('skipped_local_active_count')
            or int(payload.get('total_elapsed_ms') or 0) >= 50
        )
    if stage == 'evaluate_consumer_signal_scan_gate':
        return bool(
            not payload.get('ok_to_scan')
            or payload.get('skip_reason')
            or payload.get('orphan_findings_count')
            or payload.get('missing_reconcile_symbols_count')
            or payload.get('active_state_errors_count')
            or payload.get('exchange_activity_symbols_count')
            or payload.get('local_active_symbols_count')
            or int(payload.get('total_elapsed_ms') or 0) >= 50
        )
    if stage == 'prepare_consumer_loop_gate':
        return bool(
            not payload.get('ok_to_scan')
            or payload.get('skip_reason')
            or payload.get('active_symbols_count')
            or int(payload.get('maintain_elapsed_ms') or 0) >= 50
            or int(payload.get('scan_gate_elapsed_ms') or 0) >= 50
            or int(payload.get('active_symbols_elapsed_ms') or 0) >= 50
            or int(payload.get('total_elapsed_ms') or 0) >= 100
        )
    if stage == 'maintain_consumer_once':
        return bool(
            payload.get('pending_reconcile_error')
            or payload.get('open_trade_reconcile_error')
            or payload.get('active_state_errors_count')
            or payload.get('pending_symbols_count')
            or payload.get('open_symbols_count')
            or payload.get('touched_symbols_count')
            or int(payload.get('total_elapsed_ms') or 0) >= 50
        )
    return True


def _log_perf_stage(stage: str, **fields: Any) -> None:
    return None


def precheck_exchange_blockers(account: str, symbol: str, snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    symbol_key = str(symbol).upper().strip()

    if snapshot is not None:
        all_pos_res = snapshot.get('positions') or {'ok': False, 'reason': 'missing positions snapshot', 'data': None}
        all_ord_res = snapshot.get('orders') or {'ok': False, 'reason': 'missing orders snapshot', 'data': None}
        symbol_positions = list((snapshot.get('positions_by_symbol') or {}).get(symbol_key) or [])
        symbol_open_orders = list((snapshot.get('open_orders_by_symbol') or {}).get(symbol_key) or [])

        long_position = None
        if all_pos_res.get('ok'):
            for row in symbol_positions:
                position_side = str(row.get('position_side') or '').upper().strip()
                try:
                    qty = abs(float(row.get('qty') or 0.0))
                except (TypeError, ValueError):
                    qty = 0.0
                if position_side == FIXED_POSITION_SIDE and qty > 0:
                    long_position = row
                    break

        return {
            'position': {
                'ok': bool(all_pos_res.get('ok')),
                'reason': all_pos_res.get('reason'),
                'data': long_position,
            },
            'positions_all_sides': {
                'ok': bool(all_pos_res.get('ok')),
                'reason': all_pos_res.get('reason'),
                'data': symbol_positions,
            },
            'orders': {
                'ok': bool(all_ord_res.get('ok')),
                'reason': all_ord_res.get('reason'),
                'data': symbol_open_orders,
            },
        }

    all_pos_res = get_positions(account)
    ord_res = get_open_orders(account, symbol)

    symbol_positions: list[dict[str, Any]] = []
    long_position = None
    if all_pos_res.get('ok'):
        for row in all_pos_res.get('data') or []:
            row_symbol = str(row.get('symbol') or '').upper().strip()
            if row_symbol != symbol_key:
                continue
            symbol_positions.append(row)
            if long_position is not None:
                continue
            position_side = str(row.get('position_side') or '').upper().strip()
            try:
                qty = abs(float(row.get('qty') or 0.0))
            except (TypeError, ValueError):
                qty = 0.0
            if position_side == FIXED_POSITION_SIDE and qty > 0:
                long_position = row

    return {
        'position': {
            'ok': bool(all_pos_res.get('ok')),
            'reason': all_pos_res.get('reason'),
            'data': long_position,
        },
        'positions_all_sides': {
            'ok': bool(all_pos_res.get('ok')),
            'reason': all_pos_res.get('reason'),
            'data': symbol_positions,
        },
        'orders': ord_res,
    }


def precheck_exchange_account_flat_blockers(account: str, snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    if snapshot is not None:
        all_pos_res = snapshot.get('positions') or {'ok': False, 'reason': 'missing positions snapshot', 'data': None}
        all_ord_res = snapshot.get('orders') or {'ok': False, 'reason': 'missing orders snapshot', 'data': None}
    else:
        all_pos_res = get_positions(account)
        all_ord_res = get_open_orders(account)

    all_positions = list(all_pos_res.get('data') or []) if all_pos_res.get('ok') else []
    all_orders = list(all_ord_res.get('data') or []) if all_ord_res.get('ok') else []
    long_position = None
    for row in all_positions:
        position_side = str(row.get('position_side') or '').upper().strip()
        try:
            qty = abs(float(row.get('qty') or 0.0))
        except (TypeError, ValueError):
            qty = 0.0
        if position_side == FIXED_POSITION_SIDE and qty > 0:
            long_position = row
            break

    return {
        'position': {
            'ok': bool(all_pos_res.get('ok')),
            'reason': all_pos_res.get('reason'),
            'data': long_position,
        },
        'positions_all_sides': {
            'ok': bool(all_pos_res.get('ok')),
            'reason': all_pos_res.get('reason'),
            'data': all_positions,
        },
        'orders': {
            'ok': bool(all_ord_res.get('ok')),
            'reason': all_ord_res.get('reason'),
            'data': all_orders,
        },
    }


def has_position_or_orders(snapshot: dict[str, Any]) -> tuple[bool, str]:
    pos_res = snapshot['position']
    all_pos_res = snapshot.get('positions_all_sides') or {}
    ord_res = snapshot['orders']
    if not pos_res.get('ok'):
        return True, 'precheck_position_query_failed'
    if not all_pos_res.get('ok'):
        return True, 'precheck_positions_query_failed'
    if not ord_res.get('ok'):
        return True, 'precheck_orders_query_failed'
    if pos_res.get('data'):
        return True, 'exchange_has_position'
    if all_pos_res.get('data'):
        return True, 'exchange_has_nonlong_position'
    if ord_res.get('data'):
        return True, 'exchange_has_open_orders'
    return False, ''

def audit_consumer_orphan_exchange_activity(
    account: str,
    symbols: list[str],
    current_time_ms: int,
    current_time_bj: str,
    *,
    source: str,
    audit_enabled: bool,
    snapshot: dict[str, Any] | None = None,
    mark_reconcile: bool = True,
) -> list[dict[str, Any]]:
    audit_started_perf = time.perf_counter()
    findings: list[dict[str, Any]] = []
    if not audit_enabled:
        return findings

    local_active_symbols = set(snapshot.get('local_active_symbols') or []) if snapshot else collect_consumer_local_activity_symbols(account)
    all_positions_res = (snapshot or {}).get('positions') if snapshot else None
    if not all_positions_res:
        all_positions_res = get_positions(account)

    positions_by_symbol: dict[str, list[dict[str, Any]]] = dict((snapshot or {}).get('positions_by_symbol') or {})
    if not positions_by_symbol and all_positions_res.get('ok'):
        for row in all_positions_res.get('data') or []:
            symbol = str(row.get('symbol') or '').upper().strip()
            if not symbol:
                continue
            positions_by_symbol.setdefault(symbol, []).append(row)

    seen: set[str] = set()
    perf_seen_symbols = 0
    perf_skipped_local_active = 0
    perf_precheck_elapsed_ms = 0
    perf_mark_position_elapsed_ms = 0
    perf_mark_order_elapsed_ms = 0
    for raw_symbol in symbols:
        symbol = str(raw_symbol).upper().strip()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        perf_seen_symbols += 1
        if symbol in local_active_symbols:
            perf_skipped_local_active += 1
            continue

        precheck_started_perf = time.perf_counter()
        exch = precheck_exchange_blockers(account, symbol, snapshot=snapshot)
        perf_precheck_elapsed_ms += _perf_elapsed_ms(precheck_started_perf)

        if mark_reconcile:
            mark_position_started_perf = time.perf_counter()
            mark_position_reconcile(account, symbol, reconcile_bj=current_time_bj)
            perf_mark_position_elapsed_ms += _perf_elapsed_ms(mark_position_started_perf)

            mark_order_started_perf = time.perf_counter()
            mark_order_reconcile(account, symbol, reconcile_bj=current_time_bj)
            perf_mark_order_elapsed_ms += _perf_elapsed_ms(mark_order_started_perf)

        pos_res = exch.get('position') or {}
        ord_res = exch.get('orders') or {}
        symbol_positions = positions_by_symbol.get(symbol) or []
        has_long_position = bool(pos_res.get('ok') and pos_res.get('data'))
        has_any_position = bool(symbol_positions)
        has_orders = bool(ord_res.get('ok') and ord_res.get('data'))
        if not has_any_position and not has_orders:
            continue

        findings.append({
            'symbol': symbol,
            'has_any_position': has_any_position,
            'has_long_position': has_long_position,
            'has_orders': has_orders,
        })

        exchange_snapshot = {
            'position': pos_res,
            'orders': ord_res,
            'positions_all_sides': {
                'ok': all_positions_res.get('ok', False),
                'reason': all_positions_res.get('reason'),
                'data': symbol_positions,
            },
        }
        write_event(account, 'orphan_exchange_activity', {
            'symbol': symbol,
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'source': source,
            'exchange_snapshot': exchange_snapshot,
        })
        if has_any_position:
            write_event(account, 'orphan_exchange_position', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'source': source,
                'exchange_snapshot': exchange_snapshot['positions_all_sides'],
            })
        if has_any_position and not has_long_position:
            write_event(account, 'orphan_exchange_nonlong_position', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'source': source,
                'exchange_snapshot': exchange_snapshot['positions_all_sides'],
            })
        if has_orders:
            write_event(account, 'orphan_exchange_open_orders', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'source': source,
                'exchange_snapshot': ord_res,
            })

    total_elapsed_ms = _perf_elapsed_ms(audit_started_perf)
    _log_perf_stage(
        'audit_consumer_orphan_exchange_activity',
        account=account,
        source=source,
        bar_bj=current_time_bj,
        total_elapsed_ms=total_elapsed_ms,
        input_symbols_count=len(symbols or []),
        seen_symbols_count=perf_seen_symbols,
        skipped_local_active_count=perf_skipped_local_active,
        findings_count=len(findings),
        precheck_elapsed_ms=perf_precheck_elapsed_ms,
        mark_position_elapsed_ms=perf_mark_position_elapsed_ms,
        mark_order_elapsed_ms=perf_mark_order_elapsed_ms,
    )
    return findings

def prepare_consumer_loop_gate_impl(
    account: str,
    strategy_cfg: dict[str, Any],
    live_cfg: dict[str, Any],
    maintain_fn,
    *,
    current_time_ms: int,
    current_time_bj: str,
    candidate_symbols: list[str],
    extra_reconcile_symbols: list[str],
    latest_closes: dict[str, float],
    exchange_activity_snapshot: dict[str, Any],
    source: str = 'loop',
) -> dict[str, Any]:
    loop_gate_started_perf = time.perf_counter()

    maintain_started_perf = time.perf_counter()
    maintain_res = maintain_fn(
        account,
        strategy_cfg,
        live_cfg,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        latest_closes=latest_closes,
        source=source,
        exchange_snapshot=exchange_activity_snapshot,
    )
    maintain_elapsed_ms = _perf_elapsed_ms(maintain_started_perf)

    scan_gate_started_perf = time.perf_counter()
    scan_gate = evaluate_consumer_signal_scan_gate_impl(
        account,
        live_cfg,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        candidate_symbols=candidate_symbols,
        extra_reconcile_symbols=extra_reconcile_symbols,
        latest_closes=latest_closes,
        exchange_activity_snapshot=exchange_activity_snapshot,
        maintain_res=maintain_res,
        source=source,
    )
    scan_gate_elapsed_ms = _perf_elapsed_ms(scan_gate_started_perf)

    active_symbols_started_perf = time.perf_counter()
    active_symbols = sorted(build_consumer_active_symbols(scan_gate)) if scan_gate.get('ok_to_scan') else []
    active_symbols_elapsed_ms = _perf_elapsed_ms(active_symbols_started_perf)

    total_elapsed_ms = _perf_elapsed_ms(loop_gate_started_perf)

    _log_perf_stage(
        'prepare_consumer_loop_gate',
        account=account,
        source=source,
        bar_bj=current_time_bj,
        maintain_elapsed_ms=maintain_elapsed_ms,
        scan_gate_elapsed_ms=scan_gate_elapsed_ms,
        active_symbols_elapsed_ms=active_symbols_elapsed_ms,
        total_elapsed_ms=total_elapsed_ms,
        candidate_symbols_count=len(candidate_symbols or []),
        extra_reconcile_symbols_count=len(extra_reconcile_symbols or []),
        latest_closes_symbols_count=len(latest_closes or {}),
        ok_to_scan=bool(scan_gate.get('ok_to_scan')),
        skip_reason=scan_gate.get('skip_reason'),
        active_symbols_count=len(active_symbols),
    )

    return {
        'ok_to_scan': bool(scan_gate.get('ok_to_scan')),
        'maintain_res': maintain_res,
        'scan_gate': scan_gate,
        'exchange_snapshot': dict(scan_gate.get('exchange_snapshot') or exchange_activity_snapshot or {}),
        'active_symbols': active_symbols,
        'local_active_symbols': list(scan_gate.get('local_active_symbols') or []),
        'cross_strategy_active_symbols': list(scan_gate.get('cross_strategy_active_symbols') or []),
        'exchange_activity_symbols': list(scan_gate.get('exchange_activity_symbols') or []),
    }

def evaluate_consumer_signal_scan_gate_impl(
    account: str,
    live_cfg: dict[str, Any],
    *,
    current_time_ms: int,
    current_time_bj: str,
    candidate_symbols: list[str],
    extra_reconcile_symbols: list[str],
    latest_closes: dict[str, float],
    exchange_activity_snapshot: dict[str, Any],
    maintain_res: dict[str, Any],
    source: str = 'loop',
) -> dict[str, Any]:
    scan_gate_started_perf = time.perf_counter()
    audit_enabled = bool(live_cfg.get('audit_enabled', True))
    precheck_scope = require_consumer_precheck_scope(live_cfg)
    strategy_concurrency_scope = require_consumer_strategy_concurrency_scope(live_cfg)
    snapshot = dict(exchange_activity_snapshot or {})
    if audit_enabled and not snapshot.get('ok'):
        write_event(account, 'exchange_activity_snapshot_error', {
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'source': source,
            'exchange_snapshot': {
                'positions': snapshot.get('positions'),
                'orders': snapshot.get('orders'),
            },
        })

    local_active_symbols_started_perf = time.perf_counter()
    local_activity_symbols = collect_consumer_local_activity_symbols(account)
    snapshot['local_active_symbols'] = sorted(
        set(snapshot.get('local_active_symbols') or [])
        | set(local_activity_symbols)
    )
    local_active_symbols_elapsed_ms = _perf_elapsed_ms(local_active_symbols_started_perf)

    local_active_symbols = sorted(set(snapshot.get('local_active_symbols') or []))
    cross_strategy_active_symbols = sorted(set(snapshot.get('cross_strategy_active_symbols') or []))
    exchange_activity_symbols = sorted(set(snapshot.get('symbols') or set()))
    orphan_audit_symbols = sorted(set(local_active_symbols) | set(exchange_activity_symbols))

    orphan_audit_started_perf = time.perf_counter()
    orphan_findings = audit_consumer_orphan_exchange_activity(
        account,
        orphan_audit_symbols,
        current_time_ms,
        current_time_bj,
        source=source,
        audit_enabled=audit_enabled,
        snapshot=snapshot,
        mark_reconcile=(precheck_scope == 'account_flat'),
    )
    orphan_audit_elapsed_ms = _perf_elapsed_ms(orphan_audit_started_perf)

    pending_reconcile_error = bool(maintain_res.get('pending_reconcile_error'))
    open_trade_reconcile_error = bool(maintain_res.get('open_trade_reconcile_error'))
    active_state_errors = list(maintain_res.get('active_state_errors') or [])
    required_reconcile_symbols = sorted(
        set(local_active_symbols)
        | (set(exchange_activity_symbols) if precheck_scope == 'account_flat' else set())
    )
    latest_close_symbols = {str(symbol).upper().strip() for symbol in (latest_closes or {}).keys()}
    missing_reconcile_symbols = [symbol for symbol in required_reconcile_symbols if symbol not in latest_close_symbols]

    total_elapsed_ms = _perf_elapsed_ms(scan_gate_started_perf)

    def _return(payload: dict[str, Any]) -> dict[str, Any]:
        payload.setdefault('cross_strategy_active_symbols', cross_strategy_active_symbols)
        _log_perf_stage(
            'evaluate_consumer_signal_scan_gate',
            account=account,
            source=source,
            bar_bj=current_time_bj,
            total_elapsed_ms=total_elapsed_ms,
            local_active_symbols_elapsed_ms=local_active_symbols_elapsed_ms,
            orphan_audit_elapsed_ms=orphan_audit_elapsed_ms,
            candidate_symbols_count=len(candidate_symbols or []),
            exchange_activity_symbols_count=len(exchange_activity_symbols),
            orphan_audit_symbols_count=len(orphan_audit_symbols),
            orphan_findings_count=len(orphan_findings),
            local_active_symbols_count=len(local_active_symbols),
            required_reconcile_symbols_count=len(required_reconcile_symbols),
            missing_reconcile_symbols_count=len(missing_reconcile_symbols),
            active_state_errors_count=len(active_state_errors),
            precheck_scope=precheck_scope,
            strategy_concurrency_scope=strategy_concurrency_scope,
            skip_reason=payload.get('skip_reason'),
            ok_to_scan=bool(payload.get('ok_to_scan')),
        )
        return payload

    if orphan_findings and precheck_scope == 'account_flat':
        if audit_enabled:
            write_event(account, 'signal_scan_skipped_orphan_exchange_activity', {
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'precheck_scope': precheck_scope,
                'strategy_concurrency_scope': strategy_concurrency_scope,
                'orphan_exchange_activity': orphan_findings,
                'candidate_symbols_count': len(candidate_symbols or []),
                'extra_reconcile_symbols_count': len(extra_reconcile_symbols or []),
            })
        return _return({
            'ok_to_scan': False,
            'skip_reason': 'orphan_exchange_activity',
            'orphan_findings': orphan_findings,
            'missing_reconcile_symbols': missing_reconcile_symbols,
            'active_state_errors': active_state_errors,
            'exchange_snapshot': snapshot,
            'local_active_symbols': local_active_symbols,
            'exchange_activity_symbols': exchange_activity_symbols,
            'precheck_scope': precheck_scope,
            'strategy_concurrency_scope': strategy_concurrency_scope,
        })

    if strategy_concurrency_scope == 'account' and local_active_symbols:
        if audit_enabled:
            write_event(account, 'signal_scan_skipped_strategy_account_active', {
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'precheck_scope': precheck_scope,
                'strategy_concurrency_scope': strategy_concurrency_scope,
                'local_active_symbols': local_active_symbols,
                'candidate_symbols_count': len(candidate_symbols or []),
                'extra_reconcile_symbols_count': len(extra_reconcile_symbols or []),
            })
        return _return({
            'ok_to_scan': False,
            'skip_reason': 'strategy_account_active',
            'orphan_findings': orphan_findings,
            'missing_reconcile_symbols': missing_reconcile_symbols,
            'active_state_errors': active_state_errors,
            'exchange_snapshot': snapshot,
            'local_active_symbols': local_active_symbols,
            'exchange_activity_symbols': exchange_activity_symbols,
            'precheck_scope': precheck_scope,
            'strategy_concurrency_scope': strategy_concurrency_scope,
        })

    if pending_reconcile_error or open_trade_reconcile_error:
        if audit_enabled:
            write_event(account, 'signal_scan_skipped_reconcile_query_error', {
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'precheck_scope': precheck_scope,
                'strategy_concurrency_scope': strategy_concurrency_scope,
                'pending_reconcile_error': pending_reconcile_error,
                'open_trade_reconcile_error': open_trade_reconcile_error,
                'candidate_symbols_count': len(candidate_symbols or []),
                'extra_reconcile_symbols_count': len(extra_reconcile_symbols or []),
            })
        return _return({
            'ok_to_scan': False,
            'skip_reason': 'reconcile_query_error',
            'orphan_findings': orphan_findings,
            'missing_reconcile_symbols': missing_reconcile_symbols,
            'active_state_errors': active_state_errors,
            'exchange_snapshot': snapshot,
            'local_active_symbols': local_active_symbols,
            'exchange_activity_symbols': exchange_activity_symbols,
            'precheck_scope': precheck_scope,
            'strategy_concurrency_scope': strategy_concurrency_scope,
        })

    if missing_reconcile_symbols:
        if audit_enabled:
            write_event(account, 'signal_scan_skipped_missing_reconcile_data', {
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'precheck_scope': precheck_scope,
                'strategy_concurrency_scope': strategy_concurrency_scope,
                'missing_reconcile_symbols': missing_reconcile_symbols,
                'candidate_symbols_count': len(candidate_symbols or []),
                'extra_reconcile_symbols_count': len(extra_reconcile_symbols or []),
            })
        return _return({
            'ok_to_scan': False,
            'skip_reason': 'missing_reconcile_data',
            'orphan_findings': orphan_findings,
            'missing_reconcile_symbols': missing_reconcile_symbols,
            'active_state_errors': active_state_errors,
            'exchange_snapshot': snapshot,
            'local_active_symbols': local_active_symbols,
            'exchange_activity_symbols': exchange_activity_symbols,
            'precheck_scope': precheck_scope,
            'strategy_concurrency_scope': strategy_concurrency_scope,
        })

    if not snapshot.get('ok'):
        if audit_enabled:
            write_event(account, 'signal_scan_skipped_exchange_activity_query_error', {
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'precheck_scope': precheck_scope,
                'strategy_concurrency_scope': strategy_concurrency_scope,
                'candidate_symbols_count': len(candidate_symbols or []),
                'extra_reconcile_symbols_count': len(extra_reconcile_symbols or []),
                'exchange_snapshot': {
                    'positions': snapshot.get('positions'),
                    'orders': snapshot.get('orders'),
                },
            })
        return _return({
            'ok_to_scan': False,
            'skip_reason': 'exchange_activity_query_error',
            'orphan_findings': orphan_findings,
            'missing_reconcile_symbols': missing_reconcile_symbols,
            'active_state_errors': active_state_errors,
            'exchange_snapshot': snapshot,
            'local_active_symbols': local_active_symbols,
            'exchange_activity_symbols': exchange_activity_symbols,
            'precheck_scope': precheck_scope,
            'strategy_concurrency_scope': strategy_concurrency_scope,
        })

    if active_state_errors:
        if audit_enabled:
            write_event(account, 'signal_scan_skipped_active_state_error', {
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'precheck_scope': precheck_scope,
                'strategy_concurrency_scope': strategy_concurrency_scope,
                'active_state_errors': active_state_errors,
                'candidate_symbols_count': len(candidate_symbols or []),
                'extra_reconcile_symbols_count': len(extra_reconcile_symbols or []),
            })
        return _return({
            'ok_to_scan': False,
            'skip_reason': 'active_state_error',
            'orphan_findings': orphan_findings,
            'missing_reconcile_symbols': missing_reconcile_symbols,
            'active_state_errors': active_state_errors,
            'exchange_snapshot': snapshot,
            'local_active_symbols': local_active_symbols,
            'exchange_activity_symbols': exchange_activity_symbols,
            'precheck_scope': precheck_scope,
            'strategy_concurrency_scope': strategy_concurrency_scope,
        })

    return _return({
        'ok_to_scan': True,
        'skip_reason': '',
        'orphan_findings': orphan_findings,
        'missing_reconcile_symbols': missing_reconcile_symbols,
        'active_state_errors': active_state_errors,
        'exchange_snapshot': snapshot,
        'local_active_symbols': local_active_symbols,
        'cross_strategy_active_symbols': cross_strategy_active_symbols,
        'exchange_activity_symbols': exchange_activity_symbols,
        'precheck_scope': precheck_scope,
        'strategy_concurrency_scope': strategy_concurrency_scope,
    })


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')


def _now_bj_str() -> str:
    return datetime.now(timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')


def bootstrap_consumer_impl(
    account: str,
    strategy_cfg: dict[str, Any],
    live_cfg: dict[str, Any],
    maintain_fn,
    *,
    source: str = 'startup',
    current_time_ms: int | None = None,
    current_time_bj: str | None = None,
    exchange_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if current_time_ms is None:
        current_time_ms = int(time.time() * 1000)
    if current_time_bj is None:
        current_time_bj = _fmt_bj_from_ms(current_time_ms) or _now_bj_str()

    snapshot = dict(exchange_snapshot) if exchange_snapshot is not None else collect_consumer_exchange_activity_snapshot(account)
    raw_symbols = snapshot.get('symbols') or set()
    symbols = {str(symbol).upper().strip() for symbol in raw_symbols if str(symbol).strip()}
    snapshot['symbols'] = symbols
    local_active_symbols = sorted(collect_consumer_local_activity_symbols(account))
    snapshot['local_active_symbols'] = local_active_symbols

    maintain_res = maintain_fn(
        account,
        strategy_cfg,
        live_cfg,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        latest_closes={},
        source=source,
        exchange_snapshot=snapshot,
    )
    pending_reconcile_error = bool(maintain_res.get('pending_reconcile_error'))
    open_trade_reconcile_error = bool(maintain_res.get('open_trade_reconcile_error'))
    active_state_errors = list(maintain_res.get('active_state_errors') or [])
    blocking = bool(
        pending_reconcile_error
        or open_trade_reconcile_error
        or (not snapshot.get('ok'))
        or active_state_errors
    )
    return {
        'ok': not blocking,
        'blocking': blocking,
        'bar_ts': current_time_ms,
        'bar_bj': current_time_bj,
        'pending_reconcile_error': pending_reconcile_error,
        'open_trade_reconcile_error': open_trade_reconcile_error,
        'exchange_activity_snapshot_ok': bool(snapshot.get('ok')),
        'exchange_snapshot': snapshot,
        'exchange_symbols': sorted(symbols),
        'local_active_symbols': local_active_symbols,
        'touched_symbols': list(maintain_res.get('touched_symbols') or []),
        'pending_symbols': list(maintain_res.get('pending_symbols') or []),
        'open_symbols': list(maintain_res.get('open_symbols') or []),
        'active_state_errors': active_state_errors,
    }


def bootstrap_consumer_gate_impl(
    account: str,
    strategy_cfg: dict[str, Any],
    live_cfg: dict[str, Any],
    maintain_fn,
    *,
    candidate_symbols: list[str],
    source: str = 'startup',
) -> dict[str, Any]:
    audit_enabled = bool(live_cfg.get('audit_enabled', True))
    precheck_scope = require_consumer_precheck_scope(live_cfg)
    strategy_concurrency_scope = require_consumer_strategy_concurrency_scope(live_cfg)
    bootstrap_res = bootstrap_consumer_impl(
        account,
        strategy_cfg,
        live_cfg,
        maintain_fn,
        source=source,
    )
    current_time_ms = int(bootstrap_res['bar_ts'])
    current_time_bj = str(bootstrap_res['bar_bj'])
    startup_snapshot = dict(bootstrap_res.get('exchange_snapshot') or {})
    startup_symbols = sorted(
        set(bootstrap_res.get('local_active_symbols') or [])
        | set(startup_snapshot.get('symbols') or set())
    )
    startup_snapshot['startup_symbols'] = startup_symbols

    if audit_enabled and not bootstrap_res.get('exchange_activity_snapshot_ok'):
        write_event(account, 'exchange_activity_snapshot_error', {
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'source': source,
            'exchange_snapshot': {
                'positions': startup_snapshot.get('positions'),
                'orders': startup_snapshot.get('orders'),
            },
        })

    orphan_findings = audit_consumer_orphan_exchange_activity(
        account,
        startup_symbols,
        current_time_ms,
        current_time_bj,
        source=source,
        audit_enabled=audit_enabled,
        snapshot=startup_snapshot,
        mark_reconcile=(precheck_scope == 'account_flat'),
    )
    active_state_errors = list(bootstrap_res.get('active_state_errors') or [])
    blocking_orphan_findings = orphan_findings if precheck_scope == 'account_flat' else []
    blocking = bool(bootstrap_res.get('blocking') or blocking_orphan_findings)
    return {
        'blocking': blocking,
        'bar_ts': current_time_ms,
        'bar_bj': current_time_bj,
        'precheck_scope': precheck_scope,
        'strategy_concurrency_scope': strategy_concurrency_scope,
        'pending_reconcile_error': bool(bootstrap_res.get('pending_reconcile_error')),
        'open_trade_reconcile_error': bool(bootstrap_res.get('open_trade_reconcile_error')),
        'exchange_activity_snapshot_ok': bool(bootstrap_res.get('exchange_activity_snapshot_ok')),
        'orphan_findings': orphan_findings,
        'blocking_orphan_findings': blocking_orphan_findings,
        'active_state_errors': active_state_errors,
        'exchange_snapshot': startup_snapshot,
        'local_active_symbols': list(bootstrap_res.get('local_active_symbols') or []),
        'exchange_symbols': list(bootstrap_res.get('exchange_symbols') or []),
    }


def finalize_consumer_scan_skip_impl(
    account: str,
    *,
    current_time_ms: int,
    current_time_bj: str,
    symbols: list[str] | set[str] | tuple[str, ...],
    mark_last_processed_bar_fn,
) -> dict[str, Any]:
    processed_symbols = sorted({
        str(symbol).upper().strip()
        for symbol in (symbols or [])
        if str(symbol).strip()
    })
    for symbol in processed_symbols:
        mark_last_processed_bar_fn(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
    return {
        'processed_symbols': processed_symbols,
        'processed_count': len(processed_symbols),
    }


def finalize_consumer_no_candidate_data_impl(
    account: str,
    *,
    current_time_ms: int,
    current_time_bj: str,
    symbols: list[str] | set[str] | tuple[str, ...],
    candidate_reason: str | None,
    candidate_errors: Any,
    extra_reconcile_symbols_count: int,
    audit_enabled: bool,
    mark_last_processed_bar_fn,
) -> dict[str, Any]:
    if audit_enabled:
        write_event(account, 'signal_scan_skipped_no_candidate_data', {
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'candidate_reason': candidate_reason,
            'candidate_errors': candidate_errors,
            'extra_reconcile_symbols_count': int(extra_reconcile_symbols_count),
        })
    result = finalize_consumer_scan_skip_impl(
        account,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        symbols=symbols,
        mark_last_processed_bar_fn=mark_last_processed_bar_fn,
    )
    result.update({
        'skip_reason': 'no_candidate_data',
        'candidate_reason': candidate_reason,
    })
    return result


def finalize_consumer_signal_none_impl(
    account: str,
    *,
    current_time_ms: int,
    current_time_bj: str,
    symbols: list[str] | set[str] | tuple[str, ...],
    candidate_payload: dict[str, Any],
    extra_reconcile_symbols_count: int,
    timing_fields: dict[str, Any],
    signal_eval_started_utc_ms: int | None,
    signal_eval_finished_utc_ms: int | None,
    audit_enabled: bool,
    mark_last_processed_bar_fn,
    write_stage_record_fn,
    fmt_bj_from_ms_fn,
) -> dict[str, Any]:
    payload = {
        'bar_ts': current_time_ms,
        'bar_bj': current_time_bj,
        'freshest_bar_ts': candidate_payload.get('freshest_bar_ts'),
        'freshest_bar_bj': candidate_payload.get('freshest_bar_bj'),
        'stale_cutoff_bj': candidate_payload.get('stale_cutoff_bj'),
        'symbol_count': candidate_payload['symbol_count'],
        'stale_symbol_count': candidate_payload.get('stale_symbol_count', 0),
        'extra_reconcile_symbols_count': int(extra_reconcile_symbols_count),
    }
    if audit_enabled:
        write_event(account, 'signal_none', payload)
        write_stage_record_fn(account, 'stage6_signal', {
            **payload,
            'event': 'signal_none',
            'selected_symbol': None,
            'signal_digest': None,
            **timing_fields,
            'signal_eval_started_utc_ms': signal_eval_started_utc_ms,
            'signal_eval_started_bj': fmt_bj_from_ms_fn(signal_eval_started_utc_ms),
            'signal_eval_finished_utc_ms': signal_eval_finished_utc_ms,
            'signal_eval_finished_bj': fmt_bj_from_ms_fn(signal_eval_finished_utc_ms),
        })
    result = finalize_consumer_scan_skip_impl(
        account,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        symbols=symbols,
        mark_last_processed_bar_fn=mark_last_processed_bar_fn,
    )
    result.update({
        'skip_reason': 'signal_none',
    })
    return result


def finalize_consumer_loop_state_impl(
    account: str,
    *,
    mode: str,
    current_time_ms: int,
    current_time_bj: str,
    symbols: list[str] | set[str] | tuple[str, ...],
    audit_enabled: bool,
    scan_gate: dict[str, Any] | None = None,
    candidate_payload: dict[str, Any] | None = None,
    candidate_reason: str | None = None,
    candidate_errors: Any = None,
    extra_reconcile_symbols_count: int = 0,
    timing_fields: dict[str, Any] | None = None,
    signal_eval_started_utc_ms: int | None = None,
    signal_eval_finished_utc_ms: int | None = None,
    mark_last_processed_bar_fn=None,
    write_stage_record_fn=None,
    fmt_bj_from_ms_fn=None,
) -> dict[str, Any]:
    if mode == 'scan_blocked':
        result = finalize_consumer_scan_skip_impl(
            account,
            current_time_ms=current_time_ms,
            current_time_bj=current_time_bj,
            symbols=symbols,
            mark_last_processed_bar_fn=mark_last_processed_bar_fn,
        )
        if scan_gate is not None:
            result['skip_reason'] = str(scan_gate.get('skip_reason') or 'scan_gate_blocked')
        return result
    if mode == 'no_candidate_data':
        return finalize_consumer_no_candidate_data_impl(
            account,
            current_time_ms=current_time_ms,
            current_time_bj=current_time_bj,
            symbols=symbols,
            candidate_reason=candidate_reason,
            candidate_errors=candidate_errors,
            extra_reconcile_symbols_count=extra_reconcile_symbols_count,
            audit_enabled=audit_enabled,
            mark_last_processed_bar_fn=mark_last_processed_bar_fn,
        )
    if mode == 'signal_none':
        if candidate_payload is None:
            raise ValueError('candidate_payload is required when mode=signal_none')
        return finalize_consumer_signal_none_impl(
            account,
            current_time_ms=current_time_ms,
            current_time_bj=current_time_bj,
            symbols=symbols,
            candidate_payload=candidate_payload,
            extra_reconcile_symbols_count=extra_reconcile_symbols_count,
            timing_fields=timing_fields or {},
            signal_eval_started_utc_ms=signal_eval_started_utc_ms,
            signal_eval_finished_utc_ms=signal_eval_finished_utc_ms,
            audit_enabled=audit_enabled,
            mark_last_processed_bar_fn=mark_last_processed_bar_fn,
            write_stage_record_fn=write_stage_record_fn,
            fmt_bj_from_ms_fn=fmt_bj_from_ms_fn,
        )
    raise ValueError(f'unsupported finalize mode: {mode}')


def maintain_consumer_once_impl(
    account: str,
    strategy_cfg: dict[str, Any],
    live_cfg: dict[str, Any],
    *,
    current_time_ms: int,
    current_time_bj: str,
    latest_closes: dict[str, float],
    source: str,
    exchange_snapshot: dict[str, Any] | None = None,
    reconcile_pending_entries_fn,
    reconcile_open_trades_fn,
    extract_time_stop_config_fn,
) -> dict[str, Any]:
    maintain_started_perf = time.perf_counter()

    pending_started_perf = time.perf_counter()
    pending_reconcile_error = reconcile_pending_entries_fn(
        account,
        live_cfg,
        current_time_ms,
        current_time_bj,
        source=source,
        snapshot=exchange_snapshot,
    )
    pending_elapsed_ms = _perf_elapsed_ms(pending_started_perf)

    max_hold_mins, min_profit_pct = extract_time_stop_config_fn(strategy_cfg)

    open_started_perf = time.perf_counter()
    open_trade_reconcile_error = reconcile_open_trades_fn(
        account,
        live_cfg,
        current_time_ms,
        current_time_bj,
        latest_closes,
        max_hold_mins,
        min_profit_pct,
        source=source,
        snapshot=exchange_snapshot,
    )
    open_elapsed_ms = _perf_elapsed_ms(open_started_perf)

    state_summary = collect_consumer_state_summary(account)
    touched_symbols = sorted(set(state_summary['pending_symbols']) | set(state_summary['open_symbols']))
    total_elapsed_ms = _perf_elapsed_ms(maintain_started_perf)

    _log_perf_stage(
        'maintain_consumer_once',
        account=account,
        source=source,
        bar_bj=current_time_bj,
        pending_elapsed_ms=pending_elapsed_ms,
        open_elapsed_ms=open_elapsed_ms,
        total_elapsed_ms=total_elapsed_ms,
        touched_symbols_count=len(touched_symbols),
        pending_symbols_count=len(state_summary['pending_symbols']),
        open_symbols_count=len(state_summary['open_symbols']),
        active_state_errors_count=len(state_summary['active_state_errors']),
        latest_closes_symbols_count=len(latest_closes or {}),
        pending_reconcile_error=pending_reconcile_error,
        open_trade_reconcile_error=open_trade_reconcile_error,
    )

    return {
        'ok': not (pending_reconcile_error or open_trade_reconcile_error),
        'blocking': bool(pending_reconcile_error or open_trade_reconcile_error),
        'pending_reconcile_error': pending_reconcile_error,
        'open_trade_reconcile_error': open_trade_reconcile_error,
        'touched_symbols': touched_symbols,
        'pending_symbols': state_summary['pending_symbols'],
        'open_symbols': state_summary['open_symbols'],
        'active_state_errors': state_summary['active_state_errors'],
        'latest_closes_symbols': sorted(set((latest_closes or {}).keys())),
    }


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
    cross_strategy_active_symbols: set[str] = set()
    for symbol in candidate_symbol_set | exchange_activity_symbols | local_active_symbols:
        activity = collect_account_symbol_strategy_activity(
            account,
            symbol,
            exclude_strategy_name='snapback',
        )
        if activity.get('blocked'):
            cross_strategy_active_symbols.add(symbol)
    snapshot['symbols'] = exchange_activity_symbols
    snapshot['local_active_symbols'] = sorted(local_active_symbols)
    snapshot['cross_strategy_active_symbols'] = sorted(cross_strategy_active_symbols)
    return {
        'exchange_snapshot': snapshot,
        'exchange_activity_symbols': sorted(exchange_activity_symbols),
        'local_active_symbols': sorted(local_active_symbols),
        'cross_strategy_active_symbols': sorted(cross_strategy_active_symbols),
        'extra_reconcile_symbols': sorted((exchange_activity_symbols | local_active_symbols | cross_strategy_active_symbols) - candidate_symbol_set),
    }


def build_consumer_active_symbols(scan_gate: dict[str, Any]) -> set[str]:
    return {
        str(symbol).upper().strip()
        for symbol in (
            list(scan_gate.get('local_active_symbols') or [])
            + list(scan_gate.get('cross_strategy_active_symbols') or [])
            + list(scan_gate.get('exchange_activity_symbols') or [])
        )
        if str(symbol).strip()
    }
def collect_consumer_state_summary(account: str) -> dict[str, Any]:
    state = load_live_state(account)
    pending_symbols: list[str] = []
    open_symbols: list[str] = []
    active_state_errors: list[dict[str, Any]] = []
    for raw_symbol, payload in (state.get('symbols') or {}).items():
        if not isinstance(payload, dict):
            continue
        symbol = str(raw_symbol).upper().strip()
        if not symbol:
            continue
        has_pending_entry = bool(payload.get('pending_entry_order'))
        has_open_trade = bool(payload.get('open_trade'))
        if has_pending_entry:
            pending_symbols.append(symbol)
        if has_open_trade:
            open_symbols.append(symbol)
        error_code = payload.get('last_error_code')
        error_message = payload.get('last_error_message')
        error_bj = payload.get('last_error_bj')
        if (error_code or error_message) and (has_pending_entry or has_open_trade):
            active_state_errors.append({
                'symbol': symbol,
                'last_error_code': error_code,
                'last_error_message': error_message,
                'last_error_bj': error_bj,
            })
    return {
        'pending_symbols': sorted(set(pending_symbols)),
        'open_symbols': sorted(set(open_symbols)),
        'active_state_errors': sorted(active_state_errors, key=lambda x: (str(x.get('symbol') or ''), str(x.get('last_error_code') or ''))),
    }


def collect_consumer_active_state_errors(account: str) -> list[dict[str, Any]]:
    return list(collect_consumer_state_summary(account)['active_state_errors'])
