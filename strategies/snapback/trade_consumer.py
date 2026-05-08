from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from core.live.audit_log import append_stage_record, write_event
from core.live.binance_exec import (
    cancel_order,
    ensure_leverage,
    get_last_price,
    get_open_orders,
    get_order,
    get_position,
    place_entry_order,
    place_sl_order,
    place_time_stop_order,
    place_tp_order,
    resolve_order_fill_price,
)
from core.live.custom_id import BROKER_ID, build_client_order_id, make_order_root, parse_client_order_id
from core.live.live_state import (
    load_live_state,
    load_symbol_state,
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
from strategies.snapback.current_ledger import (
    audit_consumer_orphan_exchange_activity,
    bootstrap_consumer_gate_impl as _ledger_bootstrap_consumer_gate_impl,
    bootstrap_consumer_impl as _ledger_bootstrap_consumer_impl,
    build_consumer_active_symbols,
    build_consumer_reconcile_plan,
    collect_consumer_active_state_errors,
    collect_consumer_exchange_activity_snapshot,
    collect_consumer_local_activity_symbols,
    collect_consumer_state_summary,
    evaluate_consumer_signal_scan_gate_impl as _ledger_evaluate_consumer_signal_scan_gate_impl,
    finalize_consumer_loop_state_impl as _ledger_finalize_consumer_loop_state_impl,
    finalize_consumer_no_candidate_data_impl as _ledger_finalize_consumer_no_candidate_data_impl,
    finalize_consumer_scan_skip_impl as _ledger_finalize_consumer_scan_skip_impl,
    finalize_consumer_signal_none_impl as _ledger_finalize_consumer_signal_none_impl,
    has_position_or_orders,
    maintain_consumer_once_impl as _ledger_maintain_consumer_once_impl,
    precheck_exchange_account_flat_blockers,
    precheck_exchange_blockers,
    prepare_consumer_loop_gate_impl as _ledger_prepare_consumer_loop_gate_impl,
    require_consumer_precheck_scope,
)

BJ = timezone(timedelta(hours=8))
FIXED_POSITION_SIDE = 'LONG'
STRAT_CODE = 'SNP'
LEG_ENTRY = 'EN'
LEG_TP = 'TP'
LEG_SL = 'SL'
LEG_TIME_STOP = 'TS'
LEG_SL_FAIL_FLATTEN = 'SF'
EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN = 'SL_SUBMIT_FAILED_FLATTEN'
TERMINAL_ORDER_STATUSES = {'FILLED', 'FINISHED', 'CANCELED', 'CANCELLED', 'EXPIRED', 'REJECTED'}
FILLED_ORDER_STATUSES = {'FILLED', 'FINISHED'}

def _exit_order_leg(exit_reason: str) -> str | None:
    if exit_reason == 'TAKE_PROFIT':
        return 'TP'
    if exit_reason == 'STOP_LOSS':
        return 'SL'
    if exit_reason == 'TIME_STOP':
        return 'TS'
    if exit_reason == EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN:
        return LEG_SL_FAIL_FLATTEN
    return None

def _time_stop_exit_reason(open_trade: dict[str, Any]) -> str:
    raw_reason = str(
        open_trade.get('time_stop_exit_reason')
        or open_trade.get('protective_flatten_exit_reason')
        or ''
    ).upper().strip()
    if raw_reason == EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN:
        return EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN
    parsed = parse_client_order_id(open_trade.get('time_stop_client_order_id'), broker_id=BROKER_ID)
    if parsed.get('recognized') and str(parsed.get('leg') or '').upper().strip() == LEG_SL_FAIL_FLATTEN:
        return EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN
    return 'TIME_STOP'

def _now_bj_str() -> str:
    return datetime.now(timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')

def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')

def _cooldown_until(current_time_ms: int, cooldown_mins: int) -> tuple[int, str | None]:
    cooldown_until_ts = int(current_time_ms) + int(cooldown_mins) * 60 * 1000
    return cooldown_until_ts, _fmt_bj_from_ms(cooldown_until_ts)

def _notify(enabled: bool, message: str, label: str = 'snapback') -> None:
    if enabled:
        send_to_bot(message, label=label)

def _json_default(v: Any) -> Any:
    if hasattr(v, 'item'):
        return v.item()
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, Path):
        return str(v)
    if isinstance(v, set):
        return sorted(v)
    if isinstance(v, tuple):
        return list(v)
    raise TypeError(f'Object of type {type(v).__name__} is not JSON serializable')

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

LIVE_PROJECTION_DIR = 'output/live_projection'

def _projection_schema_version(live_cfg: dict[str, Any]) -> int:
    try:
        return int(live_cfg.get('_projection_schema_version') or 1)
    except Exception:
        return 1

def _projection_run_id(live_cfg: dict[str, Any]) -> str:
    return str(live_cfg.get('_projection_run_id') or 'UNSET').strip() or 'UNSET'

def _projection_dir(live_cfg: dict[str, Any]) -> Path:
    raw = str(live_cfg.get('_projection_output_dir') or LIVE_PROJECTION_DIR).strip() or LIVE_PROJECTION_DIR
    return Path(raw)

def _projection_path(live_cfg: dict[str, Any], kind: str) -> Path:
    run_id = _projection_run_id(live_cfg)
    return _projection_dir(live_cfg) / f'{kind}.{run_id}.jsonl'

def _json_roundtrip(data: Any) -> Any:
    return json.loads(_json_safe_dumps(data))

def _append_projection_row(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('a', encoding='utf-8') as f:
        f.write(_json_safe_dumps(row) + '\n')

def _signal_c_time_ms(signal: dict[str, Any]) -> int | None:
    context = signal.get('context') if isinstance(signal.get('context'), dict) else {}
    try:
        value = context.get('c_time')
        if value in (None, ''):
            return None
        return int(value)
    except Exception:
        return None

def _signal_selected_tp_pct(signal: dict[str, Any]) -> float | None:
    params = signal.get('params') if isinstance(signal.get('params'), dict) else {}
    context = signal.get('context') if isinstance(signal.get('context'), dict) else {}
    for raw in (params.get('selected_take_profit_pct'), params.get('selected_tp_pct'), context.get('selected_tp_pct')):
        try:
            value = float(raw)
        except Exception:
            continue
        if value > 0:
            return value
    return None

def _signal_tp_tier(signal: dict[str, Any]) -> str | None:
    context = signal.get('context') if isinstance(signal.get('context'), dict) else {}
    value = context.get('tp_tier')
    if value in (None, ''):
        return None
    return str(value)

def _fmt_notify_price(value: Any) -> str:
    try:
        px = float(value)
    except Exception:
        return 'NA'
    if px <= 0:
        return 'NA'
    return f'{px:.6f}'

def _fmt_notify_hms_from_ms(ts_ms: Any) -> str:
    try:
        value = int(ts_ms)
    except Exception:
        return 'UNKNOWN'
    if value <= 0:
        return 'UNKNOWN'
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).astimezone(BJ).strftime('%H:%M:%S')

def _snapback_notify_header(account: str, event_time_ms: Any) -> str:
    return f'[{_fmt_notify_hms_from_ms(event_time_ms)} 🦅 {STRAT_CODE}] {account}'

def _fmt_notify_pct(value: Any) -> str:
    try:
        pct = float(value) * 100.0
    except Exception:
        return 'NA'
    return f'{pct:.2f}%'

def _build_signal_locked_message(account: str, signal: dict[str, Any]) -> str:
    context = signal.get('context') if isinstance(signal.get('context'), dict) else {}
    symbol = str(signal.get('symbol') or '').upper().strip()
    current_price = _fmt_notify_price(signal.get('current_price'))
    drop_pct = _fmt_notify_pct(context.get('drop_pct'))
    vol_ratio = context.get('vol_ratio')
    try:
        vol_ratio_text = f'{float(vol_ratio):.2f}'
    except Exception:
        vol_ratio_text = 'NA'
    rebound_ratio = _fmt_notify_pct(context.get('rebound_ratio'))
    tp_tier = _signal_tp_tier(signal) or 'UNKNOWN'
    selected_tp_pct = _signal_selected_tp_pct(signal)
    tp_tier_text = f'{tp_tier}({_fmt_notify_pct(selected_tp_pct)})' if selected_tp_pct is not None else tp_tier
    return '\n'.join([
        _snapback_notify_header(account, signal.get('signal_time')),
        f'雷达锁定: {symbol}',
        f'当前价: {current_price}',
        f'15m跌幅: {drop_pct}',
        f'爆量倍数: {vol_ratio_text}',
        f'ABC反弹比例: {rebound_ratio}',
        f'TP档位: {tp_tier_text}',
    ])

def _fmt_notify_hold_mins(trade_row: dict[str, Any] | None = None) -> str:
    row = trade_row or {}
    try:
        entry_time_ms = int(row.get('entry_time') or 0)
        exit_time_ms = int(row.get('exit_time') or 0)
    except Exception:
        return 'NA'
    if entry_time_ms <= 0 or exit_time_ms <= 0 or exit_time_ms < entry_time_ms:
        return 'NA'
    return f'{(exit_time_ms - entry_time_ms) / 60000.0:.1f}m'

def _build_exit_detected_message(
    *,
    account: str,
    symbol: str,
    exit_reason: str,
    order_root: str | None,
    trade_row: dict[str, Any] | None = None,
) -> str:
    row = trade_row or {}
    entry_price = _fmt_notify_price(row.get('entry_price'))
    exit_price = _fmt_notify_price(row.get('exit_price'))
    hold_mins = _fmt_notify_hold_mins(row)
    pnl_pct = _fmt_notify_pct(row.get('pnl_pct'))
    reason_text = str(exit_reason or 'UNKNOWN_EXIT')
    return '\n'.join([
        _snapback_notify_header(account, row.get('exit_time')),
        f'离场 {str(symbol).upper().strip()}',
        f'reason={reason_text}',
        f'entry≈{entry_price}',
        f'exit≈{exit_price}',
        f'持仓={hold_mins}',
        f'pnl={pnl_pct}',
    ])

def _build_entry_confirmed_message(account: str, *, symbol: str, open_trade: dict[str, Any], fallback_entry_price: Any) -> str:
    tp_px = float(open_trade.get('tp_price') or 0.0)
    sl_px = float(open_trade.get('sl_trigger_price') or 0.0)
    entry_px = float(open_trade.get('entry_price') or fallback_entry_price or 0.0)
    return '\n'.join([
        _snapback_notify_header(account, open_trade.get('entry_submit_finished_utc_ms') or open_trade.get('entry_ts')),
        f'开仓 {str(symbol).upper().strip()}',
        f'entry≈{entry_px:.6f}',
        f'TP={tp_px:.6f}',
        f'SL={sl_px:.6f}',
    ])

def _notify_signal_locked(account: str, live_cfg: dict[str, Any], signal: dict[str, Any]) -> None:
    if not bool(live_cfg.get('notify_enabled', False)):
        return
    if not bool(live_cfg.get('notify_on_signal_locked', True)):
        return
    _notify(True, _build_signal_locked_message(account, signal))

def _emit_exit_detected(account: str, live_cfg: dict[str, Any], *, symbol: str, exit_reason: str, order_root: str | None, trade_row: dict[str, Any] | None = None) -> None:
    message = _build_exit_detected_message(
        account=account,
        symbol=symbol,
        exit_reason=exit_reason,
        order_root=order_root,
        trade_row=trade_row,
    )
    logging.info(message)
    if bool(live_cfg.get('notify_enabled', False)) and bool(live_cfg.get('notify_on_exit_detected', True)):
        _notify(True, message)

def _signal_core_fields(signal: dict[str, Any], *, fallback_time_ms: int | None = None) -> dict[str, Any]:
    signal_time = int(signal.get('signal_time') or fallback_time_ms or 0)
    signal_time_bj = signal.get('signal_time_bj') or _fmt_bj_from_ms(signal_time)
    c_time = _signal_c_time_ms(signal)
    return {
        'signal_time': signal_time,
        'signal_time_bj': signal_time_bj,
        'symbol': str(signal.get('symbol') or '').upper().strip(),
        'action': signal.get('action'),
        'current_price': _normalize_scalar(signal.get('current_price')),
        'tp_price': _normalize_scalar(signal.get('tp_price')),
        'sl_price': _normalize_scalar(signal.get('sl_price')),
        'params': _json_roundtrip(signal.get('params') or {}),
        'context': _json_roundtrip(signal.get('context') or {}),
        'c_time': c_time,
        'c_time_bj': _fmt_bj_from_ms(c_time),
        'selected_tp_pct': _normalize_scalar(_signal_selected_tp_pct(signal)),
        'tp_tier': _signal_tp_tier(signal),
    }

def append_live_signal_projection(
    account: str,
    live_cfg: dict[str, Any],
    *,
    signal: dict[str, Any],
    current_time_ms: int,
    current_time_bj: str,
    c_bar_ts: int | None,
    c_bar_bj: str | None,
    source: str,
    timing_fields: dict[str, Any] | None = None,
    signal_eval_started_utc_ms: int | None = None,
    signal_eval_finished_utc_ms: int | None = None,
) -> dict[str, Any]:
    try:
        row = {
            **_signal_core_fields(signal, fallback_time_ms=current_time_ms),
            'run_mode': 'live',
            'projection_type': 'live_signal',
            'projection_schema_version': _projection_schema_version(live_cfg),
            'strategy_name': 'snapback',
            'account': account,
            'run_id': _projection_run_id(live_cfg),
            'source': source,
            'logic_selected': True,
            'signal_digest': _signal_digest(signal),
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'c_bar_ts': c_bar_ts,
            'c_bar_bj': c_bar_bj,
            'signal_eval_started_utc_ms': signal_eval_started_utc_ms,
            'signal_eval_started_bj': _fmt_bj_from_ms(signal_eval_started_utc_ms),
            'signal_eval_finished_utc_ms': signal_eval_finished_utc_ms,
            'signal_eval_finished_bj': _fmt_bj_from_ms(signal_eval_finished_utc_ms),
        }
        if timing_fields:
            row['timing'] = _json_roundtrip(timing_fields)
        path = _projection_path(live_cfg, 'live_signals')
        _append_projection_row(path, row)
        return {'ok': True, 'path': str(path), 'row': row}
    except Exception as e:
        return {'ok': False, 'reason': str(e), 'path': str(_projection_path(live_cfg, 'live_signals'))}

def _extract_order_event_ts_ms(order_row: dict[str, Any] | None, *, fallback_ms: int | None = None) -> int | None:
    row = order_row or {}
    raw = row.get('raw') if isinstance(row.get('raw'), dict) else {}
    for key in ('updateTime', 'time', 'transactTime', 'workingTime', 'createTime', 'executedTime'):
        value = raw.get(key)
        try:
            ts_ms = int(value)
        except Exception:
            ts_ms = None
        if ts_ms and ts_ms > 0:
            return ts_ms
    return fallback_ms

def _resolve_exit_order_payload(
    exit_reason: str,
    order_checks: dict[str, Any],
    tp_cancel: dict[str, Any] | None,
    sl_cancel: dict[str, Any] | None,
    ts_cancel: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if exit_reason == 'TAKE_PROFIT':
        return (order_checks.get('tp') or {}).get('data') or (tp_cancel or {}).get('data')
    if exit_reason == 'STOP_LOSS':
        return (order_checks.get('sl') or {}).get('data') or (sl_cancel or {}).get('data')
    if exit_reason in ('TIME_STOP', EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN):
        return (order_checks.get('time_stop') or {}).get('data') or (ts_cancel or {}).get('data')
    return None

def _resolve_live_exit_price(
    exit_reason: str,
    exit_order: dict[str, Any] | None,
    *,
    fallback_tp_price: float,
    fallback_sl_price: float,
    fallback_entry_price: float,
) -> tuple[float | None, str]:
    fallback_price = fallback_entry_price
    fallback_source = 'fallback_entry_price'
    if exit_reason == 'TAKE_PROFIT' and fallback_tp_price > 0:
        fallback_price = fallback_tp_price
        fallback_source = 'fallback_tp_price'
    elif exit_reason == 'STOP_LOSS' and fallback_sl_price > 0:
        fallback_price = fallback_sl_price
        fallback_source = 'fallback_sl_price'
    res = resolve_order_fill_price(exit_order or {}, fallback_price=fallback_price if fallback_price > 0 else None)
    payload = (res.get('data') or {}) if res.get('ok') else {}
    price = payload.get('fill_price')
    source = payload.get('price_source') or fallback_source
    try:
        px = float(price)
    except Exception:
        px = None
    return px, str(source)

def _build_live_trade_projection_row(
    account: str,
    live_cfg: dict[str, Any],
    *,
    signal_snapshot: dict[str, Any],
    order_root: str | None,
    entry_time_ms: int | None,
    entry_time_source: str | None,
    entry_price: float,
    entry_price_source: str | None,
    resolved_tp_price: float,
    resolved_tp_price_source: str | None,
    resolved_sl_price: float,
    selected_tp_pct: float | None,
    tp_client_order_id: str | None,
    tp_exchange_order_id: int | None,
    sl_client_order_id: str | None,
    sl_exchange_order_id: int | None,
    time_stop_client_order_id: str | None,
    time_stop_exchange_order_id: int | None,
    entry_client_order_id: str | None,
    entry_exchange_order_id: int | None,
    exit_reason: str,
    order_checks: dict[str, Any],
    tp_cancel: dict[str, Any] | None,
    sl_cancel: dict[str, Any] | None,
    ts_cancel: dict[str, Any] | None,
    current_time_ms: int,
    current_time_bj: str,
    source: str,
) -> dict[str, Any]:
    signal_core = _signal_core_fields(signal_snapshot, fallback_time_ms=entry_time_ms or current_time_ms)
    exit_order = _resolve_exit_order_payload(exit_reason, order_checks, tp_cancel, sl_cancel, ts_cancel)
    exit_time_ms = _extract_order_event_ts_ms(exit_order, fallback_ms=current_time_ms) or current_time_ms
    exit_price, exit_price_source = _resolve_live_exit_price(
        exit_reason,
        exit_order,
        fallback_tp_price=float(resolved_tp_price or 0.0),
        fallback_sl_price=float(resolved_sl_price or 0.0),
        fallback_entry_price=float(entry_price or 0.0),
    )
    pnl_pct = None
    if entry_price and exit_price:
        pnl_pct = (float(exit_price) / float(entry_price)) - 1.0
    row = {
        'symbol': signal_core['symbol'],
        'signal_time': signal_core['signal_time'],
        'signal_price': signal_core['current_price'],
        'c_time': signal_core.get('c_time'),
        'c_time_bj': signal_core.get('c_time_bj'),
        'entry_time': int(entry_time_ms or signal_core['signal_time']),
        'entry_time_source': entry_time_source,
        'exit_time': int(exit_time_ms),
        'entry_price': float(entry_price or 0.0),
        'exit_price': float(exit_price or 0.0) if exit_price is not None else None,
        'pnl_pct': _normalize_scalar(pnl_pct),
        'reason': exit_reason,
        'exit_bar_tp_sl_both_hit': None,
        'context': signal_core['context'],
        'signal_time_bj': signal_core['signal_time_bj'],
        'entry_time_bj': _fmt_bj_from_ms(int(entry_time_ms or signal_core['signal_time'])),
        'exit_time_bj': _fmt_bj_from_ms(int(exit_time_ms)),
        'run_mode': 'live',
        'projection_type': 'live_trade',
        'projection_schema_version': _projection_schema_version(live_cfg),
        'strategy_name': 'snapback',
        'account': account,
        'run_id': _projection_run_id(live_cfg),
        'source': source,
        'bar_ts': current_time_ms,
        'bar_bj': current_time_bj,
        'order_root': order_root,
        'signal_digest': _signal_digest(signal_snapshot),
        'entry_price_source': entry_price_source,
        'exit_price_source': exit_price_source,
        'resolved_tp_price': _normalize_scalar(resolved_tp_price),
        'resolved_tp_price_source': resolved_tp_price_source,
        'resolved_sl_price': _normalize_scalar(resolved_sl_price),
        'selected_tp_pct': _normalize_scalar(selected_tp_pct),
        'tp_tier': signal_core.get('tp_tier'),
        'entry_client_order_id': entry_client_order_id,
        'entry_exchange_order_id': entry_exchange_order_id,
        'tp_order_client_id': tp_client_order_id,
        'tp_order_exchange_id': tp_exchange_order_id,
        'sl_order_client_id': sl_client_order_id,
        'sl_order_exchange_id': sl_exchange_order_id,
        'time_stop_client_order_id': time_stop_client_order_id,
        'time_stop_exchange_order_id': time_stop_exchange_order_id,
        'protective_flatten_client_order_id': time_stop_client_order_id if exit_reason == EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN else None,
        'protective_flatten_exchange_order_id': time_stop_exchange_order_id if exit_reason == EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN else None,
        'protective_flatten_exit_reason': exit_reason if exit_reason == EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN else None,
        'exit_order_client_id': (exit_order or {}).get('client_order_id'),
        'exit_order_exchange_id': (exit_order or {}).get('order_id'),
        'exit_order_status': (exit_order or {}).get('status'),
        'exit_order_leg': _exit_order_leg(exit_reason),
    }
    return row

def append_live_trade_projection_from_open_trade(
    account: str,
    live_cfg: dict[str, Any],
    *,
    open_trade: dict[str, Any],
    exit_reason: str,
    order_checks: dict[str, Any],
    tp_cancel: dict[str, Any] | None,
    sl_cancel: dict[str, Any] | None,
    ts_cancel: dict[str, Any] | None,
    current_time_ms: int,
    current_time_bj: str,
    source: str,
) -> dict[str, Any]:
    try:
        signal_snapshot = open_trade.get('signal_snapshot') if isinstance(open_trade.get('signal_snapshot'), dict) else {}
        row = _build_live_trade_projection_row(
            account,
            live_cfg,
            signal_snapshot=signal_snapshot,
            order_root=open_trade.get('order_root'),
            entry_time_ms=open_trade.get('entry_submit_finished_utc_ms') or open_trade.get('entry_ts'),
            entry_time_source='entry_submit_finished_utc_ms' if open_trade.get('entry_submit_finished_utc_ms') else 'entry_ts',
            entry_price=float(open_trade.get('entry_price') or 0.0),
            entry_price_source=open_trade.get('entry_price_source'),
            resolved_tp_price=float(open_trade.get('tp_price') or 0.0),
            resolved_tp_price_source=open_trade.get('resolved_tp_price_source'),
            resolved_sl_price=float(open_trade.get('sl_trigger_price') or 0.0),
            selected_tp_pct=_normalize_scalar(open_trade.get('selected_tp_pct')),
            tp_client_order_id=open_trade.get('tp_order_client_id'),
            tp_exchange_order_id=open_trade.get('tp_order_exchange_id'),
            sl_client_order_id=open_trade.get('sl_order_client_id'),
            sl_exchange_order_id=open_trade.get('sl_order_exchange_id'),
            time_stop_client_order_id=open_trade.get('time_stop_client_order_id'),
            time_stop_exchange_order_id=open_trade.get('time_stop_exchange_order_id'),
            entry_client_order_id=open_trade.get('entry_client_order_id'),
            entry_exchange_order_id=open_trade.get('entry_exchange_order_id'),
            exit_reason=exit_reason,
            order_checks=order_checks,
            tp_cancel=tp_cancel,
            sl_cancel=sl_cancel,
            ts_cancel=ts_cancel,
            current_time_ms=current_time_ms,
            current_time_bj=current_time_bj,
            source=source,
        )
        path = _projection_path(live_cfg, 'live_trades')
        _append_projection_row(path, row)
        return {'ok': True, 'path': str(path), 'row': row}
    except Exception as e:
        return {'ok': False, 'reason': str(e), 'path': str(_projection_path(live_cfg, 'live_trades'))}

def append_live_trade_projection_from_pending_terminal(
    account: str,
    live_cfg: dict[str, Any],
    *,
    pending: dict[str, Any],
    entry_order_res: dict[str, Any],
    exit_reason: str,
    order_checks: dict[str, Any],
    tp_cancel: dict[str, Any] | None,
    sl_cancel: dict[str, Any] | None,
    ts_cancel: dict[str, Any] | None,
    current_time_ms: int,
    current_time_bj: str,
    source: str,
) -> dict[str, Any]:
    try:
        signal_snapshot = pending.get('signal_snapshot') if isinstance(pending.get('signal_snapshot'), dict) else {}
        entry_row = (entry_order_res.get('data') or {}) if entry_order_res.get('ok') else {}
        entry_fill_res = resolve_order_fill_price(entry_row or {}, fallback_price=float(pending.get('current_price') or 0.0) or None)
        entry_fill_payload = (entry_fill_res.get('data') or {}) if entry_fill_res.get('ok') else {}
        row = _build_live_trade_projection_row(
            account,
            live_cfg,
            signal_snapshot=signal_snapshot,
            order_root=pending.get('order_root'),
            entry_time_ms=pending.get('signal_time'),
            entry_time_source='pending_signal_time',
            entry_price=float(entry_fill_payload.get('fill_price') or pending.get('current_price') or 0.0),
            entry_price_source=str(entry_fill_payload.get('price_source') or pending.get('entry_fill_price_source') or 'pending_current_price'),
            resolved_tp_price=float(pending.get('tp_price') or signal_snapshot.get('tp_price') or 0.0),
            resolved_tp_price_source=pending.get('resolved_tp_price_source'),
            resolved_sl_price=float(pending.get('sl_price') or signal_snapshot.get('sl_price') or 0.0),
            selected_tp_pct=_normalize_scalar(pending.get('selected_tp_pct')),
            tp_client_order_id=pending.get('tp_client_order_id'),
            tp_exchange_order_id=None,
            sl_client_order_id=pending.get('sl_client_order_id'),
            sl_exchange_order_id=None,
            time_stop_client_order_id=pending.get('time_stop_client_order_id'),
            time_stop_exchange_order_id=None,
            entry_client_order_id=pending.get('client_order_id'),
            entry_exchange_order_id=pending.get('exchange_order_id'),
            exit_reason=exit_reason,
            order_checks=order_checks,
            tp_cancel=tp_cancel,
            sl_cancel=sl_cancel,
            ts_cancel=ts_cancel,
            current_time_ms=current_time_ms,
            current_time_bj=current_time_bj,
            source=source,
        )
        path = _projection_path(live_cfg, 'live_trades')
        _append_projection_row(path, row)
        return {'ok': True, 'path': str(path), 'row': row}
    except Exception as e:
        return {'ok': False, 'reason': str(e), 'path': str(_projection_path(live_cfg, 'live_trades'))}

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

def _write_stage_record(account: str, stage: str, payload: dict[str, Any]) -> Path:
    return append_stage_record(account, stage, payload)

def _perf_elapsed_ms(start_perf: float) -> int:
    return int((time.perf_counter() - start_perf) * 1000)

def _log_perf_stage(stage: str, **fields: Any) -> None:
    return None

def _signal_digest(signal: dict[str, Any]) -> str:
    base = {
        'symbol': _normalize_scalar(signal.get('symbol')),
        'signal_time': _normalize_scalar(signal.get('signal_time')),
        'action': _normalize_scalar(signal.get('action')),
        'current_price': _normalize_scalar(signal.get('current_price')),
        'tp_price': _normalize_scalar(signal.get('tp_price')),
        'sl_price': _normalize_scalar(signal.get('sl_price')),
    }
    return _json_safe_dumps(base, sort_keys=True)

def _payload_client_order_ids(payload: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    for key in (
        'client_order_id',
        'entry_client_order_id',
        'tp_client_order_id',
        'sl_client_order_id',
        'time_stop_client_order_id',
        'protective_flatten_client_order_id',
        'entry_order_client_id',
        'tp_order_client_id',
        'sl_order_client_id',
    ):
        value = str(payload.get(key) or '').strip()
        if value:
            ids.append(value)
    return ids

def _payload_strategy_codes(payload: dict[str, Any]) -> set[str]:
    codes: set[str] = set()
    explicit = str(payload.get('strategy_code') or '').upper().strip()
    if explicit:
        codes.add(explicit)
    strategy_name = str(payload.get('strategy_name') or '').strip().lower().replace('-', '_')
    if strategy_name == 'snapback':
        codes.add(STRAT_CODE)
    elif strategy_name:
        codes.add(f"NAME:{strategy_name}")
    for client_order_id in _payload_client_order_ids(payload):
        parsed = parse_client_order_id(client_order_id, broker_id=BROKER_ID)
        if parsed.get('recognized') and parsed.get('strat'):
            codes.add(str(parsed['strat']).upper().strip())
    return {code for code in codes if code}

def _is_snapback_owned_payload(payload: dict[str, Any]) -> bool:
    codes = _payload_strategy_codes(payload)
    return codes == {STRAT_CODE}

def _foreign_payload_snapshot(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        'strategy_code': payload.get('strategy_code'),
        'strategy_name': payload.get('strategy_name'),
        'detected_strategy_codes': sorted(_payload_strategy_codes(payload)),
        'client_order_ids': _payload_client_order_ids(payload),
        'order_root': payload.get('order_root'),
    }

def _resolve_selected_tp_pct(signal: dict[str, Any]) -> float | None:
    params = signal.get('params') if isinstance(signal.get('params'), dict) else {}
    context = signal.get('context') if isinstance(signal.get('context'), dict) else {}
    candidates = [
        params.get('selected_take_profit_pct'),
        params.get('selected_tp_pct'),
        context.get('selected_tp_pct'),
    ]
    for raw in candidates:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        if value > 0:
            return value
    try:
        current_price = float(signal.get('current_price') or 0.0)
        signal_tp_price = float(signal.get('tp_price') or 0.0)
    except (TypeError, ValueError):
        return None
    if current_price > 0 and signal_tp_price > current_price:
        return (signal_tp_price / current_price) - 1.0
    return None

def _resolve_tp_price_from_fill(signal: dict[str, Any], entry_fill_price: float) -> tuple[float, str, float | None]:
    selected_tp_pct = _resolve_selected_tp_pct(signal)
    if entry_fill_price > 0 and selected_tp_pct is not None and selected_tp_pct > 0:
        return float(entry_fill_price) * (1.0 + float(selected_tp_pct)), 'entry_fill_pct', float(selected_tp_pct)
    fallback_tp_price = float(signal.get('tp_price') or 0.0)
    if fallback_tp_price > 0:
        return fallback_tp_price, 'signal_tp_price', selected_tp_pct
    return 0.0, 'unavailable', selected_tp_pct

def _resolve_live_entry_fill_price(
    account: str,
    symbol: str,
    entry_data: dict[str, Any],
    *,
    fallback_price: float,
) -> tuple[float, str]:
    fill_price_res = resolve_order_fill_price(entry_data, fallback_price=None)
    fill_payload = (fill_price_res.get('data') or {}) if fill_price_res.get('ok') else {}
    entry_fill_price = float(fill_payload.get('fill_price') or 0.0)
    entry_fill_price_source = str(fill_payload.get('price_source') or '')

    if entry_fill_price <= 0:
        position_res = get_position(account, symbol, FIXED_POSITION_SIDE)
        if position_res.get('ok') and position_res.get('data'):
            try:
                position_entry_price = float((position_res.get('data') or {}).get('entry_price') or 0.0)
            except (TypeError, ValueError):
                position_entry_price = 0.0
            if position_entry_price > 0:
                return position_entry_price, 'position_entry_price'

    fallback = float(fallback_price or 0.0)
    if entry_fill_price <= 0 and fallback > 0:
        return fallback, 'fallback_price'

    return entry_fill_price, (entry_fill_price_source or 'fallback_price')

def _extract_time_stop_config(strategy_cfg: dict[str, Any]) -> tuple[int, float]:
    time_stop = ((strategy_cfg or {}).get('exit_policy') or {}).get('time_stop') or {}
    return int(time_stop.get('max_hold_mins', 0)), float(time_stop.get('min_profit_pct', 0.0))

def _order_query(account: str, symbol: str, *, exchange_order_id: int | None = None, client_order_id: str | None = None, retry_max: int = 0, retry_delay_secs: float = 1.0) -> dict[str, Any]:
    if exchange_order_id is None and not client_order_id:
        return {'ok': False, 'reason': 'missing order identity', 'data': None}
    return get_order(
        account,
        symbol,
        exchange_order_id=exchange_order_id,
        client_order_id=client_order_id,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )

def _cancel_order_if_present(account: str, symbol: str, *, exchange_order_id: int | None = None, client_order_id: str | None = None, prefetched_order_res: dict[str, Any] | None = None, known_open_orders: list[dict[str, Any]] | None = None, retry_max: int = 0, retry_delay_secs: float = 1.0) -> dict[str, Any]:
    if exchange_order_id is None and not client_order_id:
        return {'ok': True, 'reason': '', 'data': None, 'skipped': True}
    matched_open_order = None
    if known_open_orders is not None:
        matched_open_order = _find_open_order(
            known_open_orders,
            exchange_order_id=exchange_order_id,
            client_order_id=client_order_id,
        )
        if matched_open_order is None:
            return {'ok': True, 'reason': '', 'data': None, 'skipped': True, 'not_in_open_orders_snapshot': True}
        cancel_res = cancel_order(
            account,
            symbol,
            exchange_order_id=exchange_order_id,
            client_order_id=client_order_id,
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
        )
        if cancel_res.get('ok'):
            cancel_res = dict(cancel_res)
            cancel_res['matched_open_order_snapshot'] = matched_open_order
        return cancel_res
    order_res = prefetched_order_res if isinstance(prefetched_order_res, dict) else None
    if not order_res:
        order_res = _order_query(account, symbol, exchange_order_id=exchange_order_id, client_order_id=client_order_id, retry_max=retry_max, retry_delay_secs=retry_delay_secs)
    if order_res.get('ok') and order_res.get('data'):
        status = str(order_res['data'].get('status') or '').upper()
        if status in TERMINAL_ORDER_STATUSES:
            return {'ok': True, 'reason': '', 'data': order_res.get('data'), 'skipped': True, 'already_terminal': True}
    return cancel_order(
        account,
        symbol,
        exchange_order_id=exchange_order_id,
        client_order_id=client_order_id,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )

def _is_order_not_exist_reason(reason: Any) -> bool:
    text = str(reason or '')
    return ('code=-2013' in text) or ('Order does not exist' in text)


def _infer_exit_reason(account: str, symbol: str, open_trade: dict[str, Any], retry_max: int, retry_delay_secs: float, known_open_orders: list[dict[str, Any]] | None = None) -> tuple[str, dict[str, Any], str | None]:
    checks: dict[str, Any] = {}

    def _resolve_leg_order(*, exchange_order_id: int | None = None, client_order_id: str | None = None) -> dict[str, Any]:
        if exchange_order_id is None and not client_order_id:
            return {
                'ok': True,
                'reason': '',
                'data': None,
                'skipped': True,
                'missing_identity': True,
            }
        matched_open_order = _find_open_order(
            known_open_orders or [],
            exchange_order_id=exchange_order_id,
            client_order_id=client_order_id,
        )
        if matched_open_order is not None:
            return {
                'ok': True,
                'reason': '',
                'data': matched_open_order,
                'skipped': True,
                'known_open_order_snapshot': True,
            }
        order_res = _order_query(
            account,
            symbol,
            exchange_order_id=exchange_order_id,
            client_order_id=client_order_id,
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
        )
        if (not order_res.get('ok')) and _is_order_not_exist_reason(order_res.get('reason')):
            return {
                'ok': True,
                'reason': '',
                'data': None,
                'skipped': True,
                'missing_on_exchange': True,
                'missing_reason': order_res.get('reason'),
            }
        return order_res

    ts_res = _resolve_leg_order(
        exchange_order_id=open_trade.get('time_stop_exchange_order_id'),
        client_order_id=open_trade.get('time_stop_client_order_id'),
    )
    checks['time_stop'] = ts_res
    if ts_res.get('ok') and ts_res.get('data') and str(ts_res['data'].get('status') or '').upper() in FILLED_ORDER_STATUSES:
        return _time_stop_exit_reason(open_trade), checks, None

    tp_res = _resolve_leg_order(
        exchange_order_id=open_trade.get('tp_order_exchange_id'),
        client_order_id=open_trade.get('tp_order_client_id'),
    )
    checks['tp'] = tp_res
    if tp_res.get('ok') and tp_res.get('data') and str(tp_res['data'].get('status') or '').upper() in FILLED_ORDER_STATUSES:
        return 'TAKE_PROFIT', checks, None

    sl_res = _resolve_leg_order(
        exchange_order_id=open_trade.get('sl_order_exchange_id'),
        client_order_id=open_trade.get('sl_order_client_id'),
    )
    checks['sl'] = sl_res
    if sl_res.get('ok') and sl_res.get('data') and str(sl_res['data'].get('status') or '').upper() in FILLED_ORDER_STATUSES:
        return 'STOP_LOSS', checks, None

    blocking_reason = None
    for leg in ('time_stop', 'tp', 'sl'):
        leg_reason = (checks.get(leg) or {}).get('reason')
        if leg_reason:
            blocking_reason = leg_reason
            break
    return 'UNKNOWN_EXIT', checks, blocking_reason

def _build_open_trade(
    entry_res: dict[str, Any],
    signal: dict[str, Any],
    tp_res: dict[str, Any],
    sl_res: dict[str, Any],
    entry_notional_usdt: float,
    *,
    order_root: str,
    entry_client_order_id: str,
    tp_client_order_id: str,
    sl_client_order_id: str,
    tp_price: float,
    sl_trigger_price: float,
    entry_price_source: str | None = None,
    entry_submit_started_utc_ms: int | None = None,
    entry_submit_finished_utc_ms: int | None = None,
    resolved_tp_price_source: str | None = None,
    selected_tp_pct: float | None = None,
) -> dict[str, Any]:
    entry = entry_res['data']
    tp = tp_res['data'] if tp_res.get('ok') else {}
    sl = sl_res['data'] if sl_res.get('ok') else {}
    return {
        'symbol': signal['symbol'],
        'strategy_name': 'snapback',
        'strategy_code': STRAT_CODE,
        'side': FIXED_POSITION_SIDE,
        'order_root': order_root,
        'entry_client_order_id': entry.get('client_order_id', entry_client_order_id),
        'entry_exchange_order_id': entry.get('exchange_order_id'),
        'entry_ts': int(signal['signal_time']),
        'entry_bj': signal['signal_time_bj'],
        'entry_price': float(entry.get('avg_price') or signal.get('current_price') or 0.0),
        'entry_price_source': entry_price_source,
        'entry_submit_started_utc_ms': entry_submit_started_utc_ms,
        'entry_submit_finished_utc_ms': entry_submit_finished_utc_ms,
        'entry_submit_started_bj': _fmt_bj_from_ms(entry_submit_started_utc_ms),
        'entry_submit_finished_bj': _fmt_bj_from_ms(entry_submit_finished_utc_ms),
        'entry_qty': float(entry.get('executed_qty') or entry.get('qty') or 0.0),
        'entry_notional_usdt': float(entry_notional_usdt),
        'signal_digest': _signal_digest(signal),
        'signal_snapshot': signal,
        'selected_tp_pct': selected_tp_pct,
        'resolved_tp_price_source': resolved_tp_price_source,
        'tp_order_client_id': tp.get('client_order_id', tp_client_order_id),
        'tp_order_exchange_id': tp.get('exchange_order_id'),
        'sl_order_client_id': sl.get('client_order_id', sl_client_order_id),
        'sl_order_exchange_id': sl.get('exchange_order_id'),
        'time_stop_client_order_id': None,
        'time_stop_exchange_order_id': None,
        'tp_price': float(tp_price or 0.0),
        'sl_trigger_price': float(sl_trigger_price or 0.0),
        'status': 'OPEN',
        'exit_submit_inflight': False,
        'last_status_bj': _now_bj_str(),
        'time_stop_last_check_bj': None,
    }

def _build_pending_entry(
    entry_res: dict[str, Any],
    signal: dict[str, Any],
    entry_notional_usdt: float,
    *,
    order_root: str,
    entry_client_order_id: str,
    tp_client_order_id: str,
    sl_client_order_id: str,
    tp_price: float,
    sl_price: float,
    entry_fill_price_source: str | None = None,
    resolved_tp_price_source: str | None = None,
    selected_tp_pct: float | None = None,
) -> dict[str, Any]:
    entry = entry_res['data']
    return {
        'symbol': signal['symbol'],
        'strategy_name': 'snapback',
        'strategy_code': STRAT_CODE,
        'order_root': order_root,
        'client_order_id': entry.get('client_order_id', entry_client_order_id),
        'exchange_order_id': entry.get('exchange_order_id'),
        'signal_time': int(signal['signal_time']),
        'signal_time_bj': signal['signal_time_bj'],
        'current_price': float(signal.get('current_price') or 0.0),
        'entry_notional_usdt': float(entry_notional_usdt),
        'signal_digest': _signal_digest(signal),
        'signal_snapshot': signal,
        'entry_fill_price_source': entry_fill_price_source,
        'resolved_tp_price_source': resolved_tp_price_source,
        'selected_tp_pct': selected_tp_pct,
        'tp_price': float(tp_price or 0.0),
        'sl_price': float(sl_price or 0.0),
        'tp_client_order_id': tp_client_order_id,
        'sl_client_order_id': sl_client_order_id,
        'time_stop_client_order_id': None,
        'created_bj': _now_bj_str(),
    }

def _recover_open_trade_from_pending(pending: dict[str, Any], position: dict[str, Any]) -> dict[str, Any]:
    signal_snapshot = pending.get('signal_snapshot') if isinstance(pending.get('signal_snapshot'), dict) else {}
    return {
        'symbol': str(pending.get('symbol') or position.get('symbol') or '').upper().strip(),
        'strategy_name': 'snapback',
        'strategy_code': STRAT_CODE,
        'side': FIXED_POSITION_SIDE,
        'order_root': pending.get('order_root'),
        'entry_client_order_id': pending.get('client_order_id'),
        'entry_exchange_order_id': pending.get('exchange_order_id'),
        'entry_ts': int(pending.get('signal_time') or 0),
        'entry_bj': pending.get('signal_time_bj'),
        'entry_price': float(position.get('entry_price') or pending.get('current_price') or 0.0),
        'entry_qty': float(position.get('qty') or 0.0),
        'entry_notional_usdt': float(pending.get('entry_notional_usdt') or 0.0),
        'signal_digest': pending.get('signal_digest'),
        'signal_snapshot': signal_snapshot,
        'entry_price_source': pending.get('entry_fill_price_source'),
        'selected_tp_pct': _normalize_scalar(pending.get('selected_tp_pct')),
        'resolved_tp_price_source': pending.get('resolved_tp_price_source'),
        'tp_order_client_id': pending.get('tp_client_order_id'),
        'tp_order_exchange_id': None,
        'sl_order_client_id': pending.get('sl_client_order_id'),
        'sl_order_exchange_id': None,
        'time_stop_client_order_id': None,
        'time_stop_exchange_order_id': None,
        'tp_price': float(pending.get('tp_price') or signal_snapshot.get('tp_price') or 0.0),
        'sl_trigger_price': float(pending.get('sl_price') or signal_snapshot.get('sl_price') or 0.0),
        'status': 'OPEN',
        'exit_submit_inflight': False,
        'last_status_bj': _now_bj_str(),
        'time_stop_last_check_bj': None,
    }

def _find_open_order(open_orders: list[dict[str, Any]], *, exchange_order_id: int | None = None, client_order_id: str | None = None) -> dict[str, Any] | None:
    if exchange_order_id is None and not client_order_id:
        return None
    for row in open_orders:
        if exchange_order_id is not None and row.get('order_id') == exchange_order_id:
            return row
        if client_order_id and str(row.get('client_order_id') or '') == str(client_order_id):
            return row
    return None

def _ensure_exit_orders(account: str, symbol: str, open_trade: dict[str, Any], position: dict[str, Any], open_orders: list[dict[str, Any]], live_cfg: dict[str, Any], current_time_ms: int, current_time_bj: str, *, source: str) -> tuple[dict[str, Any], bool]:
    audit_enabled = bool(live_cfg.get('audit_enabled', True))
    notify_enabled = bool(live_cfg.get('notify_enabled', False))
    retry_max = int(live_cfg['order_retry_max'])
    retry_delay_secs = float(live_cfg['api_retry_delay_secs'])
    qty = float(position.get('qty') or open_trade.get('entry_qty') or 0.0)

    tp_row = _find_open_order(
        open_orders,
        exchange_order_id=open_trade.get('tp_order_exchange_id'),
        client_order_id=open_trade.get('tp_order_client_id'),
    )
    sl_row = _find_open_order(
        open_orders,
        exchange_order_id=open_trade.get('sl_order_exchange_id'),
        client_order_id=open_trade.get('sl_order_client_id'),
    )

    changed = False
    if tp_row:
        if open_trade.get('tp_order_exchange_id') != tp_row.get('order_id') or open_trade.get('tp_order_client_id') != tp_row.get('client_order_id'):
            open_trade['tp_order_exchange_id'] = tp_row.get('order_id')
            open_trade['tp_order_client_id'] = tp_row.get('client_order_id')
            changed = True
    elif qty > 0 and float(open_trade.get('tp_price') or 0.0) > 0:
        tp_client_order_id = open_trade.get('tp_order_client_id') or build_client_order_id(
            broker_id=BROKER_ID,
            strat=STRAT_CODE,
            leg=LEG_TP,
            root=open_trade.get('order_root') or make_order_root(),
        )
        tp_res = place_tp_order(
            account,
            symbol,
            FIXED_POSITION_SIDE,
            qty,
            float(open_trade['tp_price']),
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
            client_order_id=tp_client_order_id,
        )
        if tp_res.get('ok'):
            tp_data = tp_res['data']
            open_trade['tp_order_client_id'] = tp_data.get('client_order_id', tp_client_order_id)
            open_trade['tp_order_exchange_id'] = tp_data.get('exchange_order_id')
            changed = True
            if audit_enabled:
                write_event(account, 'tp_recreated', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'order_root': open_trade.get('order_root'),
                    'tp_client_order_id': open_trade.get('tp_order_client_id'),
                    'exchange_snapshot': tp_res,
                })
        else:
            mark_error(account, symbol, error_code='tp_recreate_failed', error_message=tp_res.get('reason'), error_bj=current_time_bj)
            if audit_enabled:
                write_event(account, 'tp_recreate_failed', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'order_root': open_trade.get('order_root'),
                    'exchange_snapshot': tp_res,
                })

    if sl_row:
        if open_trade.get('sl_order_exchange_id') != sl_row.get('order_id') or open_trade.get('sl_order_client_id') != sl_row.get('client_order_id'):
            open_trade['sl_order_exchange_id'] = sl_row.get('order_id')
            open_trade['sl_order_client_id'] = sl_row.get('client_order_id')
            changed = True
    elif float(open_trade.get('sl_trigger_price') or 0.0) > 0:
        sl_client_order_id = open_trade.get('sl_order_client_id') or build_client_order_id(
            broker_id=BROKER_ID,
            strat=STRAT_CODE,
            leg=LEG_SL,
            root=open_trade.get('order_root') or make_order_root(),
        )
        sl_res = place_sl_order(
            account,
            symbol,
            FIXED_POSITION_SIDE,
            float(open_trade['sl_trigger_price']),
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
            client_order_id=sl_client_order_id,
        )
        if sl_res.get('ok'):
            sl_data = sl_res['data']
            open_trade['sl_order_client_id'] = sl_data.get('client_order_id', sl_client_order_id)
            open_trade['sl_order_exchange_id'] = sl_data.get('exchange_order_id')
            changed = True
            if audit_enabled:
                write_event(account, 'sl_recreated', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'order_root': open_trade.get('order_root'),
                    'sl_client_order_id': open_trade.get('sl_order_client_id'),
                    'exchange_snapshot': sl_res,
                })
        else:
            mark_error(account, symbol, error_code='sl_recreate_failed', error_message=sl_res.get('reason'), error_bj=current_time_bj)
            if audit_enabled:
                write_event(account, 'sl_recreate_failed', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'order_root': open_trade.get('order_root'),
                    'exchange_snapshot': sl_res,
                })

    if changed:
        open_trade['last_status_bj'] = current_time_bj
        set_open_trade(account, symbol, open_trade)
    return open_trade, changed

def _verify_open_trade_brackets(account: str, symbol: str, open_trade: dict[str, Any], *, retry_max: int, retry_delay_secs: float, snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    symbol_key = str(symbol).upper().strip()

    if snapshot is not None:
        positions_by_symbol = snapshot.get('positions_by_symbol') or {}
        open_orders_by_symbol = snapshot.get('open_orders_by_symbol') or {}
        symbol_positions = list(positions_by_symbol.get(symbol_key) or [])
        symbol_open_orders = list(open_orders_by_symbol.get(symbol_key) or [])

        all_pos_res = snapshot.get('positions')
        if all_pos_res is None:
            all_pos_res = {'ok': True, 'reason': '', 'data': symbol_positions}

        all_ord_res = snapshot.get('orders')
        if all_ord_res is None:
            all_ord_res = {'ok': True, 'reason': '', 'data': symbol_open_orders}

        position = None
        if all_pos_res.get('ok'):
            for row in symbol_positions:
                position_side = str(row.get('position_side') or '').upper().strip()
                try:
                    qty = abs(float(row.get('qty') or 0.0))
                except (TypeError, ValueError):
                    qty = 0.0
                if position_side == FIXED_POSITION_SIDE and qty > 0:
                    position = row
                    break

        pos_res = {
            'ok': bool(all_pos_res.get('ok')),
            'reason': all_pos_res.get('reason'),
            'data': position,
        }
        ord_res = {
            'ok': bool(all_ord_res.get('ok')),
            'reason': all_ord_res.get('reason'),
            'data': symbol_open_orders,
        }
    else:
        if snapshot is not None:
            precheck = precheck_exchange_blockers(account, symbol, snapshot=snapshot)
            pos_res = precheck.get('position') or {'ok': False, 'reason': 'missing position snapshot', 'data': None}
            ord_res = precheck.get('orders') or {'ok': False, 'reason': 'missing orders snapshot', 'data': None}
        else:
            pos_res = get_position(account, symbol, FIXED_POSITION_SIDE)
            ord_res = get_open_orders(account, symbol)
    if not pos_res.get('ok') or not ord_res.get('ok'):
        return {
            'ok': False,
            'position': pos_res,
            'orders': ord_res,
            'position_open': None,
            'tp_bound': None,
            'sl_bound': None,
        }
    position = pos_res.get('data')
    open_orders = ord_res.get('data') or []
    if not position:
        return {
            'ok': True,
            'position': pos_res,
            'orders': ord_res,
            'position_open': False,
            'tp_bound': False,
            'sl_bound': False,
        }
    tp_bound = _find_open_order(
        open_orders,
        exchange_order_id=open_trade.get('tp_order_exchange_id'),
        client_order_id=open_trade.get('tp_order_client_id'),
    ) is not None
    sl_bound = _find_open_order(
        open_orders,
        exchange_order_id=open_trade.get('sl_order_exchange_id'),
        client_order_id=open_trade.get('sl_order_client_id'),
    ) is not None
    return {
        'ok': True,
        'position': pos_res,
        'orders': ord_res,
        'position_open': True,
        'tp_bound': tp_bound,
        'sl_bound': sl_bound,
    }

def _refresh_entry_cooldown(account: str, symbol: str, current_time_ms: int, cooldown_mins: int) -> None:
    cooldown_until_ts, cooldown_until_bj = _cooldown_until(current_time_ms, cooldown_mins)
    set_cooldown(account, symbol, cooldown_until_ts=cooldown_until_ts, cooldown_until_bj=cooldown_until_bj)

def _refresh_exit_cooldown(account: str, symbol: str, current_time_ms: int, cooldown_mins: int) -> dict[str, Any]:
    cooldown_until_ts, cooldown_until_bj = _cooldown_until(current_time_ms, cooldown_mins)
    return set_cooldown(account, symbol, cooldown_until_ts=cooldown_until_ts, cooldown_until_bj=cooldown_until_bj)

def _clear_symbol_error(account: str, symbol: str) -> None:
    mark_error(account, symbol, error_code=None, error_message=None, error_bj=None)

def consumer_signal_digest(signal: dict[str, Any]) -> str:
    return _signal_digest(signal)

def precheck_consumer_exchange_blockers(account: str, symbol: str, *, snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    return precheck_exchange_blockers(account, symbol, snapshot=snapshot)

def prepare_consumer_loop_gate(
    account: str,
    strategy_cfg: dict[str, Any],
    live_cfg: dict[str, Any],
    *,
    current_time_ms: int,
    current_time_bj: str,
    candidate_symbols: list[str],
    extra_reconcile_symbols: list[str],
    latest_closes: dict[str, float],
    exchange_activity_snapshot: dict[str, Any],
    source: str = 'loop',
) -> dict[str, Any]:
    return _ledger_prepare_consumer_loop_gate_impl(
        account,
        strategy_cfg,
        live_cfg,
        maintain_consumer_once,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        candidate_symbols=candidate_symbols,
        extra_reconcile_symbols=extra_reconcile_symbols,
        latest_closes=latest_closes,
        exchange_activity_snapshot=exchange_activity_snapshot,
        source=source,
    )


def bootstrap_consumer(
    account: str,
    strategy_cfg: dict[str, Any],
    live_cfg: dict[str, Any],
    *,
    source: str = 'startup',
    current_time_ms: int | None = None,
    current_time_bj: str | None = None,
    exchange_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _ledger_bootstrap_consumer_impl(
        account,
        strategy_cfg,
        live_cfg,
        maintain_consumer_once,
        source=source,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        exchange_snapshot=exchange_snapshot,
    )


def bootstrap_consumer_gate(
    account: str,
    strategy_cfg: dict[str, Any],
    live_cfg: dict[str, Any],
    *,
    candidate_symbols: list[str],
    source: str = 'startup',
) -> dict[str, Any]:
    return _ledger_bootstrap_consumer_gate_impl(
        account,
        strategy_cfg,
        live_cfg,
        maintain_consumer_once,
        candidate_symbols=candidate_symbols,
        source=source,
    )


def evaluate_consumer_signal_scan_gate(
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
    return _ledger_evaluate_consumer_signal_scan_gate_impl(
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


def finalize_consumer_scan_skip(
    account: str,
    *,
    current_time_ms: int,
    current_time_bj: str,
    symbols: list[str] | set[str] | tuple[str, ...],
) -> dict[str, Any]:
    return _ledger_finalize_consumer_scan_skip_impl(
        account,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        symbols=symbols,
        mark_last_processed_bar_fn=mark_last_processed_bar,
    )

def finalize_consumer_no_candidate_data(
    account: str,
    *,
    current_time_ms: int,
    current_time_bj: str,
    symbols: list[str] | set[str] | tuple[str, ...],
    candidate_reason: str | None,
    candidate_errors: Any,
    extra_reconcile_symbols_count: int,
    audit_enabled: bool,
) -> dict[str, Any]:
    return _ledger_finalize_consumer_no_candidate_data_impl(
        account,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        symbols=symbols,
        candidate_reason=candidate_reason,
        candidate_errors=candidate_errors,
        extra_reconcile_symbols_count=extra_reconcile_symbols_count,
        audit_enabled=audit_enabled,
        mark_last_processed_bar_fn=mark_last_processed_bar,
    )

def finalize_consumer_signal_none(
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
) -> dict[str, Any]:
    return _ledger_finalize_consumer_signal_none_impl(
        account,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        symbols=symbols,
        candidate_payload=candidate_payload,
        extra_reconcile_symbols_count=extra_reconcile_symbols_count,
        timing_fields=timing_fields,
        signal_eval_started_utc_ms=signal_eval_started_utc_ms,
        signal_eval_finished_utc_ms=signal_eval_finished_utc_ms,
        audit_enabled=audit_enabled,
        mark_last_processed_bar_fn=mark_last_processed_bar,
        write_stage_record_fn=_write_stage_record,
        fmt_bj_from_ms_fn=_fmt_bj_from_ms,
    )

def finalize_consumer_loop_state(
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
) -> dict[str, Any]:
    return _ledger_finalize_consumer_loop_state_impl(
        account,
        mode=mode,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        symbols=symbols,
        audit_enabled=audit_enabled,
        scan_gate=scan_gate,
        candidate_payload=candidate_payload,
        candidate_reason=candidate_reason,
        candidate_errors=candidate_errors,
        extra_reconcile_symbols_count=extra_reconcile_symbols_count,
        timing_fields=timing_fields,
        signal_eval_started_utc_ms=signal_eval_started_utc_ms,
        signal_eval_finished_utc_ms=signal_eval_finished_utc_ms,
        mark_last_processed_bar_fn=mark_last_processed_bar,
        write_stage_record_fn=_write_stage_record,
        fmt_bj_from_ms_fn=_fmt_bj_from_ms,
    )

def _reconcile_pending_entries(account: str, live_cfg: dict[str, Any], current_time_ms: int, current_time_bj: str, *, source: str, snapshot: dict[str, Any] | None = None) -> bool:
    had_blocking_error = False
    state = load_live_state(account)
    symbols = state.get('symbols') or {}
    audit_enabled = bool(live_cfg.get('audit_enabled', True))
    notify_enabled = bool(live_cfg.get('notify_enabled', False))
    retry_max = int(live_cfg['order_retry_max'])
    retry_delay_secs = float(live_cfg['api_retry_delay_secs'])
    cooldown_mins = int(live_cfg['cooldown_mins'])
    for symbol, payload in symbols.items():
        if not isinstance(payload, dict):
            continue
        pending = payload.get('pending_entry_order')
        if not isinstance(pending, dict):
            continue
        if not _is_snapback_owned_payload(pending):
            had_blocking_error = True
            mark_error(
                account,
                symbol,
                error_code='foreign_pending_entry_blocked',
                error_message='snapback refused to reconcile non-SNP pending_entry_order',
                error_bj=current_time_bj,
            )
            if audit_enabled:
                write_event(account, 'foreign_pending_entry_blocked', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'foreign_payload': _foreign_payload_snapshot(pending),
                })
            continue
        open_trade = payload.get('open_trade')
        if open_trade:
            set_pending_entry_order(account, symbol, None)
            continue

        precheck = None
        if snapshot is not None:
            precheck = precheck_exchange_blockers(account, symbol, snapshot=snapshot)
            pos_res = precheck.get('position') or {'ok': False, 'reason': 'missing position snapshot', 'data': None}
        else:
            pos_res = get_position(account, symbol, FIXED_POSITION_SIDE)

        if not pos_res.get('ok'):
            had_blocking_error = True
            mark_error(
                account,
                symbol,
                error_code='pending_reconcile_query_failed',
                error_message=pos_res.get('reason'),
                error_bj=current_time_bj,
            )
            if audit_enabled:
                write_event(account, 'pending_reconcile_error', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'exchange_snapshot': {
                        'position': pos_res,
                    },
                })
            continue

        entry_res = {
            'ok': True,
            'reason': '',
            'data': None,
            'skipped': bool(pos_res.get('data')),
        }
        if pos_res.get('data'):
            if snapshot is not None:
                ord_res = precheck.get('orders') or {'ok': False, 'reason': 'missing orders snapshot', 'data': None}
            else:
                ord_res = get_open_orders(account, symbol)
            if not ord_res.get('ok'):
                had_blocking_error = True
                mark_error(
                    account,
                    symbol,
                    error_code='entry_recovery_orders_query_failed',
                    error_message=ord_res.get('reason'),
                    error_bj=current_time_bj,
                )
                if audit_enabled:
                    write_event(account, 'entry_recovery_orders_query_failed', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'source': source,
                        'order_root': pending.get('order_root'),
                        'exchange_snapshot': {
                            'entry_order': entry_res,
                            'position': pos_res,
                            'orders': ord_res,
                        },
                    })
                continue

            recovered_trade = _recover_open_trade_from_pending(pending, pos_res['data'])
            set_open_trade(account, symbol, recovered_trade)
            set_pending_entry_order(account, symbol, None)

            recovered_trade, exit_orders_changed = _ensure_exit_orders(
                account,
                symbol,
                recovered_trade,
                pos_res['data'],
                ord_res.get('data') or [],
                live_cfg,
                current_time_ms,
                current_time_bj,
                source='pending_recovery',
            )
            set_open_trade(account, symbol, recovered_trade)

            verify_res = _verify_open_trade_brackets(
                account,
                symbol,
                recovered_trade,
                retry_max=retry_max,
                retry_delay_secs=retry_delay_secs,
                snapshot=None if exit_orders_changed else snapshot,
            )
            if not verify_res.get('ok'):
                had_blocking_error = True
                verify_reason = (verify_res.get('orders') or {}).get('reason') or (verify_res.get('position') or {}).get('reason')
                mark_error(
                    account,
                    symbol,
                    error_code='entry_recovery_bracket_verify_failed',
                    error_message=verify_reason,
                    error_bj=current_time_bj,
                )
                if audit_enabled:
                    write_event(account, 'entry_recovery_bracket_verify_failed', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'source': source,
                        'order_root': recovered_trade.get('order_root'),
                        'exchange_snapshot': {
                            'position': verify_res.get('position'),
                            'orders': verify_res.get('orders'),
                        },
                    })
                continue

            if verify_res.get('position_open') and not (verify_res.get('tp_bound') and verify_res.get('sl_bound')):
                had_blocking_error = True
                mark_error(
                    account,
                    symbol,
                    error_code='entry_recovery_bracket_incomplete',
                    error_message=f"tp_bound={verify_res.get('tp_bound')}, sl_bound={verify_res.get('sl_bound')}",
                    error_bj=current_time_bj,
                )
                if audit_enabled:
                    write_event(account, 'entry_recovery_bracket_incomplete', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'source': source,
                        'order_root': recovered_trade.get('order_root'),
                        'tp_bound': verify_res.get('tp_bound'),
                        'sl_bound': verify_res.get('sl_bound'),
                        'tp_client_order_id': recovered_trade.get('tp_order_client_id'),
                        'sl_client_order_id': recovered_trade.get('sl_order_client_id'),
                        'exchange_snapshot': {
                            'position': verify_res.get('position'),
                            'orders': verify_res.get('orders'),
                        },
                    })
                continue

            _clear_symbol_error(account, symbol)
            _refresh_entry_cooldown(account, symbol, current_time_ms, cooldown_mins)
            if audit_enabled:
                write_event(account, 'entry_filled_recovered_to_open_trade', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'order_root': recovered_trade.get('order_root'),
                    'entry_client_order_id': recovered_trade.get('entry_client_order_id'),
                    'tp_client_order_id': recovered_trade.get('tp_order_client_id'),
                    'sl_client_order_id': recovered_trade.get('sl_order_client_id'),
                    'exchange_snapshot': {
                        'entry_order': entry_res,
                        'position': pos_res,
                        'orders': ord_res,
                    },
                })
                write_event(account, 'cooldown_set_after_entry_recovery', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'order_root': recovered_trade.get('order_root'),
                })
            continue
        known_symbol_open_orders = list(((precheck or {}).get('orders') or {}).get('data') or []) if snapshot is not None and ((precheck or {}).get('orders') or {}).get('ok') else None
        matched_pending_open_order = _find_open_order(
            known_symbol_open_orders or [],
            exchange_order_id=pending.get('exchange_order_id'),
            client_order_id=pending.get('client_order_id'),
        )
        if matched_pending_open_order is not None:
            entry_res = {
                'ok': True,
                'reason': '',
                'data': matched_pending_open_order,
                'skipped': True,
                'known_open_order_snapshot': True,
            }
        else:
            entry_res = _order_query(
                account,
                symbol,
                exchange_order_id=pending.get('exchange_order_id'),
                client_order_id=pending.get('client_order_id'),
                retry_max=retry_max,
                retry_delay_secs=retry_delay_secs,
            )
        if not entry_res.get('ok'):
            had_blocking_error = True
            mark_error(
                account,
                symbol,
                error_code='pending_reconcile_query_failed',
                error_message=entry_res.get('reason'),
                error_bj=current_time_bj,
            )
            if audit_enabled:
                write_event(account, 'pending_reconcile_error', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'exchange_snapshot': {
                        'entry_order': entry_res,
                        'position': pos_res,
                    },
                })
            continue

        if entry_res.get('ok') and entry_res.get('data'):
            status = str(entry_res['data'].get('status') or '').upper()
            if status in TERMINAL_ORDER_STATUSES:
                if status in FILLED_ORDER_STATUSES:
                    pending_terminal_trade = {
                        'order_root': pending.get('order_root'),
                        'entry_client_order_id': pending.get('client_order_id'),
                        'entry_exchange_order_id': pending.get('exchange_order_id'),
                        'tp_order_client_id': pending.get('tp_client_order_id'),
                        'tp_order_exchange_id': None,
                        'sl_order_client_id': pending.get('sl_client_order_id'),
                        'sl_order_exchange_id': None,
                        'time_stop_client_order_id': None,
                        'time_stop_exchange_order_id': None,
                    }
                    known_open_orders = list(((precheck or {}).get('orders') or {}).get('data') or []) if snapshot is not None and ((precheck or {}).get('orders') or {}).get('ok') else None
                    exit_reason, order_checks, blocking_reason = _infer_exit_reason(
                        account,
                        symbol,
                        pending_terminal_trade,
                        retry_max=retry_max,
                        retry_delay_secs=retry_delay_secs,
                        known_open_orders=known_open_orders,
                    )
                    tp_cancel = _cancel_order_if_present(
                        account,
                        symbol,
                        client_order_id=pending.get('tp_client_order_id'),
                        prefetched_order_res=order_checks.get('tp'),
                        known_open_orders=known_open_orders,
                        retry_max=retry_max,
                        retry_delay_secs=retry_delay_secs,
                    )
                    sl_cancel = _cancel_order_if_present(
                        account,
                        symbol,
                        client_order_id=pending.get('sl_client_order_id'),
                        prefetched_order_res=order_checks.get('sl'),
                        known_open_orders=known_open_orders,
                        retry_max=retry_max,
                        retry_delay_secs=retry_delay_secs,
                    )
                    ts_cancel = _cancel_order_if_present(
                        account,
                        symbol,
                        client_order_id=pending.get('time_stop_client_order_id'),
                        prefetched_order_res=order_checks.get('time_stop'),
                        known_open_orders=known_open_orders,
                        retry_max=retry_max,
                        retry_delay_secs=retry_delay_secs,
                    )
                    if (not tp_cancel.get('ok')) or (not sl_cancel.get('ok')) or (not ts_cancel.get('ok')):
                        had_blocking_error = True
                        cleanup_reason = tp_cancel.get('reason') or sl_cancel.get('reason') or ts_cancel.get('reason')
                        mark_error(
                            account,
                            symbol,
                            error_code='pending_terminal_cleanup_cancel_failed',
                            error_message=cleanup_reason,
                            error_bj=current_time_bj,
                        )
                        if audit_enabled:
                            write_event(account, 'pending_terminal_cleanup_cancel_failed', {
                                'symbol': symbol,
                                'bar_ts': current_time_ms,
                                'bar_bj': current_time_bj,
                                'source': source,
                                'order_root': pending.get('order_root'),
                                'exit_reason': exit_reason,
                                'entry_status': status,
                                'exchange_snapshot': {
                                    'entry_order': entry_res,
                                    'position': pos_res,
                                    'order_checks': order_checks,
                                    'tp_cancel': tp_cancel,
                                    'sl_cancel': sl_cancel,
                                    'ts_cancel': ts_cancel,
                                },
                            })
                        continue
                    set_pending_entry_order(account, symbol, None)
                    _clear_symbol_error(account, symbol)
                    _refresh_exit_cooldown(account, symbol, current_time_ms, cooldown_mins)
                    trade_projection_res = append_live_trade_projection_from_pending_terminal(
                        account,
                        live_cfg,
                        pending=pending,
                        entry_order_res=entry_res,
                        exit_reason=exit_reason,
                        order_checks=order_checks,
                        tp_cancel=tp_cancel,
                        sl_cancel=sl_cancel,
                        ts_cancel=ts_cancel,
                        current_time_ms=current_time_ms,
                        current_time_bj=current_time_bj,
                        source=f'{source}_pending_terminal',
                    )
                    if audit_enabled and not trade_projection_res.get('ok'):
                        write_event(account, 'live_trade_projection_write_failed', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': f'{source}_pending_terminal',
                            'order_root': pending.get('order_root'),
                            'reason': trade_projection_res.get('reason'),
                            'projection_path': trade_projection_res.get('path'),
                        })
                    _emit_exit_detected(
                        account,
                        live_cfg,
                        symbol=symbol,
                        exit_reason=exit_reason,
                        order_root=pending.get('order_root'),
                        trade_row=trade_projection_res.get('row') if trade_projection_res.get('ok') else None,
                    )
                    if audit_enabled:
                        write_event(account, 'entry_filled_but_position_missing', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': source,
                            'exit_reason': exit_reason,
                            'order_root': pending.get('order_root'),
                            'exchange_snapshot': {
                                'entry_order': entry_res,
                                'position': pos_res,
                                'order_checks': order_checks,
                                'tp_cancel': tp_cancel,
                                'sl_cancel': sl_cancel,
                                'ts_cancel': ts_cancel,
                            },
                        })
                        event_map = {
                            'TAKE_PROFIT': 'tp_filled',
                            'STOP_LOSS': 'sl_filled',
                            'TIME_STOP': 'time_stop_filled',
                            'UNKNOWN_EXIT': 'unknown_exit',
                        }
                        write_event(account, event_map.get(exit_reason, 'unknown_exit'), {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': f'{source}_pending_terminal',
                            'order_root': pending.get('order_root'),
                        })
                        write_event(account, 'pending_terminal_cancel_tp', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': source,
                            'exchange_snapshot': tp_cancel,
                        })
                        write_event(account, 'pending_terminal_cancel_sl', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': source,
                            'exchange_snapshot': sl_cancel,
                        })
                        write_event(account, 'pending_terminal_cancel_time_stop', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': source,
                            'exchange_snapshot': ts_cancel,
                        })
                        write_event(account, 'state_cleared_after_exit', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': f'{source}_pending_terminal',
                            'exit_reason': exit_reason,
                        })
                        write_event(account, 'cooldown_refreshed_after_pending_filled_terminal', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': source,
                            'exit_reason': exit_reason,
                        })
                else:
                    symbol_orders_res = (precheck or {}).get('orders') if snapshot is not None else None
                    if symbol_orders_res and symbol_orders_res.get('ok') and not (symbol_orders_res.get('data') or []):
                        tp_cancel = {'ok': True, 'reason': '', 'data': None, 'skipped': True, 'no_open_orders_snapshot': True}
                        sl_cancel = {'ok': True, 'reason': '', 'data': None, 'skipped': True, 'no_open_orders_snapshot': True}
                        ts_cancel = {'ok': True, 'reason': '', 'data': None, 'skipped': True, 'no_open_orders_snapshot': True}
                    else:
                        known_open_orders = list((symbol_orders_res or {}).get('data') or []) if symbol_orders_res and symbol_orders_res.get('ok') else None
                        tp_cancel = _cancel_order_if_present(
                            account,
                            symbol,
                            client_order_id=pending.get('tp_client_order_id'),
                            known_open_orders=known_open_orders,
                            retry_max=retry_max,
                            retry_delay_secs=retry_delay_secs,
                        )
                        sl_cancel = _cancel_order_if_present(
                            account,
                            symbol,
                            client_order_id=pending.get('sl_client_order_id'),
                            known_open_orders=known_open_orders,
                            retry_max=retry_max,
                            retry_delay_secs=retry_delay_secs,
                        )
                        ts_cancel = _cancel_order_if_present(
                            account,
                            symbol,
                            client_order_id=pending.get('time_stop_client_order_id'),
                            known_open_orders=known_open_orders,
                            retry_max=retry_max,
                            retry_delay_secs=retry_delay_secs,
                        )
                    if (not tp_cancel.get('ok')) or (not sl_cancel.get('ok')) or (not ts_cancel.get('ok')):
                        had_blocking_error = True
                        cleanup_reason = tp_cancel.get('reason') or sl_cancel.get('reason') or ts_cancel.get('reason')
                        mark_error(
                            account,
                            symbol,
                            error_code='pending_terminal_cleanup_cancel_failed',
                            error_message=cleanup_reason,
                            error_bj=current_time_bj,
                        )
                        if audit_enabled:
                            write_event(account, 'pending_terminal_cleanup_cancel_failed', {
                                'symbol': symbol,
                                'bar_ts': current_time_ms,
                                'bar_bj': current_time_bj,
                                'source': source,
                                'order_root': pending.get('order_root'),
                                'entry_status': status,
                                'exchange_snapshot': {
                                    'entry_order': entry_res,
                                    'position': pos_res,
                                    'tp_cancel': tp_cancel,
                                    'sl_cancel': sl_cancel,
                                    'ts_cancel': ts_cancel,
                                },
                            })
                        continue
                    set_pending_entry_order(account, symbol, None)
                    _clear_symbol_error(account, symbol)
                    if audit_enabled:
                        write_event(account, 'entry_terminal_detected', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': source,
                            'exchange_snapshot': entry_res,
                        })
                        write_event(account, 'pending_terminal_cancel_tp', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': source,
                            'exchange_snapshot': tp_cancel,
                        })
                        write_event(account, 'pending_terminal_cancel_sl', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': source,
                            'exchange_snapshot': sl_cancel,
                        })
                        write_event(account, 'pending_terminal_cancel_time_stop', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': source,
                            'exchange_snapshot': ts_cancel,
                        })
                        write_event(account, 'state_cleared_after_entry_terminal_without_fill', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': source,
                            'entry_status': status,
                            'order_root': pending.get('order_root'),
                        })

    return had_blocking_error

def _reset_inflight_exit_state(open_trade: dict[str, Any], current_time_bj: str) -> dict[str, Any]:
    open_trade['exit_submit_inflight'] = False
    open_trade['status'] = 'OPEN'
    open_trade['last_status_bj'] = current_time_bj
    return open_trade

def _reconcile_inflight_exit(account: str, symbol: str, open_trade: dict[str, Any], current_time_ms: int, current_time_bj: str, *, source: str, retry_max: int, retry_delay_secs: float, audit_enabled: bool, snapshot: dict[str, Any] | None = None) -> tuple[dict[str, Any], bool]:
    ts_exchange_order_id = open_trade.get('time_stop_exchange_order_id')
    ts_client_order_id = open_trade.get('time_stop_client_order_id')
    if ts_exchange_order_id is None and not ts_client_order_id:
        open_trade = _reset_inflight_exit_state(open_trade, current_time_bj)
        mark_error(
            account,
            symbol,
            error_code='time_stop_inflight_missing_identity',
            error_message='missing time-stop order identity while exit_submit_inflight=true',
            error_bj=current_time_bj,
        )
        if audit_enabled:
            write_event(account, 'time_stop_inflight_missing_identity', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'source': source,
                'order_root': open_trade.get('order_root'),
            })
        return open_trade, True

    known_open_orders = None
    if snapshot is not None:
        known_open_orders = list(((snapshot.get('open_orders_by_symbol') or {}).get(str(symbol).upper().strip()) or []))
        matched_ts_open_order = _find_open_order(
            known_open_orders,
            exchange_order_id=ts_exchange_order_id,
            client_order_id=ts_client_order_id,
        )
        if matched_ts_open_order is not None:
            if audit_enabled:
                write_event(account, 'time_stop_inflight_waiting', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'order_root': open_trade.get('order_root'),
                    'time_stop_client_order_id': ts_client_order_id,
                    'exchange_snapshot': {
                        'known_open_order': matched_ts_open_order,
                        'known_open_orders_count': len(known_open_orders),
                    },
                })
            return open_trade, True

    ts_order_res = _order_query(
        account,
        symbol,
        exchange_order_id=ts_exchange_order_id,
        client_order_id=ts_client_order_id,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not ts_order_res.get('ok'):
        mark_error(
            account,
            symbol,
            error_code='time_stop_inflight_query_error',
            error_message=ts_order_res.get('reason'),
            error_bj=current_time_bj,
        )
        if audit_enabled:
            write_event(account, 'time_stop_inflight_query_error', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'source': source,
                'order_root': open_trade.get('order_root'),
                'time_stop_client_order_id': ts_client_order_id,
                'exchange_snapshot': ts_order_res,
            })
        return open_trade, True

    ts_order = ts_order_res.get('data') or {}
    ts_status = str(ts_order.get('status') or '').upper()
    if ts_status in FILLED_ORDER_STATUSES:
        open_trade = _reset_inflight_exit_state(open_trade, current_time_bj)
        mark_error(
            account,
            symbol,
            error_code='time_stop_filled_but_position_still_open',
            error_message='time-stop order filled but position still open during inflight reconcile',
            error_bj=current_time_bj,
        )
        if audit_enabled:
            write_event(account, 'time_stop_filled_but_position_still_open', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'source': source,
                'order_root': open_trade.get('order_root'),
                'time_stop_client_order_id': ts_client_order_id,
                'exchange_snapshot': ts_order_res,
            })
            write_event(account, 'time_stop_inflight_reset_after_filled_position_open', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'source': source,
                'order_root': open_trade.get('order_root'),
                'time_stop_client_order_id': ts_client_order_id,
                'exchange_snapshot': ts_order_res,
            })
        return open_trade, True

    if ts_status in TERMINAL_ORDER_STATUSES:
        open_trade = _reset_inflight_exit_state(open_trade, current_time_bj)
        mark_error(
            account,
            symbol,
            error_code='time_stop_terminal_but_position_open',
            error_message=f'time-stop terminal status={ts_status} while position still open during inflight reconcile',
            error_bj=current_time_bj,
        )
        if audit_enabled:
            write_event(account, 'time_stop_terminal_but_position_open', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'source': source,
                'order_root': open_trade.get('order_root'),
                'time_stop_client_order_id': ts_client_order_id,
                'exchange_snapshot': ts_order_res,
            })
        return open_trade, True

    if audit_enabled:
        write_event(account, 'time_stop_inflight_waiting', {
            'symbol': symbol,
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'source': source,
            'order_root': open_trade.get('order_root'),
            'time_stop_client_order_id': ts_client_order_id,
            'exchange_snapshot': ts_order_res,
        })
    return open_trade, True

def _reconcile_open_trades(account: str, live_cfg: dict[str, Any], current_time_ms: int, current_time_bj: str, latest_closes: dict[str, float], max_hold_mins: int, min_profit_pct: float, *, source: str, snapshot: dict[str, Any] | None = None) -> bool:
    had_blocking_error = False
    state = load_live_state(account)
    symbols = state.get('symbols') or {}
    audit_enabled = bool(live_cfg.get('audit_enabled', True))
    notify_enabled = bool(live_cfg.get('notify_enabled', False))
    retry_max = int(live_cfg['order_retry_max'])
    retry_delay_secs = float(live_cfg['api_retry_delay_secs'])
    cooldown_mins = int(live_cfg['cooldown_mins'])
    for symbol, payload in symbols.items():
        if not isinstance(payload, dict):
            continue
        open_trade = payload.get('open_trade')
        if not isinstance(open_trade, dict):
            continue
        if not _is_snapback_owned_payload(open_trade):
            had_blocking_error = True
            mark_error(
                account,
                symbol,
                error_code='foreign_open_trade_blocked',
                error_message='snapback refused to reconcile non-SNP open_trade',
                error_bj=current_time_bj,
            )
            if audit_enabled:
                write_event(account, 'foreign_open_trade_blocked', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'foreign_payload': _foreign_payload_snapshot(open_trade),
                })
            continue
        precheck = precheck_exchange_blockers(account, symbol, snapshot=snapshot) if snapshot is not None else precheck_exchange_blockers(account, symbol)
        pos_res = precheck.get('position') or {'ok': False, 'reason': 'missing position snapshot', 'data': None}
        ord_res = precheck.get('orders') or {'ok': False, 'reason': 'missing orders snapshot', 'data': None}
        all_pos_res_from_precheck = precheck.get('positions_all_sides') or {'ok': False, 'reason': 'missing positions snapshot', 'data': None}
        mark_position_reconcile(account, symbol, reconcile_bj=current_time_bj)
        mark_order_reconcile(account, symbol, reconcile_bj=current_time_bj)
        if not pos_res.get('ok') or not ord_res.get('ok'):
            had_blocking_error = True
            reconcile_reason = ord_res.get('reason') or pos_res.get('reason')
            mark_error(
                account,
                symbol,
                error_code='open_trade_reconcile_query_failed',
                error_message=reconcile_reason,
                error_bj=current_time_bj,
            )
            if audit_enabled:
                write_event(account, 'exit_reconcile_error', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'source': source, 'exchange_snapshot': {'position': pos_res, 'orders': ord_res}})
            continue
        position = pos_res.get('data')
        open_orders = ord_res.get('data') or []
        if not position:
            all_pos_res = all_pos_res_from_precheck
            if not all_pos_res.get('ok'):
                had_blocking_error = True
                mark_error(
                    account,
                    symbol,
                    error_code='open_trade_all_positions_query_failed',
                    error_message=all_pos_res.get('reason'),
                    error_bj=current_time_bj,
                )
                if audit_enabled:
                    write_event(account, 'open_trade_all_positions_query_failed', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'source': source,
                        'order_root': open_trade.get('order_root'),
                        'exchange_snapshot': {
                            'long_position': pos_res,
                            'orders': ord_res,
                            'positions_all_sides': all_pos_res,
                        },
                    })
                continue

            symbol_key = str(symbol).upper().strip()
            symbol_positions = []
            for row in all_pos_res.get('data') or []:
                row_symbol = str(row.get('symbol') or '').upper().strip()
                if row_symbol == symbol_key:
                    symbol_positions.append(row)

            if symbol_positions:
                had_blocking_error = True
                mark_error(
                    account,
                    symbol,
                    error_code='open_trade_nonlong_position_detected',
                    error_message='local LONG open_trade exists but exchange has non-LONG position on same symbol',
                    error_bj=current_time_bj,
                )
                if audit_enabled:
                    write_event(account, 'open_trade_nonlong_position_detected', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'source': source,
                        'order_root': open_trade.get('order_root'),
                        'exchange_snapshot': {
                            'long_position': pos_res,
                            'orders': ord_res,
                            'positions_all_sides': all_pos_res,
                        },
                    })
                continue

            exit_reason, order_checks, blocking_reason = _infer_exit_reason(
                account,
                symbol,
                open_trade,
                retry_max=retry_max,
                retry_delay_secs=retry_delay_secs,
                known_open_orders=open_orders,
            )
            if ord_res.get('ok') and not open_orders:
                tp_cancel = {'ok': True, 'reason': '', 'data': None, 'skipped': True, 'no_open_orders_snapshot': True}
                sl_cancel = {'ok': True, 'reason': '', 'data': None, 'skipped': True, 'no_open_orders_snapshot': True}
                ts_cancel = {'ok': True, 'reason': '', 'data': None, 'skipped': True, 'no_open_orders_snapshot': True}
            else:
                tp_cancel = _cancel_order_if_present(account, symbol, exchange_order_id=open_trade.get('tp_order_exchange_id'), client_order_id=open_trade.get('tp_order_client_id'), prefetched_order_res=order_checks.get('tp'), known_open_orders=open_orders, retry_max=retry_max, retry_delay_secs=retry_delay_secs)
                sl_cancel = _cancel_order_if_present(account, symbol, exchange_order_id=open_trade.get('sl_order_exchange_id'), client_order_id=open_trade.get('sl_order_client_id'), prefetched_order_res=order_checks.get('sl'), known_open_orders=open_orders, retry_max=retry_max, retry_delay_secs=retry_delay_secs)
                ts_cancel = _cancel_order_if_present(account, symbol, exchange_order_id=open_trade.get('time_stop_exchange_order_id'), client_order_id=open_trade.get('time_stop_client_order_id'), prefetched_order_res=order_checks.get('time_stop'), known_open_orders=open_orders, retry_max=retry_max, retry_delay_secs=retry_delay_secs)

            if blocking_reason:
                had_blocking_error = True
                mark_error(
                    account,
                    symbol,
                    error_code='position_closed_exit_reason_infer_failed',
                    error_message=blocking_reason,
                    error_bj=current_time_bj,
                )
                if audit_enabled:
                    write_event(account, 'position_closed_exit_reason_infer_failed', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'source': source,
                        'order_root': open_trade.get('order_root'),
                        'entry_client_order_id': open_trade.get('entry_client_order_id'),
                        'tp_client_order_id': open_trade.get('tp_order_client_id'),
                        'sl_client_order_id': open_trade.get('sl_order_client_id'),
                        'time_stop_client_order_id': open_trade.get('time_stop_client_order_id'),
                        'exchange_snapshot': {
                            'position': pos_res,
                            'orders': ord_res,
                            'order_checks': order_checks,
                            'tp_cancel': tp_cancel,
                            'sl_cancel': sl_cancel,
                            'ts_cancel': ts_cancel,
                        },
                    })
                continue

            if (not tp_cancel.get('ok')) or (not sl_cancel.get('ok')) or (not ts_cancel.get('ok')):
                had_blocking_error = True
                cleanup_reason = tp_cancel.get('reason') or sl_cancel.get('reason') or ts_cancel.get('reason')
                mark_error(
                    account,
                    symbol,
                    error_code='position_closed_cleanup_cancel_failed',
                    error_message=cleanup_reason,
                    error_bj=current_time_bj,
                )
                if audit_enabled:
                    write_event(account, 'position_closed_cleanup_cancel_failed', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'source': source,
                        'exit_reason': exit_reason,
                        'order_root': open_trade.get('order_root'),
                        'entry_client_order_id': open_trade.get('entry_client_order_id'),
                        'tp_client_order_id': open_trade.get('tp_order_client_id'),
                        'sl_client_order_id': open_trade.get('sl_order_client_id'),
                        'time_stop_client_order_id': open_trade.get('time_stop_client_order_id'),
                        'exchange_snapshot': {
                            'position': pos_res,
                            'orders': ord_res,
                            'order_checks': order_checks,
                            'tp_cancel': tp_cancel,
                            'sl_cancel': sl_cancel,
                            'ts_cancel': ts_cancel,
                        },
                    })
                continue

            trade_projection_res = append_live_trade_projection_from_open_trade(
                account,
                live_cfg,
                open_trade=open_trade,
                exit_reason=exit_reason,
                order_checks=order_checks,
                tp_cancel=tp_cancel,
                sl_cancel=sl_cancel,
                ts_cancel=ts_cancel,
                current_time_ms=current_time_ms,
                current_time_bj=current_time_bj,
                source=source,
            )
            set_open_trade(account, symbol, None)
            set_pending_entry_order(account, symbol, None)
            _clear_symbol_error(account, symbol)
            _refresh_exit_cooldown(account, symbol, current_time_ms, cooldown_mins)
            _emit_exit_detected(
                account,
                live_cfg,
                symbol=symbol,
                exit_reason=exit_reason,
                order_root=open_trade.get('order_root'),
                trade_row=trade_projection_res.get('row') if trade_projection_res.get('ok') else None,
            )
            if audit_enabled and not trade_projection_res.get('ok'):
                write_event(account, 'live_trade_projection_write_failed', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'order_root': open_trade.get('order_root'),
                    'reason': trade_projection_res.get('reason'),
                    'projection_path': trade_projection_res.get('path'),
                })
            if audit_enabled:
                write_event(account, 'position_closed_detected', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'exit_reason': exit_reason,
                    'order_root': open_trade.get('order_root'),
                    'entry_client_order_id': open_trade.get('entry_client_order_id'),
                    'tp_client_order_id': open_trade.get('tp_order_client_id'),
                    'sl_client_order_id': open_trade.get('sl_order_client_id'),
                    'time_stop_client_order_id': open_trade.get('time_stop_client_order_id'),
                    'exchange_snapshot': {'position': pos_res, 'orders': ord_res, 'order_checks': order_checks, 'tp_cancel': tp_cancel, 'sl_cancel': sl_cancel, 'ts_cancel': ts_cancel},
                })
                event_map = {
                    'TAKE_PROFIT': 'tp_filled',
                    'STOP_LOSS': 'sl_filled',
                    'TIME_STOP': 'time_stop_filled',
                    EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN: 'sl_submit_failed_flatten_filled',
                    'UNKNOWN_EXIT': 'unknown_exit',
                }
                write_event(account, event_map.get(exit_reason, 'unknown_exit'), {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'source': source, 'order_root': open_trade.get('order_root')})
                write_event(
                    account,
                    'position_closed_cancel_protective_flatten' if exit_reason == EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN else 'position_closed_cancel_time_stop',
                    {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'source': source, 'exchange_snapshot': ts_cancel},
                )
                write_event(account, 'state_cleared_after_exit', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'source': source, 'exit_reason': exit_reason})
                write_event(account, 'cooldown_refreshed_after_exit', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'source': source})
            continue

        if not open_trade.get('exit_submit_inflight'):
            open_trade, exit_orders_changed = _ensure_exit_orders(
                account,
                symbol,
                open_trade,
                position,
                open_orders,
                live_cfg,
                current_time_ms,
                current_time_bj,
                source=source,
            )
            verify_res = _verify_open_trade_brackets(
                account,
                symbol,
                open_trade,
                retry_max=retry_max,
                retry_delay_secs=retry_delay_secs,
                snapshot=None if exit_orders_changed else snapshot,
            )
            bracket_gap_blocking = False
            if not verify_res.get('ok'):
                had_blocking_error = True
                bracket_gap_blocking = True
                verify_reason = (verify_res.get('orders') or {}).get('reason') or (verify_res.get('position') or {}).get('reason')
                mark_error(
                    account,
                    symbol,
                    error_code='open_trade_bracket_verify_failed',
                    error_message=verify_reason,
                    error_bj=current_time_bj,
                )
                if audit_enabled:
                    write_event(account, 'open_trade_bracket_verify_failed', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'source': source,
                        'order_root': open_trade.get('order_root'),
                        'exchange_snapshot': {
                            'position': verify_res.get('position'),
                            'orders': verify_res.get('orders'),
                        },
                    })
                if notify_enabled and live_cfg.get('notify_on_order_error', True):
                    _notify(True, f'[Snapback-Live] 风险告警 {symbol} | 持仓期 bracket 验证失败 | {verify_reason or "unknown"}')
            elif verify_res.get('position_open') and not (verify_res.get('tp_bound') and verify_res.get('sl_bound')):
                had_blocking_error = True
                bracket_gap_blocking = True
                mark_error(
                    account,
                    symbol,
                    error_code='open_trade_bracket_incomplete',
                    error_message=f"tp_bound={verify_res.get('tp_bound')}, sl_bound={verify_res.get('sl_bound')}",
                    error_bj=current_time_bj,
                )
                if audit_enabled:
                    write_event(account, 'critical_bracket_gap_during_reconcile', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'source': source,
                        'order_root': open_trade.get('order_root'),
                        'tp_bound': verify_res.get('tp_bound'),
                        'sl_bound': verify_res.get('sl_bound'),
                        'tp_client_order_id': open_trade.get('tp_order_client_id'),
                        'sl_client_order_id': open_trade.get('sl_order_client_id'),
                        'exchange_snapshot': {
                            'position': verify_res.get('position'),
                            'orders': verify_res.get('orders'),
                        },
                    })
                if notify_enabled and live_cfg.get('notify_on_order_error', True):
                    _notify(True, f'[Snapback-Live] 风险告警 {symbol} | 持仓期 bracket 仍不完整 | tp_bound={verify_res.get("tp_bound")} sl_bound={verify_res.get("sl_bound")}')
            else:
                _clear_symbol_error(account, symbol)
            set_open_trade(account, symbol, open_trade)
            if bracket_gap_blocking and audit_enabled:
                write_event(account, 'open_trade_reconcile_blocked_after_bracket_gap', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'order_root': open_trade.get('order_root'),
                    'verify_ok': verify_res.get('ok'),
                    'tp_bound': verify_res.get('tp_bound'),
                    'sl_bound': verify_res.get('sl_bound'),
                })

        if open_trade.get('exit_submit_inflight'):
            open_trade, should_skip = _reconcile_inflight_exit(
                account,
                symbol,
                open_trade,
                current_time_ms,
                current_time_bj,
                source=source,
                retry_max=retry_max,
                retry_delay_secs=retry_delay_secs,
                audit_enabled=audit_enabled,
                snapshot=snapshot,
            )
            set_open_trade(account, symbol, open_trade)

            if not open_trade.get('exit_submit_inflight'):
                # _reconcile_inflight_exit() only queries exchange state and mutates local flags;
                # it does not place/cancel any exchange orders. Reuse the symbol snapshot/query
                # we already fetched at the top of this reconcile cycle instead of querying again.
                reset_pos_res = pos_res
                reset_ord_res = ord_res
                if not reset_pos_res.get('ok') or not reset_ord_res.get('ok'):
                    had_blocking_error = True
                    verify_reason = reset_ord_res.get('reason') or reset_pos_res.get('reason')
                    mark_error(
                        account,
                        symbol,
                        error_code='time_stop_inflight_reset_repair_query_failed',
                        error_message=verify_reason,
                        error_bj=current_time_bj,
                    )
                    if audit_enabled:
                        write_event(account, 'time_stop_inflight_reset_repair_query_failed', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': source,
                            'order_root': open_trade.get('order_root'),
                            'exchange_snapshot': {
                                'position': reset_pos_res,
                                'orders': reset_ord_res,
                            },
                        })
                    continue

                reset_position = reset_pos_res.get('data')
                reset_orders = reset_ord_res.get('data') or []
                if reset_position:
                    open_trade, exit_orders_changed = _ensure_exit_orders(
                        account,
                        symbol,
                        open_trade,
                        reset_position,
                        reset_orders,
                        live_cfg,
                        current_time_ms,
                        current_time_bj,
                        source='time_stop_inflight_reset_repair',
                    )
                    set_open_trade(account, symbol, open_trade)
                    reset_verify_res = _verify_open_trade_brackets(
                        account,
                        symbol,
                        open_trade,
                        retry_max=retry_max,
                        retry_delay_secs=retry_delay_secs,
                        snapshot=None if exit_orders_changed else snapshot,
                    )
                    if audit_enabled:
                        write_event(account, 'time_stop_inflight_reset_repair_attempted', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': source,
                            'order_root': open_trade.get('order_root'),
                            'exchange_snapshot': {
                                'position': reset_pos_res,
                                'orders': reset_ord_res,
                            },
                        })
                    if not reset_verify_res.get('ok'):
                        had_blocking_error = True
                        verify_reason = (reset_verify_res.get('orders') or {}).get('reason') or (reset_verify_res.get('position') or {}).get('reason')
                        mark_error(
                            account,
                            symbol,
                            error_code='time_stop_inflight_reset_repair_verify_failed',
                            error_message=verify_reason,
                            error_bj=current_time_bj,
                        )
                        if audit_enabled:
                            write_event(account, 'time_stop_inflight_reset_repair_verify_failed', {
                                'symbol': symbol,
                                'bar_ts': current_time_ms,
                                'bar_bj': current_time_bj,
                                'source': source,
                                'order_root': open_trade.get('order_root'),
                                'exchange_snapshot': {
                                    'position': reset_verify_res.get('position'),
                                    'orders': reset_verify_res.get('orders'),
                                },
                            })
                        continue
                    if reset_verify_res.get('position_open') and not (reset_verify_res.get('tp_bound') and reset_verify_res.get('sl_bound')):
                        had_blocking_error = True
                        mark_error(
                            account,
                            symbol,
                            error_code='time_stop_inflight_reset_repair_bracket_incomplete',
                            error_message=f"tp_bound={reset_verify_res.get('tp_bound')}, sl_bound={reset_verify_res.get('sl_bound')}",
                            error_bj=current_time_bj,
                        )
                        if audit_enabled:
                            write_event(account, 'time_stop_inflight_reset_repair_bracket_incomplete', {
                                'symbol': symbol,
                                'bar_ts': current_time_ms,
                                'bar_bj': current_time_bj,
                                'source': source,
                                'order_root': open_trade.get('order_root'),
                                'tp_bound': reset_verify_res.get('tp_bound'),
                                'sl_bound': reset_verify_res.get('sl_bound'),
                                'tp_client_order_id': open_trade.get('tp_order_client_id'),
                                'sl_client_order_id': open_trade.get('sl_order_client_id'),
                                'exchange_snapshot': {
                                    'position': reset_verify_res.get('position'),
                                    'orders': reset_verify_res.get('orders'),
                                },
                            })
                        continue
                    _clear_symbol_error(account, symbol)

            if should_skip:
                continue

        latest_close = latest_closes.get(symbol)
        if latest_close is None:
            continue
        entry_ts = int(open_trade.get('entry_ts') or 0)
        entry_price = float(open_trade.get('entry_price') or 0.0)
        if entry_ts <= 0 or entry_price <= 0:
            continue
        held_mins = int((current_time_ms - entry_ts) / 60000)
        if held_mins < max_hold_mins:
            continue
        current_profit_pct = float(latest_close) / entry_price - 1.0
        open_trade['time_stop_last_check_bj'] = current_time_bj
        if current_profit_pct >= min_profit_pct:
            set_open_trade(account, symbol, open_trade)
            if audit_enabled:
                write_event(account, 'time_stop_skipped_profit_ok', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'held_mins': held_mins,
                    'current_profit_pct': current_profit_pct,
                    'min_profit_pct': min_profit_pct,
                    'order_root': open_trade.get('order_root'),
                })
            continue

        if audit_enabled:
            write_event(account, 'time_stop_triggered', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'source': source,
                'held_mins': held_mins,
                'current_profit_pct': current_profit_pct,
                'min_profit_pct': min_profit_pct,
                'order_root': open_trade.get('order_root'),
            })

        tp_cancel = _cancel_order_if_present(account, symbol, exchange_order_id=open_trade.get('tp_order_exchange_id'), client_order_id=open_trade.get('tp_order_client_id'), known_open_orders=open_orders, retry_max=retry_max, retry_delay_secs=retry_delay_secs)
        sl_cancel = _cancel_order_if_present(account, symbol, exchange_order_id=open_trade.get('sl_order_exchange_id'), client_order_id=open_trade.get('sl_order_client_id'), known_open_orders=open_orders, retry_max=retry_max, retry_delay_secs=retry_delay_secs)
        if audit_enabled:
            write_event(account, 'time_stop_cancel_tp_ok' if tp_cancel.get('ok') else 'time_stop_cancel_tp_failed', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'source': source, 'exchange_snapshot': tp_cancel})
            write_event(account, 'time_stop_cancel_sl_ok' if sl_cancel.get('ok') else 'time_stop_cancel_sl_failed', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'source': source, 'exchange_snapshot': sl_cancel})

        tp_cancel_status = str(((tp_cancel.get('data') or {}).get('status')) or '').upper()
        sl_cancel_status = str(((sl_cancel.get('data') or {}).get('status')) or '').upper()
        if tp_cancel_status in FILLED_ORDER_STATUSES or sl_cancel_status in FILLED_ORDER_STATUSES:
            had_blocking_error = True
            filled_reason = f'tp_status={tp_cancel_status or "NA"}, sl_status={sl_cancel_status or "NA"}'
            mark_error(
                account,
                symbol,
                error_code='time_stop_pre_submit_exit_already_filled',
                error_message=filled_reason,
                error_bj=current_time_bj,
            )
            if audit_enabled:
                write_event(account, 'time_stop_pre_submit_exit_already_filled', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'order_root': open_trade.get('order_root'),
                    'exchange_snapshot': {
                        'tp_cancel': tp_cancel,
                        'sl_cancel': sl_cancel,
                    },
                })
            continue

        if not tp_cancel.get('ok') or not sl_cancel.get('ok'):
            had_blocking_error = True
            cancel_reason = tp_cancel.get('reason') or sl_cancel.get('reason')
            mark_error(
                account,
                symbol,
                error_code='time_stop_pre_submit_cancel_failed',
                error_message=cancel_reason,
                error_bj=current_time_bj,
            )
            if audit_enabled:
                write_event(account, 'time_stop_pre_submit_cancel_failed', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'source': source,
                    'order_root': open_trade.get('order_root'),
                    'exchange_snapshot': {
                        'tp_cancel': tp_cancel,
                        'sl_cancel': sl_cancel,
                    },
                })
            continue

        ts_client_order_id = build_client_order_id(broker_id=BROKER_ID, strat=STRAT_CODE, leg=LEG_TIME_STOP, root=open_trade.get('order_root') or make_order_root())
        qty = float(position.get('qty') or open_trade.get('entry_qty') or 0.0)
        ts_res = place_time_stop_order(account, symbol, FIXED_POSITION_SIDE, qty, retry_max=retry_max, retry_delay_secs=retry_delay_secs, client_order_id=ts_client_order_id)
        if not ts_res.get('ok'):
            mark_error(account, symbol, error_code='time_stop_submit_failed', error_message=ts_res.get('reason'), error_bj=current_time_bj)
            if audit_enabled:
                write_event(account, 'time_stop_submit_failed', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'source': source, 'exchange_snapshot': ts_res})

            no_pre_submit_cancel_actions = bool(tp_cancel.get('skipped')) and bool(sl_cancel.get('skipped'))
            if no_pre_submit_cancel_actions:
                restore_pos_res = pos_res
                restore_ord_res = ord_res
            else:
                restore_pos_res = get_position(account, symbol, FIXED_POSITION_SIDE)
                restore_ord_res = get_open_orders(account, symbol)
            if restore_pos_res.get('ok') and restore_pos_res.get('data') and restore_ord_res.get('ok'):
                open_trade, repair_changed = _ensure_exit_orders(
                    account,
                    symbol,
                    open_trade,
                    restore_pos_res['data'],
                    restore_ord_res.get('data') or [],
                    live_cfg,
                    current_time_ms,
                    current_time_bj,
                    source='time_stop_submit_failed_repair',
                )
                set_open_trade(account, symbol, open_trade)
                if repair_changed:
                    repair_verify_res = _verify_open_trade_brackets(
                        account,
                        symbol,
                        open_trade,
                        retry_max=retry_max,
                        retry_delay_secs=retry_delay_secs,
                    )
                else:
                    time_stop_fail_repair_snapshot = {
                        'positions_by_symbol': {symbol: [restore_pos_res.get('data')]},
                        'open_orders_by_symbol': {symbol: list((restore_ord_res.get('data') or []))},
                    }
                    repair_verify_res = _verify_open_trade_brackets(
                        account,
                        symbol,
                        open_trade,
                        snapshot=time_stop_fail_repair_snapshot,
                        retry_max=retry_max,
                        retry_delay_secs=retry_delay_secs,
                    )
                if audit_enabled:
                    write_event(account, 'time_stop_submit_failed_repair_attempted', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'source': source,
                        'order_root': open_trade.get('order_root'),
                        'exchange_snapshot': {
                            'position': restore_pos_res,
                            'orders': restore_ord_res,
                        },
                    })
                if not repair_verify_res.get('ok'):
                    had_blocking_error = True
                    verify_reason = (repair_verify_res.get('orders') or {}).get('reason') or (repair_verify_res.get('position') or {}).get('reason')
                    mark_error(
                        account,
                        symbol,
                        error_code='time_stop_submit_failed_repair_verify_failed',
                        error_message=verify_reason,
                        error_bj=current_time_bj,
                    )
                    if audit_enabled:
                        write_event(account, 'time_stop_submit_failed_repair_verify_failed', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': source,
                            'order_root': open_trade.get('order_root'),
                            'exchange_snapshot': {
                                'position': repair_verify_res.get('position'),
                                'orders': repair_verify_res.get('orders'),
                            },
                        })
                elif repair_verify_res.get('position_open') and not (repair_verify_res.get('tp_bound') and repair_verify_res.get('sl_bound')):
                    had_blocking_error = True
                    mark_error(
                        account,
                        symbol,
                        error_code='time_stop_submit_failed_repair_bracket_incomplete',
                        error_message=f"tp_bound={repair_verify_res.get('tp_bound')}, sl_bound={repair_verify_res.get('sl_bound')}",
                        error_bj=current_time_bj,
                    )
                    if audit_enabled:
                        write_event(account, 'time_stop_submit_failed_repair_bracket_incomplete', {
                            'symbol': symbol,
                            'bar_ts': current_time_ms,
                            'bar_bj': current_time_bj,
                            'source': source,
                            'order_root': open_trade.get('order_root'),
                            'tp_bound': repair_verify_res.get('tp_bound'),
                            'sl_bound': repair_verify_res.get('sl_bound'),
                            'tp_client_order_id': open_trade.get('tp_order_client_id'),
                            'sl_client_order_id': open_trade.get('sl_order_client_id'),
                            'exchange_snapshot': {
                                'position': repair_verify_res.get('position'),
                                'orders': repair_verify_res.get('orders'),
                            },
                        })
                else:
                    _clear_symbol_error(account, symbol)
            else:
                had_blocking_error = True
                repair_reason = restore_ord_res.get('reason') or restore_pos_res.get('reason')
                mark_error(
                    account,
                    symbol,
                    error_code='time_stop_submit_failed_repair_query_failed',
                    error_message=repair_reason,
                    error_bj=current_time_bj,
                )
                if audit_enabled:
                    write_event(account, 'time_stop_submit_failed_repair_query_error', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'source': source,
                        'order_root': open_trade.get('order_root'),
                        'exchange_snapshot': {
                            'position': restore_pos_res,
                            'orders': restore_ord_res,
                        },
                    })
            continue

        open_trade['time_stop_client_order_id'] = ts_res['data'].get('client_order_id', ts_client_order_id)
        open_trade['time_stop_exchange_order_id'] = ts_res['data'].get('exchange_order_id')
        open_trade['exit_submit_inflight'] = True
        open_trade['status'] = 'EXIT_SUBMITTED'
        open_trade['last_status_bj'] = current_time_bj
        set_open_trade(account, symbol, open_trade)
        if audit_enabled:
            write_event(account, 'time_stop_submitted', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'source': source,
                'held_mins': held_mins,
                'current_profit_pct': current_profit_pct,
                'order_root': open_trade.get('order_root'),
                'time_stop_client_order_id': open_trade.get('time_stop_client_order_id'),
                'exchange_snapshot': ts_res,
            })
    return had_blocking_error

def _consume_signal_precheck_and_prepare(
    account: str,
    live_cfg: dict[str, Any],
    *,
    signal: dict[str, Any],
    current_time_ms: int,
    current_time_bj: str,
    c_bar_ts: int | None,
    c_bar_bj: str | None,
    source: str,
    exchange_snapshot: dict[str, Any] | None,
) -> dict[str, Any]:
    symbol = str(signal['symbol']).upper().strip()
    signal_digest = _signal_digest(signal)
    symbol_state = load_symbol_state(account, symbol)
    if symbol_state.get('last_processed_bar_ts') == current_time_ms:
        return {
            'ok': False,
            'terminal': True,
            'symbol': symbol,
            'signal_digest': signal_digest,
            'outcome': 'skipped_already_processed_bar',
            'reason': '',
        }

    mark_signal(
        account,
        symbol,
        signal_side=FIXED_POSITION_SIDE,
        signal_time_ts=int(signal.get('signal_time') or current_time_ms),
        signal_time_bj=signal.get('signal_time_bj'),
        c_bar_ts=c_bar_ts,
        c_bar_bj=c_bar_bj,
        signal_digest=signal_digest,
        signal_snapshot=signal,
    )
    _notify_signal_locked(account, live_cfg, signal)

    cooldown_until_ts = symbol_state.get('cooldown_until_ts')
    if cooldown_until_ts and int(cooldown_until_ts) > current_time_ms:
        if bool(live_cfg.get('audit_enabled', True)):
            write_event(account, 'precheck_skip', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'reason': 'cooldown_active',
            })
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return {
            'ok': False,
            'terminal': True,
            'symbol': symbol,
            'signal_digest': signal_digest,
            'outcome': 'skipped_cooldown_active',
            'reason': 'cooldown_active',
        }

    precheck_scope = require_consumer_precheck_scope(live_cfg)
    if precheck_scope == 'account_flat':
        exch = precheck_exchange_account_flat_blockers(account, snapshot=exchange_snapshot)
    else:
        exch = precheck_exchange_blockers(account, symbol, snapshot=exchange_snapshot)
    mark_position_reconcile(account, symbol, reconcile_bj=_now_bj_str())
    mark_order_reconcile(account, symbol, reconcile_bj=_now_bj_str())
    blocked, block_reason = has_position_or_orders(exch)
    if blocked:
        if block_reason in {'precheck_position_query_failed', 'precheck_positions_query_failed', 'precheck_orders_query_failed'}:
            mark_error(
                account,
                symbol,
                error_code=block_reason,
                error_message=(exch.get('orders') or {}).get('reason') or (exch.get('positions_all_sides') or {}).get('reason') or (exch.get('position') or {}).get('reason'),
                error_bj=current_time_bj,
            )
        if bool(live_cfg.get('audit_enabled', True)):
            write_event(account, 'precheck_skip', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'reason': block_reason,
                'precheck_scope': precheck_scope,
                'exchange_snapshot': exch,
            })
            _write_stage_record(account, 'stage7_precheck', {
                'event': 'precheck_skip',
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'symbol': symbol,
                'precheck_scope': precheck_scope,
                'precheck_blocked': True,
                'precheck_block_reason': block_reason,
                'precheck_position_exists': bool((exch.get('position') or {}).get('data')),
                'precheck_orders_exist': bool((exch.get('orders') or {}).get('data')),
                'precheck_any_position_exists': bool((exch.get('positions_all_sides') or {}).get('data')),
            })
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        outcome_map = {
            'exchange_has_position': 'skipped_exchange_has_position',
            'exchange_has_nonlong_position': 'skipped_exchange_has_nonlong_position',
            'exchange_has_open_orders': 'skipped_exchange_has_open_orders',
            'precheck_position_query_failed': 'failed_precheck_query',
            'precheck_positions_query_failed': 'failed_precheck_query',
            'precheck_orders_query_failed': 'failed_precheck_query',
        }
        return {
            'ok': False,
            'terminal': True,
            'symbol': symbol,
            'signal_digest': signal_digest,
            'outcome': outcome_map.get(block_reason, 'failed_precheck_query'),
            'reason': block_reason,
        }

    if bool(live_cfg.get('audit_enabled', True)):
        _write_stage_record(account, 'stage7_precheck', {
            'event': 'precheck_pass',
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'symbol': symbol,
            'precheck_scope': precheck_scope,
            'precheck_blocked': False,
            'precheck_block_reason': '',
            'precheck_position_exists': bool((exch.get('position') or {}).get('data')),
            'precheck_orders_exist': bool((exch.get('orders') or {}).get('data')),
            'precheck_any_position_exists': bool((exch.get('positions_all_sides') or {}).get('data')),
        })

    entry_notional_usdt = float(live_cfg['entry_notional_usdt'])
    current_price = float(signal.get('current_price') or 0.0)
    if current_price <= 0:
        mark_error(
            account,
            symbol,
            error_code='invalid_current_price',
            error_message=f'current_price={current_price}',
            error_bj=current_time_bj,
        )
        if bool(live_cfg.get('audit_enabled', True)):
            write_event(account, 'precheck_error', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'reason': 'invalid_current_price',
                'signal_snapshot': signal,
            })
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return {
            'ok': False,
            'terminal': True,
            'symbol': symbol,
            'signal_digest': signal_digest,
            'outcome': 'failed_invalid_current_price',
            'reason': f'current_price={current_price}',
        }

    resolved_sl_price = float(signal.get('sl_price') or 0.0)
    if resolved_sl_price > 0 and current_price <= resolved_sl_price:
        skip_reason = f'current_price={current_price:.8f} <= sl_price={resolved_sl_price:.8f}'
        mark_error(
            account,
            symbol,
            error_code='signal_invalid_sl_price',
            error_message=skip_reason,
            error_bj=current_time_bj,
        )
        if bool(live_cfg.get('audit_enabled', True)):
            write_event(account, 'precheck_skip_invalid_sl_price', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'reason': skip_reason,
                'signal_snapshot': signal,
            })
        if bool(live_cfg.get('notify_enabled', False)) and bool(live_cfg.get('notify_on_order_error', True)):
            _notify(
                True,
                f'[Snapback-Live] 跳过 {symbol} | 当前价已不高于SL，信号废弃 | current={current_price:.6f} | SL={resolved_sl_price:.6f}',
            )
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return {
            'ok': False,
            'terminal': True,
            'symbol': symbol,
            'signal_digest': signal_digest,
            'outcome': 'skipped_invalid_sl_price',
            'reason': skip_reason,
        }

    if 'pre_entry_min_sl_distance_pct' not in live_cfg:
        raise KeyError('live_config 缺少必要字段: pre_entry_min_sl_distance_pct')
    pre_entry_min_sl_distance_pct = float(live_cfg['pre_entry_min_sl_distance_pct'])
    if pre_entry_min_sl_distance_pct < 0:
        raise ValueError('live_config.pre_entry_min_sl_distance_pct must be >= 0')
    pre_entry_price_res = get_last_price(account, symbol)
    pre_entry_price = None
    if pre_entry_price_res.get('ok'):
        pre_entry_price = float((pre_entry_price_res.get('data') or {}).get('price') or 0.0)
    if pre_entry_price is None or pre_entry_price <= 0:
        reason = pre_entry_price_res.get('reason') or f'pre_entry_price={pre_entry_price}'
        mark_error(
            account,
            symbol,
            error_code='pre_entry_price_query_failed',
            error_message=reason,
            error_bj=current_time_bj,
        )
        if bool(live_cfg.get('audit_enabled', True)):
            write_event(account, 'pre_entry_price_guard_query_failed', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'price_source': 'CONTRACT_PRICE:futures_symbol_ticker',
                'current_price': current_price,
                'resolved_sl_price': resolved_sl_price,
                'min_sl_distance_pct': pre_entry_min_sl_distance_pct,
                'exchange_snapshot': pre_entry_price_res,
            })
            _write_stage_record(account, 'stage7_precheck', {
                'event': 'pre_entry_price_guard_query_failed',
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'symbol': symbol,
                'price_source': 'CONTRACT_PRICE:futures_symbol_ticker',
                'current_price': current_price,
                'resolved_sl_price': resolved_sl_price,
                'min_sl_distance_pct': pre_entry_min_sl_distance_pct,
                'reason': reason,
            })
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return {
            'ok': False,
            'terminal': True,
            'symbol': symbol,
            'signal_digest': signal_digest,
            'outcome': 'failed_pre_entry_price_query',
            'reason': reason,
        }

    pre_entry_sl_distance_pct = (pre_entry_price - resolved_sl_price) / resolved_sl_price if resolved_sl_price > 0 else None
    guard_payload = {
        'symbol': symbol,
        'bar_ts': current_time_ms,
        'bar_bj': current_time_bj,
        'price_source': 'CONTRACT_PRICE:futures_symbol_ticker',
        'current_price': current_price,
        'pre_entry_price': pre_entry_price,
        'resolved_sl_price': resolved_sl_price,
        'sl_distance_pct': pre_entry_sl_distance_pct,
        'min_sl_distance_pct': pre_entry_min_sl_distance_pct,
        'exchange_snapshot': pre_entry_price_res,
    }
    if pre_entry_sl_distance_pct is not None and pre_entry_sl_distance_pct < pre_entry_min_sl_distance_pct:
        skip_reason = (
            f'pre_entry_sl_distance_pct={pre_entry_sl_distance_pct:.8f} '
            f'< min_sl_distance_pct={pre_entry_min_sl_distance_pct:.8f}; '
            f'pre_entry_price={pre_entry_price:.8f}; sl_price={resolved_sl_price:.8f}'
        )
        if bool(live_cfg.get('audit_enabled', True)):
            write_event(account, 'pre_entry_price_guard_skip', {
                **guard_payload,
                'reason': skip_reason,
            })
            _write_stage_record(account, 'stage7_precheck', {
                'event': 'pre_entry_price_guard_skip',
                **guard_payload,
                'reason': skip_reason,
            })
        if bool(live_cfg.get('notify_enabled', False)) and bool(live_cfg.get('notify_on_signal_skip', True)):
            _notify(
                True,
                f'[Snapback-Live] 跳过 {symbol} | 入场前价格距SL过近 | pre_entry={pre_entry_price:.6f} | SL={resolved_sl_price:.6f} | distance={pre_entry_sl_distance_pct*100:.2f}% | min={pre_entry_min_sl_distance_pct*100:.2f}%',
            )
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return {
            'ok': False,
            'terminal': True,
            'symbol': symbol,
            'signal_digest': signal_digest,
            'outcome': 'skipped_pre_entry_price_too_close_to_sl',
            'reason': skip_reason,
        }

    if bool(live_cfg.get('audit_enabled', True)):
        write_event(account, 'pre_entry_price_guard_pass', guard_payload)
        _write_stage_record(account, 'stage7_precheck', {
            'event': 'pre_entry_price_guard_pass',
            **guard_payload,
        })

    requested_leverage = int(live_cfg['leverage'])
    leverage_res = ensure_leverage(account, symbol, requested_leverage)
    if bool(live_cfg.get('audit_enabled', True)):
        write_event(account, 'leverage_ensured' if leverage_res.get('ok') else 'leverage_ensure_failed', {
            'symbol': symbol,
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'requested_leverage': requested_leverage,
            'exchange_snapshot': leverage_res,
        })
    if not leverage_res.get('ok'):
        mark_error(
            account,
            symbol,
            error_code='leverage_ensure_failed',
            error_message=leverage_res.get('reason'),
            error_bj=current_time_bj,
        )
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return {
            'ok': False,
            'terminal': True,
            'symbol': symbol,
            'signal_digest': signal_digest,
            'outcome': 'failed_leverage_ensure',
            'reason': str(leverage_res.get('reason') or ''),
        }

    quantity = entry_notional_usdt / current_price
    retry_max = int(live_cfg['order_retry_max'])
    retry_delay_secs = float(live_cfg['api_retry_delay_secs'])
    order_root = make_order_root()
    entry_client_order_id = build_client_order_id(broker_id=BROKER_ID, strat=STRAT_CODE, leg=LEG_ENTRY, root=order_root)
    tp_client_order_id = build_client_order_id(broker_id=BROKER_ID, strat=STRAT_CODE, leg=LEG_TP, root=order_root)
    sl_client_order_id = build_client_order_id(broker_id=BROKER_ID, strat=STRAT_CODE, leg=LEG_SL, root=order_root)

    if bool(live_cfg.get('audit_enabled', True)):
        write_event(account, 'signal_detected', {
            'symbol': symbol,
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'c_bar_ts': c_bar_ts,
            'c_bar_bj': c_bar_bj,
            'signal_snapshot': signal,
            'order_root': order_root,
        })
        write_event(account, 'execution_plan_ready', {
            'symbol': symbol,
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'order_root': order_root,
            'entry_notional_usdt': entry_notional_usdt,
            'current_price': current_price,
            'quantity': quantity,
            'requested_leverage': requested_leverage,
        })

    return {
        'ok': True,
        'terminal': False,
        'symbol': symbol,
        'signal_digest': signal_digest,
        'order_root': order_root,
        'entry_notional_usdt': entry_notional_usdt,
        'current_price': current_price,
        'quantity': quantity,
        'retry_max': retry_max,
        'retry_delay_secs': retry_delay_secs,
        'requested_leverage': requested_leverage,
        'entry_client_order_id': entry_client_order_id,
        'tp_client_order_id': tp_client_order_id,
        'sl_client_order_id': sl_client_order_id,
        'signal': signal,
        'exchange_snapshot': exch,
    }

def _submit_entry_and_exit_orders(
    account: str,
    live_cfg: dict[str, Any],
    *,
    prep: dict[str, Any],
    current_time_ms: int,
    current_time_bj: str,
    c_bar_ts: int | None,
    c_bar_bj: str | None,
    timing_fields: dict[str, Any] | None,
    signal_eval_started_utc_ms: int | None,
    signal_eval_finished_utc_ms: int | None,
) -> dict[str, Any]:
    signal = prep['signal']
    symbol = prep['symbol']
    order_root = prep['order_root']
    entry_submit_started_utc_ms = int(time.time() * 1000)
    entry_res = place_entry_order(
        account,
        symbol,
        FIXED_POSITION_SIDE,
        prep['quantity'],
        retry_max=prep['retry_max'],
        retry_delay_secs=prep['retry_delay_secs'],
        client_order_id=prep['entry_client_order_id'],
    )
    entry_submit_finished_utc_ms = int(time.time() * 1000)
    if not entry_res['ok']:
        mark_error(account, symbol, error_code='entry_submit_failed', error_message=entry_res['reason'], error_bj=_now_bj_str())
        if bool(live_cfg.get('audit_enabled', True)):
            write_event(account, 'entry_submit_failed', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'reason': entry_res['reason'],
                'exchange_snapshot': entry_res,
                'order_root': order_root,
                'entry_client_order_id': prep['entry_client_order_id'],
            })
            stage8_payload = {
                'event': 'entry_submit_failed',
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'symbol': symbol,
                'order_root': order_root,
                'entry_client_order_id': prep['entry_client_order_id'],
                'reason': entry_res['reason'],
                'entry_ok': False,
                'signal_eval_started_utc_ms': signal_eval_started_utc_ms,
                'signal_eval_finished_utc_ms': signal_eval_finished_utc_ms,
                'entry_submit_started_utc_ms': entry_submit_started_utc_ms,
                'entry_submit_finished_utc_ms': entry_submit_finished_utc_ms,
                'entry_submit_started_bj': _fmt_bj_from_ms(entry_submit_started_utc_ms),
                'entry_submit_finished_bj': _fmt_bj_from_ms(entry_submit_finished_utc_ms),
            }
            if timing_fields:
                stage8_payload.update(timing_fields)
            _write_stage_record(account, 'stage8_exec', stage8_payload)
        if bool(live_cfg.get('notify_enabled', False)) and bool(live_cfg.get('notify_on_order_error', True)):
            _notify(True, f'[Snapback-Live] 入场失败 {symbol} | {entry_res["reason"]}')
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return {
            'ok': False,
            'terminal': True,
            'symbol': symbol,
            'signal_digest': prep['signal_digest'],
            'order_root': order_root,
            'outcome': 'failed_entry_submit',
            'reason': entry_res['reason'],
        }

    entry_data = entry_res['data']
    qty_for_exit = float(entry_data.get('executed_qty') or entry_data.get('qty') or 0.0)
    if qty_for_exit <= 0:
        qty_for_exit = prep['quantity']

    entry_fill_price, entry_fill_price_source = _resolve_live_entry_fill_price(
        account,
        symbol,
        entry_data,
        fallback_price=prep['current_price'],
    )
    resolved_tp_price, resolved_tp_price_source, selected_tp_pct = _resolve_tp_price_from_fill(signal, entry_fill_price)
    resolved_sl_price = float(signal.get('sl_price') or 0.0)

    pending_entry = _build_pending_entry(
        entry_res,
        signal,
        prep['entry_notional_usdt'],
        order_root=order_root,
        entry_client_order_id=prep['entry_client_order_id'],
        tp_client_order_id=prep['tp_client_order_id'],
        sl_client_order_id=prep['sl_client_order_id'],
        tp_price=resolved_tp_price,
        sl_price=resolved_sl_price,
        entry_fill_price_source=entry_fill_price_source,
        resolved_tp_price_source=resolved_tp_price_source,
        selected_tp_pct=selected_tp_pct,
    )
    set_pending_entry_order(account, symbol, pending_entry)

    if bool(live_cfg.get('audit_enabled', True)):
        write_event(account, 'entry_fill_observed', {
            'symbol': symbol,
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'order_root': order_root,
            'entry_fill_price': entry_fill_price,
            'entry_fill_price_source': entry_fill_price_source,
            'executed_qty': float(entry_data.get('executed_qty') or 0.0),
            'cum_quote': float(entry_data.get('cum_quote') or 0.0),
            'requested_leverage': prep.get('requested_leverage'),
        })
        write_event(account, 'exit_price_plan', {
            'symbol': symbol,
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'order_root': order_root,
            'signal_tp_price': float(signal.get('tp_price') or 0.0),
            'signal_sl_price': float(signal.get('sl_price') or 0.0),
            'entry_fill_price': entry_fill_price,
            'entry_fill_price_source': entry_fill_price_source,
            'selected_tp_pct': selected_tp_pct,
            'resolved_tp_price': resolved_tp_price,
            'resolved_tp_price_source': resolved_tp_price_source,
            'resolved_sl_price': resolved_sl_price,
        })

    sl_res = place_sl_order(
        account,
        symbol,
        FIXED_POSITION_SIDE,
        float(resolved_sl_price),
        retry_max=prep['retry_max'],
        retry_delay_secs=prep['retry_delay_secs'],
        client_order_id=prep['sl_client_order_id'],
    )
    tp_res: dict[str, Any] = {
        'ok': False,
        'reason': 'skipped_due_to_sl_failure',
        'data': None,
        'skipped': True,
    }

    if bool(live_cfg.get('audit_enabled', True)):
        write_event(account, 'entry_submitted', {
            'symbol': symbol,
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'c_bar_ts': c_bar_ts,
            'c_bar_bj': c_bar_bj,
            'exchange_snapshot': entry_res,
            'signal_snapshot': signal,
            'order_root': order_root,
            'entry_client_order_id': entry_res['data'].get('client_order_id', prep['entry_client_order_id']),
        })
        write_event(account, 'sl_submitted' if sl_res.get('ok') else 'sl_submit_failed', {
            'symbol': symbol,
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'exchange_snapshot': sl_res,
            'order_root': order_root,
            'sl_client_order_id': prep['sl_client_order_id'],
        })

    if not sl_res.get('ok'):
        ts_client_order_id = build_client_order_id(
            broker_id=BROKER_ID,
            strat=STRAT_CODE,
            leg=LEG_SL_FAIL_FLATTEN,
            root=order_root,
        )
        ts_res = place_time_stop_order(
            account,
            symbol,
            FIXED_POSITION_SIDE,
            qty_for_exit,
            retry_max=prep['retry_max'],
            retry_delay_secs=prep['retry_delay_secs'],
            client_order_id=ts_client_order_id,
            order_role=EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN,
        )
        fail_trade = _build_open_trade(
            entry_res,
            signal,
            {'ok': False, 'reason': 'tp_skipped_due_to_sl_failure', 'data': {}},
            {'ok': False, 'reason': sl_res.get('reason'), 'data': {}},
            prep['entry_notional_usdt'],
            order_root=order_root,
            entry_client_order_id=prep['entry_client_order_id'],
            tp_client_order_id=prep['tp_client_order_id'],
            sl_client_order_id=prep['sl_client_order_id'],
            tp_price=resolved_tp_price,
            sl_trigger_price=resolved_sl_price,
            entry_price_source=entry_fill_price_source,
            entry_submit_started_utc_ms=entry_submit_started_utc_ms,
            entry_submit_finished_utc_ms=entry_submit_finished_utc_ms,
            resolved_tp_price_source=resolved_tp_price_source,
            selected_tp_pct=selected_tp_pct,
        )
        fail_trade['tp_order_client_id'] = None
        fail_trade['tp_order_exchange_id'] = None
        fail_trade['sl_order_client_id'] = None
        fail_trade['sl_order_exchange_id'] = None
        fail_trade['time_stop_exit_reason'] = EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN
        fail_trade['protective_flatten_exit_reason'] = EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN
        fail_trade['protective_flatten_client_order_id'] = ts_client_order_id
        if ts_res.get('ok'):
            ts_data = ts_res.get('data') or {}
            fail_trade['time_stop_client_order_id'] = ts_data.get('client_order_id', ts_client_order_id)
            fail_trade['time_stop_exchange_order_id'] = ts_data.get('exchange_order_id')
            fail_trade['protective_flatten_client_order_id'] = ts_data.get('client_order_id', ts_client_order_id)
            fail_trade['protective_flatten_exchange_order_id'] = ts_data.get('exchange_order_id')
            fail_trade['exit_submit_inflight'] = True
            fail_trade['status'] = 'EXIT_SUBMITTED'
            fail_trade['last_status_bj'] = current_time_bj
        set_open_trade(account, symbol, fail_trade)
        set_pending_entry_order(account, symbol, None)
        mark_error(
            account,
            symbol,
            error_code='entry_sl_submit_failed',
            error_message=sl_res.get('reason'),
            error_bj=current_time_bj,
        )
        if bool(live_cfg.get('audit_enabled', True)):
            write_event(account, 'entry_sl_submit_failed', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'order_root': order_root,
                'entry_fill_price': entry_fill_price,
                'entry_fill_price_source': entry_fill_price_source,
                'resolved_sl_price': resolved_sl_price,
                'exchange_snapshot': sl_res,
            })
            write_event(account, 'sl_submit_failed_flatten_submitted' if ts_res.get('ok') else 'sl_submit_failed_flatten_submit_failed', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'source': 'entry_sl_fail_flatten',
                'order_root': order_root,
                'exit_reason': EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN,
                'protective_flatten_client_order_id': ts_client_order_id,
                'exchange_snapshot': ts_res,
            })
            stage8_payload = {
                'event': 'entry_submit',
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'symbol': symbol,
                'order_root': order_root,
                'entry_ok': bool(entry_res.get('ok')),
                'tp_ok': False,
                'sl_ok': False,
                'requested_leverage': prep.get('requested_leverage'),
                'entry_fill_price': entry_fill_price,
                'entry_fill_price_source': entry_fill_price_source,
                'resolved_tp_price': resolved_tp_price,
                'resolved_sl_price': resolved_sl_price,
                'entry_client_order_id': entry_res['data'].get('client_order_id', prep['entry_client_order_id']),
                'tp_client_order_id': prep['tp_client_order_id'],
                'sl_client_order_id': prep['sl_client_order_id'],
                'protective_flatten_client_order_id': ts_client_order_id,
                'protective_flatten_exit_reason': EXIT_REASON_SL_SUBMIT_FAILED_FLATTEN,
                'signal_eval_started_utc_ms': signal_eval_started_utc_ms,
                'signal_eval_finished_utc_ms': signal_eval_finished_utc_ms,
                'entry_submit_started_utc_ms': entry_submit_started_utc_ms,
                'entry_submit_finished_utc_ms': entry_submit_finished_utc_ms,
                'entry_submit_started_bj': _fmt_bj_from_ms(entry_submit_started_utc_ms),
                'entry_submit_finished_bj': _fmt_bj_from_ms(entry_submit_finished_utc_ms),
            }
            if timing_fields:
                stage8_payload.update(timing_fields)
            _write_stage_record(account, 'stage8_exec', stage8_payload)
        if bool(live_cfg.get('notify_enabled', False)) and bool(live_cfg.get('notify_on_order_error', True)):
            tail = '已提交SL失败保护强平' if ts_res.get('ok') else 'SL失败保护强平提交失败，请立即人工检查'
            _notify(
                True,
                f'[Snapback-Live] 风险告警 {symbol} | entry后SL建立失败 | entry≈{entry_fill_price:.6f} | SL={resolved_sl_price:.6f} | {tail}',
            )
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return {
            'ok': False,
            'terminal': True,
            'symbol': symbol,
            'signal_digest': prep['signal_digest'],
            'order_root': order_root,
            'outcome': 'failed_entry_sl_submit',
            'reason': str(sl_res.get('reason') or ''),
        }

    tp_res = place_tp_order(
        account,
        symbol,
        FIXED_POSITION_SIDE,
        qty_for_exit,
        float(resolved_tp_price),
        retry_max=prep['retry_max'],
        retry_delay_secs=prep['retry_delay_secs'],
        client_order_id=prep['tp_client_order_id'],
    )

    if bool(live_cfg.get('audit_enabled', True)):
        write_event(account, 'tp_submitted' if tp_res.get('ok') else 'tp_submit_failed', {
            'symbol': symbol,
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'exchange_snapshot': tp_res,
            'order_root': order_root,
            'tp_client_order_id': prep['tp_client_order_id'],
        })
        stage8_payload = {
            'event': 'entry_submit',
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'symbol': symbol,
            'order_root': order_root,
            'entry_ok': bool(entry_res.get('ok')),
            'tp_ok': bool(tp_res.get('ok')),
            'sl_ok': bool(sl_res.get('ok')),
            'requested_leverage': prep.get('requested_leverage'),
            'entry_fill_price': entry_fill_price,
            'entry_fill_price_source': entry_fill_price_source,
            'resolved_tp_price': resolved_tp_price,
            'resolved_sl_price': resolved_sl_price,
            'entry_client_order_id': entry_res['data'].get('client_order_id', prep['entry_client_order_id']),
            'tp_client_order_id': prep['tp_client_order_id'],
            'sl_client_order_id': prep['sl_client_order_id'],
            'signal_eval_started_utc_ms': signal_eval_started_utc_ms,
            'signal_eval_finished_utc_ms': signal_eval_finished_utc_ms,
            'entry_submit_started_utc_ms': entry_submit_started_utc_ms,
            'entry_submit_finished_utc_ms': entry_submit_finished_utc_ms,
            'entry_submit_started_bj': _fmt_bj_from_ms(entry_submit_started_utc_ms),
            'entry_submit_finished_bj': _fmt_bj_from_ms(entry_submit_finished_utc_ms),
        }
        if timing_fields:
            stage8_payload.update(timing_fields)
        _write_stage_record(account, 'stage8_exec', stage8_payload)

    open_trade = _build_open_trade(
        entry_res,
        signal,
        tp_res,
        sl_res,
        prep['entry_notional_usdt'],
        order_root=order_root,
        entry_client_order_id=prep['entry_client_order_id'],
        tp_client_order_id=prep['tp_client_order_id'],
        sl_client_order_id=prep['sl_client_order_id'],
        tp_price=resolved_tp_price,
        sl_trigger_price=resolved_sl_price,
        entry_price_source=entry_fill_price_source,
        entry_submit_started_utc_ms=entry_submit_started_utc_ms,
        entry_submit_finished_utc_ms=entry_submit_finished_utc_ms,
        resolved_tp_price_source=resolved_tp_price_source,
        selected_tp_pct=selected_tp_pct,
    )

    return {
        'ok': True,
        'terminal': False,
        'symbol': symbol,
        'signal_digest': prep['signal_digest'],
        'order_root': order_root,
        'entry_res': entry_res,
        'tp_res': tp_res,
        'sl_res': sl_res,
        'pending_entry': pending_entry,
        'open_trade': open_trade,
        'retry_max': prep['retry_max'],
        'retry_delay_secs': prep['retry_delay_secs'],
        'entry_client_order_id': prep['entry_client_order_id'],
        'tp_client_order_id': prep['tp_client_order_id'],
        'sl_client_order_id': prep['sl_client_order_id'],
        'entry_notional_usdt': prep['entry_notional_usdt'],
        'current_price': prep['current_price'],
        'requested_leverage': prep.get('requested_leverage'),
        'entry_fill_price': entry_fill_price,
        'entry_fill_price_source': entry_fill_price_source,
        'resolved_tp_price': resolved_tp_price,
        'resolved_sl_price': resolved_sl_price,
        'signal': signal,
    }

def _finalize_entry_state_after_submit(
    account: str,
    live_cfg: dict[str, Any],
    *,
    submit_ctx: dict[str, Any],
    current_time_ms: int,
    current_time_bj: str,
) -> dict[str, Any]:
    symbol = submit_ctx['symbol']
    open_trade = submit_ctx['open_trade']
    order_root = submit_ctx['order_root']

    set_open_trade(account, symbol, open_trade)
    set_pending_entry_order(account, symbol, None)
    _clear_symbol_error(account, symbol)
    _refresh_entry_cooldown(account, symbol, current_time_ms, int(live_cfg['cooldown_mins']))
    mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)

    if bool(live_cfg.get('audit_enabled', True)):
        write_event(account, 'cooldown_set_after_entry', {
            'symbol': symbol,
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'order_root': order_root,
        })

    if bool(live_cfg.get('notify_enabled', False)) and bool(live_cfg.get('notify_on_order_submit', True)):
        _notify(
            True,
            _build_entry_confirmed_message(
                account,
                symbol=symbol,
                open_trade=open_trade,
                fallback_entry_price=submit_ctx.get('entry_fill_price') or submit_ctx["current_price"],
            ),
        )

    return {
        'ok': True,
        'terminal': True,
        'symbol': symbol,
        'signal_digest': submit_ctx['signal_digest'],
        'order_root': order_root,
        'entry_client_order_id': submit_ctx['entry_client_order_id'],
        'tp_client_order_id': submit_ctx['tp_client_order_id'],
        'sl_client_order_id': submit_ctx['sl_client_order_id'],
        'entry_position_confirmed': True,
        'entry_still_pending': False,
        'entry_bracket_gap_critical': False,
        'cooldown_set': True,
        'outcome': 'consumed_open_confirmed',
        'reason': '',
    }

def maintain_consumer_once(
    account: str,
    strategy_cfg: dict[str, Any],
    live_cfg: dict[str, Any],
    *,
    current_time_ms: int,
    current_time_bj: str,
    latest_closes: dict[str, float],
    source: str,
    exchange_snapshot: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return _ledger_maintain_consumer_once_impl(
        account,
        strategy_cfg,
        live_cfg,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        latest_closes=latest_closes,
        source=source,
        exchange_snapshot=exchange_snapshot,
        reconcile_pending_entries_fn=_reconcile_pending_entries,
        reconcile_open_trades_fn=_reconcile_open_trades,
        extract_time_stop_config_fn=_extract_time_stop_config,
    )

def consume_signal(
    account: str,
    strategy_cfg: dict[str, Any],
    live_cfg: dict[str, Any],
    *,
    signal: dict[str, Any],
    current_time_ms: int,
    current_time_bj: str,
    c_bar_ts: int | None,
    c_bar_bj: str | None,
    source: str,
    exchange_snapshot: dict[str, Any] | None = None,
    timing_fields: dict[str, Any] | None = None,
    signal_eval_started_utc_ms: int | None = None,
    signal_eval_finished_utc_ms: int | None = None,
) -> dict[str, Any]:
    consume_started_perf = time.perf_counter()
    symbol = str(signal.get('symbol') or '').upper().strip()

    precheck_started_perf = time.perf_counter()
    prep = _consume_signal_precheck_and_prepare(
        account,
        live_cfg,
        signal=signal,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        c_bar_ts=c_bar_ts,
        c_bar_bj=c_bar_bj,
        source=source,
        exchange_snapshot=exchange_snapshot,
    )
    precheck_elapsed_ms = _perf_elapsed_ms(precheck_started_perf)
    if prep.get('terminal'):
        total_elapsed_ms = _perf_elapsed_ms(consume_started_perf)
        _log_perf_stage(
            'consume_signal',
            account=account,
            source=source,
            bar_bj=current_time_bj,
            symbol=symbol or prep.get('symbol'),
            signal_digest=prep.get('signal_digest'),
            precheck_elapsed_ms=precheck_elapsed_ms,
            submit_elapsed_ms=0,
            finalize_elapsed_ms=0,
            total_elapsed_ms=total_elapsed_ms,
            outcome=prep.get('outcome'),
            terminal=True,
        )
        return prep

    submit_started_perf = time.perf_counter()
    submit_ctx = _submit_entry_and_exit_orders(
        account,
        live_cfg,
        prep=prep,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        c_bar_ts=c_bar_ts,
        c_bar_bj=c_bar_bj,
        timing_fields=timing_fields,
        signal_eval_started_utc_ms=signal_eval_started_utc_ms,
        signal_eval_finished_utc_ms=signal_eval_finished_utc_ms,
    )
    submit_elapsed_ms = _perf_elapsed_ms(submit_started_perf)
    if submit_ctx.get('terminal'):
        total_elapsed_ms = _perf_elapsed_ms(consume_started_perf)
        _log_perf_stage(
            'consume_signal',
            account=account,
            source=source,
            bar_bj=current_time_bj,
            symbol=symbol or submit_ctx.get('symbol'),
            signal_digest=submit_ctx.get('signal_digest'),
            precheck_elapsed_ms=precheck_elapsed_ms,
            submit_elapsed_ms=submit_elapsed_ms,
            finalize_elapsed_ms=0,
            total_elapsed_ms=total_elapsed_ms,
            outcome=submit_ctx.get('outcome'),
            terminal=True,
            order_root=submit_ctx.get('order_root'),
        )
        return submit_ctx

    finalize_started_perf = time.perf_counter()
    final_res = _finalize_entry_state_after_submit(
        account,
        live_cfg,
        submit_ctx=submit_ctx,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
    )
    finalize_elapsed_ms = _perf_elapsed_ms(finalize_started_perf)
    total_elapsed_ms = _perf_elapsed_ms(consume_started_perf)

    _log_perf_stage(
        'consume_signal',
        account=account,
        source=source,
        bar_bj=current_time_bj,
        symbol=symbol or final_res.get('symbol'),
        signal_digest=final_res.get('signal_digest'),
        precheck_elapsed_ms=precheck_elapsed_ms,
        submit_elapsed_ms=submit_elapsed_ms,
        finalize_elapsed_ms=finalize_elapsed_ms,
        total_elapsed_ms=total_elapsed_ms,
        outcome=final_res.get('outcome'),
        terminal=True,
        order_root=final_res.get('order_root'),
    )

    return final_res
