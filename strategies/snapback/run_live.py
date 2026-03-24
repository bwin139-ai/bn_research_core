from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core.config_loader import StrategyConfig
from core.live.audit_log import get_live_audit_dir, write_event, write_runner_heartbeat, write_runner_started
from core.live.binance_exec import (
    cancel_order,
    get_open_orders,
    get_order,
    get_position,
    get_positions,
    place_entry_order,
    place_sl_order,
    place_time_stop_order,
    place_tp_order,
)
from core.live.custom_id import BROKER_ID, build_client_order_id, make_order_root
from core.live.live_state import (
    load_live_state,
    load_symbol_state,
    mark_error,
    mark_last_processed_bar,
    mark_loop_heartbeat,
    mark_order_reconcile,
    mark_position_reconcile,
    mark_signal,
    set_cooldown,
    set_open_trade,
    set_pending_entry_order,
)
from core.live.market_data import build_live_inputs, list_candidate_symbols
from core.message_bridge import send_to_bot
from strategies.snapback.logic import WashoutSnapbackStrategy

BJ = timezone(timedelta(hours=8))
FIXED_POSITION_SIDE = 'LONG'
STRAT_CODE = 'SNP'
LEG_ENTRY = 'EN'
LEG_TP = 'TP'
LEG_SL = 'SL'
LEG_TIME_STOP = 'TS'
TERMINAL_ORDER_STATUSES = {'FILLED', 'CANCELED', 'CANCELLED', 'EXPIRED', 'REJECTED'}
FILLED_ORDER_STATUSES = {'FILLED'}


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _now_bj_str() -> str:
    return datetime.now(timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')


def _now_utc_ms() -> int:
    return int(time.time() * 1000)


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')


def _cooldown_until(current_time_ms: int, cooldown_mins: int) -> tuple[int, str | None]:
    cooldown_until_ts = int(current_time_ms) + int(cooldown_mins) * 60 * 1000
    return cooldown_until_ts, _fmt_bj_from_ms(cooldown_until_ts)


def _load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f'配置文件缺失: {path}')
    with p.open('r', encoding='utf-8') as f:
        return json.load(f)


def _load_live_config(path: str) -> dict[str, Any]:
    data = _load_json(path)
    required = ['enabled', 'account', 'lookback_bars', 'exclude_symbols', 'entry_notional_usdt', 'leverage', 'cooldown_mins', 'order_retry_max', 'api_retry_delay_secs', 'audit_enabled', 'notify_enabled']
    for key in required:
        if key not in data:
            raise KeyError(f'live_config 缺少必要字段: {key}')
    if not isinstance(data.get('exclude_symbols'), list):
        raise TypeError('exclude_symbols 必须是 list')
    return data


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


def _json_sha256(data: Any) -> str:
    return _sha256_text(json.dumps(data, ensure_ascii=False, sort_keys=True, separators=(',', ':')))


def _read_text(path: str) -> str:
    return Path(path).read_text(encoding='utf-8')


def _write_config_snapshot(account: str, config_path: str, live_config_path: str, strategy_cfg: dict[str, Any], live_cfg: dict[str, Any]) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    ts_utc = now.strftime('%Y%m%dT%H%M%SZ')
    ts_bj = now.astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')
    account_key = str(account).strip()
    snapshot_dir = get_live_audit_dir() / 'config_snapshots'
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    config_abs = str(Path(config_path).resolve())
    live_config_abs = str(Path(live_config_path).resolve())
    config_text = _read_text(config_path)
    live_config_text = _read_text(live_config_path)

    snapshot = {
        'schema_version': 1,
        'run_mode': 'live',
        'strategy': 'snapback',
        'account': account_key,
        'snapshot_ts_utc': now.isoformat(),
        'snapshot_ts_bj': ts_bj,
        'config_path': config_path,
        'config_abs_path': config_abs,
        'live_config_path': live_config_path,
        'live_config_abs_path': live_config_abs,
        'strategy_config_sha256': _json_sha256(strategy_cfg),
        'live_config_sha256': _json_sha256(live_cfg),
        'strategy_config_file_sha256': _sha256_text(config_text),
        'live_config_file_sha256': _sha256_text(live_config_text),
        'strategy_config': strategy_cfg,
        'live_config': live_cfg,
    }

    snapshot_path = snapshot_dir / f'snapback_{account_key}_{ts_utc}.config_snapshot.json'
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + '\\n', encoding='utf-8')

    return {
        'snapshot_path': str(snapshot_path),
        'snapshot_bj': ts_bj,
        'strategy_config_sha256': snapshot['strategy_config_sha256'],
        'live_config_sha256': snapshot['live_config_sha256'],
        'strategy_config_file_sha256': snapshot['strategy_config_file_sha256'],
        'live_config_file_sha256': snapshot['live_config_file_sha256'],
    }


def _notify(enabled: bool, message: str, label: str = 'snapback') -> None:
    if enabled:
        send_to_bot(message, label=label)


def _active_symbols_from_state(account: str) -> set[str]:
    state = load_live_state(account)
    out: set[str] = set()
    for symbol, payload in (state.get('symbols') or {}).items():
        if not isinstance(payload, dict):
            continue
        if payload.get('pending_entry_order') or payload.get('open_trade'):
            out.add(str(symbol).upper().strip())
    return out


def _symbols_with_local_activity(account: str) -> set[str]:
    state = load_live_state(account)
    out: set[str] = set()
    for symbol, payload in (state.get('symbols') or {}).items():
        if not isinstance(payload, dict):
            continue
        if payload.get('pending_entry_order') or payload.get('open_trade'):
            out.add(str(symbol).upper().strip())
    return out


def _collect_active_state_errors(account: str) -> list[dict[str, Any]]:
    state = load_live_state(account)
    out: list[dict[str, Any]] = []
    for raw_symbol, payload in (state.get('symbols') or {}).items():
        if not isinstance(payload, dict):
            continue
        if not (payload.get('pending_entry_order') or payload.get('open_trade')):
            continue
        error_code = payload.get('last_error_code')
        if not error_code:
            continue
        out.append({
            'symbol': str(raw_symbol).upper().strip(),
            'error_code': error_code,
            'error_message': payload.get('last_error_message'),
            'error_bj': payload.get('last_error_bj'),
            'has_pending_entry_order': bool(payload.get('pending_entry_order')),
            'has_open_trade': bool(payload.get('open_trade')),
        })
    return out


def _collect_exchange_activity_snapshot(account: str) -> dict[str, Any]:
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


def _audit_orphan_exchange_activity(account: str, symbols: list[str], current_time_ms: int, current_time_bj: str, *, source: str, audit_enabled: bool, snapshot: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    if not audit_enabled:
        return findings

    local_active_symbols = set(snapshot.get('local_active_symbols') or []) if snapshot else _symbols_with_local_activity(account)
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
    for raw_symbol in symbols:
        symbol = str(raw_symbol).upper().strip()
        if not symbol or symbol in seen:
            continue
        seen.add(symbol)
        if symbol in local_active_symbols:
            continue

        exch = _precheck_exchange_blockers(account, symbol, snapshot=snapshot)
        mark_position_reconcile(account, symbol, reconcile_bj=current_time_bj)
        mark_order_reconcile(account, symbol, reconcile_bj=current_time_bj)

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
    return findings


def _sleep_until_next_closed_bar() -> None:
    now = datetime.now(timezone.utc)
    next_minute = (now.replace(second=0, microsecond=0) + timedelta(minutes=1, seconds=1))
    delay = max(0.2, (next_minute - now).total_seconds())
    time.sleep(delay)


def _signal_digest(signal: dict[str, Any]) -> str:
    base = {
        'symbol': signal.get('symbol'),
        'signal_time': signal.get('signal_time'),
        'action': signal.get('action'),
        'current_price': signal.get('current_price'),
        'tp_price': signal.get('tp_price'),
        'sl_price': signal.get('sl_price'),
    }
    return json.dumps(base, ensure_ascii=False, sort_keys=True)


def _precheck_exchange_blockers(account: str, symbol: str, snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
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

    pos_res = get_position(account, symbol, FIXED_POSITION_SIDE)
    all_pos_res = get_positions(account)
    ord_res = get_open_orders(account, symbol)

    symbol_positions: list[dict[str, Any]] = []
    if all_pos_res.get('ok'):
        for row in all_pos_res.get('data') or []:
            row_symbol = str(row.get('symbol') or '').upper().strip()
            if row_symbol == symbol_key:
                symbol_positions.append(row)

    return {
        'position': pos_res,
        'positions_all_sides': {
            'ok': bool(all_pos_res.get('ok')),
            'reason': all_pos_res.get('reason'),
            'data': symbol_positions,
        },
        'orders': ord_res,
    }


def _has_position_or_orders(snapshot: dict[str, Any]) -> tuple[bool, str]:
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


def _cancel_order_if_present(account: str, symbol: str, *, exchange_order_id: int | None = None, client_order_id: str | None = None, retry_max: int = 0, retry_delay_secs: float = 1.0) -> dict[str, Any]:
    if exchange_order_id is None and not client_order_id:
        return {'ok': True, 'reason': '', 'data': None, 'skipped': True}
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


def _infer_exit_reason(account: str, symbol: str, open_trade: dict[str, Any], retry_max: int, retry_delay_secs: float) -> tuple[str, dict[str, Any]]:
    checks: dict[str, Any] = {}
    ts_res = _order_query(account, symbol, exchange_order_id=open_trade.get('time_stop_exchange_order_id'), client_order_id=open_trade.get('time_stop_client_order_id'), retry_max=retry_max, retry_delay_secs=retry_delay_secs)
    tp_res = _order_query(account, symbol, exchange_order_id=open_trade.get('tp_order_exchange_id'), client_order_id=open_trade.get('tp_order_client_id'), retry_max=retry_max, retry_delay_secs=retry_delay_secs)
    sl_res = _order_query(account, symbol, exchange_order_id=open_trade.get('sl_order_exchange_id'), client_order_id=open_trade.get('sl_order_client_id'), retry_max=retry_max, retry_delay_secs=retry_delay_secs)
    checks['time_stop'] = ts_res
    checks['tp'] = tp_res
    checks['sl'] = sl_res
    if ts_res.get('ok') and ts_res.get('data') and str(ts_res['data'].get('status') or '').upper() in FILLED_ORDER_STATUSES:
        return 'TIME_STOP', checks
    if tp_res.get('ok') and tp_res.get('data') and str(tp_res['data'].get('status') or '').upper() in FILLED_ORDER_STATUSES:
        return 'TAKE_PROFIT', checks
    if sl_res.get('ok') and sl_res.get('data') and str(sl_res['data'].get('status') or '').upper() in FILLED_ORDER_STATUSES:
        return 'STOP_LOSS', checks
    return 'UNKNOWN_EXIT', checks



def _build_open_trade(entry_res: dict[str, Any], signal: dict[str, Any], tp_res: dict[str, Any], sl_res: dict[str, Any], entry_notional_usdt: float, *, order_root: str, entry_client_order_id: str, tp_client_order_id: str, sl_client_order_id: str) -> dict[str, Any]:
    entry = entry_res['data']
    tp = tp_res['data'] if tp_res.get('ok') else {}
    sl = sl_res['data'] if sl_res.get('ok') else {}
    return {
        'symbol': signal['symbol'],
        'side': FIXED_POSITION_SIDE,
        'order_root': order_root,
        'entry_client_order_id': entry.get('client_order_id', entry_client_order_id),
        'entry_exchange_order_id': entry.get('exchange_order_id'),
        'entry_ts': int(signal['signal_time']),
        'entry_bj': signal['signal_time_bj'],
        'entry_price': float(entry.get('avg_price') or signal.get('current_price') or 0.0),
        'entry_qty': float(entry.get('executed_qty') or entry.get('qty') or 0.0),
        'entry_notional_usdt': float(entry_notional_usdt),
        'signal_digest': _signal_digest(signal),
        'signal_snapshot': signal,
        'tp_order_client_id': tp.get('client_order_id', tp_client_order_id),
        'tp_order_exchange_id': tp.get('exchange_order_id'),
        'sl_order_client_id': sl.get('client_order_id', sl_client_order_id),
        'sl_order_exchange_id': sl.get('exchange_order_id'),
        'time_stop_client_order_id': None,
        'time_stop_exchange_order_id': None,
        'tp_price': float(signal.get('tp_price') or 0.0),
        'sl_trigger_price': float(signal.get('sl_price') or 0.0),
        'status': 'OPEN',
        'exit_submit_inflight': False,
        'last_status_bj': _now_bj_str(),
        'time_stop_last_check_bj': None,
    }


def _build_pending_entry(entry_res: dict[str, Any], signal: dict[str, Any], entry_notional_usdt: float, *, order_root: str, entry_client_order_id: str, tp_client_order_id: str, sl_client_order_id: str) -> dict[str, Any]:
    entry = entry_res['data']
    return {
        'symbol': signal['symbol'],
        'order_root': order_root,
        'client_order_id': entry.get('client_order_id', entry_client_order_id),
        'exchange_order_id': entry.get('exchange_order_id'),
        'signal_time': int(signal['signal_time']),
        'signal_time_bj': signal['signal_time_bj'],
        'current_price': float(signal.get('current_price') or 0.0),
        'entry_notional_usdt': float(entry_notional_usdt),
        'signal_digest': _signal_digest(signal),
        'signal_snapshot': signal,
        'tp_price': float(signal.get('tp_price') or 0.0),
        'sl_price': float(signal.get('sl_price') or 0.0),
        'tp_client_order_id': tp_client_order_id,
        'sl_client_order_id': sl_client_order_id,
        'created_bj': _now_bj_str(),
    }


def _recover_open_trade_from_pending(pending: dict[str, Any], position: dict[str, Any]) -> dict[str, Any]:
    signal_snapshot = pending.get('signal_snapshot') if isinstance(pending.get('signal_snapshot'), dict) else {}
    return {
        'symbol': str(pending.get('symbol') or position.get('symbol') or '').upper().strip(),
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


def _ensure_exit_orders(account: str, symbol: str, open_trade: dict[str, Any], position: dict[str, Any], open_orders: list[dict[str, Any]], live_cfg: dict[str, Any], current_time_ms: int, current_time_bj: str, *, source: str) -> dict[str, Any]:
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
    return open_trade

def _verify_open_trade_brackets(account: str, symbol: str, open_trade: dict[str, Any], *, retry_max: int, retry_delay_secs: float, snapshot: dict[str, Any] | None = None) -> dict[str, Any]:
    symbol_key = str(symbol).upper().strip()

    if snapshot is not None:
        all_pos_res = snapshot.get('positions') or {'ok': False, 'reason': 'missing positions snapshot', 'data': None}
        all_ord_res = snapshot.get('orders') or {'ok': False, 'reason': 'missing orders snapshot', 'data': None}
        symbol_positions = list((snapshot.get('positions_by_symbol') or {}).get(symbol_key) or [])
        symbol_open_orders = list((snapshot.get('open_orders_by_symbol') or {}).get(symbol_key) or [])

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
            precheck = _precheck_exchange_blockers(account, symbol, snapshot=snapshot)
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
        open_trade = payload.get('open_trade')
        if open_trade:
            set_pending_entry_order(account, symbol, None)
            continue
        entry_res = _order_query(
            account,
            symbol,
            exchange_order_id=pending.get('exchange_order_id'),
            client_order_id=pending.get('client_order_id'),
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
        )
        if snapshot is not None:
            precheck = _precheck_exchange_blockers(account, symbol, snapshot=snapshot)
            pos_res = precheck.get('position') or {'ok': False, 'reason': 'missing position snapshot', 'data': None}
        else:
            pos_res = get_position(account, symbol, FIXED_POSITION_SIDE)
        if not entry_res.get('ok') or not pos_res.get('ok'):
            had_blocking_error = True
            reconcile_reason = entry_res.get('reason') or pos_res.get('reason')
            mark_error(
                account,
                symbol,
                error_code='pending_reconcile_query_failed',
                error_message=reconcile_reason,
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
        if pos_res.get('ok') and pos_res.get('data'):
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

            recovered_trade = _ensure_exit_orders(
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
                snapshot=snapshot,
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
                    exit_reason, order_checks = _infer_exit_reason(
                        account,
                        symbol,
                        pending_terminal_trade,
                        retry_max=retry_max,
                        retry_delay_secs=retry_delay_secs,
                    )
                    tp_cancel = _cancel_order_if_present(
                        account,
                        symbol,
                        client_order_id=pending.get('tp_client_order_id'),
                        retry_max=retry_max,
                        retry_delay_secs=retry_delay_secs,
                    )
                    sl_cancel = _cancel_order_if_present(
                        account,
                        symbol,
                        client_order_id=pending.get('sl_client_order_id'),
                        retry_max=retry_max,
                        retry_delay_secs=retry_delay_secs,
                    )
                    ts_cancel = _cancel_order_if_present(
                        account,
                        symbol,
                        client_order_id=pending.get('time_stop_client_order_id'),
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
                    tp_cancel = _cancel_order_if_present(
                        account,
                        symbol,
                        client_order_id=pending.get('tp_client_order_id'),
                        retry_max=retry_max,
                        retry_delay_secs=retry_delay_secs,
                    )
                    sl_cancel = _cancel_order_if_present(
                        account,
                        symbol,
                        client_order_id=pending.get('sl_client_order_id'),
                        retry_max=retry_max,
                        retry_delay_secs=retry_delay_secs,
                    )
                    ts_cancel = _cancel_order_if_present(
                        account,
                        symbol,
                        client_order_id=pending.get('time_stop_client_order_id'),
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


def _reconcile_inflight_exit(account: str, symbol: str, open_trade: dict[str, Any], current_time_ms: int, current_time_bj: str, *, source: str, retry_max: int, retry_delay_secs: float, audit_enabled: bool) -> tuple[dict[str, Any], bool]:
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
        pos_res = get_position(account, symbol, FIXED_POSITION_SIDE)
        ord_res = get_open_orders(account, symbol)
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
            if snapshot is not None:
                all_pos_res = snapshot.get('positions') or {'ok': False, 'reason': 'missing positions snapshot', 'data': None}
            else:
                all_pos_res = get_positions(account)
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

            exit_reason, order_checks = _infer_exit_reason(account, symbol, open_trade, retry_max=retry_max, retry_delay_secs=retry_delay_secs)
            tp_cancel = _cancel_order_if_present(account, symbol, exchange_order_id=open_trade.get('tp_order_exchange_id'), client_order_id=open_trade.get('tp_order_client_id'), retry_max=retry_max, retry_delay_secs=retry_delay_secs)
            sl_cancel = _cancel_order_if_present(account, symbol, exchange_order_id=open_trade.get('sl_order_exchange_id'), client_order_id=open_trade.get('sl_order_client_id'), retry_max=retry_max, retry_delay_secs=retry_delay_secs)
            ts_cancel = _cancel_order_if_present(account, symbol, exchange_order_id=open_trade.get('time_stop_exchange_order_id'), client_order_id=open_trade.get('time_stop_client_order_id'), retry_max=retry_max, retry_delay_secs=retry_delay_secs)

            infer_reason = (order_checks.get('time_stop') or {}).get('reason') or (order_checks.get('tp') or {}).get('reason') or (order_checks.get('sl') or {}).get('reason')
            if infer_reason:
                had_blocking_error = True
                mark_error(
                    account,
                    symbol,
                    error_code='position_closed_exit_reason_infer_failed',
                    error_message=infer_reason,
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

            set_open_trade(account, symbol, None)
            set_pending_entry_order(account, symbol, None)
            _clear_symbol_error(account, symbol)
            _refresh_exit_cooldown(account, symbol, current_time_ms, cooldown_mins)
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
                event_map = {'TAKE_PROFIT': 'tp_filled', 'STOP_LOSS': 'sl_filled', 'TIME_STOP': 'time_stop_filled', 'UNKNOWN_EXIT': 'unknown_exit'}
                write_event(account, event_map.get(exit_reason, 'unknown_exit'), {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'source': source, 'order_root': open_trade.get('order_root')})
                write_event(account, 'position_closed_cancel_time_stop', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'source': source, 'exchange_snapshot': ts_cancel})
                write_event(account, 'state_cleared_after_exit', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'source': source, 'exit_reason': exit_reason})
                write_event(account, 'cooldown_refreshed_after_exit', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'source': source})
            continue

        if not open_trade.get('exit_submit_inflight'):
            open_trade = _ensure_exit_orders(
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
                snapshot=snapshot,
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
            if bracket_gap_blocking:
                if audit_enabled:
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
                continue

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
            )
            set_open_trade(account, symbol, open_trade)

            if not open_trade.get('exit_submit_inflight'):
                reset_pos_res = get_position(account, symbol, FIXED_POSITION_SIDE)
                reset_ord_res = get_open_orders(account, symbol)
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
                    open_trade = _ensure_exit_orders(
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
                        snapshot=snapshot,
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

        tp_cancel = _cancel_order_if_present(account, symbol, exchange_order_id=open_trade.get('tp_order_exchange_id'), client_order_id=open_trade.get('tp_order_client_id'), retry_max=retry_max, retry_delay_secs=retry_delay_secs)
        sl_cancel = _cancel_order_if_present(account, symbol, exchange_order_id=open_trade.get('sl_order_exchange_id'), client_order_id=open_trade.get('sl_order_client_id'), retry_max=retry_max, retry_delay_secs=retry_delay_secs)
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

            restore_pos_res = get_position(account, symbol, FIXED_POSITION_SIDE)
            restore_ord_res = get_open_orders(account, symbol)
            if restore_pos_res.get('ok') and restore_pos_res.get('data') and restore_ord_res.get('ok'):
                open_trade = _ensure_exit_orders(
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
                repair_verify_res = _verify_open_trade_brackets(
                    account,
                    symbol,
                    open_trade,
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

def _bootstrap_reconcile(account: str, strategy_cfg: dict[str, Any], live_cfg: dict[str, Any]) -> dict[str, Any]:
    current_time_ms = _now_utc_ms()
    current_time_bj = _fmt_bj_from_ms(current_time_ms) or _now_bj_str()
    audit_enabled = bool(live_cfg.get('audit_enabled', True))

    startup_snapshot = _collect_exchange_activity_snapshot(account)
    local_active_symbols = _symbols_with_local_activity(account)
    startup_snapshot['local_active_symbols'] = local_active_symbols
    startup_snapshot['startup_symbols'] = sorted(
        local_active_symbols
        | set(list_candidate_symbols(account, exclude_symbols=live_cfg.get('exclude_symbols') or []))
        | set(startup_snapshot['symbols'])
    )

    pending_reconcile_error = _reconcile_pending_entries(
        account,
        live_cfg,
        current_time_ms,
        current_time_bj,
        source='startup',
        snapshot=startup_snapshot,
    )
    max_hold_mins, min_profit_pct = _extract_time_stop_config(strategy_cfg)
    open_trade_reconcile_error = _reconcile_open_trades(
        account,
        live_cfg,
        current_time_ms,
        current_time_bj,
        {},
        max_hold_mins,
        min_profit_pct,
        source='startup',
        snapshot=startup_snapshot,
    )

    exchange_activity_snapshot = _collect_exchange_activity_snapshot(account)
    exchange_activity_snapshot['local_active_symbols'] = local_active_symbols
    startup_symbols = sorted(
        local_active_symbols
        | set(list_candidate_symbols(account, exclude_symbols=live_cfg.get('exclude_symbols') or []))
        | set(exchange_activity_snapshot['symbols'])
    )
    exchange_activity_snapshot['startup_symbols'] = startup_symbols
    if audit_enabled and not exchange_activity_snapshot.get('ok'):
        write_event(account, 'exchange_activity_snapshot_error', {
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'source': 'startup',
            'exchange_snapshot': {
                'positions': exchange_activity_snapshot.get('positions'),
                'orders': exchange_activity_snapshot.get('orders'),
            },
        })
    orphan_findings = _audit_orphan_exchange_activity(
        account,
        startup_symbols,
        current_time_ms,
        current_time_bj,
        source='startup',
        audit_enabled=audit_enabled,
        snapshot=exchange_activity_snapshot,
    )
    active_state_errors = _collect_active_state_errors(account)
    blocking = bool(
        pending_reconcile_error
        or open_trade_reconcile_error
        or (not exchange_activity_snapshot.get('ok'))
        or orphan_findings
        or active_state_errors
    )
    return {
        'blocking': blocking,
        'bar_ts': current_time_ms,
        'bar_bj': current_time_bj,
        'pending_reconcile_error': pending_reconcile_error,
        'open_trade_reconcile_error': open_trade_reconcile_error,
        'exchange_activity_snapshot_ok': bool(exchange_activity_snapshot.get('ok')),
        'orphan_findings': orphan_findings,
        'active_state_errors': active_state_errors,
    }


def _run_once(strategy_cfg: dict[str, Any], live_cfg: dict[str, Any]) -> None:
    account = str(live_cfg['account']).strip()
    notify_enabled = bool(live_cfg.get('notify_enabled', False))
    audit_enabled = bool(live_cfg.get('audit_enabled', True))
    lookback_bars = int(live_cfg['lookback_bars'])
    candidate_symbols = list_candidate_symbols(account, exclude_symbols=live_cfg.get('exclude_symbols') or [])
    exchange_activity_snapshot = _collect_exchange_activity_snapshot(account)
    exchange_activity_symbols = set(exchange_activity_snapshot['symbols'])
    local_activity_symbols = _symbols_with_local_activity(account)
    extra_reconcile_symbols = sorted((exchange_activity_symbols | local_activity_symbols) - set(candidate_symbols))

    candidate_md_res = build_live_inputs(account, candidate_symbols, lookback_bars, strategy_cfg)
    extra_md_res: dict[str, Any] | None = None
    if extra_reconcile_symbols:
        extra_md_res = build_live_inputs(account, extra_reconcile_symbols, lookback_bars, None)

    candidate_payload = candidate_md_res.get('data') if candidate_md_res.get('ok') else None
    extra_payload = extra_md_res.get('data') if extra_md_res and extra_md_res.get('ok') else None

    payload = candidate_payload or extra_payload
    if payload is None:
        if audit_enabled:
            write_event(account, 'data_error', {
                'reason': candidate_md_res.get('reason') or (extra_md_res or {}).get('reason') or 'no_live_inputs',
                'candidate_errors': candidate_md_res.get('errors'),
                'extra_errors': (extra_md_res or {}).get('errors'),
                'candidate_symbols_count': len(candidate_symbols),
                'extra_reconcile_symbols_count': len(extra_reconcile_symbols),
            })
        return

    current_time_ms = int(payload['latest_closed_bar_ts'])
    current_time_bj = payload['latest_closed_bar_bj']

    candidate_cross_section = candidate_payload['cross_section'] if candidate_payload else None
    candidate_full_df = dict(candidate_payload['full_df']) if candidate_payload else {}
    extra_full_df = dict(extra_payload['full_df']) if extra_payload else {}
    merged_full_df = dict(candidate_full_df)
    merged_full_df.update(extra_full_df)
    latest_closes = {
        str(symbol).upper().strip(): float(df.loc[current_time_ms, 'close'])
        for symbol, df in merged_full_df.items()
        if current_time_ms in df.index
    }
    max_hold_mins, min_profit_pct = _extract_time_stop_config(strategy_cfg)

    pending_reconcile_error = _reconcile_pending_entries(account, live_cfg, current_time_ms, current_time_bj, source='loop')
    open_trade_reconcile_error = _reconcile_open_trades(account, live_cfg, current_time_ms, current_time_bj, latest_closes, max_hold_mins, min_profit_pct, source='loop')
    if audit_enabled and not exchange_activity_snapshot.get('ok'):
        write_event(account, 'exchange_activity_snapshot_error', {
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'source': 'loop',
            'exchange_snapshot': {
                'positions': exchange_activity_snapshot.get('positions'),
                'orders': exchange_activity_snapshot.get('orders'),
            },
        })

    exchange_activity_snapshot['local_active_symbols'] = local_activity_symbols
    orphan_findings = _audit_orphan_exchange_activity(
        account,
        sorted(set(candidate_symbols) | exchange_activity_symbols),
        current_time_ms,
        current_time_bj,
        source='loop',
        audit_enabled=audit_enabled,
        snapshot=exchange_activity_snapshot,
    )

    if orphan_findings:
        if audit_enabled:
            write_event(account, 'signal_scan_skipped_orphan_exchange_activity', {
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'orphan_exchange_activity': orphan_findings,
                'candidate_symbols_count': len(candidate_symbols),
                'extra_reconcile_symbols_count': len(extra_reconcile_symbols),
            })
        for symbol in merged_full_df.keys():
            mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return

    if pending_reconcile_error or open_trade_reconcile_error:
        if audit_enabled:
            write_event(account, 'signal_scan_skipped_reconcile_query_error', {
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'pending_reconcile_error': pending_reconcile_error,
                'open_trade_reconcile_error': open_trade_reconcile_error,
                'candidate_symbols_count': len(candidate_symbols),
                'extra_reconcile_symbols_count': len(extra_reconcile_symbols),
            })
        for symbol in merged_full_df.keys():
            mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return

    required_reconcile_symbols = sorted(local_activity_symbols | exchange_activity_symbols)
    missing_reconcile_symbols = [symbol for symbol in required_reconcile_symbols if symbol not in latest_closes]
    if missing_reconcile_symbols:
        if audit_enabled:
            write_event(account, 'signal_scan_skipped_missing_reconcile_data', {
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'missing_reconcile_symbols': missing_reconcile_symbols,
                'candidate_symbols_count': len(candidate_symbols),
                'extra_reconcile_symbols_count': len(extra_reconcile_symbols),
                'candidate_reason': candidate_md_res.get('reason'),
                'extra_reason': (extra_md_res or {}).get('reason'),
            })
        for symbol in merged_full_df.keys():
            mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return

    if not candidate_payload:
        if audit_enabled:
            write_event(account, 'signal_scan_skipped_no_candidate_data', {
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'candidate_reason': candidate_md_res.get('reason'),
                'candidate_errors': candidate_md_res.get('errors'),
                'extra_reconcile_symbols_count': len(extra_reconcile_symbols),
            })
        for symbol in merged_full_df.keys():
            mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return

    if not exchange_activity_snapshot.get('ok'):
        if audit_enabled:
            write_event(account, 'signal_scan_skipped_exchange_activity_query_error', {
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'candidate_symbols_count': len(candidate_symbols),
                'extra_reconcile_symbols_count': len(extra_reconcile_symbols),
                'exchange_snapshot': {
                    'positions': exchange_activity_snapshot.get('positions'),
                    'orders': exchange_activity_snapshot.get('orders'),
                },
            })
        for symbol in merged_full_df.keys():
            mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return

    active_state_errors = _collect_active_state_errors(account)
    if active_state_errors:
        if audit_enabled:
            write_event(account, 'signal_scan_skipped_active_state_error', {
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'active_state_errors': active_state_errors,
                'candidate_symbols_count': len(candidate_symbols),
                'extra_reconcile_symbols_count': len(extra_reconcile_symbols),
            })
        for symbol in merged_full_df.keys():
            mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return

    cross_section = candidate_cross_section
    full_df = candidate_full_df
    strategy = WashoutSnapbackStrategy(strategy_cfg)
    active_symbols = _active_symbols_from_state(account) | exchange_activity_symbols
    signal = strategy.on_kline_close(current_time_ms, cross_section, active_symbols, full_df)

    if not signal:
        if audit_enabled:
            write_event(account, 'signal_none', {
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'freshest_bar_ts': candidate_payload.get('freshest_bar_ts'),
                'freshest_bar_bj': candidate_payload.get('freshest_bar_bj'),
                'stale_cutoff_bj': candidate_payload.get('stale_cutoff_bj'),
                'symbol_count': candidate_payload['symbol_count'],
                'stale_symbol_count': candidate_payload.get('stale_symbol_count', 0),
                'extra_reconcile_symbols_count': len(extra_reconcile_symbols),
            })
        for symbol in merged_full_df.keys():
            mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return

    symbol = str(signal['symbol']).upper().strip()
    signal_digest = _signal_digest(signal)
    symbol_state = load_symbol_state(account, symbol)
    if symbol_state.get('last_processed_bar_ts') == current_time_ms:
        return

    mark_signal(account, symbol, signal_side=FIXED_POSITION_SIDE, signal_bar_ts=current_time_ms, signal_digest=signal_digest, signal_snapshot=signal)

    cooldown_until_ts = symbol_state.get('cooldown_until_ts')
    if cooldown_until_ts and int(cooldown_until_ts) > current_time_ms:
        if audit_enabled:
            write_event(account, 'precheck_skip', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'reason': 'cooldown_active'})
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return

    exch = _precheck_exchange_blockers(account, symbol, snapshot=exchange_activity_snapshot)
    mark_position_reconcile(account, symbol, reconcile_bj=_now_bj_str())
    mark_order_reconcile(account, symbol, reconcile_bj=_now_bj_str())
    blocked, block_reason = _has_position_or_orders(exch)
    if blocked:
        if block_reason in {'precheck_position_query_failed', 'precheck_positions_query_failed', 'precheck_orders_query_failed'}:
            mark_error(
                account,
                symbol,
                error_code=block_reason,
                error_message=(exch.get('orders') or {}).get('reason') or (exch.get('positions_all_sides') or {}).get('reason') or (exch.get('position') or {}).get('reason'),
                error_bj=current_time_bj,
            )
        if audit_enabled:
            write_event(account, 'precheck_skip', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'reason': block_reason, 'exchange_snapshot': exch})
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return

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
        if audit_enabled:
            write_event(account, 'precheck_error', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'reason': 'invalid_current_price',
                'signal_snapshot': signal,
            })
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return

    quantity = entry_notional_usdt / current_price
    retry_max = int(live_cfg['order_retry_max'])
    retry_delay_secs = float(live_cfg['api_retry_delay_secs'])
    order_root = make_order_root()
    entry_client_order_id = build_client_order_id(broker_id=BROKER_ID, strat=STRAT_CODE, leg=LEG_ENTRY, root=order_root)
    tp_client_order_id = build_client_order_id(broker_id=BROKER_ID, strat=STRAT_CODE, leg=LEG_TP, root=order_root)
    sl_client_order_id = build_client_order_id(broker_id=BROKER_ID, strat=STRAT_CODE, leg=LEG_SL, root=order_root)

    if audit_enabled:
        write_event(account, 'signal_detected', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'signal_snapshot': signal, 'order_root': order_root})

    entry_res = place_entry_order(account, symbol, FIXED_POSITION_SIDE, quantity, retry_max=retry_max, retry_delay_secs=retry_delay_secs, client_order_id=entry_client_order_id)
    if not entry_res['ok']:
        mark_error(account, symbol, error_code='entry_submit_failed', error_message=entry_res['reason'], error_bj=_now_bj_str())
        if audit_enabled:
            write_event(account, 'entry_submit_failed', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'reason': entry_res['reason'], 'exchange_snapshot': entry_res, 'order_root': order_root, 'entry_client_order_id': entry_client_order_id})
        if notify_enabled and live_cfg.get('notify_on_order_error', True):
            _notify(True, f'[Snapback-Live] 入场失败 {symbol} | {entry_res["reason"]}')
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return

    pending_entry = _build_pending_entry(
        entry_res,
        signal,
        entry_notional_usdt,
        order_root=order_root,
        entry_client_order_id=entry_client_order_id,
        tp_client_order_id=tp_client_order_id,
        sl_client_order_id=sl_client_order_id,
    )
    set_pending_entry_order(account, symbol, pending_entry)
    entry_data = entry_res['data']
    qty_for_exit = float(entry_data.get('executed_qty') or entry_data.get('qty') or 0.0)
    if qty_for_exit <= 0:
        qty_for_exit = quantity

    tp_res = place_tp_order(account, symbol, FIXED_POSITION_SIDE, qty_for_exit, float(signal['tp_price']), retry_max=retry_max, retry_delay_secs=retry_delay_secs, client_order_id=tp_client_order_id)
    sl_res = place_sl_order(account, symbol, FIXED_POSITION_SIDE, float(signal['sl_price']), retry_max=retry_max, retry_delay_secs=retry_delay_secs, client_order_id=sl_client_order_id)

    if audit_enabled:
        write_event(account, 'entry_submitted', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'exchange_snapshot': entry_res, 'signal_snapshot': signal, 'order_root': order_root, 'entry_client_order_id': entry_res['data'].get('client_order_id', entry_client_order_id)})
        write_event(account, 'tp_submitted' if tp_res.get('ok') else 'tp_submit_failed', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'exchange_snapshot': tp_res, 'order_root': order_root, 'tp_client_order_id': tp_client_order_id})
        write_event(account, 'sl_submitted' if sl_res.get('ok') else 'sl_submit_failed', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'exchange_snapshot': sl_res, 'order_root': order_root, 'sl_client_order_id': sl_client_order_id})

    open_trade = _build_open_trade(entry_res, signal, tp_res, sl_res, entry_notional_usdt, order_root=order_root, entry_client_order_id=entry_client_order_id, tp_client_order_id=tp_client_order_id, sl_client_order_id=sl_client_order_id)

    pos_after_entry = get_position(account, symbol, FIXED_POSITION_SIDE)
    orders_after_entry = get_open_orders(account, symbol)
    pos_after_entry_data = pos_after_entry.get('data') if pos_after_entry.get('ok') else None
    orders_after_entry_data = orders_after_entry.get('data') or [] if orders_after_entry.get('ok') else []
    tp_bound_initial = _find_open_order(
        orders_after_entry_data,
        exchange_order_id=open_trade.get('tp_order_exchange_id'),
        client_order_id=open_trade.get('tp_order_client_id'),
    ) is not None if orders_after_entry.get('ok') else False
    sl_bound_initial = _find_open_order(
        orders_after_entry_data,
        exchange_order_id=open_trade.get('sl_order_exchange_id'),
        client_order_id=open_trade.get('sl_order_client_id'),
    ) is not None if orders_after_entry.get('ok') else False
    should_repair_brackets = bool(
        pos_after_entry_data and (
            (not tp_res.get('ok'))
            or (not sl_res.get('ok'))
            or (orders_after_entry.get('ok') and not (tp_bound_initial and sl_bound_initial))
        )
    )
    if audit_enabled:
        write_event(account, 'entry_immediate_bracket_check', {
            'symbol': symbol,
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'order_root': order_root,
            'tp_submit_ok': bool(tp_res.get('ok')),
            'sl_submit_ok': bool(sl_res.get('ok')),
            'tp_bound_initial': tp_bound_initial,
            'sl_bound_initial': sl_bound_initial,
            'position_snapshot': pos_after_entry,
            'open_orders_snapshot': orders_after_entry,
        })

    if should_repair_brackets:
        open_trade = _ensure_exit_orders(
            account,
            symbol,
            open_trade,
            pos_after_entry_data,
            orders_after_entry_data,
            live_cfg,
            current_time_ms,
            current_time_bj,
            source='entry_immediate_repair',
        )

    entry_fast_terminal = False
    entry_still_pending = False
    entry_position_confirmed = False
    entry_bracket_gap_critical = False
    verify_pos_res = get_position(account, symbol, FIXED_POSITION_SIDE)
    verify_orders_res = get_open_orders(account, symbol)
    verify_position = verify_pos_res.get('data') if verify_pos_res.get('ok') else None
    verify_orders = verify_orders_res.get('data') or [] if verify_orders_res.get('ok') else []
    if not verify_pos_res.get('ok') or not verify_orders_res.get('ok'):
        verify_reason = verify_orders_res.get('reason') or verify_pos_res.get('reason')
        mark_error(
            account,
            symbol,
            error_code='entry_immediate_bracket_verify_failed',
            error_message=verify_reason,
            error_bj=current_time_bj,
        )
        if audit_enabled:
            write_event(account, 'entry_immediate_bracket_verify_failed', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'order_root': order_root,
                'exchange_snapshot': {
                    'position': verify_pos_res,
                    'orders': verify_orders_res,
                },
            })
            write_event(account, 'critical_bracket_gap_after_entry', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'reason': 'entry_immediate_bracket_verify_failed',
                'order_root': order_root,
                'tp_client_order_id': open_trade.get('tp_order_client_id'),
                'sl_client_order_id': open_trade.get('sl_order_client_id'),
                'exchange_snapshot': {
                    'position': verify_pos_res,
                    'orders': verify_orders_res,
                },
            })
        entry_bracket_gap_critical = True
        if notify_enabled and live_cfg.get('notify_on_order_error', True):
            _notify(True, f'[Snapback-Live] 风险告警 {symbol} | entry后 bracket 验证失败 | {verify_reason or "unknown"}')
    elif not verify_position:
        entry_still_pending = True
        if audit_enabled:
            write_event(account, 'entry_pending_waiting_fill', {
                'symbol': symbol,
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'order_root': order_root,
                'entry_client_order_id': open_trade.get('entry_client_order_id'),
                'tp_client_order_id': open_trade.get('tp_order_client_id'),
                'sl_client_order_id': open_trade.get('sl_order_client_id'),
                'exchange_snapshot': {
                    'position': verify_pos_res,
                    'orders': verify_orders_res,
                },
            })
    else:
        entry_position_confirmed = True
        confirmed_trade = _recover_open_trade_from_pending(pending_entry, verify_position)
        confirmed_trade['tp_order_client_id'] = open_trade.get('tp_order_client_id')
        confirmed_trade['tp_order_exchange_id'] = open_trade.get('tp_order_exchange_id')
        confirmed_trade['sl_order_client_id'] = open_trade.get('sl_order_client_id')
        confirmed_trade['sl_order_exchange_id'] = open_trade.get('sl_order_exchange_id')
        confirmed_trade['time_stop_client_order_id'] = open_trade.get('time_stop_client_order_id')
        confirmed_trade['time_stop_exchange_order_id'] = open_trade.get('time_stop_exchange_order_id')
        open_trade = confirmed_trade
        set_open_trade(account, symbol, open_trade)
        set_pending_entry_order(account, symbol, None)

        open_trade = _ensure_exit_orders(
            account,
            symbol,
            open_trade,
            verify_position,
            verify_orders,
            live_cfg,
            current_time_ms,
            current_time_bj,
            source='entry_immediate_confirmed_repair',
        )
        set_open_trade(account, symbol, open_trade)

        verify_res = _verify_open_trade_brackets(
            account,
            symbol,
            open_trade,
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
        )
        if not verify_res.get('ok'):
            verify_reason = (verify_res.get('orders') or {}).get('reason') or (verify_res.get('position') or {}).get('reason')
            mark_error(
                account,
                symbol,
                error_code='entry_immediate_bracket_verify_failed',
                error_message=verify_reason,
                error_bj=current_time_bj,
            )
            if audit_enabled:
                write_event(account, 'entry_immediate_bracket_verify_failed', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'order_root': order_root,
                    'exchange_snapshot': {
                        'position': verify_res.get('position'),
                        'orders': verify_res.get('orders'),
                    },
                })
                write_event(account, 'critical_bracket_gap_after_entry', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'reason': 'entry_immediate_bracket_verify_failed',
                    'order_root': order_root,
                    'tp_client_order_id': open_trade.get('tp_order_client_id'),
                    'sl_client_order_id': open_trade.get('sl_order_client_id'),
                    'exchange_snapshot': {
                        'position': verify_res.get('position'),
                        'orders': verify_res.get('orders'),
                    },
                })
            entry_bracket_gap_critical = True
            if notify_enabled and live_cfg.get('notify_on_order_error', True):
                _notify(True, f'[Snapback-Live] 风险告警 {symbol} | entry后 bracket 验证失败 | {verify_reason or "unknown"}')
        else:
            tp_bound = bool(verify_res.get('tp_bound'))
            sl_bound = bool(verify_res.get('sl_bound'))
            if not (tp_bound and sl_bound):
                mark_error(
                    account,
                    symbol,
                    error_code='entry_immediate_bracket_incomplete',
                    error_message=f'tp_bound={tp_bound}, sl_bound={sl_bound}',
                    error_bj=current_time_bj,
                )
                if audit_enabled:
                    write_event(account, 'entry_immediate_bracket_incomplete', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'order_root': order_root,
                        'tp_bound': tp_bound,
                        'sl_bound': sl_bound,
                        'tp_client_order_id': open_trade.get('tp_order_client_id'),
                        'sl_client_order_id': open_trade.get('sl_order_client_id'),
                        'exchange_snapshot': {
                            'position': verify_res.get('position'),
                            'orders': verify_res.get('orders'),
                        },
                    })
                    write_event(account, 'critical_bracket_gap_after_entry', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'reason': 'entry_immediate_bracket_incomplete',
                        'order_root': order_root,
                        'tp_bound': tp_bound,
                        'sl_bound': sl_bound,
                        'tp_client_order_id': open_trade.get('tp_order_client_id'),
                        'sl_client_order_id': open_trade.get('sl_order_client_id'),
                        'exchange_snapshot': {
                            'position': verify_res.get('position'),
                            'orders': verify_res.get('orders'),
                        },
                    })
                entry_bracket_gap_critical = True
                if notify_enabled and live_cfg.get('notify_on_order_error', True):
                    _notify(True, f'[Snapback-Live] 风险告警 {symbol} | entry后 bracket 仍不完整 | tp_bound={tp_bound} sl_bound={sl_bound}')

    if entry_position_confirmed:
        if not entry_bracket_gap_critical:
            _clear_symbol_error(account, symbol)
        _refresh_entry_cooldown(account, symbol, current_time_ms, int(live_cfg['cooldown_mins']))
        if audit_enabled:
            write_event(account, 'cooldown_set_after_entry', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'order_root': order_root})
            if entry_bracket_gap_critical:
                write_event(account, 'entry_submit_notify_suppressed_bracket_gap', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'order_root': order_root,
                })
    elif audit_enabled:
        write_event(account, 'cooldown_deferred_until_position_confirmed', {
            'symbol': symbol,
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'order_root': order_root,
            'entry_still_pending': entry_still_pending,
            'entry_bracket_gap_critical': entry_bracket_gap_critical,
        })
    mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)

    if entry_position_confirmed and (not entry_bracket_gap_critical) and notify_enabled and live_cfg.get('notify_on_order_submit', True):
        tp_px = float(signal.get('tp_price') or 0.0)
        sl_px = float(signal.get('sl_price') or 0.0)
        _notify(True, f'[Snapback-Live] 开仓 {symbol} | entry≈{current_price:.6f} | TP={tp_px:.6f} | SL={sl_px:.6f}')


def main() -> None:
    parser = argparse.ArgumentParser(description='Snapback live runner')
    parser.add_argument('--config', default='strategies/snapback/config.json')
    parser.add_argument('--live-config', default='strategies/snapback/live_config.json')
    args = parser.parse_args()

    setup_logging()
    strategy_cfg = StrategyConfig.load(args.config)
    live_cfg = _load_live_config(args.live_config)
    if not bool(live_cfg.get('enabled', False)):
        raise SystemExit('live_config enabled=false，拒绝启动实盘 runner')

    account = str(live_cfg['account']).strip()
    if not account:
        raise SystemExit('live_config account 不能为空')

    snapshot_meta = _write_config_snapshot(account, args.config, args.live_config, strategy_cfg, live_cfg)

    mark_loop_heartbeat(account, runner_pid=os.getpid())
    write_runner_started(account, {
        'config_path': args.config,
        'live_config_path': args.live_config,
        'config_snapshot_path': snapshot_meta['snapshot_path'],
        'strategy_config_sha256': snapshot_meta['strategy_config_sha256'],
        'live_config_sha256': snapshot_meta['live_config_sha256'],
        'strategy_config_file_sha256': snapshot_meta['strategy_config_file_sha256'],
        'live_config_file_sha256': snapshot_meta['live_config_file_sha256'],
        'started_bj': _now_bj_str(),
    })
    bootstrap_res = _bootstrap_reconcile(account, strategy_cfg, live_cfg)
    if bootstrap_res.get('blocking'):
        write_event(account, 'startup_reconcile_blocked', {
            'bar_ts': bootstrap_res.get('bar_ts'),
            'bar_bj': bootstrap_res.get('bar_bj'),
            'pending_reconcile_error': bootstrap_res.get('pending_reconcile_error'),
            'open_trade_reconcile_error': bootstrap_res.get('open_trade_reconcile_error'),
            'exchange_activity_snapshot_ok': bootstrap_res.get('exchange_activity_snapshot_ok'),
            'orphan_findings': bootstrap_res.get('orphan_findings'),
            'active_state_errors': bootstrap_res.get('active_state_errors'),
        })
        if bool(live_cfg.get('notify_enabled', False)) and bool(live_cfg.get('notify_on_order_error', True)):
            _notify(True, f'[Snapback-Live] startup blocked | account={account} | reconcile/orphan/state error detected')
        raise SystemExit('startup blocked: reconcile/orphan/state error detected')
    if bool(live_cfg.get('notify_enabled', False)):
        _notify(True, f'[Snapback-Live] runner started | account={account}')

    while True:
        try:
            mark_loop_heartbeat(account, runner_pid=os.getpid())
            write_runner_heartbeat(account, {'heartbeat_bj': _now_bj_str()})
            _run_once(strategy_cfg, live_cfg)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            write_event(account, 'runner_error', {'reason': str(e), 'error_bj': _now_bj_str()})
            if bool(live_cfg.get('notify_enabled', False)) and bool(live_cfg.get('notify_on_order_error', True)):
                _notify(True, f'[Snapback-Live] runner error | {e}')
        _sleep_until_next_closed_bar()


if __name__ == '__main__':
    main()
