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


def _precheck_exchange_blockers(account: str, symbol: str) -> dict[str, Any]:
    pos_res = get_position(account, symbol, FIXED_POSITION_SIDE)
    ord_res = get_open_orders(account, symbol)
    return {'position': pos_res, 'orders': ord_res}


def _has_position_or_orders(snapshot: dict[str, Any]) -> tuple[bool, str]:
    pos_res = snapshot['position']
    ord_res = snapshot['orders']
    if pos_res.get('ok') and pos_res.get('data'):
        return True, 'exchange_has_position'
    if ord_res.get('ok') and ord_res.get('data'):
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

def _refresh_entry_cooldown(account: str, symbol: str, current_time_ms: int, cooldown_mins: int) -> None:
    cooldown_until_ts, cooldown_until_bj = _cooldown_until(current_time_ms, cooldown_mins)
    set_cooldown(account, symbol, cooldown_until_ts=cooldown_until_ts, cooldown_until_bj=cooldown_until_bj)


def _refresh_exit_cooldown(account: str, symbol: str, current_time_ms: int, cooldown_mins: int) -> dict[str, Any]:
    cooldown_until_ts, cooldown_until_bj = _cooldown_until(current_time_ms, cooldown_mins)
    return set_cooldown(account, symbol, cooldown_until_ts=cooldown_until_ts, cooldown_until_bj=cooldown_until_bj)



def _reconcile_pending_entries(account: str, live_cfg: dict[str, Any], current_time_ms: int, current_time_bj: str, *, source: str) -> None:
    state = load_live_state(account)
    symbols = state.get('symbols') or {}
    audit_enabled = bool(live_cfg.get('audit_enabled', True))
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
        pos_res = get_position(account, symbol, FIXED_POSITION_SIDE)
        if pos_res.get('ok') and pos_res.get('data'):
            recovered_trade = _recover_open_trade_from_pending(pending, pos_res['data'])
            set_open_trade(account, symbol, recovered_trade)
            set_pending_entry_order(account, symbol, None)
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
                    'exchange_snapshot': {'entry_order': entry_res, 'position': pos_res},
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
                set_pending_entry_order(account, symbol, None)
                if audit_enabled:
                    write_event(account, 'entry_terminal_detected', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'source': source,
                        'exchange_snapshot': entry_res,
                    })


def _reconcile_open_trades(account: str, live_cfg: dict[str, Any], current_time_ms: int, current_time_bj: str, latest_closes: dict[str, float], max_hold_mins: int, min_profit_pct: float, *, source: str) -> None:
    state = load_live_state(account)
    symbols = state.get('symbols') or {}
    audit_enabled = bool(live_cfg.get('audit_enabled', True))
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
            if audit_enabled:
                write_event(account, 'exit_reconcile_error', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'source': source, 'exchange_snapshot': {'position': pos_res, 'orders': ord_res}})
            continue
        position = pos_res.get('data')
        open_orders = ord_res.get('data') or []
        if not position:
            exit_reason, order_checks = _infer_exit_reason(account, symbol, open_trade, retry_max=retry_max, retry_delay_secs=retry_delay_secs)
            tp_cancel = _cancel_order_if_present(account, symbol, exchange_order_id=open_trade.get('tp_order_exchange_id'), client_order_id=open_trade.get('tp_order_client_id'), retry_max=retry_max, retry_delay_secs=retry_delay_secs)
            sl_cancel = _cancel_order_if_present(account, symbol, exchange_order_id=open_trade.get('sl_order_exchange_id'), client_order_id=open_trade.get('sl_order_client_id'), retry_max=retry_max, retry_delay_secs=retry_delay_secs)
            set_open_trade(account, symbol, None)
            set_pending_entry_order(account, symbol, None)
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
                    'exchange_snapshot': {'position': pos_res, 'orders': ord_res, 'order_checks': order_checks, 'tp_cancel': tp_cancel, 'sl_cancel': sl_cancel},
                })
                event_map = {'TAKE_PROFIT': 'tp_filled', 'STOP_LOSS': 'sl_filled', 'TIME_STOP': 'time_stop_filled', 'UNKNOWN_EXIT': 'unknown_exit'}
                write_event(account, event_map.get(exit_reason, 'unknown_exit'), {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'source': source, 'order_root': open_trade.get('order_root')})
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

        if open_trade.get('exit_submit_inflight'):
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

        ts_client_order_id = build_client_order_id(broker_id=BROKER_ID, strat=STRAT_CODE, leg=LEG_TIME_STOP, root=open_trade.get('order_root') or make_order_root())
        qty = float(position.get('qty') or open_trade.get('entry_qty') or 0.0)
        ts_res = place_time_stop_order(account, symbol, FIXED_POSITION_SIDE, qty, retry_max=retry_max, retry_delay_secs=retry_delay_secs, client_order_id=ts_client_order_id)
        if not ts_res.get('ok'):
            mark_error(account, symbol, error_code='time_stop_submit_failed', error_message=ts_res.get('reason'), error_bj=current_time_bj)
            if audit_enabled:
                write_event(account, 'time_stop_submit_failed', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'source': source, 'exchange_snapshot': ts_res})
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

def _bootstrap_reconcile(account: str, strategy_cfg: dict[str, Any], live_cfg: dict[str, Any]) -> None:
    current_time_ms = _now_utc_ms()
    current_time_bj = _fmt_bj_from_ms(current_time_ms) or _now_bj_str()
    _reconcile_pending_entries(account, live_cfg, current_time_ms, current_time_bj, source='startup')
    max_hold_mins, min_profit_pct = _extract_time_stop_config(strategy_cfg)
    _reconcile_open_trades(account, live_cfg, current_time_ms, current_time_bj, {}, max_hold_mins, min_profit_pct, source='startup')


def _run_once(strategy_cfg: dict[str, Any], live_cfg: dict[str, Any]) -> None:
    account = str(live_cfg['account']).strip()
    notify_enabled = bool(live_cfg.get('notify_enabled', False))
    audit_enabled = bool(live_cfg.get('audit_enabled', True))
    lookback_bars = int(live_cfg['lookback_bars'])
    symbols = list_candidate_symbols(account, exclude_symbols=live_cfg.get('exclude_symbols') or [])
    md_res = build_live_inputs(account, symbols, lookback_bars, strategy_cfg)
    if not md_res['ok']:
        if audit_enabled:
            write_event(account, 'data_error', {'reason': md_res['reason'], 'errors': md_res.get('errors')})
        return

    payload = md_res['data']
    current_time_ms = int(payload['latest_closed_bar_ts'])
    current_time_bj = payload['latest_closed_bar_bj']
    cross_section = payload['cross_section']
    full_df = payload['full_df']
    latest_closes = {str(symbol).upper().strip(): float(df.loc[current_time_ms, 'close']) for symbol, df in full_df.items() if current_time_ms in df.index}
    max_hold_mins, min_profit_pct = _extract_time_stop_config(strategy_cfg)

    _reconcile_pending_entries(account, live_cfg, current_time_ms, current_time_bj, source='loop')
    _reconcile_open_trades(account, live_cfg, current_time_ms, current_time_bj, latest_closes, max_hold_mins, min_profit_pct, source='loop')

    strategy = WashoutSnapbackStrategy(strategy_cfg)
    active_symbols = _active_symbols_from_state(account)
    signal = strategy.on_kline_close(current_time_ms, cross_section, active_symbols, full_df)

    if not signal:
        if audit_enabled:
            write_event(account, 'signal_none', {
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'freshest_bar_ts': payload.get('freshest_bar_ts'),
                'freshest_bar_bj': payload.get('freshest_bar_bj'),
                'stale_cutoff_bj': payload.get('stale_cutoff_bj'),
                'symbol_count': payload['symbol_count'],
                'stale_symbol_count': payload.get('stale_symbol_count', 0),
            })
        for symbol in full_df.keys():
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

    exch = _precheck_exchange_blockers(account, symbol)
    mark_position_reconcile(account, symbol, reconcile_bj=_now_bj_str())
    mark_order_reconcile(account, symbol, reconcile_bj=_now_bj_str())
    blocked, block_reason = _has_position_or_orders(exch)
    if blocked:
        if audit_enabled:
            write_event(account, 'precheck_skip', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'reason': block_reason, 'exchange_snapshot': exch})
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return

    entry_notional_usdt = float(live_cfg['entry_notional_usdt'])
    current_price = float(signal.get('current_price') or 0.0)
    if current_price <= 0:
        if audit_enabled:
            write_event(account, 'precheck_error', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'reason': 'invalid_current_price'})
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
    set_open_trade(account, symbol, open_trade)
    set_pending_entry_order(account, symbol, None)
    _refresh_entry_cooldown(account, symbol, current_time_ms, int(live_cfg['cooldown_mins']))
    if audit_enabled:
        write_event(account, 'cooldown_set_after_entry', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'order_root': order_root})
    mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)

    if notify_enabled and live_cfg.get('notify_on_order_submit', True):
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
    _bootstrap_reconcile(account, strategy_cfg, live_cfg)
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
