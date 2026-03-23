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
    get_open_orders,
    get_position,
    place_entry_order,
    place_sl_order,
    place_tp_order,
)
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


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _now_bj_str() -> str:
    return datetime.now(timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')


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
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')

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


def _build_open_trade(entry_res: dict[str, Any], signal: dict[str, Any], tp_res: dict[str, Any], sl_res: dict[str, Any], entry_notional_usdt: float) -> dict[str, Any]:
    entry = entry_res['data']
    tp = tp_res['data'] if tp_res.get('ok') else {}
    sl = sl_res['data'] if sl_res.get('ok') else {}
    return {
        'symbol': signal['symbol'],
        'side': FIXED_POSITION_SIDE,
        'entry_client_order_id': entry.get('client_order_id'),
        'entry_exchange_order_id': entry.get('exchange_order_id'),
        'entry_ts': int(signal['signal_time']),
        'entry_bj': signal['signal_time_bj'],
        'entry_price': float(entry.get('avg_price') or signal.get('current_price') or 0.0),
        'entry_qty': float(entry.get('executed_qty') or entry.get('qty') or 0.0),
        'entry_notional_usdt': float(entry_notional_usdt),
        'signal_digest': _signal_digest(signal),
        'signal_snapshot': signal,
        'tp_order_client_id': tp.get('client_order_id'),
        'tp_order_exchange_id': tp.get('exchange_order_id'),
        'sl_order_client_id': sl.get('client_order_id'),
        'sl_order_exchange_id': sl.get('exchange_order_id'),
        'tp_price': float(signal.get('tp_price') or 0.0),
        'sl_trigger_price': float(signal.get('sl_price') or 0.0),
        'status': 'OPEN',
        'last_status_bj': _now_bj_str(),
    }


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

    if audit_enabled:
        write_event(account, 'signal_detected', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'signal_snapshot': signal})

    entry_res = place_entry_order(account, symbol, FIXED_POSITION_SIDE, quantity, retry_max=retry_max, retry_delay_secs=retry_delay_secs)
    if not entry_res['ok']:
        mark_error(account, symbol, error_code='entry_submit_failed', error_message=entry_res['reason'], error_bj=_now_bj_str())
        if audit_enabled:
            write_event(account, 'entry_submit_failed', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'reason': entry_res['reason'], 'exchange_snapshot': entry_res})
        if notify_enabled and live_cfg.get('notify_on_order_error', True):
            _notify(True, f'[Snapback-Live] 入场失败 {symbol} | {entry_res["reason"]}')
        mark_last_processed_bar(account, symbol, bar_ts=current_time_ms, bar_bj=current_time_bj)
        return

    set_pending_entry_order(account, symbol, entry_res['data'])
    entry_data = entry_res['data']
    qty_for_exit = float(entry_data.get('executed_qty') or entry_data.get('qty') or 0.0)
    if qty_for_exit <= 0:
        qty_for_exit = quantity

    tp_res = place_tp_order(account, symbol, FIXED_POSITION_SIDE, qty_for_exit, float(signal['tp_price']), retry_max=retry_max, retry_delay_secs=retry_delay_secs)
    sl_res = place_sl_order(account, symbol, FIXED_POSITION_SIDE, float(signal['sl_price']), retry_max=retry_max, retry_delay_secs=retry_delay_secs)

    if audit_enabled:
        write_event(account, 'entry_submitted', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'exchange_snapshot': entry_res, 'signal_snapshot': signal})
        write_event(account, 'tp_submitted' if tp_res.get('ok') else 'tp_submit_failed', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'exchange_snapshot': tp_res})
        write_event(account, 'sl_submitted' if sl_res.get('ok') else 'sl_submit_failed', {'symbol': symbol, 'bar_ts': current_time_ms, 'bar_bj': current_time_bj, 'exchange_snapshot': sl_res})

    open_trade = _build_open_trade(entry_res, signal, tp_res, sl_res, entry_notional_usdt)
    set_open_trade(account, symbol, open_trade)
    set_pending_entry_order(account, symbol, None)
    cooldown_mins = int(live_cfg['cooldown_mins'])
    cooldown_until_ts = current_time_ms + cooldown_mins * 60 * 1000
    cooldown_until_bj = datetime.fromtimestamp(cooldown_until_ts / 1000.0, tz=timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')
    set_cooldown(account, symbol, cooldown_until_ts=cooldown_until_ts, cooldown_until_bj=cooldown_until_bj)
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
