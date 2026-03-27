from __future__ import annotations

import argparse
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
from core.live.binance_exec import get_open_orders, get_positions
from core.live.live_state import load_live_state, mark_loop_heartbeat
from core.live.market_data import build_live_inputs
from strategies.snapback.trade_consumer import consume_signal, maintain_consumer_once

BJ = timezone(timedelta(hours=8))


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def _load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f'配置文件缺失: {path}')
    with p.open('r', encoding='utf-8') as f:
        return json.load(f)


def _load_live_config(path: str) -> dict[str, Any]:
    data = _load_json(path)
    required = ['enabled', 'account', 'exclude_symbols', 'entry_notional_usdt', 'leverage', 'cooldown_mins', 'order_retry_max', 'api_retry_delay_secs', 'audit_enabled', 'notify_enabled']
    for key in required:
        if key not in data:
            raise KeyError(f'live_config 缺少必要字段: {key}')
    if not isinstance(data.get('exclude_symbols'), list):
        raise TypeError('exclude_symbols 必须是 list')
    return data


def _now_utc_ms() -> int:
    return int(time.time() * 1000)


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')


def _load_signal_envelope(path: str) -> tuple[dict[str, Any], dict[str, Any]]:
    data = _load_json(path)
    signal = data.get('signal') if isinstance(data.get('signal'), dict) else data
    context = data.get('context') if isinstance(data.get('context'), dict) else {}
    if not isinstance(signal, dict):
        raise TypeError('signal 文件格式错误: signal 必须是 dict')
    required = ['symbol', 'signal_time', 'signal_time_bj', 'current_price', 'tp_price', 'sl_price']
    for key in required:
        if key not in signal:
            raise KeyError(f'signal 缺少必要字段: {key}')
    return signal, context


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


def _active_symbols_from_state(account: str) -> set[str]:
    state = load_live_state(account)
    out: set[str] = set()
    for symbol, payload in (state.get('symbols') or {}).items():
        if not isinstance(payload, dict):
            continue
        if payload.get('pending_entry_order') or payload.get('open_trade'):
            out.add(str(symbol).upper().strip())
    return out


def _build_consumer_latest_closes(account: str, strategy_cfg: dict[str, Any], live_cfg: dict[str, Any]) -> tuple[dict[str, float], int, str, dict[str, Any] | None, dict[str, Any]]:
    runtime_cfg = (strategy_cfg or {}).get('runtime') or {}
    if 'max_history_window_mins' not in runtime_cfg:
        raise KeyError('strategy_cfg.runtime.max_history_window_mins missing')
    history_window_mins = int(runtime_cfg['max_history_window_mins'])
    if history_window_mins <= 0:
        raise ValueError('strategy_cfg.runtime.max_history_window_mins must be > 0')

    exchange_snapshot = _collect_exchange_activity_snapshot(account)
    symbols = sorted(_active_symbols_from_state(account) | set(exchange_snapshot.get('symbols') or set()))
    if not symbols:
        now_ms = _now_utc_ms()
        return {}, now_ms, _fmt_bj_from_ms(now_ms) or '', None, exchange_snapshot

    md_res = build_live_inputs(account, symbols, history_window_mins, strategy_cfg, audit_label='consumer')
    if not md_res.get('ok'):
        raise RuntimeError(md_res.get('reason') or 'consumer build_live_inputs failed')
    payload = md_res.get('data') or {}
    c_bar_ts = int(payload['latest_closed_bar_ts'])
    current_time_ms = int(payload.get('signal_time_ts') or (c_bar_ts + 60000))
    current_time_bj = str(payload.get('signal_time_bj') or _fmt_bj_from_ms(current_time_ms) or '')
    full_df = dict(payload.get('full_df') or {})
    latest_closes = {
        str(symbol).upper().strip(): float(df.loc[c_bar_ts, 'close'])
        for symbol, df in full_df.items()
        if c_bar_ts in df.index
    }
    return latest_closes, current_time_ms, current_time_bj, payload, exchange_snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description='Snapback consumer runner')
    parser.add_argument('--config', default='strategies/snapback/config.json')
    parser.add_argument('--live-config', default='strategies/snapback/live_config.json')
    parser.add_argument('--signal-file', required=True)
    parser.add_argument('--maintain-loop', action='store_true')
    parser.add_argument('--maintain-interval-secs', type=int, default=60)
    parser.add_argument('--maintain-iterations', type=int, default=0, help='0 means infinite when --maintain-loop is set')
    args = parser.parse_args()

    setup_logging()
    strategy_cfg = StrategyConfig.load(args.config)
    live_cfg = _load_live_config(args.live_config)
    if not bool(live_cfg.get('enabled', False)):
        raise SystemExit('live_config enabled=false，拒绝启动 consumer runner')

    account = str(live_cfg['account']).strip()
    if not account:
        raise SystemExit('live_config account 不能为空')

    signal, context = _load_signal_envelope(args.signal_file)
    current_time_ms = int(context.get('current_time_ms') or signal.get('signal_time') or _now_utc_ms())
    current_time_bj = str(context.get('current_time_bj') or _fmt_bj_from_ms(current_time_ms) or '')
    c_bar_ts = context.get('c_bar_ts')
    c_bar_bj = context.get('c_bar_bj')

    exchange_snapshot = _collect_exchange_activity_snapshot(account)
    mark_loop_heartbeat(account, runner_pid=os.getpid())
    result = consume_signal(
        account,
        strategy_cfg,
        live_cfg,
        signal=signal,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        c_bar_ts=c_bar_ts,
        c_bar_bj=c_bar_bj,
        source='manual_feed',
        exchange_snapshot=exchange_snapshot,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))

    if not args.maintain_loop:
        return

    iterations = 0
    while True:
        if args.maintain_iterations > 0 and iterations >= args.maintain_iterations:
            return
        latest_closes, maintain_time_ms, maintain_time_bj, _payload, exchange_snapshot = _build_consumer_latest_closes(account, strategy_cfg, live_cfg)
        maintain_res = maintain_consumer_once(
            account,
            strategy_cfg,
            live_cfg,
            current_time_ms=maintain_time_ms,
            current_time_bj=maintain_time_bj,
            latest_closes=latest_closes,
            source='manual_maintain',
            exchange_snapshot=exchange_snapshot,
        )
        print(json.dumps(maintain_res, ensure_ascii=False, indent=2, default=str))
        iterations += 1
        time.sleep(max(1, int(args.maintain_interval_secs)))


if __name__ == '__main__':
    main()
