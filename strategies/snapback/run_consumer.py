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
from core.live.live_state import mark_loop_heartbeat
from core.live.market_data import build_live_inputs
from strategies.snapback.trade_consumer import (
    bootstrap_consumer,
    collect_consumer_exchange_activity_snapshot,
    collect_consumer_local_activity_symbols,
    consume_signal,
    maintain_consumer_once,
)

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
    required = ['enabled', 'account', 'exclude_symbols', 'entry_notional_usdt', 'leverage', 'cooldown_mins', 'order_retry_max', 'api_retry_delay_secs', 'pre_entry_min_sl_distance_pct', 'precheck_scope', 'strategy_concurrency_scope', 'audit_enabled', 'notify_enabled']
    for key in required:
        if key not in data:
            raise KeyError(f'live_config 缺少必要字段: {key}')
    if not isinstance(data.get('exclude_symbols'), list):
        raise TypeError('exclude_symbols 必须是 list')
    if str(data.get('precheck_scope') or '').strip() not in {'symbol', 'account_flat'}:
        raise ValueError('live_config.precheck_scope must be symbol or account_flat')
    if str(data.get('strategy_concurrency_scope') or '').strip() not in {'symbol', 'account'}:
        raise ValueError('live_config.strategy_concurrency_scope must be symbol or account')
    return data


def _now_utc_ms() -> int:
    return int(time.time() * 1000)


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')


def _require_dict(data: Any, *, label: str) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise TypeError(f'{label} 必须是 dict')
    return data


def _require_keys(data: dict[str, Any], keys: list[str], *, label: str) -> None:
    for key in keys:
        if key not in data:
            raise KeyError(f'{label} 缺少必要字段: {key}')


def _require_nonempty_str(value: Any, *, label: str) -> str:
    out = str(value or '').strip()
    if not out:
        raise ValueError(f'{label} 不能为空')
    return out


def _require_int_like(value: Any, *, label: str, allow_none: bool = False) -> int | None:
    if value is None:
        if allow_none:
            return None
        raise ValueError(f'{label} 不能为空')
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f'{label} 必须是 int') from exc


def _require_positive_float(value: Any, *, label: str) -> float:
    try:
        out = float(value)
    except (TypeError, ValueError) as exc:
        raise TypeError(f'{label} 必须是 float') from exc
    if out <= 0:
        raise ValueError(f'{label} 必须 > 0')
    return out


def _normalize_signal_envelope(path: str) -> tuple[dict[str, Any], dict[str, Any]]:
    data = _require_dict(_load_json(path), label='signal 文件顶层')
    _require_keys(data, ['signal', 'context'], label='signal 文件顶层')
    signal = _require_dict(data.get('signal'), label='signal')
    context = _require_dict(data.get('context'), label='context')

    _require_keys(signal, ['symbol', 'action', 'signal_time', 'signal_time_bj', 'current_price', 'tp_price', 'sl_price'], label='signal')
    _require_keys(context, ['current_time_ms', 'current_time_bj', 'c_bar_ts', 'c_bar_bj', 'source'], label='context')

    normalized_signal = dict(signal)
    normalized_signal['symbol'] = _require_nonempty_str(signal.get('symbol'), label='signal.symbol').upper()
    normalized_signal['action'] = _require_nonempty_str(signal.get('action'), label='signal.action')
    normalized_signal['signal_time'] = _require_int_like(signal.get('signal_time'), label='signal.signal_time')
    normalized_signal['signal_time_bj'] = _require_nonempty_str(signal.get('signal_time_bj'), label='signal.signal_time_bj')
    normalized_signal['current_price'] = _require_positive_float(signal.get('current_price'), label='signal.current_price')
    normalized_signal['tp_price'] = _require_positive_float(signal.get('tp_price'), label='signal.tp_price')
    normalized_signal['sl_price'] = _require_positive_float(signal.get('sl_price'), label='signal.sl_price')

    normalized_context = dict(context)
    normalized_context['current_time_ms'] = _require_int_like(context.get('current_time_ms'), label='context.current_time_ms')
    normalized_context['current_time_bj'] = _require_nonempty_str(context.get('current_time_bj'), label='context.current_time_bj')
    normalized_context['c_bar_ts'] = _require_int_like(context.get('c_bar_ts'), label='context.c_bar_ts', allow_none=True)
    normalized_context['c_bar_bj'] = None if context.get('c_bar_bj') is None else _require_nonempty_str(context.get('c_bar_bj'), label='context.c_bar_bj')
    normalized_context['source'] = _require_nonempty_str(context.get('source'), label='context.source')
    return normalized_signal, normalized_context


def _summarize_result(result: dict[str, Any]) -> dict[str, Any]:
    return {
        'ok': bool(result.get('ok')),
        'blocking': bool(result.get('blocking', False)),
        'symbol': result.get('symbol'),
        'outcome': result.get('outcome'),
        'reason': result.get('reason'),
        'order_root': result.get('order_root'),
        'entry_position_confirmed': result.get('entry_position_confirmed'),
        'entry_still_pending': result.get('entry_still_pending'),
        'entry_bracket_gap_critical': result.get('entry_bracket_gap_critical'),
        'pending_reconcile_error': result.get('pending_reconcile_error'),
        'open_trade_reconcile_error': result.get('open_trade_reconcile_error'),
        'pending_symbols': result.get('pending_symbols'),
        'open_symbols': result.get('open_symbols'),
        'active_state_errors': result.get('active_state_errors'),
        'exchange_activity_snapshot_ok': result.get('exchange_activity_snapshot_ok'),
        'local_active_symbols': result.get('local_active_symbols'),
    }


def _print_result(tag: str, result: dict[str, Any]) -> None:
    print(f'===== {tag} summary =====')
    print(json.dumps(_summarize_result(result), ensure_ascii=False, indent=2, default=str))
    print(f'===== {tag} full =====')
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))


def _build_consumer_latest_closes(account: str, strategy_cfg: dict[str, Any], live_cfg: dict[str, Any]) -> tuple[dict[str, float], int, str, dict[str, Any] | None, dict[str, Any]]:
    runtime_cfg = (strategy_cfg or {}).get('runtime') or {}
    if 'max_history_window_mins' not in runtime_cfg:
        raise KeyError('strategy_cfg.runtime.max_history_window_mins missing')
    history_window_mins = int(runtime_cfg['max_history_window_mins'])
    if history_window_mins <= 0:
        raise ValueError('strategy_cfg.runtime.max_history_window_mins must be > 0')

    exchange_snapshot = collect_consumer_exchange_activity_snapshot(account)
    symbols = sorted(collect_consumer_local_activity_symbols(account) | set(exchange_snapshot.get('symbols') or set()))
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
    parser.add_argument('--mode', choices=['feed', 'feed_and_maintain', 'maintain_only'], default='feed', help='feed=只投喂一次; feed_and_maintain=投喂后继续维护; maintain_only=不投喂，只跑维护')
    parser.add_argument('--signal-file')
    parser.add_argument('--maintain-interval-secs', type=int, default=60)
    parser.add_argument('--maintain-iterations', type=int, default=0, help='0 means infinite when mode requires maintain loop')
    args = parser.parse_args()

    setup_logging()
    strategy_cfg = StrategyConfig.load(args.config)
    live_cfg = _load_live_config(args.live_config)
    if not bool(live_cfg.get('enabled', False)):
        raise SystemExit('live_config enabled=false，拒绝启动 consumer runner')

    account = str(live_cfg['account']).strip()
    if not account:
        raise SystemExit('live_config account 不能为空')

    requires_signal = args.mode in {'feed', 'feed_and_maintain'}
    if requires_signal and not args.signal_file:
        raise SystemExit('当前 mode 需要 --signal-file')
    if (not requires_signal) and args.signal_file:
        raise SystemExit('maintain_only 模式禁止传入 --signal-file')

    mark_loop_heartbeat(account, runner_pid=os.getpid())

    bootstrap_res = bootstrap_consumer(
        account,
        strategy_cfg,
        live_cfg,
        source='manual_startup',
    )
    _print_result('bootstrap', bootstrap_res)
    if bootstrap_res.get('blocking'):
        raise SystemExit('bootstrap blocked: reconcile/state/exchange snapshot error detected')

    if requires_signal:
        signal, context = _normalize_signal_envelope(args.signal_file)
        exchange_snapshot = dict(bootstrap_res.get('exchange_snapshot') or {})
        consume_res = consume_signal(
            account,
            strategy_cfg,
            live_cfg,
            signal=signal,
            current_time_ms=int(context['current_time_ms']),
            current_time_bj=str(context['current_time_bj']),
            c_bar_ts=context.get('c_bar_ts'),
            c_bar_bj=context.get('c_bar_bj'),
            source=str(context['source']),
            exchange_snapshot=exchange_snapshot,
        )
        _print_result('consume', consume_res)
        if args.mode == 'feed':
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
        _print_result('maintain', maintain_res)
        iterations += 1
        time.sleep(max(1, int(args.maintain_interval_secs)))


if __name__ == '__main__':
    main()
