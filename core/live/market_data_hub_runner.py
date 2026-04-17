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

from core.live.audit_log import write_event
from core.live.market_data import list_candidate_symbols
from core.live.market_data_hub_store import write_current_snapshot
from core.live.market_data_hub import (
    build_live_inputs_via_hub,
    build_market_snapshot_via_hub,
    finalize_candidate_payload_via_hub,
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


def _load_hub_config(path: str) -> dict[str, Any]:
    data = _load_json(path)
    if 'account' not in data:
        raise KeyError(f'hub_config 缺少必要字段: account | {path}')
    if 'history_window_mins' not in data:
        raise KeyError(f'hub_config 缺少必要字段: history_window_mins | {path}')
    data.setdefault('audit_enabled', True)
    data.setdefault('enabled', True)
    data.setdefault('publish_config_snapshot', True)
    data.setdefault('min_24h_quote_volume', 30000000)
    return data


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')


def _next_signal_check_epoch(now_epoch: float | None = None) -> float:
    if now_epoch is None:
        now_epoch = time.time()
    now = datetime.fromtimestamp(now_epoch, tz=timezone.utc)
    current_minute_second_second = now.replace(second=1, microsecond=0)
    if now < current_minute_second_second:
        return current_minute_second_second.timestamp()
    next_minute_second_second = now.replace(second=0, microsecond=0) + timedelta(minutes=1, seconds=1)
    return next_minute_second_second.timestamp()


def _sleep_until_next_signal_check(target_epoch: float | None) -> float:
    if target_epoch is None:
        target_epoch = _next_signal_check_epoch()
    now_epoch = time.time()
    while target_epoch <= now_epoch:
        target_epoch += 60.0
    while True:
        remaining = target_epoch - time.time()
        if remaining <= 0:
            break
        if remaining > 1.0:
            time.sleep(min(remaining - 0.2, 10.0))
        elif remaining > 0.2:
            time.sleep(max(0.05, remaining - 0.05))
        else:
            time.sleep(min(remaining, 0.02))
    return target_epoch


def _run_account_once(hub_cfg: dict[str, Any]) -> None:
    account = str(hub_cfg['account']).strip()
    audit_enabled = bool(hub_cfg.get('audit_enabled', True))
    history_window_mins = int(hub_cfg['history_window_mins'])
    min_24h_quote_volume = float(hub_cfg.get('min_24h_quote_volume', 30000000) or 0.0)

    market_snapshot = build_market_snapshot_via_hub(account, audit_enabled=audit_enabled)
    latest_closed_bar_ts = int(market_snapshot['latest_closed_bar_ts'])
    signal_time_ts = int(market_snapshot['signal_time_ts'])
    signal_time_bj = str(market_snapshot['signal_time_bj'])
    candidate_symbols = list_candidate_symbols(account)
    symbol_24h_quote_volume_1m = dict(market_snapshot.get('symbol_24h_quote_volume_1m') or {})
    finalize_symbols = [
        symbol for symbol in candidate_symbols
        if float(symbol_24h_quote_volume_1m.get(str(symbol).upper().strip()) or 0.0) >= min_24h_quote_volume
    ]
    if not finalize_symbols:
        write_event(account, 'hub_candidate_prefilter_empty', {
            'bar_ts': signal_time_ts,
            'bar_bj': signal_time_bj,
            'latest_closed_bar_ts': latest_closed_bar_ts,
            'latest_closed_bar_bj': market_snapshot.get('latest_closed_bar_bj'),
            'min_24h_quote_volume': min_24h_quote_volume,
            'market_total_24h_vol_1m_rollsum': market_snapshot.get('market_total_24h_vol_1m_rollsum'),
            'market_total_24h_symbol_count_1m_rollsum': market_snapshot.get('market_total_24h_symbol_count_1m_rollsum'),
        })
        return

    candidate_res = build_live_inputs_via_hub(
        account,
        finalize_symbols,
        history_window_mins,
        None,
        audit_label='candidate',
        latest_closed_bar_ts=latest_closed_bar_ts,
        ticker_map=dict(market_snapshot['ticker_map']),
        audit_enabled=audit_enabled,
        use_full_market_inputs=True,
    )
    candidate_payload = candidate_res.get('data') if candidate_res.get('ok') else None
    if not candidate_payload:
        write_event(account, 'hub_candidate_inputs_failed', {
            'bar_ts': signal_time_ts,
            'bar_bj': signal_time_bj,
            'reason': candidate_res.get('reason'),
            'errors': candidate_res.get('errors'),
        })
        return

    finalize_candidate_payload_via_hub(
        account,
        candidate_payload,
        history_window_mins=history_window_mins,
        c_bar_ts=int(candidate_payload['latest_closed_bar_ts']),
        c_bar_bj=str(candidate_payload['latest_closed_bar_bj']),
        current_time_ms=int(candidate_payload.get('signal_time_ts') or signal_time_ts),
        current_time_bj=str(candidate_payload.get('signal_time_bj') or signal_time_bj),
        candidate_md_finished_utc_ms=int(time.time() * 1000),
        audit_enabled=audit_enabled,
        latest_closed_bar_ts=latest_closed_bar_ts,
        ticker_map=dict(market_snapshot['ticker_map']),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description='Market data hub runner')
    parser.add_argument('--hub-config', action='append', required=True, dest='hub_configs')
    args = parser.parse_args()

    setup_logging()
    hub_cfgs = [_load_hub_config(path) for path in args.hub_configs]
    for hub_cfg in hub_cfgs:
        if bool(hub_cfg.get('publish_config_snapshot', True)):
            account = str(hub_cfg['account']).strip()
            write_current_snapshot(account, 'hub_config', {
                'schema_version': 1,
                'account': account,
                'snapshot_name': 'hub_config',
                'published_utc_ms': int(time.time() * 1000),
                'published_bj': _fmt_bj_from_ms(int(time.time() * 1000)),
                'hub_config': dict(hub_cfg),
            })

    next_signal_check_epoch: float | None = None
    while True:
        next_signal_check_epoch = _sleep_until_next_signal_check(next_signal_check_epoch)
        for hub_cfg in hub_cfgs:
            account = str(hub_cfg['account']).strip()
            if not bool(hub_cfg.get('enabled', True)):
                continue
            try:
                _run_account_once(hub_cfg)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logging.exception('hub runner error | account=%s', account)
                write_event(account, 'hub_runner_error', {
                    'reason': str(e),
                    'error_bj': _fmt_bj_from_ms(int(time.time() * 1000)),
                })
        if next_signal_check_epoch is not None:
            next_signal_check_epoch += 60.0


if __name__ == '__main__':
    main()
