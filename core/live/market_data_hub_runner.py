from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core.live.audit_log import write_event
from core.live.market_data import (
    list_candidate_symbols,
    read_hub_owned_1m_rollsum_market_view,
    refresh_hub_owned_1m_rollsum_for_symbols,
)
from core.live.market_data_hub_store import read_current_snapshot, write_current_pickle, write_current_snapshot
from core.live.market_data_hub import (
    build_live_inputs_via_hub,
    build_market_snapshot_via_hub,
    finalize_candidate_payload_via_hub,
)

BJ = timezone(timedelta(hours=8))
_HUB_OWNED_1M_ROLLSUM_REFRESH_PROGRESS_SNAPSHOT = 'rollsum_refresh_progress'
_BINANCE_BAN_UNTIL_RE = re.compile(r'banned until (\d{10,})', re.IGNORECASE)


def _load_rollsum_refresh_start_idx(account: str) -> int:
    snapshot = read_current_snapshot(account, _HUB_OWNED_1M_ROLLSUM_REFRESH_PROGRESS_SNAPSHOT)
    if not isinstance(snapshot, dict):
        return 0
    try:
        return max(0, int(snapshot.get('next_start_idx') or 0))
    except Exception:
        return 0


def _pick_round_robin_refresh_batch(symbols: list[str], start_idx: int, batch_size: int) -> tuple[list[str], int]:
    ordered = [str(symbol).upper().strip() for symbol in symbols if str(symbol).strip()]
    if not ordered:
        return [], 0
    size = max(1, min(int(batch_size), len(ordered)))
    start = int(start_idx) % len(ordered)
    batch = [ordered[(start + i) % len(ordered)] for i in range(size)]
    next_start_idx = (start + size) % len(ordered)
    return batch, next_start_idx


def _write_rollsum_refresh_progress_snapshot(
    account: str,
    *,
    latest_closed_bar_ts: int,
    signal_time_ts: int,
    batch_symbols: list[str],
    batch_size: int,
    total_symbol_count: int,
    next_start_idx: int,
    refresh_started_utc_ms: int,
    refresh_finished_utc_ms: int,
) -> None:
    payload = {
        'schema_version': 1,
        'account': str(account).strip(),
        'snapshot_name': _HUB_OWNED_1M_ROLLSUM_REFRESH_PROGRESS_SNAPSHOT,
        'published_utc_ms': int(refresh_finished_utc_ms),
        'published_bj': _fmt_bj_from_ms(int(refresh_finished_utc_ms)),
        'latest_closed_bar_ts': int(latest_closed_bar_ts),
        'latest_closed_bar_bj': _fmt_bj_from_ms(int(latest_closed_bar_ts)),
        'signal_time_ts': int(signal_time_ts),
        'signal_time_bj': _fmt_bj_from_ms(int(signal_time_ts)),
        'total_symbol_count': int(total_symbol_count),
        'batch_size': int(batch_size),
        'batch_symbol_count': int(len(batch_symbols)),
        'batch_symbols': list(batch_symbols),
        'next_start_idx': int(next_start_idx),
        'refresh_started_utc_ms': int(refresh_started_utc_ms),
        'refresh_started_bj': _fmt_bj_from_ms(int(refresh_started_utc_ms)),
        'refresh_finished_utc_ms': int(refresh_finished_utc_ms),
        'refresh_finished_bj': _fmt_bj_from_ms(int(refresh_finished_utc_ms)),
        'refresh_elapsed_ms': int(max(0, int(refresh_finished_utc_ms) - int(refresh_started_utc_ms))),
    }
    write_current_snapshot(account, _HUB_OWNED_1M_ROLLSUM_REFRESH_PROGRESS_SNAPSHOT, payload)
    write_current_pickle(account, _HUB_OWNED_1M_ROLLSUM_REFRESH_PROGRESS_SNAPSHOT, dict(payload))


def _refresh_rollsum_batch(
    account: str,
    *,
    candidate_symbols: list[str],
    latest_closed_bar_ts: int,
    signal_time_ts: int,
    hub_cfg: dict[str, Any],
) -> dict[str, Any]:
    refresh_batch_size = int(hub_cfg.get('rollsum_refresh_batch_size', 80) or 80)
    refresh_start_idx = _load_rollsum_refresh_start_idx(account)
    refresh_batch_symbols, next_refresh_start_idx = _pick_round_robin_refresh_batch(
        candidate_symbols,
        refresh_start_idx,
        refresh_batch_size,
    )
    refresh_started_utc_ms = int(time.time() * 1000)
    refresh_hub_owned_1m_rollsum_for_symbols(
        account,
        refresh_batch_symbols,
        latest_closed_bar_ts=latest_closed_bar_ts,
    )
    refresh_finished_utc_ms = int(time.time() * 1000)
    _write_rollsum_refresh_progress_snapshot(
        account,
        latest_closed_bar_ts=latest_closed_bar_ts,
        signal_time_ts=signal_time_ts,
        batch_symbols=refresh_batch_symbols,
        batch_size=refresh_batch_size,
        total_symbol_count=len(candidate_symbols),
        next_start_idx=next_refresh_start_idx,
        refresh_started_utc_ms=refresh_started_utc_ms,
        refresh_finished_utc_ms=refresh_finished_utc_ms,
    )
    return {
        'batch_symbols': list(refresh_batch_symbols),
        'batch_size': int(refresh_batch_size),
        'refresh_started_utc_ms': int(refresh_started_utc_ms),
        'refresh_finished_utc_ms': int(refresh_finished_utc_ms),
        'refresh_elapsed_ms': int(max(0, refresh_finished_utc_ms - refresh_started_utc_ms)),
    }


def _extract_binance_ban_until_utc_ms(exc: Exception) -> int | None:
    text = str(exc or '')
    if not text:
        return None
    m = _BINANCE_BAN_UNTIL_RE.search(text)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None



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
    data.setdefault('min_24h_quote_volume', 50000000)
    data.setdefault('rollsum_refresh_batch_size', 80)
    return data


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')


def _write_empty_hub_inputs_snapshot(
    account: str,
    snapshot_name: str,
    *,
    latest_closed_bar_ts: int,
    latest_closed_bar_bj: str | None,
    signal_time_ts: int,
    signal_time_bj: str | None,
    reason: str,
    min_24h_quote_volume: float,
    market_total_24h_vol_1m_rollsum: float,
    market_total_24h_symbol_count_1m_rollsum: int,
    market_total_24h_vol_source: str,
    market_total_24h_vol_status: str,
) -> None:
    published_utc_ms = int(time.time() * 1000)
    payload = {
        'schema_version': 1,
        'account': str(account).strip(),
        'snapshot_name': snapshot_name,
        'published_utc_ms': published_utc_ms,
        'published_bj': _fmt_bj_from_ms(published_utc_ms),
        'latest_closed_bar_ts': int(latest_closed_bar_ts),
        'latest_closed_bar_bj': latest_closed_bar_bj,
        'signal_time_ts': int(signal_time_ts),
        'signal_time_bj': signal_time_bj,
        'symbol_count': 0,
        'bars_loaded_min': 0,
        'bars_loaded_max': 0,
        'reason': reason,
        'min_24h_quote_volume': float(min_24h_quote_volume),
        'market_total_24h_vol_1m_rollsum': float(market_total_24h_vol_1m_rollsum),
        'market_total_24h_symbol_count_1m_rollsum': int(market_total_24h_symbol_count_1m_rollsum),
        'market_total_24h_vol_source': str(market_total_24h_vol_source),
        'market_total_24h_vol_status': str(market_total_24h_vol_status),
    }
    write_current_snapshot(account, snapshot_name, payload)
    write_current_pickle(account, snapshot_name, dict(payload))


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
    latest_closed_bar_bj = str(market_snapshot.get('latest_closed_bar_bj') or _fmt_bj_from_ms(latest_closed_bar_ts))
    signal_time_ts = int(market_snapshot['signal_time_ts'])
    signal_time_bj = str(market_snapshot['signal_time_bj'])
    market_total_24h_vol_1m_rollsum = float(market_snapshot.get('market_total_24h_vol_1m_rollsum') or 0.0)
    market_total_24h_symbol_count_1m_rollsum = int(market_snapshot.get('market_total_24h_symbol_count_1m_rollsum') or 0)
    market_total_24h_vol_source = str(market_snapshot.get('market_total_24h_vol_source') or '')
    market_total_24h_vol_status = str(market_snapshot.get('market_total_24h_vol_1m_rollsum_status') or '')
    candidate_symbols = list_candidate_symbols(account)
    rollsum_refreshed_this_round = False

    if market_total_24h_vol_status != 'ready_hub_owned_1m':
        refresh_res = _refresh_rollsum_batch(
            account,
            candidate_symbols=candidate_symbols,
            latest_closed_bar_ts=latest_closed_bar_ts,
            signal_time_ts=signal_time_ts,
            hub_cfg=hub_cfg,
        )
        rollsum_refreshed_this_round = True
        rollsum_view = read_hub_owned_1m_rollsum_market_view(
            account,
            dict(market_snapshot['ticker_map']),
        )
        market_total_24h_vol_1m_rollsum = float(rollsum_view.get('market_total_24h_vol_1m_rollsum') or 0.0)
        market_total_24h_symbol_count_1m_rollsum = int(rollsum_view.get('market_total_24h_symbol_count_1m_rollsum') or 0)
        market_total_24h_vol_source = str(rollsum_view.get('market_total_24h_vol_source') or '')
        market_total_24h_vol_status = str(rollsum_view.get('market_total_24h_vol_status') or '')
        if market_total_24h_vol_status != 'ready_hub_owned_1m':
            warming_payload = read_current_snapshot(account, _HUB_OWNED_1M_ROLLSUM_REFRESH_PROGRESS_SNAPSHOT) or {}
        else:
            warming_payload = {}
        write_event(account, 'hub_owned_1m_rollsum_warming', {
            'bar_ts': signal_time_ts,
            'bar_bj': signal_time_bj,
            'latest_closed_bar_ts': latest_closed_bar_ts,
            'latest_closed_bar_bj': latest_closed_bar_bj,
            'warming_symbol_count': int(len(refresh_res.get('batch_symbols') or [])),
            'warming_reason': '' if market_total_24h_vol_status == 'ready_hub_owned_1m' else 'hub_owned_1m_rollsum_not_ready',
            'warming_errors': {},
            'min_24h_quote_volume': min_24h_quote_volume,
            'market_total_24h_vol_1m_rollsum': market_total_24h_vol_1m_rollsum,
            'market_total_24h_symbol_count_1m_rollsum': market_total_24h_symbol_count_1m_rollsum,
            'market_total_24h_vol_source': market_total_24h_vol_source,
            'market_total_24h_vol_status': market_total_24h_vol_status,
            'refresh_batch_symbols': list(refresh_res.get('batch_symbols') or []),
            'refresh_batch_size': int(refresh_res.get('batch_size') or 0),
            'refresh_elapsed_ms': int(refresh_res.get('refresh_elapsed_ms') or 0),
        })
        if market_total_24h_vol_status != 'ready_hub_owned_1m':
            reason = 'hub_owned_1m_rollsum_not_ready'
            _write_empty_hub_inputs_snapshot(
                account,
                'candidate_inputs',
                latest_closed_bar_ts=latest_closed_bar_ts,
                latest_closed_bar_bj=latest_closed_bar_bj,
                signal_time_ts=signal_time_ts,
                signal_time_bj=signal_time_bj,
                reason=reason,
                min_24h_quote_volume=min_24h_quote_volume,
                market_total_24h_vol_1m_rollsum=market_total_24h_vol_1m_rollsum,
                market_total_24h_symbol_count_1m_rollsum=market_total_24h_symbol_count_1m_rollsum,
                market_total_24h_vol_source=market_total_24h_vol_source,
                market_total_24h_vol_status=market_total_24h_vol_status,
            )
            _write_empty_hub_inputs_snapshot(
                account,
                'finalized_candidate_inputs',
                latest_closed_bar_ts=latest_closed_bar_ts,
                latest_closed_bar_bj=latest_closed_bar_bj,
                signal_time_ts=signal_time_ts,
                signal_time_bj=signal_time_bj,
                reason=reason,
                min_24h_quote_volume=min_24h_quote_volume,
                market_total_24h_vol_1m_rollsum=market_total_24h_vol_1m_rollsum,
                market_total_24h_symbol_count_1m_rollsum=market_total_24h_symbol_count_1m_rollsum,
                market_total_24h_vol_source=market_total_24h_vol_source,
                market_total_24h_vol_status=market_total_24h_vol_status,
            )
            return

    rollsum_view = read_hub_owned_1m_rollsum_market_view(
        account,
        dict(market_snapshot['ticker_map']),
    )
    market_total_24h_vol_1m_rollsum = float(rollsum_view.get('market_total_24h_vol_1m_rollsum') or 0.0)
    market_total_24h_symbol_count_1m_rollsum = int(rollsum_view.get('market_total_24h_symbol_count_1m_rollsum') or 0)
    market_total_24h_vol_source = str(rollsum_view.get('market_total_24h_vol_source') or '')
    market_total_24h_vol_status = str(rollsum_view.get('market_total_24h_vol_status') or '')
    if market_total_24h_vol_status != 'ready_hub_owned_1m':
        reason = 'hub_owned_1m_rollsum_regressed_not_ready'
        write_event(account, reason, {
            'bar_ts': signal_time_ts,
            'bar_bj': signal_time_bj,
            'latest_closed_bar_ts': latest_closed_bar_ts,
            'latest_closed_bar_bj': latest_closed_bar_bj,
            'min_24h_quote_volume': min_24h_quote_volume,
            'market_total_24h_vol_1m_rollsum': market_total_24h_vol_1m_rollsum,
            'market_total_24h_symbol_count_1m_rollsum': market_total_24h_symbol_count_1m_rollsum,
            'market_total_24h_vol_source': market_total_24h_vol_source,
            'market_total_24h_vol_status': market_total_24h_vol_status,
        })
        _write_empty_hub_inputs_snapshot(
            account,
            'candidate_inputs',
            latest_closed_bar_ts=latest_closed_bar_ts,
            latest_closed_bar_bj=latest_closed_bar_bj,
            signal_time_ts=signal_time_ts,
            signal_time_bj=signal_time_bj,
            reason=reason,
            min_24h_quote_volume=min_24h_quote_volume,
            market_total_24h_vol_1m_rollsum=market_total_24h_vol_1m_rollsum,
            market_total_24h_symbol_count_1m_rollsum=market_total_24h_symbol_count_1m_rollsum,
            market_total_24h_vol_source=market_total_24h_vol_source,
            market_total_24h_vol_status=market_total_24h_vol_status,
        )
        _write_empty_hub_inputs_snapshot(
            account,
            'finalized_candidate_inputs',
            latest_closed_bar_ts=latest_closed_bar_ts,
            latest_closed_bar_bj=latest_closed_bar_bj,
            signal_time_ts=signal_time_ts,
            signal_time_bj=signal_time_bj,
            reason=reason,
            min_24h_quote_volume=min_24h_quote_volume,
            market_total_24h_vol_1m_rollsum=market_total_24h_vol_1m_rollsum,
            market_total_24h_symbol_count_1m_rollsum=market_total_24h_symbol_count_1m_rollsum,
            market_total_24h_vol_source=market_total_24h_vol_source,
            market_total_24h_vol_status=market_total_24h_vol_status,
        )
        return

    prefilter_source = 'hub_owned_1m_rollsum'
    symbol_24h_quote_volume_map = dict(rollsum_view.get('symbol_24h_quote_volume_1m') or {})
    finalize_symbols = [
        symbol for symbol in candidate_symbols
        if float(symbol_24h_quote_volume_map.get(str(symbol).upper().strip()) or 0.0) >= min_24h_quote_volume
    ]
    if not finalize_symbols:
        reason = 'hub_candidate_prefilter_empty'
        write_event(account, reason, {
            'bar_ts': signal_time_ts,
            'bar_bj': signal_time_bj,
            'latest_closed_bar_ts': latest_closed_bar_ts,
            'latest_closed_bar_bj': latest_closed_bar_bj,
            'min_24h_quote_volume': min_24h_quote_volume,
            'market_total_24h_vol_1m_rollsum': market_total_24h_vol_1m_rollsum,
            'market_total_24h_symbol_count_1m_rollsum': market_total_24h_symbol_count_1m_rollsum,
            'market_total_24h_vol_source': market_total_24h_vol_source,
            'market_total_24h_vol_status': market_total_24h_vol_status,
            'prefilter_source': prefilter_source,
        })
        _write_empty_hub_inputs_snapshot(
            account,
            'candidate_inputs',
            latest_closed_bar_ts=latest_closed_bar_ts,
            latest_closed_bar_bj=latest_closed_bar_bj,
            signal_time_ts=signal_time_ts,
            signal_time_bj=signal_time_bj,
            reason=reason,
            min_24h_quote_volume=min_24h_quote_volume,
            market_total_24h_vol_1m_rollsum=market_total_24h_vol_1m_rollsum,
            market_total_24h_symbol_count_1m_rollsum=market_total_24h_symbol_count_1m_rollsum,
            market_total_24h_vol_source=market_total_24h_vol_source,
            market_total_24h_vol_status=market_total_24h_vol_status,
        )
        _write_empty_hub_inputs_snapshot(
            account,
            'finalized_candidate_inputs',
            latest_closed_bar_ts=latest_closed_bar_ts,
            latest_closed_bar_bj=latest_closed_bar_bj,
            signal_time_ts=signal_time_ts,
            signal_time_bj=signal_time_bj,
            reason=reason,
            min_24h_quote_volume=min_24h_quote_volume,
            market_total_24h_vol_1m_rollsum=market_total_24h_vol_1m_rollsum,
            market_total_24h_symbol_count_1m_rollsum=market_total_24h_symbol_count_1m_rollsum,
            market_total_24h_vol_source=market_total_24h_vol_source,
            market_total_24h_vol_status=market_total_24h_vol_status,
        )
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

    if not rollsum_refreshed_this_round:
        _refresh_rollsum_batch(
            account,
            candidate_symbols=candidate_symbols,
            latest_closed_bar_ts=latest_closed_bar_ts,
            signal_time_ts=signal_time_ts,
            hub_cfg=hub_cfg,
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
        max_ban_until_epoch: float | None = None
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
                ban_until_utc_ms = _extract_binance_ban_until_utc_ms(e)
                write_event(account, 'hub_runner_error', {
                    'reason': str(e),
                    'error_bj': _fmt_bj_from_ms(int(time.time() * 1000)),
                    'ban_until_utc_ms': ban_until_utc_ms,
                    'ban_until_bj': _fmt_bj_from_ms(ban_until_utc_ms) if ban_until_utc_ms else None,
                })
                if ban_until_utc_ms is not None:
                    ban_until_epoch = (int(ban_until_utc_ms) / 1000.0) + 1.0
                    max_ban_until_epoch = max(max_ban_until_epoch or 0.0, ban_until_epoch)
                    logging.warning('hub runner backoff | account=%s | until_bj=%s', account, _fmt_bj_from_ms(int(ban_until_utc_ms)))
                    write_event(account, 'hub_runner_backoff', {
                        'reason': 'binance_ip_ban',
                        'error_bj': _fmt_bj_from_ms(int(time.time() * 1000)),
                        'ban_until_utc_ms': int(ban_until_utc_ms),
                        'ban_until_bj': _fmt_bj_from_ms(int(ban_until_utc_ms)),
                    })
        if next_signal_check_epoch is not None:
            next_signal_check_epoch += 60.0
            if max_ban_until_epoch is not None:
                next_signal_check_epoch = max(next_signal_check_epoch, max_ban_until_epoch)


if __name__ == '__main__':
    main()
