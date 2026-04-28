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

from core.live.audit_log import append_stage_record, write_event
from core.live.market_data import (
    list_candidate_symbols,
    read_hub_owned_1m_rollsum_market_view,
    refresh_hub_owned_1m_rollsum_for_symbols,
)
from core.live.rate_limit_guard import read_binance_rest_quota_state
from core.live.market_data_hub_store import (
    read_current_snapshot,
    write_current_pickle,
    write_current_snapshot,
    write_shared_current_pickle,
    write_shared_current_snapshot,
)
from core.message_bridge import send_to_bot
from core.live.market_data_hub import (
    build_live_inputs_via_hub,
    build_market_snapshot_via_hub,
    finalize_candidate_payload_via_hub,
)

BJ = timezone(timedelta(hours=8))
_HUB_OWNED_1M_ROLLSUM_REFRESH_PROGRESS_SNAPSHOT = 'rollsum_refresh_progress'
_BINANCE_BAN_UNTIL_RE = re.compile(r'banned until (\d{10,})', re.IGNORECASE)
_MARKET_TOTAL_24H_VOL_STATS_WINDOW = 30
_MARKET_TOTAL_24H_VOL_ROLLING: dict[str, list[dict[str, Any]]] = {}
_FINALIZE_QUALITY_STATS_WINDOW = 30
_FINALIZE_QUALITY_ROLLING: dict[str, list[dict[str, Any]]] = {}
_BINANCE_REST_QUOTA_STATS_WINDOW = 30
_BINANCE_REST_QUOTA_ROLLING: dict[str, list[dict[str, Any]]] = {}


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
    refresh_batch_size = int(hub_cfg['rollsum_refresh_batch_size'])
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


def _require_bool(cfg: dict[str, Any], path: str, key: str) -> bool:
    if key not in cfg:
        raise KeyError(f'hub_config 缺少必要字段: {key} | {path}')
    value = cfg[key]
    if not isinstance(value, bool):
        raise TypeError(f'hub_config 字段类型错误: {key} 必须是 bool | {path}')
    return value


def _require_non_empty_str(cfg: dict[str, Any], path: str, key: str) -> str:
    if key not in cfg:
        raise KeyError(f'hub_config 缺少必要字段: {key} | {path}')
    value = str(cfg[key]).strip()
    if not value:
        raise ValueError(f'hub_config 字段不能为空: {key} | {path}')
    return value


def _require_positive_int(cfg: dict[str, Any], path: str, key: str) -> int:
    if key not in cfg:
        raise KeyError(f'hub_config 缺少必要字段: {key} | {path}')
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f'hub_config 字段类型错误: {key} 必须是 int | {path}')
    try:
        out = int(value)
    except Exception as e:
        raise TypeError(f'hub_config 字段类型错误: {key} 必须是 int | {path}') from e
    if out <= 0:
        raise ValueError(f'hub_config 字段必须 > 0: {key} | {path}')
    return out


def _require_non_negative_float(cfg: dict[str, Any], path: str, key: str) -> float:
    if key not in cfg:
        raise KeyError(f'hub_config 缺少必要字段: {key} | {path}')
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f'hub_config 字段类型错误: {key} 必须是 number | {path}')
    try:
        out = float(value)
    except Exception as e:
        raise TypeError(f'hub_config 字段类型错误: {key} 必须是 number | {path}') from e
    if out < 0:
        raise ValueError(f'hub_config 字段必须 >= 0: {key} | {path}')
    return out


def _require_symbol_list(cfg: dict[str, Any], path: str, key: str) -> list[str]:
    if key not in cfg:
        raise KeyError(f'hub_config 缺少必要字段: {key} | {path}')
    raw = cfg[key]
    if not isinstance(raw, list):
        raise TypeError(f'hub_config 字段类型错误: {key} 必须是 list | {path}')
    out: list[str] = []
    for idx, item in enumerate(raw):
        symbol = str(item).upper().strip()
        if not symbol:
            raise ValueError(f'hub_config {key}[{idx}] 不能为空 | {path}')
        out.append(symbol)
    return out


def _load_hub_config(path: str) -> dict[str, Any]:
    data = _load_json(path)
    if not isinstance(data, dict):
        raise TypeError(f'hub_config 顶层必须是 JSON object | {path}')
    account = _require_non_empty_str(data, path, 'account')
    return {
        'enabled': _require_bool(data, path, 'enabled'),
        'account': account,
        'audit_enabled': _require_bool(data, path, 'audit_enabled'),
        'notify_enabled': _require_bool(data, path, 'notify_enabled'),
        'history_window_mins': _require_positive_int(data, path, 'history_window_mins'),
        'publish_config_snapshot': _require_bool(data, path, 'publish_config_snapshot'),
        'min_24h_quote_volume': _require_non_negative_float(data, path, 'min_24h_quote_volume'),
        'rollsum_refresh_batch_size': _require_positive_int(data, path, 'rollsum_refresh_batch_size'),
        'exclude_symbols': _require_symbol_list(data, path, 'exclude_symbols'),
    }


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')


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
    return str(v)


def _json_safe_dumps(data: Any, *, sort_keys: bool = False, separators: tuple[str, str] | None = None) -> str:
    kwargs: dict[str, Any] = {
        'ensure_ascii': False,
        'default': _json_default,
    }
    if sort_keys:
        kwargs['sort_keys'] = True
    if separators is not None:
        kwargs['separators'] = separators
    return json.dumps(data, **kwargs)


def _notify(enabled: bool, message: str, label: str = 'snapback') -> None:
    if enabled:
        send_to_bot(message, label=label)


def _write_stage_record(account: str, stage: str, payload: dict[str, Any]) -> Path:
    return append_stage_record(account, stage, payload)


def _record_market_total_24h_vol_sample(
    account: str,
    *,
    notify_enabled: bool,
    audit_enabled: bool,
    current_time_ms: int,
    current_time_bj: str,
    c_bar_ts: int,
    c_bar_bj: str,
    market_total_24h_vol: float,
    market_total_24h_symbol_count: int,
    market_total_24h_vol_api: float | None,
    market_total_24h_symbol_count_api: int | None,
) -> None:
    account_key = str(account).strip() or 'unknown'
    sample = {
        'bar_ts': int(current_time_ms),
        'bar_bj': str(current_time_bj),
        'c_bar_ts': int(c_bar_ts),
        'c_bar_bj': str(c_bar_bj),
        'market_total_24h_vol': float(market_total_24h_vol),
        'market_total_24h_symbol_count': int(market_total_24h_symbol_count),
        'market_total_24h_vol_api': float(market_total_24h_vol_api) if market_total_24h_vol_api is not None else None,
        'market_total_24h_symbol_count_api': int(market_total_24h_symbol_count_api) if market_total_24h_symbol_count_api is not None else None,
    }
    bucket = _MARKET_TOTAL_24H_VOL_ROLLING.setdefault(account_key, [])
    bucket.append(sample)
    if len(bucket) < _MARKET_TOTAL_24H_VOL_STATS_WINDOW:
        return

    window = bucket[:_MARKET_TOTAL_24H_VOL_STATS_WINDOW]
    del bucket[:_MARKET_TOTAL_24H_VOL_STATS_WINDOW]

    values = [float(x['market_total_24h_vol']) for x in window]
    min_value = min(values)
    max_value = max(values)
    avg_value = sum(values) / float(len(values))
    api_values = [float(x['market_total_24h_vol_api']) for x in window if x.get('market_total_24h_vol_api') is not None]
    min_value_api = min(api_values) if api_values else None
    max_value_api = max(api_values) if api_values else None
    avg_value_api = (sum(api_values) / float(len(api_values))) if api_values else None
    payload = {
        'account': account_key,
        'window_rounds': int(len(window)),
        'first_bar_ts': int(window[0]['bar_ts']),
        'first_bar_bj': str(window[0]['bar_bj']),
        'last_bar_ts': int(window[-1]['bar_ts']),
        'last_bar_bj': str(window[-1]['bar_bj']),
        'first_c_bar_ts': int(window[0]['c_bar_ts']),
        'first_c_bar_bj': str(window[0]['c_bar_bj']),
        'last_c_bar_ts': int(window[-1]['c_bar_ts']),
        'last_c_bar_bj': str(window[-1]['c_bar_bj']),
        'market_total_24h_vol_min_observed': float(min_value),
        'market_total_24h_vol_max_observed': float(max_value),
        'market_total_24h_vol_avg_observed': float(avg_value),
        'market_total_24h_vol_min_api_observed': float(min_value_api) if min_value_api is not None else None,
        'market_total_24h_vol_max_api_observed': float(max_value_api) if max_value_api is not None else None,
        'market_total_24h_vol_avg_api_observed': float(avg_value_api) if avg_value_api is not None else None,
        'market_total_24h_symbol_count_min_observed': int(min(int(x['market_total_24h_symbol_count']) for x in window)),
        'market_total_24h_symbol_count_max_observed': int(max(int(x['market_total_24h_symbol_count']) for x in window)),
        'market_total_24h_symbol_count_min_api_observed': int(min(int(x['market_total_24h_symbol_count_api']) for x in window if x.get('market_total_24h_symbol_count_api') is not None)) if any(x.get('market_total_24h_symbol_count_api') is not None for x in window) else None,
        'market_total_24h_symbol_count_max_api_observed': int(max(int(x['market_total_24h_symbol_count_api']) for x in window if x.get('market_total_24h_symbol_count_api') is not None)) if any(x.get('market_total_24h_symbol_count_api') is not None for x in window) else None,
    }

    if audit_enabled:
        _write_stage_record(account_key, 'market_total_24h_vol_stats', payload)

    body = _json_safe_dumps(payload, sort_keys=True, separators=(',', ':'))
    logging.info('[market_total_24h_vol_stats] %s', body)

    msg = (
        f'[DataHub] market_total_24h_vol 30轮统计 | account={account_key} | '
        f'window={payload["first_bar_bj"]} ~ {payload["last_bar_bj"]} | '
        f'min={min_value:.2f} | max={max_value:.2f} | avg={avg_value:.2f}'
    )
    if min_value_api is not None and max_value_api is not None and avg_value_api is not None:
        msg += (
            f' | api_min={min_value_api:.2f}'
            f' | api_max={max_value_api:.2f}'
            f' | api_avg={avg_value_api:.2f}'
        )
    _notify(bool(notify_enabled), msg)


def _record_finalize_quality_sample(
    account: str,
    *,
    notify_enabled: bool,
    audit_enabled: bool,
    current_time_ms: int,
    current_time_bj: str,
    c_bar_ts: int,
    c_bar_bj: str,
    candidate_symbol_count_before_finalize: int,
    candidate_symbol_count_after_finalize: int,
    finalize_removed_symbol_count: int,
    finalize_summary: dict[str, Any],
) -> None:
    account_key = str(account).strip() or 'unknown'
    sample = {
        'bar_ts': int(current_time_ms),
        'bar_bj': str(current_time_bj),
        'c_bar_ts': int(c_bar_ts),
        'c_bar_bj': str(c_bar_bj),
        'candidate_symbol_count_before_finalize': int(candidate_symbol_count_before_finalize),
        'candidate_symbol_count_after_finalize': int(candidate_symbol_count_after_finalize),
        'finalize_removed_symbol_count': int(finalize_removed_symbol_count),
        'finalize_rounds': int(finalize_summary.get('finalize_rounds') or 0),
        'deadline_hit': bool(finalize_summary.get('deadline_hit')),
        'all_passed': bool(finalize_summary.get('all_passed')),
        'all_passed_elapsed_ms': (
            int(finalize_summary.get('all_passed_elapsed_ms'))
            if finalize_summary.get('all_passed_elapsed_ms') is not None
            else None
        ),
        'timeout_not_finalized_count': int(finalize_summary.get('timeout_not_finalized_count') or 0),
        'verify_failed_count': int(finalize_summary.get('verify_failed_count') or 0),
        'delayed_finalize_count': int(finalize_summary.get('delayed_finalize_count') or 0),
        'passed_count': int(finalize_summary.get('passed_count') or 0),
    }
    bucket = _FINALIZE_QUALITY_ROLLING.setdefault(account_key, [])
    bucket.append(sample)
    if len(bucket) < _FINALIZE_QUALITY_STATS_WINDOW:
        return

    window = bucket[:_FINALIZE_QUALITY_STATS_WINDOW]
    del bucket[:_FINALIZE_QUALITY_STATS_WINDOW]

    elapsed_values = [int(x['all_passed_elapsed_ms']) for x in window if x.get('all_passed_elapsed_ms') is not None]
    before_values = [int(x['candidate_symbol_count_before_finalize']) for x in window]
    after_values = [int(x['candidate_symbol_count_after_finalize']) for x in window]
    removed_values = [int(x['finalize_removed_symbol_count']) for x in window]
    timeout_values = [int(x['timeout_not_finalized_count']) for x in window]
    verify_failed_values = [int(x['verify_failed_count']) for x in window]
    delayed_values = [int(x['delayed_finalize_count']) for x in window]
    rounds_values = [int(x['finalize_rounds']) for x in window]
    passed_values = [int(x['passed_count']) for x in window]
    all_passed_count = sum(1 for x in window if bool(x.get('all_passed')))
    deadline_hit_count = sum(1 for x in window if bool(x.get('deadline_hit')))
    timeout_round_count = sum(1 for x in window if int(x.get('timeout_not_finalized_count') or 0) > 0)

    payload = {
        'account': account_key,
        'window_rounds': int(len(window)),
        'first_bar_ts': int(window[0]['bar_ts']),
        'first_bar_bj': str(window[0]['bar_bj']),
        'last_bar_ts': int(window[-1]['bar_ts']),
        'last_bar_bj': str(window[-1]['bar_bj']),
        'first_c_bar_ts': int(window[0]['c_bar_ts']),
        'first_c_bar_bj': str(window[0]['c_bar_bj']),
        'last_c_bar_ts': int(window[-1]['c_bar_ts']),
        'last_c_bar_bj': str(window[-1]['c_bar_bj']),
        'all_passed_count': int(all_passed_count),
        'deadline_hit_count': int(deadline_hit_count),
        'timeout_round_count': int(timeout_round_count),
        'timeout_not_finalized_count_max': int(max(timeout_values)) if timeout_values else 0,
        'verify_failed_count_max': int(max(verify_failed_values)) if verify_failed_values else 0,
        'delayed_finalize_count_max': int(max(delayed_values)) if delayed_values else 0,
        'candidate_symbol_count_before_finalize_avg': float(sum(before_values) / float(len(before_values))) if before_values else None,
        'candidate_symbol_count_after_finalize_avg': float(sum(after_values) / float(len(after_values))) if after_values else None,
        'finalize_removed_symbol_count_avg': float(sum(removed_values) / float(len(removed_values))) if removed_values else None,
        'finalize_removed_symbol_count_max': int(max(removed_values)) if removed_values else 0,
        'finalize_rounds_avg': float(sum(rounds_values) / float(len(rounds_values))) if rounds_values else None,
        'passed_count_avg': float(sum(passed_values) / float(len(passed_values))) if passed_values else None,
        'all_passed_elapsed_ms_min': int(min(elapsed_values)) if elapsed_values else None,
        'all_passed_elapsed_ms_max': int(max(elapsed_values)) if elapsed_values else None,
        'all_passed_elapsed_ms_avg': float(sum(elapsed_values) / float(len(elapsed_values))) if elapsed_values else None,
    }

    if audit_enabled:
        _write_stage_record(account_key, 'finalize_quality_stats', payload)

    body = _json_safe_dumps(payload, sort_keys=True, separators=(',', ':'))
    logging.info('[finalize_quality_stats] %s', body)

    msg = (
        f'[DataHub] finalize 30轮统计 | account={account_key} | '
        f'window={payload["first_bar_bj"]} ~ {payload["last_bar_bj"]} | '
        f'all_passed={all_passed_count}/{len(window)} | '
        f'deadline_hit={deadline_hit_count} | '
        f'timeout_rounds={timeout_round_count}'
    )
    if payload['all_passed_elapsed_ms_min'] is not None:
        msg += (
            f' | elapsed_ms(min/max/avg)='
            f'{int(payload["all_passed_elapsed_ms_min"])}/'
            f'{int(payload["all_passed_elapsed_ms_max"])}/'
            f'{float(payload["all_passed_elapsed_ms_avg"]):.1f}'
        )
    if payload['candidate_symbol_count_before_finalize_avg'] is not None and payload['candidate_symbol_count_after_finalize_avg'] is not None:
        msg += (
            f' | candidates_avg='
            f'{float(payload["candidate_symbol_count_before_finalize_avg"]):.1f}'
            f'->{float(payload["candidate_symbol_count_after_finalize_avg"]):.1f}'
        )
    msg += (
        f' | removed_max={int(payload["finalize_removed_symbol_count_max"])}'
        f' | timeout_max={int(payload["timeout_not_finalized_count_max"])}'
    )
    _notify(bool(notify_enabled), msg)


def _record_binance_rest_quota_sample(
    account: str,
    *,
    notify_enabled: bool,
    audit_enabled: bool,
    current_time_ms: int,
    current_time_bj: str,
    c_bar_ts: int,
    c_bar_bj: str,
) -> None:
    quota = read_binance_rest_quota_state()
    if not isinstance(quota, dict):
        return
    account_key = str(account).strip() or 'unknown'
    sample = {
        'bar_ts': int(current_time_ms),
        'bar_bj': str(current_time_bj),
        'c_bar_ts': int(c_bar_ts),
        'c_bar_bj': str(c_bar_bj),
        'used_weight_1m': int(quota.get('used_weight_1m') or 0),
        'used_weight_1m_delta': int(quota.get('used_weight_1m_delta') or 0),
        'order_count_10s': int(quota.get('order_count_10s') or 0),
        'order_count_1m': int(quota.get('order_count_1m') or 0),
        'guard_sleep_count_total': int(quota.get('guard_sleep_count_total') or 0),
        'guard_sleep_secs_total': float(quota.get('guard_sleep_secs_total') or 0.0),
        'ban_count_total': int(quota.get('ban_count_total') or 0),
        'weight_limit_1m': int(quota.get('weight_limit_1m') or 0),
        'weight_guard_threshold_1m': int(quota.get('weight_guard_threshold_1m') or 0),
        'source': str(quota.get('source') or ''),
        'observed_bj': str(quota.get('observed_bj') or ''),
    }
    bucket = _BINANCE_REST_QUOTA_ROLLING.setdefault(account_key, [])
    bucket.append(sample)
    if len(bucket) < _BINANCE_REST_QUOTA_STATS_WINDOW:
        return

    window = bucket[:_BINANCE_REST_QUOTA_STATS_WINDOW]
    del bucket[:_BINANCE_REST_QUOTA_STATS_WINDOW]

    weight_values = [int(x['used_weight_1m']) for x in window]
    delta_values = [int(x['used_weight_1m_delta']) for x in window]
    order_10s_values = [int(x['order_count_10s']) for x in window]
    order_1m_values = [int(x['order_count_1m']) for x in window]
    sleep_count_delta = int(window[-1]['guard_sleep_count_total']) - int(window[0]['guard_sleep_count_total'])
    sleep_secs_delta = float(window[-1]['guard_sleep_secs_total']) - float(window[0]['guard_sleep_secs_total'])
    ban_count_delta = int(window[-1]['ban_count_total']) - int(window[0]['ban_count_total'])
    payload = {
        'account': account_key,
        'window_rounds': int(len(window)),
        'first_bar_ts': int(window[0]['bar_ts']),
        'first_bar_bj': str(window[0]['bar_bj']),
        'last_bar_ts': int(window[-1]['bar_ts']),
        'last_bar_bj': str(window[-1]['bar_bj']),
        'first_c_bar_ts': int(window[0]['c_bar_ts']),
        'first_c_bar_bj': str(window[0]['c_bar_bj']),
        'last_c_bar_ts': int(window[-1]['c_bar_ts']),
        'last_c_bar_bj': str(window[-1]['c_bar_bj']),
        'used_weight_1m_min_observed': int(min(weight_values)) if weight_values else 0,
        'used_weight_1m_max_observed': int(max(weight_values)) if weight_values else 0,
        'used_weight_1m_avg_observed': float(sum(weight_values) / float(len(weight_values))) if weight_values else 0.0,
        'used_weight_1m_delta_max_observed': int(max(delta_values)) if delta_values else 0,
        'order_count_10s_max_observed': int(max(order_10s_values)) if order_10s_values else 0,
        'order_count_1m_max_observed': int(max(order_1m_values)) if order_1m_values else 0,
        'guard_sleep_count_delta': int(max(0, sleep_count_delta)),
        'guard_sleep_secs_delta': float(max(0.0, sleep_secs_delta)),
        'ban_count_delta': int(max(0, ban_count_delta)),
        'weight_limit_1m': int(window[-1]['weight_limit_1m']),
        'weight_guard_threshold_1m': int(window[-1]['weight_guard_threshold_1m']),
        'last_source': str(window[-1]['source']),
        'last_observed_bj': str(window[-1]['observed_bj']),
    }

    if audit_enabled:
        _write_stage_record(account_key, 'binance_rest_quota_stats', payload)

    body = _json_safe_dumps(payload, sort_keys=True, separators=(',', ':'))
    logging.info('[binance_rest_quota_stats] %s', body)

    msg = (
        f'[DataHub] binance_rest_quota 30轮统计 | account={account_key} | '
        f'window={payload["first_bar_bj"]} ~ {payload["last_bar_bj"]} | '
        f'used_weight_1m(min/max/avg)='
        f'{int(payload["used_weight_1m_min_observed"])}/'
        f'{int(payload["used_weight_1m_max_observed"])}/'
        f'{float(payload["used_weight_1m_avg_observed"]):.1f} | '
        f'used_delta_max={int(payload["used_weight_1m_delta_max_observed"])} | '
        f'order10s_max={int(payload["order_count_10s_max_observed"])} | '
        f'order1m_max={int(payload["order_count_1m_max_observed"])} | '
        f'guard_sleep_count={int(payload["guard_sleep_count_delta"])} | '
        f'guard_sleep_secs={float(payload["guard_sleep_secs_delta"]):.1f} | '
        f'ban_count={int(payload["ban_count_delta"])}'
    )
    _notify(bool(notify_enabled), msg)


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
    write_shared_current_snapshot(snapshot_name, payload)
    write_shared_current_pickle(snapshot_name, dict(payload))


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
    audit_enabled = bool(hub_cfg['audit_enabled'])
    notify_enabled = bool(hub_cfg['notify_enabled'])
    history_window_mins = int(hub_cfg['history_window_mins'])
    min_24h_quote_volume = float(hub_cfg['min_24h_quote_volume'])
    exclude_symbols = list(hub_cfg['exclude_symbols'])

    market_snapshot = build_market_snapshot_via_hub(account, audit_enabled=audit_enabled)
    latest_closed_bar_ts = int(market_snapshot['latest_closed_bar_ts'])
    latest_closed_bar_bj = str(market_snapshot.get('latest_closed_bar_bj') or _fmt_bj_from_ms(latest_closed_bar_ts))
    signal_time_ts = int(market_snapshot['signal_time_ts'])
    signal_time_bj = str(market_snapshot['signal_time_bj'])
    market_total_24h_vol_1m_rollsum = float(market_snapshot.get('market_total_24h_vol_1m_rollsum') or 0.0)
    market_total_24h_symbol_count_1m_rollsum = int(market_snapshot.get('market_total_24h_symbol_count_1m_rollsum') or 0)
    market_total_24h_vol_source = str(market_snapshot.get('market_total_24h_vol_source') or '')
    market_total_24h_vol_status = str(market_snapshot.get('market_total_24h_vol_1m_rollsum_status') or '')
    # Keep strategy-facing candidate selection scoped by hub exclude_symbols, but
    # refresh the hub-owned 1m rollsum on the full market universe so the market
    # total 24h volume stays truly market-wide and comparable with Binance 24h API.
    refresh_symbols = list_candidate_symbols(account)
    candidate_symbols = list_candidate_symbols(account, exclude_symbols=exclude_symbols)
    rollsum_refreshed_this_round = False

    if market_total_24h_vol_status != 'ready_hub_owned_1m':
        refresh_res = _refresh_rollsum_batch(
            account,
            candidate_symbols=refresh_symbols,
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
    try:
        _record_market_total_24h_vol_sample(
            account,
            notify_enabled=notify_enabled,
            audit_enabled=audit_enabled,
            current_time_ms=signal_time_ts,
            current_time_bj=signal_time_bj,
            c_bar_ts=latest_closed_bar_ts,
            c_bar_bj=latest_closed_bar_bj,
            market_total_24h_vol=market_total_24h_vol_1m_rollsum,
            market_total_24h_symbol_count=market_total_24h_symbol_count_1m_rollsum,
            market_total_24h_vol_api=(
                float(market_snapshot.get('market_total_24h_vol_api'))
                if market_snapshot.get('market_total_24h_vol_api') is not None
                else None
            ),
            market_total_24h_symbol_count_api=(
                int(market_snapshot.get('market_total_24h_symbol_count_api'))
                if market_snapshot.get('market_total_24h_symbol_count_api') is not None
                else None
            ),
        )
    except Exception as e:
        logging.warning('[market_total_24h_vol_stats] record_failed | account=%s | reason=%s', account, e)
    try:
        _record_binance_rest_quota_sample(
            account,
            notify_enabled=notify_enabled,
            audit_enabled=audit_enabled,
            current_time_ms=signal_time_ts,
            current_time_bj=signal_time_bj,
            c_bar_ts=latest_closed_bar_ts,
            c_bar_bj=latest_closed_bar_bj,
        )
    except Exception as e:
        logging.warning('[binance_rest_quota_stats] record_failed | account=%s | reason=%s', account, e)
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
        shared_output=True,
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

    finalized_payload = finalize_candidate_payload_via_hub(
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
    try:
        candidate_symbol_count_before_finalize = int((candidate_payload or {}).get('symbol_count') or 0)
        candidate_symbol_count_after_finalize = int((finalized_payload or {}).get('symbol_count') or 0)
        finalize_removed_symbol_count = max(0, candidate_symbol_count_before_finalize - candidate_symbol_count_after_finalize)
        finalize_summary = dict((finalized_payload or {}).get('finalize_summary') or {})
        if finalize_summary:
            _record_finalize_quality_sample(
                account,
                notify_enabled=notify_enabled,
                audit_enabled=audit_enabled,
                current_time_ms=int((finalized_payload or {}).get('signal_time_ts') or signal_time_ts),
                current_time_bj=str((finalized_payload or {}).get('signal_time_bj') or signal_time_bj),
                c_bar_ts=int((finalized_payload or {}).get('latest_closed_bar_ts') or latest_closed_bar_ts),
                c_bar_bj=str((finalized_payload or {}).get('latest_closed_bar_bj') or latest_closed_bar_bj),
                candidate_symbol_count_before_finalize=candidate_symbol_count_before_finalize,
                candidate_symbol_count_after_finalize=candidate_symbol_count_after_finalize,
                finalize_removed_symbol_count=finalize_removed_symbol_count,
                finalize_summary=finalize_summary,
            )
    except Exception as e:
        logging.warning('[finalize_quality_stats] record_failed | account=%s | reason=%s', account, e)

    if not rollsum_refreshed_this_round:
        _refresh_rollsum_batch(
            account,
            candidate_symbols=refresh_symbols,
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
        if bool(hub_cfg['publish_config_snapshot']):
            account = str(hub_cfg['account']).strip()
            write_shared_current_snapshot('hub_config', {
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
            if not bool(hub_cfg['enabled']):
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
