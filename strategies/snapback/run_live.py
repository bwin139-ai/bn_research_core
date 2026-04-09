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

from copy import deepcopy

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core.config_loader import StrategyConfig
from core.live.audit_log import append_stage_record, get_live_audit_dir, write_event, write_runner_heartbeat, write_runner_started
from core.live.live_state import (
    load_cooldown_map,
    mark_loop_heartbeat,
    sync_cooldown_map,
)
from core.live.market_data import (
    build_live_inputs,
    build_market_snapshot,
    list_candidate_symbols,
    merge_shared_symbol_bars_cache_stats,
    new_shared_symbol_bars_cache_stats,
)
from core.message_bridge import send_to_bot
from strategies.snapback.logic import WashoutSnapbackStrategy
from strategies.snapback.trade_consumer import (
    append_live_signal_projection,
    bootstrap_consumer_gate,
    build_consumer_reconcile_plan,
    consume_signal,
    consumer_signal_digest,
    finalize_consumer_loop_state,
    prepare_consumer_loop_gate,
)

BJ = timezone(timedelta(hours=8))
_MARKET_DATA_LOGGERS: dict[str, logging.Logger] = {}
_MARKET_DATA_LOG_DIR = Path('output/logs')
_CANDIDATE_FINALIZE_CB_DEADLINE_SECS = 50
_CANDIDATE_FINALIZE_PROBE_INTERVAL_SECS = 1


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


def _perf_elapsed_ms(start_perf: float) -> int:
    return int(round((time.perf_counter() - start_perf) * 1000))


def _market_data_log_path(account: str) -> Path:
    account_key = str(account).strip() or 'unknown'
    _MARKET_DATA_LOG_DIR.mkdir(parents=True, exist_ok=True)
    return _MARKET_DATA_LOG_DIR / f'snapback_market_data.{account_key}.log'


def _get_market_data_logger(account: str) -> logging.Logger:
    account_key = str(account).strip() or 'unknown'
    logger = _MARKET_DATA_LOGGERS.get(account_key)
    if logger is not None:
        return logger
    logger = logging.getLogger(f'snapback.market_data.{account_key}')
    logger.setLevel(logging.INFO)
    logger.propagate = False
    if not logger.handlers:
        handler = logging.FileHandler(_market_data_log_path(account_key), encoding='utf-8')
        handler.setLevel(logging.INFO)
        handler.setFormatter(logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s'))
        logger.addHandler(handler)
    _MARKET_DATA_LOGGERS[account_key] = logger
    return logger


def _log_market_data_event(account: str, level: int, message: str, *args: Any) -> None:
    logger = _get_market_data_logger(account)
    logger.log(level, message, *args)


def _log_perf_stage(account: str, stage: str, payload: dict[str, Any]) -> None:
    try:
        body = _json_safe_dumps(payload, sort_keys=True, separators=(',', ':'))
    except Exception:
        body = str(payload)
    _log_market_data_event(account, logging.INFO, '[perf:%s] %s', stage, body)



def _hydrate_strategy_cooldowns(strategy: WashoutSnapbackStrategy, account: str, current_time_ms: int) -> None:
    strategy.cooldown_until = load_cooldown_map(account, now_ts=current_time_ms)


def _persist_strategy_cooldowns(strategy: WashoutSnapbackStrategy, account: str, current_time_ms: int) -> None:
    sync_cooldown_map(account, getattr(strategy, 'cooldown_until', {}) or {}, now_ts=current_time_ms)


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


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode('utf-8')).hexdigest()


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


def _json_sha256(data: Any) -> str:
    return _sha256_text(_json_safe_dumps(data, sort_keys=True, separators=(',', ':')))


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
    snapshot_path.write_text(_json_safe_dumps(snapshot, indent=2) + '\n', encoding='utf-8')

    return {
        'snapshot_path': str(snapshot_path),
        'snapshot_bj': ts_bj,
        'strategy_config_sha256': snapshot['strategy_config_sha256'],
        'live_config_sha256': snapshot['live_config_sha256'],
        'strategy_config_file_sha256': snapshot['strategy_config_file_sha256'],
        'live_config_file_sha256': snapshot['live_config_file_sha256'],
    }


def _build_live_projection_run_id(account: str) -> str:
    ts_utc = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    account_key = str(account).upper().strip()
    return f'SNAPBACKLIVE_{account_key}_{ts_utc}'


def _live_projection_schema_version() -> int:
    return 2


def _notify(enabled: bool, message: str, label: str = 'snapback') -> None:
    if enabled:
        send_to_bot(message, label=label)


def _cache_miss_symbols_preview(stats: dict[str, Any] | None, key: str, limit: int = 8) -> list[str] | None:
    if not isinstance(stats, dict):
        return None
    items = [str(x).upper().strip() for x in (stats.get(key) or []) if str(x).strip()]
    return items[:limit]



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


def _write_stage_record(account: str, stage: str, payload: dict[str, Any]) -> Path:
    return append_stage_record(account, stage, payload)


def _series_value(row: Any, key: str) -> Any:
    try:
        value = row.get(key)
    except Exception:
        value = None
    try:
        import pandas as _pd  # type: ignore
        if _pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _epoch_to_iso(epoch: float | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()


def _epoch_to_bj(epoch: float | None) -> str | None:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')


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


def _history_records_from_df(df: Any) -> list[dict[str, Any]]:
    if df is None:
        return []
    try:
        if df.empty:
            return []
    except Exception:
        return []

    out: list[dict[str, Any]] = []
    hist = df.sort_index()
    for open_time_ms, row in hist.iterrows():
        out.append({
            'open_time_ms': int(open_time_ms),
            'open_time_bj': _fmt_bj_from_ms(int(open_time_ms)),
            'open': _series_value(row, 'open'),
            'high': _series_value(row, 'high'),
            'low': _series_value(row, 'low'),
            'close': _series_value(row, 'close'),
            'quote_asset_volume': _series_value(row, 'quote_asset_volume'),
            'chg_24h': _series_value(row, 'chg_24h'),
            'vol_24h': _series_value(row, 'vol_24h'),
            'high_idx': _series_value(row, 'high_idx'),
            'low_idx': _series_value(row, 'low_idx'),
            'close_idx': _series_value(row, 'close_idx'),
        })
    return out


def _write_stage3_enriched_snapshot(account: str, audit_label: str, current_time_ms: int, current_time_bj: str, full_df: dict[str, Any], timing_fields: dict[str, Any]) -> None:
    for symbol, df in (full_df or {}).items():
        _write_stage_record(account, 'stage3_enriched', {
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'audit_label': audit_label,
            'symbol': str(symbol).upper().strip(),
            'history_bars': _history_records_from_df(df),
            **timing_fields,
        })


def _candidate_finalize_deadline_utc_ms(c_bar_ts: int) -> int:
    return int(c_bar_ts) + 60000 + int(_CANDIDATE_FINALIZE_CB_DEADLINE_SECS * 1000)


def _sleep_until_utc_ms(target_utc_ms: int, *, deadline_utc_ms: int | None = None) -> bool:
    target_utc_ms = int(target_utc_ms)
    if deadline_utc_ms is not None and target_utc_ms > int(deadline_utc_ms):
        return False
    remaining = (target_utc_ms / 1000.0) - time.time()
    if remaining > 0:
        time.sleep(remaining)
    if deadline_utc_ms is not None and int(time.time() * 1000) > int(deadline_utc_ms):
        return False
    return True


def _next_finalize_probe_utc_ms(previous_probe_utc_ms: int | None, first_snapshot_finished_utc_ms: int | None) -> int | None:
    if previous_probe_utc_ms is None:
        if first_snapshot_finished_utc_ms is None:
            return None
        return int(first_snapshot_finished_utc_ms) + int(_CANDIDATE_FINALIZE_PROBE_INTERVAL_SECS * 1000)
    return int(previous_probe_utc_ms) + int(_CANDIDATE_FINALIZE_PROBE_INTERVAL_SECS * 1000)


def _extract_closed_bar_snapshot(df: Any, c_bar_ts: int) -> dict[str, Any] | None:
    if df is None:
        return None
    try:
        if c_bar_ts not in df.index:
            return None
        row = df.loc[c_bar_ts]
    except Exception:
        return None
    return {
        'open': _normalize_scalar(_series_value(row, 'open')),
        'high': _normalize_scalar(_series_value(row, 'high')),
        'low': _normalize_scalar(_series_value(row, 'low')),
        'close': _normalize_scalar(_series_value(row, 'close')),
        'quote_asset_volume': _normalize_scalar(_series_value(row, 'quote_asset_volume')),
        'high_idx': _normalize_scalar(_series_value(row, 'high_idx')),
        'low_idx': _normalize_scalar(_series_value(row, 'low_idx')),
        'close_idx': _normalize_scalar(_series_value(row, 'close_idx')),
    }


def _drop_symbol_from_cross_section(cross_section: Any, symbol: str) -> Any:
    if cross_section is None:
        return cross_section
    try:
        return cross_section.drop(index=[symbol], errors='ignore')
    except Exception:
        return cross_section


def _build_finalized_candidate_payload(
    candidate_payload: dict[str, Any],
    candidate_cross_section: Any,
    candidate_full_df: dict[str, Any],
    finalize_cache_stats: dict[str, Any],
    finalize_summary: dict[str, Any],
) -> dict[str, Any]:
    latest_closed_bar_ts = int((candidate_payload or {}).get('latest_closed_bar_ts') or 0)
    latest_closed_bar_bj = (candidate_payload or {}).get('latest_closed_bar_bj') or _fmt_bj_from_ms(latest_closed_bar_ts)
    return {
        **candidate_payload,
        'cross_section': candidate_cross_section,
        'full_df': candidate_full_df,
        'symbol_count': int(len(candidate_full_df)),
        'bars_loaded_min': int(min(len(df) for df in candidate_full_df.values())) if candidate_full_df else 0,
        'bars_loaded_max': int(max(len(df) for df in candidate_full_df.values())) if candidate_full_df else 0,
        'freshest_bar_ts': latest_closed_bar_ts,
        'freshest_bar_bj': latest_closed_bar_bj,
        'stale_cutoff_ts': latest_closed_bar_ts,
        'stale_cutoff_bj': latest_closed_bar_bj,
        'stale_symbol_count': 0,
        'stale_symbols': {},
        'finalize_shared_symbol_bars_cache': finalize_cache_stats,
        'finalize_summary': finalize_summary,
    }


def _finalize_candidate_payload(
    account: str,
    strategy_cfg: dict[str, Any],
    candidate_payload: dict[str, Any],
    *,
    history_window_mins: int,
    c_bar_ts: int,
    c_bar_bj: str,
    current_time_ms: int,
    current_time_bj: str,
    candidate_md_finished_utc_ms: int | None,
    audit_enabled: bool,
    latest_closed_bar_ts: int,
    ticker_map: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    candidate_cross_section = candidate_payload['cross_section']
    candidate_full_df = dict(candidate_payload['full_df'])
    finalize_cache_stats = new_shared_symbol_bars_cache_stats()
    finalize_deadline_utc_ms = _candidate_finalize_deadline_utc_ms(c_bar_ts)

    changed_symbols: set[str] = set()
    passed_symbols: list[str] = []
    timeout_symbols: list[str] = []
    verify_failed_symbols: list[str] = []

    last_snapshots: dict[str, dict[str, Any]] = {}
    last_valid_probe_utc_ms_by_symbol: dict[str, int] = {}
    last_valid_probe_bj_by_symbol: dict[str, str] = {}
    pending_symbols: set[str] = set()

    for raw_symbol in list(candidate_full_df.keys()):
        symbol = str(raw_symbol).upper().strip()
        initial_df = candidate_full_df.get(raw_symbol)
        initial_snapshot = _extract_closed_bar_snapshot(initial_df, c_bar_ts)
        if initial_snapshot is None:
            verify_failed_symbols.append(symbol)
            candidate_full_df.pop(raw_symbol, None)
            candidate_cross_section = _drop_symbol_from_cross_section(candidate_cross_section, symbol)
            continue
        pending_symbols.add(symbol)
        last_snapshots[symbol] = initial_snapshot
        last_valid_probe_utc_ms_by_symbol[symbol] = int(candidate_md_finished_utc_ms) if candidate_md_finished_utc_ms is not None else int(current_time_ms)
        last_valid_probe_bj_by_symbol[symbol] = _fmt_bj_from_ms(last_valid_probe_utc_ms_by_symbol[symbol]) or current_time_bj

    finalize_summary = {
        'processed_symbols': int(len(pending_symbols)),
        'verify_failed_count': int(len(verify_failed_symbols)),
        'delayed_finalize_count': 0,
        'verify_failed_symbols': verify_failed_symbols,
        'delayed_symbols': [],
        'skipped_due_deadline': False,
        'deadline_hit': False,
        'finalize_deadline_utc_ms': finalize_deadline_utc_ms,
        'finalize_deadline_bj': _fmt_bj_from_ms(finalize_deadline_utc_ms),
        'finalize_probe_interval_secs': int(_CANDIDATE_FINALIZE_PROBE_INTERVAL_SECS),
        'finalize_rounds': 0,
        'initial_pending_symbol_count': int(len(pending_symbols)),
        'candidate_md_finished_utc_ms': int(candidate_md_finished_utc_ms) if candidate_md_finished_utc_ms is not None else None,
        'candidate_md_finished_bj': _fmt_bj_from_ms(candidate_md_finished_utc_ms),
        'all_passed': False,
        'all_passed_utc_ms': None,
        'all_passed_bj': None,
        'all_passed_elapsed_ms': None,
        'passed_count': 0,
        'passed_symbols': [],
        'timeout_not_finalized_count': 0,
        'timeout_not_finalized_symbols': [],
        'last_valid_probe_utc_ms_by_symbol': dict(last_valid_probe_utc_ms_by_symbol),
        'last_valid_probe_bj_by_symbol': dict(last_valid_probe_bj_by_symbol),
    }

    if not pending_symbols:
        return _build_finalized_candidate_payload(
            candidate_payload,
            candidate_cross_section,
            candidate_full_df,
            finalize_cache_stats,
            finalize_summary,
        )

    next_probe_utc_ms = _next_finalize_probe_utc_ms(None, candidate_md_finished_utc_ms)
    while pending_symbols and next_probe_utc_ms is not None:
        if not _sleep_until_utc_ms(next_probe_utc_ms, deadline_utc_ms=finalize_deadline_utc_ms):
            break
        round_probe_utc_ms = int(time.time() * 1000)
        round_probe_bj = _fmt_bj_from_ms(round_probe_utc_ms) or current_time_bj
        finalize_summary['finalize_rounds'] = int(finalize_summary.get('finalize_rounds', 0)) + 1

        round_pending_symbols = sorted(pending_symbols)
        for symbol in round_pending_symbols:
            refresh_res = build_live_inputs(
                account,
                [symbol],
                history_window_mins,
                strategy_cfg,
                audit_label='candidate_finalize',
                latest_closed_bar_ts=latest_closed_bar_ts,
                ticker_map=ticker_map,
            )
            refresh_payload = refresh_res.get('data') if refresh_res.get('ok') else None
            finalize_cache_stats = merge_shared_symbol_bars_cache_stats(
                finalize_cache_stats,
                (refresh_payload or {}).get('shared_symbol_bars_cache'),
            )
            refreshed_c_bar_ts = int(refresh_payload['latest_closed_bar_ts']) if refresh_payload else None
            refreshed_full_df = dict((refresh_payload or {}).get('full_df') or {})
            refreshed_df = refreshed_full_df.get(symbol)
            refreshed_snapshot = _extract_closed_bar_snapshot(refreshed_df, c_bar_ts) if refreshed_df is not None else None

            if (not refresh_res.get('ok')) or refresh_payload is None or refreshed_c_bar_ts != c_bar_ts or refreshed_snapshot is None:
                _log_market_data_event(
                    account,
                    logging.WARNING,
                    '[c_bar_finalize] probe_pending | symbol=%s | c_bar_bj=%s | round_probe_bj=%s | reason=%s | refreshed_c_bar_bj=%s',
                    symbol,
                    c_bar_bj,
                    round_probe_bj,
                    refresh_res.get('reason') or 'refresh_payload_invalid',
                    _fmt_bj_from_ms(refreshed_c_bar_ts),
                )
                if audit_enabled:
                    write_event(account, 'c_bar_finalize_probe_pending', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'c_bar_ts': c_bar_ts,
                        'c_bar_bj': c_bar_bj,
                        'round_probe_utc_ms': round_probe_utc_ms,
                        'round_probe_bj': round_probe_bj,
                        'refreshed_c_bar_ts': refreshed_c_bar_ts,
                        'refreshed_c_bar_bj': _fmt_bj_from_ms(refreshed_c_bar_ts),
                        'reason': refresh_res.get('reason') or 'refresh_payload_invalid',
                    })
                continue

            last_valid_probe_utc_ms_by_symbol[symbol] = round_probe_utc_ms
            last_valid_probe_bj_by_symbol[symbol] = round_probe_bj

            previous_snapshot = last_snapshots.get(symbol)
            if refreshed_snapshot == previous_snapshot:
                pending_symbols.discard(symbol)
                passed_symbols.append(symbol)
                candidate_full_df[symbol] = refreshed_df
                refreshed_cross_section = (refresh_payload or {}).get('cross_section')
                try:
                    if refreshed_cross_section is not None and symbol in refreshed_cross_section.index:
                        candidate_cross_section.loc[symbol] = refreshed_cross_section.loc[symbol]
                except Exception:
                    candidate_cross_section = _drop_symbol_from_cross_section(candidate_cross_section, symbol)
                _log_market_data_event(
                    account,
                    logging.INFO,
                    '[c_bar_finalize] passed | symbol=%s | c_bar_bj=%s | round_probe_bj=%s | finalize_round=%s',
                    symbol,
                    c_bar_bj,
                    round_probe_bj,
                    finalize_summary['finalize_rounds'],
                )
                if audit_enabled:
                    write_event(account, 'c_bar_finalize_passed', {
                        'symbol': symbol,
                        'bar_ts': current_time_ms,
                        'bar_bj': current_time_bj,
                        'c_bar_ts': c_bar_ts,
                        'c_bar_bj': c_bar_bj,
                        'round_probe_utc_ms': round_probe_utc_ms,
                        'round_probe_bj': round_probe_bj,
                        'finalize_round': finalize_summary['finalize_rounds'],
                    })
                continue

            changed_fields = [
                field
                for field in refreshed_snapshot.keys()
                if refreshed_snapshot.get(field) != (previous_snapshot or {}).get(field)
            ]
            last_snapshots[symbol] = refreshed_snapshot
            candidate_full_df[symbol] = refreshed_df
            refreshed_cross_section = (refresh_payload or {}).get('cross_section')
            try:
                if refreshed_cross_section is not None and symbol in refreshed_cross_section.index:
                    candidate_cross_section.loc[symbol] = refreshed_cross_section.loc[symbol]
            except Exception:
                candidate_cross_section = _drop_symbol_from_cross_section(candidate_cross_section, symbol)

            if symbol not in changed_symbols:
                changed_symbols.add(symbol)
                delayed_symbols = finalize_summary.setdefault('delayed_symbols', [])
                delayed_symbols.append(symbol)
                finalize_summary['delayed_finalize_count'] = int(finalize_summary.get('delayed_finalize_count', 0)) + 1

            _log_market_data_event(
                account,
                logging.WARNING,
                '[c_bar_finalize] still_pending | symbol=%s | c_bar_bj=%s | round_probe_bj=%s | changed_fields=%s',
                symbol,
                c_bar_bj,
                round_probe_bj,
                ','.join(changed_fields),
            )
            if audit_enabled:
                write_event(account, 'c_bar_finalize_still_pending', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'c_bar_ts': c_bar_ts,
                    'c_bar_bj': c_bar_bj,
                    'round_probe_utc_ms': round_probe_utc_ms,
                    'round_probe_bj': round_probe_bj,
                    'finalize_round': finalize_summary['finalize_rounds'],
                    'changed_fields': changed_fields,
                    'previous_snapshot': previous_snapshot,
                    'refreshed_snapshot': refreshed_snapshot,
                })

        if (not pending_symbols) and (not finalize_summary.get('all_passed')):
            all_passed_utc_ms = max(
                int(last_valid_probe_utc_ms_by_symbol.get(symbol, round_probe_utc_ms))
                for symbol in passed_symbols
            ) if passed_symbols else int(round_probe_utc_ms)
            all_passed_elapsed_ms = (
                int(all_passed_utc_ms) - int(candidate_md_finished_utc_ms)
                if candidate_md_finished_utc_ms is not None else None
            )
            finalize_summary['all_passed'] = True
            finalize_summary['all_passed_utc_ms'] = int(all_passed_utc_ms)
            finalize_summary['all_passed_bj'] = _fmt_bj_from_ms(all_passed_utc_ms)
            finalize_summary['all_passed_elapsed_ms'] = int(all_passed_elapsed_ms) if all_passed_elapsed_ms is not None else None
            _log_market_data_event(
                account,
                logging.INFO,
                '[c_bar_finalize] all_passed | c_bar_bj=%s | round_probe_bj=%s | all_passed_elapsed_ms=%s | finalize_rounds=%s',
                c_bar_bj,
                round_probe_bj,
                finalize_summary['all_passed_elapsed_ms'],
                finalize_summary['finalize_rounds'],
            )
            if audit_enabled:
                write_event(account, 'c_bar_finalize_all_passed', {
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'c_bar_ts': c_bar_ts,
                    'c_bar_bj': c_bar_bj,
                    'all_passed_utc_ms': finalize_summary['all_passed_utc_ms'],
                    'all_passed_bj': finalize_summary['all_passed_bj'],
                    'all_passed_elapsed_ms': finalize_summary['all_passed_elapsed_ms'],
                    'finalize_rounds': finalize_summary['finalize_rounds'],
                    'passed_count': int(len(passed_symbols)),
                })

        next_probe_utc_ms = _next_finalize_probe_utc_ms(next_probe_utc_ms, candidate_md_finished_utc_ms)

    if pending_symbols:
        finalize_summary['deadline_hit'] = True
        for symbol in sorted(pending_symbols):
            timeout_symbols.append(symbol)
            candidate_full_df.pop(symbol, None)
            candidate_cross_section = _drop_symbol_from_cross_section(candidate_cross_section, symbol)
            _log_market_data_event(
                account,
                logging.WARNING,
                '[c_bar_finalize] timeout_not_finalized | symbol=%s | c_bar_bj=%s | deadline_bj=%s',
                symbol,
                c_bar_bj,
                finalize_summary['finalize_deadline_bj'],
            )
            if audit_enabled:
                write_event(account, 'c_bar_finalize_timeout_not_finalized', {
                    'symbol': symbol,
                    'bar_ts': current_time_ms,
                    'bar_bj': current_time_bj,
                    'c_bar_ts': c_bar_ts,
                    'c_bar_bj': c_bar_bj,
                    'finalize_deadline_utc_ms': finalize_deadline_utc_ms,
                    'finalize_deadline_bj': finalize_summary['finalize_deadline_bj'],
                    'last_valid_probe_utc_ms': last_valid_probe_utc_ms_by_symbol.get(symbol),
                    'last_valid_probe_bj': last_valid_probe_bj_by_symbol.get(symbol),
                })

    finalize_summary['passed_count'] = int(len(passed_symbols))
    finalize_summary['passed_symbols'] = passed_symbols
    finalize_summary['timeout_not_finalized_count'] = int(len(timeout_symbols))
    finalize_summary['timeout_not_finalized_symbols'] = timeout_symbols
    finalize_summary['last_valid_probe_utc_ms_by_symbol'] = dict(last_valid_probe_utc_ms_by_symbol)
    finalize_summary['last_valid_probe_bj_by_symbol'] = dict(last_valid_probe_bj_by_symbol)

    return _build_finalized_candidate_payload(
        candidate_payload,
        candidate_cross_section,
        candidate_full_df,
        finalize_cache_stats,
        finalize_summary,
    )


def _build_stage5_structure_rows(c_bar_ts: int, signal_time_ms: int, signal_time_bj: str, cross_section: Any, active_symbols: set[str], full_df: dict[str, Any], strategy_cfg: dict[str, Any], *, logic_selected_symbol: str | None, signal_digest: str | None) -> list[dict[str, Any]]:
    import pandas as pd  # type: ignore

    universe = (strategy_cfg or {}).get('universe') or {}
    structure = (strategy_cfg or {}).get('structure') or {}
    selloff = (structure.get('selloff') or {})
    rebound = (structure.get('rebound') or {})
    basis = (structure.get('basis') or {})
    s_to_c_window = (structure.get('s_to_c_window') or {})
    exit_policy = (strategy_cfg or {}).get('exit_policy') or {}
    take_profit = (exit_policy.get('take_profit') or {})
    strong_mode = (take_profit.get('strong_mode') or {})

    min_24h_vol = float(universe.get('24h_quote_volume_min', 0.0))
    min_24h_chg = float(((universe.get('24h_chg_pct') or {}).get('min', -100.0)))
    max_24h_chg = float(((universe.get('24h_chg_pct') or {}).get('max', 1000.0)))

    drop_window = int(s_to_c_window.get('mins', 0))
    min_drop_window_chg = float(((s_to_c_window.get('chg_pct') or {}).get('min', -100.0))) / 100.0
    max_drop_window_chg = float(((s_to_c_window.get('chg_pct') or {}).get('max', 1000.0))) / 100.0

    min_ab_bars = int(((selloff.get('ab_bars') or {}).get('min', 0)))
    max_ab_bars = int(((selloff.get('ab_bars') or {}).get('max', 999999)))
    min_drop_pct = float(((selloff.get('a_to_c_drop_pct') or {}).get('min', 0.0)))
    max_drop_pct = float(((selloff.get('a_to_c_drop_pct') or {}).get('max', 1e9)))
    vol_climax_window = int(((selloff.get('vol_climax') or {}).get('recent_window_mins', 1)))
    vol_baseline_window = int(((selloff.get('vol_climax') or {}).get('baseline_window_mins', 1)))
    min_vol_ratio = float(((selloff.get('vol_climax') or {}).get('ratio_min', 0.0)))

    min_rebound_ratio = float(((rebound.get('ratio') or {}).get('min', 0.0)))
    max_rebound_ratio = float(((rebound.get('ratio') or {}).get('max', 1e9)))
    min_bc_bars = int(rebound.get('bc_bars_min', 0))
    max_basis_b_pct = float(((basis.get('b_pct') or {}).get('max', 1e9)))

    base_tp_pct = float(take_profit.get('base_pct', 0.0))
    strong_tp_pct = float(take_profit.get('strong_pct', 0.0))
    strong_tp_min_drop_pct = float(strong_mode.get('a_to_c_drop_pct_min', 1e9))
    strong_tp_min_rebound_ratio = float(strong_mode.get('rebound_ratio_min', 1e9))

    audit_rows: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []

    if cross_section is None or getattr(cross_section, 'empty', True):
        return audit_rows

    cs = cross_section.dropna(subset=['vol_24h', 'chg_24h']).copy()
    for sym, row in cross_section.iterrows():
        symbol = str(sym).upper().strip()
        base = {
            'symbol': symbol,
            'bar_ts': signal_time_ms,
            'bar_bj': signal_time_bj,
            'signal_time_ts': signal_time_ms,
            'signal_time_bj': signal_time_bj,
            'c_bar_ts': c_bar_ts,
            'c_bar_bj': _fmt_bj_from_ms(c_bar_ts),
            'active_symbols_contains': bool(symbol in active_symbols),
            'logic_selected_symbol': logic_selected_symbol,
            'logic_selected': bool(logic_selected_symbol == symbol),
            'signal_digest': signal_digest,
            'cross_close': _series_value(row, 'close'),
            'cross_quote_asset_volume': _series_value(row, 'quote_asset_volume'),
            'cross_chg_24h': _series_value(row, 'chg_24h'),
            'cross_vol_24h': _series_value(row, 'vol_24h'),
            'cross_high_idx': _series_value(row, 'high_idx'),
            'cross_low_idx': _series_value(row, 'low_idx'),
            'cross_close_idx': _series_value(row, 'close_idx'),
            'min_24h_vol': min_24h_vol,
            'min_24h_chg_pct': min_24h_chg,
            'max_24h_chg_pct': max_24h_chg,
        }
        if symbol not in cs.index:
            base.update({
                'stage5_pass': False,
                'is_candidate': False,
                'fail_reason': 'filtered_out_before_structure',
            })
            audit_rows.append(base)
            continue

        row2 = cs.loc[sym]
        if symbol in active_symbols:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'active_symbol_skip'})
            audit_rows.append(base)
            continue

        sym_df = (full_df or {}).get(sym)
        if sym_df is None:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'full_df_missing'})
            audit_rows.append(base)
            continue

        idx = sym_df.index.searchsorted(c_bar_ts, side='right')
        base['history_searchsorted_idx'] = int(idx)
        if idx < vol_baseline_window:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'history_too_short_before_baseline_window'})
            audit_rows.append(base)
            continue

        start_idx = max(0, idx - vol_baseline_window - 5)
        history_df = sym_df.iloc[start_idx:idx]
        base['history_start_idx'] = int(start_idx)
        base['history_rows'] = int(len(history_df))
        if len(history_df) < vol_baseline_window:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'history_too_short_after_slice'})
            audit_rows.append(base)
            continue

        current_price = row2['close']
        recent_drop_df = history_df.tail(drop_window)
        sc_window_df = history_df.tail(drop_window + 1)
        if len(sc_window_df) < drop_window + 1:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'sc_window_too_short'})
            audit_rows.append(base)
            continue

        s_ts = int(sc_window_df.index[0])
        s_close = sc_window_df.iloc[0]['close']
        base.update({
            's_time': s_ts,
            's_time_bj': _fmt_bj_from_ms(s_ts),
            's_close': _normalize_scalar(s_close),
            'c_time': c_bar_ts,
            'c_time_bj': _fmt_bj_from_ms(c_bar_ts),
            'c_price': _normalize_scalar(current_price),
        })
        if pd.isna(s_close) or s_close <= 0:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'invalid_s_close'})
            audit_rows.append(base)
            continue

        drop_window_chg = (current_price - s_close) / s_close
        base['drop_window_chg'] = _normalize_scalar(drop_window_chg)
        if drop_window_chg < min_drop_window_chg:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'drop_window_chg_below_min'})
            audit_rows.append(base)
            continue
        if drop_window_chg > max_drop_window_chg:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'drop_window_chg_above_max'})
            audit_rows.append(base)
            continue
        if row2['chg_24h'] > 0 and drop_window_chg > 0:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'hot_market_quadrant_skip'})
            audit_rows.append(base)
            continue

        recent_high_ts = int(recent_drop_df['high'].idxmax())
        recent_high_price = recent_drop_df.loc[recent_high_ts, 'high']
        ac_df = recent_drop_df.loc[recent_high_ts:]
        base.update({
            'a_time': recent_high_ts,
            'a_time_bj': _fmt_bj_from_ms(recent_high_ts),
            'a_high_price': _normalize_scalar(recent_high_price),
            'ac_rows': int(len(ac_df)),
        })
        if ac_df.empty:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'ac_df_empty'})
            audit_rows.append(base)
            continue

        drop_pct = ((recent_high_price - current_price) / recent_high_price) if recent_high_price > 0 else 0.0
        base['drop_pct'] = _normalize_scalar(drop_pct)
        if drop_pct < min_drop_pct:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'drop_pct_below_min'})
            audit_rows.append(base)
            continue
        if drop_pct > max_drop_pct:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'drop_pct_above_max'})
            audit_rows.append(base)
            continue

        vol_climax = history_df['quote_asset_volume'].tail(vol_climax_window).mean()
        vol_baseline = history_df['quote_asset_volume'].tail(vol_baseline_window).mean()
        vol_ratio = vol_climax / vol_baseline if vol_baseline > 0 else 0.0
        base.update({
            'vol_climax': _normalize_scalar(vol_climax),
            'vol_baseline': _normalize_scalar(vol_baseline),
            'vol_ratio': _normalize_scalar(vol_ratio),
        })
        if vol_ratio < min_vol_ratio:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'vol_ratio_below_min'})
            audit_rows.append(base)
            continue

        b_contract_ts = int(ac_df['low'].idxmin())
        b_contract_price = ac_df.loc[b_contract_ts, 'low']
        b_index_price = ac_df.loc[b_contract_ts, 'low_idx']
        base.update({
            'b_time': b_contract_ts,
            'b_time_bj': _fmt_bj_from_ms(b_contract_ts),
            'b_contract_price': _normalize_scalar(b_contract_price),
            'b_index_price': _normalize_scalar(b_index_price),
        })
        if pd.isna(b_index_price) or b_index_price <= 0:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'invalid_b_index_price'})
            audit_rows.append(base)
            continue

        basis_b_pct = (b_contract_price - b_index_price) / b_index_price
        base['basis_b_pct'] = _normalize_scalar(basis_b_pct)
        if basis_b_pct > max_basis_b_pct:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'basis_b_pct_above_max'})
            audit_rows.append(base)
            continue

        extreme_drop_range = recent_high_price - b_index_price
        base['extreme_drop_range'] = _normalize_scalar(extreme_drop_range)
        if extreme_drop_range <= 0:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'extreme_drop_range_non_positive'})
            audit_rows.append(base)
            continue
        if current_price <= b_index_price:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'current_price_below_or_equal_b_index'})
            audit_rows.append(base)
            continue

        b_pos = int(ac_df.index.get_indexer([b_contract_ts])[0])
        base['b_pos'] = b_pos
        if b_pos < 0:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'invalid_b_pos'})
            audit_rows.append(base)
            continue

        ab_bars = b_pos
        base['ab_bars'] = ab_bars
        if ab_bars < min_ab_bars:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'ab_bars_below_min'})
            audit_rows.append(base)
            continue
        if ab_bars > max_ab_bars:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'ab_bars_above_max'})
            audit_rows.append(base)
            continue

        bc_bars = (len(ac_df) - 1) - b_pos
        base['bc_bars'] = bc_bars
        if bc_bars < min_bc_bars:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'bc_bars_below_min'})
            audit_rows.append(base)
            continue

        rebound_ratio = (current_price - b_index_price) / extreme_drop_range
        base['rebound_ratio'] = _normalize_scalar(rebound_ratio)
        if rebound_ratio < min_rebound_ratio:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'rebound_ratio_below_min'})
            audit_rows.append(base)
            continue
        if rebound_ratio > max_rebound_ratio:
            base.update({'stage5_pass': False, 'is_candidate': False, 'fail_reason': 'rebound_ratio_above_max'})
            audit_rows.append(base)
            continue

        selected_tp_pct = base_tp_pct
        tp_tier = 'BASE'
        if drop_pct >= strong_tp_min_drop_pct and rebound_ratio >= strong_tp_min_rebound_ratio:
            selected_tp_pct = strong_tp_pct
            tp_tier = 'STRONG'

        base.update({
            'stage5_pass': True,
            'is_candidate': True,
            'fail_reason': '',
            'trigger_name': 'ABC_BINDEX',
            'selected_tp_pct': _normalize_scalar(selected_tp_pct),
            'tp_tier': tp_tier,
        })
        audit_rows.append(base)
        candidates.append(base)

    candidates_sorted = sorted(candidates, key=lambda x: x['drop_pct'], reverse=True)
    audit_selected_symbol = candidates_sorted[0]['symbol'] if candidates_sorted else None
    candidate_rank_map = {row['symbol']: i + 1 for i, row in enumerate(candidates_sorted)}
    for row in audit_rows:
        row['audit_selected_symbol'] = audit_selected_symbol
        row['audit_selected'] = bool(audit_selected_symbol == row['symbol'])
        row['candidate_rank'] = candidate_rank_map.get(row['symbol'])
    return audit_rows




def _next_signal_check_epoch(now_epoch: float | None = None) -> float:
    if now_epoch is None:
        now_epoch = time.time()
    now = datetime.fromtimestamp(now_epoch, tz=timezone.utc)
    current_minute_second_second = now.replace(second=5, microsecond=0)
    if now < current_minute_second_second:
        return current_minute_second_second.timestamp()
    next_minute_second_second = now.replace(second=0, microsecond=0) + timedelta(minutes=1, seconds=5)
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


def _run_once(strategy_cfg: dict[str, Any], live_cfg: dict[str, Any], scheduled_signal_check_epoch: float | None = None) -> None:
    account = str(live_cfg['account']).strip()
    notify_enabled = bool(live_cfg.get('notify_enabled', False))
    audit_enabled = bool(live_cfg.get('audit_enabled', True))
    run_once_perf_started = time.perf_counter()
    loop_started_epoch = time.time()
    loop_started_utc_ms = int(loop_started_epoch * 1000)
    loop_started_bj = _fmt_bj_from_ms(loop_started_utc_ms) or _now_bj_str()
    scheduled_signal_check_utc = _epoch_to_iso(scheduled_signal_check_epoch)
    scheduled_signal_check_bj = _epoch_to_bj(scheduled_signal_check_epoch)
    runtime_cfg = (strategy_cfg or {}).get('runtime') or {}
    if 'max_history_window_mins' not in runtime_cfg:
        raise KeyError('strategy_cfg.runtime.max_history_window_mins missing')
    history_window_mins = int(runtime_cfg['max_history_window_mins'])
    if history_window_mins <= 0:
        raise ValueError('strategy_cfg.runtime.max_history_window_mins must be > 0')

    candidate_symbols_count = 0
    extra_reconcile_symbols_count = 0
    exchange_activity_symbols_count = 0
    local_activity_symbols_count = 0
    active_symbols_count = 0

    candidate_plan_elapsed_ms: int | None = None
    candidate_md_elapsed_ms: int | None = None
    extra_md_elapsed_ms: int | None = None
    stage3_elapsed_ms: int | None = None
    latest_closes_elapsed_ms: int | None = None
    loop_gate_elapsed_ms: int | None = None
    stage4_elapsed_ms: int | None = None
    signal_eval_elapsed_ms: int | None = None
    stage5_elapsed_ms: int | None = None
    finalize_elapsed_ms: int | None = None
    live_signal_projection_elapsed_ms: int | None = None
    consume_signal_elapsed_ms: int | None = None
    signal_present = False
    signal_symbol: str | None = None
    signal_digest_preview: str | None = None
    candidate_cache_stats: dict[str, Any] | None = None
    extra_cache_stats: dict[str, Any] | None = None
    finalize_cache_stats: dict[str, Any] | None = None
    finalize_summary: dict[str, Any] | None = None
    candidate_symbol_count_before_finalize: int | None = None
    candidate_symbol_count_after_finalize: int | None = None
    finalize_removed_symbol_count: int | None = None
    finalize_removed_ratio_pct: float | None = None
    finalize_verify_failed_ratio_pct: float | None = None
    finalize_delayed_ratio_pct: float | None = None
    finalize_kept_ratio_pct: float | None = None
    finalize_unchanged_count: int | None = None
    finalize_unchanged_ratio_pct: float | None = None
    finalize_affected_ratio_pct: float | None = None

    current_time_ms: int | None = None
    current_time_bj: str | None = None
    c_bar_ts: int | None = None
    c_bar_bj: str | None = None

    def _emit_run_once_perf(outcome: str) -> None:
        payload = {
            'account': account,
            'bar_bj': current_time_bj,
            'bar_ts': current_time_ms,
            'c_bar_bj': c_bar_bj,
            'c_bar_ts': c_bar_ts,
            'outcome': outcome,
            'scheduled_signal_check_bj': scheduled_signal_check_bj,
            'market_snapshot_fetched_bj': market_snapshot_fetched_bj if 'market_snapshot_fetched_bj' in locals() else None,
            'shared_symbol_bars_cache_enabled': True,
            'history_window_mins': history_window_mins,
            'candidate_contract_cache_hits': (candidate_cache_stats or {}).get('contract_hits') if 'candidate_cache_stats' in locals() else None,
            'candidate_contract_cache_misses': (candidate_cache_stats or {}).get('contract_misses') if 'candidate_cache_stats' in locals() else None,
            'candidate_index_cache_hits': (candidate_cache_stats or {}).get('index_hits') if 'candidate_cache_stats' in locals() else None,
            'candidate_index_cache_misses': (candidate_cache_stats or {}).get('index_misses') if 'candidate_cache_stats' in locals() else None,
            'candidate_contract_cache_miss_symbols_preview': _cache_miss_symbols_preview(candidate_cache_stats, 'contract_miss_symbols') if 'candidate_cache_stats' in locals() else None,
            'candidate_index_cache_miss_symbols_preview': _cache_miss_symbols_preview(candidate_cache_stats, 'index_miss_symbols') if 'candidate_cache_stats' in locals() else None,
            'extra_contract_cache_hits': (extra_cache_stats or {}).get('contract_hits') if 'extra_cache_stats' in locals() else None,
            'extra_contract_cache_misses': (extra_cache_stats or {}).get('contract_misses') if 'extra_cache_stats' in locals() else None,
            'extra_index_cache_hits': (extra_cache_stats or {}).get('index_hits') if 'extra_cache_stats' in locals() else None,
            'extra_index_cache_misses': (extra_cache_stats or {}).get('index_misses') if 'extra_cache_stats' in locals() else None,
            'extra_contract_cache_miss_symbols_preview': _cache_miss_symbols_preview(extra_cache_stats, 'contract_miss_symbols') if 'extra_cache_stats' in locals() else None,
            'extra_index_cache_miss_symbols_preview': _cache_miss_symbols_preview(extra_cache_stats, 'index_miss_symbols') if 'extra_cache_stats' in locals() else None,
            'finalize_contract_cache_hits': (finalize_cache_stats or {}).get('contract_hits') if 'finalize_cache_stats' in locals() else None,
            'finalize_contract_cache_misses': (finalize_cache_stats or {}).get('contract_misses') if 'finalize_cache_stats' in locals() else None,
            'finalize_index_cache_hits': (finalize_cache_stats or {}).get('index_hits') if 'finalize_cache_stats' in locals() else None,
            'finalize_index_cache_misses': (finalize_cache_stats or {}).get('index_misses') if 'finalize_cache_stats' in locals() else None,
            'finalize_contract_cache_miss_symbols_preview': _cache_miss_symbols_preview(finalize_cache_stats, 'contract_miss_symbols') if 'finalize_cache_stats' in locals() else None,
            'finalize_index_cache_miss_symbols_preview': _cache_miss_symbols_preview(finalize_cache_stats, 'index_miss_symbols') if 'finalize_cache_stats' in locals() else None,
            'finalize_processed_symbols': (finalize_summary or {}).get('processed_symbols') if 'finalize_summary' in locals() else None,
            'finalize_verify_failed_count': (finalize_summary or {}).get('verify_failed_count') if 'finalize_summary' in locals() else None,
            'finalize_delayed_finalize_count': (finalize_summary or {}).get('delayed_finalize_count') if 'finalize_summary' in locals() else None,
            'finalize_verify_failed_symbols_preview': _cache_miss_symbols_preview(finalize_summary, 'verify_failed_symbols') if 'finalize_summary' in locals() else None,
            'finalize_delayed_symbols_preview': _cache_miss_symbols_preview(finalize_summary, 'delayed_symbols') if 'finalize_summary' in locals() else None,
            'finalize_passed_count': (finalize_summary or {}).get('passed_count') if 'finalize_summary' in locals() else None,
            'finalize_timeout_not_finalized_count': (finalize_summary or {}).get('timeout_not_finalized_count') if 'finalize_summary' in locals() else None,
            'finalize_timeout_not_finalized_symbols_preview': _cache_miss_symbols_preview(finalize_summary, 'timeout_not_finalized_symbols') if 'finalize_summary' in locals() else None,
            'finalize_rounds': (finalize_summary or {}).get('finalize_rounds') if 'finalize_summary' in locals() else None,
            'finalize_deadline_hit': (finalize_summary or {}).get('deadline_hit') if 'finalize_summary' in locals() else None,
            'finalize_all_passed': (finalize_summary or {}).get('all_passed') if 'finalize_summary' in locals() else None,
            'finalize_all_passed_utc_ms': (finalize_summary or {}).get('all_passed_utc_ms') if 'finalize_summary' in locals() else None,
            'finalize_all_passed_bj': (finalize_summary or {}).get('all_passed_bj') if 'finalize_summary' in locals() else None,
            'finalize_all_passed_elapsed_ms': (finalize_summary or {}).get('all_passed_elapsed_ms') if 'finalize_summary' in locals() else None,
            'candidate_symbol_count_before_finalize': candidate_symbol_count_before_finalize,
            'candidate_symbol_count_after_finalize': candidate_symbol_count_after_finalize,
            'finalize_removed_symbol_count': finalize_removed_symbol_count,
            'finalize_removed_ratio_pct': finalize_removed_ratio_pct,
            'finalize_verify_failed_ratio_pct': finalize_verify_failed_ratio_pct,
            'finalize_delayed_ratio_pct': finalize_delayed_ratio_pct,
            'finalize_kept_ratio_pct': finalize_kept_ratio_pct,
            'finalize_unchanged_count': finalize_unchanged_count,
            'finalize_unchanged_ratio_pct': finalize_unchanged_ratio_pct,
            'finalize_affected_ratio_pct': finalize_affected_ratio_pct,
            'finalize_timeout_not_finalized_count': (finalize_summary or {}).get('timeout_not_finalized_count'),
            'finalize_timeout_not_finalized_symbols': (finalize_summary or {}).get('timeout_not_finalized_symbols'),
            'finalize_passed_count': (finalize_summary or {}).get('passed_count'),
            'finalize_passed_symbols': (finalize_summary or {}).get('passed_symbols'),
            'finalize_rounds': (finalize_summary or {}).get('finalize_rounds'),
            'finalize_probe_interval_secs': (finalize_summary or {}).get('finalize_probe_interval_secs'),
            'finalize_deadline_utc_ms': (finalize_summary or {}).get('finalize_deadline_utc_ms'),
            'finalize_deadline_bj': (finalize_summary or {}).get('finalize_deadline_bj'),
            'candidate_symbols_count': candidate_symbols_count,
            'extra_reconcile_symbols_count': extra_reconcile_symbols_count,
            'exchange_activity_symbols_count': exchange_activity_symbols_count,
            'local_activity_symbols_count': local_activity_symbols_count,
            'active_symbols_count': active_symbols_count,
            'candidate_plan_elapsed_ms': candidate_plan_elapsed_ms,
            'candidate_md_elapsed_ms': candidate_md_elapsed_ms,
            'extra_md_elapsed_ms': extra_md_elapsed_ms,
            'stage3_elapsed_ms': stage3_elapsed_ms,
            'latest_closes_elapsed_ms': latest_closes_elapsed_ms,
            'loop_gate_elapsed_ms': loop_gate_elapsed_ms,
            'stage4_elapsed_ms': stage4_elapsed_ms,
            'signal_eval_elapsed_ms': signal_eval_elapsed_ms,
            'stage5_elapsed_ms': stage5_elapsed_ms,
            'finalize_elapsed_ms': finalize_elapsed_ms,
            'live_signal_projection_elapsed_ms': live_signal_projection_elapsed_ms,
            'consume_signal_elapsed_ms': consume_signal_elapsed_ms,
            'signal_present': signal_present,
            'signal_symbol': signal_symbol,
            'signal_digest': signal_digest_preview,
            'total_elapsed_ms': _perf_elapsed_ms(run_once_perf_started),
        }
        _log_perf_stage(account, 'run_once', payload)
        if audit_enabled:
            _write_stage_record(account, 'stage0_run_once_perf', payload)

    candidate_plan_perf_started = time.perf_counter()
    candidate_symbols = list_candidate_symbols(account, exclude_symbols=live_cfg.get('exclude_symbols') or [])
    reconcile_plan = build_consumer_reconcile_plan(account, candidate_symbols)
    candidate_plan_elapsed_ms = _perf_elapsed_ms(candidate_plan_perf_started)

    exchange_activity_snapshot = dict(reconcile_plan['exchange_snapshot'])
    exchange_activity_symbols = set(reconcile_plan['exchange_activity_symbols'])
    local_activity_symbols = set(reconcile_plan['local_active_symbols'])
    extra_reconcile_symbols = list(reconcile_plan['extra_reconcile_symbols'])
    candidate_symbols_count = len(candidate_symbols)
    extra_reconcile_symbols_count = len(extra_reconcile_symbols)
    exchange_activity_symbols_count = len(exchange_activity_symbols)
    local_activity_symbols_count = len(local_activity_symbols)

    market_snapshot = build_market_snapshot(account)
    latest_closed_bar_ts_snapshot = int(market_snapshot['latest_closed_bar_ts'])
    ticker_map_snapshot = dict(market_snapshot['ticker_map'])
    market_snapshot_fetched_utc_ms = int(market_snapshot['market_snapshot_fetched_utc_ms'])
    market_snapshot_fetched_bj = str(market_snapshot['market_snapshot_fetched_bj'])

    candidate_md_started_utc_ms = _now_utc_ms()
    candidate_md_perf_started = time.perf_counter()
    candidate_md_res = build_live_inputs(
        account,
        candidate_symbols,
        history_window_mins,
        strategy_cfg,
        audit_label='candidate',
        latest_closed_bar_ts=latest_closed_bar_ts_snapshot,
        ticker_map=ticker_map_snapshot,
    )
    candidate_md_elapsed_ms = _perf_elapsed_ms(candidate_md_perf_started)
    candidate_md_finished_utc_ms = _now_utc_ms()
    extra_md_res: dict[str, Any] | None = None
    extra_md_started_utc_ms: int | None = None
    extra_md_finished_utc_ms: int | None = None
    if extra_reconcile_symbols:
        extra_md_started_utc_ms = _now_utc_ms()
        extra_md_perf_started = time.perf_counter()
        extra_md_res = build_live_inputs(
            account,
            extra_reconcile_symbols,
            history_window_mins,
            strategy_cfg,
            audit_label='reconcile',
            latest_closed_bar_ts=latest_closed_bar_ts_snapshot,
            ticker_map=ticker_map_snapshot,
        )
        extra_md_elapsed_ms = _perf_elapsed_ms(extra_md_perf_started)
        extra_md_finished_utc_ms = _now_utc_ms()

    candidate_payload = candidate_md_res.get('data') if candidate_md_res.get('ok') else None
    extra_payload = extra_md_res.get('data') if extra_md_res and extra_md_res.get('ok') else None
    candidate_cache_stats = dict((candidate_payload or {}).get('shared_symbol_bars_cache') or {}) if candidate_payload else None
    extra_cache_stats = dict((extra_payload or {}).get('shared_symbol_bars_cache') or {}) if extra_payload else None

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
        _emit_run_once_perf('no_live_inputs')
        return

    c_bar_ts = int(payload['latest_closed_bar_ts'])
    c_bar_bj = payload['latest_closed_bar_bj']
    current_time_ms = int(payload.get('signal_time_ts') or (c_bar_ts + 60000))
    current_time_bj = str(payload.get('signal_time_bj') or _fmt_bj_from_ms(current_time_ms) or '')

    if candidate_payload:
        candidate_symbol_count_before_finalize = int((candidate_payload or {}).get('symbol_count') or 0)
        finalize_perf_started = time.perf_counter()
        candidate_payload = _finalize_candidate_payload(
            account,
            strategy_cfg,
            candidate_payload,
            history_window_mins=history_window_mins,
            c_bar_ts=c_bar_ts,
            c_bar_bj=c_bar_bj,
            current_time_ms=current_time_ms,
            current_time_bj=current_time_bj,
            candidate_md_finished_utc_ms=candidate_md_finished_utc_ms,
            audit_enabled=audit_enabled,
            latest_closed_bar_ts=latest_closed_bar_ts_snapshot,
            ticker_map=ticker_map_snapshot,
        )
        finalize_elapsed_ms = _perf_elapsed_ms(finalize_perf_started)
        candidate_symbol_count_after_finalize = int((candidate_payload or {}).get('symbol_count') or 0)
        finalize_removed_symbol_count = max(0, int(candidate_symbol_count_before_finalize or 0) - int(candidate_symbol_count_after_finalize or 0))
        finalize_removed_ratio_pct = round((float(finalize_removed_symbol_count) / float(candidate_symbol_count_before_finalize) * 100.0), 2) if int(candidate_symbol_count_before_finalize or 0) > 0 else None
        if payload is candidate_md_res.get('data'):
            payload = candidate_payload
    finalize_cache_stats = dict((candidate_payload or {}).get('finalize_shared_symbol_bars_cache') or {}) if candidate_payload else None
    finalize_summary = deepcopy((candidate_payload or {}).get('finalize_summary') or {}) if candidate_payload else None
    finalize_verify_failed_ratio_pct = round((float((finalize_summary or {}).get('verify_failed_count') or 0) / float(candidate_symbol_count_before_finalize) * 100.0), 2) if int(candidate_symbol_count_before_finalize or 0) > 0 else None
    finalize_delayed_ratio_pct = round((float((finalize_summary or {}).get('delayed_finalize_count') or 0) / float(candidate_symbol_count_before_finalize) * 100.0), 2) if int(candidate_symbol_count_before_finalize or 0) > 0 else None
    finalize_kept_ratio_pct = round((float(candidate_symbol_count_after_finalize or 0) / float(candidate_symbol_count_before_finalize) * 100.0), 2) if int(candidate_symbol_count_before_finalize or 0) > 0 else None
    finalize_unchanged_count = max(0, int(candidate_symbol_count_after_finalize or 0) - int((finalize_summary or {}).get('delayed_finalize_count') or 0))
    finalize_unchanged_ratio_pct = round((float(finalize_unchanged_count) / float(candidate_symbol_count_before_finalize) * 100.0), 2) if int(candidate_symbol_count_before_finalize or 0) > 0 else None
    finalize_affected_ratio_pct = round((float(((finalize_summary or {}).get('verify_failed_count') or 0) + ((finalize_summary or {}).get('delayed_finalize_count') or 0)) / float(candidate_symbol_count_before_finalize) * 100.0), 2) if int(candidate_symbol_count_before_finalize or 0) > 0 else None
    if audit_enabled and finalize_summary is not None:
        write_event(account, 'c_bar_finalize_summary', {
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'c_bar_ts': c_bar_ts,
            'c_bar_bj': c_bar_bj,
            'finalize_elapsed_ms': finalize_elapsed_ms,
            'candidate_symbol_count_before_finalize': candidate_symbol_count_before_finalize,
            'candidate_symbol_count_after_finalize': candidate_symbol_count_after_finalize,
            'finalize_removed_symbol_count': finalize_removed_symbol_count,
            'finalize_removed_ratio_pct': finalize_removed_ratio_pct,
            'finalize_verify_failed_ratio_pct': finalize_verify_failed_ratio_pct,
            'finalize_delayed_ratio_pct': finalize_delayed_ratio_pct,
            'finalize_kept_ratio_pct': finalize_kept_ratio_pct,
            'finalize_unchanged_count': finalize_unchanged_count,
            'finalize_unchanged_ratio_pct': finalize_unchanged_ratio_pct,
            'finalize_affected_ratio_pct': finalize_affected_ratio_pct,
            'finalize_timeout_not_finalized_count': (finalize_summary or {}).get('timeout_not_finalized_count'),
            'finalize_timeout_not_finalized_symbols': (finalize_summary or {}).get('timeout_not_finalized_symbols'),
            'finalize_passed_count': (finalize_summary or {}).get('passed_count'),
            'finalize_passed_symbols': (finalize_summary or {}).get('passed_symbols'),
            'finalize_rounds': (finalize_summary or {}).get('finalize_rounds'),
            'finalize_probe_interval_secs': (finalize_summary or {}).get('finalize_probe_interval_secs'),
            'finalize_deadline_utc_ms': (finalize_summary or {}).get('finalize_deadline_utc_ms'),
            'finalize_deadline_bj': (finalize_summary or {}).get('finalize_deadline_bj'),
            'deadline_hit': (finalize_summary or {}).get('deadline_hit'),
            'all_passed': (finalize_summary or {}).get('all_passed'),
            'all_passed_utc_ms': (finalize_summary or {}).get('all_passed_utc_ms'),
            'all_passed_bj': (finalize_summary or {}).get('all_passed_bj'),
            'all_passed_elapsed_ms': (finalize_summary or {}).get('all_passed_elapsed_ms'),
            **finalize_summary,
        })

    timing_fields = {
        'loop_started_utc_ms': loop_started_utc_ms,
        'loop_started_bj': loop_started_bj,
        'scheduled_signal_check_utc': scheduled_signal_check_utc,
        'scheduled_signal_check_bj': scheduled_signal_check_bj,
        'market_snapshot_fetched_utc_ms': market_snapshot_fetched_utc_ms,
        'market_snapshot_fetched_bj': market_snapshot_fetched_bj,
        'shared_symbol_bars_cache_enabled': True,
        'history_window_mins': history_window_mins,
        'candidate_contract_cache_hits': (candidate_cache_stats or {}).get('contract_hits'),
        'candidate_contract_cache_misses': (candidate_cache_stats or {}).get('contract_misses'),
        'candidate_index_cache_hits': (candidate_cache_stats or {}).get('index_hits'),
        'candidate_index_cache_misses': (candidate_cache_stats or {}).get('index_misses'),
        'candidate_contract_cache_miss_symbols': (candidate_cache_stats or {}).get('contract_miss_symbols'),
        'candidate_index_cache_miss_symbols': (candidate_cache_stats or {}).get('index_miss_symbols'),
        'extra_contract_cache_hits': (extra_cache_stats or {}).get('contract_hits'),
        'extra_contract_cache_misses': (extra_cache_stats or {}).get('contract_misses'),
        'extra_index_cache_hits': (extra_cache_stats or {}).get('index_hits'),
        'extra_index_cache_misses': (extra_cache_stats or {}).get('index_misses'),
        'extra_contract_cache_miss_symbols': (extra_cache_stats or {}).get('contract_miss_symbols'),
        'extra_index_cache_miss_symbols': (extra_cache_stats or {}).get('index_miss_symbols'),
        'finalize_contract_cache_hits': (finalize_cache_stats or {}).get('contract_hits'),
        'finalize_contract_cache_misses': (finalize_cache_stats or {}).get('contract_misses'),
        'finalize_index_cache_hits': (finalize_cache_stats or {}).get('index_hits'),
        'finalize_index_cache_misses': (finalize_cache_stats or {}).get('index_misses'),
        'finalize_contract_cache_miss_symbols': (finalize_cache_stats or {}).get('contract_miss_symbols'),
        'finalize_index_cache_miss_symbols': (finalize_cache_stats or {}).get('index_miss_symbols'),
        'finalize_processed_symbols': (finalize_summary or {}).get('processed_symbols'),
        'finalize_verify_failed_count': (finalize_summary or {}).get('verify_failed_count'),
        'finalize_delayed_finalize_count': (finalize_summary or {}).get('delayed_finalize_count'),
        'finalize_verify_failed_symbols': (finalize_summary or {}).get('verify_failed_symbols'),
        'finalize_delayed_symbols': (finalize_summary or {}).get('delayed_symbols'),
        'finalize_passed_count': (finalize_summary or {}).get('passed_count'),
        'finalize_passed_symbols': (finalize_summary or {}).get('passed_symbols'),
        'finalize_timeout_not_finalized_count': (finalize_summary or {}).get('timeout_not_finalized_count'),
        'finalize_timeout_not_finalized_symbols': (finalize_summary or {}).get('timeout_not_finalized_symbols'),
        'finalize_rounds': (finalize_summary or {}).get('finalize_rounds'),
        'finalize_probe_interval_secs': (finalize_summary or {}).get('finalize_probe_interval_secs'),
        'finalize_deadline_utc_ms': (finalize_summary or {}).get('finalize_deadline_utc_ms'),
        'finalize_deadline_bj': (finalize_summary or {}).get('finalize_deadline_bj'),
        'deadline_hit': (finalize_summary or {}).get('deadline_hit'),
        'all_passed': (finalize_summary or {}).get('all_passed'),
        'all_passed_utc_ms': (finalize_summary or {}).get('all_passed_utc_ms'),
        'all_passed_bj': (finalize_summary or {}).get('all_passed_bj'),
        'all_passed_elapsed_ms': (finalize_summary or {}).get('all_passed_elapsed_ms'),
        'finalize_elapsed_ms': finalize_elapsed_ms,
        'candidate_symbol_count_before_finalize': candidate_symbol_count_before_finalize,
        'candidate_symbol_count_after_finalize': candidate_symbol_count_after_finalize,
        'finalize_removed_symbol_count': finalize_removed_symbol_count,
        'finalize_removed_ratio_pct': finalize_removed_ratio_pct,
        'finalize_verify_failed_ratio_pct': finalize_verify_failed_ratio_pct,
        'finalize_delayed_ratio_pct': finalize_delayed_ratio_pct,
        'finalize_kept_ratio_pct': finalize_kept_ratio_pct,
        'finalize_unchanged_count': finalize_unchanged_count,
        'finalize_unchanged_ratio_pct': finalize_unchanged_ratio_pct,
        'finalize_affected_ratio_pct': finalize_affected_ratio_pct,
        'candidate_md_started_utc_ms': candidate_md_started_utc_ms,
        'candidate_md_started_bj': _fmt_bj_from_ms(candidate_md_started_utc_ms),
        'candidate_md_finished_utc_ms': candidate_md_finished_utc_ms,
        'candidate_md_finished_bj': _fmt_bj_from_ms(candidate_md_finished_utc_ms),
        'extra_md_started_utc_ms': extra_md_started_utc_ms,
        'extra_md_started_bj': _fmt_bj_from_ms(extra_md_started_utc_ms) if extra_md_started_utc_ms is not None else None,
        'extra_md_finished_utc_ms': extra_md_finished_utc_ms,
        'extra_md_finished_bj': _fmt_bj_from_ms(extra_md_finished_utc_ms) if extra_md_finished_utc_ms is not None else None,
        'signal_time_ts': current_time_ms,
        'signal_time_bj': current_time_bj,
        'c_bar_ts': c_bar_ts,
        'c_bar_bj': c_bar_bj,
        'latest_closed_bar_ts': c_bar_ts,
        'latest_closed_bar_bj': c_bar_bj,
    }

    candidate_cross_section = candidate_payload['cross_section'] if candidate_payload else None
    candidate_full_df = dict(candidate_payload['full_df']) if candidate_payload else {}
    extra_full_df = dict(extra_payload['full_df']) if extra_payload else {}
    merged_full_df = dict(candidate_full_df)
    merged_full_df.update(extra_full_df)

    stage3_perf_started = time.perf_counter()
    if audit_enabled:
        if candidate_full_df:
            _write_stage3_enriched_snapshot(account, 'candidate', current_time_ms, current_time_bj, candidate_full_df, timing_fields)
        if extra_full_df:
            _write_stage3_enriched_snapshot(account, 'reconcile', current_time_ms, current_time_bj, extra_full_df, timing_fields)
    stage3_elapsed_ms = _perf_elapsed_ms(stage3_perf_started)

    latest_closes_perf_started = time.perf_counter()
    latest_closes = {
        str(symbol).upper().strip(): float(df.loc[c_bar_ts, 'close'])
        for symbol, df in merged_full_df.items()
        if c_bar_ts in df.index
    }
    latest_closes_elapsed_ms = _perf_elapsed_ms(latest_closes_perf_started)

    loop_gate_perf_started = time.perf_counter()
    loop_gate = prepare_consumer_loop_gate(
        account,
        strategy_cfg,
        live_cfg,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        candidate_symbols=candidate_symbols,
        extra_reconcile_symbols=extra_reconcile_symbols,
        latest_closes=latest_closes,
        exchange_activity_snapshot=exchange_activity_snapshot,
        source='loop',
    )
    loop_gate_elapsed_ms = _perf_elapsed_ms(loop_gate_perf_started)
    scan_gate = loop_gate['scan_gate']
    active_symbols = {
        str(symbol).upper().strip()
        for symbol in (loop_gate.get('active_symbols') or [])
        if str(symbol).strip()
    }
    active_symbols_count = len(active_symbols)
    if not loop_gate.get('ok_to_scan'):
        finalize_consumer_loop_state(
            account,
            mode='scan_blocked',
            current_time_ms=current_time_ms,
            current_time_bj=current_time_bj,
            symbols=list(merged_full_df.keys()),
            audit_enabled=audit_enabled,
            scan_gate=scan_gate,
        )
        _emit_run_once_perf('scan_blocked')
        return

    exchange_activity_snapshot = dict(loop_gate.get('exchange_snapshot') or exchange_activity_snapshot)
    if not candidate_payload:
        finalize_consumer_loop_state(
            account,
            mode='no_candidate_data',
            current_time_ms=current_time_ms,
            current_time_bj=current_time_bj,
            symbols=list(merged_full_df.keys()),
            audit_enabled=audit_enabled,
            candidate_reason=candidate_md_res.get('reason'),
            candidate_errors=candidate_md_res.get('errors'),
            extra_reconcile_symbols_count=len(extra_reconcile_symbols),
        )
        _emit_run_once_perf('no_candidate_data')
        return

    cross_section = candidate_cross_section
    full_df = candidate_full_df
    strategy = WashoutSnapbackStrategy(strategy_cfg)
    _hydrate_strategy_cooldowns(strategy, account, current_time_ms)
    pre_signal_cooldown_map = dict(getattr(strategy, 'cooldown_until', {}) or {})

    stage4_perf_started = time.perf_counter()
    if audit_enabled:
        for stage_symbol, row in cross_section.iterrows():
            symbol_key = str(stage_symbol).upper().strip()
            _write_stage_record(account, 'stage4_input_snapshot', {
                'bar_ts': current_time_ms,
                'bar_bj': current_time_bj,
                'symbol': symbol_key,
                'close': _series_value(row, 'close'),
                'quote_asset_volume': _series_value(row, 'quote_asset_volume'),
                'chg_24h': _series_value(row, 'chg_24h'),
                'vol_24h': _series_value(row, 'vol_24h'),
                'high_idx': _series_value(row, 'high_idx'),
                'low_idx': _series_value(row, 'low_idx'),
                'close_idx': _series_value(row, 'close_idx'),
                'active_symbols_contains': bool(symbol_key in active_symbols),
                'input_pass_to_logic': True,
                **timing_fields,
            })
    stage4_elapsed_ms = _perf_elapsed_ms(stage4_perf_started)

    signal_eval_started_utc_ms = _now_utc_ms()
    signal_eval_perf_started = time.perf_counter()
    signal = strategy.on_kline_close(c_bar_ts, cross_section, active_symbols, full_df)
    # 只持久化进入本轮前已经存在于 state 的 cooldown。
    # strategy.on_kline_close() 在“刚选出 signal”时会先写内部 cooldown，
    # 若这里立即 sync 回 state，trade_consumer.consume_signal() 会在同一轮
    # 因 cooldown_active 直接跳过这条刚产生的 signal。
    strategy.cooldown_until = pre_signal_cooldown_map
    _persist_strategy_cooldowns(strategy, account, current_time_ms)
    signal_eval_elapsed_ms = _perf_elapsed_ms(signal_eval_perf_started)
    signal_eval_finished_utc_ms = _now_utc_ms()
    signal_present = bool(signal)
    signal_symbol = str(signal['symbol']).upper().strip() if signal else None
    signal_digest_preview = consumer_signal_digest(signal) if signal else None

    stage5_perf_started = time.perf_counter()
    stage5_rows = _build_stage5_structure_rows(
        c_bar_ts,
        current_time_ms,
        current_time_bj,
        cross_section,
        active_symbols,
        full_df,
        strategy_cfg,
        logic_selected_symbol=signal_symbol,
        signal_digest=signal_digest_preview,
    )
    if audit_enabled:
        for stage5_row in stage5_rows:
            _write_stage_record(account, 'stage5_structure_audit', {
                **stage5_row,
                **timing_fields,
                'signal_eval_started_utc_ms': signal_eval_started_utc_ms,
                'signal_eval_started_bj': _fmt_bj_from_ms(signal_eval_started_utc_ms),
                'signal_eval_finished_utc_ms': signal_eval_finished_utc_ms,
                'signal_eval_finished_bj': _fmt_bj_from_ms(signal_eval_finished_utc_ms),
            })
    stage5_elapsed_ms = _perf_elapsed_ms(stage5_perf_started)

    if not signal:
        finalize_consumer_loop_state(
            account,
            mode='signal_none',
            current_time_ms=current_time_ms,
            current_time_bj=current_time_bj,
            symbols=list(merged_full_df.keys()),
            audit_enabled=audit_enabled,
            candidate_payload=candidate_payload,
            extra_reconcile_symbols_count=len(extra_reconcile_symbols),
            timing_fields=timing_fields,
            signal_eval_started_utc_ms=signal_eval_started_utc_ms,
            signal_eval_finished_utc_ms=signal_eval_finished_utc_ms,
        )
        _emit_run_once_perf('signal_none')
        return

    live_signal_projection_perf_started = time.perf_counter()
    live_signal_projection_res = append_live_signal_projection(
        account,
        live_cfg,
        signal=signal,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        c_bar_ts=c_bar_ts,
        c_bar_bj=c_bar_bj,
        source='loop',
        timing_fields=timing_fields,
        signal_eval_started_utc_ms=signal_eval_started_utc_ms,
        signal_eval_finished_utc_ms=signal_eval_finished_utc_ms,
    )
    live_signal_projection_elapsed_ms = _perf_elapsed_ms(live_signal_projection_perf_started)
    if audit_enabled and not live_signal_projection_res.get('ok'):
        write_event(account, 'live_signal_projection_write_failed', {
            'bar_ts': current_time_ms,
            'bar_bj': current_time_bj,
            'symbol': str(signal.get('symbol') or '').upper().strip(),
            'reason': live_signal_projection_res.get('reason'),
            'projection_path': live_signal_projection_res.get('path'),
        })

    consume_signal_perf_started = time.perf_counter()
    consume_signal(
        account,
        strategy_cfg,
        live_cfg,
        signal=signal,
        current_time_ms=current_time_ms,
        current_time_bj=current_time_bj,
        c_bar_ts=c_bar_ts,
        c_bar_bj=c_bar_bj,
        source='loop',
        exchange_snapshot=exchange_activity_snapshot,
        timing_fields=timing_fields,
        signal_eval_started_utc_ms=signal_eval_started_utc_ms,
        signal_eval_finished_utc_ms=signal_eval_finished_utc_ms,
    )
    consume_signal_elapsed_ms = _perf_elapsed_ms(consume_signal_perf_started)
    _emit_run_once_perf('signal_consumed')
    return

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

    live_cfg['_projection_run_id'] = _build_live_projection_run_id(account)
    live_cfg['_projection_output_dir'] = 'output/live_projection'
    live_cfg['_projection_schema_version'] = _live_projection_schema_version()
    live_cfg['_projection_strategy_name'] = 'snapback'

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
        'projection_run_id': live_cfg.get('_projection_run_id'),
        'projection_output_dir': live_cfg.get('_projection_output_dir'),
        'projection_schema_version': live_cfg.get('_projection_schema_version'),
        'projection_strategy_name': live_cfg.get('_projection_strategy_name'),
        'started_bj': _now_bj_str(),
    })
    bootstrap_res = bootstrap_consumer_gate(
        account,
        strategy_cfg,
        live_cfg,
        candidate_symbols=list_candidate_symbols(account, exclude_symbols=live_cfg.get('exclude_symbols') or []),
        source='startup',
    )
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

    next_signal_check_epoch: float | None = None
    while True:
        try:
            next_signal_check_epoch = _sleep_until_next_signal_check(next_signal_check_epoch)
            mark_loop_heartbeat(account, runner_pid=os.getpid())
            write_runner_heartbeat(account, {
                'heartbeat_bj': _now_bj_str(),
                'scheduled_signal_check_utc': datetime.fromtimestamp(next_signal_check_epoch, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                'scheduled_signal_check_bj': datetime.fromtimestamp(next_signal_check_epoch, tz=timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S'),
            })
            _run_once(strategy_cfg, live_cfg, scheduled_signal_check_epoch=next_signal_check_epoch)
        except KeyboardInterrupt:
            raise
        except Exception as e:
            write_event(account, 'runner_error', {'reason': str(e), 'error_bj': _now_bj_str()})
            if bool(live_cfg.get('notify_enabled', False)) and bool(live_cfg.get('notify_on_order_error', True)):
                _notify(True, f'[Snapback-Live] runner error | {e}')
        finally:
            if next_signal_check_epoch is not None:
                next_signal_check_epoch += 60.0


if __name__ == '__main__':
    main()
