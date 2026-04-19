from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

from core.live.audit_log import write_event
from core.live.market_data import (
    build_live_inputs,
    build_live_inputs_full_market,
    build_market_snapshot,
    merge_shared_symbol_bars_cache_stats,
    new_shared_symbol_bars_cache_stats,
)
from core.live.market_data_hub_store import append_daily_snapshot, read_current_pickle, read_current_snapshot, write_current_pickle, write_current_snapshot

BJ = timezone(timedelta(hours=8))
_CANDIDATE_FINALIZE_CB_DEADLINE_SECS = 50
_CANDIDATE_FINALIZE_PROBE_INTERVAL_SECS = 2


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")

_HUB_SNAPSHOT_MAX_AGE_SECS = 75


def _snapshot_age_secs(snapshot: dict[str, Any] | None, *, now_ms: int | None = None) -> float | None:
    if not isinstance(snapshot, dict):
        return None
    fetched_utc_ms = snapshot.get('market_snapshot_fetched_utc_ms')
    if fetched_utc_ms is None:
        fetched_utc_ms = snapshot.get('published_utc_ms')
    if fetched_utc_ms is None:
        return None
    try:
        current_ms = int(now_ms) if now_ms is not None else int(time.time() * 1000)
        return max(0.0, (current_ms - int(fetched_utc_ms)) / 1000.0)
    except Exception:
        return None


def _raise_if_snapshot_stale(account: str, name: str, snapshot: dict[str, Any] | None, *, max_age_secs: int | None) -> dict[str, Any]:
    if not isinstance(snapshot, dict):
        raise RuntimeError(f'hub snapshot missing: account={account} name={name}')
    if max_age_secs is None:
        return snapshot
    age_secs = _snapshot_age_secs(snapshot)
    if age_secs is None:
        raise RuntimeError(f'hub snapshot missing_age: account={account} name={name}')
    if float(age_secs) > float(max_age_secs):
        raise RuntimeError(
            f'hub snapshot stale: account={account} name={name} age_secs={age_secs:.2f} max_age_secs={int(max_age_secs)}'
        )
    return snapshot


def load_market_snapshot_from_hub(account: str, *, max_age_secs: int = _HUB_SNAPSHOT_MAX_AGE_SECS) -> dict[str, Any]:
    meta = read_current_snapshot(account, 'market_snapshot')
    _raise_if_snapshot_stale(account, 'market_snapshot', meta, max_age_secs=max_age_secs)
    payload = read_current_pickle(account, 'market_snapshot')
    if not isinstance(payload, dict):
        raise RuntimeError(f'hub payload missing: account={account} name=market_snapshot')
    return payload


def load_finalized_candidate_inputs_from_hub(account: str, *, max_age_secs: int = _HUB_SNAPSHOT_MAX_AGE_SECS) -> dict[str, Any]:
    meta = read_current_snapshot(account, 'finalized_candidate_inputs')
    _raise_if_snapshot_stale(account, 'finalized_candidate_inputs', meta, max_age_secs=max_age_secs)
    payload = read_current_pickle(account, 'finalized_candidate_inputs')
    if not isinstance(payload, dict):
        raise RuntimeError(f'hub payload missing: account={account} name=finalized_candidate_inputs')
    return payload


def load_candidate_inputs_from_hub(account: str, *, max_age_secs: int = _HUB_SNAPSHOT_MAX_AGE_SECS) -> dict[str, Any]:
    meta = read_current_snapshot(account, 'candidate_inputs')
    _raise_if_snapshot_stale(account, 'candidate_inputs', meta, max_age_secs=max_age_secs)
    payload = read_current_pickle(account, 'candidate_inputs')
    if not isinstance(payload, dict):
        raise RuntimeError(f'hub payload missing: account={account} name=candidate_inputs')
    return payload


def build_market_snapshot_via_hub(account: str, *, audit_enabled: bool) -> dict[str, Any]:
    snapshot = build_market_snapshot(account)
    published_utc_ms = int(time.time() * 1000)
    payload = {
        'schema_version': 1,
        'account': str(account).strip(),
        'snapshot_name': 'market_snapshot',
        'latest_closed_bar_ts': int(snapshot['latest_closed_bar_ts']),
        'latest_closed_bar_bj': snapshot['latest_closed_bar_bj'],
        'signal_time_ts': int(snapshot['signal_time_ts']),
        'signal_time_bj': snapshot['signal_time_bj'],
        'market_snapshot_fetched_utc_ms': int(snapshot['market_snapshot_fetched_utc_ms']),
        'market_snapshot_fetched_bj': snapshot['market_snapshot_fetched_bj'],
        'published_utc_ms': published_utc_ms,
        'published_bj': _fmt_bj_from_ms(published_utc_ms),
        'market_total_24h_vol_api': float(snapshot.get('market_total_24h_vol_api') or 0.0),
        'market_total_24h_symbol_count_api': int(snapshot.get('market_total_24h_symbol_count_api') or 0),
        'market_total_24h_vol_1m_rollsum': float(snapshot.get('market_total_24h_vol_1m_rollsum') or 0.0),
        'market_total_24h_symbol_count_1m_rollsum': int(snapshot.get('market_total_24h_symbol_count_1m_rollsum') or 0),
        'market_total_24h_symbol_count': int(snapshot.get('market_total_24h_symbol_count_1m_rollsum') or 0),
        'market_total_24h_vol_source': str(snapshot.get('market_total_24h_vol_source') or ''),
        'market_total_24h_vol_1m_rollsum_status': str(snapshot.get('market_total_24h_vol_status') or ''),
        'hub_owned_1m_rollsum_state_updated_utc_ms': snapshot.get('hub_owned_1m_rollsum_state_updated_utc_ms'),
        'hub_owned_1m_rollsum_state_updated_bj': snapshot.get('hub_owned_1m_rollsum_state_updated_bj'),
        'missing_symbol_count_1m_rollsum': int(snapshot.get('missing_symbol_count_1m_rollsum') or 0),
        'partial_symbol_count_1m_rollsum': int(snapshot.get('partial_symbol_count_1m_rollsum') or 0),
    }
    write_current_snapshot(account, 'market_snapshot', payload)
    write_current_pickle(account, 'market_snapshot', snapshot)
    if audit_enabled:
        append_daily_snapshot(account, 'market_snapshot', payload, day_bj=str(snapshot.get('signal_time_bj') or '')[:10])
    return snapshot


def build_live_inputs_via_hub(
    account: str,
    symbols: list[str],
    history_window_mins: int,
    strategy_cfg: dict[str, Any] | None = None,
    *,
    audit_label: str = 'candidate',
    latest_closed_bar_ts: int | None = None,
    ticker_map: dict[str, dict[str, Any]] | None = None,
    audit_enabled: bool = False,
    use_full_market_inputs: bool = False,
) -> dict[str, Any]:
    if use_full_market_inputs:
        res = build_live_inputs_full_market(
            account,
            symbols,
            history_window_mins,
            audit_label=audit_label,
            latest_closed_bar_ts=latest_closed_bar_ts,
            ticker_map=ticker_map,
        )
    else:
        res = build_live_inputs(
            account,
            symbols,
            history_window_mins,
            strategy_cfg,
            audit_label=audit_label,
            latest_closed_bar_ts=latest_closed_bar_ts,
            ticker_map=ticker_map,
        )
    published_utc_ms = int(time.time() * 1000)
    payload = {
        'schema_version': 1,
        'account': str(account).strip(),
        'snapshot_name': f'{str(audit_label).strip()}_inputs',
        'audit_label': str(audit_label).strip(),
        'ok': bool(res.get('ok')),
        'reason': res.get('reason'),
        'published_utc_ms': published_utc_ms,
        'published_bj': _fmt_bj_from_ms(published_utc_ms),
        'latest_closed_bar_ts': int(((res.get('data') or {}).get('latest_closed_bar_ts') or 0)) if res.get('data') else int(latest_closed_bar_ts or 0),
        'latest_closed_bar_bj': ((res.get('data') or {}).get('latest_closed_bar_bj')) if res.get('data') else _fmt_bj_from_ms(latest_closed_bar_ts),
        'signal_time_ts': int(((res.get('data') or {}).get('signal_time_ts') or 0)) if res.get('data') else None,
        'signal_time_bj': ((res.get('data') or {}).get('signal_time_bj')) if res.get('data') else None,
        'symbol_count': int(((res.get('data') or {}).get('symbol_count') or 0)),
        'bars_loaded_min': ((res.get('data') or {}).get('bars_loaded_min')),
        'bars_loaded_max': ((res.get('data') or {}).get('bars_loaded_max')),
        'stale_symbol_count': int(((res.get('data') or {}).get('stale_symbol_count') or 0)),
        'shared_symbol_bars_cache': (res.get('data') or {}).get('shared_symbol_bars_cache'),
        'errors': res.get('errors'),
    }
    write_current_snapshot(account, f'{audit_label}_inputs', payload)
    if bool(res.get('ok')) and isinstance(res.get('data'), dict):
        write_current_pickle(account, f'{audit_label}_inputs', res.get('data'))
    if audit_enabled:
        append_daily_snapshot(account, f'{audit_label}_inputs', payload, day_bj=str(payload.get('signal_time_bj') or '')[:10])
    return res


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
    finalized_payload = {
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
    return finalized_payload


def finalize_candidate_payload_via_hub(
    account: str,
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
        finalized_payload = _build_finalized_candidate_payload(candidate_payload, candidate_cross_section, candidate_full_df, finalize_cache_stats, finalize_summary)
        _write_finalize_snapshots(account, finalized_payload)
        return finalized_payload

    next_probe_utc_ms = _next_finalize_probe_utc_ms(None, candidate_md_finished_utc_ms)
    while pending_symbols and next_probe_utc_ms is not None:
        if not _sleep_until_utc_ms(next_probe_utc_ms, deadline_utc_ms=finalize_deadline_utc_ms):
            break
        round_probe_utc_ms = int(time.time() * 1000)
        round_probe_bj = _fmt_bj_from_ms(round_probe_utc_ms) or current_time_bj
        finalize_summary['finalize_rounds'] = int(finalize_summary.get('finalize_rounds', 0)) + 1

        round_pending_symbols = sorted(pending_symbols)
        for symbol in round_pending_symbols:
            if int(time.time() * 1000) > int(finalize_deadline_utc_ms):
                finalize_summary['skipped_due_deadline'] = True
                break
            refresh_res = build_live_inputs_full_market(
                account,
                [symbol],
                history_window_mins,
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

    finalized_payload = _build_finalized_candidate_payload(
        candidate_payload,
        candidate_cross_section,
        candidate_full_df,
        finalize_cache_stats,
        finalize_summary,
    )
    _write_finalize_snapshots(account, finalized_payload)
    return finalized_payload


def _write_finalize_snapshots(account: str, finalized_payload: dict[str, Any]) -> None:
    finalize_summary = dict(finalized_payload.get('finalize_summary') or {})
    published_utc_ms = int(time.time() * 1000)
    payload = {
        'schema_version': 1,
        'account': str(account).strip(),
        'snapshot_name': 'finalized_candidate_inputs',
        'published_utc_ms': published_utc_ms,
        'published_bj': _fmt_bj_from_ms(published_utc_ms),
        'latest_closed_bar_ts': int(finalized_payload.get('latest_closed_bar_ts') or 0),
        'latest_closed_bar_bj': finalized_payload.get('latest_closed_bar_bj'),
        'signal_time_ts': int(finalized_payload.get('signal_time_ts') or 0),
        'signal_time_bj': finalized_payload.get('signal_time_bj'),
        'symbol_count': int(finalized_payload.get('symbol_count') or 0),
        'bars_loaded_min': finalized_payload.get('bars_loaded_min'),
        'bars_loaded_max': finalized_payload.get('bars_loaded_max'),
        'finalize_summary': finalize_summary,
        'finalize_shared_symbol_bars_cache': finalized_payload.get('finalize_shared_symbol_bars_cache'),
    }
    write_current_snapshot(account, 'finalized_candidate_inputs', payload)
    write_current_pickle(account, 'finalized_candidate_inputs', finalized_payload)
    append_daily_snapshot(account, 'finalized_candidate_inputs', payload, day_bj=str(payload.get('signal_time_bj') or '')[:10])

