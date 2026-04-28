from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from core.live.audit_log import append_stage_record, get_stage_audit_dir
from core.live.binance_client import get_client, get_index_price_klines
from core.live.rate_limit_guard import sleep_if_binance_rest_banned

PROJECT_ROOT = Path(__file__).resolve().parents[2]

_BJ = timezone(timedelta(hours=8))
_INTERVAL = '1m'
_STAGE_AUDIT_DIRNAME = 'stage_audit'


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(_BJ).strftime('%Y-%m-%d %H:%M:%S')


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def _to_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _exchange_server_time_ms(account: str) -> int:
    sleep_if_binance_rest_banned(source='market_data.futures_time')
    client = get_client(account)
    return int(client.futures_time()['serverTime'])


def _last_closed_bar_open_time_ms(account: str) -> int:
    server_ms = _exchange_server_time_ms(account)
    return (server_ms // 60000) * 60000 - 60000


def _signal_time_ms_from_latest_closed_bar(latest_closed_bar_ts: int) -> int:
    return int(latest_closed_bar_ts) + 60000


_SHARED_MARKET_DIRNAME = 'shared_market'
_SHARED_TICKER_TTL_SECS = 55
_SHARED_EXCHANGE_INFO_TTL_SECS = 300
_SHARED_LATEST_CLOSED_BAR_TTL_SECS = 2
_SHARED_SYMBOL_BARS_TTL_SECS = 2
_SHARED_SYMBOL_BARS_INCREMENTAL_MAX_LIMIT = 1000
_SHARED_SYMBOL_BARS_INCREMENTAL_OVERLAP_BARS = 3
_KLINES_1M_DIR = Path(PROJECT_ROOT) / 'data/klines_1m'
_MARKET_24H_ROLLSUM_WINDOW_BARS = 1440
_HUB_OWNED_1M_ROLLSUM_STATE_FILENAME = 'hub_owned_1m_rollsum_state.json'
_SYMBOL_24H_QUOTE_VOLUME_CACHE: dict[str, dict[str, Any]] = {}


def _shared_market_dir() -> Path:
    path = get_stage_audit_dir().parent / _SHARED_MARKET_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    unique_suffix = f".{os.getpid()}.{time.time_ns()}.tmp"
    tmp_path = path.with_name(path.name + unique_suffix)
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False) + '\n', encoding='utf-8')
    os.replace(tmp_path, path)

def _read_json_file(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return None


def _cache_is_fresh(snapshot: dict[str, Any] | None, ttl_secs: int) -> bool:
    if not isinstance(snapshot, dict):
        return False
    fetched_utc_ms = snapshot.get('fetched_utc_ms')
    if fetched_utc_ms is None:
        return False
    try:
        age_ms = int(time.time() * 1000) - int(fetched_utc_ms)
    except Exception:
        return False
    return age_ms >= 0 and age_ms <= int(ttl_secs * 1000)


def _exchange_info_snapshot_path() -> Path:
    return _shared_market_dir() / 'futures_exchange_info.shared.json'


def _ticker_snapshot_path() -> Path:
    return _shared_market_dir() / 'futures_ticker.shared.json'


def _hub_owned_1m_rollsum_state_path() -> Path:
    return _shared_market_dir() / _HUB_OWNED_1M_ROLLSUM_STATE_FILENAME


def _latest_closed_bar_snapshot_path() -> Path:
    return _shared_market_dir() / 'latest_closed_bar.shared.json'


def _symbol_bars_dir() -> Path:
    path = _shared_market_dir() / 'bars'
    path.mkdir(parents=True, exist_ok=True)
    return path


def _safe_symbol_key(symbol: str) -> str:
    return str(symbol).upper().strip()


def _symbol_bars_snapshot_path(symbol: str, limit: int, kind: str) -> Path:
    symbol_key = _safe_symbol_key(symbol)
    return _symbol_bars_dir() / f'{symbol_key}.{kind}.{int(limit)}.shared.json'


def _new_shared_symbol_bars_cache_stats() -> dict[str, Any]:
    return {
        'contract_hits': 0,
        'contract_misses': 0,
        'index_hits': 0,
        'index_misses': 0,
        'contract_miss_symbols': [],
        'index_miss_symbols': [],
        'contract_hit_coverage_misses': 0,
        'index_hit_coverage_misses': 0,
        'contract_hit_coverage_miss_symbols': [],
        'index_hit_coverage_miss_symbols': [],
        'last_events': [],
    }



def _record_shared_symbol_bars_cache_diag(
    stats: dict[str, Any] | None,
    *,
    kind: str,
    symbol: str,
    cache_hit: bool,
    required_latest_closed_bar_ts: int | None,
    required_window_bars: int | None,
    cached_max_open_time_ms: int | None,
    coverage_ok: bool | None,
) -> None:
    if stats is None:
        return
    symbol_key = _safe_symbol_key(symbol)
    event = {
        'symbol': symbol_key,
        'kind': kind,
        'cache_hit': bool(cache_hit),
        'required_latest_closed_bar_ts': int(required_latest_closed_bar_ts) if required_latest_closed_bar_ts is not None else None,
        'required_latest_closed_bar_bj': _fmt_bj_from_ms(int(required_latest_closed_bar_ts)) if required_latest_closed_bar_ts is not None else None,
        'required_window_bars': int(required_window_bars) if required_window_bars is not None else None,
        'cached_max_open_time_ms': int(cached_max_open_time_ms) if cached_max_open_time_ms is not None else None,
        'cached_max_open_time_bj': _fmt_bj_from_ms(int(cached_max_open_time_ms)) if cached_max_open_time_ms is not None else None,
        'coverage_ok': coverage_ok,
    }
    last_events = stats.setdefault('last_events', [])
    last_events.append(event)
    if len(last_events) > 20:
        del last_events[:-20]

    if cache_hit and coverage_ok is False:
        if kind == 'contract':
            stats['contract_hit_coverage_misses'] = int(stats.get('contract_hit_coverage_misses', 0)) + 1
            miss_symbols = stats.setdefault('contract_hit_coverage_miss_symbols', [])
            if symbol_key not in miss_symbols:
                miss_symbols.append(symbol_key)
        elif kind == 'index':
            stats['index_hit_coverage_misses'] = int(stats.get('index_hit_coverage_misses', 0)) + 1
            miss_symbols = stats.setdefault('index_hit_coverage_miss_symbols', [])
            if symbol_key not in miss_symbols:
                miss_symbols.append(symbol_key)

def _record_shared_symbol_bars_cache_event(stats: dict[str, Any] | None, *, kind: str, symbol: str, cache_hit: bool) -> None:
    if stats is None:
        return
    symbol_key = _safe_symbol_key(symbol)
    if kind == 'contract':
        if cache_hit:
            stats['contract_hits'] = int(stats.get('contract_hits', 0)) + 1
        else:
            stats['contract_misses'] = int(stats.get('contract_misses', 0)) + 1
            miss_symbols = stats.setdefault('contract_miss_symbols', [])
            if symbol_key not in miss_symbols:
                miss_symbols.append(symbol_key)
        return
    if kind == 'index':
        if cache_hit:
            stats['index_hits'] = int(stats.get('index_hits', 0)) + 1
        else:
            stats['index_misses'] = int(stats.get('index_misses', 0)) + 1
            miss_symbols = stats.setdefault('index_miss_symbols', [])
            if symbol_key not in miss_symbols:
                miss_symbols.append(symbol_key)
        return
    raise ValueError(f'unsupported bars kind: {kind}')


def new_shared_symbol_bars_cache_stats() -> dict[str, Any]:
    return _new_shared_symbol_bars_cache_stats()


def merge_shared_symbol_bars_cache_stats(target: dict[str, Any] | None, incoming: dict[str, Any] | None) -> dict[str, Any]:
    out = dict(target or _new_shared_symbol_bars_cache_stats())
    src = dict(incoming or {})
    out['contract_hits'] = int(out.get('contract_hits', 0)) + int(src.get('contract_hits', 0) or 0)
    out['contract_misses'] = int(out.get('contract_misses', 0)) + int(src.get('contract_misses', 0) or 0)
    out['index_hits'] = int(out.get('index_hits', 0)) + int(src.get('index_hits', 0) or 0)
    out['index_misses'] = int(out.get('index_misses', 0)) + int(src.get('index_misses', 0) or 0)
    out['contract_hit_coverage_misses'] = int(out.get('contract_hit_coverage_misses', 0)) + int(src.get('contract_hit_coverage_misses', 0) or 0)
    out['index_hit_coverage_misses'] = int(out.get('index_hit_coverage_misses', 0)) + int(src.get('index_hit_coverage_misses', 0) or 0)

    contract_miss_symbols = [str(x).upper().strip() for x in (out.get('contract_miss_symbols') or []) if str(x).strip()]
    for symbol in (src.get('contract_miss_symbols') or []):
        symbol_key = _safe_symbol_key(symbol)
        if symbol_key not in contract_miss_symbols:
            contract_miss_symbols.append(symbol_key)
    out['contract_miss_symbols'] = contract_miss_symbols

    index_miss_symbols = [str(x).upper().strip() for x in (out.get('index_miss_symbols') or []) if str(x).strip()]
    for symbol in (src.get('index_miss_symbols') or []):
        symbol_key = _safe_symbol_key(symbol)
        if symbol_key not in index_miss_symbols:
            index_miss_symbols.append(symbol_key)
    out['index_miss_symbols'] = index_miss_symbols

    contract_hit_coverage_miss_symbols = [str(x).upper().strip() for x in (out.get('contract_hit_coverage_miss_symbols') or []) if str(x).strip()]
    for symbol in (src.get('contract_hit_coverage_miss_symbols') or []):
        symbol_key = _safe_symbol_key(symbol)
        if symbol_key not in contract_hit_coverage_miss_symbols:
            contract_hit_coverage_miss_symbols.append(symbol_key)
    out['contract_hit_coverage_miss_symbols'] = contract_hit_coverage_miss_symbols

    index_hit_coverage_miss_symbols = [str(x).upper().strip() for x in (out.get('index_hit_coverage_miss_symbols') or []) if str(x).strip()]
    for symbol in (src.get('index_hit_coverage_miss_symbols') or []):
        symbol_key = _safe_symbol_key(symbol)
        if symbol_key not in index_hit_coverage_miss_symbols:
            index_hit_coverage_miss_symbols.append(symbol_key)
    out['index_hit_coverage_miss_symbols'] = index_hit_coverage_miss_symbols

    last_events = list(out.get('last_events') or [])
    for event in (src.get('last_events') or []):
        if not isinstance(event, dict):
            continue
        event_copy = dict(event)
        if event_copy not in last_events:
            last_events.append(event_copy)
    if len(last_events) > 20:
        last_events = last_events[-20:]
    out['last_events'] = last_events
    return out



def _cached_rows_max_open_time_ms(snapshot: dict[str, Any] | None) -> int | None:
    if not isinstance(snapshot, dict):
        return None
    rows = snapshot.get('data')
    if not isinstance(rows, list) or not rows:
        return None
    max_open_time_ms = None
    for row in rows:
        try:
            open_time_ms = _to_int(row[0])
        except Exception:
            continue
        if max_open_time_ms is None or open_time_ms > max_open_time_ms:
            max_open_time_ms = open_time_ms
    return max_open_time_ms

def _recent_window_coverage_info(
    open_times: list[int],
    latest_closed_bar_ts: int | None,
    required_window_bars: int | None,
) -> dict[str, Any]:
    latest_ts = _to_int(latest_closed_bar_ts, default=0)
    window_bars = _to_int(required_window_bars, default=0)
    if latest_ts <= 0 or window_bars <= 0:
        return {
            'ok': True,
            'latest_closed_bar_ts': latest_closed_bar_ts,
            'required_window_bars': required_window_bars,
            'latest_present': True,
            'actual_count': int(len(open_times)),
            'missing_count': 0,
            'expected_start_ts': None,
            'expected_start_bj': None,
            'actual_start_ts': int(min(open_times)) if open_times else None,
            'actual_start_bj': _fmt_bj_from_ms(int(min(open_times))) if open_times else None,
            'actual_end_ts': int(max(open_times)) if open_times else None,
            'actual_end_bj': _fmt_bj_from_ms(int(max(open_times))) if open_times else None,
        }
    expected_start_ts = int(latest_ts - (window_bars - 1) * 60000)
    expected = set(range(expected_start_ts, latest_ts + 60000, 60000))
    actual = {
        int(ts)
        for ts in open_times
        if _to_int(ts, default=0) >= expected_start_ts and _to_int(ts, default=0) <= latest_ts
    }
    latest_present = int(latest_ts) in actual
    missing_count = int(len(expected - actual))
    actual_sorted = sorted(actual)
    return {
        'ok': bool(missing_count == 0 and len(actual) == window_bars and latest_present),
        'latest_closed_bar_ts': int(latest_ts),
        'required_window_bars': int(window_bars),
        'latest_present': bool(latest_present),
        'actual_count': int(len(actual)),
        'missing_count': missing_count,
        'expected_start_ts': int(expected_start_ts),
        'expected_start_bj': _fmt_bj_from_ms(int(expected_start_ts)),
        'actual_start_ts': int(actual_sorted[0]) if actual_sorted else None,
        'actual_start_bj': _fmt_bj_from_ms(int(actual_sorted[0])) if actual_sorted else None,
        'actual_end_ts': int(actual_sorted[-1]) if actual_sorted else None,
        'actual_end_bj': _fmt_bj_from_ms(int(actual_sorted[-1])) if actual_sorted else None,
    }


def _cached_rows_cover_latest_closed_bar(
    snapshot: dict[str, Any] | None,
    required_latest_closed_bar_ts: int | None,
    required_window_bars: int | None = None,
) -> bool:
    if required_latest_closed_bar_ts is None:
        return True
    if not isinstance(snapshot, dict):
        return False
    rows = snapshot.get('data')
    if not isinstance(rows, list) or not rows:
        return False
    info = _recent_window_coverage_info(
        [_to_int(row[0], default=0) for row in rows if isinstance(row, (list, tuple)) and len(row) > 0],
        required_latest_closed_bar_ts,
        required_window_bars,
    )
    return bool(info.get('ok'))


def _format_recent_window_reason(kind: str, info: dict[str, Any]) -> str:
    label = str(kind).strip() or 'window'
    actual_count = int(info.get('actual_count') or 0)
    required_window_bars = int(info.get('required_window_bars') or 0)
    missing_count = int(info.get('missing_count') or 0)
    return f'{label}_recent_window_not_continuous({actual_count}/{required_window_bars},missing={missing_count})'


def _frame_recent_window_reason(
    frame: pd.DataFrame,
    *,
    kind: str,
    latest_closed_bar_ts: int,
    required_window_bars: int,
) -> str | None:
    if frame is None or frame.empty:
        return _format_recent_window_reason(
            kind,
            _recent_window_coverage_info([], latest_closed_bar_ts, required_window_bars),
        )
    info = _recent_window_coverage_info(
        [int(x) for x in frame.index.tolist()],
        latest_closed_bar_ts,
        required_window_bars,
    )
    if bool(info.get('ok')):
        return None
    return _format_recent_window_reason(kind, info)


def _is_recent_prefix_continuous(
    open_times: list[int],
    latest_closed_bar_ts: int,
) -> bool:
    clean = sorted({int(ts) for ts in open_times if _to_int(ts, default=0) > 0 and _to_int(ts, default=0) <= int(latest_closed_bar_ts)})
    if not clean:
        return False
    info = _recent_window_coverage_info(
        clean,
        latest_closed_bar_ts,
        len(clean),
    )
    return bool(info.get('ok'))


def _normalize_kline_rows(
    rows: list[list[Any]] | None,
    *,
    latest_closed_bar_ts: int | None,
    keep_limit: int | None,
) -> list[list[Any]]:
    merged: dict[int, list[Any]] = {}
    max_ts = _to_int(latest_closed_bar_ts, default=0)
    for row in list(rows or []):
        if not isinstance(row, (list, tuple)) or len(row) == 0:
            continue
        open_time_ms = _to_int(row[0], default=0)
        if open_time_ms <= 0:
            continue
        if max_ts > 0 and open_time_ms > max_ts:
            continue
        merged[int(open_time_ms)] = list(row)
    ordered = [merged[ts] for ts in sorted(merged.keys())]
    if keep_limit is not None and int(keep_limit) > 0 and len(ordered) > int(keep_limit):
        ordered = ordered[-int(keep_limit):]
    return ordered


def _build_incremental_kline_fetch_plan(
    cached_rows: list[list[Any]] | None,
    *,
    latest_closed_bar_ts: int | None,
    keep_limit: int,
) -> dict[str, Any] | None:
    latest_ts = _to_int(latest_closed_bar_ts, default=0)
    if latest_ts <= 0:
        return None
    normalized_cached_rows = _normalize_kline_rows(
        cached_rows,
        latest_closed_bar_ts=latest_ts,
        keep_limit=keep_limit,
    )
    if not normalized_cached_rows:
        return None
    open_times = [_to_int(row[0], default=0) for row in normalized_cached_rows if isinstance(row, (list, tuple)) and len(row) > 0]
    open_times = sorted({int(ts) for ts in open_times if int(ts) > 0})
    if not open_times:
        return None
    cached_max_open_time_ms = int(open_times[-1])
    if cached_max_open_time_ms >= latest_ts:
        return None
    if not _is_recent_prefix_continuous(open_times, cached_max_open_time_ms):
        return None
    lag_bars = max(0, (latest_ts - cached_max_open_time_ms) // 60000)
    if lag_bars <= 0:
        return None
    overlap_bars = min(_SHARED_SYMBOL_BARS_INCREMENTAL_OVERLAP_BARS, max(1, len(open_times) - 1))
    remote_limit = int(lag_bars + overlap_bars + 2)
    if remote_limit > int(_SHARED_SYMBOL_BARS_INCREMENTAL_MAX_LIMIT):
        return None
    return {
        'start_time': max(0, cached_max_open_time_ms - overlap_bars * 60000),
        'end_time': latest_ts,
        'remote_limit': remote_limit,
        'cached_rows': normalized_cached_rows,
        'cached_max_open_time_ms': cached_max_open_time_ms,
        'lag_bars': int(lag_bars),
    }


def _fetch_remote_symbol_bar_rows(
    account: str,
    symbol: str,
    *,
    kind: str,
    limit: int,
    start_time: int | None = None,
    end_time: int | None = None,
) -> list[list[Any]]:
    if kind == 'contract':
        return _fetch_symbol_klines_remote(
            account,
            symbol,
            limit,
            start_time=start_time,
            end_time=end_time,
        )
    if kind == 'index':
        return _fetch_symbol_index_price_klines_remote(
            account,
            symbol,
            limit,
            start_time=start_time,
            end_time=end_time,
        )
    raise ValueError(f'unsupported bars kind: {kind}')


def _load_or_refresh_symbol_bar_rows(
    account: str,
    symbol: str,
    limit: int,
    *,
    kind: str,
    cache_stats: dict[str, Any] | None = None,
    required_latest_closed_bar_ts: int | None = None,
) -> list[list[Any]]:
    path = _symbol_bars_snapshot_path(symbol, limit, kind)
    cached = _read_json_file(path)
    cached_rows = cached.get('data') if isinstance(cached, dict) else None
    cached_max_open_time_ms = _cached_rows_max_open_time_ms(cached)
    coverage_ok = _cached_rows_cover_latest_closed_bar(cached, required_latest_closed_bar_ts, limit)
    if _cache_is_fresh(cached, _SHARED_SYMBOL_BARS_TTL_SECS) and coverage_ok:
        rows = cached_rows
        if isinstance(rows, list):
            _record_shared_symbol_bars_cache_event(cache_stats, kind=kind, symbol=symbol, cache_hit=True)
            _record_shared_symbol_bars_cache_diag(
                cache_stats,
                kind=kind,
                symbol=symbol,
                cache_hit=True,
                required_latest_closed_bar_ts=required_latest_closed_bar_ts,
                required_window_bars=limit,
                cached_max_open_time_ms=cached_max_open_time_ms,
                coverage_ok=coverage_ok,
            )
            return rows
    if _cache_is_fresh(cached, _SHARED_SYMBOL_BARS_TTL_SECS) and not coverage_ok:
        _record_shared_symbol_bars_cache_diag(
            cache_stats,
            kind=kind,
            symbol=symbol,
            cache_hit=True,
            required_latest_closed_bar_ts=required_latest_closed_bar_ts,
            required_window_bars=limit,
            cached_max_open_time_ms=cached_max_open_time_ms,
            coverage_ok=coverage_ok,
        )
    now_ms = int(time.time() * 1000)
    rows: list[list[Any]]
    incremental_plan = _build_incremental_kline_fetch_plan(
        cached_rows if isinstance(cached_rows, list) else None,
        latest_closed_bar_ts=required_latest_closed_bar_ts,
        keep_limit=int(limit),
    )
    if incremental_plan is not None:
        fetched_rows = _fetch_remote_symbol_bar_rows(
            account,
            symbol,
            kind=kind,
            limit=int(incremental_plan['remote_limit']),
            start_time=int(incremental_plan['start_time']),
            end_time=int(incremental_plan['end_time']),
        )
        rows = _normalize_kline_rows(
            list(incremental_plan['cached_rows']) + list(fetched_rows or []),
            latest_closed_bar_ts=required_latest_closed_bar_ts,
            keep_limit=int(limit),
        )
        payload_preview = {
            'data': rows,
        }
        if required_latest_closed_bar_ts is not None and not _cached_rows_cover_latest_closed_bar(payload_preview, required_latest_closed_bar_ts, limit):
            incremental_plan = None
    if incremental_plan is None:
        remote_limit = int(limit)
        if required_latest_closed_bar_ts is not None:
            # Binance latest-N queries often include the still-open 1m bar. Pull one extra
            # and normalize back to the requested closed-bar window.
            remote_limit = int(limit) + 1
        rows = _fetch_remote_symbol_bar_rows(
            account,
            symbol,
            kind=kind,
            limit=remote_limit,
        )
        rows = _normalize_kline_rows(
            rows,
            latest_closed_bar_ts=required_latest_closed_bar_ts,
            keep_limit=int(limit),
        )
    payload = {
        'fetched_utc_ms': now_ms,
        'fetched_bj': _fmt_bj_from_ms(now_ms),
        'symbol': _safe_symbol_key(symbol),
        'limit': int(limit),
        'kind': kind,
        'data': rows,
    }
    _atomic_write_json(path, payload)
    _record_shared_symbol_bars_cache_event(cache_stats, kind=kind, symbol=symbol, cache_hit=False)
    _record_shared_symbol_bars_cache_diag(
        cache_stats,
        kind=kind,
        symbol=symbol,
        cache_hit=False,
        required_latest_closed_bar_ts=required_latest_closed_bar_ts,
        required_window_bars=limit,
        cached_max_open_time_ms=_cached_rows_max_open_time_ms(payload),
        coverage_ok=_cached_rows_cover_latest_closed_bar(payload, required_latest_closed_bar_ts, limit),
    )
    return rows


def _load_or_refresh_latest_closed_bar_snapshot(account: str) -> dict[str, Any]:
    path = _latest_closed_bar_snapshot_path()
    cached = _read_json_file(path)
    if _cache_is_fresh(cached, _SHARED_LATEST_CLOSED_BAR_TTL_SECS):
        return cached
    now_ms = int(time.time() * 1000)
    latest_closed_bar_ts = _last_closed_bar_open_time_ms(account)
    signal_time_ts = _signal_time_ms_from_latest_closed_bar(latest_closed_bar_ts)
    payload = {
        'fetched_utc_ms': now_ms,
        'fetched_bj': _fmt_bj_from_ms(now_ms),
        'latest_closed_bar_ts': latest_closed_bar_ts,
        'latest_closed_bar_bj': _fmt_bj_from_ms(latest_closed_bar_ts),
        'signal_time_ts': signal_time_ts,
        'signal_time_bj': _fmt_bj_from_ms(signal_time_ts),
    }
    _atomic_write_json(path, payload)
    return payload


def _load_or_refresh_exchange_info(account: str) -> dict[str, Any]:
    path = _exchange_info_snapshot_path()
    cached = _read_json_file(path)
    if _cache_is_fresh(cached, _SHARED_EXCHANGE_INFO_TTL_SECS):
        return cached
    sleep_if_binance_rest_banned(source='market_data.exchange_info')
    client = get_client(account)
    now_ms = int(time.time() * 1000)
    payload = {
        'fetched_utc_ms': now_ms,
        'fetched_bj': _fmt_bj_from_ms(now_ms),
        'data': client.futures_exchange_info(),
    }
    _atomic_write_json(path, payload)
    return payload


def _load_or_refresh_ticker_rows(account: str) -> dict[str, Any]:
    path = _ticker_snapshot_path()
    cached = _read_json_file(path)
    if _cache_is_fresh(cached, _SHARED_TICKER_TTL_SECS):
        return cached
    sleep_if_binance_rest_banned(source='market_data.futures_ticker')
    client = get_client(account)
    now_ms = int(time.time() * 1000)
    payload = {
        'fetched_utc_ms': now_ms,
        'fetched_bj': _fmt_bj_from_ms(now_ms),
        'data': client.futures_ticker(),
    }
    _atomic_write_json(path, payload)
    return payload


def _rollsum_refresh_limit_for_symbol(rec: dict[str, Any] | None, latest_closed_bar_ts: int) -> int:
    if not isinstance(rec, dict):
        return _MARKET_24H_ROLLSUM_WINDOW_BARS
    window_size = _to_int(rec.get('window_size'), default=len(list(rec.get('rows') or [])))
    if window_size < _MARKET_24H_ROLLSUM_WINDOW_BARS or not bool(rec.get('is_ready_24h')):
        return _MARKET_24H_ROLLSUM_WINDOW_BARS
    last_bar_ts = _to_int(rec.get('latest_bar_ts'), default=0)
    if last_bar_ts <= 0:
        return _MARKET_24H_ROLLSUM_WINDOW_BARS
    lag_bars = max(0, (int(latest_closed_bar_ts) - int(last_bar_ts)) // 60000)
    if lag_bars <= 3:
        return max(2, int(lag_bars) + 2)
    if lag_bars <= 60:
        return int(lag_bars) + 2
    return _MARKET_24H_ROLLSUM_WINDOW_BARS



def refresh_hub_owned_1m_rollsum_for_symbols(
    account: str,
    symbols: list[str],
    *,
    latest_closed_bar_ts: int,
) -> dict[str, Any]:
    symbol_list = [str(symbol).upper().strip() for symbol in symbols if str(symbol).strip()]
    if not symbol_list:
        return _load_hub_owned_1m_rollsum_state()

    existing_state = _load_hub_owned_1m_rollsum_state()
    existing_symbols_state = dict(existing_state.get('symbols') or {})
    contract_frames: list[pd.DataFrame] = []
    for symbol in symbol_list:
        try:
            refresh_limit = _rollsum_refresh_limit_for_symbol(
                existing_symbols_state.get(symbol),
                int(latest_closed_bar_ts),
            )
            rows = _fetch_symbol_klines(
                account,
                symbol,
                refresh_limit,
                required_latest_closed_bar_ts=latest_closed_bar_ts,
            )
            raw_df = _rows_to_raw_df(symbol, rows, latest_closed_bar_ts)
            if raw_df.empty:
                continue
            contract_frames.append(raw_df[['symbol', 'open_time_ms', 'quote_asset_volume']].copy())
        except Exception:
            continue
    return _merge_contract_frames_into_hub_owned_1m_rollsum_state(
        contract_frames,
        latest_closed_bar_ts=latest_closed_bar_ts,
    )




def _empty_hub_owned_1m_rollsum_state() -> dict[str, Any]:
    return {
        'schema_version': 1,
        'updated_utc_ms': None,
        'updated_bj': None,
        'symbols': {},
    }


def _load_hub_owned_1m_rollsum_state() -> dict[str, Any]:
    path = _hub_owned_1m_rollsum_state_path()
    data = _read_json_file(path)
    if not isinstance(data, dict):
        return _empty_hub_owned_1m_rollsum_state()
    symbols = data.get('symbols')
    if not isinstance(symbols, dict):
        data['symbols'] = {}
    data.setdefault('schema_version', 1)
    data.setdefault('updated_utc_ms', None)
    data.setdefault('updated_bj', None)
    return data


def _save_hub_owned_1m_rollsum_state(state: dict[str, Any]) -> None:
    now_ms = int(time.time() * 1000)
    payload = dict(state or {})
    payload['schema_version'] = 1
    payload['updated_utc_ms'] = now_ms
    payload['updated_bj'] = _fmt_bj_from_ms(now_ms)
    payload['symbols'] = dict(payload.get('symbols') or {})
    _atomic_write_json(_hub_owned_1m_rollsum_state_path(), payload)


def _normalize_symbol_rollsum_rows(rows: list[list[Any]], latest_closed_bar_ts: int) -> list[list[Any]]:
    merged: dict[int, float] = {}
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 2:
            continue
        ts = _to_int(row[0], default=0)
        if ts <= 0 or ts > int(latest_closed_bar_ts):
            continue
        merged[int(ts)] = _to_float(row[1])
    ordered = sorted(merged.items(), key=lambda x: x[0])
    if len(ordered) > _MARKET_24H_ROLLSUM_WINDOW_BARS:
        ordered = ordered[-_MARKET_24H_ROLLSUM_WINDOW_BARS:]
    return [[int(ts), float(qav)] for ts, qav in ordered]


def _merge_contract_frames_into_hub_owned_1m_rollsum_state(
    frames: list[pd.DataFrame],
    *,
    latest_closed_bar_ts: int,
) -> dict[str, Any]:
    if not frames:
        return _load_hub_owned_1m_rollsum_state()
    state = _load_hub_owned_1m_rollsum_state()
    symbols_state = dict(state.get('symbols') or {})
    changed = False

    for frame in frames:
        if frame is None or frame.empty:
            continue
        cols = {str(c) for c in frame.columns}
        if 'symbol' not in cols or 'open_time_ms' not in cols or 'quote_asset_volume' not in cols:
            continue
        symbol = str(frame.iloc[0]['symbol']).upper().strip()
        existing_rows = list((symbols_state.get(symbol) or {}).get('rows') or [])
        new_rows = [
            [int(ts), float(qav)]
            for ts, qav in frame[['open_time_ms', 'quote_asset_volume']].itertuples(index=False, name=None)
            if _to_int(ts, default=0) > 0 and _to_int(ts, default=0) <= int(latest_closed_bar_ts)
        ]
        merged_rows = _normalize_symbol_rollsum_rows(existing_rows + new_rows, int(latest_closed_bar_ts))
        if not merged_rows:
            continue
        changed = True
        window_size = int(len(merged_rows))
        coverage_info = _recent_window_coverage_info(
            [int(row[0]) for row in merged_rows if isinstance(row, (list, tuple)) and len(row) >= 2],
            latest_closed_bar_ts,
            _MARKET_24H_ROLLSUM_WINDOW_BARS,
        )
        open_times = [int(row[0]) for row in merged_rows if isinstance(row, (list, tuple)) and len(row) >= 2]
        is_newly_listed_24h = bool(
            window_size < _MARKET_24H_ROLLSUM_WINDOW_BARS
            and _is_recent_prefix_continuous(open_times, int(latest_closed_bar_ts))
        )
        symbols_state[symbol] = {
            'latest_bar_ts': int(merged_rows[-1][0]),
            'rows': merged_rows,
            'window_size': window_size,
            'quote_volume_24h': float(sum(float(row[1]) for row in merged_rows)),
            'is_continuous_24h': bool(coverage_info.get('ok')),
            'missing_bar_count_24h': int(coverage_info.get('missing_count') or 0),
            'is_newly_listed_24h': is_newly_listed_24h,
            'is_ready_24h': bool(coverage_info.get('ok')),
        }

    state['symbols'] = symbols_state
    if changed:
        _save_hub_owned_1m_rollsum_state(state)
    return state


def _market_total_24h_vol_from_hub_owned_1m_state(
    account: str,
    ticker_map: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    info_snapshot = _load_or_refresh_exchange_info(account)
    state = _load_hub_owned_1m_rollsum_state()
    symbols_state = dict(state.get('symbols') or {})
    ready_symbol_map: dict[str, float] = {}
    api_symbol_map: dict[str, float] = {}
    missing_symbols: list[str] = []
    partial_symbols: list[str] = []
    newly_listed_symbols: list[str] = []

    for item in info_snapshot['data'].get('symbols', []):
        if str(item.get('status')) != 'TRADING':
            continue
        if str(item.get('contractType')) != 'PERPETUAL':
            continue
        if str(item.get('quoteAsset')) != 'USDT':
            continue
        symbol = str(item.get('symbol', '')).upper().strip()
        if not symbol:
            continue
        ticker = ticker_map.get(symbol)
        if ticker:
            api_symbol_map[symbol] = _to_float(ticker.get('quoteVolume'))
        rec = symbols_state.get(symbol)
        if not isinstance(rec, dict):
            missing_symbols.append(symbol)
            continue
        rows = list(rec.get('rows') or [])
        window_size = int(rec.get('window_size') or len(rows))
        if bool(rec.get('is_newly_listed_24h')):
            newly_listed_symbols.append(symbol)
            continue
        if window_size < _MARKET_24H_ROLLSUM_WINDOW_BARS or not bool(rec.get('is_ready_24h')):
            partial_symbols.append(symbol)
            continue
        ready_symbol_map[symbol] = _to_float(rec.get('quote_volume_24h'))

    total = float(sum(ready_symbol_map.values()))
    ready_count = int(len(ready_symbol_map))
    if ready_count <= 0:
        status = 'not_ready_hub_owned_1m'
    elif missing_symbols or partial_symbols:
        status = 'warming_hub_owned_1m'
    else:
        status = 'ready_hub_owned_1m'

    return {
        'market_total_24h_vol_1m_rollsum': total,
        'market_total_24h_symbol_count_1m_rollsum': ready_count,
        'symbol_24h_quote_volume_1m': ready_symbol_map,
        'symbol_24h_quote_volume_api': api_symbol_map,
        'missing_symbol_count_1m_rollsum': int(len(missing_symbols)),
        'missing_symbols_1m_rollsum': missing_symbols,
        'partial_symbol_count_1m_rollsum': int(len(partial_symbols)),
        'partial_symbols_1m_rollsum': partial_symbols,
        'newly_listed_symbol_count_1m_rollsum': int(len(newly_listed_symbols)),
        'newly_listed_symbols_1m_rollsum': newly_listed_symbols,
        'market_total_24h_vol_source': 'hub_owned_1m_rollsum',
        'market_total_24h_vol_status': status,
        'hub_owned_1m_rollsum_state_updated_utc_ms': state.get('updated_utc_ms'),
        'hub_owned_1m_rollsum_state_updated_bj': state.get('updated_bj'),
    }


def _symbol_kline_parquet_columns(path: Path) -> list[str]:
    try:
        import pyarrow.parquet as pq  # type: ignore
        return [str(x) for x in pq.ParquetFile(path).schema.names]
    except Exception:
        return []


def _pick_first_existing_column(columns: list[str], candidates: list[str]) -> str | None:
    lookup = {str(x).strip(): str(x).strip() for x in columns if str(x).strip()}
    for name in candidates:
        if name in lookup:
            return lookup[name]
    return None


def _normalize_open_time_ms_series(series: pd.Series) -> pd.Series:
    s = series
    if str(getattr(s, 'dtype', '')).startswith('datetime64'):
        return (s.view('int64') // 1_000_000).astype('int64')
    numeric = pd.to_numeric(s, errors='coerce')
    if numeric.notna().sum() == 0:
        parsed = pd.to_datetime(s, errors='coerce', utc=True)
        return (parsed.view('int64') // 1_000_000).astype('int64')
    max_abs = float(numeric.abs().max() or 0.0)
    if max_abs >= 1e18:
        return (numeric // 1_000_000).astype('int64')
    if max_abs >= 1e15:
        return (numeric // 1_000).astype('int64')
    if max_abs >= 1e12:
        return numeric.astype('int64')
    return (numeric * 1000).astype('int64')


def _read_symbol_quote_volume_frame(path: Path) -> pd.DataFrame | None:
    columns = _symbol_kline_parquet_columns(path)
    time_col = _pick_first_existing_column(columns, ['open_time_ms', 'open_time', 'open_ts', 'timestamp', 'ts'])
    volume_col = _pick_first_existing_column(columns, ['quote_asset_volume', 'quoteVolume', 'quote_volume', 'qav'])
    if time_col is None or volume_col is None:
        try:
            df = pd.read_parquet(path)
        except Exception:
            return None
        cols = [str(x) for x in df.columns]
        time_col = _pick_first_existing_column(cols, ['open_time_ms', 'open_time', 'open_ts', 'timestamp', 'ts'])
        volume_col = _pick_first_existing_column(cols, ['quote_asset_volume', 'quoteVolume', 'quote_volume', 'qav'])
        if time_col is None or volume_col is None:
            return None
        data = df[[time_col, volume_col]].copy()
    else:
        try:
            data = pd.read_parquet(path, columns=[time_col, volume_col]).copy()
        except Exception:
            try:
                data = pd.read_parquet(path)[[time_col, volume_col]].copy()
            except Exception:
                return None
    data.columns = ['open_time_ms', 'quote_asset_volume']
    if data.empty:
        return None
    data = data.dropna(subset=['open_time_ms']).copy()
    if data.empty:
        return None
    data['open_time_ms'] = _normalize_open_time_ms_series(data['open_time_ms'])
    data['quote_asset_volume'] = pd.to_numeric(data['quote_asset_volume'], errors='coerce').fillna(0.0)
    data = data.dropna(subset=['open_time_ms'])
    if data.empty:
        return None
    data['open_time_ms'] = data['open_time_ms'].astype('int64')
    return data


def _symbol_kline_parquet_path(symbol: str) -> Path:
    return _KLINES_1M_DIR / f'{_safe_symbol_key(symbol)}.parquet'


def _load_symbol_24h_quote_volume_from_parquet(symbol: str, latest_closed_bar_ts: int) -> dict[str, Any] | None:
    symbol_key = _safe_symbol_key(symbol)
    path = _symbol_kline_parquet_path(symbol_key)
    if not path.exists():
        return None
    cache = _SYMBOL_24H_QUOTE_VOLUME_CACHE.get(symbol_key)
    try:
        file_mtime_ns = path.stat().st_mtime_ns
    except Exception:
        file_mtime_ns = None
    if isinstance(cache, dict):
        if cache.get('latest_closed_bar_ts') == int(latest_closed_bar_ts) and cache.get('file_mtime_ns') == file_mtime_ns:
            return dict(cache)

    df = _read_symbol_quote_volume_frame(path)
    if df is None or df.empty:
        return None

    df = df[df['open_time_ms'] <= int(latest_closed_bar_ts)]
    if df.empty:
        return None

    df = df.sort_values('open_time_ms').drop_duplicates(subset=['open_time_ms'], keep='last').tail(_MARKET_24H_ROLLSUM_WINDOW_BARS)
    if df.empty:
        return None

    rows = [
        (int(open_time_ms), float(quote_asset_volume))
        for open_time_ms, quote_asset_volume in df[['open_time_ms', 'quote_asset_volume']].itertuples(index=False, name=None)
    ]
    total = float(sum(v for _, v in rows))
    coverage_info = _recent_window_coverage_info(
        [int(open_time_ms) for open_time_ms, _ in rows],
        latest_closed_bar_ts,
        _MARKET_24H_ROLLSUM_WINDOW_BARS,
    )
    out = {
        'symbol': symbol_key,
        'latest_closed_bar_ts': int(latest_closed_bar_ts),
        'latest_bar_ts': int(rows[-1][0]),
        'rows': rows,
        'window_size': int(len(rows)),
        'quote_volume_24h': float(total),
        'is_continuous_24h': bool(coverage_info.get('ok')),
        'missing_bar_count_24h': int(coverage_info.get('missing_count') or 0),
        'is_newly_listed_24h': bool(
            len(rows) < _MARKET_24H_ROLLSUM_WINDOW_BARS
            and _is_recent_prefix_continuous([int(open_time_ms) for open_time_ms, _ in rows], int(latest_closed_bar_ts))
        ),
        'file_mtime_ns': file_mtime_ns,
    }
    _SYMBOL_24H_QUOTE_VOLUME_CACHE[symbol_key] = dict(out)
    return out



def _market_total_24h_vol_from_live_ticker_map(account: str, ticker_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    info_snapshot = _load_or_refresh_exchange_info(account)
    total = 0.0
    symbol_count = 0
    symbol_map: dict[str, float] = {}
    for item in info_snapshot['data'].get('symbols', []):
        if str(item.get('status')) != 'TRADING':
            continue
        if str(item.get('contractType')) != 'PERPETUAL':
            continue
        if str(item.get('quoteAsset')) != 'USDT':
            continue
        symbol = str(item.get('symbol', '')).upper().strip()
        if not symbol:
            continue
        ticker = ticker_map.get(symbol)
        if not ticker:
            continue
        quote_vol = _to_float(ticker.get('quoteVolume'))
        symbol_map[symbol] = quote_vol
        total += quote_vol
        symbol_count += 1
    return {
        'market_total_24h_vol_1m_rollsum': float(total),
        'market_total_24h_symbol_count_1m_rollsum': int(symbol_count),
        'symbol_24h_quote_volume_1m': symbol_map,
        'missing_symbol_count_1m_rollsum': 0,
        'missing_symbols_1m_rollsum': [],
        'partial_symbol_count_1m_rollsum': 0,
        'partial_symbols_1m_rollsum': [],
        'newly_listed_symbol_count_1m_rollsum': 0,
        'newly_listed_symbols_1m_rollsum': [],
        'market_total_24h_vol_source': 'futures_ticker_live',
        'market_total_24h_vol_status': 'ready_live_api',
    }


def read_hub_owned_1m_rollsum_market_view(
    account: str,
    ticker_map: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    return _market_total_24h_vol_from_hub_owned_1m_state(account, ticker_map)


def build_market_snapshot(account: str) -> dict[str, Any]:
    latest_closed_bar_snapshot = _load_or_refresh_latest_closed_bar_snapshot(account)
    latest_closed_bar_ts = int(latest_closed_bar_snapshot['latest_closed_bar_ts'])
    ticker_map = _ticker_map(account)
    market_total_24h_payload_api = _market_total_24h_vol_from_ticker_map(account, ticker_map)
    market_total_24h_payload_rollsum = _market_total_24h_vol_from_hub_owned_1m_state(account, ticker_map)
    market_total_24h_vol_1m_rollsum = float(market_total_24h_payload_rollsum.get('market_total_24h_vol_1m_rollsum') or 0.0)
    return {
        'latest_closed_bar_ts': latest_closed_bar_ts,
        'latest_closed_bar_bj': latest_closed_bar_snapshot['latest_closed_bar_bj'],
        'signal_time_ts': int(latest_closed_bar_snapshot['signal_time_ts']),
        'signal_time_bj': latest_closed_bar_snapshot['signal_time_bj'],
        'market_snapshot_fetched_utc_ms': int(latest_closed_bar_snapshot['fetched_utc_ms']),
        'market_snapshot_fetched_bj': latest_closed_bar_snapshot['fetched_bj'],
        'ticker_map': ticker_map,
        'market_total_24h_vol': market_total_24h_vol_1m_rollsum,
        'market_total_24h_vol_api': float(market_total_24h_payload_api['market_total_24h_vol']),
        'market_total_24h_symbol_count_api': int(market_total_24h_payload_api['market_total_24h_symbol_count']),
        'market_total_24h_vol_1m_rollsum': market_total_24h_vol_1m_rollsum,
        'market_total_24h_symbol_count_1m_rollsum': int(market_total_24h_payload_rollsum.get('market_total_24h_symbol_count_1m_rollsum') or 0),
        'symbol_24h_quote_volume_1m': dict(market_total_24h_payload_rollsum.get('symbol_24h_quote_volume_1m') or {}),
        'symbol_24h_quote_volume_api': dict(market_total_24h_payload_rollsum.get('symbol_24h_quote_volume_api') or {}),
        'missing_symbol_count_1m_rollsum': int(market_total_24h_payload_rollsum.get('missing_symbol_count_1m_rollsum') or 0),
        'missing_symbols_1m_rollsum': list(market_total_24h_payload_rollsum.get('missing_symbols_1m_rollsum') or []),
        'partial_symbol_count_1m_rollsum': int(market_total_24h_payload_rollsum.get('partial_symbol_count_1m_rollsum') or 0),
        'partial_symbols_1m_rollsum': list(market_total_24h_payload_rollsum.get('partial_symbols_1m_rollsum') or []),
        'newly_listed_symbol_count_1m_rollsum': int(market_total_24h_payload_rollsum.get('newly_listed_symbol_count_1m_rollsum') or 0),
        'newly_listed_symbols_1m_rollsum': list(market_total_24h_payload_rollsum.get('newly_listed_symbols_1m_rollsum') or []),
        'market_total_24h_vol_source': str(market_total_24h_payload_rollsum.get('market_total_24h_vol_source') or ''),
        'market_total_24h_vol_1m_rollsum_status': str(market_total_24h_payload_rollsum.get('market_total_24h_vol_status') or ''),
        'hub_owned_1m_rollsum_state_updated_utc_ms': market_total_24h_payload_rollsum.get('hub_owned_1m_rollsum_state_updated_utc_ms'),
        'hub_owned_1m_rollsum_state_updated_bj': market_total_24h_payload_rollsum.get('hub_owned_1m_rollsum_state_updated_bj'),
    }


def _stage_audit_dir() -> Path:
    return get_stage_audit_dir()


def _append_stage_jsonl(account: str, stage: str, payload: dict[str, Any]) -> Path:
    return append_stage_record(account, stage, payload)


def _write_stage3_parquet(
    account: str,
    audit_label: str,
    c_bar_ts: int,
    signal_time_ts: int,
    rows: pd.DataFrame,
) -> Path:
    account_key = str(account).strip()
    payload = rows.copy()
    payload['c_bar_ts'] = int(c_bar_ts)
    payload['c_bar_bj'] = _fmt_bj_from_ms(int(c_bar_ts))
    payload['signal_time_ts'] = int(signal_time_ts)
    payload['signal_time_bj'] = _fmt_bj_from_ms(int(signal_time_ts))
    path = _stage_audit_dir() / f'snapback_{account_key}.stage3_bars.{audit_label}.{c_bar_ts}.parquet'
    payload.to_parquet(path, index=False)
    return path


def list_candidate_symbols(account: str, *, exclude_symbols: list[str] | None = None) -> list[str]:
    info_snapshot = _load_or_refresh_exchange_info(account)
    info = info_snapshot['data']
    exclude = {str(x).upper().strip() for x in (exclude_symbols or []) if str(x).strip()}
    out: list[str] = []
    for item in info.get('symbols', []):
        if str(item.get('status')) != 'TRADING':
            continue
        if str(item.get('contractType')) != 'PERPETUAL':
            continue
        if str(item.get('quoteAsset')) != 'USDT':
            continue
        symbol = str(item.get('symbol', '')).upper().strip()
        if not symbol or symbol in exclude:
            continue
        out.append(symbol)
    out.sort()
    return out


def _ticker_map(account: str) -> dict[str, dict[str, Any]]:
    rows = _load_or_refresh_ticker_rows(account)['data']
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get('symbol', '')).upper().strip()
        if symbol:
            out[symbol] = row
    return out


def _market_total_24h_vol_from_ticker_map(account: str, ticker_map: dict[str, dict[str, Any]]) -> dict[str, Any]:
    info_snapshot = _load_or_refresh_exchange_info(account)
    total = 0.0
    symbol_count = 0
    for item in info_snapshot['data'].get('symbols', []):
        if str(item.get('status')) != 'TRADING':
            continue
        if str(item.get('contractType')) != 'PERPETUAL':
            continue
        if str(item.get('quoteAsset')) != 'USDT':
            continue
        symbol = str(item.get('symbol', '')).upper().strip()
        if not symbol:
            continue
        ticker = ticker_map.get(symbol)
        if not ticker:
            continue
        total += _to_float(ticker.get('quoteVolume'))
        symbol_count += 1
    return {
        'market_total_24h_vol': float(total),
        'market_total_24h_symbol_count': int(symbol_count),
    }


def _require_universe_cfg(strategy_cfg: dict[str, Any] | None) -> tuple[float, float, float]:
    if not isinstance(strategy_cfg, dict):
        raise KeyError('strategy_cfg missing')
    universe = strategy_cfg.get('universe')
    if not isinstance(universe, dict):
        raise KeyError('strategy_cfg.universe missing')
    if '24h_quote_volume_min' not in universe:
        raise KeyError('strategy_cfg.universe.24h_quote_volume_min missing')
    chg_cfg = universe.get('24h_chg_pct')
    if not isinstance(chg_cfg, dict):
        raise KeyError('strategy_cfg.universe.24h_chg_pct missing')
    if 'min' not in chg_cfg:
        raise KeyError('strategy_cfg.universe.24h_chg_pct.min missing')
    if 'max' not in chg_cfg:
        raise KeyError('strategy_cfg.universe.24h_chg_pct.max missing')
    vol_min = _to_float(universe['24h_quote_volume_min'])
    chg_min = _to_float(chg_cfg['min'])
    chg_max = _to_float(chg_cfg['max'])
    return vol_min, chg_min, chg_max


def _filter_symbols_by_universe(
    symbols: list[str],
    metric_frames: dict[str, pd.DataFrame],
    strategy_cfg: dict[str, Any] | None,
    *,
    account: str,
    latest_closed_bar_ts: int,
    audit_label: str,
    ticker_map: dict[str, dict[str, Any]] | None = None,
    validate_metric_window: bool = True,
) -> tuple[list[str], dict[str, str]]:
    vol_min, chg_min, chg_max = _require_universe_cfg(strategy_cfg)
    eligible: list[str] = []
    errors: dict[str, str] = {}
    c_bar_bj = _fmt_bj_from_ms(latest_closed_bar_ts)
    signal_time_ts = _signal_time_ms_from_latest_closed_bar(latest_closed_bar_ts)
    signal_time_bj = _fmt_bj_from_ms(signal_time_ts)
    for symbol in symbols:
        metric_df = metric_frames.get(symbol)
        metric_row, metric_reason = _latest_metric_row(
            metric_df,
            latest_closed_bar_ts,
            validate_metric_window=validate_metric_window,
        )
        ticker = (ticker_map or {}).get(symbol)
        ticker_quote_volume = _to_float(ticker.get('quoteVolume')) if isinstance(ticker, dict) else None
        ticker_chg_pct = _to_float(ticker.get('priceChangePercent')) if isinstance(ticker, dict) else None
        if metric_row is None:
            reason = metric_reason or 'missing_24h_metric_frame'
            errors[symbol] = reason
            _append_stage_jsonl(account, 'stage2_universe', {
                'audit_label': audit_label,
                'bar_ts': signal_time_ts,
                'bar_bj': signal_time_bj,
                'signal_time_ts': signal_time_ts,
                'signal_time_bj': signal_time_bj,
                'c_bar_ts': latest_closed_bar_ts,
                'c_bar_bj': c_bar_bj,
                'symbol': symbol,
                'vol_24h': None,
                'chg_24h': None,
                'ticker_quote_volume': ticker_quote_volume,
                'ticker_chg_pct': ticker_chg_pct,
                'universe_pass': False,
                'universe_fail_reason': reason,
            })
            continue
        quote_vol = _to_float(metric_row.get('vol_24h'))
        chg_ratio = _to_float(metric_row.get('chg_24h'))
        chg_pct = float(chg_ratio * 100.0)
        reason = ''
        if metric_reason:
            reason = metric_reason
        elif quote_vol < vol_min:
            reason = 'quote_volume_below_min'
        elif chg_pct < chg_min:
            reason = 'chg_pct_below_min'
        elif chg_pct > chg_max:
            reason = 'chg_pct_above_max'
        if reason:
            _append_stage_jsonl(account, 'stage2_universe', {
                'audit_label': audit_label,
                'bar_ts': signal_time_ts,
                'bar_bj': signal_time_bj,
                'signal_time_ts': signal_time_ts,
                'signal_time_bj': signal_time_bj,
                'c_bar_ts': latest_closed_bar_ts,
                'c_bar_bj': c_bar_bj,
                'symbol': symbol,
                'vol_24h': quote_vol,
                'chg_24h': chg_ratio,
                'ticker_quote_volume': ticker_quote_volume,
                'ticker_chg_pct': ticker_chg_pct,
                'universe_pass': False,
                'universe_fail_reason': reason,
            })
            continue
        eligible.append(symbol)
        _append_stage_jsonl(account, 'stage2_universe', {
            'audit_label': audit_label,
            'bar_ts': signal_time_ts,
            'bar_bj': signal_time_bj,
            'signal_time_ts': signal_time_ts,
            'signal_time_bj': signal_time_bj,
            'c_bar_ts': latest_closed_bar_ts,
            'c_bar_bj': c_bar_bj,
            'symbol': symbol,
            'vol_24h': quote_vol,
            'chg_24h': chg_ratio,
            'ticker_quote_volume': ticker_quote_volume,
            'ticker_chg_pct': ticker_chg_pct,
            'universe_pass': True,
            'universe_fail_reason': '',
        })
    return eligible, errors


def _fetch_symbol_klines_remote(
    account: str,
    symbol: str,
    limit: int,
    *,
    start_time: int | None = None,
    end_time: int | None = None,
) -> list[list[Any]]:
    sleep_if_binance_rest_banned(source='market_data.futures_klines')
    client = get_client(account)
    params: dict[str, Any] = {
        'symbol': symbol,
        'interval': _INTERVAL,
        'limit': int(limit),
    }
    if start_time is not None:
        params['startTime'] = int(start_time)
    if end_time is not None:
        params['endTime'] = int(end_time)
    return client.futures_klines(**params)


def _fetch_symbol_index_price_klines_remote(
    account: str,
    symbol: str,
    limit: int,
    *,
    start_time: int | None = None,
    end_time: int | None = None,
) -> list[list[Any]]:
    sleep_if_binance_rest_banned(source='market_data.index_price_klines')
    return get_index_price_klines(
        account,
        symbol,
        interval=_INTERVAL,
        limit=int(limit),
        start_time=start_time,
        end_time=end_time,
    )


def _fetch_symbol_klines(
    account: str,
    symbol: str,
    limit: int,
    *,
    cache_stats: dict[str, Any] | None = None,
    required_latest_closed_bar_ts: int | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
) -> list[list[Any]]:
    if start_time is not None or end_time is not None:
        return _fetch_symbol_klines_remote(
            account,
            symbol,
            limit,
            start_time=start_time,
            end_time=end_time,
        )
    return _load_or_refresh_symbol_bar_rows(
        account,
        symbol,
        limit,
        kind='contract',
        cache_stats=cache_stats,
        required_latest_closed_bar_ts=required_latest_closed_bar_ts,
    )


def _fetch_symbol_index_price_klines(
    account: str,
    symbol: str,
    limit: int,
    *,
    cache_stats: dict[str, Any] | None = None,
    required_latest_closed_bar_ts: int | None = None,
    start_time: int | None = None,
    end_time: int | None = None,
) -> list[list[Any]]:
    if start_time is not None or end_time is not None:
        return _fetch_symbol_index_price_klines_remote(
            account,
            symbol,
            limit,
            start_time=start_time,
            end_time=end_time,
        )
    return _load_or_refresh_symbol_bar_rows(
        account,
        symbol,
        limit,
        kind='index',
        cache_stats=cache_stats,
        required_latest_closed_bar_ts=required_latest_closed_bar_ts,
    )


def _rows_to_raw_df(symbol: str, rows: list[list[Any]], latest_closed_bar_ts: int) -> pd.DataFrame:
    if not rows:
        raise ValueError(f'{symbol} kline rows empty')
    data = []
    for row in rows:
        open_time_ms = _to_int(row[0])
        if open_time_ms > latest_closed_bar_ts:
            continue
        data.append({
            'symbol': symbol,
            'open_time_ms': open_time_ms,
            'open': _to_float(row[1]),
            'high': _to_float(row[2]),
            'low': _to_float(row[3]),
            'close': _to_float(row[4]),
            'quote_asset_volume': _to_float(row[7]),
        })
    df = pd.DataFrame(data)
    if df.empty:
        raise ValueError(f'{symbol} has no closed 1m bars')
    df = df.sort_values('open_time_ms').drop_duplicates(subset=['open_time_ms'], keep='last').reset_index(drop=True)
    return df


def _rows_to_df(raw_df: pd.DataFrame) -> pd.DataFrame:
    df = raw_df.copy()
    df['high_idx'] = float('nan')
    df['low_idx'] = float('nan')
    df['close_idx'] = float('nan')
    window_24h = _MARKET_24H_ROLLSUM_WINDOW_BARS
    df = df.sort_values('open_time_ms').drop_duplicates(subset=['open_time_ms'], keep='last').reset_index(drop=True)
    df['chg_24h'] = df['close'] / df['close'].shift(window_24h) - 1.0
    df['vol_24h'] = df['quote_asset_volume'].rolling(window=window_24h, min_periods=1).sum()
    df.set_index('open_time_ms', inplace=True)
    df.index = df.index.astype('int64')
    df.sort_index(inplace=True)
    return df


def _required_contract_metric_bars() -> int:
    return int(_MARKET_24H_ROLLSUM_WINDOW_BARS) + 1


def _metric_window_reason(df: pd.DataFrame, latest_closed_bar_ts: int) -> str | None:
    info = _recent_window_coverage_info(
        [int(x) for x in df.index.tolist()],
        latest_closed_bar_ts,
        _required_contract_metric_bars(),
    )
    if bool(info.get('ok')):
        return None
    if _is_recent_prefix_continuous([int(x) for x in df.index.tolist()], int(latest_closed_bar_ts)):
        return 'chg_24h_insufficient_history'
    return _format_recent_window_reason('contract_24h_metric', info)


def _latest_metric_row(
    df: pd.DataFrame,
    latest_closed_bar_ts: int,
    *,
    validate_metric_window: bool = True,
) -> tuple[pd.Series | None, str]:
    if df is None or df.empty:
        return None, 'contract_24h_metric_empty'
    if int(latest_closed_bar_ts) not in df.index:
        return None, 'latest_closed_bar_missing'
    row = df.loc[int(latest_closed_bar_ts)].copy()
    metric_reason = _metric_window_reason(df, int(latest_closed_bar_ts)) if validate_metric_window else ''
    if pd.isna(row.get('chg_24h')):
        return row, metric_reason or 'chg_24h_missing'
    if pd.isna(row.get('vol_24h')):
        return row, 'vol_24h_missing'
    return row, ''


def _prefetch_contract_metric_frames(
    account: str,
    symbols: list[str],
    *,
    latest_closed_bar_ts: int,
    cache_stats: dict[str, Any] | None = None,
) -> tuple[dict[str, pd.DataFrame], dict[str, str]]:
    out: dict[str, pd.DataFrame] = {}
    errors: dict[str, str] = {}
    limit = _required_contract_metric_bars()
    for symbol in symbols:
        try:
            rows = _fetch_symbol_klines(
                account,
                symbol,
                limit,
                cache_stats=cache_stats,
                required_latest_closed_bar_ts=latest_closed_bar_ts,
            )
            raw_df = _rows_to_raw_df(symbol, rows, latest_closed_bar_ts)
            out[symbol] = _rows_to_df(raw_df)
        except Exception as e:
            errors[symbol] = str(e)
    return out, errors


def _rows_to_index_df(symbol: str, rows: list[list[Any]], latest_closed_bar_ts: int) -> pd.DataFrame:
    if not rows:
        raise ValueError(f'{symbol} index price kline rows empty')
    data = []
    for row in rows:
        open_time_ms = _to_int(row[0])
        if open_time_ms > latest_closed_bar_ts:
            continue
        data.append({
            'open_time_ms': open_time_ms,
            'high_idx': _to_float(row[2]),
            'low_idx': _to_float(row[3]),
            'close_idx': _to_float(row[4]),
        })
    df = pd.DataFrame(data)
    if df.empty:
        raise ValueError(f'{symbol} has no closed 1m index bars')
    df = df.sort_values('open_time_ms').drop_duplicates(subset=['open_time_ms'], keep='last').reset_index(drop=True)
    df.set_index('open_time_ms', inplace=True)
    df.index = df.index.astype('int64')
    df.sort_index(inplace=True)
    return df


def _build_live_inputs_for_symbols(
    account: str,
    symbols: list[str],
    history_window_mins: int,
    *,
    audit_label: str,
    latest_closed_bar_ts: int,
    ticker_map: dict[str, dict[str, Any]],
    write_stage3: bool = True,
    update_hub_owned_rollsum: bool = True,
    shared_symbol_bars_cache: dict[str, Any] | None = None,
    contract_metric_frames: dict[str, pd.DataFrame] | None = None,
) -> dict[str, Any]:
    errors: dict[str, str] = {}
    keep = int(history_window_mins)
    shared_symbol_bars_cache = shared_symbol_bars_cache if shared_symbol_bars_cache is not None else _new_shared_symbol_bars_cache_stats()

    histories: dict[str, pd.DataFrame] = {}
    stale_symbols: dict[str, str] = {}
    cross_rows: list[pd.Series] = []
    stage3_frames: list[pd.DataFrame] = []
    signal_time_ts = _signal_time_ms_from_latest_closed_bar(latest_closed_bar_ts)

    for symbol in symbols:
        try:
            metric_df = (contract_metric_frames or {}).get(symbol)
            if metric_df is None:
                rows = _fetch_symbol_klines(
                    account,
                    symbol,
                    max(keep, _required_contract_metric_bars()),
                    cache_stats=shared_symbol_bars_cache,
                    required_latest_closed_bar_ts=latest_closed_bar_ts,
                )
                raw_df = _rows_to_raw_df(symbol, rows, latest_closed_bar_ts)
                df = _rows_to_df(raw_df)
            else:
                df = metric_df.copy()
                raw_df = df.reset_index()[['symbol', 'open_time_ms', 'open', 'high', 'low', 'close', 'quote_asset_volume']].copy()
            stage3_frames.append(raw_df.tail(keep).reset_index(drop=True))
            df = df.tail(keep).copy()
            contract_window_reason = _frame_recent_window_reason(
                df,
                kind='contract',
                latest_closed_bar_ts=latest_closed_bar_ts,
                required_window_bars=keep,
            )
            if contract_window_reason is not None:
                stale_symbols[symbol] = contract_window_reason
                continue
            index_rows = _fetch_symbol_index_price_klines(
                account,
                symbol,
                keep,
                cache_stats=shared_symbol_bars_cache,
                required_latest_closed_bar_ts=latest_closed_bar_ts,
            )
            index_df = _rows_to_index_df(symbol, index_rows, latest_closed_bar_ts)
            index_window_reason = _frame_recent_window_reason(
                index_df,
                kind='index',
                latest_closed_bar_ts=latest_closed_bar_ts,
                required_window_bars=keep,
            )
            if index_window_reason is not None:
                stale_symbols[symbol] = index_window_reason
                continue
            aligned_idx = index_df.reindex(df.index)
            if aligned_idx[['high_idx', 'low_idx', 'close_idx']].isna().any().any():
                stale_symbols[symbol] = 'index_alignment_missing'
                continue
            df[['high_idx', 'low_idx', 'close_idx']] = aligned_idx[['high_idx', 'low_idx', 'close_idx']]
            histories[symbol] = df
            row = df.loc[latest_closed_bar_ts].copy()
            row.name = symbol
            cross_rows.append(row)
        except Exception as e:
            errors[symbol] = str(e)

    if stage3_frames and (write_stage3 or update_hub_owned_rollsum):
        stage3_df = pd.concat(stage3_frames, ignore_index=True)
        if write_stage3:
            _write_stage3_parquet(account, audit_label, latest_closed_bar_ts, signal_time_ts, stage3_df)
        if update_hub_owned_rollsum:
            _merge_contract_frames_into_hub_owned_1m_rollsum_state(stage3_frames, latest_closed_bar_ts=latest_closed_bar_ts)

    if not histories or not cross_rows:
        return {'ok': False, 'reason': 'no live symbol history loaded from binance', 'data': None, 'errors': errors | stale_symbols}

    cross_section = pd.DataFrame(cross_rows)
    cross_section.index.name = 'symbol'
    freshest_ts = latest_closed_bar_ts

    return {
        'ok': True,
        'reason': '',
        'errors': errors,
        'data': {
            'freshest_bar_ts': freshest_ts,
            'freshest_bar_bj': _fmt_bj_from_ms(freshest_ts),
            'stale_cutoff_ts': freshest_ts,
            'stale_cutoff_bj': _fmt_bj_from_ms(freshest_ts),
            'stale_symbol_count': len(stale_symbols),
            'stale_symbols': stale_symbols,
            'latest_closed_bar_ts': latest_closed_bar_ts,
            'latest_closed_bar_bj': _fmt_bj_from_ms(latest_closed_bar_ts),
            'signal_time_ts': signal_time_ts,
            'signal_time_bj': _fmt_bj_from_ms(signal_time_ts),
            'cross_section': cross_section,
            'full_df': histories,
            'symbol_count': len(histories),
            'bars_loaded_min': int(min(len(df) for df in histories.values())),
            'bars_loaded_max': int(max(len(df) for df in histories.values())),
            'shared_symbol_bars_cache': shared_symbol_bars_cache,
        },
    }


def build_live_inputs(
    account: str,
    symbols: list[str],
    history_window_mins: int,
    strategy_cfg: dict[str, Any] | None = None,
    *,
    audit_label: str = 'candidate',
    latest_closed_bar_ts: int | None = None,
    ticker_map: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    history_window_mins = int(history_window_mins)
    if history_window_mins <= 0:
        raise ValueError('history_window_mins must be > 0')

    latest_closed_bar_ts = int(latest_closed_bar_ts) if latest_closed_bar_ts is not None else _last_closed_bar_open_time_ms(account)
    ticker_map = ticker_map if ticker_map is not None else _ticker_map(account)
    shared_symbol_bars_cache = _new_shared_symbol_bars_cache_stats()
    contract_metric_frames, prefetch_errors = _prefetch_contract_metric_frames(
        account,
        symbols,
        latest_closed_bar_ts=latest_closed_bar_ts,
        cache_stats=shared_symbol_bars_cache,
    )
    eligible_symbols, universe_errors = _filter_symbols_by_universe(
        symbols,
        contract_metric_frames,
        strategy_cfg,
        account=account,
        latest_closed_bar_ts=latest_closed_bar_ts,
        audit_label=audit_label,
        ticker_map=ticker_map,
    )
    if not eligible_symbols:
        return {'ok': False, 'reason': 'no eligible symbols after 24h universe filter', 'data': None, 'errors': prefetch_errors | universe_errors}

    res = _build_live_inputs_for_symbols(
        account,
        eligible_symbols,
        history_window_mins,
        audit_label=audit_label,
        latest_closed_bar_ts=latest_closed_bar_ts,
        ticker_map=ticker_map,
        shared_symbol_bars_cache=shared_symbol_bars_cache,
        contract_metric_frames=contract_metric_frames,
    )
    merged_errors = dict(prefetch_errors)
    merged_errors.update(universe_errors)
    merged_errors.update(res.get('errors') or {})
    res['errors'] = merged_errors
    return res


def build_live_inputs_full_market_light_refresh(
    account: str,
    symbols: list[str],
    history_window_mins: int,
    *,
    audit_label: str = 'candidate_finalize',
    latest_closed_bar_ts: int | None = None,
    ticker_map: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    history_window_mins = int(history_window_mins)
    if history_window_mins <= 0:
        raise ValueError('history_window_mins must be > 0')
    latest_closed_bar_ts = int(latest_closed_bar_ts) if latest_closed_bar_ts is not None else _last_closed_bar_open_time_ms(account)
    ticker_map = ticker_map if ticker_map is not None else _ticker_map(account)
    return _build_live_inputs_for_symbols(
        account,
        [str(symbol).upper().strip() for symbol in symbols if str(symbol).strip()],
        history_window_mins,
        audit_label=audit_label,
        latest_closed_bar_ts=latest_closed_bar_ts,
        ticker_map=ticker_map,
        write_stage3=False,
        update_hub_owned_rollsum=False,
    )


def build_live_inputs_full_market(
    account: str,
    symbols: list[str],
    history_window_mins: int,
    *,
    audit_label: str = 'hub_full_market',
    latest_closed_bar_ts: int | None = None,
    ticker_map: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    history_window_mins = int(history_window_mins)
    if history_window_mins <= 0:
        raise ValueError('history_window_mins must be > 0')
    latest_closed_bar_ts = int(latest_closed_bar_ts) if latest_closed_bar_ts is not None else _last_closed_bar_open_time_ms(account)
    ticker_map = ticker_map if ticker_map is not None else _ticker_map(account)
    return _build_live_inputs_for_symbols(
        account,
        [str(symbol).upper().strip() for symbol in symbols if str(symbol).strip()],
        history_window_mins,
        audit_label=audit_label,
        latest_closed_bar_ts=latest_closed_bar_ts,
        ticker_map=ticker_map,
    )


def filter_loaded_payload_by_universe(
    account: str,
    loaded_payload: dict[str, Any],
    strategy_cfg: dict[str, Any],
    *,
    symbols: list[str] | None = None,
    ticker_map: dict[str, dict[str, Any]] | None = None,
    audit_label: str = 'candidate',
) -> dict[str, Any]:
    if not isinstance(loaded_payload, dict):
        return {'ok': False, 'reason': 'loaded_payload_missing', 'data': None, 'errors': {}}
    latest_closed_bar_ts = int(loaded_payload.get('latest_closed_bar_ts') or 0)
    if latest_closed_bar_ts <= 0:
        return {'ok': False, 'reason': 'loaded_payload_missing_latest_closed_bar_ts', 'data': None, 'errors': {}}
    full_df = dict(loaded_payload.get('full_df') or {})
    candidate_symbols = [str(x).upper().strip() for x in (symbols or list(full_df.keys())) if str(x).strip()]
    eligible_symbols, universe_errors = _filter_symbols_by_universe(
        candidate_symbols,
        full_df,
        strategy_cfg,
        account=account,
        latest_closed_bar_ts=latest_closed_bar_ts,
        audit_label=audit_label,
        ticker_map=ticker_map,
        validate_metric_window=False,
    )
    if not eligible_symbols:
        return {'ok': False, 'reason': 'no eligible symbols after 24h universe filter', 'data': None, 'errors': universe_errors}

    cross_section = loaded_payload.get('cross_section')
    filtered_full_df: dict[str, pd.DataFrame] = {}
    filtered_rows: list[pd.Series] = []
    errors = dict(universe_errors)
    stale_symbols = dict(loaded_payload.get('stale_symbols') or {})

    for symbol in eligible_symbols:
        df = full_df.get(symbol)
        if df is None:
            errors[symbol] = 'symbol_missing_from_loaded_payload'
            continue
        filtered_full_df[symbol] = df
        try:
            if cross_section is not None and symbol in cross_section.index:
                row = cross_section.loc[symbol].copy()
                row.name = symbol
                filtered_rows.append(row)
            elif latest_closed_bar_ts in df.index:
                row = df.loc[latest_closed_bar_ts].copy()
                row.name = symbol
                filtered_rows.append(row)
            else:
                stale_symbols[symbol] = 'latest_closed_bar_missing_after_hub_load'
        except Exception as e:
            errors[symbol] = str(e)

    if not filtered_full_df or not filtered_rows:
        return {'ok': False, 'reason': 'no eligible symbols left after hub universe filter', 'data': None, 'errors': errors | stale_symbols}

    filtered_cross_section = pd.DataFrame(filtered_rows)
    filtered_cross_section.index.name = 'symbol'
    freshest_ts = latest_closed_bar_ts
    data = {
        **loaded_payload,
        'freshest_bar_ts': freshest_ts,
        'freshest_bar_bj': _fmt_bj_from_ms(freshest_ts),
        'stale_cutoff_ts': freshest_ts,
        'stale_cutoff_bj': _fmt_bj_from_ms(freshest_ts),
        'stale_symbol_count': len(stale_symbols),
        'stale_symbols': stale_symbols,
        'cross_section': filtered_cross_section,
        'full_df': filtered_full_df,
        'symbol_count': len(filtered_full_df),
        'bars_loaded_min': int(min(len(df) for df in filtered_full_df.values())),
        'bars_loaded_max': int(max(len(df) for df in filtered_full_df.values())),
    }
    return {'ok': True, 'reason': '', 'data': data, 'errors': errors}
