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


def _shared_market_dir() -> Path:
    path = get_stage_audit_dir().parent / _SHARED_MARKET_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp_path = path.with_suffix(path.suffix + '.tmp')
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
    }


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


def _load_or_refresh_symbol_bar_rows(account: str, symbol: str, limit: int, *, kind: str, cache_stats: dict[str, Any] | None = None) -> list[list[Any]]:
    path = _symbol_bars_snapshot_path(symbol, limit, kind)
    cached = _read_json_file(path)
    if _cache_is_fresh(cached, _SHARED_SYMBOL_BARS_TTL_SECS):
        rows = cached.get('data')
        if isinstance(rows, list):
            _record_shared_symbol_bars_cache_event(cache_stats, kind=kind, symbol=symbol, cache_hit=True)
            return rows
    now_ms = int(time.time() * 1000)
    if kind == 'contract':
        rows = _fetch_symbol_klines_remote(account, symbol, limit)
    elif kind == 'index':
        rows = _fetch_symbol_index_price_klines_remote(account, symbol, limit)
    else:
        raise ValueError(f'unsupported bars kind: {kind}')
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
    client = get_client(account)
    now_ms = int(time.time() * 1000)
    payload = {
        'fetched_utc_ms': now_ms,
        'fetched_bj': _fmt_bj_from_ms(now_ms),
        'data': client.futures_ticker(),
    }
    _atomic_write_json(path, payload)
    return payload



def build_market_snapshot(account: str) -> dict[str, Any]:
    latest_closed_bar_snapshot = _load_or_refresh_latest_closed_bar_snapshot(account)
    return {
        'latest_closed_bar_ts': int(latest_closed_bar_snapshot['latest_closed_bar_ts']),
        'latest_closed_bar_bj': latest_closed_bar_snapshot['latest_closed_bar_bj'],
        'signal_time_ts': int(latest_closed_bar_snapshot['signal_time_ts']),
        'signal_time_bj': latest_closed_bar_snapshot['signal_time_bj'],
        'market_snapshot_fetched_utc_ms': int(latest_closed_bar_snapshot['fetched_utc_ms']),
        'market_snapshot_fetched_bj': latest_closed_bar_snapshot['fetched_bj'],
        'ticker_map': _ticker_map(account),
    }


def _stage_audit_dir() -> Path:
    return get_stage_audit_dir()


def _append_stage_jsonl(account: str, stage: str, payload: dict[str, Any]) -> Path:
    return append_stage_record(account, stage, payload)


def _write_stage3_parquet(account: str, audit_label: str, bar_ts: int, rows: pd.DataFrame) -> Path:
    account_key = str(account).strip()
    path = _stage_audit_dir() / f'snapback_{account_key}.stage3_bars.{audit_label}.{bar_ts}.parquet'
    rows.to_parquet(path, index=False)
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
    ticker_map: dict[str, dict[str, Any]],
    strategy_cfg: dict[str, Any] | None,
    *,
    account: str,
    latest_closed_bar_ts: int,
    audit_label: str,
) -> tuple[list[str], dict[str, str]]:
    vol_min, chg_min, chg_max = _require_universe_cfg(strategy_cfg)
    eligible: list[str] = []
    errors: dict[str, str] = {}
    c_bar_bj = _fmt_bj_from_ms(latest_closed_bar_ts)
    signal_time_ts = _signal_time_ms_from_latest_closed_bar(latest_closed_bar_ts)
    signal_time_bj = _fmt_bj_from_ms(signal_time_ts)
    for symbol in symbols:
        ticker = ticker_map.get(symbol)
        if not ticker:
            reason = 'missing_24h_ticker'
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
                'ticker_quote_volume': None,
                'ticker_chg_pct': None,
                'universe_pass': False,
                'universe_fail_reason': reason,
            })
            continue
        quote_vol = _to_float(ticker.get('quoteVolume'))
        chg_pct = _to_float(ticker.get('priceChangePercent'))
        reason = ''
        if quote_vol < vol_min:
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
                'ticker_quote_volume': quote_vol,
                'ticker_chg_pct': chg_pct,
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
            'ticker_quote_volume': quote_vol,
            'ticker_chg_pct': chg_pct,
            'universe_pass': True,
            'universe_fail_reason': '',
        })
    return eligible, errors


def _fetch_symbol_klines_remote(account: str, symbol: str, limit: int) -> list[list[Any]]:
    client = get_client(account)
    return client.futures_klines(symbol=symbol, interval=_INTERVAL, limit=int(limit))


def _fetch_symbol_index_price_klines_remote(account: str, symbol: str, limit: int) -> list[list[Any]]:
    return get_index_price_klines(account, symbol, interval=_INTERVAL, limit=int(limit))


def _fetch_symbol_klines(account: str, symbol: str, limit: int, *, cache_stats: dict[str, Any] | None = None) -> list[list[Any]]:
    return _load_or_refresh_symbol_bar_rows(account, symbol, limit, kind='contract', cache_stats=cache_stats)


def _fetch_symbol_index_price_klines(account: str, symbol: str, limit: int, *, cache_stats: dict[str, Any] | None = None) -> list[list[Any]]:
    return _load_or_refresh_symbol_bar_rows(account, symbol, limit, kind='index', cache_stats=cache_stats)


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


def _rows_to_df(raw_df: pd.DataFrame, ticker_24h: dict[str, Any]) -> pd.DataFrame:
    df = raw_df.copy()
    df['high_idx'] = float('nan')
    df['low_idx'] = float('nan')
    df['close_idx'] = float('nan')
    chg_ratio = _to_float(ticker_24h.get('priceChangePercent')) / 100.0
    vol_24h = _to_float(ticker_24h.get('quoteVolume'))
    df['chg_24h'] = chg_ratio
    df['vol_24h'] = vol_24h
    df.set_index('open_time_ms', inplace=True)
    df.index = df.index.astype('int64')
    df.sort_index(inplace=True)
    return df


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
    errors: dict[str, str] = {}
    history_window_mins = int(history_window_mins)
    if history_window_mins <= 0:
        raise ValueError('history_window_mins must be > 0')

    latest_closed_bar_ts = int(latest_closed_bar_ts) if latest_closed_bar_ts is not None else _last_closed_bar_open_time_ms(account)
    signal_time_ts = _signal_time_ms_from_latest_closed_bar(latest_closed_bar_ts)
    ticker_map = ticker_map if ticker_map is not None else _ticker_map(account)
    eligible_symbols, universe_errors = _filter_symbols_by_universe(
        symbols,
        ticker_map,
        strategy_cfg,
        account=account,
        latest_closed_bar_ts=latest_closed_bar_ts,
        audit_label=audit_label,
    )
    errors.update(universe_errors)
    if not eligible_symbols:
        return {'ok': False, 'reason': 'no eligible symbols after 24h universe filter', 'data': None, 'errors': errors}

    keep = int(history_window_mins)
    shared_symbol_bars_cache = _new_shared_symbol_bars_cache_stats()

    histories: dict[str, pd.DataFrame] = {}
    stale_symbols: dict[str, str] = {}
    cross_rows: list[pd.Series] = []
    stage3_frames: list[pd.DataFrame] = []

    for symbol in eligible_symbols:
        try:
            rows = _fetch_symbol_klines(account, symbol, keep, cache_stats=shared_symbol_bars_cache)
            raw_df = _rows_to_raw_df(symbol, rows, latest_closed_bar_ts)
            stage3_frames.append(raw_df)
            df = _rows_to_df(raw_df, ticker_map.get(symbol) or {})
            if latest_closed_bar_ts not in df.index:
                stale_symbols[symbol] = _fmt_bj_from_ms(_to_int(df.index.max()))
                continue
            index_rows = _fetch_symbol_index_price_klines(account, symbol, keep, cache_stats=shared_symbol_bars_cache)
            index_df = _rows_to_index_df(symbol, index_rows, latest_closed_bar_ts)
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

    if stage3_frames:
        stage3_df = pd.concat(stage3_frames, ignore_index=True)
        _write_stage3_parquet(account, audit_label, signal_time_ts, stage3_df)

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
