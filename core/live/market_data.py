from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from core.live.audit_log import get_live_audit_dir
from core.live.binance_client import get_client

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


def _stage_audit_dir() -> Path:
    path = get_live_audit_dir() / _STAGE_AUDIT_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def _append_stage_jsonl(account: str, stage: str, payload: dict[str, Any]) -> Path:
    account_key = str(account).strip()
    path = _stage_audit_dir() / f'snapback_{account_key}.{stage}.jsonl'
    record = {
        'ts_utc': datetime.now(timezone.utc).isoformat(),
        'ts_bj': datetime.now(timezone.utc).astimezone(_BJ).strftime('%Y-%m-%d %H:%M:%S'),
        'account': account_key,
        'run_mode': 'live',
        'stage': stage,
    }
    record.update(payload)
    with path.open('a', encoding='utf-8') as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + '\n')
    return path


def _write_stage3_parquet(account: str, audit_label: str, bar_ts: int, rows: pd.DataFrame) -> Path:
    account_key = str(account).strip()
    path = _stage_audit_dir() / f'snapback_{account_key}.stage3_bars.{audit_label}.{bar_ts}.parquet'
    rows.to_parquet(path, index=False)
    return path


def list_candidate_symbols(account: str, *, exclude_symbols: list[str] | None = None) -> list[str]:
    client = get_client(account)
    info = client.futures_exchange_info()
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
    client = get_client(account)
    rows = client.futures_ticker()
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
    bar_bj = _fmt_bj_from_ms(latest_closed_bar_ts)
    for symbol in symbols:
        ticker = ticker_map.get(symbol)
        if not ticker:
            reason = 'missing_24h_ticker'
            errors[symbol] = reason
            _append_stage_jsonl(account, 'stage2_universe', {
                'audit_label': audit_label,
                'bar_ts': latest_closed_bar_ts,
                'bar_bj': bar_bj,
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
                'bar_ts': latest_closed_bar_ts,
                'bar_bj': bar_bj,
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
            'bar_ts': latest_closed_bar_ts,
            'bar_bj': bar_bj,
            'symbol': symbol,
            'ticker_quote_volume': quote_vol,
            'ticker_chg_pct': chg_pct,
            'universe_pass': True,
            'universe_fail_reason': '',
        })
    return eligible, errors


def _fetch_symbol_klines(account: str, symbol: str, limit: int) -> list[list[Any]]:
    client = get_client(account)
    return client.futures_klines(symbol=symbol, interval=_INTERVAL, limit=int(limit))


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


def _build_index_df(account: str, latest_closed_bar_ts: int, keep: int) -> pd.DataFrame:
    # NOTE: benchmark/index logic intentionally left untouched in this batch.
    # This patch batch only adds live-input audit and removes lookback/default-value issues.
    benchmark_weights = {
        'BTCUSDT': 0.56,
        'ETHUSDT': 0.24,
        'BNBUSDT': 0.12,
        'SOLUSDT': 0.08,
    }
    series_map: dict[str, pd.DataFrame] = {}
    for symbol, weight in benchmark_weights.items():
        rows = _fetch_symbol_klines(account, symbol, keep)
        data = []
        for row in rows:
            open_time_ms = _to_int(row[0])
            if open_time_ms > latest_closed_bar_ts:
                continue
            data.append({
                'open_time_ms': open_time_ms,
                'high': _to_float(row[2]),
                'low': _to_float(row[3]),
                'close': _to_float(row[4]),
            })
        df = pd.DataFrame(data)
        if df.empty:
            raise ValueError(f'benchmark {symbol} has no closed 1m bars')
        df = df.sort_values('open_time_ms').drop_duplicates(subset=['open_time_ms'], keep='last').reset_index(drop=True)
        df.set_index('open_time_ms', inplace=True)
        df.index = df.index.astype('int64')
        base_close = float(df['close'].iloc[0])
        if base_close <= 0:
            raise ValueError(f'benchmark {symbol} base close invalid')
        part = pd.DataFrame(index=df.index)
        part[f'{symbol}_close'] = (df['close'] / base_close) * float(weight)
        part[f'{symbol}_high'] = (df['high'] / base_close) * float(weight)
        part[f'{symbol}_low'] = (df['low'] / base_close) * float(weight)
        series_map[symbol] = part
    merged = pd.concat(series_map.values(), axis=1, join='inner').sort_index()
    if merged.empty:
        raise ValueError('benchmark merged index empty')
    out = pd.DataFrame(index=merged.index)
    out['close_idx'] = merged[[f'{s}_close' for s in benchmark_weights]].sum(axis=1)
    out['high_idx'] = merged[[f'{s}_high' for s in benchmark_weights]].sum(axis=1)
    out['low_idx'] = merged[[f'{s}_low' for s in benchmark_weights]].sum(axis=1)
    return out


def build_live_inputs(
    account: str,
    symbols: list[str],
    history_window_mins: int,
    strategy_cfg: dict[str, Any] | None = None,
    *,
    audit_label: str = 'candidate',
) -> dict[str, Any]:
    errors: dict[str, str] = {}
    history_window_mins = int(history_window_mins)
    if history_window_mins <= 0:
        raise ValueError('history_window_mins must be > 0')

    latest_closed_bar_ts = _last_closed_bar_open_time_ms(account)
    ticker_map = _ticker_map(account)
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
    index_df = _build_index_df(account, latest_closed_bar_ts, keep)

    histories: dict[str, pd.DataFrame] = {}
    stale_symbols: dict[str, str] = {}
    cross_rows: list[pd.Series] = []
    stage3_frames: list[pd.DataFrame] = []

    for symbol in eligible_symbols:
        try:
            rows = _fetch_symbol_klines(account, symbol, keep)
            raw_df = _rows_to_raw_df(symbol, rows, latest_closed_bar_ts)
            stage3_frames.append(raw_df)
            df = _rows_to_df(raw_df, ticker_map.get(symbol) or {})
            if latest_closed_bar_ts not in df.index:
                stale_symbols[symbol] = _fmt_bj_from_ms(_to_int(df.index.max()))
                continue
            aligned_idx = index_df.reindex(df.index)
            if aligned_idx[['high_idx', 'low_idx', 'close_idx']].isna().any().any():
                stale_symbols[symbol] = 'benchmark_alignment_missing'
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
        _write_stage3_parquet(account, audit_label, latest_closed_bar_ts, stage3_df)

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
            'cross_section': cross_section,
            'full_df': histories,
            'symbol_count': len(histories),
            'bars_loaded_min': int(min(len(df) for df in histories.values())),
            'bars_loaded_max': int(max(len(df) for df in histories.values())),
        },
    }
