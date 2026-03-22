from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.parquet as pq

_BJ = timezone(timedelta(hours=8))
_MAX_SYMBOL_STALE_MS = 2 * 60 * 1000


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def get_klines_root() -> Path:
    raw = os.getenv("BN_KLINES_ROOT", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return _repo_root() / "data" / "klines_1m"


def list_candidate_symbols(*, exclude_symbols: list[str] | None = None) -> list[str]:
    root = get_klines_root()
    if not root.exists():
        raise FileNotFoundError(f"klines root not found: {root}")
    exclude = {str(x).upper().strip() for x in (exclude_symbols or []) if str(x).strip()}
    symbols: list[str] = []
    for p in sorted(root.iterdir()):
        if not p.is_dir():
            continue
        sym = p.name.upper().strip()
        if sym in exclude:
            continue
        symbols.append(sym)
    return symbols


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(_BJ).strftime("%Y-%m-%d %H:%M:%S")


def _read_symbol_parquet(symbol: str) -> pd.DataFrame:
    sym = str(symbol).upper().strip()
    sym_dir = get_klines_root() / sym
    if not sym_dir.exists():
        raise FileNotFoundError(f"symbol dir not found: {sym_dir}")
    files = sorted(sym_dir.glob('*.parquet'))
    if not files:
        raise FileNotFoundError(f"no parquet files found for {sym}")
    table = pq.read_table([str(p) for p in files])
    df = table.to_pandas()
    if df.empty:
        raise ValueError(f"empty parquet for {sym}")
    return df


def _prepare_symbol_df(symbol: str, df: pd.DataFrame, lookback_bars: int) -> pd.DataFrame:
    required = ['open_time_ms', 'open', 'high', 'low', 'close', 'quote_asset_volume']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise KeyError(f"{symbol} missing required columns: {missing}")

    out = df.copy()
    out = out.sort_values('open_time_ms').drop_duplicates(subset=['open_time_ms'], keep='last')
    out = out.reset_index(drop=True)
    out['symbol'] = symbol

    for idx_col in ('high_idx', 'low_idx', 'close_idx'):
        if idx_col not in out.columns:
            out[idx_col] = float('nan')

    window_24h = 24 * 60
    out['chg_24h'] = out['close'] / out['close'].shift(window_24h) - 1.0
    out['vol_24h'] = out['quote_asset_volume'].rolling(window=window_24h, min_periods=1).sum()

    keep = max(int(lookback_bars), window_24h + 5)
    if len(out) > keep:
        out = out.iloc[-keep:].copy()

    out.set_index('open_time_ms', inplace=True)
    out.index = out.index.astype('int64')
    out.sort_index(inplace=True)
    return out


def load_symbol_history(symbol: str, lookback_bars: int) -> dict[str, Any]:
    sym = str(symbol).upper().strip()
    try:
        df_raw = _read_symbol_parquet(sym)
        df = _prepare_symbol_df(sym, df_raw, lookback_bars)
        if df.empty:
            return {'ok': False, 'reason': f'{sym} history empty after prepare', 'data': None}
        latest_ts = int(df.index.max())
        latest_row = df.loc[latest_ts]
        return {
            'ok': True,
            'reason': '',
            'data': {
                'symbol': sym,
                'df': df,
                'latest_closed_bar_ts': latest_ts,
                'latest_closed_bar_bj': _fmt_bj_from_ms(latest_ts),
                'bars_count': int(len(df)),
                'latest_close': float(latest_row['close']),
            },
        }
    except Exception as e:
        return {'ok': False, 'reason': str(e), 'data': None}


def build_live_inputs(symbols: list[str], lookback_bars: int) -> dict[str, Any]:
    histories: dict[str, pd.DataFrame] = {}
    latest_map: dict[str, int] = {}
    errors: dict[str, str] = {}

    for symbol in symbols:
        res = load_symbol_history(symbol, lookback_bars)
        if not res['ok']:
            errors[str(symbol).upper().strip()] = res['reason']
            continue
        payload = res['data']
        histories[payload['symbol']] = payload['df']
        latest_map[payload['symbol']] = int(payload['latest_closed_bar_ts'])

    if not histories:
        return {
            'ok': False,
            'reason': 'no symbol history loaded',
            'data': None,
            'errors': errors,
        }

    freshest_ts = max(latest_map.values())
    stale_cutoff_ts = freshest_ts - _MAX_SYMBOL_STALE_MS
    eligible_symbols = {
        symbol: ts
        for symbol, ts in latest_map.items()
        if int(ts) >= stale_cutoff_ts
    }
    stale_symbols = {
        symbol: _fmt_bj_from_ms(ts)
        for symbol, ts in latest_map.items()
        if symbol not in eligible_symbols
    }

    if not eligible_symbols:
        return {
            'ok': False,
            'reason': 'all symbols stale after freshness filter',
            'data': None,
            'errors': errors,
        }

    latest_common_ts = min(eligible_symbols.values())
    cross_rows: list[pd.Series] = []
    full_df: dict[str, pd.DataFrame] = {}

    for symbol, df in histories.items():
        if symbol not in eligible_symbols:
            continue
        clipped = df[df.index <= latest_common_ts].copy()
        if clipped.empty or latest_common_ts not in clipped.index:
            continue
        full_df[symbol] = clipped
        row = clipped.loc[latest_common_ts].copy()
        row.name = symbol
        cross_rows.append(row)

    if not cross_rows:
        return {
            'ok': False,
            'reason': 'no cross section rows at latest common closed bar',
            'data': None,
            'errors': errors,
        }

    cross_section = pd.DataFrame(cross_rows)
    cross_section.index.name = 'symbol'

    return {
        'ok': True,
        'reason': '',
        'errors': errors,
        'data': {
            'freshest_bar_ts': freshest_ts,
            'freshest_bar_bj': _fmt_bj_from_ms(freshest_ts),
            'stale_cutoff_ts': stale_cutoff_ts,
            'stale_cutoff_bj': _fmt_bj_from_ms(stale_cutoff_ts),
            'stale_symbol_count': len(stale_symbols),
            'stale_symbols': stale_symbols,
            'latest_closed_bar_ts': latest_common_ts,
            'latest_closed_bar_bj': _fmt_bj_from_ms(latest_common_ts),
            'cross_section': cross_section,
            'full_df': full_df,
            'symbol_count': len(full_df),
            'bars_loaded_min': int(min(len(df) for df in full_df.values())),
            'bars_loaded_max': int(max(len(df) for df in full_df.values())),
        },
    }
