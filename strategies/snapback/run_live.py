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
from core.live.market_data import build_live_inputs, list_candidate_symbols
from core.message_bridge import send_to_bot
from strategies.snapback.logic import WashoutSnapbackStrategy
from strategies.snapback.trade_consumer import (
    bootstrap_consumer_gate,
    build_consumer_reconcile_plan,
    consume_signal,
    consumer_signal_digest,
    finalize_consumer_loop_state,
    prepare_consumer_loop_gate,
)

BJ = timezone(timedelta(hours=8))


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


def _notify(enabled: bool, message: str, label: str = 'snapback') -> None:
    if enabled:
        send_to_bot(message, label=label)



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
    current_minute_first_second = now.replace(second=1, microsecond=0)
    if now < current_minute_first_second:
        return current_minute_first_second.timestamp()
    next_minute_first_second = now.replace(second=0, microsecond=0) + timedelta(minutes=1, seconds=1)
    return next_minute_first_second.timestamp()


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
    candidate_symbols = list_candidate_symbols(account, exclude_symbols=live_cfg.get('exclude_symbols') or [])
    reconcile_plan = build_consumer_reconcile_plan(account, candidate_symbols)
    exchange_activity_snapshot = dict(reconcile_plan['exchange_snapshot'])
    exchange_activity_symbols = set(reconcile_plan['exchange_activity_symbols'])
    local_activity_symbols = set(reconcile_plan['local_active_symbols'])
    extra_reconcile_symbols = list(reconcile_plan['extra_reconcile_symbols'])

    candidate_md_started_utc_ms = _now_utc_ms()
    candidate_md_res = build_live_inputs(account, candidate_symbols, history_window_mins, strategy_cfg, audit_label='candidate')
    candidate_md_finished_utc_ms = _now_utc_ms()
    extra_md_res: dict[str, Any] | None = None
    extra_md_started_utc_ms: int | None = None
    extra_md_finished_utc_ms: int | None = None
    if extra_reconcile_symbols:
        extra_md_started_utc_ms = _now_utc_ms()
        extra_md_res = build_live_inputs(account, extra_reconcile_symbols, history_window_mins, strategy_cfg, audit_label='reconcile')
        extra_md_finished_utc_ms = _now_utc_ms()

    candidate_payload = candidate_md_res.get('data') if candidate_md_res.get('ok') else None
    extra_payload = extra_md_res.get('data') if extra_md_res and extra_md_res.get('ok') else None

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
        return

    c_bar_ts = int(payload['latest_closed_bar_ts'])
    c_bar_bj = payload['latest_closed_bar_bj']
    current_time_ms = int(payload.get('signal_time_ts') or (c_bar_ts + 60000))
    current_time_bj = str(payload.get('signal_time_bj') or _fmt_bj_from_ms(current_time_ms) or '')
    timing_fields = {
        'loop_started_utc_ms': loop_started_utc_ms,
        'loop_started_bj': loop_started_bj,
        'scheduled_signal_check_utc': scheduled_signal_check_utc,
        'scheduled_signal_check_bj': scheduled_signal_check_bj,
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

    if audit_enabled:
        if candidate_full_df:
            _write_stage3_enriched_snapshot(account, 'candidate', current_time_ms, current_time_bj, candidate_full_df, timing_fields)
        if extra_full_df:
            _write_stage3_enriched_snapshot(account, 'reconcile', current_time_ms, current_time_bj, extra_full_df, timing_fields)

    latest_closes = {
        str(symbol).upper().strip(): float(df.loc[c_bar_ts, 'close'])
        for symbol, df in merged_full_df.items()
        if c_bar_ts in df.index
    }

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
    scan_gate = loop_gate['scan_gate']
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
        return

    cross_section = candidate_cross_section
    full_df = candidate_full_df
    strategy = WashoutSnapbackStrategy(strategy_cfg)
    _hydrate_strategy_cooldowns(strategy, account, current_time_ms)
    active_symbols = {
        str(symbol).upper().strip()
        for symbol in (loop_gate.get('active_symbols') or [])
        if str(symbol).strip()
    }

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

    signal_eval_started_utc_ms = _now_utc_ms()
    signal = strategy.on_kline_close(c_bar_ts, cross_section, active_symbols, full_df)
    _persist_strategy_cooldowns(strategy, account, current_time_ms)
    signal_eval_finished_utc_ms = _now_utc_ms()
    signal_digest_preview = consumer_signal_digest(signal) if signal else None
    stage5_rows = _build_stage5_structure_rows(
        c_bar_ts,
        current_time_ms,
        current_time_bj,
        cross_section,
        active_symbols,
        full_df,
        strategy_cfg,
        logic_selected_symbol=(str(signal['symbol']).upper().strip() if signal else None),
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
        return

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
