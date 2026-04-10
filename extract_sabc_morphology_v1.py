#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow.parquet as pq

BJ_OFFSET_HOURS = 8


def _read_jsonl(path: str) -> List[Dict[str, Any]]:
    rows = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                rows.append(json.loads(s))
    return rows


def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None or (isinstance(v, str) and not v.strip()):
            return default
        fv = float(v)
        if math.isnan(fv) or math.isinf(fv):
            return default
        return fv
    except Exception:
        return default


def _safe_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


def _fmt_bj(ts_ms: Optional[int]) -> Optional[str]:
    if ts_ms is None:
        return None
    return (pd.to_datetime(ts_ms, unit="ms", utc=True) + pd.Timedelta(hours=BJ_OFFSET_HOURS)).strftime("%Y-%m-%d %H:%M:%S")


class SymbolStore:
    def __init__(self, data_dir: str):
        self.data_dir = Path(data_dir)
        self._cache: Dict[str, Optional[pd.DataFrame]] = {}

    def get(self, symbol: str) -> Optional[pd.DataFrame]:
        sym = str(symbol).upper().strip()
        if sym in self._cache:
            return self._cache[sym]
        sym_dir = self.data_dir / sym
        if not sym_dir.exists():
            self._cache[sym] = None
            return None
        files = sorted(str(p) for p in sym_dir.iterdir() if p.suffix == ".parquet")
        if not files:
            self._cache[sym] = None
            return None
        tbl = pq.read_table(files)
        df = tbl.to_pandas().sort_values("open_time_ms").set_index("open_time_ms")
        for col in ["open", "high", "low", "close", "quote_asset_volume", "high_idx", "low_idx", "close_idx"]:
            if col not in df.columns:
                df[col] = float("nan")
        self._cache[sym] = df
        return df


@dataclass
class JoinedTrade:
    signal: Dict[str, Any]
    trade: Dict[str, Any]


def _trade_outcome(trade: Dict[str, Any]) -> str:
    reason = str(trade.get("exit_reason") or trade.get("reason") or "").upper().strip()
    if "TAKE_PROFIT" in reason or reason == "TP":
        return "TP"
    if "STOP_LOSS" in reason or reason == "SL":
        return "SL"
    if "TIME" in reason or "TIMEOUT" in reason:
        return "TIMEOUT"
    pnl_pct = _safe_float(trade.get("pnl_pct"), None)
    if pnl_pct is None:
        return "UNKNOWN"
    return "WIN_OTHER" if pnl_pct > 0 else ("LOSS_OTHER" if pnl_pct < 0 else "FLAT")


def _join_signals_trades(signals: List[Dict[str, Any]], trades: List[Dict[str, Any]]) -> List[JoinedTrade]:
    trade_map: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
    for t in trades:
        symbol = str(t.get("symbol") or "").upper().strip()
        signal_time = _safe_int(t.get("signal_time"), None)
        if symbol and signal_time is not None:
            trade_map.setdefault((symbol, signal_time), []).append(t)
    joined: List[JoinedTrade] = []
    for s in signals:
        symbol = str(s.get("symbol") or "").upper().strip()
        signal_time = _safe_int(s.get("signal_time"), None)
        if not symbol or signal_time is None:
            continue
        rows = trade_map.get((symbol, signal_time)) or []
        if rows:
            joined.append(JoinedTrade(signal=s, trade=rows.pop(0)))
    return joined


def _window_slice(df: pd.DataFrame, start_ms: int, end_ms: int) -> pd.DataFrame:
    return df.loc[(df.index >= int(start_ms)) & (df.index <= int(end_ms))].copy()


def _count_monotonic(series: pd.Series, mode: str) -> int:
    if series is None or len(series) < 2:
        return 0
    diffs = series.diff().dropna()
    return int((diffs < 0).sum()) if mode == "down" else int((diffs > 0).sum())


def _range_pos(value: Optional[float], low: Optional[float], high: Optional[float]) -> Optional[float]:
    if value is None or low is None or high is None:
        return None
    span = high - low
    return None if span <= 0 else (value - low) / span


def _extract_row(df: pd.DataFrame, ts_ms: int) -> Optional[pd.Series]:
    try:
        if ts_ms not in df.index:
            return None
        row = df.loc[ts_ms]
        return row.iloc[-1] if isinstance(row, pd.DataFrame) else row
    except Exception:
        return None


def build_features(store: SymbolStore, item: JoinedTrade, pre_window_bars: int = 60, post_b_window_bars: int = 8) -> Dict[str, Any]:
    s = item.signal
    t = item.trade
    ctx = dict(s.get("context") or {})
    params = dict(s.get("params") or {})
    symbol = str(s.get("symbol") or t.get("symbol") or "").upper().strip()
    signal_time = _safe_int(s.get("signal_time"), None)
    current_price = _safe_float(s.get("current_price"), None)
    tp_price = _safe_float(s.get("tp_price"), None)
    sl_price = _safe_float(s.get("sl_price"), None)
    s_time = _safe_int(ctx.get("s_time"), None)
    a_time = _safe_int(ctx.get("a_time"), None)
    b_time = _safe_int(ctx.get("b_time"), None)
    c_time = _safe_int(ctx.get("c_time"), signal_time)
    s_close = _safe_float(ctx.get("s_close"), None)
    a_high = _safe_float(ctx.get("a_high_price"), None)
    b_contract = _safe_float(ctx.get("b_contract_price"), None)
    b_index = _safe_float(ctx.get("b_index_price"), None)
    c_price = _safe_float(ctx.get("c_price"), current_price)
    c_index = _safe_float(ctx.get("c_index_price"), None)
    if None in (signal_time, s_time, a_time, b_time, c_time, s_close, a_high, b_contract, b_index, c_price):
        return {"symbol": symbol, "signal_time": signal_time, "outcome": _trade_outcome(t), "feature_status": "missing_core_context"}
    df = store.get(symbol)
    if df is None or df.empty:
        return {"symbol": symbol, "signal_time": signal_time, "outcome": _trade_outcome(t), "feature_status": "symbol_df_missing"}
    if any(_extract_row(df, ts) is None for ts in [s_time, a_time, b_time, c_time]):
        return {"symbol": symbol, "signal_time": signal_time, "outcome": _trade_outcome(t), "feature_status": "missing_sabc_rows"}

    pre_df = _window_slice(df, max(0, s_time - pre_window_bars * 60_000), s_time)
    ab_df = _window_slice(df, a_time, b_time)
    bc_df = _window_slice(df, b_time, c_time)
    post_b_df = _window_slice(df, b_time, min(int(c_time + post_b_window_bars * 60_000), int(df.index.max())))

    sa_bars = max(0, int(round((a_time - s_time) / 60_000)))
    ab_bars = max(0, int(round((b_time - a_time) / 60_000)))
    bc_bars = max(0, int(round((c_time - b_time) / 60_000)))
    sc_bars = max(0, int(round((c_time - s_time) / 60_000)))

    sa_chg = (a_high - s_close) / s_close if s_close and s_close > 0 else None
    ab_drop_pct = (a_high - b_contract) / a_high if a_high and a_high > 0 else None
    ab_drop_pct_idx = (a_high - b_index) / a_high if a_high and a_high > 0 else None
    bc_rebound_pct_contract = (c_price - b_contract) / b_contract if b_contract and b_contract > 0 else None
    bc_rebound_pct_index = (c_price - b_index) / b_index if b_index and b_index > 0 else None
    sc_net_chg = (c_price - s_close) / s_close if s_close and s_close > 0 else None

    c_pos_in_ac_contract = _range_pos(c_price, b_contract, a_high)
    c_pos_in_ac_index = _range_pos(c_price, b_index, a_high)

    high_series_bc = bc_df["high"] if not bc_df.empty else pd.Series(dtype=float)
    low_series_bc = bc_df["low"] if not bc_df.empty else pd.Series(dtype=float)
    bc_peak_high = _safe_float(high_series_bc.max(), None) if len(high_series_bc) else None
    bc_low = _safe_float(low_series_bc.min(), None) if len(low_series_bc) else None
    c_vs_bc_peak = (c_price / bc_peak_high - 1.0) if bc_peak_high and bc_peak_high > 0 else None
    c_close_pos_in_bc_range = _range_pos(c_price, bc_low, bc_peak_high)

    pre_close = pre_df["close"] if not pre_df.empty else pd.Series(dtype=float)
    pre_high = pre_df["high"] if not pre_df.empty else pd.Series(dtype=float)
    pre_low = pre_df["low"] if not pre_df.empty else pd.Series(dtype=float)
    pre_idx_close = pre_df["close_idx"].dropna() if (not pre_df.empty and "close_idx" in pre_df.columns) else pd.Series(dtype=float)

    pre_trend_chg = None
    if len(pre_close) >= 2:
        first_pre, last_pre = _safe_float(pre_close.iloc[0], None), _safe_float(pre_close.iloc[-1], None)
        if first_pre and first_pre > 0 and last_pre is not None:
            pre_trend_chg = (last_pre - first_pre) / first_pre

    pre_idx_trend_chg = None
    if len(pre_idx_close) >= 2:
        first_idx, last_idx = _safe_float(pre_idx_close.iloc[0], None), _safe_float(pre_idx_close.iloc[-1], None)
        if first_idx and first_idx > 0 and last_idx is not None:
            pre_idx_trend_chg = (last_idx - first_idx) / first_idx

    pre_red_bar_ratio = float((pre_df["close"] < pre_df["open"]).sum()) / float(len(pre_df)) if not pre_df.empty else None

    ab_speed = (ab_drop_pct / ab_bars) if ab_drop_pct is not None and ab_bars > 0 else None
    bc_speed = (bc_rebound_pct_index / bc_bars) if bc_rebound_pct_index is not None and bc_bars > 0 else None
    sc_speed = (sc_net_chg / sc_bars) if sc_net_chg is not None and sc_bars > 0 else None
    speed_ratio_bc_over_ab = (bc_speed / ab_speed) if (bc_speed is not None and ab_speed not in (None, 0)) else None

    ab_vol_mean = _safe_float(ab_df["quote_asset_volume"].mean(), None) if not ab_df.empty else None
    bc_vol_mean = _safe_float(bc_df["quote_asset_volume"].mean(), None) if not bc_df.empty else None
    bc_over_ab_vol = (bc_vol_mean / ab_vol_mean) if ab_vol_mean and ab_vol_mean > 0 and bc_vol_mean is not None else None

    post_b_high = _safe_float(post_b_df["high"].max(), None) if not post_b_df.empty else None
    post_b_close_last = _safe_float(post_b_df["close"].iloc[-1], None) if not post_b_df.empty else None
    post_b_recover_ratio = None
    if post_b_high is not None and a_high is not None and b_index is not None and (a_high - b_index) > 0:
        post_b_recover_ratio = (post_b_high - b_index) / (a_high - b_index)
    post_b_close_ret = (post_b_close_last - b_index) / b_index if post_b_close_last is not None and b_index and b_index > 0 else None

    return {
        "symbol": symbol,
        "signal_time": int(signal_time),
        "signal_time_bj": _fmt_bj(signal_time),
        "entry_time": _safe_int(t.get("entry_time"), None),
        "entry_time_bj": _fmt_bj(_safe_int(t.get("entry_time"), None)),
        "exit_time": _safe_int(t.get("exit_time"), None),
        "exit_time_bj": _fmt_bj(_safe_int(t.get("exit_time"), None)),
        "outcome": _trade_outcome(t),
        "pnl_pct": _safe_float(t.get("pnl_pct"), None),
        "mfe_pct": _safe_float(t.get("mfe_pct"), None),
        "mae_pct": _safe_float(t.get("mae_pct"), None),
        "hold_mins": _safe_float(t.get("hold_minutes") or t.get("hold_mins"), None),
        "feature_status": "ok",
        "s_time": int(s_time), "a_time": int(a_time), "b_time": int(b_time), "c_time": int(c_time),
        "s_close": s_close, "a_high_price": a_high, "b_contract_price": b_contract, "b_index_price": b_index, "c_price": c_price, "c_index_price": c_index,
        "signal_tp_price": tp_price, "signal_sl_price": sl_price,
        "sa_bars": sa_bars, "ab_bars": ab_bars, "bc_bars": bc_bars, "sc_bars": sc_bars,
        "sa_chg": sa_chg, "ab_drop_pct_contract": ab_drop_pct, "ab_drop_pct_index": ab_drop_pct_idx,
        "bc_rebound_pct_contract": bc_rebound_pct_contract, "bc_rebound_pct_index": bc_rebound_pct_index, "sc_net_chg": sc_net_chg,
        "ab_vs_bc_bars_ratio": (ab_bars / bc_bars) if bc_bars > 0 else None, "bc_vs_sc_bars_ratio": (bc_bars / sc_bars) if sc_bars > 0 else None,
        "c_pos_in_ac_contract": c_pos_in_ac_contract, "c_pos_in_ac_index": c_pos_in_ac_index,
        "c_close_pos_in_bc_range": c_close_pos_in_bc_range, "c_vs_bc_peak": c_vs_bc_peak,
        "ab_drop_speed": ab_speed, "bc_rebound_speed": bc_speed, "sc_net_speed": sc_speed, "speed_ratio_bc_over_ab": speed_ratio_bc_over_ab,
        "drop_window_chg": _safe_float(ctx.get("drop_window_chg"), None), "vol_ratio": _safe_float(ctx.get("vol_ratio"), None),
        "rebound_ratio": _safe_float(ctx.get("rebound_ratio"), None), "basis_b_pct": _safe_float(ctx.get("basis_b_pct"), None), "basis_c_pct": _safe_float(ctx.get("basis_c_pct"), None),
        "ab_vol_mean": ab_vol_mean, "bc_vol_mean": bc_vol_mean, "bc_over_ab_vol_ratio": bc_over_ab_vol,
        "post_b_recover_ratio": post_b_recover_ratio, "post_b_close_ret": post_b_close_ret,
        "pre_window_bars": pre_window_bars, "pre_trend_chg": pre_trend_chg, "pre_idx_trend_chg": pre_idx_trend_chg,
        "pre_lower_high_count": _count_monotonic(pre_high, "down"), "pre_lower_low_count": _count_monotonic(pre_low, "down"), "pre_red_bar_ratio": pre_red_bar_ratio,
        "param_selected_take_profit_pct": _safe_float(params.get("selected_take_profit_pct"), None),
        "param_min_ab_bars": _safe_int(params.get("min_ab_bars"), None), "param_min_bc_bars": _safe_int(params.get("min_bc_bars"), None),
        "param_min_rebound_ratio": _safe_float(params.get("min_rebound_ratio"), None), "param_max_rebound_ratio": _safe_float(params.get("max_rebound_ratio"), None),
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Extract SABC morphology features from sim signals/trades")
    ap.add_argument("--signals", required=True)
    ap.add_argument("--trades", required=True)
    ap.add_argument("--data-dir", default="data/klines_1m")
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--out-jsonl", default="")
    ap.add_argument("--pre-window-bars", type=int, default=60)
    ap.add_argument("--post-b-window-bars", type=int, default=8)
    args = ap.parse_args()

    signals = _read_jsonl(args.signals)
    trades = _read_jsonl(args.trades)
    joined = _join_signals_trades(signals, trades)
    store = SymbolStore(args.data_dir)
    rows = [build_features(store, item, args.pre_window_bars, args.post_b_window_bars) for item in joined]
    df = pd.DataFrame(rows)

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)

    if args.out_jsonl:
        out_jsonl = Path(args.out_jsonl)
        out_jsonl.parent.mkdir(parents=True, exist_ok=True)
        with open(out_jsonl, "w", encoding="utf-8") as f:
            for row in rows:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    ok_count = int((df.get("feature_status") == "ok").sum()) if not df.empty and "feature_status" in df.columns else 0
    print("=== SABC morphology extract done ===")
    print(f"rows_total : {len(df)}")
    print(f"rows_ok    : {ok_count}")
    print(f"out_csv    : {out_csv}")
    if args.out_jsonl:
        print(f"out_jsonl  : {args.out_jsonl}")


if __name__ == "__main__":
    main()
