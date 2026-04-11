#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pyarrow.parquet as pq

BJ_OFFSET_HOURS = 8
EPS = 1e-9


# -----------------------------
# basic io helpers
# -----------------------------

def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None:
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
    return (
        pd.to_datetime(int(ts_ms), unit="ms", utc=True)
        + pd.Timedelta(hours=BJ_OFFSET_HOURS)
    ).strftime("%Y-%m-%d %H:%M:%S")


def _read_jsonl_lines(lines: Iterable[bytes | str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for line in lines:
        s = line.decode("utf-8") if isinstance(line, bytes) else str(line)
        s = s.strip()
        if s:
            rows.append(json.loads(s))
    return rows


def _load_from_zip(bundle_zip: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any], str]:
    zpath = Path(bundle_zip)
    if not zpath.exists():
        raise FileNotFoundError(f"bundle zip not found: {bundle_zip}")

    signals_name = None
    trades_name = None
    summary_name = None
    with zipfile.ZipFile(zpath, "r") as zf:
        names = [n for n in zf.namelist() if not n.endswith("/") and "__MACOSX" not in n and not Path(n).name.startswith("._")]
        for name in names:
            base = Path(name).name
            if base.startswith("sim_signals.") and base.endswith(".jsonl"):
                signals_name = name
            elif base.startswith("sim_trades.") and base.endswith(".jsonl"):
                trades_name = name
            elif base.startswith("sim_summary.") and base.endswith(".json"):
                summary_name = name
        if not signals_name or not trades_name or not summary_name:
            raise FileNotFoundError("bundle zip missing sim_signals / sim_trades / sim_summary")

        with zf.open(signals_name) as fp:
            signals = _read_jsonl_lines(fp)
        with zf.open(trades_name) as fp:
            trades = _read_jsonl_lines(fp)
        with zf.open(summary_name) as fp:
            summary = json.load(fp)

    run_id = Path(trades_name).name.replace("sim_trades.", "").replace(".jsonl", "")
    return signals, trades, summary, run_id


def _load_explicit(signals_path: str, trades_path: str, summary_path: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any], str]:
    with open(signals_path, "r", encoding="utf-8") as f:
        signals = _read_jsonl_lines(f)
    with open(trades_path, "r", encoding="utf-8") as f:
        trades = _read_jsonl_lines(f)
    with open(summary_path, "r", encoding="utf-8") as f:
        summary = json.load(f)
    run_id = Path(trades_path).name.replace("sim_trades.", "").replace(".jsonl", "")
    return signals, trades, summary, run_id


# -----------------------------
# store
# -----------------------------

class SymbolStore:
    def __init__(self, data_dir: str, required_cols: List[str]):
        self.data_dir = Path(data_dir)
        self.required_cols = list(required_cols)
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
        for col in self.required_cols:
            if col not in df.columns:
                raise KeyError(f"symbol={sym} parquet missing required column: {col}")
        self._cache[sym] = df
        return df


@dataclass
class JoinedTrade:
    signal: Dict[str, Any]
    trade: Dict[str, Any]


def _trade_outcome_strict(trade: Dict[str, Any]) -> str:
    if "reason" not in trade:
        raise KeyError("trade missing required field: reason")
    reason = str(trade["reason"]).upper().strip()
    if reason == "TAKE_PROFIT":
        return "TP"
    if reason == "STOP_LOSS":
        return "SL"
    if reason == "TIMEOUT":
        return "TS"
    return reason or "UNKNOWN"


def _join_signals_trades_strict(signals: List[Dict[str, Any]], trades: List[Dict[str, Any]]) -> List[JoinedTrade]:
    trade_map: Dict[Tuple[str, int], List[Dict[str, Any]]] = {}
    for t in trades:
        symbol = str(t.get("symbol", "")).upper().strip()
        signal_time = _safe_int(t.get("signal_time"), None)
        if not symbol or signal_time is None:
            raise ValueError("trade row missing valid symbol/signal_time")
        trade_map.setdefault((symbol, signal_time), []).append(t)

    joined: List[JoinedTrade] = []
    for s in signals:
        symbol = str(s.get("symbol", "")).upper().strip()
        signal_time = _safe_int(s.get("signal_time"), None)
        if not symbol or signal_time is None:
            raise ValueError("signal row missing valid symbol/signal_time")
        rows = trade_map.get((symbol, signal_time)) or []
        if not rows:
            raise KeyError(f"no matching trade for signal: symbol={symbol} signal_time={signal_time}")
        joined.append(JoinedTrade(signal=s, trade=rows.pop(0)))
    return joined


# -----------------------------
# feature primitives
# -----------------------------

def _window_slice(df: pd.DataFrame, start_ms: int, end_ms: int) -> pd.DataFrame:
    return df.loc[(df.index >= int(start_ms)) & (df.index <= int(end_ms))].copy()


def _extract_row(df: pd.DataFrame, ts_ms: int) -> Optional[pd.Series]:
    try:
        if int(ts_ms) not in df.index:
            return None
        row = df.loc[int(ts_ms)]
        return row.iloc[-1] if isinstance(row, pd.DataFrame) else row
    except Exception:
        return None


def _third_bucket(pos01: Optional[float]) -> Optional[str]:
    if pos01 is None:
        return None
    if pos01 < 1.0 / 3.0:
        return "front"
    if pos01 < 2.0 / 3.0:
        return "mid"
    return "back"


def _safe_ratio(a: Optional[float], b: Optional[float]) -> Optional[float]:
    if a is None or b is None or abs(b) <= EPS:
        return None
    return a / b


def _build_anchor_close_seq(ab_df: pd.DataFrame, a_high_price: float, b_contract_price: float) -> List[float]:
    closes = [float(v) for v in ab_df["close"].tolist()] if not ab_df.empty else []
    seq = [float(a_high_price)] + closes + [float(b_contract_price)]
    out: List[float] = []
    for x in seq:
        if not out or abs(out[-1] - x) > EPS:
            out.append(x)
    return out


def _path_length(seq: List[float]) -> Optional[float]:
    if len(seq) < 2:
        return None
    return sum(abs(seq[i] - seq[i - 1]) for i in range(1, len(seq)))


def _ab_path_efficiency(a_high_price: float, b_contract_price: float, seq: List[float]) -> Optional[float]:
    path_len = _path_length(seq)
    if path_len is None or path_len <= EPS:
        return None
    net_displacement = abs(float(a_high_price) - float(b_contract_price))
    return net_displacement / path_len


def _zigzag_pivots(seq: List[float], pivot_abs: float) -> List[float]:
    if not seq:
        return []
    pts = [float(x) for x in seq]
    if len(pts) == 1 or pivot_abs <= EPS:
        return pts[:]

    pivots: List[float] = [pts[0]]
    candidate = pts[0]
    direction = 0  # 1=up, -1=down, 0=unknown

    for p in pts[1:]:
        p = float(p)
        if direction >= 0:
            if p >= candidate:
                candidate = p
                pivots[-1] = p
            elif (candidate - p) >= pivot_abs:
                direction = -1
                candidate = p
                pivots.append(p)
        if direction <= 0:
            if p <= candidate:
                candidate = p
                pivots[-1] = p
            elif (p - candidate) >= pivot_abs:
                direction = 1
                candidate = p
                pivots.append(p)

    out: List[float] = []
    for x in pivots:
        if not out or abs(out[-1] - x) > EPS:
            out.append(x)
    return out


def _ab_step_drop_count(a_high_price: float, b_contract_price: float, seq: List[float]) -> Optional[int]:
    if len(seq) < 2:
        return None
    total_drop = max(0.0, float(a_high_price) - float(b_contract_price))
    if total_drop <= EPS:
        return 0

    pivot_abs = max(total_drop * 0.055, float(a_high_price) * 0.0007)
    leg_min_abs = max(total_drop * 0.16, float(a_high_price) * 0.0013)
    recover_min_abs = max(total_drop * 0.11, float(a_high_price) * 0.0011)
    rebreak_min_abs = max(total_drop * 0.035, float(a_high_price) * 0.0006)

    pivots = _zigzag_pivots(seq, pivot_abs)
    if len(pivots) < 2:
        return 0

    steps = 0
    last_leg_low: Optional[float] = None

    for prev, curr in zip(pivots[:-1], pivots[1:]):
        prev = float(prev)
        curr = float(curr)

        if prev <= curr:
            continue
        leg_drop_abs = prev - curr
        if leg_drop_abs < leg_min_abs:
            if last_leg_low is None or curr < last_leg_low:
                last_leg_low = curr
            continue

        if steps == 0:
            steps = 1
            last_leg_low = curr
            continue

        recovery_abs = (prev - last_leg_low) if last_leg_low is not None else 0.0
        rebreak_abs = (last_leg_low - curr) if last_leg_low is not None else 0.0
        if recovery_abs >= recover_min_abs and rebreak_abs >= rebreak_min_abs:
            steps += 1
            last_leg_low = curr
        else:
            if last_leg_low is None or curr < last_leg_low:
                last_leg_low = curr

    return int(steps)


def _a_peak_sharpness(df: pd.DataFrame, s_time: int, a_time: int, b_time: int, a_high_price: float, flank_bars: int = 3) -> Optional[float]:
    left_start = max(int(s_time), int(a_time) - flank_bars * 60_000)
    left_end = int(a_time) - 60_000
    right_start = int(a_time) + 60_000
    right_end = min(int(b_time), int(a_time) + flank_bars * 60_000)
    left_df = _window_slice(df, left_start, left_end) if left_end >= left_start else pd.DataFrame()
    right_df = _window_slice(df, right_start, right_end) if right_end >= right_start else pd.DataFrame()
    if left_df.empty or right_df.empty:
        return None
    left_max = _safe_float(left_df["high"].max(), None)
    right_max = _safe_float(right_df["high"].max(), None)
    if left_max is None or right_max is None or a_high_price <= 0:
        return None
    left_gap = max(0.0, (a_high_price - left_max) / a_high_price)
    right_gap = max(0.0, (a_high_price - right_max) / a_high_price)
    return (left_gap + right_gap) / 2.0


def _ab_peak_vol_position(ab_df: pd.DataFrame) -> Tuple[Optional[float], Optional[str]]:
    if ab_df.empty or "quote_asset_volume" not in ab_df.columns:
        return None, None
    vols = ab_df["quote_asset_volume"].astype(float)
    if vols.empty or vols.isna().all():
        return None, None
    idx = int(vols.idxmax())
    bars = list(ab_df.index)
    if idx not in ab_df.index or len(bars) == 1:
        return None, None
    pos = bars.index(idx) / max(len(bars) - 1, 1)
    return float(pos), _third_bucket(float(pos))


def _ab_pullback_stats(pivots: List[float], total_drop: float) -> Tuple[int, Optional[float]]:
    if len(pivots) < 2 or total_drop <= EPS:
        return 0, None
    pullback_count = 0
    pullback_sum = 0.0
    for prev, curr in zip(pivots[:-1], pivots[1:]):
        prev = float(prev)
        curr = float(curr)
        if curr > prev:
            pullback_count += 1
            pullback_sum += (curr - prev)
    return int(pullback_count), (pullback_sum / total_drop) if total_drop > EPS else None


def _ab_path_type(
    ab_path_efficiency: Optional[float],
    ab_step_drop_count: Optional[int],
    ab_pullback_count: int,
    ab_pullback_share: Optional[float],
) -> Optional[str]:
    if ab_path_efficiency is None:
        return None
    step = int(ab_step_drop_count or 0)
    pullback_share = 0.0 if ab_pullback_share is None else float(ab_pullback_share)

    if step >= 2:
        if ab_path_efficiency >= 0.78 and pullback_share <= 0.18:
            return "clean_two_leg"
        return "staircase_two_leg"

    if ab_path_efficiency < 0.55:
        return "messy_one_leg"
    if ab_path_efficiency >= 0.90 and ab_pullback_count <= 1 and pullback_share <= 0.12:
        return "flush_one_leg"
    if ab_path_efficiency >= 0.72:
        return "clean_one_leg"
    return "structured_one_leg"


# -----------------------------
# extraction
# -----------------------------

def build_sab_row(contract_store: SymbolStore, item: JoinedTrade, run_id: str, summary: Dict[str, Any]) -> Dict[str, Any]:
    s = item.signal
    t = item.trade
    ctx = dict(s.get("context") or {})

    required_signal = ["symbol", "signal_time", "context", "current_price", "tp_price", "sl_price"]
    required_trade = ["entry_time", "exit_time", "entry_price", "exit_price", "pnl_pct", "reason"]
    required_ctx = ["s_time", "s_close", "a_time", "a_high_price", "b_time", "b_contract_price", "b_index_price", "c_time", "c_price", "ab_bars", "drop_pct", "ab_drop_pct_index"]
    for field in required_signal:
        if field not in s:
            raise KeyError(f"signal missing required field: {field}")
    for field in required_trade:
        if field not in t:
            raise KeyError(f"trade missing required field: {field}")
    for field in required_ctx:
        if field not in ctx:
            raise KeyError(f"signal.context missing required field: {field}")

    symbol = str(s["symbol"]).upper().strip()
    signal_time = _safe_int(s["signal_time"], None)
    entry_time = _safe_int(t["entry_time"], None)
    exit_time = _safe_int(t["exit_time"], None)
    s_time = _safe_int(ctx["s_time"], None)
    a_time = _safe_int(ctx["a_time"], None)
    b_time = _safe_int(ctx["b_time"], None)
    c_time = _safe_int(ctx["c_time"], None)
    s_close = _safe_float(ctx["s_close"], None)
    a_high_price = _safe_float(ctx["a_high_price"], None)
    b_contract_price = _safe_float(ctx["b_contract_price"], None)
    b_index_price = _safe_float(ctx["b_index_price"], None)
    drop_pct = _safe_float(ctx["drop_pct"], None)
    ab_drop_pct_index = _safe_float(ctx["ab_drop_pct_index"], None)
    for v in [signal_time, entry_time, exit_time, s_time, a_time, b_time, c_time, s_close, a_high_price, b_contract_price, b_index_price, drop_pct, ab_drop_pct_index]:
        if v is None:
            raise ValueError(f"core parse failed: symbol={symbol} signal_time={signal_time}")

    df = contract_store.get(symbol)
    if df is None or df.empty:
        raise FileNotFoundError(f"contract parquet missing: {symbol}")
    for ts in [s_time, a_time, b_time, c_time]:
        if _extract_row(df, ts) is None:
            raise KeyError(f"symbol={symbol} missing one of S/A/B/C rows in contract parquet")

    sa_bars = max(0, int(round((a_time - s_time) / 60_000)))
    ab_bars = max(0, int(round((b_time - a_time) / 60_000)))
    sa_chg_pct = ((a_high_price - s_close) / s_close) if s_close > 0 else None
    ab_vs_sa_amp_ratio = _safe_ratio(ab_drop_pct_index, abs(sa_chg_pct) if sa_chg_pct is not None else None)
    ab_drop_speed = _safe_ratio(ab_drop_pct_index, ab_bars if ab_bars > 0 else None)

    ab_df = _window_slice(df, a_time, b_time)
    seq = _build_anchor_close_seq(ab_df, a_high_price, b_contract_price)
    total_drop = max(0.0, float(a_high_price) - float(b_contract_price))
    pivots = _zigzag_pivots(seq, max(total_drop * 0.055, float(a_high_price) * 0.0007)) if total_drop > EPS else seq[:]
    ab_path_efficiency = _ab_path_efficiency(a_high_price, b_contract_price, seq)
    ab_step_drop_count = _ab_step_drop_count(a_high_price, b_contract_price, seq)
    ab_pullback_count, ab_pullback_share = _ab_pullback_stats(pivots, total_drop)
    ab_path_type = _ab_path_type(ab_path_efficiency, ab_step_drop_count, ab_pullback_count, ab_pullback_share)
    a_peak_sharpness = _a_peak_sharpness(df, s_time, a_time, b_time, a_high_price, flank_bars=3)
    ab_peak_vol_pos01, ab_peak_vol_position = _ab_peak_vol_position(ab_df)

    row: Dict[str, Any] = {
        "run_id": run_id,
        "symbol": symbol,
        "signal_time": int(signal_time),
        "signal_time_bj": _fmt_bj(signal_time),
        "entry_time": int(entry_time),
        "entry_time_bj": _fmt_bj(entry_time),
        "exit_time": int(exit_time),
        "exit_time_bj": _fmt_bj(exit_time),
        "outcome": _trade_outcome_strict(t),
        "pnl_pct": _safe_float(t.get("pnl_pct"), None),
        "hold_mins": (exit_time - entry_time) / 60000.0,
        "feature_scope": "hb_sab_only",
        "feature_status": "ok",
        # anchors
        "s_time": int(s_time),
        "a_time": int(a_time),
        "b_time": int(b_time),
        "c_time": int(c_time),
        "s_close": s_close,
        "a_high_price": a_high_price,
        "b_contract_price": b_contract_price,
        "b_index_price": b_index_price,
        # first-batch SAB fingerprints
        "sa_bars": sa_bars,
        "ab_bars": ab_bars,
        "sa_chg_pct": sa_chg_pct,
        "ab_drop_pct_index": ab_drop_pct_index,
        "ab_vs_sa_amp_ratio": ab_vs_sa_amp_ratio,
        "ab_drop_speed": ab_drop_speed,
        "ab_path_efficiency": ab_path_efficiency,
        "ab_step_drop_count": ab_step_drop_count,
        "ab_pullback_count": ab_pullback_count,
        "ab_pullback_share": ab_pullback_share,
        "ab_path_type": ab_path_type,
        "a_peak_sharpness": a_peak_sharpness,
        "ab_peak_vol_pos01": ab_peak_vol_pos01,
        "ab_peak_vol_position": ab_peak_vol_position,
        # carry-through audit context
        "drop_pct": drop_pct,
        "drop_window_chg": _safe_float(ctx.get("drop_window_chg"), None),
        "vol_ratio": _safe_float(ctx.get("vol_ratio"), None),
        "rebound_ratio": _safe_float(ctx.get("rebound_ratio"), None),
        "basis_b_pct": _safe_float(ctx.get("basis_b_pct"), None),
        "summary_trade_count": summary.get("trade_count"),
        "summary_final_equity_simple_net_usdt": summary.get("final_equity_simple_net_usdt"),
    }
    return row


def main() -> None:
    ap = argparse.ArgumentParser(description="Extract first-batch SAB fingerprints using HBs only")
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--bundle-zip", help="zip containing sim_summary / sim_signals / sim_trades")
    src.add_argument("--signals", help="sim_signals jsonl path")
    ap.add_argument("--trades", default="", help="sim_trades jsonl path (required with --signals)")
    ap.add_argument("--summary", default="", help="sim_summary json path (required with --signals)")
    ap.add_argument("--data-dir", default="data/klines_1m")
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--out-jsonl", default="")
    args = ap.parse_args()

    if args.bundle_zip:
        signals, trades, summary, run_id = _load_from_zip(args.bundle_zip)
    else:
        if not args.trades or not args.summary:
            raise SystemExit("--signals mode requires --trades and --summary")
        signals, trades, summary, run_id = _load_explicit(args.signals, args.trades, args.summary)

    joined = _join_signals_trades_strict(signals, trades)
    contract_store = SymbolStore(args.data_dir, ["open", "high", "low", "close", "quote_asset_volume"])
    rows = [build_sab_row(contract_store, item, run_id, summary) for item in joined]
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

    print("=== SAB fingerprint extract v1 done ===")
    print(f"run_id       : {run_id}")
    print(f"rows_total   : {len(df)}")
    print(f"rows_ok      : {int((df['feature_status'] == 'ok').sum())}")
    print(f"out_csv      : {out_csv}")
    if args.out_jsonl:
        print(f"out_jsonl    : {args.out_jsonl}")


if __name__ == "__main__":
    main()
