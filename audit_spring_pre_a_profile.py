#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

BJ_OFFSET_MS = 8 * 60 * 60 * 1000
MINUTE_MS = 60 * 1000


def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None:
            return default
        if isinstance(v, str) and not v.strip():
            return default
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return default
        return x
    except Exception:
        return default


def _safe_int(v: Any, default: Optional[int] = None) -> Optional[int]:
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


def _bj_text(ts_ms: Any) -> str:
    ts = _safe_int(ts_ms, None)
    if ts is None:
        return ""
    return pd.to_datetime(ts + BJ_OFFSET_MS, unit="ms").strftime("%Y-%m-%d %H:%M")


def _month_bj(ts_ms: Any) -> str:
    ts = _safe_int(ts_ms, None)
    if ts is None:
        return ""
    return pd.to_datetime(ts + BJ_OFFSET_MS, unit="ms").strftime("%Y-%m")


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def _normalize_symbol_df(df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    if isinstance(df.index, pd.MultiIndex):
        if "symbol" in df.index.names:
            try:
                df = df.xs(symbol, level="symbol")
            except Exception:
                pass
        if isinstance(df.index, pd.MultiIndex):
            df = df.reset_index()
    else:
        has_ts_col = any(c in df.columns for c in ["open_time_ms", "open_time", "timestamp", "ts", "time"])
        df = df.copy() if has_ts_col else df.reset_index()

    ts_col = None
    for c in ["open_time_ms", "open_time", "timestamp", "ts", "time"]:
        if c in df.columns:
            ts_col = c
            break
    if ts_col is None:
        for c in df.columns:
            if str(c).lower() in {"index", "datetime"}:
                ts_col = c
                break
    if ts_col is None:
        return pd.DataFrame()

    out = df.copy()
    if pd.api.types.is_datetime64_any_dtype(out[ts_col]):
        out["open_time_ms"] = (pd.to_datetime(out[ts_col], utc=True).astype("int64") // 10**6).astype("int64")
    else:
        out["open_time_ms"] = pd.to_numeric(out[ts_col], errors="coerce").astype("Int64")
    out = out.dropna(subset=["open_time_ms"]).copy()
    out["open_time_ms"] = out["open_time_ms"].astype("int64")
    return out.drop_duplicates("open_time_ms").sort_values("open_time_ms").set_index("open_time_ms")


def _load_symbol_df(kline_root: Path, symbol: str) -> pd.DataFrame:
    # Current project layout:
    #   data/klines_1m/{SYMBOL}/{YYYY-MM}.parquet
    symbol_dir = kline_root / symbol
    if symbol_dir.exists() and symbol_dir.is_dir():
        monthly_paths = sorted(symbol_dir.glob("*.parquet"))
        if monthly_paths:
            frames = []
            for path in monthly_paths:
                try:
                    frames.append(pd.read_parquet(path))
                except Exception:
                    continue
            if frames:
                return _normalize_symbol_df(pd.concat(frames, ignore_index=True), symbol)

    # Defensive compatibility for one-file layouts used by ad-hoc exports.
    candidates = [
        kline_root / f"{symbol}.parquet",
        kline_root / symbol / "1m.parquet",
        kline_root / symbol / f"{symbol}.parquet",
    ]
    path = next((p for p in candidates if p.exists()), None)
    if path is None:
        matches = list(kline_root.rglob(f"{symbol}*.parquet"))
        path = matches[0] if matches else None
    if path is None:
        return pd.DataFrame()
    try:
        return _normalize_symbol_df(pd.read_parquet(path), symbol)
    except Exception:
        return pd.DataFrame()


def _sum_col(df: pd.DataFrame, preferred: List[str]) -> float:
    for col in preferred:
        if col in df.columns:
            return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
    return 0.0


def _mean_col(df: pd.DataFrame, col: str) -> Optional[float]:
    if col not in df.columns or df.empty:
        return None
    s = pd.to_numeric(df[col], errors="coerce").dropna()
    if s.empty:
        return None
    return float(s.mean())


def _bar_count_if(df: pd.DataFrame, expr: str) -> int:
    if df.empty or "close" not in df.columns or "open" not in df.columns:
        return 0
    o = pd.to_numeric(df["open"], errors="coerce")
    c = pd.to_numeric(df["close"], errors="coerce")
    if expr == "up":
        return int((c > o).sum())
    if expr == "down":
        return int((c < o).sum())
    return int((c == o).sum())


def _features_for_trade(trade: Dict[str, Any], symbol_df: pd.DataFrame) -> Dict[str, Any]:
    ctx = dict(trade.get("context") or {})
    symbol = str(trade.get("symbol") or "")
    signal_time = _safe_int(trade.get("signal_time"), None)
    entry_time = _safe_int(trade.get("entry_time"), None)
    exit_time = _safe_int(trade.get("exit_time"), None)
    a_time = _safe_int(ctx.get("a_time_ms"), None)
    c_time = _safe_int(ctx.get("c_time_ms"), None)
    pattern_window_bars = _safe_int(ctx.get("pattern_window_bars"), 60) or 60

    if a_time is None and signal_time is not None:
        # Fallback only for defensive auditing; normal Spring trades should have a_time_ms.
        a_time = signal_time - MINUTE_MS
    s_time = (c_time - max(0, pattern_window_bars - 1) * MINUTE_MS) if c_time is not None else None
    if s_time is None or a_time is None or symbol_df.empty:
        pre = pd.DataFrame()
    else:
        pre = symbol_df[(symbol_df.index >= int(s_time)) & (symbol_df.index <= int(a_time))].copy()

    first = pre.iloc[0] if not pre.empty else None
    last = pre.iloc[-1] if not pre.empty else None
    high = _safe_float(pd.to_numeric(pre["high"], errors="coerce").max(), None) if "high" in pre.columns and not pre.empty else None
    low = _safe_float(pd.to_numeric(pre["low"], errors="coerce").min(), None) if "low" in pre.columns and not pre.empty else None
    start_close = _safe_float(first.get("close"), None) if first is not None else None
    end_close = _safe_float(last.get("close"), None) if last is not None else None
    a_close = _safe_float(ctx.get("a_close"), end_close)
    a_high = _safe_float(ctx.get("a_high"), None)
    a_low = None
    if a_time is not None and a_time in symbol_df.index and "low" in symbol_df.columns:
        a_low = _safe_float(symbol_df.loc[a_time].get("low"), None)

    pre_chg_pct = ((end_close / start_close) - 1.0) if start_close and end_close and start_close > 0 else None
    pre_range_pct = ((high / low) - 1.0) if high and low and low > 0 else None
    pre_high_to_a_close_pct = ((high / a_close) - 1.0) if high and a_close and a_close > 0 else None
    pre_a_close_pos_in_range = ((a_close - low) / (high - low)) if a_close is not None and high is not None and low is not None and high > low else None
    pre_a_high_pos_in_range = ((a_high - low) / (high - low)) if a_high is not None and high is not None and low is not None and high > low else None

    quote_vol = _sum_col(pre, ["quote_asset_volume", "quote_volume", "quote_vol"])
    base_vol = _sum_col(pre, ["volume", "base_volume"])
    bars = int(len(pre))
    up_bars = _bar_count_if(pre, "up")
    down_bars = _bar_count_if(pre, "down")
    flat_bars = max(0, bars - up_bars - down_bars)

    pnl_pct = _safe_float(trade.get("pnl_pct"), 0.0) or 0.0
    entry_price = _safe_float(trade.get("entry_price"), None)
    sl_price = _safe_float(trade.get("context", {}).get("stop_loss_price"), _safe_float(trade.get("sl_price"), None))
    risk_pct = ((entry_price - sl_price) / entry_price) if entry_price and sl_price and entry_price > 0 else _safe_float(ctx.get("risk_pct"), None)

    return {
        "symbol": symbol,
        "reason": str(trade.get("reason") or ""),
        "is_win": int(pnl_pct > 0),
        "is_loss": int(pnl_pct < 0),
        "pnl_pct": pnl_pct,
        "pnl_u_100": pnl_pct * 100.0,
        "signal_time": signal_time,
        "signal_time_bj": trade.get("signal_time_bj") or _bj_text(signal_time),
        "entry_time_bj": trade.get("entry_time_bj") or _bj_text(entry_time),
        "exit_time_bj": trade.get("exit_time_bj") or _bj_text(exit_time),
        "month_bj": _month_bj(exit_time),
        "s_time_bj": _bj_text(s_time),
        "a_time_bj": _bj_text(a_time),
        "pre_a_bars": bars,
        "pre_a_start_close": start_close,
        "pre_a_end_close": end_close,
        "pre_a_high": high,
        "pre_a_low": low,
        "pre_a_chg_pct": pre_chg_pct,
        "pre_a_range_pct": pre_range_pct,
        "pre_a_high_to_a_close_pct": pre_high_to_a_close_pct,
        "pre_a_close_pos_in_range": pre_a_close_pos_in_range,
        "pre_a_high_pos_in_range": pre_a_high_pos_in_range,
        "pre_a_quote_vol": quote_vol,
        "pre_a_base_vol": base_vol,
        "pre_a_avg_quote_vol_per_bar": quote_vol / bars if bars else None,
        "pre_a_up_bars": up_bars,
        "pre_a_down_bars": down_bars,
        "pre_a_flat_bars": flat_bars,
        "pre_a_up_ratio": up_bars / bars if bars else None,
        "pre_a_down_ratio": down_bars / bars if bars else None,
        "score_order": _safe_int(ctx.get("score_order"), None),
        "score": _safe_int(ctx.get("score"), None),
        "chg_24h": _safe_float(ctx.get("chg_24h"), None),
        "vol_24h": _safe_float(ctx.get("vol_24h"), None),
        "ab_bars": _safe_int(ctx.get("ab_bars"), None),
        "bc_bars": _safe_int(ctx.get("bc_bars"), None),
        "bc_over_ab_bars": _safe_float(ctx.get("bc_over_ab_bars"), None),
        "ab_chg_pct": _safe_float(ctx.get("ab_chg_pct"), None),
        "rebound_ratio": _safe_float(ctx.get("rebound_ratio"), None),
        "vol_ratio": _safe_float(ctx.get("vol_ratio"), None),
        "risk_pct": risk_pct,
    }


def _bucket(v: Any, cuts: List[float], labels: List[str], default: str = "NA") -> str:
    fv = _safe_float(v, None)
    if fv is None:
        return default
    for cut, label in zip(cuts, labels):
        if fv < cut:
            return label
    return labels[-1]


def _add_buckets(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["pnl_bucket"] = out["pnl_pct"].apply(lambda x: _bucket(x, [-0.10, -0.05, 0.0, 0.05, 0.10], ["<-10%", "-10~-5%", "-5~0%", "0~5%", "5~10%", ">=10%"] ))
    out["pre_a_chg_bucket"] = out["pre_a_chg_pct"].apply(lambda x: _bucket(x, [-0.05, 0.0, 0.05, 0.10, 0.20], ["<-5%", "-5~0%", "0~5%", "5~10%", "10~20%", ">=20%"] ))
    out["pre_a_range_bucket"] = out["pre_a_range_pct"].apply(lambda x: _bucket(x, [0.05, 0.10, 0.20, 0.35], ["<5%", "5~10%", "10~20%", "20~35%", ">=35%"] ))
    out["pre_a_high_to_a_close_bucket"] = out["pre_a_high_to_a_close_pct"].apply(lambda x: _bucket(x, [0.03, 0.06, 0.10, 0.20], ["<3%", "3~6%", "6~10%", "10~20%", ">=20%"] ))
    out["pre_a_close_pos_bucket"] = out["pre_a_close_pos_in_range"].apply(lambda x: _bucket(x, [0.25, 0.50, 0.75], ["low_quarter", "lower_mid", "upper_mid", "high_quarter"] ))
    out["risk_bucket"] = out["risk_pct"].apply(lambda x: _bucket(x, [0.05, 0.08, 0.12], ["<5%", "5~8%", "8~12%", ">=12%"] ))
    return out


def _group_summary(df: pd.DataFrame, by: List[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    g = df.groupby(by, dropna=False)
    rows = []
    for key, sub in g:
        if not isinstance(key, tuple):
            key = (key,)
        item = {name: val for name, val in zip(by, key)}
        item.update({
            "trade_count": int(len(sub)),
            "win_count": int((sub["pnl_pct"] > 0).sum()),
            "loss_count": int((sub["pnl_pct"] < 0).sum()),
            "win_rate_pct": round(float((sub["pnl_pct"] > 0).mean() * 100.0), 2) if len(sub) else 0.0,
            "pnl_u_100_sum": round(float(sub["pnl_u_100"].sum()), 6),
            "avg_pnl_pct": round(float(sub["pnl_pct"].mean() * 100.0), 6),
            "avg_pre_a_chg_pct": round(float(sub["pre_a_chg_pct"].mean() * 100.0), 6) if sub["pre_a_chg_pct"].notna().any() else None,
            "avg_pre_a_range_pct": round(float(sub["pre_a_range_pct"].mean() * 100.0), 6) if sub["pre_a_range_pct"].notna().any() else None,
            "avg_pre_a_high_to_a_close_pct": round(float(sub["pre_a_high_to_a_close_pct"].mean() * 100.0), 6) if sub["pre_a_high_to_a_close_pct"].notna().any() else None,
            "avg_pre_a_close_pos": round(float(sub["pre_a_close_pos_in_range"].mean()), 6) if sub["pre_a_close_pos_in_range"].notna().any() else None,
            "avg_pre_a_up_ratio": round(float(sub["pre_a_up_ratio"].mean()), 6) if sub["pre_a_up_ratio"].notna().any() else None,
            "avg_risk_pct": round(float(sub["risk_pct"].mean() * 100.0), 6) if sub["risk_pct"].notna().any() else None,
            "avg_score_order": round(float(sub["score_order"].mean()), 6) if sub["score_order"].notna().any() else None,
            "avg_vol_ratio": round(float(sub["vol_ratio"].mean()), 6) if sub["vol_ratio"].notna().any() else None,
        })
        rows.append(item)
    out = pd.DataFrame(rows)
    return out.sort_values(by + ["trade_count"], ascending=[True] * len(by) + [False])


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit Spring-SABC pre-A / S-A background profile from sim trades.")
    ap.add_argument("--trades", required=True, help="Path to sim_trades.<RUN_ID>.jsonl")
    ap.add_argument("--kline-root", default="data/klines_1m", help="Root directory of 1m kline parquet files")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--top-n", type=int, default=30, help="Top wins/losses row count")
    args = ap.parse_args()

    trades_path = Path(args.trades)
    kline_root = Path(args.kline_root)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    trades = _read_jsonl(trades_path)
    symbol_cache: Dict[str, pd.DataFrame] = {}
    rows: List[Dict[str, Any]] = []
    missing_symbols = []
    for trade in trades:
        symbol = str(trade.get("symbol") or "")
        if symbol not in symbol_cache:
            symbol_cache[symbol] = _load_symbol_df(kline_root, symbol)
            if symbol_cache[symbol].empty:
                missing_symbols.append(symbol)
        rows.append(_features_for_trade(trade, symbol_cache[symbol]))

    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("no trades loaded")
    df = _add_buckets(df)

    df.to_csv(out_dir / "trades_with_pre_a_features.csv", index=False)
    df.sort_values("pnl_pct", ascending=True).head(args.top_n).to_csv(out_dir / "pre_a_top_losses.csv", index=False)
    df.sort_values("pnl_pct", ascending=False).head(args.top_n).to_csv(out_dir / "pre_a_top_wins.csv", index=False)

    groups = {
        "pre_a_summary_by_reason.csv": ["reason"],
        "pre_a_summary_by_pnl_bucket.csv": ["pnl_bucket"],
        "pre_a_summary_by_month.csv": ["month_bj"],
        "pre_a_summary_by_score_order.csv": ["score_order"],
        "pre_a_summary_by_pre_a_chg_bucket.csv": ["pre_a_chg_bucket"],
        "pre_a_summary_by_pre_a_range_bucket.csv": ["pre_a_range_bucket"],
        "pre_a_summary_by_pre_a_high_to_a_close_bucket.csv": ["pre_a_high_to_a_close_bucket"],
        "pre_a_summary_by_pre_a_close_pos_bucket.csv": ["pre_a_close_pos_bucket"],
        "pre_a_summary_by_risk_bucket.csv": ["risk_bucket"],
        "pre_a_summary_by_reason_and_pre_a_chg_bucket.csv": ["reason", "pre_a_chg_bucket"],
        "pre_a_summary_by_reason_and_pre_a_range_bucket.csv": ["reason", "pre_a_range_bucket"],
    }
    for filename, by in groups.items():
        _group_summary(df, by).to_csv(out_dir / filename, index=False)

    summary = {
        "trades_path": str(trades_path),
        "kline_root": str(kline_root),
        "trade_count": int(len(df)),
        "symbol_count": int(df["symbol"].nunique()),
        "missing_symbol_count": int(len(set(missing_symbols))),
        "missing_symbols": sorted(set(missing_symbols)),
        "win_count": int((df["pnl_pct"] > 0).sum()),
        "loss_count": int((df["pnl_pct"] < 0).sum()),
        "win_rate_pct": round(float((df["pnl_pct"] > 0).mean() * 100.0), 2),
        "gross_pnl_u_100": round(float(df["pnl_u_100"].sum()), 6),
        "avg_pre_a_chg_pct": round(float(df["pre_a_chg_pct"].mean() * 100.0), 6) if df["pre_a_chg_pct"].notna().any() else None,
        "avg_pre_a_range_pct": round(float(df["pre_a_range_pct"].mean() * 100.0), 6) if df["pre_a_range_pct"].notna().any() else None,
        "avg_pre_a_high_to_a_close_pct": round(float(df["pre_a_high_to_a_close_pct"].mean() * 100.0), 6) if df["pre_a_high_to_a_close_pct"].notna().any() else None,
        "outputs": sorted([p.name for p in out_dir.glob("*.csv")]) + ["summary.json"],
    }
    with (out_dir / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("=== Spring-SABC pre-A profile audit done ===")
    print(f"trades_path : {trades_path}")
    print(f"kline_root  : {kline_root}")
    print(f"out_dir     : {out_dir}")
    print(f"trades      : {summary['trade_count']}")
    print(f"wins/losses : {summary['win_count']} / {summary['loss_count']}")
    print(f"gross pnl   : {summary['gross_pnl_u_100']} U per 100U fixed stake")
    print("outputs     : summary.json, trades_with_pre_a_features.csv, grouped CSVs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
