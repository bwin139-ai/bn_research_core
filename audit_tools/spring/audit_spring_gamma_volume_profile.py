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
    if df is None or df.empty:
        return pd.DataFrame()
    out = df.copy()
    if isinstance(out.index, pd.MultiIndex):
        if "symbol" in out.index.names:
            try:
                out = out.xs(symbol, level="symbol").copy()
            except Exception:
                out = out.reset_index()
        else:
            out = out.reset_index()
    elif "open_time_ms" not in out.columns:
        out = out.reset_index()

    ts_col = None
    for c in ["open_time_ms", "open_time", "timestamp", "ts", "time"]:
        if c in out.columns:
            ts_col = c
            break
    if ts_col is None:
        for c in out.columns:
            if str(c).lower() in {"index", "datetime"}:
                ts_col = c
                break
    if ts_col is None:
        return pd.DataFrame()

    if pd.api.types.is_datetime64_any_dtype(out[ts_col]):
        out["open_time_ms"] = (pd.to_datetime(out[ts_col], utc=True).astype("int64") // 10**6).astype("int64")
    else:
        out["open_time_ms"] = pd.to_numeric(out[ts_col], errors="coerce").astype("Int64")
    out = out.dropna(subset=["open_time_ms"]).copy()
    out["open_time_ms"] = out["open_time_ms"].astype("int64")
    out = out.drop_duplicates("open_time_ms").sort_values("open_time_ms").set_index("open_time_ms")
    return out


def _load_symbol_df(kline_root: Path, symbol: str) -> pd.DataFrame:
    symbol_dir = kline_root / symbol
    if symbol_dir.exists() and symbol_dir.is_dir():
        parts = []
        for p in sorted(symbol_dir.glob("*.parquet")):
            try:
                parts.append(pd.read_parquet(p))
            except Exception:
                continue
        if parts:
            return _normalize_symbol_df(pd.concat(parts, ignore_index=True), symbol)

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


def _sum_volume(df: pd.DataFrame) -> float:
    if df.empty:
        return 0.0
    for col in ["quote_asset_volume", "quote_volume", "quote_vol", "volume", "base_volume"]:
        if col in df.columns:
            return float(pd.to_numeric(df[col], errors="coerce").fillna(0).sum())
    return 0.0


def _bucket(v: Any, cuts: List[float], labels: List[str], default: str = "NA") -> str:
    fv = _safe_float(v, None)
    if fv is None:
        return default
    for cut, label in zip(cuts, labels):
        if fv < cut:
            return label
    return labels[-1]


def _context_has_gamma_features(trade: Dict[str, Any]) -> bool:
    ctx = dict(trade.get("context") or {})
    return (
        _safe_float(ctx.get("vol_gamma_a"), None) is not None
        and _safe_float(ctx.get("vol_ac"), None) is not None
        and _safe_float(ctx.get("gamma_ac_vol_ratio"), None) is not None
        and _safe_int(ctx.get("bars_ac"), None) is not None
    )


def _features_for_trade(trade: Dict[str, Any], symbol_df: pd.DataFrame) -> Dict[str, Any]:
    ctx = dict(trade.get("context") or {})
    symbol = str(trade.get("symbol") or "")
    signal_time = _safe_int(trade.get("signal_time"), None)
    entry_time = _safe_int(trade.get("entry_time"), None)
    exit_time = _safe_int(trade.get("exit_time"), None)
    a_time = _safe_int(ctx.get("a_time_ms"), None)
    c_time = _safe_int(ctx.get("c_time_ms"), None)

    bars_ac = _safe_int(ctx.get("bars_ac"), None)
    gamma_time = _safe_int(ctx.get("gamma_time_ms"), None)
    vol_gamma_a = _safe_float(ctx.get("vol_gamma_a"), None)
    vol_ac = _safe_float(ctx.get("vol_ac"), None)
    vol_ac_over_gamma_a = _safe_float(ctx.get("gamma_ac_vol_ratio"), None)
    gamma_a_bars = int(bars_ac or 0)
    ac_bars = int(bars_ac or 0)

    if (
        (bars_ac is None or gamma_time is None or vol_gamma_a is None or vol_ac is None or vol_ac_over_gamma_a is None)
        and a_time is not None
        and c_time is not None
        and c_time > a_time
        and not symbol_df.empty
    ):
        bars_ac = int(round((c_time - a_time) / MINUTE_MS))
        if bars_ac > 0:
            gamma_time = gamma_time if gamma_time is not None else (a_time - bars_ac * MINUTE_MS)
            # Equal-length windows: (gamma, A] and (A, C]
            gamma_a_df = symbol_df[(symbol_df.index > gamma_time) & (symbol_df.index <= a_time)].copy()
            ac_df = symbol_df[(symbol_df.index > a_time) & (symbol_df.index <= c_time)].copy()
            gamma_a_bars = int(len(gamma_a_df))
            ac_bars = int(len(ac_df))
            vol_gamma_a = _sum_volume(gamma_a_df)
            vol_ac = _sum_volume(ac_df)
            if vol_gamma_a and vol_gamma_a > 0:
                vol_ac_over_gamma_a = vol_ac / vol_gamma_a

    pnl_pct = _safe_float(trade.get("pnl_pct"), 0.0) or 0.0
    entry_price = _safe_float(trade.get("entry_price"), None)
    sl_price = _safe_float(ctx.get("stop_loss_price"), _safe_float(trade.get("sl_price"), None))
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
        "gamma_time": gamma_time,
        "gamma_time_bj": _bj_text(gamma_time),
        "a_time": a_time,
        "a_time_bj": _bj_text(a_time),
        "c_time": c_time,
        "c_time_bj": _bj_text(c_time),
        "bars_ac": bars_ac,
        "gamma_a_bars": gamma_a_bars,
        "ac_bars": ac_bars,
        "vol_gamma_a": vol_gamma_a,
        "vol_ac": vol_ac,
        "vol_ac_over_gamma_a": vol_ac_over_gamma_a,
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


def _add_buckets(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["ratio_bucket"] = out["vol_ac_over_gamma_a"].apply(lambda x: _bucket(x, [1.0, 1.5, 2.0], ["<1", "1-1.5", "1.5-2", ">=2"]))
    out["pnl_bucket"] = out["pnl_pct"].apply(lambda x: _bucket(x, [-0.10, -0.05, 0.0, 0.05, 0.10], ["<-10%", "-10~-5%", "-5~0%", "0~5%", "5~10%", ">=10%"]))
    out["risk_bucket"] = out["risk_pct"].apply(lambda x: _bucket(x, [0.05, 0.08, 0.12], ["<5%", "5~8%", "8~12%", ">=12%"]))
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
            "avg_ratio": round(float(sub["vol_ac_over_gamma_a"].mean()), 6) if sub["vol_ac_over_gamma_a"].notna().any() else None,
            "median_ratio": round(float(sub["vol_ac_over_gamma_a"].median()), 6) if sub["vol_ac_over_gamma_a"].notna().any() else None,
            "avg_vol_gamma_a": round(float(sub["vol_gamma_a"].mean()), 6) if sub["vol_gamma_a"].notna().any() else None,
            "avg_vol_ac": round(float(sub["vol_ac"].mean()), 6) if sub["vol_ac"].notna().any() else None,
            "avg_bars_ac": round(float(sub["bars_ac"].mean()), 6) if sub["bars_ac"].notna().any() else None,
            "avg_score_order": round(float(sub["score_order"].mean()), 6) if sub["score_order"].notna().any() else None,
            "avg_ab_chg_pct": round(float(sub["ab_chg_pct"].mean() * 100.0), 6) if sub["ab_chg_pct"].notna().any() else None,
            "avg_rebound_ratio_pct": round(float(sub["rebound_ratio"].mean() * 100.0), 6) if sub["rebound_ratio"].notna().any() else None,
            "avg_risk_pct": round(float(sub["risk_pct"].mean() * 100.0), 6) if sub["risk_pct"].notna().any() else None,
        })
        rows.append(item)
    out = pd.DataFrame(rows)
    return out.sort_values(by + ["trade_count"], ascending=[True] * len(by) + [False])


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit Spring-SABC gamma-A vs A-C volume symmetry profile from sim trades.")
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
        use_context_only = _context_has_gamma_features(trade)
        if symbol not in symbol_cache:
            symbol_cache[symbol] = pd.DataFrame() if use_context_only else _load_symbol_df(kline_root, symbol)
            if not use_context_only and symbol_cache[symbol].empty:
                missing_symbols.append(symbol)
        rows.append(_features_for_trade(trade, symbol_cache[symbol]))

    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("no trades loaded")
    df = _add_buckets(df)

    df.to_csv(out_dir / "trades_with_gamma_volume_features.csv", index=False)
    df.sort_values("pnl_pct", ascending=True).head(args.top_n).to_csv(out_dir / "gamma_volume_top_losses.csv", index=False)
    df.sort_values("pnl_pct", ascending=False).head(args.top_n).to_csv(out_dir / "gamma_volume_top_wins.csv", index=False)

    groups = {
        "gamma_volume_summary_by_ratio_bucket.csv": ["ratio_bucket"],
        "gamma_volume_summary_by_reason.csv": ["reason"],
        "gamma_volume_summary_by_month.csv": ["month_bj"],
        "gamma_volume_summary_by_score_order.csv": ["score_order"],
        "gamma_volume_summary_by_pnl_bucket.csv": ["pnl_bucket"],
        "gamma_volume_summary_by_risk_bucket.csv": ["risk_bucket"],
        "gamma_volume_summary_by_reason_and_ratio_bucket.csv": ["reason", "ratio_bucket"],
        "gamma_volume_summary_by_score_order_and_ratio_bucket.csv": ["score_order", "ratio_bucket"],
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
        "valid_ratio_count": int(df["vol_ac_over_gamma_a"].notna().sum()),
        "win_count": int((df["pnl_pct"] > 0).sum()),
        "loss_count": int((df["pnl_pct"] < 0).sum()),
        "win_rate_pct": round(float((df["pnl_pct"] > 0).mean() * 100.0), 2),
        "gross_pnl_u_100": round(float(df["pnl_u_100"].sum()), 6),
        "avg_vol_ac_over_gamma_a": round(float(df["vol_ac_over_gamma_a"].mean()), 6) if df["vol_ac_over_gamma_a"].notna().any() else None,
        "median_vol_ac_over_gamma_a": round(float(df["vol_ac_over_gamma_a"].median()), 6) if df["vol_ac_over_gamma_a"].notna().any() else None,
        "outputs": sorted([p.name for p in out_dir.glob("*.csv")]) + ["summary.json"],
    }
    with (out_dir / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("=== Spring-SABC gamma-volume profile audit done ===")
    print(f"trades_path : {trades_path}")
    print(f"kline_root  : {kline_root}")
    print(f"out_dir     : {out_dir}")
    print(f"trades      : {summary['trade_count']}")
    print(f"wins/losses : {summary['win_count']} / {summary['loss_count']}")
    print(f"valid ratio : {summary['valid_ratio_count']}")
    print(f"gross pnl   : {summary['gross_pnl_u_100']} U per 100U fixed stake")
    print("outputs     : summary.json, trades_with_gamma_volume_features.csv, grouped CSVs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
