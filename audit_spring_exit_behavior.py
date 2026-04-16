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
                out = out.xs(symbol, level="symbol")
            except Exception:
                pass
        if isinstance(out.index, pd.MultiIndex):
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
        out["open_time_ms"] = pd.to_numeric(out[ts_col], errors="coerce")
    out = out.dropna(subset=["open_time_ms"]).copy()
    out["open_time_ms"] = out["open_time_ms"].astype("int64")
    out = out.drop_duplicates("open_time_ms").sort_values("open_time_ms").set_index("open_time_ms")
    return out


def _load_symbol_df(kline_root: Path, symbol: str) -> pd.DataFrame:
    symbol_dir = kline_root / symbol
    monthly = sorted(symbol_dir.glob("*.parquet")) if symbol_dir.exists() and symbol_dir.is_dir() else []
    if monthly:
        parts = []
        for path in monthly:
            try:
                parts.append(pd.read_parquet(path))
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


def _row_at_or_before(df: pd.DataFrame, ts_ms: int) -> Optional[pd.Series]:
    if df.empty:
        return None
    try:
        pos = df.index.searchsorted(int(ts_ms), side="right") - 1
    except Exception:
        return None
    if pos < 0 or pos >= len(df):
        return None
    return df.iloc[int(pos)]


def _first_hit_time(path: pd.DataFrame, entry_price: float, target_pct: float) -> Optional[int]:
    if path.empty or "high" not in path.columns:
        return None
    highs = pd.to_numeric(path["high"], errors="coerce")
    target_price = entry_price * (1.0 + target_pct)
    hit = highs >= target_price
    if not bool(hit.any()):
        return None
    return int(hit[hit].index[0])


def _bucket(v: Any, cuts: List[float], labels: List[str], default: str = "NA") -> str:
    fv = _safe_float(v, None)
    if fv is None:
        return default
    for cut, label in zip(cuts, labels):
        if fv < cut:
            return label
    return labels[-1]


def _features_for_trade(trade: Dict[str, Any], symbol_df: pd.DataFrame, horizons: List[int]) -> Dict[str, Any]:
    ctx = dict(trade.get("context") or {})
    symbol = str(trade.get("symbol") or "")
    entry_time = _safe_int(trade.get("entry_time"), None)
    exit_time = _safe_int(trade.get("exit_time"), None)
    signal_time = _safe_int(trade.get("signal_time"), None)
    entry_price = _safe_float(trade.get("entry_price"), _safe_float(trade.get("signal_price"), None))
    exit_price = _safe_float(trade.get("exit_price"), None)
    pnl_pct = _safe_float(trade.get("pnl_pct"), 0.0) or 0.0
    sl_price = _safe_float(ctx.get("stop_loss_price"), _safe_float(trade.get("sl_price"), None))
    risk_pct = _safe_float(ctx.get("risk_pct"), None)
    if risk_pct is None and entry_price and sl_price and entry_price > 0:
        risk_pct = (entry_price - sl_price) / entry_price

    if entry_time is None or exit_time is None or entry_price is None or entry_price <= 0 or symbol_df.empty:
        path = pd.DataFrame()
    else:
        path = symbol_df[(symbol_df.index >= int(entry_time)) & (symbol_df.index <= int(exit_time))].copy()

    result: Dict[str, Any] = {
        "symbol": symbol,
        "reason": str(trade.get("reason") or ""),
        "signal_time": signal_time,
        "signal_time_bj": trade.get("signal_time_bj") or _bj_text(signal_time),
        "entry_time": entry_time,
        "entry_time_bj": trade.get("entry_time_bj") or _bj_text(entry_time),
        "exit_time": exit_time,
        "exit_time_bj": trade.get("exit_time_bj") or _bj_text(exit_time),
        "month_bj": _month_bj(exit_time),
        "hold_mins": int(round((exit_time - entry_time) / 60000.0)) if entry_time is not None and exit_time is not None and exit_time >= entry_time else None,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "sl_price": sl_price,
        "pnl_pct": pnl_pct,
        "pnl_u_100": pnl_pct * 100.0,
        "risk_pct": risk_pct,
        "score_order": _safe_int(ctx.get("score_order"), None),
        "score": _safe_int(ctx.get("score"), None),
        "chg_24h": _safe_float(ctx.get("chg_24h"), None),
        "vol_24h": _safe_float(ctx.get("vol_24h"), None),
        "ab_bars": _safe_int(ctx.get("ab_bars"), None),
        "bc_bars": _safe_int(ctx.get("bc_bars"), None),
        "ab_chg_pct": _safe_float(ctx.get("ab_chg_pct"), None),
        "rebound_ratio": _safe_float(ctx.get("rebound_ratio"), None),
        "vol_ratio": _safe_float(ctx.get("vol_ratio"), None),
        "gamma_ac_vol_ratio": _safe_float(ctx.get("gamma_ac_vol_ratio"), None),
        "path_bars": int(len(path)),
    }

    if path.empty or not {"high", "low", "close"}.issubset(set(path.columns)):
        result.update({
            "mfe_pct": None,
            "mae_pct": None,
            "mfe_r": None,
            "mae_r": None,
            "giveback_pct": None,
            "giveback_r": None,
            "min_close_pnl_pct": None,
            "max_close_pnl_pct": None,
        })
    else:
        highs = pd.to_numeric(path["high"], errors="coerce")
        lows = pd.to_numeric(path["low"], errors="coerce")
        closes = pd.to_numeric(path["close"], errors="coerce")
        mfe_pct = float((highs.max() / entry_price) - 1.0) if highs.notna().any() else None
        mae_pct = float((lows.min() / entry_price) - 1.0) if lows.notna().any() else None
        min_close_pnl = float((closes.min() / entry_price) - 1.0) if closes.notna().any() else None
        max_close_pnl = float((closes.max() / entry_price) - 1.0) if closes.notna().any() else None
        mfe_r = (mfe_pct / risk_pct) if mfe_pct is not None and risk_pct and risk_pct > 0 else None
        mae_r = (mae_pct / risk_pct) if mae_pct is not None and risk_pct and risk_pct > 0 else None
        giveback_pct = (mfe_pct - pnl_pct) if mfe_pct is not None else None
        giveback_r = (giveback_pct / risk_pct) if giveback_pct is not None and risk_pct and risk_pct > 0 else None
        result.update({
            "mfe_pct": mfe_pct,
            "mae_pct": mae_pct,
            "mfe_r": mfe_r,
            "mae_r": mae_r,
            "giveback_pct": giveback_pct,
            "giveback_r": giveback_r,
            "min_close_pnl_pct": min_close_pnl,
            "max_close_pnl_pct": max_close_pnl,
        })

        for r in [0.5, 1.0, 1.5, 2.0, 3.0]:
            target_pct = (risk_pct * r) if risk_pct is not None else None
            hit_time = _first_hit_time(path, entry_price, target_pct) if target_pct is not None else None
            key = str(r).replace(".", "p")
            result[f"reached_{key}r"] = int(hit_time is not None)
            result[f"time_to_{key}r_mins"] = int(round((hit_time - entry_time) / 60000.0)) if hit_time is not None and entry_time is not None else None

        result["reached_0p5r_then_loss"] = int(bool(result.get("reached_0p5r")) and pnl_pct < 0)
        result["reached_1p0r_then_not_tp"] = int(bool(result.get("reached_1p0r")) and str(trade.get("reason") or "") != "TAKE_PROFIT")

    for mins in horizons:
        val = None
        if entry_time is not None and exit_time is not None and entry_price and entry_price > 0:
            target_time = int(entry_time) + int(mins) * MINUTE_MS
            if target_time <= int(exit_time):
                row = _row_at_or_before(symbol_df, target_time)
                if row is not None:
                    close = _safe_float(row.get("close"), None)
                    if close is not None:
                        val = (close / entry_price) - 1.0
        result[f"pnl_at_{mins}m_pct"] = val
        result[f"pnl_at_{mins}m_r"] = (val / risk_pct) if val is not None and risk_pct and risk_pct > 0 else None

    return result


def _add_buckets(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["pnl_bucket"] = out["pnl_pct"].apply(lambda x: _bucket(x, [-0.10, -0.05, 0.0, 0.05, 0.10], ["<-10%", "-10~-5%", "-5~0%", "0~5%", "5~10%", ">=10%"]))
    out["mfe_r_bucket"] = out["mfe_r"].apply(lambda x: _bucket(x, [0.5, 1.0, 1.5, 2.0, 3.0], ["<0.5R", "0.5~1R", "1~1.5R", "1.5~2R", "2~3R", ">=3R"]))
    out["mae_r_bucket"] = out["mae_r"].apply(lambda x: _bucket(x, [-1.0, -0.5, -0.2, 0.0], ["<=-1R", "-1~-0.5R", "-0.5~-0.2R", "-0.2~0R", ">=0R"]))
    out["giveback_r_bucket"] = out["giveback_r"].apply(lambda x: _bucket(x, [0.25, 0.5, 1.0, 2.0], ["<0.25R", "0.25~0.5R", "0.5~1R", "1~2R", ">=2R"]))
    out["risk_bucket"] = out["risk_pct"].apply(lambda x: _bucket(x, [0.05, 0.08, 0.12], ["<5%", "5~8%", "8~12%", ">=12%"]))
    out["gamma_ac_vol_ratio_bucket"] = out["gamma_ac_vol_ratio"].apply(lambda x: _bucket(x, [1.0, 1.5, 2.0], ["<1", "1~1.5", "1.5~2", ">=2"]))
    return out


def _group_summary(df: pd.DataFrame, by: List[str]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    rows = []
    for key, sub in df.groupby(by, dropna=False):
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
            "avg_mfe_pct": round(float(sub["mfe_pct"].mean() * 100.0), 6) if sub["mfe_pct"].notna().any() else None,
            "avg_mae_pct": round(float(sub["mae_pct"].mean() * 100.0), 6) if sub["mae_pct"].notna().any() else None,
            "avg_mfe_r": round(float(sub["mfe_r"].mean()), 6) if sub["mfe_r"].notna().any() else None,
            "avg_mae_r": round(float(sub["mae_r"].mean()), 6) if sub["mae_r"].notna().any() else None,
            "avg_giveback_r": round(float(sub["giveback_r"].mean()), 6) if sub["giveback_r"].notna().any() else None,
            "reached_0p5r_rate_pct": round(float(sub.get("reached_0p5r", pd.Series(dtype=float)).mean() * 100.0), 2) if "reached_0p5r" in sub else None,
            "reached_1p0r_rate_pct": round(float(sub.get("reached_1p0r", pd.Series(dtype=float)).mean() * 100.0), 2) if "reached_1p0r" in sub else None,
            "reached_2p0r_rate_pct": round(float(sub.get("reached_2p0r", pd.Series(dtype=float)).mean() * 100.0), 2) if "reached_2p0r" in sub else None,
            "avg_hold_mins": round(float(sub["hold_mins"].mean()), 6) if sub["hold_mins"].notna().any() else None,
            "avg_score_order": round(float(sub["score_order"].mean()), 6) if sub["score_order"].notna().any() else None,
        })
        rows.append(item)
    out = pd.DataFrame(rows)
    return out.sort_values(by + ["trade_count"], ascending=[True] * len(by) + [False])


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit Spring-SABC exit behavior from sim trades and 1m contract klines.")
    ap.add_argument("--trades", required=True, help="Path to sim_trades.<RUN_ID>.jsonl")
    ap.add_argument("--kline-root", default="data/klines_1m", help="Root directory of contract 1m kline parquet files")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--horizons", default="5,10,15,20,30,60", help="Comma-separated minutes for pnl snapshots after entry")
    ap.add_argument("--top-n", type=int, default=30, help="Top row count for diagnostic CSVs")
    args = ap.parse_args()

    trades_path = Path(args.trades)
    kline_root = Path(args.kline_root)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    horizons = [int(x.strip()) for x in str(args.horizons).split(",") if x.strip()]

    trades = _read_jsonl(trades_path)
    symbol_cache: Dict[str, pd.DataFrame] = {}
    missing_symbols: List[str] = []
    rows: List[Dict[str, Any]] = []
    for trade in trades:
        symbol = str(trade.get("symbol") or "")
        if symbol not in symbol_cache:
            symbol_cache[symbol] = _load_symbol_df(kline_root, symbol)
            if symbol_cache[symbol].empty:
                missing_symbols.append(symbol)
        rows.append(_features_for_trade(trade, symbol_cache[symbol], horizons))

    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("no trades loaded")
    df = _add_buckets(df)

    df.to_csv(out_dir / "trades_with_exit_behavior.csv", index=False)
    df.sort_values("pnl_pct", ascending=True).head(args.top_n).to_csv(out_dir / "exit_behavior_top_losses.csv", index=False)
    df.sort_values("pnl_pct", ascending=False).head(args.top_n).to_csv(out_dir / "exit_behavior_top_wins.csv", index=False)
    df.sort_values("giveback_r", ascending=False, na_position="last").head(args.top_n).to_csv(out_dir / "exit_behavior_top_giveback.csv", index=False)

    groups = {
        "exit_behavior_summary_by_reason.csv": ["reason"],
        "exit_behavior_summary_by_month.csv": ["month_bj"],
        "exit_behavior_summary_by_score_order.csv": ["score_order"],
        "exit_behavior_summary_by_mfe_r_bucket.csv": ["mfe_r_bucket"],
        "exit_behavior_summary_by_mae_r_bucket.csv": ["mae_r_bucket"],
        "exit_behavior_summary_by_giveback_r_bucket.csv": ["giveback_r_bucket"],
        "exit_behavior_summary_by_risk_bucket.csv": ["risk_bucket"],
        "exit_behavior_summary_by_gamma_ac_vol_ratio_bucket.csv": ["gamma_ac_vol_ratio_bucket"],
        "exit_behavior_summary_by_reason_and_mfe_r_bucket.csv": ["reason", "mfe_r_bucket"],
        "exit_behavior_summary_by_reason_and_mae_r_bucket.csv": ["reason", "mae_r_bucket"],
        "exit_behavior_summary_by_score_order_and_mfe_r_bucket.csv": ["score_order", "mfe_r_bucket"],
        "exit_behavior_summary_by_reached_0p5r_then_loss.csv": ["reached_0p5r_then_loss"],
        "exit_behavior_summary_by_reached_1p0r_then_not_tp.csv": ["reached_1p0r_then_not_tp"],
    }
    for filename, by in groups.items():
        cols = [c for c in by if c in df.columns]
        if cols:
            _group_summary(df, cols).to_csv(out_dir / filename, index=False)

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
        "avg_mfe_pct": round(float(df["mfe_pct"].mean() * 100.0), 6) if df["mfe_pct"].notna().any() else None,
        "avg_mae_pct": round(float(df["mae_pct"].mean() * 100.0), 6) if df["mae_pct"].notna().any() else None,
        "avg_mfe_r": round(float(df["mfe_r"].mean()), 6) if df["mfe_r"].notna().any() else None,
        "avg_mae_r": round(float(df["mae_r"].mean()), 6) if df["mae_r"].notna().any() else None,
        "reached_0p5r_count": int(df.get("reached_0p5r", pd.Series(dtype=int)).sum()) if "reached_0p5r" in df else 0,
        "reached_1p0r_count": int(df.get("reached_1p0r", pd.Series(dtype=int)).sum()) if "reached_1p0r" in df else 0,
        "reached_2p0r_count": int(df.get("reached_2p0r", pd.Series(dtype=int)).sum()) if "reached_2p0r" in df else 0,
        "reached_0p5r_then_loss_count": int(df.get("reached_0p5r_then_loss", pd.Series(dtype=int)).sum()) if "reached_0p5r_then_loss" in df else 0,
        "reached_1p0r_then_not_tp_count": int(df.get("reached_1p0r_then_not_tp", pd.Series(dtype=int)).sum()) if "reached_1p0r_then_not_tp" in df else 0,
        "horizons_mins": horizons,
        "outputs": sorted([p.name for p in out_dir.glob("*.csv")]) + ["summary.json"],
    }
    with (out_dir / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("=== Spring-SABC exit behavior audit done ===")
    print(f"trades_path : {trades_path}")
    print(f"kline_root  : {kline_root}")
    print(f"out_dir     : {out_dir}")
    print(f"trades      : {summary['trade_count']}")
    print(f"wins/losses : {summary['win_count']} / {summary['loss_count']}")
    print(f"gross pnl   : {summary['gross_pnl_u_100']} U per 100U fixed stake")
    print("outputs     : summary.json, trades_with_exit_behavior.csv, grouped CSVs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
