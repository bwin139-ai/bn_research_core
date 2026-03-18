#!/usr/bin/env python3
import argparse
import json
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd
import numpy as np

CHG24_BUCKETS = [
    (-np.inf, -0.20, "<=-20%"),
    (-0.20, -0.10, "(-20%,-10%]"),
    (-0.10, 0.00, "(-10%,0%]"),
    (0.00, 0.10, "(0%,10%]"),
    (0.10, 0.20, "(10%,20%]"),
    (0.20, 0.40, "(20%,40%]"),
    (0.40, 0.80, "(40%,80%]"),
    (0.80, np.inf, "(80%,+inf)"),
]

DW_BUCKETS = [
    (-np.inf, -0.10, "<=-10%"),
    (-0.10, -0.05, "(-10%,-5%]"),
    (-0.05, -0.03, "(-5%,-3%]"),
    (-0.03, -0.01, "(-3%,-1%]"),
    (-0.01, 0.00, "(-1%,0%]"),
    (0.00, 0.05, "(0%,5%]"),
    (0.05, 0.10, "(5%,10%]"),
    (0.10, np.inf, "(10%,+inf)"),
]

REASON_ORDER = ["TAKE_PROFIT", "TIME_STOP", "STOP_LOSS", "BREAKEVEN_STOP"]

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--regime-start", required=True)
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--grid-csv", required=True)
    ap.add_argument("--post-csv", required=True)
    ap.add_argument("--summary-json", required=True)
    return ap.parse_args()

def find_paths(run_id: str):
    base = Path("output/state")
    trades = base / f"sim_trades.{run_id}.jsonl"
    summary = base / f"sim_summary.{run_id}.json"
    if not trades.exists():
        raise FileNotFoundError(f"Trades file not found: {trades}")
    if not summary.exists():
        raise FileNotFoundError(f"Summary file not found: {summary}")
    return trades, summary

def bucketize(v, buckets):
    if pd.isna(v):
        return "NA"
    for lo, hi, label in buckets:
        if v <= hi and v > lo:
            return label
    return "NA"

def parse_ts(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return pd.NaT
    # ms epoch
    if isinstance(v, (int, float)) and abs(v) > 10_000_000_000:
        return pd.to_datetime(int(v), unit="ms", utc=True)
    # s epoch
    if isinstance(v, (int, float)) and abs(v) > 1_000_000_000:
        return pd.to_datetime(int(v), unit="s", utc=True)
    try:
        ts = pd.to_datetime(v, utc=True)
        return ts
    except Exception:
        return pd.NaT

def pct(v):
    return None if pd.isna(v) else float(v) * 100.0

def load_trades(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            t = json.loads(line)
            ctx = t.get("context", {}) or {}
            params = t.get("params", {}) or {}
            rows.append({
                "symbol": t.get("symbol"),
                "reason": t.get("reason"),
                "pnl_pct": t.get("pnl_pct"),
                "exit_time": parse_ts(t.get("exit_time")),
                "entry_time": parse_ts(t.get("entry_time")),
                "chg_24h": ctx.get("chg_24h", t.get("chg_24h")),
                "drop_window_chg": ctx.get("drop_window_chg", t.get("drop_window_chg")),
                "drop_pct": ctx.get("drop_pct", t.get("drop_pct")),
                "rebound_ratio": ctx.get("rebound_ratio", t.get("rebound_ratio")),
                "bc_ab_ratio": ctx.get("bc_ab_ratio", t.get("bc_ab_ratio")),
                "vol_ratio": ctx.get("vol_ratio", t.get("vol_ratio")),
                "mfe_pct": t.get("mfe_pct", ctx.get("mfe_pct")),
                "mae_pct": t.get("mae_pct", ctx.get("mae_pct")),
                "s_time": parse_ts(ctx.get("s_time")),
                "c_time": parse_ts(ctx.get("c_time")),
                "drop_window_mins": params.get("drop_window_mins", ctx.get("drop_window_mins")),
            })
    df = pd.DataFrame(rows)
    num_cols = ["pnl_pct","chg_24h","drop_window_chg","drop_pct","rebound_ratio","bc_ab_ratio","vol_ratio","mfe_pct","mae_pct"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def make_grid(df):
    rows = []
    for regime in ["pre_regime", "post_regime"]:
        dfr = df[df["regime"] == regime]
        for x in [b[2] for b in CHG24_BUCKETS]:
            for y in [b[2] for b in DW_BUCKETS]:
                cell = dfr[(dfr["chg_24h_bucket"] == x) & (dfr["drop_window_chg_bucket"] == y)]
                if len(cell) == 0:
                    continue
                rec = {
                    "regime": regime,
                    "chg_24h_bucket": x,
                    "drop_window_chg_bucket": y,
                    "count": int(len(cell)),
                    "avg_pnl_pct": pct(cell["pnl_pct"].mean()),
                    "median_pnl_pct": pct(cell["pnl_pct"].median()),
                    "avg_chg_24h_pct": pct(cell["chg_24h"].mean()),
                    "avg_drop_window_chg_pct": pct(cell["drop_window_chg"].mean()),
                    "avg_drop_pct": pct(cell["drop_pct"].mean()),
                    "avg_rebound_ratio_pct": pct(cell["rebound_ratio"].mean()),
                    "avg_bc_ab_ratio_pct": pct(cell["bc_ab_ratio"].mean()),
                    "avg_vol_ratio": None if cell["vol_ratio"].dropna().empty else float(cell["vol_ratio"].mean()),
                    "avg_mfe_pct": pct(cell["mfe_pct"].mean()) if "mfe_pct" in cell else None,
                    "avg_mae_pct": pct(cell["mae_pct"].mean()) if "mae_pct" in cell else None,
                }
                for reason in REASON_ORDER:
                    rec[f"{reason}_count"] = int((cell["reason"] == reason).sum())
                    rec[f"{reason}_rate"] = float((cell["reason"] == reason).mean())
                rows.append(rec)
    return pd.DataFrame(rows)

def reason_stats(df):
    out = {}
    for regime in ["pre_regime", "post_regime"]:
        dfr = df[df["regime"] == regime]
        rec = {
            "count": int(len(dfr)),
            "pnl_pct_sum": pct(dfr["pnl_pct"].sum()),
            "avg_pnl_pct": pct(dfr["pnl_pct"].mean()),
            "median_pnl_pct": pct(dfr["pnl_pct"].median()),
            "win_rate": float((dfr["pnl_pct"] > 0).mean()) if len(dfr) else None,
            "reason_counts": dfr["reason"].value_counts(dropna=False).to_dict(),
        }
        out[regime] = rec
    return out

def top_records(df, n=20):
    cols = ["symbol","reason","pnl_pct","chg_24h","drop_window_chg","chg_24h_bucket","drop_window_chg_bucket","exit_time","regime"]
    d = df.copy()
    for c in ["pnl_pct","chg_24h","drop_window_chg"]:
        d[c+"_pct"] = d[c] * 100.0
    view_cols = ["symbol","reason","pnl_pct_pct","chg_24h_pct","drop_window_chg_pct","chg_24h_bucket","drop_window_chg_bucket","exit_time","regime"]
    worst = d.sort_values("pnl_pct", ascending=True)[view_cols].head(n).copy()
    best = d.sort_values("pnl_pct", ascending=False)[view_cols].head(n).copy()
    for x in (worst, best):
        x["exit_time"] = x["exit_time"].astype(str)
    return worst.to_dict(orient="records"), best.to_dict(orient="records")

def main():
    args = parse_args()
    trades_path, summary_path = find_paths(args.run_id)
    df = load_trades(trades_path)
    regime_start = pd.to_datetime(args.regime_start, utc=True)
    df["regime"] = np.where(df["exit_time"] >= regime_start, "post_regime", "pre_regime")
    df["chg_24h_bucket"] = df["chg_24h"].apply(lambda v: bucketize(v, CHG24_BUCKETS))
    df["drop_window_chg_bucket"] = df["drop_window_chg"].apply(lambda v: bucketize(v, DW_BUCKETS))
    df["cell_bucket"] = df["chg_24h_bucket"] + " | " + df["drop_window_chg_bucket"]

    # Write detail/post
    out_df = df.copy()
    for c in ["pnl_pct","chg_24h","drop_window_chg","drop_pct","rebound_ratio","bc_ab_ratio","mfe_pct","mae_pct"]:
        if c in out_df.columns:
            out_df[c + "_pct"] = out_df[c] * 100.0
    out_df["exit_time"] = out_df["exit_time"].astype(str)
    out_df["entry_time"] = out_df["entry_time"].astype(str)
    out_df["s_time"] = out_df["s_time"].astype(str)
    out_df["c_time"] = out_df["c_time"].astype(str)
    Path(args.out_csv).parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(args.out_csv, index=False)
    out_df[out_df["regime"] == "post_regime"].to_csv(args.post_csv, index=False)

    grid = make_grid(df)
    grid.to_csv(args.grid_csv, index=False)

    with summary_path.open("r", encoding="utf-8") as f:
        sim_summary = json.load(f)

    worst, best = top_records(df, n=20)
    summary = {
        "run_id": args.run_id,
        "regime_start": args.regime_start,
        "rows": int(len(df)),
        "post_rows": int((df["regime"] == "post_regime").sum()),
        "pre_rows": int((df["regime"] == "pre_regime").sum()),
        "reason_stats": reason_stats(df),
        "chg_24h_bucket_counts_by_regime": {
            regime: df[df["regime"] == regime]["chg_24h_bucket"].value_counts().to_dict()
            for regime in ["pre_regime", "post_regime"]
        },
        "drop_window_chg_bucket_counts_by_regime": {
            regime: df[df["regime"] == regime]["drop_window_chg_bucket"].value_counts().to_dict()
            for regime in ["pre_regime", "post_regime"]
        },
        "grid_rows": int(len(grid)),
        "worst_pnl_top20": worst,
        "best_pnl_top20": best,
        "run_config": sim_summary.get("run_config"),
    }
    Path(args.summary_json).write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=== audit_regime_jan2026_slice 完成 ===")
    print(f"run_id       : {args.run_id}")
    print(f"trades       : {trades_path}")
    print(f"summary json : {args.summary_json}")
    print(f"detail csv   : {args.out_csv}")
    print(f"grid csv     : {args.grid_csv}")
    print(f"post csv     : {args.post_csv}")
    print(f"rows         : {len(df)}")
    print(f"post rows    : {(df['regime'] == 'post_regime').sum()}")

if __name__ == "__main__":
    main()
