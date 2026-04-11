#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

KEY_FEATURES = [
    "ab_drop_pct_index",
    "bc_rebound_pct_index",
    "sc_net_chg",
    "ab_vs_bc_bars_ratio",
    "c_pos_in_ac_index",
    "c_close_pos_in_bc_range",
    "ab_drop_speed",
    "bc_rebound_speed",
    "speed_ratio_bc_over_ab",
    "vol_ratio",
    "rebound_ratio",
    "basis_b_pct",
    "pre_trend_chg",
    "pre_idx_trend_chg",
    "pre_lower_high_count",
    "pre_lower_low_count",
    "pre_red_bar_ratio",
]


def _bucket(series: pd.Series, q: int = 5) -> pd.Series:
    try:
        return pd.qcut(series, q=q, duplicates="drop")
    except Exception:
        return pd.Series([None] * len(series), index=series.index)


def _group_summary(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    grp = df.groupby(group_col, dropna=False)
    out = grp.agg(
        trade_count=("symbol", "count"),
        tp_rate=("outcome", lambda s: float((s == "TP").mean()) if len(s) else 0.0),
        sl_rate=("outcome", lambda s: float((s == "SL").mean()) if len(s) else 0.0),
        timeout_rate=("outcome", lambda s: float((s == "TIMEOUT").mean()) if len(s) else 0.0),
        avg_pnl_pct=("pnl_pct", "mean"),
        med_pnl_pct=("pnl_pct", "median"),
        avg_hold_mins=("hold_mins", "mean"),
    ).reset_index()
    return out.sort_values("trade_count", ascending=False)


def _prototype_cluster(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["shape_depth"] = pd.cut(work["ab_drop_pct_index"], bins=[-999, 0.08, 0.12, 0.18, 999], labels=["shallow", "mid", "deep", "extreme"])
    work["shape_rebound"] = pd.cut(work["rebound_ratio"], bins=[-999, 0.12, 0.18, 0.24, 999], labels=["weak", "mid", "strong", "over"])
    work["shape_speed"] = pd.cut(work["speed_ratio_bc_over_ab"], bins=[-999, 0.2, 0.5, 1.0, 999], labels=["very_slow", "slow", "balanced", "fast"])
    work["shape_trend"] = pd.cut(work["pre_trend_chg"], bins=[-999, -0.08, -0.03, 0.02, 999], labels=["hard_down", "soft_down", "flat", "up"])
    work["morph_type"] = (
        work["shape_depth"].astype(str)
        + "|"
        + work["shape_rebound"].astype(str)
        + "|"
        + work["shape_speed"].astype(str)
        + "|"
        + work["shape_trend"].astype(str)
    )
    return work


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze extracted SABC morphology features (pre-C only)")
    ap.add_argument("--input-csv", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--min-trade-count", type=int, default=5)
    args = ap.parse_args()

    df = pd.read_csv(args.input_csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if "feature_scope" not in df.columns or not (df["feature_scope"] == "pre_c_only").all():
        raise ValueError("input csv is not strict pre-C morphology output")

    df_ok = df[df["feature_status"] == "ok"].copy()
    if df_ok.empty:
        raise SystemExit("no feature_status=ok rows")

    summary = {
        "feature_scope": "pre_c_only",
        "rows_total": int(len(df)),
        "rows_ok": int(len(df_ok)),
        "tp_count": int((df_ok["outcome"] == "TP").sum()),
        "sl_count": int((df_ok["outcome"] == "SL").sum()),
        "timeout_count": int((df_ok["outcome"] == "TIMEOUT").sum()),
        "avg_pnl_pct": float(df_ok["pnl_pct"].mean()),
        "avg_hold_mins": float(df_ok["hold_mins"].mean()),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    df_ok.groupby("outcome")[KEY_FEATURES + ["pnl_pct", "hold_mins"]].mean(numeric_only=True).to_csv(
        out_dir / "outcome_feature_means.csv"
    )

    bucket_dir = out_dir / "bucket_tables"
    bucket_dir.mkdir(parents=True, exist_ok=True)
    for feat in KEY_FEATURES:
        if feat not in df_ok.columns:
            continue
        work = df_ok[[feat, "symbol", "outcome", "pnl_pct", "hold_mins"]].dropna().copy()
        if len(work) < max(20, args.min_trade_count * 2):
            continue
        work[f"{feat}_bucket"] = _bucket(work[feat], q=5)
        table = _group_summary(work, f"{feat}_bucket")
        table = table[table["trade_count"] >= args.min_trade_count]
        table.to_csv(bucket_dir / f"bucket_{feat}.csv", index=False)

    typed = _prototype_cluster(df_ok)
    morph_table = _group_summary(typed, "morph_type")
    morph_table[morph_table["trade_count"] >= args.min_trade_count].to_csv(out_dir / "morph_type_summary.csv", index=False)
    typed.to_csv(out_dir / "morph_rows_typed.csv", index=False)

    print("=== SABC morphology analysis v2 done ===")
    print(f"rows_ok  : {len(df_ok)}")
    print(f"out_dir  : {out_dir}")


if __name__ == "__main__":
    main()
