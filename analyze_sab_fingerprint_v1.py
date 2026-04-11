#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List

import pandas as pd

FEATURES = [
    "sa_bars",
    "ab_bars",
    "sa_chg_pct",
    "ab_drop_pct_index",
    "ab_vs_sa_amp_ratio",
    "ab_drop_speed",
    "ab_path_efficiency",
    "ab_step_drop_count",
    "a_peak_sharpness",
    "ab_peak_vol_pos01",
]

CROSS_PAIRS = [
    ("ab_path_efficiency", "ab_step_drop_count"),
    ("ab_vs_sa_amp_ratio", "ab_path_efficiency"),
    ("ab_step_drop_count", "ab_peak_vol_position"),
]


def _bucket_quantile(series: pd.Series, q: int = 5) -> pd.Series:
    try:
        return pd.qcut(series, q=q, duplicates="drop")
    except Exception:
        return pd.Series([None] * len(series), index=series.index)


def _normalize_outcome(v: object) -> str:
    s = str(v).upper().strip()
    if s in {"TAKE_PROFIT", "TP"}:
        return "TP"
    if s in {"STOP_LOSS", "SL"}:
        return "SL"
    if s in {"TIMEOUT", "TIME_STOP", "TS"}:
        return "TS"
    return s


def _group_summary(df: pd.DataFrame, group_cols: List[str]) -> pd.DataFrame:
    grp = df.groupby(group_cols, dropna=False, observed=False)
    out = grp.agg(
        sample_count=("symbol", "count"),
        avg_pnl_pct=("pnl_pct", "mean"),
        med_pnl_pct=("pnl_pct", "median"),
        tp_rate=("outcome", lambda s: float((s == "TP").mean()) if len(s) else 0.0),
        sl_rate=("outcome", lambda s: float((s == "SL").mean()) if len(s) else 0.0),
        ts_rate=("outcome", lambda s: float((s == "TS").mean()) if len(s) else 0.0),
        avg_hold_mins=("hold_mins", "mean"),
    ).reset_index()
    return out.sort_values(["sample_count", "avg_pnl_pct"], ascending=[False, False])


def _proto_bucket_sharpness(v: float | None) -> str | None:
    if v is None or pd.isna(v):
        return None
    if v < 0.003:
        return "flat_top"
    if v < 0.010:
        return "rounded_top"
    return "sharp_top"


def _build_proto_type(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["depth_band"] = pd.cut(
        work["ab_drop_pct_index"],
        bins=[-999, 0.08, 0.12, 0.18, 999],
        labels=["shallow", "mid", "deep", "extreme"],
    )
    work["eff_band"] = pd.cut(
        work["ab_path_efficiency"],
        bins=[-999, 0.55, 0.72, 0.85, 999],
        labels=["curvy", "mixed", "direct", "very_direct"],
    )
    work["step_band"] = pd.cut(
        work["ab_step_drop_count"],
        bins=[-999, 1, 2, 99],
        labels=["one_leg", "two_leg", "multi_leg"],
    )
    work["sharp_band"] = work["a_peak_sharpness"].map(_proto_bucket_sharpness)
    work["proto_type"] = (
        work["depth_band"].astype(str)
        + "|"
        + work["eff_band"].astype(str)
        + "|"
        + work["step_band"].astype(str)
        + "|"
        + work["sharp_band"].astype(str)
    )
    return work


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze first-batch SAB fingerprint ledger")
    ap.add_argument("--input-csv", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--min-sample", type=int, default=5)
    args = ap.parse_args()

    df = pd.read_csv(args.input_csv)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if "feature_scope" not in df.columns or not (df["feature_scope"] == "hb_sab_only").all():
        raise ValueError("input csv is not strict hb_sab_only SAB fingerprint output")

    df_ok = df[df["feature_status"] == "ok"].copy()
    if df_ok.empty:
        raise SystemExit("no feature_status=ok rows")

    df_ok["outcome"] = df_ok["outcome"].map(_normalize_outcome)

    summary = {
        "feature_scope": "hb_sab_only",
        "rows_total": int(len(df)),
        "rows_ok": int(len(df_ok)),
        "tp_count": int((df_ok["outcome"] == "TP").sum()),
        "sl_count": int((df_ok["outcome"] == "SL").sum()),
        "ts_count": int((df_ok["outcome"] == "TS").sum()),
        "avg_pnl_pct": float(df_ok["pnl_pct"].mean()),
        "avg_hold_mins": float(df_ok["hold_mins"].mean()),
        "run_ids": sorted({str(x) for x in df_ok["run_id"].dropna().unique().tolist()}),
    }
    (out_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    df_ok.groupby("outcome")[FEATURES + ["pnl_pct", "hold_mins"]].mean(numeric_only=True).to_csv(
        out_dir / "outcome_feature_means.csv"
    )

    bucket_dir = out_dir / "bucket_tables"
    bucket_dir.mkdir(parents=True, exist_ok=True)
    for feat in FEATURES:
        if feat not in df_ok.columns:
            continue
        work = df_ok[[feat, "symbol", "outcome", "pnl_pct", "hold_mins"]].dropna().copy()
        if len(work) < max(20, args.min_sample * 2):
            continue
        work[f"{feat}_bucket"] = _bucket_quantile(work[feat], q=5)
        table = _group_summary(work, [f"{feat}_bucket"])
        table = table[table["sample_count"] >= args.min_sample]
        table.to_csv(bucket_dir / f"bucket_{feat}.csv", index=False)

    cross_dir = out_dir / "cross_tables"
    cross_dir.mkdir(parents=True, exist_ok=True)
    for feat_a, feat_b in CROSS_PAIRS:
        cols = [feat_a, feat_b, "ab_peak_vol_position", "symbol", "outcome", "pnl_pct", "hold_mins"]
        cols = list(dict.fromkeys([c for c in cols if c in df_ok.columns]))
        work = df_ok[cols].copy()
        if feat_a == "ab_step_drop_count":
            work[feat_a + "_bucket"] = work[feat_a]
        else:
            work = work.dropna(subset=[feat_a])
            work[feat_a + "_bucket"] = _bucket_quantile(work[feat_a], q=4)
        if feat_b == "ab_peak_vol_position":
            work = work.dropna(subset=[feat_b])
            work[feat_b + "_bucket"] = work[feat_b]
        elif feat_b == "ab_step_drop_count":
            work[feat_b + "_bucket"] = work[feat_b]
        else:
            work = work.dropna(subset=[feat_b])
            work[feat_b + "_bucket"] = _bucket_quantile(work[feat_b], q=4)
        table = _group_summary(work, [feat_a + "_bucket", feat_b + "_bucket"])
        table = table[table["sample_count"] >= args.min_sample]
        table.to_csv(cross_dir / f"cross_{feat_a}__{feat_b}.csv", index=False)

    typed = _build_proto_type(df_ok)
    proto = _group_summary(typed, ["proto_type"])
    proto = proto[proto["sample_count"] >= args.min_sample]
    proto.to_csv(out_dir / "proto_type_summary.csv", index=False)
    typed.to_csv(out_dir / "rows_typed.csv", index=False)

    print("=== SAB fingerprint analysis v1 done ===")
    print(f"rows_ok    : {len(df_ok)}")
    print(f"out_dir    : {out_dir}")


if __name__ == "__main__":
    main()
