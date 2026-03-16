#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def bucket_bc_bars(v):
    if pd.isna(v):
        return None
    x = int(v)
    if x == 0:
        return "0"
    if x == 1:
        return "1"
    if x == 2:
        return "2"
    return ">=3"


def bucket_bc_ab(v):
    if pd.isna(v):
        return None
    x = float(v)
    return "<0.15" if x < 0.15 else ">=0.15"


def summarize(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    rows = []
    for group_value, sub in df.groupby(group_col, dropna=False):
        if group_value is None or (isinstance(group_value, float) and pd.isna(group_value)):
            continue
        rows.append(
            {
                "group": str(group_value),
                "count": int(len(sub)),
                "avg_pnl_pct": round(float(sub["pnl_pct"].mean()), 4),
                "median_pnl_pct": round(float(sub["pnl_pct"].median()), 4),
                "avg_mae_pct": round(float(sub["mae_pct"].mean()), 4),
                "avg_mfe_pct": round(float(sub["mfe_pct"].mean()), 4),
                "avg_mae_to_loss_ratio": round(float(sub["mae_to_loss_ratio"].mean()), 4),
            }
        )
    out = pd.DataFrame(rows)
    if not out.empty:
        order_map = {"0": 0, "1": 1, "2": 2, ">=3": 3, "<0.15": 0, ">=0.15": 1}
        out["_ord"] = out["group"].map(order_map).fillna(999)
        out = out.sort_values(["_ord", "group"]).drop(columns=["_ord"])
    return out


def print_table(title: str, df: pd.DataFrame) -> None:
    print()
    print("=" * 80)
    print(title)
    print("=" * 80)
    if df.empty:
        print("(empty)")
        return
    print(df.to_string(index=False))


def main() -> None:
    ap = argparse.ArgumentParser(description="按 bc_bars / bc_ab 对 extreme_mae_audit 结果分层")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--audit-csv", default=None)
    ap.add_argument("--out-dir", default="output/state")
    args = ap.parse_args()

    audit_csv = Path(args.audit_csv) if args.audit_csv else Path("output/state") / f"extreme_mae_audit.{args.run_id}.csv"
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not audit_csv.exists():
        raise SystemExit(f"[FAIL] audit csv 不存在: {audit_csv}")

    df = pd.read_csv(audit_csv)
    required = ["pnl_pct", "mfe_pct", "mae_pct", "mae_to_loss_ratio", "bc_bars", "bc_vs_ab_ratio_index"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise SystemExit(f"[FAIL] audit csv 缺少列: {missing}")

    df["bc_bars_bucket"] = df["bc_bars"].map(bucket_bc_bars)
    df["bc_ab_bucket"] = df["bc_vs_ab_ratio_index"].map(bucket_bc_ab)

    bars_table = summarize(df.dropna(subset=["bc_bars_bucket"]), "bc_bars_bucket")
    bcab_table = summarize(df.dropna(subset=["bc_ab_bucket"]), "bc_ab_bucket")

    print_table("表 1：高风险样本按 bc_bars 分层", bars_table)
    print_table("表 2：高风险样本按 bc/ab 分层", bcab_table)

    base = out_dir / f"extreme_mae_bucket_validation.{args.run_id}"
    bars_csv = Path(str(base) + ".bc_bars.csv")
    bcab_csv = Path(str(base) + ".bc_ab.csv")
    summary_json = Path(str(base) + ".summary.json")

    bars_table.to_csv(bars_csv, index=False)
    bcab_table.to_csv(bcab_csv, index=False)
    summary = {
        "run_id": args.run_id,
        "audit_csv": str(audit_csv),
        "bc_bars_csv": str(bars_csv),
        "bc_ab_csv": str(bcab_csv),
        "rows": int(len(df)),
    }
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print()
    print(f"CSV 输出: {bars_csv}")
    print(f"CSV 输出: {bcab_csv}")
    print(f"Summary 输出: {summary_json}")


if __name__ == "__main__":
    main()
