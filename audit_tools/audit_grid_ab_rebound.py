#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ab_bars × rebound_ratio 二维格子审计

输入:
- 带 sample_group / is_cold_zone / ab_bars / rebound_ratio / pnl_pct / reason 的 tagged csv

输出:
- all csv: 全样本格子统计
- cold csv: 冷区格子统计
- summary json: 摘要

固定 compare groups:
- pre_good
- post_bad
- post_good

固定统计:
- count
- win_rate
- avg_pnl_pct
- median_pnl_pct
- pnl_pct_sum
- tp_ratio
- ts_ratio
- sl_ratio
"""

import argparse
import json
from pathlib import Path
from typing import List, Tuple

import pandas as pd


AB_BUCKETS: List[Tuple[float, float, str]] = [
    (0, 2, "<=2"),
    (2, 4, "(2,4]"),
    (4, 6, "(4,6]"),
    (6, 10, "(6,10]"),
    (10, float("inf"), ">10"),
]

REB_BUCKETS: List[Tuple[float, float, str]] = [
    (float("-inf"), 0.15, "<=0.15"),
    (0.15, 0.30, "(0.15,0.30]"),
    (0.30, 0.50, "(0.30,0.50]"),
    (0.50, 0.70, "(0.50,0.70]"),
    (0.70, float("inf"), ">0.70"),
]

GROUPS = ["pre_good", "post_bad", "post_good"]
REQUIRED_COLS = ["sample_group", "is_cold_zone", "ab_bars", "rebound_ratio", "pnl_pct", "reason"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit ab_bars x rebound_ratio grid.")
    p.add_argument("--input-csv", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--all-csv", required=True)
    p.add_argument("--cold-csv", required=True)
    p.add_argument("--summary-json", required=True)
    return p.parse_args()


def ensure_parent(path_str: str) -> None:
    Path(path_str).parent.mkdir(parents=True, exist_ok=True)


def assign_bucket(value: float, buckets: List[Tuple[float, float, str]]) -> str:
    for lo, hi, label in buckets:
        if lo == float("-inf") and value <= hi:
            return label
        if hi == float("inf") and value > lo:
            return label
        if value > lo and value <= hi:
            return label
        if lo == 0 and value <= hi:
            return label
    raise ValueError(f"值未落入任何桶: {value}")


def calc_group_grid(df: pd.DataFrame, scope: str) -> pd.DataFrame:
    rows = []
    for group in GROUPS:
        gdf = df[df["sample_group"] == group].copy()
        if gdf.empty:
            raise SystemExit(f"样本组为空，无法继续: {group} ({scope})")
        for ab_lo, ab_hi, ab_label in AB_BUCKETS:
            if ab_hi == float("inf"):
                ab_mask = gdf["ab_bars"] > ab_lo
            elif ab_lo == 0:
                ab_mask = gdf["ab_bars"] <= ab_hi
            else:
                ab_mask = (gdf["ab_bars"] > ab_lo) & (gdf["ab_bars"] <= ab_hi)
            for reb_lo, reb_hi, reb_label in REB_BUCKETS:
                if reb_lo == float("-inf"):
                    reb_mask = gdf["rebound_ratio"] <= reb_hi
                elif reb_hi == float("inf"):
                    reb_mask = gdf["rebound_ratio"] > reb_lo
                else:
                    reb_mask = (gdf["rebound_ratio"] > reb_lo) & (gdf["rebound_ratio"] <= reb_hi)

                cell = gdf[ab_mask & reb_mask]
                count = int(len(cell))
                if count == 0:
                    win_rate = None
                    avg_pnl = None
                    median_pnl = None
                    pnl_sum = None
                    tp_ratio = None
                    ts_ratio = None
                    sl_ratio = None
                else:
                    win_rate = float((cell["pnl_pct"] > 0).mean())
                    avg_pnl = float(cell["pnl_pct"].mean())
                    median_pnl = float(cell["pnl_pct"].median())
                    pnl_sum = float(cell["pnl_pct"].sum())
                    reasons = cell["reason"].astype(str)
                    tp_ratio = float((reasons == "TAKE_PROFIT").mean())
                    ts_ratio = float((reasons == "TIME_STOP").mean())
                    sl_ratio = float((reasons == "STOP_LOSS").mean())
                rows.append({
                    "scope": scope,
                    "group_name": group,
                    "ab_bucket": ab_label,
                    "rebound_bucket": reb_label,
                    "count": count,
                    "win_rate": win_rate,
                    "avg_pnl_pct": avg_pnl,
                    "median_pnl_pct": median_pnl,
                    "pnl_pct_sum": pnl_sum,
                    "tp_ratio": tp_ratio,
                    "ts_ratio": ts_ratio,
                    "sl_ratio": sl_ratio,
                })
    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()
    ensure_parent(args.all_csv)
    ensure_parent(args.cold_csv)
    ensure_parent(args.summary_json)

    df = pd.read_csv(args.input_csv)
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise SystemExit(f"输入 csv 缺少必需字段: {', '.join(missing)}")
    if df.empty:
        raise SystemExit("输入 csv 为空，无法继续。")

    work = df.copy()
    for col in ["ab_bars", "rebound_ratio", "pnl_pct"]:
        work[col] = pd.to_numeric(work[col], errors="coerce")
    work["is_cold_zone"] = pd.to_numeric(work["is_cold_zone"], errors="coerce")

    if work["ab_bars"].isna().any():
        raise SystemExit("存在 ab_bars 缺失，无法继续。")
    if work["rebound_ratio"].isna().any():
        raise SystemExit("存在 rebound_ratio 缺失，无法继续。")
    if work["pnl_pct"].isna().any():
        raise SystemExit("存在 pnl_pct 缺失，无法继续。")
    if work["is_cold_zone"].isna().any():
        raise SystemExit("存在 is_cold_zone 缺失，无法继续。")

    for group in GROUPS:
        if (work["sample_group"] == group).sum() == 0:
            raise SystemExit(f"样本组为空，无法继续: {group}")

    all_grid = calc_group_grid(work, "all")
    cold = work[work["is_cold_zone"] == 1].copy()
    if cold.empty:
        raise SystemExit("冷区样本为空，无法继续。")
    for group in GROUPS:
        if (cold["sample_group"] == group).sum() == 0:
            raise SystemExit(f"冷区样本组为空，无法继续: {group}")
    cold_grid = calc_group_grid(cold, "cold")

    all_grid.to_csv(args.all_csv, index=False)
    cold_grid.to_csv(args.cold_csv, index=False)

    summary = {
        "run_id": args.run_id,
        "input_csv": args.input_csv,
        "input_rows": int(len(work)),
        "cold_rows": int(len(cold)),
        "groups": GROUPS,
        "ab_buckets": [x[2] for x in AB_BUCKETS],
        "rebound_buckets": [x[2] for x in REB_BUCKETS],
        "group_counts_all": {g: int((work["sample_group"] == g).sum()) for g in GROUPS},
        "group_counts_cold": {g: int((cold["sample_group"] == g).sum()) for g in GROUPS},
        "zero_count_cells_all": int((all_grid["count"] == 0).sum()),
        "zero_count_cells_cold": int((cold_grid["count"] == 0).sum()),
    }
    with open(args.summary_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("=== audit_grid_ab_rebound 完成 ===")
    print(f"run_id       : {args.run_id}")
    print(f"input csv    : {args.input_csv}")
    print(f"all csv      : {args.all_csv}")
    print(f"cold csv     : {args.cold_csv}")
    print(f"summary json : {args.summary_json}")
    print(f"rows         : {len(work)}")
    print(f"cold rows    : {len(cold)}")
    print(f"groups       : {', '.join(GROUPS)}")


if __name__ == "__main__":
    main()
