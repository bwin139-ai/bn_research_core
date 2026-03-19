#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
from pathlib import Path
from typing import Dict, List

import pandas as pd

DEFAULT_FIELDS = [
    "bc_ab_ratio",
    "drop_pct",
    "rebound_ratio",
    "chg_24h",
    "drop_window_chg",
]

COMPARE_PAIRS = [
    ("pre_good_vs_post_bad_all", "pre_good", "post_bad", "all"),
    ("pre_good_vs_post_bad_cold", "pre_good", "post_bad", "cold"),
    ("post_good_vs_post_bad_all", "post_good", "post_bad", "all"),
    ("post_good_vs_post_bad_cold", "post_good", "post_bad", "cold"),
]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Audit single-field distributions on tagged sample groups."
    )
    p.add_argument("--input-csv", required=True, help="Tagged CSV from audit_sample_groups.py")
    p.add_argument("--run-id", default="", help="Run id for summary only")
    p.add_argument(
        "--fields",
        nargs="+",
        default=DEFAULT_FIELDS,
        help="Field names to audit. Default uses currently available core C-pre fields.",
    )
    p.add_argument("--all-csv", required=True, help="Output CSV for all-sample distributions")
    p.add_argument("--cold-csv", required=True, help="Output CSV for cold-zone distributions")
    p.add_argument("--diff-csv", required=True, help="Output CSV for pairwise difference summary")
    p.add_argument("--summary-json", required=True, help="Output JSON summary")
    return p.parse_args()


def ensure_parent(path_str: str) -> None:
    Path(path_str).parent.mkdir(parents=True, exist_ok=True)


def require_columns(df: pd.DataFrame, cols: List[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise SystemExit(f"{label} 缺失必需字段: {missing}")


def stats_for_series(s: pd.Series) -> Dict[str, float]:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return {
            "count": 0,
            "mean": None,
            "median": None,
            "std": None,
            "min": None,
            "p10": None,
            "p25": None,
            "p75": None,
            "p90": None,
            "max": None,
        }
    return {
        "count": int(s.shape[0]),
        "mean": float(s.mean()),
        "median": float(s.median()),
        "std": None if s.shape[0] < 2 else float(s.std(ddof=1)),
        "min": float(s.min()),
        "p10": float(s.quantile(0.10)),
        "p25": float(s.quantile(0.25)),
        "p75": float(s.quantile(0.75)),
        "p90": float(s.quantile(0.90)),
        "max": float(s.max()),
    }


def build_dist_rows(df: pd.DataFrame, groups: List[str], fields: List[str]) -> List[Dict]:
    rows: List[Dict] = []
    for group_name in groups:
        gdf = df[df["sample_group"] == group_name]
        if gdf.empty:
            raise SystemExit(f"样本组为空，无法继续: {group_name}")
        for field in fields:
            stats = stats_for_series(gdf[field])
            row = {"group_name": group_name, "field_name": field}
            row.update(stats)
            rows.append(row)
    return rows


def find_stats(dist_df: pd.DataFrame, group_name: str, field_name: str) -> pd.Series:
    hit = dist_df[(dist_df["group_name"] == group_name) & (dist_df["field_name"] == field_name)]
    if hit.empty:
        raise SystemExit(f"未找到统计结果: group={group_name}, field={field_name}")
    return hit.iloc[0]


def build_diff_rows(all_df: pd.DataFrame, cold_df: pd.DataFrame, fields: List[str]) -> List[Dict]:
    rows: List[Dict] = []
    for compare_pair, base_group, target_group, scope in COMPARE_PAIRS:
        src = all_df if scope == "all" else cold_df
        for field in fields:
            base = find_stats(src, base_group, field)
            target = find_stats(src, target_group, field)

            base_median = base["median"]
            target_median = target["median"]
            median_diff = None if pd.isna(base_median) or pd.isna(target_median) else float(target_median - base_median)

            base_mean = base["mean"]
            target_mean = target["mean"]
            mean_diff = None if pd.isna(base_mean) or pd.isna(target_mean) else float(target_mean - base_mean)

            base_p25 = base["p25"]
            target_p25 = target["p25"]
            p25_diff = None if pd.isna(base_p25) or pd.isna(target_p25) else float(target_p25 - base_p25)

            base_p75 = base["p75"]
            target_p75 = target["p75"]
            p75_diff = None if pd.isna(base_p75) or pd.isna(target_p75) else float(target_p75 - base_p75)

            direction = "equal_or_near"
            if median_diff is not None:
                if median_diff > 0:
                    direction = "target_higher"
                elif median_diff < 0:
                    direction = "target_lower"

            rows.append(
                {
                    "compare_pair": compare_pair,
                    "field_name": field,
                    "base_group": base_group,
                    "target_group": target_group,
                    "scope": scope,
                    "base_count": int(base["count"]),
                    "target_count": int(target["count"]),
                    "base_mean": None if pd.isna(base_mean) else float(base_mean),
                    "target_mean": None if pd.isna(target_mean) else float(target_mean),
                    "mean_diff": mean_diff,
                    "base_median": None if pd.isna(base_median) else float(base_median),
                    "target_median": None if pd.isna(target_median) else float(target_median),
                    "median_diff": median_diff,
                    "base_p25": None if pd.isna(base_p25) else float(base_p25),
                    "target_p25": None if pd.isna(target_p25) else float(target_p25),
                    "p25_diff": p25_diff,
                    "base_p75": None if pd.isna(base_p75) else float(base_p75),
                    "target_p75": None if pd.isna(target_p75) else float(target_p75),
                    "p75_diff": p75_diff,
                    "direction": direction,
                }
            )
    return rows


def main() -> None:
    args = parse_args()
    input_csv = Path(args.input_csv)
    if not input_csv.exists():
        raise SystemExit(f"输入文件不存在: {input_csv}")

    df = pd.read_csv(input_csv)
    if df.empty:
        raise SystemExit(f"输入表为空: {input_csv}")

    require_columns(df, ["sample_group", "is_cold_zone"], "输入表")
    require_columns(df, args.fields, "输入表")

    groups = ["pre_good", "post_bad", "post_good"]
    for g in groups:
        if (df["sample_group"] == g).sum() == 0:
            raise SystemExit(f"样本组为空，无法继续: {g}")

    cold_df_src = df[df["is_cold_zone"] == 1].copy()
    if cold_df_src.empty:
        raise SystemExit("冷区样本为空，无法继续")

    for g in groups:
        if (cold_df_src["sample_group"] == g).sum() == 0:
            raise SystemExit(f"冷区样本组为空，无法继续: {g}")

    all_rows = build_dist_rows(df, groups, args.fields)
    cold_rows = build_dist_rows(cold_df_src, groups, args.fields)

    all_out = pd.DataFrame(all_rows)
    cold_out = pd.DataFrame(cold_rows)
    diff_out = pd.DataFrame(build_diff_rows(all_out, cold_out, args.fields))

    for out_path in [args.all_csv, args.cold_csv, args.diff_csv, args.summary_json]:
        ensure_parent(out_path)

    all_out.to_csv(args.all_csv, index=False)
    cold_out.to_csv(args.cold_csv, index=False)
    diff_out.to_csv(args.diff_csv, index=False)

    missing_counts = {field: int(pd.to_numeric(df[field], errors="coerce").isna().sum()) for field in args.fields}
    summary = {
        "run_id": args.run_id,
        "input_csv": str(input_csv),
        "input_rows": int(df.shape[0]),
        "cold_rows": int(cold_df_src.shape[0]),
        "fields": args.fields,
        "compare_pairs": [x[0] for x in COMPARE_PAIRS],
        "group_counts_all": {g: int((df["sample_group"] == g).sum()) for g in groups},
        "group_counts_cold": {g: int((cold_df_src["sample_group"] == g).sum()) for g in groups},
        "missing_value_counts_by_field": missing_counts,
    }
    with open(args.summary_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("=== audit_field_distributions 完成 ===")
    print(f"run_id       : {args.run_id}")
    print(f"input csv    : {args.input_csv}")
    print(f"all csv      : {args.all_csv}")
    print(f"cold csv     : {args.cold_csv}")
    print(f"diff csv     : {args.diff_csv}")
    print(f"summary json : {args.summary_json}")
    print(f"rows         : {df.shape[0]}")
    print(f"cold rows    : {cold_df_src.shape[0]}")
    print(f"fields       : {', '.join(args.fields)}")


if __name__ == "__main__":
    main()
