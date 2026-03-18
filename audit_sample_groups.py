#!/usr/bin/env python3
"""Tag audit sample groups for downstream Snapback audit analysis.

This script reads an existing per-trade audit CSV, validates required columns,
adds standardized sample-group labels, and exports:
1) a tagged full CSV
2) a cold-zone-only CSV
3) a summary JSON
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, Any

import pandas as pd

REQUIRED_COLUMNS = ["entry_time", "pnl_pct", "chg_24h", "drop_window_chg"]
DEFAULT_REGIME_START = "2026-01-01T00:00:00+00:00"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Tag audit samples into pre/post, good/bad, and cold-zone groups."
    )
    parser.add_argument("--input-csv", required=True, help="Input audit detail CSV.")
    parser.add_argument(
        "--regime-start",
        default=DEFAULT_REGIME_START,
        help=f"Regime split timestamp (default: {DEFAULT_REGIME_START})",
    )
    parser.add_argument("--tagged-csv", required=True, help="Output full tagged CSV.")
    parser.add_argument("--cold-csv", required=True, help="Output cold-zone-only CSV.")
    parser.add_argument("--summary-json", required=True, help="Output summary JSON.")
    parser.add_argument("--run-id", default="", help="Optional run id for summary JSON.")
    return parser.parse_args()


def ensure_parent_dir(path_str: str) -> None:
    Path(path_str).parent.mkdir(parents=True, exist_ok=True)


def require_columns(df: pd.DataFrame) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise SystemExit(f"配置加载失败: 输入CSV缺失必需字段: {missing}")


def parse_times(df: pd.DataFrame, column: str) -> pd.Series:
    parsed = pd.to_datetime(df[column], utc=True, errors="coerce")
    bad_count = int(parsed.isna().sum())
    if bad_count > 0:
        raise SystemExit(f"时间解析失败: 字段 {column} 存在 {bad_count} 行无法解析")
    return parsed


def build_group_columns(df: pd.DataFrame, regime_start_ts: pd.Timestamp) -> pd.DataFrame:
    out = df.copy()

    out["entry_time"] = parse_times(out, "entry_time")

    out["regime_bucket"] = out["entry_time"].apply(
        lambda x: "pre" if x < regime_start_ts else "post"
    )

    def map_outcome(v: Any) -> str:
        if pd.isna(v):
            raise SystemExit("字段 pnl_pct 存在空值，无法分配 outcome_bucket")
        if v > 0:
            return "good"
        if v < 0:
            return "bad"
        return "flat"

    out["outcome_bucket"] = out["pnl_pct"].apply(map_outcome)
    out["sample_group"] = out["regime_bucket"] + "_" + out["outcome_bucket"]

    def map_cold(row: pd.Series) -> int:
        chg_24h = row["chg_24h"]
        drop_window_chg = row["drop_window_chg"]
        if pd.isna(chg_24h) or pd.isna(drop_window_chg):
            raise SystemExit("字段 chg_24h/drop_window_chg 存在空值，无法计算 is_cold_zone")
        return int((chg_24h <= 0) and (drop_window_chg <= 0))

    out["is_cold_zone"] = out.apply(map_cold, axis=1)
    out["sample_group_cold"] = out.apply(
        lambda row: f"{row['sample_group']}_cold" if row["is_cold_zone"] == 1 else "",
        axis=1,
    )

    return out


def count_values(series: pd.Series, expected_keys: list[str]) -> Dict[str, int]:
    vc = series.value_counts(dropna=False).to_dict()
    return {key: int(vc.get(key, 0)) for key in expected_keys}


def reason_summary(df: pd.DataFrame) -> Dict[str, Dict[str, int]]:
    if "reason" not in df.columns:
        return {}
    result: Dict[str, Dict[str, int]] = {}
    for group_name, group_df in df.groupby("sample_group"):
        vc = group_df["reason"].value_counts(dropna=False).to_dict()
        result[str(group_name)] = {str(k): int(v) for k, v in vc.items()}
    return result


def build_summary(df: pd.DataFrame, cold_df: pd.DataFrame, run_id: str, regime_start: str) -> Dict[str, Any]:
    sample_groups = [
        "pre_good", "pre_bad", "pre_flat",
        "post_good", "post_bad", "post_flat",
    ]
    sample_groups_cold = [f"{g}_cold" for g in sample_groups]

    summary: Dict[str, Any] = {
        "run_id": run_id,
        "regime_start": regime_start,
        "input_rows": int(len(df)),
        "tagged_rows": int(len(df)),
        "cold_rows": int(len(cold_df)),
        "sample_group_counts": count_values(df["sample_group"], sample_groups),
        "sample_group_cold_counts": count_values(
            cold_df["sample_group_cold"], sample_groups_cold
        ),
    }

    rs = reason_summary(df)
    if rs:
        summary["reason_counts_by_sample_group"] = rs

    return summary


def main() -> None:
    args = parse_args()

    input_path = Path(args.input_csv)
    if not input_path.exists():
        raise SystemExit(f"输入文件不存在: {input_path}")

    df = pd.read_csv(input_path)
    if df.empty:
        raise SystemExit(f"输入CSV为空: {input_path}")

    require_columns(df)

    regime_start_ts = pd.to_datetime(args.regime_start, utc=True, errors="coerce")
    if pd.isna(regime_start_ts):
        raise SystemExit(f"regime_start 无法解析: {args.regime_start}")

    tagged_df = build_group_columns(df, regime_start_ts)
    cold_df = tagged_df[tagged_df["is_cold_zone"] == 1].copy()

    ensure_parent_dir(args.tagged_csv)
    ensure_parent_dir(args.cold_csv)
    ensure_parent_dir(args.summary_json)

    tagged_df.to_csv(args.tagged_csv, index=False)
    cold_df.to_csv(args.cold_csv, index=False)

    summary = build_summary(tagged_df, cold_df, args.run_id, args.regime_start)
    with open(args.summary_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("=== audit_sample_groups 完成 ===")
    print(f"run_id       : {args.run_id}")
    print(f"input csv    : {args.input_csv}")
    print(f"tagged csv   : {args.tagged_csv}")
    print(f"cold csv     : {args.cold_csv}")
    print(f"summary json : {args.summary_json}")
    print(f"rows         : {len(tagged_df)}")
    print(f"cold rows    : {len(cold_df)}")


if __name__ == "__main__":
    main()
