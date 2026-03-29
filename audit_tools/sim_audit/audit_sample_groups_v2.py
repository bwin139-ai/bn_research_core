#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = ["entry_time", "pnl_pct", "chg_24h", "drop_window_chg"]


def parse_args():
    p = argparse.ArgumentParser(description="Tag audit samples with regime/outcome/cold-zone groups.")
    p.add_argument("--input-csv", required=True)
    p.add_argument("--regime-start", required=True, help="ISO8601, e.g. 2026-01-01T00:00:00+00:00")
    p.add_argument("--run-id", required=True)
    p.add_argument("--tagged-csv", required=True)
    p.add_argument("--cold-csv", required=True)
    p.add_argument("--summary-json", required=True)
    return p.parse_args()


def parse_entry_time(series: pd.Series) -> pd.Series:
    # Fact-based handling:
    # 1) numeric int64 milliseconds since epoch
    # 2) stringified numeric milliseconds
    # 3) normal datetime strings
    if pd.api.types.is_integer_dtype(series) or pd.api.types.is_float_dtype(series):
        return pd.to_datetime(series, unit="ms", utc=True, errors="coerce")

    as_str = series.astype(str).str.strip()
    numeric_mask = as_str.str.fullmatch(r"\d+")
    out = pd.Series(pd.NaT, index=series.index, dtype="datetime64[ns, UTC]")
    if numeric_mask.any():
        out.loc[numeric_mask] = pd.to_datetime(as_str.loc[numeric_mask].astype("int64"), unit="ms", utc=True, errors="coerce")
    if (~numeric_mask).any():
        out.loc[~numeric_mask] = pd.to_datetime(as_str.loc[~numeric_mask], utc=True, errors="coerce")
    return out


def main():
    args = parse_args()
    df = pd.read_csv(args.input_csv)

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise SystemExit(f"输入 csv 缺少必需字段: {', '.join(missing)}")
    if df.empty:
        raise SystemExit("输入 csv 为空，无法继续。")

    entry_dt = parse_entry_time(df["entry_time"])
    if entry_dt.isna().any():
        bad = int(entry_dt.isna().sum())
        raise SystemExit(f"entry_time 解析失败，共 {bad} 行。")

    regime_start = pd.to_datetime(args.regime_start, utc=True, errors="raise")

    df = df.copy()
    df["regime_bucket"] = "pre"
    df.loc[entry_dt >= regime_start, "regime_bucket"] = "post"

    df["outcome_bucket"] = "flat"
    df.loc[df["pnl_pct"] > 0, "outcome_bucket"] = "good"
    df.loc[df["pnl_pct"] < 0, "outcome_bucket"] = "bad"

    df["sample_group"] = df["regime_bucket"] + "_" + df["outcome_bucket"]
    df["is_cold_zone"] = ((df["chg_24h"] <= 0) & (df["drop_window_chg"] <= 0)).astype(int)
    df["sample_group_cold"] = ""
    cold_mask = df["is_cold_zone"] == 1
    df.loc[cold_mask, "sample_group_cold"] = df.loc[cold_mask, "sample_group"] + "_cold"

    Path(args.tagged_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(args.cold_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(args.summary_json).parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(args.tagged_csv, index=False)
    df.loc[cold_mask].to_csv(args.cold_csv, index=False)

    summary = {
        "run_id": args.run_id,
        "regime_start": args.regime_start,
        "input_rows": int(len(df)),
        "tagged_rows": int(len(df)),
        "cold_rows": int(cold_mask.sum()),
        "sample_group_counts": {k: int(v) for k, v in df["sample_group"].value_counts(dropna=False).to_dict().items()},
        "sample_group_cold_counts": {k: int(v) for k, v in df.loc[cold_mask, "sample_group_cold"].value_counts(dropna=False).to_dict().items()},
    }
    with open(args.summary_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("=== audit_sample_groups 完成 ===")
    print(f"run_id       : {args.run_id}")
    print(f"input csv    : {args.input_csv}")
    print(f"tagged csv   : {args.tagged_csv}")
    print(f"cold csv     : {args.cold_csv}")
    print(f"summary json : {args.summary_json}")
    print(f"rows         : {len(df)}")
    print(f"cold rows    : {int(cold_mask.sum())}")


if __name__ == "__main__":
    main()
