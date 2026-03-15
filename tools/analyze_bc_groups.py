#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd


def find_default_paths(run_id: str) -> Tuple[Path, Path]:
    state_dir = Path("output/state")
    audit_csv = state_dir / f"extreme_mae_audit.{run_id}.csv"
    trades_jsonl = state_dir / f"sim_trades.{run_id}.jsonl"
    return audit_csv, trades_jsonl


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception as exc:
                raise ValueError(f"JSONL 解析失败: {path} 第 {line_no} 行: {exc}") from exc
    return rows


def pct_from_trade(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value) * 100.0
    except Exception:
        return None


def normalize_reason(value: Any) -> str:
    return str(value or "").strip().upper()


def build_trade_df(trades_jsonl: Path) -> pd.DataFrame:
    rows = read_jsonl(trades_jsonl)
    out: List[Dict[str, Any]] = []
    for row in rows:
        context = row.get("context") or {}
        out.append(
            {
                "symbol": row.get("symbol"),
                "entry_time": row.get("entry_time"),
                "exit_time": row.get("exit_time"),
                "reason": normalize_reason(row.get("reason")),
                "pnl_pct": pct_from_trade(row.get("pnl_pct")),
                "trigger_name": context.get("trigger_name"),
                "tp_tier": context.get("tp_tier"),
            }
        )
    df = pd.DataFrame(out)
    if df.empty:
        raise ValueError("trades 为空")
    return df


def build_audit_df(audit_csv: Path) -> pd.DataFrame:
    df = pd.read_csv(audit_csv)
    if df.empty:
        raise ValueError("audit csv 为空")
    required = [
        "symbol",
        "entry_time",
        "reason",
        "pnl_pct",
        "mfe_pct",
        "mae_pct",
        "bc_bars",
        "bc_vs_ab_ratio_index",
    ]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"audit csv 缺少列: {missing}")
    return df


def merge_data(audit_df: pd.DataFrame, trade_df: pd.DataFrame) -> pd.DataFrame:
    merged = audit_df.merge(
        trade_df,
        on=["symbol", "entry_time"],
        how="left",
        suffixes=("", "_trade"),
    )
    merged["reason_norm"] = merged["reason"].fillna(merged["reason_trade"]).map(normalize_reason)
    merged["pnl_pct_norm"] = merged["pnl_pct"]
    mask = merged["pnl_pct_norm"].isna()
    merged.loc[mask, "pnl_pct_norm"] = merged.loc[mask, "pnl_pct_trade"]
    return merged


def bucket_bc_bars(v: Any) -> Optional[str]:
    if pd.isna(v):
        return None
    try:
        x = int(v)
    except Exception:
        return None
    if x == 0:
        return "0"
    if x == 1:
        return "1"
    if x == 2:
        return "2"
    if x >= 3:
        return ">=3"
    return None


def bucket_bc_ab(v: Any) -> Optional[str]:
    if pd.isna(v):
        return None
    try:
        x = float(v)
    except Exception:
        return None
    return "<0.15" if x < 0.15 else ">=0.15"


def reason_ratio(df: pd.DataFrame, reason: str) -> float:
    if len(df) == 0:
        return 0.0
    return float((df["reason_norm"] == reason).mean() * 100.0)


def summarize_group(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    order_map = {"0": 0, "1": 1, "2": 2, ">=3": 3, "<0.15": 0, ">=0.15": 1}
    for group_value, sub in df.groupby(group_col, dropna=False):
        if group_value is None or (isinstance(group_value, float) and pd.isna(group_value)):
            continue
        rows.append(
            {
                "group": str(group_value),
                "count": int(len(sub)),
                "avg_pnl_pct": round(float(sub["pnl_pct_norm"].mean()), 4),
                "median_pnl_pct": round(float(sub["pnl_pct_norm"].median()), 4),
                "tp_ratio_pct": round(reason_ratio(sub, "TAKE_PROFIT"), 2),
                "sl_ratio_pct": round(reason_ratio(sub, "STOP_LOSS"), 2),
                "ts_ratio_pct": round(reason_ratio(sub, "TIME_STOP"), 2),
                "avg_mae_pct": round(float(sub["mae_pct"].mean()), 4),
                "avg_mfe_pct": round(float(sub["mfe_pct"].mean()), 4),
            }
        )
    out = pd.DataFrame(rows)
    if not out.empty:
        out["_ord"] = out["group"].map(order_map).fillna(999)
        out = out.sort_values(["_ord", "group"]).drop(columns=["_ord"])
    return out


def summarize_cross(df: pd.DataFrame) -> pd.DataFrame:
    mask = df["bc_bars_bucket"].isin(["0", "1"]) & (df["bc_ab_bucket"] == "<0.15")
    sub = df.loc[mask].copy()
    if sub.empty:
        return pd.DataFrame(
            [{
                "group": "bc_bars in {0,1} AND bc/ab < 0.15",
                "count": 0,
                "avg_pnl_pct": None,
                "median_pnl_pct": None,
                "tp_ratio_pct": None,
                "sl_ratio_pct": None,
                "ts_ratio_pct": None,
                "avg_mae_pct": None,
                "avg_mfe_pct": None,
            }]
        )
    return pd.DataFrame(
        [{
            "group": "bc_bars in {0,1} AND bc/ab < 0.15",
            "count": int(len(sub)),
            "avg_pnl_pct": round(float(sub["pnl_pct_norm"].mean()), 4),
            "median_pnl_pct": round(float(sub["pnl_pct_norm"].median()), 4),
            "tp_ratio_pct": round(reason_ratio(sub, "TAKE_PROFIT"), 2),
            "sl_ratio_pct": round(reason_ratio(sub, "STOP_LOSS"), 2),
            "ts_ratio_pct": round(reason_ratio(sub, "TIME_STOP"), 2),
            "avg_mae_pct": round(float(sub["mae_pct"].mean()), 4),
            "avg_mfe_pct": round(float(sub["mfe_pct"].mean()), 4),
        }]
    )


def print_section(title: str, df: pd.DataFrame) -> None:
    print()
    print("=" * 80)
    print(title)
    print("=" * 80)
    if df.empty:
        print("(empty)")
        return
    print(df.to_string(index=False))


def main() -> None:
    ap = argparse.ArgumentParser(description="分层验证 bc_bars / bc_ab 与交易结果关系")
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--audit-csv", default=None)
    ap.add_argument("--trades-jsonl", default=None)
    ap.add_argument("--out-dir", default="output/state")
    args = ap.parse_args()

    default_audit_csv, default_trades_jsonl = find_default_paths(args.run_id)
    audit_csv = Path(args.audit_csv) if args.audit_csv else default_audit_csv
    trades_jsonl = Path(args.trades_jsonl) if args.trades_jsonl else default_trades_jsonl
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not audit_csv.exists():
        raise FileNotFoundError(f"audit csv 不存在: {audit_csv}")
    if not trades_jsonl.exists():
        raise FileNotFoundError(f"trades jsonl 不存在: {trades_jsonl}")

    audit_df = build_audit_df(audit_csv)
    trade_df = build_trade_df(trades_jsonl)
    df = merge_data(audit_df, trade_df)

    df["bc_bars_bucket"] = df["bc_bars"].map(bucket_bc_bars)
    df["bc_ab_bucket"] = df["bc_vs_ab_ratio_index"].map(bucket_bc_ab)

    bars_table = summarize_group(df.dropna(subset=["bc_bars_bucket"]), "bc_bars_bucket")
    bcab_table = summarize_group(df.dropna(subset=["bc_ab_bucket"]), "bc_ab_bucket")
    cross_table = summarize_cross(df)

    print_section("表 1：按 bc_bars 分层", bars_table)
    print_section("表 2：按 bc/ab 分层", bcab_table)
    print_section("附加：交叉提示", cross_table)

    base_prefix = out_dir / f"bc_layer_validation.{args.run_id}"
    bars_csv = Path(str(base_prefix) + ".bc_bars.csv")
    bcab_csv = Path(str(base_prefix) + ".bc_ab.csv")
    cross_csv = Path(str(base_prefix) + ".cross.csv")
    summary_json = Path(str(base_prefix) + ".summary.json")

    bars_table.to_csv(bars_csv, index=False)
    bcab_table.to_csv(bcab_csv, index=False)
    cross_table.to_csv(cross_csv, index=False)

    summary = {
        "run_id": args.run_id,
        "audit_csv": str(audit_csv),
        "trades_jsonl": str(trades_jsonl),
        "bars_table_csv": str(bars_csv),
        "bcab_table_csv": str(bcab_csv),
        "cross_table_csv": str(cross_csv),
        "total_rows_joined": int(len(df)),
    }
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print()
    print(f"CSV 输出: {bars_csv}")
    print(f"CSV 输出: {bcab_csv}")
    print(f"CSV 输出: {cross_csv}")
    print(f"Summary 输出: {summary_json}")


if __name__ == "__main__":
    main()
