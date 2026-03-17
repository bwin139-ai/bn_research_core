#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd


CHG24_BUCKETS = [
    # chg_24h in sim_trades context is stored as ratio, e.g. 0.10 == 10%
    (-math.inf, -0.20, "<=-20%"),
    (-0.20, -0.10, "(-20%,-10%]"),
    (-0.10, 0.0, "(-10%,0%]"),
    (0.0, 0.10, "(0%,10%]"),
    (0.10, 0.20, "(10%,20%]"),
    (0.20, 0.40, "(20%,40%]"),
    (0.40, 0.80, "(40%,80%]"),
    (0.80, math.inf, "(80%,+inf)"),
]

DW_BUCKETS = [
    (-math.inf, -0.10, "<=-10%"),
    (-0.10, -0.05, "(-10%,-5%]"),
    (-0.05, -0.03, "(-5%,-3%]"),
    (-0.03, -0.01, "(-3%,-1%]"),
    (-0.01, 0.0, "(-1%,0%]"),
    (0.0, 0.05, "(0%,5%]"),
    (0.05, 0.10, "(5%,10%]"),
    (0.10, math.inf, "(10%,+inf)"),
]

REASONS = ["TAKE_PROFIT", "TIME_STOP", "STOP_LOSS", "BREAKEVEN_STOP"]


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit 24hChg × drop_window_chg 2D buckets for Snapback.")
    p.add_argument("--run-id", required=True)
    p.add_argument("--state-dir", default="output/state")
    p.add_argument("--out-csv", required=True, help="Per-trade detail CSV with 2D buckets.")
    p.add_argument("--summary-json", required=True)
    p.add_argument("--grid-csv", required=True, help="2D grid cell statistics CSV.")
    p.add_argument("--worst-csv", default="", help="Optional CSV for worst pnl trades with bucket labels.")
    return p.parse_args()


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def first_not_none(*vals: Any) -> Any:
    for v in vals:
        if v is not None:
            return v
    return None


def to_float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except Exception:
        return None
    if math.isnan(x):
        return None
    return x


def bucket_of(val: float | None, specs: list[tuple[float, float, str]]) -> str | None:
    if val is None:
        return None
    for lo, hi, label in specs:
        if val > lo and val <= hi:
            return label
        if lo == -math.inf and val <= hi:
            return label
    return None


def pct(v: float | None) -> float | None:
    return None if v is None else v * 100.0


def safe_mean(s: pd.Series) -> float | None:
    s = pd.to_numeric(s, errors="coerce").dropna()
    return None if s.empty else float(s.mean())


def safe_median(s: pd.Series) -> float | None:
    s = pd.to_numeric(s, errors="coerce").dropna()
    return None if s.empty else float(s.median())


def safe_count(s: pd.Series) -> int:
    return int(pd.to_numeric(s, errors="coerce").notna().sum())


def build_df(trades: list[dict[str, Any]], run_cfg: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for t in trades:
        ctx = t.get("context") or {}
        params = t.get("params") or {}
        s_close = to_float(first_not_none(ctx.get("s_close"), t.get("s_close")))
        c_close = to_float(first_not_none(ctx.get("c_close"), ctx.get("c_contract_price"), t.get("c_close"), t.get("c_contract_price")))
        dw = to_float(first_not_none(ctx.get("drop_window_chg"), t.get("drop_window_chg")))
        if dw is None and s_close not in (None, 0.0) and c_close is not None:
            dw = (c_close - s_close) / s_close

        row = {
            "symbol": first_not_none(t.get("symbol"), t.get("sym")),
            "reason": first_not_none(t.get("reason"), t.get("exit_reason")),
            "pnl_pct": to_float(first_not_none(t.get("pnl_pct"), ctx.get("pnl_pct"))),
            "mfe_pct": to_float(first_not_none(t.get("mfe_pct"), ctx.get("mfe_pct"))),
            "mae_pct": to_float(first_not_none(t.get("mae_pct"), ctx.get("mae_pct"))),
            "chg_24h": to_float(first_not_none(ctx.get("chg_24h"), t.get("chg_24h"), ctx.get("change_24h"))),
            "drop_window_chg": dw,
            "drop_pct": to_float(first_not_none(ctx.get("drop_pct"), t.get("drop_pct"))),
            "rebound_ratio": to_float(first_not_none(ctx.get("rebound_ratio"), t.get("rebound_ratio"))),
            "bc_ab_ratio": to_float(first_not_none(ctx.get("bc_ab_ratio"), t.get("bc_ab_ratio"))),
            "vol_ratio": to_float(first_not_none(ctx.get("vol_ratio"), t.get("vol_ratio"))),
            "drop_window_mins": first_not_none(params.get("drop_window_mins"), run_cfg.get("drop_window_mins")),
            "s_time": first_not_none(ctx.get("s_time"), t.get("s_time")),
            "s_close": s_close,
            "c_time": first_not_none(ctx.get("c_time"), t.get("c_time")),
            "c_close": c_close,
            "a_time": first_not_none(ctx.get("a_time"), t.get("a_time")),
            "a_price": to_float(first_not_none(ctx.get("a_price"), t.get("a_price"), ctx.get("recent_high_price"))),
            "b_time": first_not_none(ctx.get("b_time"), t.get("b_time")),
            "b_price": to_float(first_not_none(ctx.get("b_price"), t.get("b_price"), ctx.get("b_contract_price"))),
        }
        row["chg_24h_bucket"] = bucket_of(row["chg_24h"], CHG24_BUCKETS)
        row["drop_window_bucket"] = bucket_of(row["drop_window_chg"], DW_BUCKETS)
        row["cell_bucket"] = None
        if row["chg_24h_bucket"] and row["drop_window_bucket"]:
            row["cell_bucket"] = f"{row['chg_24h_bucket']} | {row['drop_window_bucket']}"
        rows.append(row)
    return pd.DataFrame(rows)


def reason_stats(df: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for reason, g in df.groupby("reason", dropna=False):
        out[str(reason)] = {
            "count": int(len(g)),
            "avg_pnl_pct": safe_mean(g["pnl_pct"]),
            "median_pnl_pct": safe_median(g["pnl_pct"]),
            "avg_drop_window_chg_pct": pct(safe_mean(g["drop_window_chg"])),
            "median_drop_window_chg_pct": pct(safe_median(g["drop_window_chg"])),
            "avg_24h_chg_pct": pct(safe_mean(g["chg_24h"])),
            "median_24h_chg_pct": pct(safe_median(g["chg_24h"])),
        }
    return out


def build_grid(df: pd.DataFrame) -> pd.DataFrame:
    cells: list[dict[str, Any]] = []
    valid = df.dropna(subset=["chg_24h_bucket", "drop_window_bucket"]).copy()
    for (b24, bdw), g in valid.groupby(["chg_24h_bucket", "drop_window_bucket"], dropna=False):
        row: dict[str, Any] = {
            "chg_24h_bucket": b24,
            "drop_window_bucket": bdw,
            "count": int(len(g)),
            "avg_pnl_pct": safe_mean(g["pnl_pct"]),
            "median_pnl_pct": safe_median(g["pnl_pct"]),
            "avg_chg_24h_pct": pct(safe_mean(g["chg_24h"])),
            "median_chg_24h_pct": pct(safe_median(g["chg_24h"])),
            "avg_drop_window_chg_pct": pct(safe_mean(g["drop_window_chg"])),
            "median_drop_window_chg_pct": pct(safe_median(g["drop_window_chg"])),
            "avg_drop_pct": safe_mean(g["drop_pct"]),
            "avg_rebound_ratio": safe_mean(g["rebound_ratio"]),
            "avg_bc_ab_ratio": safe_mean(g["bc_ab_ratio"]),
            "avg_vol_ratio": safe_mean(g["vol_ratio"]),
            "rows_with_mae_pct": safe_count(g["mae_pct"]),
            "rows_with_mfe_pct": safe_count(g["mfe_pct"]),
            "avg_mae_pct": safe_mean(g["mae_pct"]),
            "median_mae_pct": safe_median(g["mae_pct"]),
            "avg_mfe_pct": safe_mean(g["mfe_pct"]),
            "median_mfe_pct": safe_median(g["mfe_pct"]),
        }
        for reason in REASONS:
            c = int((g["reason"] == reason).sum())
            row[f"{reason}_count"] = c
            row[f"{reason}_rate"] = None if len(g) == 0 else float(c / len(g))
        cells.append(row)
    grid = pd.DataFrame(cells)
    if not grid.empty:
        grid = grid.sort_values(["chg_24h_bucket", "drop_window_bucket"], kind="stable")
    return grid


def grid_dict(grid: pd.DataFrame) -> dict[str, Any]:
    out: dict[str, Any] = defaultdict(dict)
    for _, r in grid.iterrows():
        out[str(r["chg_24h_bucket"])][str(r["drop_window_bucket"])] = {
            "count": int(r["count"]),
            "avg_pnl_pct": None if pd.isna(r["avg_pnl_pct"]) else float(r["avg_pnl_pct"]),
            "median_pnl_pct": None if pd.isna(r["median_pnl_pct"]) else float(r["median_pnl_pct"]),
            "TAKE_PROFIT_rate": None if pd.isna(r["TAKE_PROFIT_rate"]) else float(r["TAKE_PROFIT_rate"]),
            "TIME_STOP_rate": None if pd.isna(r["TIME_STOP_rate"]) else float(r["TIME_STOP_rate"]),
            "STOP_LOSS_rate": None if pd.isna(r["STOP_LOSS_rate"]) else float(r["STOP_LOSS_rate"]),
            "BREAKEVEN_STOP_rate": None if pd.isna(r["BREAKEVEN_STOP_rate"]) else float(r["BREAKEVEN_STOP_rate"]),
        }
    return dict(out)


def top_records(df: pd.DataFrame, col: str, n: int, ascending: bool) -> list[dict[str, Any]]:
    if col not in df.columns:
        return []
    x = df.copy()
    x[col] = pd.to_numeric(x[col], errors="coerce")
    x = x.dropna(subset=[col])
    if x.empty:
        return []
    x = x.sort_values(col, ascending=ascending).head(n)
    cols = [
        "symbol", "reason", "pnl_pct", "mfe_pct", "mae_pct", "chg_24h",
        "drop_window_chg", "chg_24h_bucket", "drop_window_bucket", "cell_bucket",
        "drop_pct", "rebound_ratio", "bc_ab_ratio", "vol_ratio",
    ]
    out: list[dict[str, Any]] = []
    for _, r in x.iterrows():
        item: dict[str, Any] = {}
        for c in cols:
            if c in r.index:
                v = r[c]
                item[c] = None if pd.isna(v) else (float(v) if isinstance(v, (int, float)) else v)
        out.append(item)
    return out


def main() -> None:
    args = parse_args()
    state_dir = Path(args.state_dir)
    trades_path = state_dir / f"sim_trades.{args.run_id}.jsonl"
    summary_path = state_dir / f"sim_summary.{args.run_id}.json"
    if not trades_path.exists():
        raise SystemExit(f"交易文件不存在: {trades_path}")
    if not summary_path.exists():
        raise SystemExit(f"Summary 文件不存在: {summary_path}")

    trades = load_jsonl(trades_path)
    sim_summary = load_json(summary_path)
    run_cfg = sim_summary.get("run_config") or {}

    df = build_df(trades, run_cfg)
    Path(args.out_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(args.summary_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.grid_csv).parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(args.out_csv, index=False)
    grid = build_grid(df)
    grid.to_csv(args.grid_csv, index=False)

    summary = {
        "run_id": args.run_id,
        "trades_path": str(trades_path),
        "summary_path": str(summary_path),
        "run_config": {
            "min_24h_chg": run_cfg.get("min_24h_chg"),
            "max_24h_chg": run_cfg.get("max_24h_chg"),
            "drop_window_mins": run_cfg.get("drop_window_mins"),
            "min_drop_window_chg": run_cfg.get("min_drop_window_chg"),
            "max_drop_window_chg": run_cfg.get("max_drop_window_chg"),
        },
        "counts": {
            "total_rows": int(len(df)),
            "rows_with_chg_24h": safe_count(df["chg_24h"]),
            "rows_with_drop_window_chg": safe_count(df["drop_window_chg"]),
            "rows_with_both_axes": int(df.dropna(subset=["chg_24h_bucket", "drop_window_bucket"]).shape[0]),
            "rows_with_mae_pct": safe_count(df["mae_pct"]),
            "rows_with_mfe_pct": safe_count(df["mfe_pct"]),
        },
        "axis_distribution": {
            "chg_24h_stats": {
                "avg_pct": pct(safe_mean(df["chg_24h"])),
                "median_pct": pct(safe_median(df["chg_24h"])),
            },
            "drop_window_chg_stats": {
                "avg_pct": pct(safe_mean(df["drop_window_chg"])),
                "median_pct": pct(safe_median(df["drop_window_chg"])),
            },
            "chg_24h_bucket_counts": df["chg_24h_bucket"].value_counts(dropna=False).to_dict(),
            "drop_window_bucket_counts": df["drop_window_bucket"].value_counts(dropna=False).to_dict(),
        },
        "reason_stats": reason_stats(df),
        "grid": grid_dict(grid),
        "best_pnl_top20": top_records(df, "pnl_pct", 20, ascending=False),
        "worst_pnl_top20": top_records(df, "pnl_pct", 20, ascending=True),
        "worst_mae_top20": top_records(df, "mae_pct", 20, ascending=True),
        "best_mfe_top20": top_records(df, "mfe_pct", 20, ascending=False),
    }

    with Path(args.summary_json).open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    if args.worst_csv:
        worst_df = pd.DataFrame(summary["worst_pnl_top20"])
        Path(args.worst_csv).parent.mkdir(parents=True, exist_ok=True)
        worst_df.to_csv(args.worst_csv, index=False)

    print("=== audit_24hchg_vs_drop_window 完成 ===")
    print(f"run_id       : {args.run_id}")
    print(f"trades       : {trades_path}")
    print(f"summary      : {summary_path}")
    print(f"detail csv   : {args.out_csv}")
    print(f"summary json : {args.summary_json}")
    print(f"grid csv     : {args.grid_csv}")
    if args.worst_csv:
        print(f"worst csv    : {args.worst_csv}")
    print(f"rows         : {len(df)}")
    print(f"with both    : {summary['counts']['rows_with_both_axes']}")


if __name__ == "__main__":
    main()
