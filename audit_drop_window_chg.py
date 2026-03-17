#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


REASON_ORDER = ["TAKE_PROFIT", "TIME_STOP", "STOP_LOSS"]
DROP_WINDOW_BUCKETS = [
    (-10**9, -0.10, "<=-10%"),
    (-0.10, -0.05, "(-10%,-5%]"),
    (-0.05, -0.03, "(-5%,-3%]"),
    (-0.03, -0.01, "(-3%,-1%]"),
    (-0.01, 0.00, "(-1%,0%]"),
    (0.00, 10**9, ">0%"),
]
MAE_BUCKETS = [
    (-10**9, -0.05, "<=-5%"),
    (-0.05, -0.03, "(-5%,-3%]"),
    (-0.03, -0.02, "(-3%,-2%]"),
    (-0.02, -0.01, "(-2%,-1%]"),
    (-0.01, 0.00, "(-1%,0%]"),
    (0.00, 10**9, ">=0%"),
]


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        if isinstance(v, str) and not v.strip():
            return None
        x = float(v)
        if math.isnan(x):
            return None
        return x
    except Exception:
        return None



def _safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        if isinstance(v, str) and not v.strip():
            return None
        return int(float(v))
    except Exception:
        return None



def _fmt_time_ms(ms: Any) -> Optional[str]:
    iv = _safe_int(ms)
    if iv is None:
        return None
    try:
        return (pd.to_datetime(iv, unit="ms", utc=True) + pd.Timedelta(hours=8)).strftime("%Y-%m-%d %H:%M")
    except Exception:
        return None



def _bucket_value(x: Optional[float], specs: List[tuple[float, float, str]]) -> Optional[str]:
    if x is None:
        return None
    for lo, hi, label in specs:
        if x > lo and x <= hi:
            return label
    return None



def _reason_sort_key(reason: str) -> int:
    try:
        return REASON_ORDER.index(reason)
    except ValueError:
        return len(REASON_ORDER)



def _find_run_summary(run_id: str) -> Optional[Path]:
    p = Path("output/state") / f"sim_summary.{run_id}.json"
    return p if p.exists() else None



def _find_run_trades(run_id: str) -> Optional[Path]:
    p = Path("output/state") / f"sim_trades.{run_id}.jsonl"
    return p if p.exists() else None



def _load_trades(trades_path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with trades_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows



def _extract_row(tr: Dict[str, Any], run_config: Dict[str, Any]) -> Dict[str, Any]:
    ctx = tr.get("context") or {}
    params = tr.get("params") or {}

    a_time_ms = ctx.get("a_time")
    b_time_ms = ctx.get("b_time")
    c_time_ms = ctx.get("c_time") or tr.get("signal_time")
    s_time_ms = ctx.get("s_time")

    a_price = _safe_float(ctx.get("a_price", ctx.get("recent_high_price")))
    b_price = _safe_float(ctx.get("b_price", ctx.get("b_contract_price")))
    c_price = _safe_float(ctx.get("c_price", tr.get("signal_price")))
    s_close = _safe_float(ctx.get("s_close"))

    ab_bars = _safe_float(ctx.get("ab_bars"))
    bc_bars = _safe_float(ctx.get("bc_bars"))
    bc_ab_ratio = _safe_float(ctx.get("bc_ab_ratio"))
    if bc_ab_ratio is None and ab_bars not in (None, 0) and bc_bars is not None:
        bc_ab_ratio = bc_bars / ab_bars

    drop_window_mins = _safe_int(params.get("drop_window_mins", run_config.get("drop_window_mins")))
    drop_window_chg = _safe_float(ctx.get("drop_window_chg"))
    if drop_window_chg is None and s_close not in (None, 0) and c_price is not None:
        drop_window_chg = (c_price - s_close) / s_close

    row = {
        "symbol": tr.get("symbol"),
        "reason": tr.get("reason"),
        "entry_time": _fmt_time_ms(tr.get("entry_time")),
        "exit_time": _fmt_time_ms(tr.get("exit_time")),
        "hold_mins": _safe_float(tr.get("hold_mins")),
        "pnl_pct": _safe_float(tr.get("pnl_pct")),
        "mfe_pct": _safe_float(tr.get("mfe_pct")),
        "mae_pct": _safe_float(tr.get("mae_pct")),
        "chg_24h": _safe_float(tr.get("chg_24h")),
        "vol_24h": _safe_float(tr.get("vol_24h")),
        "drop_pct": _safe_float(ctx.get("drop_pct")),
        "rebound_ratio": _safe_float(ctx.get("rebound_ratio")),
        "vol_ratio": _safe_float(ctx.get("vol_ratio")),
        "ab_bars": ab_bars,
        "bc_bars": bc_bars,
        "bc_ab_ratio": bc_ab_ratio,
        "drop_window_mins": drop_window_mins,
        "drop_window_chg": drop_window_chg,
        "drop_window_chg_bucket": _bucket_value(drop_window_chg, DROP_WINDOW_BUCKETS),
        "mae_bucket": _bucket_value(_safe_float(tr.get("mae_pct")), MAE_BUCKETS),
        "s_time": _fmt_time_ms(s_time_ms),
        "s_close": s_close,
        "a_time": _fmt_time_ms(a_time_ms),
        "a_price": a_price,
        "b_time": _fmt_time_ms(b_time_ms),
        "b_price": b_price,
        "c_time": _fmt_time_ms(c_time_ms),
        "c_price": c_price,
    }
    row["is_loss"] = bool(row["pnl_pct"] is not None and row["pnl_pct"] < 0)
    row["is_extreme_mae"] = bool(row["mae_pct"] is not None and row["mae_pct"] <= -0.05)
    row["is_extreme_loss"] = bool(row["pnl_pct"] is not None and row["pnl_pct"] <= -0.02)
    return row



def _series_stats(s: pd.Series) -> Dict[str, Any]:
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return {"count": 0}
    q = s.quantile([0.1, 0.25, 0.5, 0.75, 0.9]).to_dict()
    return {
        "count": int(s.shape[0]),
        "mean": float(s.mean()),
        "median": float(s.median()),
        "min": float(s.min()),
        "max": float(s.max()),
        "p10": float(q.get(0.1)),
        "p25": float(q.get(0.25)),
        "p50": float(q.get(0.5)),
        "p75": float(q.get(0.75)),
        "p90": float(q.get(0.9)),
    }



def build_summary(df: pd.DataFrame, run_id: str, run_config: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "run_id": run_id,
        "strategy_name": run_config.get("strategy_name"),
        "drop_window_mins": run_config.get("drop_window_mins"),
        "min_drop_window_chg": run_config.get("min_drop_window_chg"),
        "max_drop_window_chg": run_config.get("max_drop_window_chg"),
        "totals": {
            "rows": int(len(df)),
            "rows_with_drop_window_chg": int(df["drop_window_chg"].notna().sum()),
            "rows_with_s_time": int(df["s_time"].notna().sum()),
            "rows_with_s_close": int(df["s_close"].notna().sum()),
        },
        "drop_window_chg_overall": _series_stats(df["drop_window_chg"]),
        "by_reason": {},
        "bucket_by_reason": {},
        "mae_bucket_by_drop_window_bucket": {},
        "segments": {},
        "extreme_samples": {},
    }

    for reason, g in df.groupby("reason", dropna=False):
        reason_key = str(reason)
        out["by_reason"][reason_key] = {
            "count": int(len(g)),
            "avg_pnl_pct": float(pd.to_numeric(g["pnl_pct"], errors="coerce").mean()),
            "median_pnl_pct": float(pd.to_numeric(g["pnl_pct"], errors="coerce").median()),
            "avg_mae_pct": float(pd.to_numeric(g["mae_pct"], errors="coerce").mean()),
            "median_mae_pct": float(pd.to_numeric(g["mae_pct"], errors="coerce").median()),
            "drop_window_chg": _series_stats(g["drop_window_chg"]),
        }

    bucket_reason = (
        df.dropna(subset=["drop_window_chg_bucket", "reason"])
        .groupby(["drop_window_chg_bucket", "reason"], dropna=False)
        .agg(
            count=("symbol", "count"),
            avg_pnl_pct=("pnl_pct", "mean"),
            median_pnl_pct=("pnl_pct", "median"),
            avg_mae_pct=("mae_pct", "mean"),
            median_mae_pct=("mae_pct", "median"),
        )
        .reset_index()
    )
    out["bucket_by_reason"] = bucket_reason.to_dict(orient="records")

    mae_cross = (
        df.dropna(subset=["drop_window_chg_bucket", "mae_bucket"])
        .groupby(["drop_window_chg_bucket", "mae_bucket"], dropna=False)
        .agg(count=("symbol", "count"), avg_pnl_pct=("pnl_pct", "mean"), avg_mae_pct=("mae_pct", "mean"))
        .reset_index()
    )
    out["mae_bucket_by_drop_window_bucket"] = mae_cross.to_dict(orient="records")

    segments = {
        "positive_24h": df[df["chg_24h"].fillna(-999) > 0],
        "non_positive_24h": df[df["chg_24h"].fillna(0) <= 0],
        "extreme_mae": df[df["is_extreme_mae"]],
        "extreme_loss": df[df["is_extreme_loss"]],
        "stop_loss_only": df[df["reason"] == "STOP_LOSS"],
        "time_stop_only": df[df["reason"] == "TIME_STOP"],
        "take_profit_only": df[df["reason"] == "TAKE_PROFIT"],
    }
    for name, g in segments.items():
        out["segments"][name] = {
            "count": int(len(g)),
            "drop_window_chg": _series_stats(g["drop_window_chg"]),
            "avg_pnl_pct": None if g.empty else float(pd.to_numeric(g["pnl_pct"], errors="coerce").mean()),
            "avg_mae_pct": None if g.empty else float(pd.to_numeric(g["mae_pct"], errors="coerce").mean()),
        }

    sort_cols = ["pnl_pct", "mae_pct"]
    extreme_loss = df.sort_values(sort_cols, ascending=[True, True]).head(20)
    extreme_mae = df.sort_values(["mae_pct", "pnl_pct"], ascending=[True, True]).head(20)
    out["extreme_samples"]["worst_pnl_top20"] = extreme_loss[
        ["symbol", "reason", "pnl_pct", "mae_pct", "mfe_pct", "chg_24h", "drop_pct", "rebound_ratio", "bc_ab_ratio", "drop_window_chg", "drop_window_chg_bucket", "s_time", "c_time"]
    ].to_dict(orient="records")
    out["extreme_samples"]["worst_mae_top20"] = extreme_mae[
        ["symbol", "reason", "pnl_pct", "mae_pct", "mfe_pct", "chg_24h", "drop_pct", "rebound_ratio", "bc_ab_ratio", "drop_window_chg", "drop_window_chg_bucket", "s_time", "c_time"]
    ].to_dict(orient="records")
    return out



def main() -> None:
    ap = argparse.ArgumentParser(description="Audit drop_window_chg for Snapback trades")
    ap.add_argument("--run-id", default="", help="Backtest RUNID, e.g. SNAP_V4.3_...")
    ap.add_argument("--trades", default="", help="Path to sim_trades.<RUNID>.jsonl")
    ap.add_argument("--summary", default="", help="Path to sim_summary.<RUNID>.json")
    ap.add_argument("--out-csv", required=True, help="Output detailed csv path")
    ap.add_argument("--summary-json", required=True, help="Output summary json path")
    ap.add_argument("--bucket-csv", default="", help="Optional output bucket-by-reason csv path")
    args = ap.parse_args()

    trades_path = Path(args.trades) if args.trades else None
    summary_path = Path(args.summary) if args.summary else None
    run_id = args.run_id.strip()

    if run_id:
        if trades_path is None:
            trades_path = _find_run_trades(run_id)
        if summary_path is None:
            summary_path = _find_run_summary(run_id)
    if trades_path is None or not trades_path.exists():
        raise SystemExit("未找到 trades 文件，请提供 --trades 或 --run-id")
    if summary_path is None or not summary_path.exists():
        raise SystemExit("未找到 summary 文件，请提供 --summary 或 --run-id")

    with summary_path.open("r", encoding="utf-8") as f:
        summary_obj = json.load(f)
    run_config = summary_obj.get("run_config") or {}
    resolved_run_id = run_id or str(summary_obj.get("run_id") or trades_path.stem)

    trades = _load_trades(trades_path)
    rows = [_extract_row(tr, run_config) for tr in trades]
    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("trades 为空，无法审计")

    df["reason"] = df["reason"].astype(str)
    df = df.sort_values(by=["reason", "pnl_pct", "mae_pct"], key=None, ascending=[True, False, False])
    # keep business-friendly order in exported csv
    df["_reason_order"] = df["reason"].map(lambda x: _reason_sort_key(str(x)))
    df = df.sort_values(by=["_reason_order", "pnl_pct", "mae_pct"], ascending=[True, False, False]).drop(columns=["_reason_order"])

    out_csv = Path(args.out_csv)
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)

    summary = build_summary(df, resolved_run_id, run_config)
    out_summary = Path(args.summary_json)
    out_summary.parent.mkdir(parents=True, exist_ok=True)
    out_summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.bucket_csv:
        bucket_df = pd.DataFrame(summary["bucket_by_reason"])
        out_bucket = Path(args.bucket_csv)
        out_bucket.parent.mkdir(parents=True, exist_ok=True)
        bucket_df.to_csv(out_bucket, index=False)

    print("=== audit_drop_window_chg 完成 ===")
    print(f"run_id       : {resolved_run_id}")
    print(f"trades       : {trades_path}")
    print(f"summary      : {summary_path}")
    print(f"detail csv   : {out_csv}")
    print(f"summary json : {out_summary}")
    if args.bucket_csv:
        print(f"bucket csv   : {args.bucket_csv}")
    print(f"rows         : {len(df)}")
    print(f"with dw chg  : {int(df['drop_window_chg'].notna().sum())}")


if __name__ == "__main__":
    main()
