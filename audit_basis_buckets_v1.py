#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
audit_basis_buckets_v1.py

目的：
对 sim_trades JSONL 做 “B点相对基差” 一维分桶审计。

基差定义：
    basis_b_pct = (b_contract_price - b_index_price) / b_index_price

默认分桶：
    按分位数做 5 桶（样本数更均匀，适合第一轮验明正身）

输入要求：
    每条 trade 至少应能提供：
    - pnl_pct（顶层）
    - exit_reason（顶层，可缺失）
    - context.b_contract_price / context.b_index_price
      若顶层也有 b_contract_price / b_index_price，也会优先读取顶层，再回退 context

输出：
    - basis_bucket_summary.csv
    - basis_samples.csv
    - basis_summary.json
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import statistics
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _first_not_none(*values: Any) -> Any:
    for v in values:
        if v is not None:
            return v
    return None


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        f = float(v)
    except Exception:
        return None
    if math.isnan(f) or math.isinf(f):
        return None
    return f


def _safe_ratio(num: float, den: float) -> Optional[float]:
    if den == 0:
        return None
    return num / den


def _parse_iso_to_dt(s: Any) -> Optional[datetime]:
    if s is None:
        return None
    text = str(s).strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _bucket_label(i: int, lo: float, hi: float, total: int) -> str:
    left = "[" if i == 0 else "("
    right = "]"
    lo_s = f"{lo:.8f}"
    hi_s = f"{hi:.8f}"
    return f"Q{i+1}/{total} {left}{lo_s}, {hi_s}{right}"


def _quantile_edges(sorted_vals: List[float], q: int) -> List[float]:
    if not sorted_vals:
        return []
    n = len(sorted_vals)
    if q <= 1:
        return [sorted_vals[0], sorted_vals[-1]]
    edges: List[float] = []
    for i in range(q + 1):
        idx = round((n - 1) * i / q)
        edges.append(sorted_vals[idx])
    for i in range(1, len(edges)):
        if edges[i] < edges[i - 1]:
            edges[i] = edges[i - 1]
    return edges


def _assign_bucket(value: float, edges: List[float]) -> int:
    last = len(edges) - 2
    for i in range(len(edges) - 1):
        lo = edges[i]
        hi = edges[i + 1]
        if i == 0:
            if lo <= value <= hi:
                return i
        elif i == last:
            if lo < value <= hi:
                return i
        else:
            if lo < value <= hi:
                return i
    if value <= edges[0]:
        return 0
    return last


def load_trades(jsonl_path: str, start_iso: Optional[str] = None, end_iso: Optional[str] = None) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    start_dt = _parse_iso_to_dt(start_iso)
    end_dt = _parse_iso_to_dt(end_iso)
    if start_iso and start_dt is None:
        raise RuntimeError(f"start-iso 非法: {start_iso}")
    if end_iso and end_dt is None:
        raise RuntimeError(f"end-iso 非法: {end_iso}")
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception as e:
                raise RuntimeError(f"JSONL 解析失败 line={line_no}: {e}") from e

            ctx = obj.get("context") if isinstance(obj.get("context"), dict) else {}

            pnl_pct = _to_float(obj.get("pnl_pct"))
            if pnl_pct is None:
                continue

            b_contract = _to_float(_first_not_none(
                obj.get("b_contract_price"),
                ctx.get("b_contract_price"),
            ))
            b_index = _to_float(_first_not_none(
                obj.get("b_index_price"),
                ctx.get("b_index_price"),
            ))
            if b_contract is None or b_index is None or b_index == 0:
                continue

            basis_b_pct = _safe_ratio(b_contract - b_index, b_index)
            if basis_b_pct is None:
                continue

            exit_reason = str(_first_not_none(obj.get("exit_reason"), ""))

            exit_time = _first_not_none(obj.get("exit_time"), "")
            exit_dt = _parse_iso_to_dt(exit_time)
            if start_dt is not None:
                if exit_dt is None or exit_dt < start_dt:
                    continue
            if end_dt is not None:
                if exit_dt is None or exit_dt >= end_dt:
                    continue

            row = {
                "symbol": _first_not_none(obj.get("symbol"), ctx.get("symbol"), ""),
                "signal_time": _first_not_none(obj.get("signal_time"), ctx.get("signal_time"), ""),
                "exit_time": exit_time,
                "pnl_pct": pnl_pct,
                "exit_reason": exit_reason,
                "b_contract_price": b_contract,
                "b_index_price": b_index,
                "basis_b_pct": basis_b_pct,
                "ab_bars": _to_float(_first_not_none(obj.get("ab_bars"), ctx.get("ab_bars"))),
                "bc_bars": _to_float(_first_not_none(obj.get("bc_bars"), ctx.get("bc_bars"))),
                "rebound_ratio": _to_float(_first_not_none(obj.get("rebound_ratio"), ctx.get("rebound_ratio"))),
                "drop_window_chg": _to_float(_first_not_none(obj.get("drop_window_chg"), ctx.get("drop_window_chg"))),
                "chg_24h": _to_float(_first_not_none(obj.get("chg_24h"), ctx.get("chg_24h"))),
            }
            rows.append(row)
    return rows


def summarize_bucket(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    pnls = [r["pnl_pct"] for r in rows]
    wins = sum(1 for x in pnls if x > 0)
    losses = sum(1 for x in pnls if x < 0)
    flats = sum(1 for x in pnls if x == 0)
    count = len(rows)

    exit_counts: Dict[str, int] = {}
    for r in rows:
        reason = r.get("exit_reason", "") or ""
        exit_counts[reason] = exit_counts.get(reason, 0) + 1

    basis_vals = [r["basis_b_pct"] for r in rows]

    return {
        "count": count,
        "profit_count": wins,
        "loss_count": losses,
        "flat_count": flats,
        "win_rate": (wins / count) if count else None,
        "pnl_sum": sum(pnls),
        "avg_pnl": (sum(pnls) / count) if count else None,
        "median_pnl": statistics.median(pnls) if pnls else None,
        "basis_b_pct_mean": (sum(basis_vals) / len(basis_vals)) if basis_vals else None,
        "basis_b_pct_median": statistics.median(basis_vals) if basis_vals else None,
        "tp_count": exit_counts.get("TAKE_PROFIT", 0),
        "sl_count": exit_counts.get("STOP_LOSS", 0),
        "ts_count": exit_counts.get("TIME_STOP", 0),
        "other_exit_count": count - exit_counts.get("TAKE_PROFIT", 0) - exit_counts.get("STOP_LOSS", 0) - exit_counts.get("TIME_STOP", 0),
    }


def write_csv(path: str, fieldnames: List[str], rows: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True, help="运行标识；未显式传路径时，会按 run-id 自动推导默认路径")
    ap.add_argument("--trades-jsonl", required=False, help="sim_trades JSONL；默认 output/state/sim_trades.{run_id}.jsonl")
    ap.add_argument("--out-dir", required=False, help="输出目录；默认 output/state/basis_audit.{run_id}")
    ap.add_argument("--quantiles", type=int, default=5, help="分位数桶数，默认 5")
    ap.add_argument("--start-iso", required=False, help="按 exit_time 过滤；起始时间（含边界），默认不过滤")
    ap.add_argument("--end-iso", required=False, help="按 exit_time 过滤；结束时间（不含边界），默认不过滤")
    args = ap.parse_args()

    if not args.trades_jsonl:
        args.trades_jsonl = os.path.join("output", "state", f"sim_trades.{args.run_id}.jsonl")
    if not args.out_dir:
        args.out_dir = os.path.join("output", "state", f"basis_audit.{args.run_id}")

    if not os.path.exists(args.trades_jsonl):
        raise RuntimeError(f"sim_trades 文件不存在: {args.trades_jsonl}")

    os.makedirs(args.out_dir, exist_ok=True)

    rows = load_trades(args.trades_jsonl, start_iso=args.start_iso, end_iso=args.end_iso)
    if not rows:
        raise RuntimeError("未读取到可用 trade；请检查 sim_trades 是否包含 pnl_pct / context.b_contract_price / context.b_index_price")

    basis_vals = sorted(r["basis_b_pct"] for r in rows)
    edges = _quantile_edges(basis_vals, args.quantiles)

    bucketed: List[Dict[str, Any]] = []
    bucket_rows: Dict[int, List[Dict[str, Any]]] = {i: [] for i in range(len(edges) - 1)}

    for r in rows:
        idx = _assign_bucket(r["basis_b_pct"], edges)
        label = _bucket_label(idx, edges[idx], edges[idx + 1], len(edges) - 1)
        rr = dict(r)
        rr["basis_bucket_idx"] = idx
        rr["basis_bucket"] = label
        bucketed.append(rr)
        bucket_rows[idx].append(rr)

    summary_rows: List[Dict[str, Any]] = []
    for i in range(len(edges) - 1):
        lo = edges[i]
        hi = edges[i + 1]
        label = _bucket_label(i, lo, hi, len(edges) - 1)
        stats = summarize_bucket(bucket_rows[i])
        summary_rows.append({
            "basis_bucket_idx": i,
            "basis_bucket": label,
            "basis_b_pct_lo": lo,
            "basis_b_pct_hi": hi,
            **stats,
        })

    bucketed_sorted = sorted(
        bucketed,
        key=lambda r: (r["basis_bucket_idx"], r["pnl_pct"], str(r.get("symbol", "")), str(r.get("signal_time", ""))),
    )

    write_csv(
        os.path.join(args.out_dir, "basis_bucket_summary.csv"),
        [
            "basis_bucket_idx",
            "basis_bucket",
            "basis_b_pct_lo",
            "basis_b_pct_hi",
            "count",
            "profit_count",
            "loss_count",
            "flat_count",
            "win_rate",
            "pnl_sum",
            "avg_pnl",
            "median_pnl",
            "basis_b_pct_mean",
            "basis_b_pct_median",
            "tp_count",
            "sl_count",
            "ts_count",
            "other_exit_count",
        ],
        summary_rows,
    )

    write_csv(
        os.path.join(args.out_dir, "basis_samples.csv"),
        [
            "basis_bucket_idx",
            "basis_bucket",
            "symbol",
            "signal_time",
            "exit_time",
            "pnl_pct",
            "exit_reason",
            "b_contract_price",
            "b_index_price",
            "basis_b_pct",
            "ab_bars",
            "bc_bars",
            "rebound_ratio",
            "drop_window_chg",
            "chg_24h",
        ],
        bucketed_sorted,
    )

    total_stats = summarize_bucket(rows)
    summary_json = {
        "run_id": args.run_id,
        "trades_jsonl": args.trades_jsonl,
        "out_dir": args.out_dir,
        "quantiles": args.quantiles,
        "start_iso": args.start_iso,
        "end_iso": args.end_iso,
        "basis_formula": "(b_contract_price - b_index_price) / b_index_price",
        "all_trades": len(rows),
        "bucket_count": len(edges) - 1,
        "overall": total_stats,
        "bucket_edges": edges,
        "written": [
            "basis_bucket_summary.csv",
            "basis_samples.csv",
            "basis_summary.json",
        ],
    }
    with open(os.path.join(args.out_dir, "basis_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary_json, f, ensure_ascii=False, indent=2)

    print("=== audit_basis_buckets_v1 完成 ===")
    print(f"run_id        : {args.run_id}")
    print(f"trades_jsonl  : {args.trades_jsonl}")
    print(f"out_dir       : {args.out_dir}")
    print(f"quantiles     : {args.quantiles}")
    print(f"start_iso     : {args.start_iso}")
    print(f"end_iso       : {args.end_iso}")
    print("basis_formula : (b_contract_price - b_index_price) / b_index_price")
    print(f"all_trades    : {len(rows)}")
    print(f"bucket_count  : {len(edges) - 1}")
    print("written       : basis_bucket_summary.csv, basis_samples.csv, basis_summary.json")


if __name__ == "__main__":
    main()
