#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
bc_bars 一维审计工具（独立脚本）

功能：
1. 从 sim_trades JSONL 读取交易
2. 提取 bc_bars（优先顶层，回退 context）
3. 统计每个 bc_bars 值的表现
4. 默认对大尾部做合并桶：11+
5. 可选按时间窗口过滤（基于 exit_time）

默认路径（run-id 驱动）：
- trades_jsonl = output/state/sim_trades.{run_id}.jsonl
- out_dir       = output/state/bc_bars_audit.{run_id}
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _first_not_none(*values):
    for v in values:
        if v is not None:
            return v
    return None


def _safe_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _safe_int(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _parse_dt(v: Any) -> Optional[datetime]:
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        if x > 1e12:
            return datetime.fromtimestamp(x / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(x, tz=timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    # numeric string -> epoch sec/ms
    try:
        x = float(s)
        if x > 1e12:
            return datetime.fromtimestamp(x / 1000.0, tz=timezone.utc)
        if x > 1e9:
            return datetime.fromtimestamp(x, tz=timezone.utc)
    except ValueError:
        pass
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _bucket_label(bc_bars: int, tail_from: int) -> str:
    if bc_bars >= tail_from:
        return f"{tail_from}+"
    return str(bc_bars)


def load_trades(
    trades_jsonl: Path,
    start_iso: Optional[str],
    end_iso: Optional[str],
) -> List[Dict[str, Any]]:
    start_dt = _parse_dt(start_iso) if start_iso else None
    end_dt = _parse_dt(end_iso) if end_iso else None

    rows: List[Dict[str, Any]] = []
    with trades_jsonl.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            exit_dt = _parse_dt(_first_not_none(obj.get("exit_time"), obj.get("close_time"), obj.get("exit_time_ms")))
            if start_dt is not None:
                if exit_dt is None or exit_dt < start_dt:
                    continue
            if end_dt is not None:
                if exit_dt is None or exit_dt >= end_dt:
                    continue

            context = obj.get("context") or {}
            bc_bars = _safe_int(_first_not_none(obj.get("bc_bars"), context.get("bc_bars")))
            pnl_pct = _safe_float(_first_not_none(obj.get("pnl_pct"), obj.get("pnl"), obj.get("pnlPercent"), context.get("pnl_pct")))
            if bc_bars is None or pnl_pct is None:
                continue

            reason = _first_not_none(obj.get("exit_reason"), obj.get("reason"), "")
            symbol = _first_not_none(obj.get("symbol"), context.get("symbol"), "")
            signal_time = _first_not_none(obj.get("signal_time"), context.get("signal_time"), "")
            ab_bars = _safe_int(_first_not_none(obj.get("ab_bars"), context.get("ab_bars")))
            rebound_ratio = _safe_float(_first_not_none(obj.get("rebound_ratio"), context.get("rebound_ratio")))
            basis_b_pct = _safe_float(_first_not_none(obj.get("basis_b_pct"), context.get("basis_b_pct")))

            rows.append({
                "line_no": line_no,
                "symbol": symbol,
                "signal_time": signal_time,
                "exit_time": _first_not_none(obj.get("exit_time"), ""),
                "bc_bars": bc_bars,
                "ab_bars": ab_bars,
                "rebound_ratio": rebound_ratio,
                "basis_b_pct": basis_b_pct,
                "pnl_pct": pnl_pct,
                "reason": str(reason or ""),
            })
    return rows


def summarize_bucket(rows: List[Dict[str, Any]], bucket: str) -> Dict[str, Any]:
    pnls = [float(r["pnl_pct"]) for r in rows]
    profit_count = sum(1 for x in pnls if x > 0)
    loss_count = sum(1 for x in pnls if x < 0)
    flat_count = sum(1 for x in pnls if x == 0)
    reasons = [str(r.get("reason", "")).upper() for r in rows]
    tp_count = sum(1 for r in reasons if "TAKE_PROFIT" in r or r == "TP")
    sl_count = sum(1 for r in reasons if "STOP_LOSS" in r or r == "SL")
    ts_count = sum(1 for r in reasons if "TIME_STOP" in r or r == "TS")

    ab_vals = [r["ab_bars"] for r in rows if r.get("ab_bars") is not None]
    reb_vals = [float(r["rebound_ratio"]) for r in rows if r.get("rebound_ratio") is not None]
    basis_vals = [float(r["basis_b_pct"]) for r in rows if r.get("basis_b_pct") is not None]

    return {
        "bucket": bucket,
        "count": len(rows),
        "profit_count": profit_count,
        "loss_count": loss_count,
        "flat_count": flat_count,
        "win_rate": round(profit_count / len(rows), 6) if rows else 0.0,
        "pnl_sum": round(sum(pnls), 10),
        "avg_pnl": round(sum(pnls) / len(pnls), 10) if pnls else 0.0,
        "median_pnl": round(median(pnls), 10) if pnls else 0.0,
        "tp_count": tp_count,
        "sl_count": sl_count,
        "ts_count": ts_count,
        "ab_bars_mean": round(sum(ab_vals) / len(ab_vals), 6) if ab_vals else "",
        "rebound_ratio_mean": round(sum(reb_vals) / len(reb_vals), 10) if reb_vals else "",
        "basis_b_pct_mean": round(sum(basis_vals) / len(basis_vals), 10) if basis_vals else "",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="bc_bars 一维审计（每值一桶，大尾部合并）")
    parser.add_argument("--run-id", required=True, help="RUNID，用于自动推导输入输出路径")
    parser.add_argument("--trades-jsonl", default="", help="可选覆盖：sim_trades 路径")
    parser.add_argument("--out-dir", default="", help="可选覆盖：输出目录")
    parser.add_argument("--tail-from", type=int, default=11, help="从该值开始合并为一个尾桶，默认 11 -> 11+")
    parser.add_argument("--start-iso", default="", help="可选：起始时间（按 exit_time 过滤，含边界）")
    parser.add_argument("--end-iso", default="", help="可选：结束时间（按 exit_time 过滤，不含边界）")
    args = parser.parse_args()

    trades_jsonl = Path(args.trades_jsonl) if args.trades_jsonl else Path(f"output/state/sim_trades.{args.run_id}.jsonl")
    out_dir = Path(args.out_dir) if args.out_dir else Path(f"output/state/bc_bars_audit.{args.run_id}")
    tail_from = int(args.tail_from)

    if not trades_jsonl.exists():
        raise FileNotFoundError(f"sim_trades 不存在: {trades_jsonl}")

    rows = load_trades(trades_jsonl, start_iso=args.start_iso or None, end_iso=args.end_iso or None)
    if not rows:
        raise RuntimeError("未读取到可用 trade；请检查 sim_trades 是否包含 pnl_pct / bc_bars")

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        grouped[_bucket_label(int(r["bc_bars"]), tail_from)].append(r)

    def sort_key(label: str) -> Tuple[int, int]:
        if label.endswith("+"):
            return (int(label[:-1]), 1)
        return (int(label), 0)

    summary_rows = [summarize_bucket(grouped[k], k) for k in sorted(grouped.keys(), key=sort_key)]

    sample_rows = []
    for k in sorted(grouped.keys(), key=sort_key):
        for r in grouped[k]:
            sample_rows.append({
                "bucket": k,
                **r,
            })

    summary_json = {
        "run_id": args.run_id,
        "trades_jsonl": str(trades_jsonl),
        "out_dir": str(out_dir),
        "tail_from": tail_from,
        "start_iso": args.start_iso,
        "end_iso": args.end_iso,
        "all_trades": len(rows),
        "bucket_count": len(summary_rows),
        "buckets": summary_rows,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(
        out_dir / "bc_bars_bucket_summary.csv",
        summary_rows,
        [
            "bucket", "count", "profit_count", "loss_count", "flat_count",
            "win_rate", "pnl_sum", "avg_pnl", "median_pnl",
            "tp_count", "sl_count", "ts_count",
            "ab_bars_mean", "rebound_ratio_mean", "basis_b_pct_mean",
        ],
    )
    _write_csv(
        out_dir / "bc_bars_samples.csv",
        sample_rows,
        [
            "bucket", "line_no", "symbol", "signal_time", "exit_time",
            "bc_bars", "ab_bars", "rebound_ratio", "basis_b_pct",
            "pnl_pct", "reason",
        ],
    )
    with (out_dir / "bc_bars_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary_json, f, ensure_ascii=False, indent=2)

    print("=== audit_bc_bars_buckets 完成 ===")
    print(f"run_id       : {args.run_id}")
    print(f"trades_jsonl : {trades_jsonl}")
    print(f"out_dir      : {out_dir}")
    print(f"tail_from    : {tail_from}")
    print(f"start_iso    : {args.start_iso}")
    print(f"end_iso      : {args.end_iso}")
    print(f"all_trades   : {len(rows)}")
    print(f"bucket_count : {len(summary_rows)}")
    print("written      : bc_bars_bucket_summary.csv, bc_bars_samples.csv, bc_bars_summary.json")


if __name__ == "__main__":
    main()
