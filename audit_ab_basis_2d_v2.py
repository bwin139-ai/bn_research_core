#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ab_bars × basis_b_pct 二维审计（v2）
- 只需要 --run-id
- 支持时间窗口过滤（按 exit_time）
- ab 轴改为语义桶，避免 11+ 吞掉主体样本
- basis 轴沿用手工边界
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Optional


DEFAULT_BASIS_BUCKETS = [-100.0, -0.03, -0.02, -0.015, -0.01, -0.0075, -0.005, 1.0]
# 语义桶：1,2,3,4,5,6,7-8,9-10,11-14,15-20,21+
AB_SEMANTIC_BUCKETS = [
    (1, 1, "1"),
    (2, 2, "2"),
    (3, 3, "3"),
    (4, 4, "4"),
    (5, 5, "5"),
    (6, 6, "6"),
    (7, 8, "7-8"),
    (9, 10, "9-10"),
    (11, 14, "11-14"),
    (15, 20, "15-20"),
    (21, 10**9, "21+"),
]


def _first_not_none(*vals: Any) -> Any:
    for v in vals:
        if v is not None:
            return v
    return None


def _to_float(v: Any) -> Optional[float]:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def _to_int(v: Any) -> Optional[int]:
    if v is None or v == "":
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return None


def _parse_dt(v: Any) -> Optional[datetime]:
    if v is None or v == "":
        return None
    # epoch seconds/ms
    if isinstance(v, (int, float)):
        x = float(v)
        if abs(x) > 1e11:
            x = x / 1000.0
        return datetime.fromtimestamp(x, tz=timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    if s.isdigit():
        x = float(s)
        if abs(x) > 1e11:
            x = x / 1000.0
        return datetime.fromtimestamp(x, tz=timezone.utc)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _basis_bucket_label(x: float, edges: list[float]) -> str:
    for i in range(len(edges) - 1):
        lo = edges[i]
        hi = edges[i + 1]
        if x >= lo and x < hi:
            return f"[{lo:.4f},{hi:.4f})"
    if x == edges[-1]:
        lo = edges[-2]
        hi = edges[-1]
        return f"[{lo:.4f},{hi:.4f})"
    return "OUT_OF_RANGE"


def _ab_bucket_label(ab: int) -> str:
    for lo, hi, label in AB_SEMANTIC_BUCKETS:
        if lo <= ab <= hi:
            return label
    return "NA"


@dataclass
class TradeRow:
    symbol: str
    signal_time: Any
    exit_time: Any
    pnl_pct: float
    reason: str
    ab_bars: int
    basis_b_pct: float
    bc_bars: Optional[int]
    rebound_ratio: Optional[float]
    drop_window_chg: Optional[float]
    chg_24h: Optional[float]


def load_trades(path: Path, start_iso: Optional[str], end_iso: Optional[str]) -> list[TradeRow]:
    start_dt = _parse_dt(start_iso) if start_iso else None
    end_dt = _parse_dt(end_iso) if end_iso else None
    rows: list[TradeRow] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            ctx = obj.get("context") or {}
            pnl_pct = _to_float(_first_not_none(obj.get("pnl_pct"), ctx.get("pnl_pct")))
            b_contract = _to_float(_first_not_none(obj.get("b_contract_price"), ctx.get("b_contract_price")))
            b_index = _to_float(_first_not_none(obj.get("b_index_price"), ctx.get("b_index_price")))
            ab_bars = _to_int(_first_not_none(obj.get("ab_bars"), ctx.get("ab_bars")))
            if pnl_pct is None or b_contract is None or b_index in (None, 0) or ab_bars is None:
                continue

            exit_time = _first_not_none(obj.get("exit_time"), ctx.get("exit_time"))
            exit_dt = _parse_dt(exit_time)
            if start_dt is not None:
                if exit_dt is None or exit_dt < start_dt:
                    continue
            if end_dt is not None:
                if exit_dt is None or exit_dt >= end_dt:
                    continue

            basis_b_pct = (b_contract - b_index) / b_index
            rows.append(
                TradeRow(
                    symbol=str(_first_not_none(obj.get("symbol"), ctx.get("symbol"), "")),
                    signal_time=_first_not_none(obj.get("signal_time"), ctx.get("signal_time")),
                    exit_time=exit_time,
                    pnl_pct=pnl_pct,
                    reason=str(_first_not_none(obj.get("exit_reason"), obj.get("reason"), ctx.get("exit_reason"), ctx.get("reason"), "")),
                    ab_bars=ab_bars,
                    basis_b_pct=basis_b_pct,
                    bc_bars=_to_int(_first_not_none(obj.get("bc_bars"), ctx.get("bc_bars"))),
                    rebound_ratio=_to_float(_first_not_none(obj.get("rebound_ratio"), ctx.get("rebound_ratio"))),
                    drop_window_chg=_to_float(_first_not_none(obj.get("drop_window_chg"), ctx.get("drop_window_chg"))),
                    chg_24h=_to_float(_first_not_none(obj.get("chg_24h"), ctx.get("chg_24h"))),
                )
            )
    return rows


def _stats(rows: list[TradeRow]) -> dict[str, Any]:
    count = len(rows)
    pnls = [r.pnl_pct for r in rows]
    profit = sum(1 for x in pnls if x > 0)
    loss = sum(1 for x in pnls if x < 0)
    flat = count - profit - loss
    tp = sum(1 for r in rows if r.reason == "TAKE_PROFIT")
    sl = sum(1 for r in rows if r.reason == "STOP_LOSS")
    ts = sum(1 for r in rows if r.reason == "TIME_STOP")
    return {
        "count": count,
        "profit_count": profit,
        "loss_count": loss,
        "flat_count": flat,
        "win_rate": (profit / count if count else 0.0),
        "pnl_sum": sum(pnls),
        "avg_pnl": (sum(pnls) / count if count else 0.0),
        "median_pnl": (median(pnls) if pnls else 0.0),
        "tp_count": tp,
        "sl_count": sl,
        "ts_count": ts,
        "basis_mean": (sum(r.basis_b_pct for r in rows) / count if count else 0.0),
        "basis_median": (median([r.basis_b_pct for r in rows]) if rows else 0.0),
        "ab_mean": (sum(r.ab_bars for r in rows) / count if count else 0.0),
        "ab_median": (median([r.ab_bars for r in rows]) if rows else 0.0),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--trades-jsonl", default="")
    ap.add_argument("--out-dir", default="")
    ap.add_argument("--start-iso", default="")
    ap.add_argument("--end-iso", default="")
    ap.add_argument(
        "--basis-buckets",
        default=",".join(str(x) for x in DEFAULT_BASIS_BUCKETS),
        help="comma-separated bucket edges",
    )
    args = ap.parse_args()

    trades_jsonl = Path(args.trades_jsonl) if args.trades_jsonl else Path(f"output/state/sim_trades.{args.run_id}.jsonl")
    out_dir = Path(args.out_dir) if args.out_dir else Path(f"output/state/ab_basis_2d_v2.{args.run_id}")
    out_dir.mkdir(parents=True, exist_ok=True)

    basis_edges = [float(x.strip()) for x in args.basis_buckets.split(",") if x.strip()]
    if len(basis_edges) < 2:
        raise SystemExit("basis bucket edges 至少需要两个")

    rows = load_trades(trades_jsonl, args.start_iso or None, args.end_iso or None)
    if not rows:
        raise RuntimeError("未读取到可用 trade；请检查 sim_trades / 时间窗口 / 字段完整性")

    grouped: dict[tuple[str, str], list[TradeRow]] = defaultdict(list)
    for r in rows:
        ab_label = _ab_bucket_label(r.ab_bars)
        basis_label = _basis_bucket_label(r.basis_b_pct, basis_edges)
        grouped[(ab_label, basis_label)].append(r)

    summary_csv = out_dir / "ab_basis_2d_summary.csv"
    with summary_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "ab_bucket", "basis_bucket",
            "count", "profit_count", "loss_count", "flat_count",
            "win_rate", "pnl_sum", "avg_pnl", "median_pnl",
            "tp_count", "sl_count", "ts_count",
            "basis_mean", "basis_median", "ab_mean", "ab_median",
        ])
        # 按 AB 桶顺序，再按 basis 边界顺序
        ab_order = [x[2] for x in AB_SEMANTIC_BUCKETS]
        basis_order = [_basis_bucket_label((basis_edges[i] + basis_edges[i+1]) / 2.0, basis_edges) for i in range(len(basis_edges)-1)]
        for ab_label in ab_order:
            for basis_label in basis_order:
                bucket_rows = grouped.get((ab_label, basis_label), [])
                if not bucket_rows:
                    continue
                s = _stats(bucket_rows)
                w.writerow([
                    ab_label, basis_label,
                    s["count"], s["profit_count"], s["loss_count"], s["flat_count"],
                    f'{s["win_rate"]:.6f}', f'{s["pnl_sum"]:.6f}', f'{s["avg_pnl"]:.6f}', f'{s["median_pnl"]:.6f}',
                    s["tp_count"], s["sl_count"], s["ts_count"],
                    f'{s["basis_mean"]:.6f}', f'{s["basis_median"]:.6f}', f'{s["ab_mean"]:.6f}', f'{s["ab_median"]:.6f}',
                ])

    samples_csv = out_dir / "ab_basis_2d_samples.csv"
    with samples_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "symbol", "signal_time", "exit_time", "pnl_pct", "reason",
            "ab_bars", "ab_bucket", "basis_b_pct", "basis_bucket",
            "bc_bars", "rebound_ratio", "drop_window_chg", "chg_24h",
        ])
        for r in rows:
            w.writerow([
                r.symbol, r.signal_time, r.exit_time, f"{r.pnl_pct:.6f}", r.reason,
                r.ab_bars, _ab_bucket_label(r.ab_bars), f"{r.basis_b_pct:.6f}", _basis_bucket_label(r.basis_b_pct, basis_edges),
                "" if r.bc_bars is None else r.bc_bars,
                "" if r.rebound_ratio is None else f"{r.rebound_ratio:.6f}",
                "" if r.drop_window_chg is None else f"{r.drop_window_chg:.6f}",
                "" if r.chg_24h is None else f"{r.chg_24h:.6f}",
            ])

    summary_json = out_dir / "ab_basis_2d_summary.json"
    payload = {
        "run_id": args.run_id,
        "trades_jsonl": str(trades_jsonl),
        "out_dir": str(out_dir),
        "start_iso": args.start_iso,
        "end_iso": args.end_iso,
        "basis_bucket_edges": basis_edges,
        "ab_buckets": [x[2] for x in AB_SEMANTIC_BUCKETS],
        "total_trades": len(rows),
        "overall": _stats(rows),
        "written": [
            "ab_basis_2d_summary.csv",
            "ab_basis_2d_samples.csv",
            "ab_basis_2d_summary.json",
        ],
    }
    summary_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=== audit_ab_basis_2d_v2 完成 ===")
    print(f"run_id       : {args.run_id}")
    print(f"trades_jsonl : {trades_jsonl}")
    print(f"out_dir      : {out_dir}")
    print(f"start_iso    : {args.start_iso or '(all)'}")
    print(f"end_iso      : {args.end_iso or '(all)'}")
    print(f"total_trades : {len(rows)}")
    print("written      : ab_basis_2d_summary.csv, ab_basis_2d_samples.csv, ab_basis_2d_summary.json")


if __name__ == "__main__":
    main()
