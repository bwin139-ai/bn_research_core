#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any

DEFAULT_START_ISO = "2026-01-01T00:00:00+00:00"
DEFAULT_BASIS_BUCKETS = [-100.0, -0.03, -0.02, -0.015, -0.01, -0.0075, -0.005, 1.0]
AB_BUCKETS = [
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


def _parse_time(v: Any) -> datetime | None:
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)):
        x = float(v)
        if x > 1e12:
            x /= 1000.0
        return datetime.fromtimestamp(x, tz=timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
        x = float(s)
        if abs(x) > 1e12:
            x /= 1000.0
        return datetime.fromtimestamp(x, tz=timezone.utc)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt


def _to_float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        x = float(v)
    except Exception:
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    return x


def _ab_bucket_label(ab: int) -> str:
    for lo, hi, label in AB_BUCKETS:
        if lo <= ab <= hi:
            return label
    return "unknown"


def _basis_bucket_label(x: float, edges: list[float]) -> str:
    for i in range(len(edges) - 1):
        lo = edges[i]
        hi = edges[i + 1]
        if i == len(edges) - 2:
            if lo <= x <= hi:
                return f"[{lo:.4f},{hi:.4f}]"
        if lo <= x < hi:
            return f"[{lo:.4f},{hi:.4f})"
    return f"overflow({x:.6f})"


@dataclass
class TradeRow:
    symbol: str
    signal_time: str
    exit_time: str
    pnl_pct: float
    reason: str
    ab_bars: int
    bc_bars: int | None
    rebound_ratio: float | None
    basis_b_pct: float
    ab_bucket: str
    basis_bucket: str


def load_rows(trades_jsonl: Path, start_iso: str | None, end_iso: str | None, basis_edges: list[float]) -> list[TradeRow]:
    start_dt = _parse_time(start_iso) if start_iso else None
    end_dt = _parse_time(end_iso) if end_iso else None
    rows: list[TradeRow] = []
    with trades_jsonl.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            ctx = obj.get("context") or {}
            exit_time = _first_not_none(obj.get("exit_time"), ctx.get("exit_time"))
            exit_dt = _parse_time(exit_time)
            if start_dt and (exit_dt is None or exit_dt < start_dt):
                continue
            if end_dt and (exit_dt is None or exit_dt >= end_dt):
                continue

            pnl = _to_float(_first_not_none(obj.get("pnl_pct"), obj.get("pnl")))
            if pnl is None:
                continue
            b_contract = _to_float(_first_not_none(ctx.get("b_contract_price"), obj.get("b_contract_price")))
            b_index = _to_float(_first_not_none(ctx.get("b_index_price"), obj.get("b_index_price")))
            if b_contract is None or b_index in (None, 0.0):
                continue
            basis = (b_contract - b_index) / b_index

            ab_bars_val = _to_float(_first_not_none(obj.get("ab_bars"), ctx.get("ab_bars")))
            if ab_bars_val is None:
                a_t = _parse_time(_first_not_none(ctx.get("a_time_ms"), ctx.get("a_ts_ms"), ctx.get("a_time"), ctx.get("a_ts"), obj.get("a_time")))
                b_t = _parse_time(_first_not_none(ctx.get("b_time_ms"), ctx.get("b_ts_ms"), ctx.get("b_time"), ctx.get("b_ts"), obj.get("b_time")))
                if a_t is None or b_t is None:
                    continue
                ab_bars = max(0, int(round((b_t - a_t).total_seconds() / 60.0)))
            else:
                ab_bars = int(round(ab_bars_val))

            bc_bars_val = _to_float(_first_not_none(obj.get("bc_bars"), ctx.get("bc_bars")))
            bc_bars = None if bc_bars_val is None else int(round(bc_bars_val))
            rebound_ratio = _to_float(_first_not_none(obj.get("rebound_ratio"), ctx.get("rebound_ratio")))
            reason = str(_first_not_none(obj.get("exit_reason"), obj.get("reason"), ""))
            symbol = str(_first_not_none(obj.get("symbol"), ctx.get("symbol"), ""))
            signal_time = str(_first_not_none(obj.get("signal_time"), ctx.get("signal_time"), ""))
            exit_time_str = str(exit_time or "")
            rows.append(
                TradeRow(
                    symbol=symbol,
                    signal_time=signal_time,
                    exit_time=exit_time_str,
                    pnl_pct=pnl,
                    reason=reason,
                    ab_bars=ab_bars,
                    bc_bars=bc_bars,
                    rebound_ratio=rebound_ratio,
                    basis_b_pct=basis,
                    ab_bucket=_ab_bucket_label(ab_bars),
                    basis_bucket=_basis_bucket_label(basis, basis_edges),
                )
            )
    return rows


def summarize(rows: list[TradeRow]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    groups: dict[tuple[str, str], list[TradeRow]] = defaultdict(list)
    for r in rows:
        groups[(r.ab_bucket, r.basis_bucket)].append(r)

    summary_rows: list[dict[str, Any]] = []
    for (ab_bucket, basis_bucket), items in groups.items():
        pnls = [x.pnl_pct for x in items]
        profits = sum(1 for x in pnls if x > 0)
        losses = sum(1 for x in pnls if x < 0)
        flats = sum(1 for x in pnls if x == 0)
        reasons = [x.reason for x in items]
        summary_rows.append({
            "ab_bucket": ab_bucket,
            "basis_bucket": basis_bucket,
            "count": len(items),
            "profit_count": profits,
            "loss_count": losses,
            "flat_count": flats,
            "win_rate": profits / len(items),
            "pnl_sum": sum(pnls),
            "avg_pnl": sum(pnls) / len(items),
            "median_pnl": median(pnls),
            "basis_mean": sum(x.basis_b_pct for x in items) / len(items),
            "basis_median": median([x.basis_b_pct for x in items]),
            "ab_bars_mean": sum(x.ab_bars for x in items) / len(items),
            "bc_bars_mean": None if not any(x.bc_bars is not None for x in items) else sum((x.bc_bars or 0) for x in items if x.bc_bars is not None) / sum(1 for x in items if x.bc_bars is not None),
            "rebound_ratio_mean": None if not any(x.rebound_ratio is not None for x in items) else sum((x.rebound_ratio or 0.0) for x in items if x.rebound_ratio is not None) / sum(1 for x in items if x.rebound_ratio is not None),
            "tp_count": sum(1 for r in reasons if r == "TAKE_PROFIT"),
            "sl_count": sum(1 for r in reasons if r == "STOP_LOSS"),
            "ts_count": sum(1 for r in reasons if r == "TIME_STOP"),
        })

    ab_order = {label: idx for idx, (_, _, label) in enumerate(AB_BUCKETS)}
    summary_rows.sort(key=lambda d: (ab_order.get(d["ab_bucket"], 999), d["basis_mean"]))

    stubborn_bad = [
        x for x in summary_rows
        if x["count"] >= 3 and x["median_pnl"] < 0 and x["loss_count"] > x["profit_count"]
    ]
    stubborn_bad.sort(key=lambda d: (d["median_pnl"], d["avg_pnl"], -d["count"]))
    return summary_rows, stubborn_bad


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_samples(path: Path, rows: list[TradeRow]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "symbol", "signal_time", "exit_time", "pnl_pct", "reason",
            "ab_bars", "bc_bars", "rebound_ratio", "basis_b_pct",
            "ab_bucket", "basis_bucket",
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({k: getattr(r, k) for k in fieldnames})


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Post-only ab_bars × basis_b_pct 2D audit")
    p.add_argument("--run-id", required=True)
    p.add_argument("--trades-jsonl")
    p.add_argument("--out-dir")
    p.add_argument("--start-iso", default=DEFAULT_START_ISO)
    p.add_argument("--end-iso", default=None)
    p.add_argument("--basis-buckets", default=",".join(str(x) for x in DEFAULT_BASIS_BUCKETS))
    return p.parse_args()


def main() -> None:
    args = parse_args()
    trades_jsonl = Path(args.trades_jsonl or f"output/state/sim_trades.{args.run_id}.jsonl")
    out_dir = Path(args.out_dir or f"output/state/post_ab_basis_2d.{args.run_id}")
    out_dir.mkdir(parents=True, exist_ok=True)
    basis_edges = [float(x) for x in str(args.basis_buckets).split(",") if str(x).strip()]
    if len(basis_edges) < 2:
        raise RuntimeError("basis buckets 至少需要两个边界")
    rows = load_rows(trades_jsonl, args.start_iso, args.end_iso, basis_edges)
    if not rows:
        raise RuntimeError("未读取到可用 trade；请检查时间窗口和字段是否存在")
    summary_rows, stubborn_bad = summarize(rows)
    write_csv(out_dir / "ab_basis_2d_summary.csv", summary_rows)
    write_csv(out_dir / "ab_basis_2d_stubborn_bad_cells.csv", stubborn_bad)
    write_samples(out_dir / "ab_basis_2d_samples.csv", rows)
    payload = {
        "run_id": args.run_id,
        "trades_jsonl": str(trades_jsonl),
        "out_dir": str(out_dir),
        "start_iso": args.start_iso,
        "end_iso": args.end_iso,
        "basis_buckets": basis_edges,
        "total_rows": len(rows),
        "non_empty_cells": len(summary_rows),
        "stubborn_bad_cells": len(stubborn_bad),
        "ab_median": median([r.ab_bars for r in rows]),
        "basis_median": median([r.basis_b_pct for r in rows]),
        "written": [
            "ab_basis_2d_summary.csv",
            "ab_basis_2d_stubborn_bad_cells.csv",
            "ab_basis_2d_samples.csv",
            "ab_basis_2d_summary.json",
        ],
    }
    (out_dir / "ab_basis_2d_summary.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("=== post ab×basis 2D audit 完成 ===")
    for k, v in payload.items():
        print(f"{k:22}: {v}")


if __name__ == "__main__":
    main()
