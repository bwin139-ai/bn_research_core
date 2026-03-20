#!/usr/bin/env python3
import argparse
import csv
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean, median
from typing import Any, Dict, List, Optional, Tuple


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit ab_bars x basis_b_pct 2D buckets from sim_trades jsonl")
    p.add_argument("--run-id", required=True, help="Run id, used to derive default input/output paths")
    p.add_argument("--trades-jsonl", default=None, help="Override trades jsonl path")
    p.add_argument("--out-dir", default=None, help="Override output dir")
    p.add_argument("--start-iso", default=None, help="Inclusive start time filter on exit_time")
    p.add_argument("--end-iso", default=None, help="Exclusive end time filter on exit_time")
    p.add_argument("--basis-buckets", default="-100,-0.03,-0.02,-0.015,-0.01,-0.0075,-0.005,1", help="Comma-separated bucket edges for basis_b_pct. Use ascending values. Default isolates light-discount danger zone.")
    p.add_argument("--ab-tail-from", type=int, default=11, help="ab_bars >= this value are merged into one tail bucket like '11+'").add
    return p.parse_args()


def _parse_args_fixed() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit ab_bars x basis_b_pct 2D buckets from sim_trades jsonl")
    p.add_argument("--run-id", required=True, help="Run id, used to derive default input/output paths")
    p.add_argument("--trades-jsonl", default=None, help="Override trades jsonl path")
    p.add_argument("--out-dir", default=None, help="Override output dir")
    p.add_argument("--start-iso", default=None, help="Inclusive start time filter on exit_time")
    p.add_argument("--end-iso", default=None, help="Exclusive end time filter on exit_time")
    p.add_argument(
        "--basis-buckets",
        default="-100,-0.03,-0.02,-0.015,-0.01,-0.0075,-0.005,1",
        help="Comma-separated ascending bucket edges for basis_b_pct. Default isolates medium/light discount ranges.",
    )
    p.add_argument(
        "--ab-tail-from",
        type=int,
        default=11,
        help="ab_bars >= this value are merged into one tail bucket like '11+'",
    )
    return p.parse_args()


def parse_time(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        ts = float(value)
        if ts > 1e12:
            ts /= 1000.0
        elif ts > 1e10:
            ts /= 1000.0
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    if s.isdigit():
        return parse_time(int(s))
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


def first_not_none(*vals: Any) -> Any:
    for v in vals:
        if v is not None:
            return v
    return None


def as_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
    except Exception:
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    return x


def as_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        try:
            return int(float(v))
        except Exception:
            return None


def parse_edges(text: str) -> List[float]:
    edges = [float(x.strip()) for x in text.split(",") if x.strip()]
    if len(edges) < 2:
        raise ValueError("basis bucket edges must contain at least 2 values")
    if any(edges[i] >= edges[i + 1] for i in range(len(edges) - 1)):
        raise ValueError("basis bucket edges must be strictly ascending")
    return edges


def label_basis_bucket(x: float, edges: List[float]) -> str:
    for i in range(len(edges) - 1):
        lo, hi = edges[i], edges[i + 1]
        if (x >= lo) and (x < hi or (i == len(edges) - 2 and x <= hi)):
            if i == 0:
                return f"[{lo:.4f},{hi:.4f})"
            if i == len(edges) - 2:
                return f"[{lo:.4f},{hi:.4f}]"
            return f"[{lo:.4f},{hi:.4f})"
    return f"<{edges[0]:.4f}" if x < edges[0] else f">={edges[-1]:.4f}"


def label_ab_bucket(ab: int, tail_from: int) -> str:
    return f"{tail_from}+" if ab >= tail_from else str(ab)


def load_rows(path: Path, start_dt: Optional[datetime], end_dt: Optional[datetime]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            ctx = obj.get("context") or {}
            exit_time = first_not_none(obj.get("exit_time"), obj.get("close_time"), obj.get("time"))
            exit_dt = parse_time(exit_time)
            if start_dt is not None:
                if exit_dt is None or exit_dt < start_dt:
                    continue
            if end_dt is not None:
                if exit_dt is None or exit_dt >= end_dt:
                    continue

            pnl = as_float(first_not_none(obj.get("pnl_pct"), obj.get("pnl")))
            b_contract = as_float(first_not_none(obj.get("b_contract_price"), ctx.get("b_contract_price")))
            b_index = as_float(first_not_none(obj.get("b_index_price"), ctx.get("b_index_price")))
            ab_bars = as_int(first_not_none(obj.get("ab_bars"), ctx.get("ab_bars")))
            bc_bars = as_int(first_not_none(obj.get("bc_bars"), ctx.get("bc_bars")))
            rebound_ratio = as_float(first_not_none(obj.get("rebound_ratio"), ctx.get("rebound_ratio")))
            drop_window_chg = as_float(first_not_none(obj.get("drop_window_chg"), ctx.get("drop_window_chg")))
            chg_24h = as_float(first_not_none(obj.get("chg_24h"), ctx.get("chg_24h")))
            reason = first_not_none(obj.get("exit_reason"), obj.get("reason"), "")
            symbol = first_not_none(obj.get("symbol"), ctx.get("symbol"), "")
            signal_time = first_not_none(obj.get("signal_time"), ctx.get("signal_time"), "")

            if pnl is None or b_contract is None or b_index is None or ab_bars is None:
                continue
            if b_index == 0:
                continue
            basis_b_pct = (b_contract - b_index) / b_index
            rows.append(
                {
                    "symbol": symbol,
                    "signal_time": signal_time,
                    "exit_time": exit_time,
                    "pnl_pct": pnl,
                    "reason": reason,
                    "ab_bars": ab_bars,
                    "bc_bars": bc_bars,
                    "rebound_ratio": rebound_ratio,
                    "drop_window_chg": drop_window_chg,
                    "chg_24h": chg_24h,
                    "b_contract_price": b_contract,
                    "b_index_price": b_index,
                    "basis_b_pct": basis_b_pct,
                }
            )
    return rows


def safe_mean(vals: List[float]) -> Optional[float]:
    return mean(vals) if vals else None


def safe_median(vals: List[float]) -> Optional[float]:
    return median(vals) if vals else None


def classify_pnl(x: float) -> str:
    if x > 0:
        return "profit"
    if x < 0:
        return "loss"
    return "flat"


def summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    pnls = [r["pnl_pct"] for r in rows]
    basis = [r["basis_b_pct"] for r in rows]
    abs = [r["ab_bars"] for r in rows if r.get("ab_bars") is not None]
    bc = [r["bc_bars"] for r in rows if r.get("bc_bars") is not None]
    rr = [r["rebound_ratio"] for r in rows if r.get("rebound_ratio") is not None]
    dw = [r["drop_window_chg"] for r in rows if r.get("drop_window_chg") is not None]
    ch24 = [r["chg_24h"] for r in rows if r.get("chg_24h") is not None]
    tp = sum(1 for r in rows if str(r.get("reason", "")).upper() == "TAKE_PROFIT")
    sl = sum(1 for r in rows if str(r.get("reason", "")).upper() == "STOP_LOSS")
    ts = sum(1 for r in rows if str(r.get("reason", "")).upper() == "TIME_STOP")
    profits = sum(1 for x in pnls if x > 0)
    losses = sum(1 for x in pnls if x < 0)
    flats = sum(1 for x in pnls if x == 0)
    return {
        "count": len(rows),
        "profit_count": profits,
        "loss_count": losses,
        "flat_count": flats,
        "win_rate": (profits / len(rows)) if rows else None,
        "pnl_sum": sum(pnls) if pnls else None,
        "avg_pnl": safe_mean(pnls),
        "median_pnl": safe_median(pnls),
        "tp_count": tp,
        "sl_count": sl,
        "ts_count": ts,
        "basis_mean": safe_mean(basis),
        "basis_median": safe_median(basis),
        "ab_bars_mean": safe_mean([float(x) for x in abs]) if abs else None,
        "ab_bars_median": safe_median([float(x) for x in abs]) if abs else None,
        "bc_bars_mean": safe_mean([float(x) for x in bc]) if bc else None,
        "bc_bars_median": safe_median([float(x) for x in bc]) if bc else None,
        "rebound_ratio_mean": safe_mean(rr),
        "rebound_ratio_median": safe_median(rr),
        "drop_window_chg_mean": safe_mean(dw),
        "drop_window_chg_median": safe_median(dw),
        "chg_24h_mean": safe_mean(ch24),
        "chg_24h_median": safe_median(ch24),
    }


def write_csv(path: Path, rows: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def main() -> None:
    args = _parse_args_fixed()
    trades_jsonl = Path(args.trades_jsonl or f"output/state/sim_trades.{args.run_id}.jsonl")
    out_dir = Path(args.out_dir or f"output/state/ab_basis_2d_audit.{args.run_id}")
    out_dir.mkdir(parents=True, exist_ok=True)
    start_dt = parse_time(args.start_iso) if args.start_iso else None
    end_dt = parse_time(args.end_iso) if args.end_iso else None
    edges = parse_edges(args.basis_buckets)

    rows = load_rows(trades_jsonl, start_dt=start_dt, end_dt=end_dt)
    if not rows:
        raise RuntimeError("未读取到可用 trade；请检查 sim_trades 是否包含 pnl_pct / ab_bars / context.b_contract_price / context.b_index_price")

    groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        ab_label = label_ab_bucket(r["ab_bars"], args.ab_tail_from)
        basis_label = label_basis_bucket(r["basis_b_pct"], edges)
        groups[(ab_label, basis_label)].append(r)

    def ab_sort_key(lbl: str) -> Tuple[int, int]:
        if lbl.endswith("+"):
            return (1, int(lbl[:-1]))
        return (0, int(lbl))

    ab_labels = sorted({k[0] for k in groups}, key=ab_sort_key)
    basis_labels = []
    seen = set()
    for r in rows:
        lbl = label_basis_bucket(r["basis_b_pct"], edges)
        if lbl not in seen:
            basis_labels.append(lbl)
            seen.add(lbl)

    summary_rows: List[Dict[str, Any]] = []
    sample_rows: List[Dict[str, Any]] = []
    for ab_label in ab_labels:
        for basis_label in basis_labels:
            cell = groups.get((ab_label, basis_label), [])
            if not cell:
                continue
            s = summarize(cell)
            row = {"ab_bucket": ab_label, "basis_bucket": basis_label, **s}
            summary_rows.append(row)
            for item in cell:
                sample_rows.append(
                    {
                        "ab_bucket": ab_label,
                        "basis_bucket": basis_label,
                        **item,
                    }
                )

    summary_fields = [
        "ab_bucket", "basis_bucket", "count", "profit_count", "loss_count", "flat_count", "win_rate",
        "pnl_sum", "avg_pnl", "median_pnl", "tp_count", "sl_count", "ts_count",
        "basis_mean", "basis_median", "ab_bars_mean", "ab_bars_median", "bc_bars_mean", "bc_bars_median",
        "rebound_ratio_mean", "rebound_ratio_median", "drop_window_chg_mean", "drop_window_chg_median",
        "chg_24h_mean", "chg_24h_median",
    ]
    sample_fields = [
        "ab_bucket", "basis_bucket", "symbol", "signal_time", "exit_time", "pnl_pct", "reason",
        "ab_bars", "bc_bars", "rebound_ratio", "drop_window_chg", "chg_24h",
        "b_contract_price", "b_index_price", "basis_b_pct",
    ]

    write_csv(out_dir / "ab_basis_2d_summary.csv", summary_rows, summary_fields)
    write_csv(out_dir / "ab_basis_2d_samples.csv", sample_rows, sample_fields)

    payload = {
        "run_id": args.run_id,
        "trades_jsonl": str(trades_jsonl),
        "out_dir": str(out_dir),
        "start_iso": args.start_iso,
        "end_iso": args.end_iso,
        "basis_bucket_edges": edges,
        "ab_tail_from": args.ab_tail_from,
        "all_trades": len(rows),
        "non_empty_cells": len(summary_rows),
        "overall": summarize(rows),
    }
    with (out_dir / "ab_basis_2d_summary.json").open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print("=== audit_ab_basis_2d 完成 ===")
    print(f"run_id          : {args.run_id}")
    print(f"trades_jsonl    : {trades_jsonl}")
    print(f"out_dir         : {out_dir}")
    print(f"start_iso       : {args.start_iso}")
    print(f"end_iso         : {args.end_iso}")
    print(f"basis_edges     : {edges}")
    print(f"ab_tail_from    : {args.ab_tail_from}")
    print(f"all_trades      : {len(rows)}")
    print(f"non_empty_cells : {len(summary_rows)}")
    print("written         : ab_basis_2d_summary.csv, ab_basis_2d_samples.csv, ab_basis_2d_summary.json")


if __name__ == "__main__":
    main()
