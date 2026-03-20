#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Compare two sim_trades runs with pre/post split.
Default use:
  old_run_id = baseline (e.g. bc_bars_min=1)
  new_run_id = candidate (e.g. bc_bars_min=0)

Matching is by (symbol, signal_time).
Outputs:
- overall_old_new.csv
- pre_old_new.csv
- post_old_new.csv
- added_trades.csv          # in new not in old
- removed_trades.csv        # in old not in new
- added_summary.csv
- removed_summary.csv
- summary.json
"""

from __future__ import annotations
import argparse
import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional, Tuple

DEFAULT_CUTOFF = "2026-01-01T00:00:00+00:00"


def parse_dt(v: Any) -> Optional[datetime]:
    if v is None or v == "":
        return None
    if isinstance(v, datetime):
        return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
    if isinstance(v, (int, float)):
        x = float(v)
        if x > 1e12:
            x /= 1000.0
        return datetime.fromtimestamp(x, tz=timezone.utc)
    s = str(v).strip()
    if not s:
        return None
    if s.isdigit():
        x = float(s)
        if x > 1e12:
            x /= 1000.0
        return datetime.fromtimestamp(x, tz=timezone.utc)
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def first_not_none(*vals):
    for v in vals:
        if v is not None:
            return v
    return None


def parse_reason(obj: Dict[str, Any]) -> str:
    return str(first_not_none(obj.get("exit_reason"), obj.get("reason"), "") or "")


def safe_float(v: Any) -> Optional[float]:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def load_trades(path: Path) -> List[Dict[str, Any]]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            ctx = obj.get("context") or {}
            symbol = str(first_not_none(obj.get("symbol"), ctx.get("symbol"), "") or "")
            signal_time = first_not_none(
                obj.get("signal_time"),
                obj.get("signal_time_ms"),
                ctx.get("signal_time"),
                ctx.get("signal_time_ms"),
            )
            signal_dt = parse_dt(signal_time)
            exit_time = first_not_none(obj.get("exit_time"), obj.get("exit_time_ms"))
            exit_dt = parse_dt(exit_time)
            pnl_pct = safe_float(first_not_none(obj.get("pnl_pct"), obj.get("pnl")))
            if symbol == "" or signal_dt is None or pnl_pct is None:
                continue
            reason = parse_reason(obj)
            row = {
                "symbol": symbol,
                "signal_time": signal_dt.isoformat(),
                "signal_dt": signal_dt,
                "exit_dt": exit_dt,
                "pnl_pct": pnl_pct,
                "reason": reason,
                "ab_bars": safe_float(first_not_none(obj.get("ab_bars"), ctx.get("ab_bars"))),
                "bc_bars": safe_float(first_not_none(obj.get("bc_bars"), ctx.get("bc_bars"))),
                "rebound_ratio": safe_float(first_not_none(obj.get("rebound_ratio"), ctx.get("rebound_ratio"))),
                "basis_b_pct": safe_float(first_not_none(obj.get("basis_b_pct"), ctx.get("basis_b_pct"))),
            }
            rows.append(row)
    return rows


def key_of(r: Dict[str, Any]) -> Tuple[str, str]:
    return (r["symbol"], r["signal_time"])


def summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    cnt = len(rows)
    profits = [r for r in rows if r["pnl_pct"] > 0]
    losses = [r for r in rows if r["pnl_pct"] < 0]
    flats = [r for r in rows if r["pnl_pct"] == 0]
    pnl_vals = [r["pnl_pct"] for r in rows]

    def rc(name: str) -> int:
        return sum(1 for r in rows if r["reason"] == name)

    out = {
        "count": cnt,
        "profit_count": len(profits),
        "loss_count": len(losses),
        "flat_count": len(flats),
        "win_rate": (len(profits) / cnt) if cnt else None,
        "pnl_sum": sum(pnl_vals) if cnt else 0.0,
        "avg_pnl": (sum(pnl_vals) / cnt) if cnt else None,
        "median_pnl": (median(pnl_vals) if cnt else None),
        "tp_count": rc("TAKE_PROFIT"),
        "sl_count": rc("STOP_LOSS"),
        "ts_count": rc("TIME_STOP"),
    }
    for field in ("ab_bars", "bc_bars", "rebound_ratio", "basis_b_pct"):
        vals = [r[field] for r in rows if r.get(field) is not None]
        out[f"{field}_mean"] = (sum(vals) / len(vals)) if vals else None
        out[f"{field}_median"] = (median(vals) if vals else None)
    return out


def split_pre_post(rows: List[Dict[str, Any]], cutoff: datetime) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    pre, post = [], []
    for r in rows:
        dt = r["exit_dt"] or r["signal_dt"]
        if dt < cutoff:
            pre.append(r)
        else:
            post.append(r)
    return pre, post


def write_dict_csv(path: Path, rows: List[Dict[str, Any]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with path.open("w", encoding="utf-8", newline="") as f:
            f.write("")
        return
    fields = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


def write_compare_csv(path: Path, sections: List[Tuple[str, Dict[str, Any]]]):
    rows = []
    for label, stats in sections:
        row = {"scope": label}
        row.update(stats)
        rows.append(row)
    write_dict_csv(path, rows)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--old-run-id", required=True)
    ap.add_argument("--new-run-id", required=True)
    ap.add_argument("--old-trades-jsonl")
    ap.add_argument("--new-trades-jsonl")
    ap.add_argument("--out-dir")
    ap.add_argument("--regime-cutoff-iso", default=DEFAULT_CUTOFF)
    args = ap.parse_args()

    old_path = Path(args.old_trades_jsonl or f"output/state/sim_trades.{args.old_run_id}.jsonl")
    new_path = Path(args.new_trades_jsonl or f"output/state/sim_trades.{args.new_run_id}.jsonl")
    out_dir = Path(args.out_dir or f"output/state/pre_post_diff_audit.{args.old_run_id}__VS__{args.new_run_id}")
    cutoff = parse_dt(args.regime_cutoff_iso)
    if cutoff is None:
        raise RuntimeError("invalid --regime-cutoff-iso")

    old_rows = load_trades(old_path)
    new_rows = load_trades(new_path)
    old_map = {key_of(r): r for r in old_rows}
    new_map = {key_of(r): r for r in new_rows}

    old_pre, old_post = split_pre_post(old_rows, cutoff)
    new_pre, new_post = split_pre_post(new_rows, cutoff)

    added_keys = sorted(set(new_map) - set(old_map))
    removed_keys = sorted(set(old_map) - set(new_map))
    added_rows = [new_map[k] for k in added_keys]
    removed_rows = [old_map[k] for k in removed_keys]
    added_pre, added_post = split_pre_post(added_rows, cutoff)
    removed_pre, removed_post = split_pre_post(removed_rows, cutoff)

    out_dir.mkdir(parents=True, exist_ok=True)
    write_compare_csv(out_dir / "overall_old_new.csv", [
        ("old_all", summarize(old_rows)),
        ("new_all", summarize(new_rows)),
    ])
    write_compare_csv(out_dir / "pre_old_new.csv", [
        ("old_pre", summarize(old_pre)),
        ("new_pre", summarize(new_pre)),
    ])
    write_compare_csv(out_dir / "post_old_new.csv", [
        ("old_post", summarize(old_post)),
        ("new_post", summarize(new_post)),
    ])

    write_dict_csv(out_dir / "added_trades.csv", [
        {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in r.items() if k not in ("signal_dt", "exit_dt")}
        for r in added_rows
    ])
    write_dict_csv(out_dir / "removed_trades.csv", [
        {k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in r.items() if k not in ("signal_dt", "exit_dt")}
        for r in removed_rows
    ])
    write_compare_csv(out_dir / "added_summary.csv", [
        ("added_all", summarize(added_rows)),
        ("added_pre", summarize(added_pre)),
        ("added_post", summarize(added_post)),
    ])
    write_compare_csv(out_dir / "removed_summary.csv", [
        ("removed_all", summarize(removed_rows)),
        ("removed_pre", summarize(removed_pre)),
        ("removed_post", summarize(removed_post)),
    ])

    summary = {
        "old_run_id": args.old_run_id,
        "new_run_id": args.new_run_id,
        "regime_cutoff_iso": cutoff.isoformat(),
        "old_all": summarize(old_rows),
        "new_all": summarize(new_rows),
        "old_pre": summarize(old_pre),
        "new_pre": summarize(new_pre),
        "old_post": summarize(old_post),
        "new_post": summarize(new_post),
        "added_all": summarize(added_rows),
        "added_pre": summarize(added_pre),
        "added_post": summarize(added_post),
        "removed_all": summarize(removed_rows),
        "removed_pre": summarize(removed_pre),
        "removed_post": summarize(removed_post),
    }
    with (out_dir / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("=== pre/post diff audit 完成 ===")
    print(f"old_run_id      : {args.old_run_id}")
    print(f"new_run_id      : {args.new_run_id}")
    print(f"old_trades_jsonl: {old_path}")
    print(f"new_trades_jsonl: {new_path}")
    print(f"out_dir         : {out_dir}")
    print(f"regime_cutoff   : {cutoff.isoformat()}")
    print(f"old_count       : {len(old_rows)}")
    print(f"new_count       : {len(new_rows)}")
    print(f"added_count     : {len(added_rows)}")
    print(f"removed_count   : {len(removed_rows)}")
    print("written         : overall_old_new.csv, pre_old_new.csv, post_old_new.csv, added_trades.csv, removed_trades.csv, added_summary.csv, removed_summary.csv, summary.json")


if __name__ == "__main__":
    main()
