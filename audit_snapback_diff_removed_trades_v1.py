#!/usr/bin/env python3
import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from statistics import median
from typing import Any, Dict, List, Optional, Tuple


DEFAULT_STATE_DIR = Path("output/state")


def first_not_none(*vals):
    for v in vals:
        if v is not None:
            return v
    return None


def parse_dt_utc(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        # epoch ms or seconds
        fv = float(value)
        if abs(fv) >= 1e11:
            return datetime.fromtimestamp(fv / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(fv, tz=timezone.utc)
    s = str(value).strip()
    if not s:
        return None
    # numeric string
    if s.isdigit() or (s.startswith('-') and s[1:].isdigit()):
        iv = int(s)
        if abs(iv) >= 10**11:
            return datetime.fromtimestamp(iv / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(iv, tz=timezone.utc)
    if s.endswith('Z'):
        s = s[:-1] + '+00:00'
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def key_for_trade(obj: Dict[str, Any]) -> Tuple[Any, Any]:
    return (obj.get("symbol"), obj.get("signal_time"))


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def basis_b_pct(obj: Dict[str, Any]) -> Optional[float]:
    ctx = obj.get("context") or {}
    v = first_not_none(ctx.get("basis_b_pct"), obj.get("basis_b_pct"))
    if v is not None:
        try:
            return float(v)
        except Exception:
            return None
    b_contract = first_not_none(ctx.get("b_contract_price"), obj.get("b_contract_price"))
    b_index = first_not_none(ctx.get("b_index_price"), obj.get("b_index_price"))
    if b_contract is None or b_index in (None, 0, 0.0):
        return None
    try:
        return (float(b_contract) - float(b_index)) / float(b_index)
    except Exception:
        return None


def summarize(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    pnl_list = [float(r.get("pnl_pct", 0.0)) for r in rows]
    reasons = Counter((r.get("reason") or r.get("exit_reason") or "UNKNOWN") for r in rows)
    basis_list = [b for b in (basis_b_pct(r) for r in rows) if b is not None]
    res = {
        "count": len(rows),
        "profit_count": sum(1 for x in pnl_list if x > 0),
        "loss_count": sum(1 for x in pnl_list if x < 0),
        "flat_count": sum(1 for x in pnl_list if x == 0),
        "win_rate": (sum(1 for x in pnl_list if x > 0) / len(pnl_list)) if pnl_list else 0.0,
        "pnl_sum": sum(pnl_list),
        "avg_pnl": (sum(pnl_list) / len(pnl_list)) if pnl_list else 0.0,
        "median_pnl": median(pnl_list) if pnl_list else 0.0,
        "tp_count": int(reasons.get("TAKE_PROFIT", 0)),
        "sl_count": int(reasons.get("STOP_LOSS", 0)),
        "ts_count": int(reasons.get("TIME_STOP", 0)),
        "basis_mean": (sum(basis_list) / len(basis_list)) if basis_list else None,
        "basis_median": median(basis_list) if basis_list else None,
    }
    return res


def split_by_cutoff(rows: List[Dict[str, Any]], cutoff_dt: datetime) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    pre, post = [], []
    for r in rows:
        exit_time = first_not_none(r.get("exit_time"), r.get("signal_time"), r.get("entry_time"))
        dt = parse_dt_utc(exit_time)
        if dt is None:
            continue
        if dt < cutoff_dt:
            pre.append(r)
        else:
            post.append(r)
    return pre, post


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "symbol", "signal_time_bj", "entry_time_bj", "exit_time_bj", "pnl_pct", "reason",
        "basis_b_pct", "ab_bars", "bc_bars", "rebound_ratio", "drop_window_chg", "chg_24h",
        "b_contract_price", "b_index_price", "signal_time", "entry_time", "exit_time"
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            ctx = r.get("context") or {}
            w.writerow({
                "symbol": r.get("symbol"),
                "signal_time_bj": r.get("signal_time_bj"),
                "entry_time_bj": r.get("entry_time_bj"),
                "exit_time_bj": r.get("exit_time_bj"),
                "pnl_pct": r.get("pnl_pct"),
                "reason": r.get("reason") or r.get("exit_reason"),
                "basis_b_pct": basis_b_pct(r),
                "ab_bars": ctx.get("ab_bars"),
                "bc_bars": ctx.get("bc_bars"),
                "rebound_ratio": ctx.get("rebound_ratio"),
                "drop_window_chg": ctx.get("drop_window_chg"),
                "chg_24h": ctx.get("chg_24h"),
                "b_contract_price": ctx.get("b_contract_price"),
                "b_index_price": ctx.get("b_index_price"),
                "signal_time": r.get("signal_time"),
                "entry_time": r.get("entry_time"),
                "exit_time": r.get("exit_time"),
            })


def write_summary_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    sections = []
    sections.append(("removed_only", summarize(rows)))
    cutoff = parse_dt_utc("2026-01-01T00:00:00+00:00")
    pre, post = split_by_cutoff(rows, cutoff)
    sections.append(("removed_pre", summarize(pre)))
    sections.append(("removed_post", summarize(post)))

    with path.open("w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "scope", "count", "profit_count", "loss_count", "flat_count", "win_rate",
            "pnl_sum", "avg_pnl", "median_pnl", "tp_count", "sl_count", "ts_count",
            "basis_mean", "basis_median"
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for name, stat in sections:
            row = {"scope": name}
            row.update(stat)
            w.writerow(row)


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit trades removed by new Snapback version vs baseline.")
    ap.add_argument("--old-run-id", required=True)
    ap.add_argument("--new-run-id", required=True)
    ap.add_argument("--old-trades-jsonl")
    ap.add_argument("--new-trades-jsonl")
    ap.add_argument("--out-dir")
    ap.add_argument("--regime-cutoff-iso", default="2026-01-01T00:00:00+00:00")
    args = ap.parse_args()

    old_path = Path(args.old_trades_jsonl) if args.old_trades_jsonl else DEFAULT_STATE_DIR / f"sim_trades.{args.old_run_id}.jsonl"
    new_path = Path(args.new_trades_jsonl) if args.new_trades_jsonl else DEFAULT_STATE_DIR / f"sim_trades.{args.new_run_id}.jsonl"
    out_dir = Path(args.out_dir) if args.out_dir else DEFAULT_STATE_DIR / f"diff_audit.{args.old_run_id}__VS__{args.new_run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)

    old_rows = load_jsonl(old_path)
    new_rows = load_jsonl(new_path)

    old_map = {key_for_trade(r): r for r in old_rows}
    new_keys = {key_for_trade(r) for r in new_rows}

    removed = [old_map[k] for k in old_map.keys() - new_keys]
    removed.sort(key=lambda r: (float(r.get("pnl_pct", 0.0)), r.get("symbol") or ""))

    # summary json
    cutoff = parse_dt_utc(args.regime_cutoff_iso)
    pre, post = split_by_cutoff(removed, cutoff)
    summary = {
        "old_run_id": args.old_run_id,
        "new_run_id": args.new_run_id,
        "old_trades": len(old_rows),
        "new_trades": len(new_rows),
        "removed_count": len(removed),
        "added_count": len({key_for_trade(r) for r in new_rows} - set(old_map.keys())),
        "removed_summary": summarize(removed),
        "removed_pre_summary": summarize(pre),
        "removed_post_summary": summarize(post),
        "regime_cutoff_iso": args.regime_cutoff_iso,
    }

    with (out_dir / "removed_trades_summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    write_csv(out_dir / "removed_trades.csv", removed)
    write_summary_csv(out_dir / "removed_trades_summary.csv", removed)

    print("=== audit_snapback_diff_removed_trades 完成 ===")
    print(f"old_run_id     : {args.old_run_id}")
    print(f"new_run_id     : {args.new_run_id}")
    print(f"old_trades     : {len(old_rows)}")
    print(f"new_trades     : {len(new_rows)}")
    print(f"removed_trades : {len(removed)}")
    print(f"out_dir        : {out_dir}")
    print("written        : removed_trades.csv, removed_trades_summary.csv, removed_trades_summary.json")


if __name__ == "__main__":
    main()
