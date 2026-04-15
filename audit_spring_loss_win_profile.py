#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spring-SABC loss/win structure audit.

Example:
PYTHONPATH=/root/bn_research_core /root/service_env/bin/python audit_spring_loss_win_profile.py \
  --trades output/state/sim_trades.SPRING_V1_30D_P6_0415T1941_ALL.jsonl \
  --out-dir output/state/spring_loss_win_profile.SPRING_V1_30D_P6_0415T1941_ALL
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None:
            return default
        if isinstance(v, str) and not v.strip():
            return default
        f = float(v)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


def safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(v)
    except Exception:
        return default


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise SystemExit(f"JSONL parse failed at line {line_no}: {e}") from e
    return rows


def month_from_bj(value: str) -> str:
    if not value:
        return "UNKNOWN"
    return value[:7] if len(value) >= 7 else "UNKNOWN"


def flatten_trade(t: Dict[str, Any]) -> Dict[str, Any]:
    ctx = dict(t.get("context") or {})
    pnl_pct = safe_float(t.get("pnl_pct"), 0.0) or 0.0
    entry_price = safe_float(t.get("entry_price"), None)
    sl_price = safe_float(ctx.get("stop_loss_price", t.get("sl_price")), None)
    tp_price = safe_float(t.get("tp_price"), None)

    risk_pct = None
    if entry_price is not None and sl_price is not None and entry_price > 0:
        risk_pct = (entry_price - sl_price) / entry_price

    reward_pct = None
    if entry_price is not None and tp_price is not None and entry_price > 0:
        reward_pct = (tp_price - entry_price) / entry_price

    return {
        "symbol": str(t.get("symbol") or ""),
        "signal_time_bj": str(t.get("signal_time_bj") or ""),
        "entry_time_bj": str(t.get("entry_time_bj") or ""),
        "exit_time_bj": str(t.get("exit_time_bj") or ""),
        "month": month_from_bj(str(t.get("entry_time_bj") or t.get("signal_time_bj") or "")),
        "reason": str(t.get("reason") or ""),
        "pnl_pct": pnl_pct,
        "pnl_u_100": pnl_pct * 100.0,
        "win_loss": "WIN" if pnl_pct > 0 else ("LOSS" if pnl_pct < 0 else "FLAT"),
        "entry_price": entry_price,
        "exit_price": safe_float(t.get("exit_price"), None),
        "risk_pct": risk_pct,
        "reward_pct": reward_pct,
        "score_order": safe_int(ctx.get("score_order"), 0),
        "score": safe_int(ctx.get("score"), 0),
        "chg_24h": safe_float(ctx.get("chg_24h"), None),
        "vol_24h": safe_float(ctx.get("vol_24h"), None),
        "a_time_ms": safe_int(ctx.get("a_time_ms"), 0),
        "b_time_ms": safe_int(ctx.get("b_time_ms"), 0),
        "c_time_ms": safe_int(ctx.get("c_time_ms"), 0),
        "a_close": safe_float(ctx.get("a_close"), None),
        "a_high": safe_float(ctx.get("a_high"), None),
        "b_close": safe_float(ctx.get("b_close"), None),
        "b_low": safe_float(ctx.get("b_low"), None),
        "c_close": safe_float(ctx.get("c_close"), None),
        "ab_bars": safe_int(ctx.get("ab_bars"), 0),
        "bc_bars": safe_int(ctx.get("bc_bars"), 0),
        "ab_down_run_bars": safe_int(ctx.get("ab_down_run_bars"), 0),
        "ab_required_bars_min": safe_int(ctx.get("ab_required_bars_min"), 0),
        "b_search_rank_from_c": safe_int(ctx.get("b_search_rank_from_c"), 0),
        "ab_chg_pct": safe_float(ctx.get("ab_chg_pct"), None),
        "rebound_ratio": safe_float(ctx.get("rebound_ratio"), None),
        "bc_over_ab_bars": safe_float(ctx.get("bc_over_ab_bars"), None),
        "vol_ratio": safe_float(ctx.get("vol_ratio"), None),
        "ab_avg_vol": safe_float(ctx.get("ab_avg_vol"), None),
        "baseline_avg_vol": safe_float(ctx.get("baseline_avg_vol"), None),
        "ab_low_min": safe_float(ctx.get("ab_low_min"), None),
        "b_low_is_ab_low": bool(ctx.get("b_low_is_ab_low")),
        "take_profit_mode": str(ctx.get("take_profit_mode") or ""),
        "abc_selection_mode": str(ctx.get("abc_selection_mode") or ""),
        "b_scan_direction": str(ctx.get("b_scan_direction") or ""),
        "b_initial_filter": str(ctx.get("b_initial_filter") or ""),
        "ab_down_rule": str(ctx.get("ab_down_rule") or ""),
    }


def mean(values: Iterable[Optional[float]]) -> Optional[float]:
    xs = [float(x) for x in values if x is not None]
    if not xs:
        return None
    return sum(xs) / len(xs)


def pct(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return x * 100.0


def summarize_rows(rows: List[Dict[str, Any]], group_keys: List[str]) -> List[Dict[str, Any]]:
    groups: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        key = tuple(r.get(k) for k in group_keys)
        groups[key].append(r)

    out: List[Dict[str, Any]] = []
    for key, items in groups.items():
        wins = [x for x in items if x["pnl_pct"] > 0]
        losses = [x for x in items if x["pnl_pct"] < 0]
        rec: Dict[str, Any] = {k: v for k, v in zip(group_keys, key)}
        rec.update({
            "trade_count": len(items),
            "win_count": len(wins),
            "loss_count": len(losses),
            "win_rate_pct": round(len(wins) / len(items) * 100.0, 2) if items else 0.0,
            "pnl_u_100_sum": round(sum(float(x["pnl_u_100"]) for x in items), 6),
            "avg_pnl_pct": round((mean([x["pnl_pct"] for x in items]) or 0.0) * 100.0, 6),
            "avg_win_pct": round((mean([x["pnl_pct"] for x in wins]) or 0.0) * 100.0, 6),
            "avg_loss_pct": round((mean([x["pnl_pct"] for x in losses]) or 0.0) * 100.0, 6),
            "max_win_pct": round(max([x["pnl_pct"] for x in items]) * 100.0, 6),
            "max_loss_pct": round(min([x["pnl_pct"] for x in items]) * 100.0, 6),
            "avg_score_order": round(mean([x["score_order"] for x in items]) or 0.0, 4),
            "avg_ab_chg_pct": round(pct(mean([x["ab_chg_pct"] for x in items])) or 0.0, 6),
            "avg_rebound_ratio_pct": round(pct(mean([x["rebound_ratio"] for x in items])) or 0.0, 6),
            "avg_vol_ratio": round(mean([x["vol_ratio"] for x in items]) or 0.0, 6),
            "avg_ab_bars": round(mean([x["ab_bars"] for x in items]) or 0.0, 4),
            "avg_bc_bars": round(mean([x["bc_bars"] for x in items]) or 0.0, 4),
            "avg_bc_over_ab": round(mean([x["bc_over_ab_bars"] for x in items]) or 0.0, 6),
            "avg_risk_pct": round(pct(mean([x["risk_pct"] for x in items])) or 0.0, 6),
            "avg_b_search_rank_from_c": round(mean([x["b_search_rank_from_c"] for x in items]) or 0.0, 4),
        })
        out.append(rec)

    out.sort(key=lambda x: (-x["trade_count"], x.get("pnl_u_100_sum", 0)))
    return out


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: List[str] = []
    for r in rows:
        for k in r.keys():
            if k not in fieldnames:
                fieldnames.append(k)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def bucketize(value: Optional[float], bins: List[Tuple[float, float, str]]) -> str:
    if value is None:
        return "NA"
    for lo, hi, label in bins:
        if lo <= value < hi:
            return label
    return "OTHER"


def add_buckets(rows: List[Dict[str, Any]]) -> None:
    for r in rows:
        r["ab_chg_bucket"] = bucketize(r["ab_chg_pct"], [
            (0.00, 0.06, "<6%"),
            (0.06, 0.08, "6-8%"),
            (0.08, 0.10, "8-10%"),
            (0.10, 0.15, "10-15%"),
            (0.15, 9.99, ">=15%"),
        ])
        r["rebound_bucket"] = bucketize(r["rebound_ratio"], [
            (0.00, 0.75, "<75%"),
            (0.75, 1.00, "75-100%"),
            (1.00, 1.50, "100-150%"),
            (1.50, 9.99, ">=150%"),
        ])
        r["vol_ratio_bucket"] = bucketize(r["vol_ratio"], [
            (0.00, 2.00, "<2"),
            (2.00, 3.00, "2-3"),
            (3.00, 5.00, "3-5"),
            (5.00, 9.99, "5-10"),
            (9.99, 9999.0, ">=10"),
        ])
        r["risk_bucket"] = bucketize(r["risk_pct"], [
            (0.00, 0.03, "<3%"),
            (0.03, 0.05, "3-5%"),
            (0.05, 0.08, "5-8%"),
            (0.08, 0.12, "8-12%"),
            (0.12, 9.99, ">=12%"),
        ])


def main() -> int:
    ap = argparse.ArgumentParser(description="Audit Spring-SABC win/loss structure profile.")
    ap.add_argument("--trades", required=True, help="Path to sim_trades.<RUN_ID>.jsonl")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--top-n", type=int, default=30, help="Top N largest winners/losers")
    args = ap.parse_args()

    trades_path = Path(args.trades)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    raw = read_jsonl(trades_path)
    rows = [flatten_trade(x) for x in raw]
    add_buckets(rows)

    wins = [r for r in rows if r["pnl_pct"] > 0]
    losses = [r for r in rows if r["pnl_pct"] < 0]

    summary = {
        "trades_path": str(trades_path),
        "trade_count": len(rows),
        "win_count": len(wins),
        "loss_count": len(losses),
        "win_rate_pct": round(len(wins) / len(rows) * 100.0, 2) if rows else 0.0,
        "gross_pnl_u_100": round(sum(r["pnl_u_100"] for r in rows), 6),
        "win_pnl_u_100": round(sum(r["pnl_u_100"] for r in wins), 6),
        "loss_pnl_u_100": round(sum(r["pnl_u_100"] for r in losses), 6),
        "avg_win_pct": round((mean([r["pnl_pct"] for r in wins]) or 0.0) * 100.0, 6),
        "avg_loss_pct": round((mean([r["pnl_pct"] for r in losses]) or 0.0) * 100.0, 6),
        "avg_win_ab_chg_pct": round((mean([r["ab_chg_pct"] for r in wins]) or 0.0) * 100.0, 6),
        "avg_loss_ab_chg_pct": round((mean([r["ab_chg_pct"] for r in losses]) or 0.0) * 100.0, 6),
        "avg_win_rebound_ratio_pct": round((mean([r["rebound_ratio"] for r in wins]) or 0.0) * 100.0, 6),
        "avg_loss_rebound_ratio_pct": round((mean([r["rebound_ratio"] for r in losses]) or 0.0) * 100.0, 6),
        "avg_win_vol_ratio": round(mean([r["vol_ratio"] for r in wins]) or 0.0, 6),
        "avg_loss_vol_ratio": round(mean([r["vol_ratio"] for r in losses]) or 0.0, 6),
        "avg_win_risk_pct": round((mean([r["risk_pct"] for r in wins]) or 0.0) * 100.0, 6),
        "avg_loss_risk_pct": round((mean([r["risk_pct"] for r in losses]) or 0.0) * 100.0, 6),
    }

    write_csv(out_dir / "trades_flat.csv", rows)
    write_csv(out_dir / "top_losses.csv", sorted(losses, key=lambda x: x["pnl_pct"])[: args.top_n])
    write_csv(out_dir / "top_wins.csv", sorted(wins, key=lambda x: x["pnl_pct"], reverse=True)[: args.top_n])

    write_csv(out_dir / "by_win_loss.csv", summarize_rows(rows, ["win_loss"]))
    write_csv(out_dir / "by_reason.csv", summarize_rows(rows, ["reason"]))
    write_csv(out_dir / "by_month.csv", summarize_rows(rows, ["month"]))
    write_csv(out_dir / "by_score_order.csv", summarize_rows(rows, ["score_order"]))
    write_csv(out_dir / "by_ab_bars.csv", summarize_rows(rows, ["ab_bars"]))
    write_csv(out_dir / "by_bc_bars.csv", summarize_rows(rows, ["bc_bars"]))
    write_csv(out_dir / "by_ab_chg_bucket.csv", summarize_rows(rows, ["ab_chg_bucket"]))
    write_csv(out_dir / "by_rebound_bucket.csv", summarize_rows(rows, ["rebound_bucket"]))
    write_csv(out_dir / "by_vol_ratio_bucket.csv", summarize_rows(rows, ["vol_ratio_bucket"]))
    write_csv(out_dir / "by_risk_bucket.csv", summarize_rows(rows, ["risk_bucket"]))
    write_csv(out_dir / "by_month_reason.csv", summarize_rows(rows, ["month", "reason"]))
    write_csv(out_dir / "by_score_order_reason.csv", summarize_rows(rows, ["score_order", "reason"]))

    with (out_dir / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("=== Spring-SABC loss/win profile audit done ===")
    print(f"trades_path : {trades_path}")
    print(f"out_dir     : {out_dir}")
    print(f"trades      : {summary['trade_count']}")
    print(f"wins/losses : {summary['win_count']} / {summary['loss_count']}")
    print(f"gross pnl   : {summary['gross_pnl_u_100']} U per 100U fixed stake")
    print("outputs     : summary.json, trades_flat.csv, top_losses.csv, top_wins.csv, grouped CSVs")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
