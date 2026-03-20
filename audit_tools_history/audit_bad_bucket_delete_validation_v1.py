#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
audit_bad_bucket_delete_validation_v1.py

针对 Snapback-ABC_BINDEX 的单一坏桶，做“删桶验证”。

当前默认坏桶：
    ab_bars <= 2 and rebound_ratio <= 0.15

输出：
1) 全样本 before/after
2) pre-regime before/after
3) post-regime before/after
4) 坏桶自身统计（overall/pre/post）
5) 删桶影响占比表
6) 坏桶样本清单（按 pnl_pct 从差到好）
7) summary.json

说明：
- 字段抽取与当前 visual_audit_bucketizer_v5 对齐：优先取 trade 顶层字段，缺失时回退到 context。
- ab_bars 缺失时，若 context 内有 a_time / b_time，则按毫秒差换算分钟数。
- regime 切分基于 signal_time（毫秒 UTC）与用户提供的 cutoff。
"""

import argparse
import csv
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple


@dataclass
class TradeRow:
    symbol: str
    signal_time: Optional[int]
    entry_time: Optional[int]
    pnl_pct: float
    reason: Optional[str]
    ab_bars: Optional[float]
    bc_bars: Optional[float]
    bc_ab_ratio: Optional[float]
    drop_pct: Optional[float]
    rebound_ratio: Optional[float]
    drop_window_chg: Optional[float]
    chg_24h: Optional[float]


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades-jsonl", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--run-id", required=True)

    ap.add_argument("--ab-max", type=float, default=2.0,
                    help="坏桶条件：ab_bars <= ab_max，默认 2")
    ap.add_argument("--rebound-max", type=float, default=0.15,
                    help="坏桶条件：rebound_ratio <= rebound_max，默认 0.15")

    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--regime-cutoff-ms", type=int,
                       help="按 signal_time 毫秒 UTC 做 pre/post 切分")
    group.add_argument("--regime-cutoff-iso",
                       help="按 ISO 时间做 pre/post 切分，例如 2025-12-01T00:00:00+00:00")
    return ap.parse_args()


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def to_float(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        if isinstance(v, float) and math.isnan(v):
            return None
        return float(v)
    try:
        x = float(v)
        if math.isnan(x):
            return None
        return x
    except Exception:
        return None


def to_int(v):
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        if math.isnan(v):
            return None
        return int(v)
    try:
        return int(v)
    except Exception:
        return None


def median(vals: List[float]):
    clean = [v for v in vals if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not clean:
        return ""
    clean.sort()
    n = len(clean)
    if n % 2 == 1:
        return clean[n // 2]
    return (clean[n // 2 - 1] + clean[n // 2]) / 2


def pct_change(before, after):
    if before in (None, ""):
        return ""
    if before == 0:
        return ""
    if after in (None, ""):
        return ""
    return (after - before) / before


def iso_to_ms(s: str) -> int:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def load_trade_rows(path: Path) -> List[TradeRow]:
    rows: List[TradeRow] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            ctx = obj.get("context", {}) or {}

            ab_bars = to_float(obj.get("ab_bars", ctx.get("ab_bars")))
            if ab_bars is None and ctx.get("a_time") is not None and ctx.get("b_time") is not None:
                a_time = to_int(ctx.get("a_time"))
                b_time = to_int(ctx.get("b_time"))
                if a_time is not None and b_time is not None:
                    ab_bars = (b_time - a_time) / 60000.0

            row = TradeRow(
                symbol=obj.get("symbol") or "",
                signal_time=to_int(obj.get("signal_time")),
                entry_time=to_int(obj.get("entry_time")),
                pnl_pct=to_float(obj.get("pnl_pct")) or 0.0,
                reason=obj.get("reason"),
                ab_bars=ab_bars,
                bc_bars=to_float(obj.get("bc_bars", ctx.get("bc_bars"))),
                bc_ab_ratio=to_float(obj.get("bc_ab_ratio", ctx.get("bc_ab_ratio"))),
                drop_pct=to_float(obj.get("drop_pct", ctx.get("drop_pct"))),
                rebound_ratio=to_float(obj.get("rebound_ratio", ctx.get("rebound_ratio"))),
                drop_window_chg=to_float(obj.get("drop_window_chg", ctx.get("drop_window_chg"))),
                chg_24h=to_float(obj.get("chg_24h", ctx.get("chg_24h"))),
            )
            rows.append(row)
    return rows


def in_bad_bucket(row: TradeRow, ab_max: float, rebound_max: float) -> bool:
    if row.ab_bars is None or row.rebound_ratio is None:
        return False
    return row.ab_bars <= ab_max and row.rebound_ratio <= rebound_max


def regime_of(row: TradeRow, cutoff_ms: int) -> str:
    if row.signal_time is None:
        return "unknown"
    return "pre" if row.signal_time < cutoff_ms else "post"


def summarize_rows(scope: str, rows: List[TradeRow]) -> Dict[str, object]:
    wins = sum(1 for r in rows if r.pnl_pct > 0)
    losses = sum(1 for r in rows if r.pnl_pct < 0)
    flats = sum(1 for r in rows if r.pnl_pct == 0)
    pnls = [r.pnl_pct for r in rows]
    count = len(rows)
    pnl_sum = sum(pnls)
    return {
        "scope": scope,
        "trades": count,
        "wins": wins,
        "losses": losses,
        "flats": flats,
        "win_rate": (wins / count) if count else "",
        "pnl_sum": pnl_sum if count else "",
        "avg_pnl": (pnl_sum / count) if count else "",
        "median_pnl": median(pnls),
    }


def before_after_rows(scope_prefix: str, before_rows: List[TradeRow], after_rows: List[TradeRow]) -> List[Dict[str, object]]:
    return [
        summarize_rows(f"{scope_prefix}_before", before_rows),
        summarize_rows(f"{scope_prefix}_after_exclude_bucket", after_rows),
    ]


def bucket_stats_rows(bucket_rows: List[TradeRow], cutoff_ms: int) -> List[Dict[str, object]]:
    overall = summarize_rows("bucket_overall", bucket_rows)
    pre = summarize_rows("bucket_pre", [r for r in bucket_rows if regime_of(r, cutoff_ms) == "pre"])
    post = summarize_rows("bucket_post", [r for r in bucket_rows if regime_of(r, cutoff_ms) == "post"])
    return [overall, pre, post]


def impact_rows(before_rows: List[TradeRow], after_rows: List[TradeRow]) -> List[Dict[str, object]]:
    before = summarize_rows("before", before_rows)
    after = summarize_rows("after", after_rows)
    metrics = ["trades", "wins", "losses", "pnl_sum"]
    rows = []
    for metric in metrics:
        b = before[metric]
        a = after[metric]
        delta = (a - b) if isinstance(a, (int, float)) and isinstance(b, (int, float)) else ""
        rows.append({
            "metric": metric,
            "before": b,
            "after": a,
            "delta": delta,
            "delta_pct": pct_change(b, a),
        })
    return rows


def trade_to_dict(row: TradeRow) -> Dict[str, object]:
    return {
        "symbol": row.symbol,
        "signal_time": row.signal_time,
        "entry_time": row.entry_time,
        "pnl_pct": row.pnl_pct,
        "reason": row.reason,
        "ab_bars": row.ab_bars,
        "bc_bars": row.bc_bars,
        "bc_ab_ratio": row.bc_ab_ratio,
        "drop_pct": row.drop_pct,
        "rebound_ratio": row.rebound_ratio,
        "drop_window_chg": row.drop_window_chg,
        "chg_24h": row.chg_24h,
    }


def write_csv(path: Path, rows: List[Dict[str, object]]):
    with path.open("w", newline="", encoding="utf-8") as f:
        if not rows:
            f.write("")
            return
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def main():
    args = parse_args()
    trades_path = Path(args.trades_jsonl)
    out_dir = Path(args.out_dir)
    ensure_dir(out_dir)

    cutoff_ms = args.regime_cutoff_ms if args.regime_cutoff_ms is not None else iso_to_ms(args.regime_cutoff_iso)

    all_rows = load_trade_rows(trades_path)
    bad_bucket_rows = [r for r in all_rows if in_bad_bucket(r, args.ab_max, args.rebound_max)]
    keep_rows = [r for r in all_rows if not in_bad_bucket(r, args.ab_max, args.rebound_max)]

    pre_before = [r for r in all_rows if regime_of(r, cutoff_ms) == "pre"]
    pre_after = [r for r in keep_rows if regime_of(r, cutoff_ms) == "pre"]
    post_before = [r for r in all_rows if regime_of(r, cutoff_ms) == "post"]
    post_after = [r for r in keep_rows if regime_of(r, cutoff_ms) == "post"]

    overall_table = before_after_rows("overall", all_rows, keep_rows)
    pre_table = before_after_rows("pre", pre_before, pre_after)
    post_table = before_after_rows("post", post_before, post_after)
    bucket_table = bucket_stats_rows(bad_bucket_rows, cutoff_ms)
    impact_table = impact_rows(all_rows, keep_rows)

    bucket_samples = [trade_to_dict(r) for r in sorted(bad_bucket_rows, key=lambda x: x.pnl_pct)]

    write_csv(out_dir / "overall_before_after.csv", overall_table)
    write_csv(out_dir / "pre_before_after.csv", pre_table)
    write_csv(out_dir / "post_before_after.csv", post_table)
    write_csv(out_dir / "bucket_stats.csv", bucket_table)
    write_csv(out_dir / "impact_summary.csv", impact_table)
    write_csv(out_dir / "bucket_samples.csv", bucket_samples)

    summary = {
        "run_id": args.run_id,
        "trades_jsonl": str(trades_path),
        "out_dir": str(out_dir),
        "bucket_rule": {
            "ab_bars_lte": args.ab_max,
            "rebound_ratio_lte": args.rebound_max,
        },
        "regime_cutoff_ms": cutoff_ms,
        "all_trades": len(all_rows),
        "bad_bucket_trades": len(bad_bucket_rows),
        "kept_trades": len(keep_rows),
        "files": {
            "overall_before_after": "overall_before_after.csv",
            "pre_before_after": "pre_before_after.csv",
            "post_before_after": "post_before_after.csv",
            "bucket_stats": "bucket_stats.csv",
            "impact_summary": "impact_summary.csv",
            "bucket_samples": "bucket_samples.csv",
        },
    }
    with (out_dir / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("=== audit_bad_bucket_delete_validation 完成 ===")
    print(f"run_id            : {args.run_id}")
    print(f"trades_jsonl      : {trades_path}")
    print(f"out_dir           : {out_dir}")
    print(f"regime_cutoff_ms  : {cutoff_ms}")
    print(f"bucket_rule       : ab_bars <= {args.ab_max}, rebound_ratio <= {args.rebound_max}")
    print(f"all_trades        : {len(all_rows)}")
    print(f"bad_bucket_trades : {len(bad_bucket_rows)}")
    print(f"kept_trades       : {len(keep_rows)}")
    print("written           : overall_before_after.csv, pre_before_after.csv, post_before_after.csv, bucket_stats.csv, impact_summary.csv, bucket_samples.csv, summary.json")


if __name__ == "__main__":
    main()
