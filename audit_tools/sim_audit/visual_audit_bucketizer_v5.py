#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
visual_audit_bucketizer_v5.py

基于 sim_trades + png 目录，按 ab_bars × rebound_ratio 分桶，
并把 png 复制/链接到带盈亏摘要的 profit/loss/flat 子目录。

已按真实链路对齐：
- png 文件名时间 = signal_time(UTC) + 1 minute
- 文件名格式: SNAP_YYYYMMDD_HHMM_SYMBOL_TP|TS|SL.png
"""
import argparse
import csv
import json
import math
import os
import re
import shutil
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from pathlib import Path

PNG_PAT = re.compile(r"^SNAP_(\d{8}_\d{4})_(.+)_(TP|TS|SL)\.png$")

REASON_MAP = {
    "TAKE_PROFIT": "TP",
    "TIME_STOP": "TS",
    "STOP_LOSS": "SL",
}

AB_BUCKETS = [
    ("<=2", lambda x: x <= 2),
    ("(2,4]", lambda x: 2 < x <= 4),
    ("(4,6]", lambda x: 4 < x <= 6),
    ("(6,10]", lambda x: 6 < x <= 10),
    (">10", lambda x: x > 10),
]

REB_BUCKETS = [
    ("<=0.15", lambda x: x <= 0.15),
    ("(0.15,0.30]", lambda x: 0.15 < x <= 0.30),
    ("(0.30,0.50]", lambda x: 0.30 < x <= 0.50),
    ("(0.50,0.70]", lambda x: 0.50 < x <= 0.70),
    (">0.70", lambda x: x > 0.70),
]


def bucketize(value, buckets):
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return "MISSING"
    for name, fn in buckets:
        if fn(value):
            return name
    return "MISSING"


def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--png-dir")
    ap.add_argument("--trades-jsonl")
    ap.add_argument("--out-dir")
    ap.add_argument("--copy-mode", choices=["copy", "symlink"], default="copy")
    return ap.parse_args()


def resolve_paths(args):
    run_id = args.run_id
    png_dir = Path(args.png_dir) if args.png_dir else Path("output/state") / f"sim_viz_{run_id}"
    trades_jsonl = Path(args.trades_jsonl) if args.trades_jsonl else Path("output/state") / f"sim_trades.{run_id}.jsonl"
    out_dir = Path(args.out_dir) if args.out_dir else Path("output/visual_audit") / run_id

    missing = []
    if not png_dir.exists():
        missing.append(f"png dir not found: {png_dir}")
    if not png_dir.is_dir():
        missing.append(f"png dir is not a directory: {png_dir}")
    if not trades_jsonl.exists():
        missing.append(f"trades jsonl not found: {trades_jsonl}")
    if trades_jsonl.exists() and not trades_jsonl.is_file():
        missing.append(f"trades jsonl is not a file: {trades_jsonl}")
    if missing:
        raise SystemExit("\n".join(missing))

    return png_dir, trades_jsonl, out_dir


def trade_key(obj):
    ts = obj.get("signal_time")
    symbol = obj.get("symbol")
    reason = REASON_MAP.get(obj.get("reason"), obj.get("reason"))
    if ts is None or not symbol or not reason:
        return None
    kdt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc) + timedelta(minutes=1)
    return (kdt.strftime("%Y%m%d_%H%M"), symbol, reason)


def parse_png_key(name: str):
    m = PNG_PAT.match(name)
    if not m:
        return None
    return (m.group(1), m.group(2), m.group(3))


def load_trades(path: Path):
    by_key = {}
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            key = trade_key(obj)
            if key is None:
                continue
            by_key[key] = obj
    return by_key


def action_copy(src: Path, dst: Path, mode: str):
    if dst.exists() or dst.is_symlink():
        dst.unlink()
    if mode == "copy":
        shutil.copy2(src, dst)
    else:
        os.symlink(src, dst)


def median(vals):
    vals = [v for v in vals if v is not None and not (isinstance(v, float) and math.isnan(v))]
    if not vals:
        return ""
    vals = sorted(vals)
    n = len(vals)
    return vals[n // 2] if n % 2 == 1 else (vals[n // 2 - 1] + vals[n // 2]) / 2


def fmt_pnl_sum(x: float) -> str:
    return f"{x:+.6f}"


def outcome_dir_name(outcome: str, rows) -> str:
    count = len(rows)
    pnl_sum = sum((r.get("pnl_pct") or 0.0) for r in rows)
    return f"{outcome}__n{count}__sum{fmt_pnl_sum(pnl_sum)}"


def main():
    args = parse_args()
    png_dir, trades_path, out_dir = resolve_paths(args)
    reports_dir = out_dir / "reports"
    ensure_dir(reports_dir)

    trades = load_trades(trades_path)
    matched_trade_keys = set()
    unmatched_png = []
    matched = 0

    # 第一遍：完成匹配与分桶，先不落文件，先收集行，方便给目录名加入盈亏摘要
    cluster_rows = []
    cluster_acc = defaultdict(list)
    outcome_acc = defaultdict(list)  # key=(cluster_id, outcome)
    png_assignments = []

    png_files = sorted(png_dir.glob("*.png"))
    for p in png_files:
        key = parse_png_key(p.name)
        if key is None:
            unmatched_png.append({"png_filename": p.name, "reason": "filename_no_match"})
            continue
        trade = trades.get(key)
        if trade is None:
            unmatched_png.append({
                "png_filename": p.name,
                "reason": "no_trade_match",
                "parsed_time": key[0],
                "parsed_symbol": key[1],
                "parsed_reason": key[2],
            })
            continue

        matched += 1
        matched_trade_keys.add(key)
        ctx = trade.get("context", {}) or {}

        ab_bars = trade.get("ab_bars", ctx.get("ab_bars"))
        if ab_bars is None and ctx.get("a_time") is not None and ctx.get("b_time") is not None:
            ab_bars = int((ctx["b_time"] - ctx["a_time"]) / 60000)
        rebound_ratio = trade.get("rebound_ratio", ctx.get("rebound_ratio"))
        bc_bars = trade.get("bc_bars", ctx.get("bc_bars"))
        bc_ab_ratio = trade.get("bc_ab_ratio", ctx.get("bc_ab_ratio"))
        drop_pct = trade.get("drop_pct", ctx.get("drop_pct"))
        drop_window_chg = trade.get("drop_window_chg", ctx.get("drop_window_chg"))

        ab_bucket = bucketize(ab_bars, AB_BUCKETS)
        rebound_bucket = bucketize(rebound_ratio, REB_BUCKETS)
        cluster_id = f"ab_{ab_bucket}__reb_{rebound_bucket}"

        pnl = trade.get("pnl_pct", 0.0)
        if pnl > 0:
            outcome = "profit"
        elif pnl < 0:
            outcome = "loss"
        else:
            outcome = "flat"

        row = {
            "cluster_id": cluster_id,
            "bucket_path": "",  # 第二遍填充真实目录
            "png_filename": p.name,
            "symbol": trade.get("symbol"),
            "signal_time": trade.get("signal_time"),
            "entry_time": trade.get("entry_time"),
            "pnl_pct": pnl,
            "reason": trade.get("reason"),
            "ab_bars": ab_bars,
            "bc_bars": bc_bars,
            "bc_ab_ratio": bc_ab_ratio,
            "drop_pct": drop_pct,
            "rebound_ratio": rebound_ratio,
            "drop_window_chg": drop_window_chg,
            "outcome": outcome,
        }
        cluster_rows.append(row)
        cluster_acc[cluster_id].append(row)
        outcome_acc[(cluster_id, outcome)].append(row)
        png_assignments.append((p, row))

    # 第二遍：根据 outcome 桶的样本数和盈亏汇总命名目录，再落 png
    outcome_dir_map = {}
    for (cluster_id, outcome), rows in outcome_acc.items():
        outcome_dir_map[(cluster_id, outcome)] = outcome_dir_name(outcome, rows)

    for src_png, row in png_assignments:
        cluster_id = row["cluster_id"]
        outcome = row["outcome"]
        outcome_dir = outcome_dir_map[(cluster_id, outcome)]
        bucket_dir = out_dir / cluster_id / outcome_dir
        ensure_dir(bucket_dir)
        dst = bucket_dir / src_png.name
        action_copy(src_png, dst, args.copy_mode)
        row["bucket_path"] = str(bucket_dir)

    unmatched_trade = []
    for key, trade in trades.items():
        if key not in matched_trade_keys:
            unmatched_trade.append({
                "match_time": key[0],
                "symbol": key[1],
                "reason_short": key[2],
                "signal_time": trade.get("signal_time"),
                "symbol_raw": trade.get("symbol"),
                "reason_raw": trade.get("reason"),
            })

    cluster_index_path = reports_dir / "cluster_index.csv"
    with cluster_index_path.open("w", newline="", encoding="utf-8") as f:
        if cluster_rows:
            writer = csv.DictWriter(f, fieldnames=list(cluster_rows[0].keys()))
            writer.writeheader()
            writer.writerows(cluster_rows)
        else:
            f.write("")

    summary_rows = []
    for cid, rows in sorted(cluster_acc.items()):
        pnls = [r["pnl_pct"] for r in rows]
        summary_rows.append({
            "cluster_id": cid,
            "count": len(rows),
            "profit_count": sum(1 for r in rows if r["outcome"] == "profit"),
            "loss_count": sum(1 for r in rows if r["outcome"] == "loss"),
            "flat_count": sum(1 for r in rows if r["outcome"] == "flat"),
            "profit_dir_name": outcome_dir_map.get((cid, "profit"), ""),
            "loss_dir_name": outcome_dir_map.get((cid, "loss"), ""),
            "flat_dir_name": outcome_dir_map.get((cid, "flat"), ""),
            "avg_pnl_pct": sum(pnls) / len(pnls) if pnls else "",
            "median_pnl_pct": median(pnls),
            "ab_bars_median": median([r["ab_bars"] for r in rows]),
            "bc_bars_median": median([r["bc_bars"] for r in rows]),
            "bc_ab_ratio_median": median([r["bc_ab_ratio"] for r in rows]),
            "drop_pct_median": median([r["drop_pct"] for r in rows]),
            "rebound_ratio_median": median([r["rebound_ratio"] for r in rows]),
            "drop_window_chg_median": median([r["drop_window_chg"] for r in rows]),
        })

    cluster_summary_path = reports_dir / "cluster_summary.csv"
    with cluster_summary_path.open("w", newline="", encoding="utf-8") as f:
        if summary_rows:
            writer = csv.DictWriter(f, fieldnames=list(summary_rows[0].keys()))
            writer.writeheader()
            writer.writerows(summary_rows)
        else:
            f.write("")

    unmatched_png_path = reports_dir / "unmatched_png.csv"
    with unmatched_png_path.open("w", newline="", encoding="utf-8") as f:
        if unmatched_png:
            writer = csv.DictWriter(f, fieldnames=list(unmatched_png[0].keys()))
            writer.writeheader()
            writer.writerows(unmatched_png)
        else:
            f.write("")

    unmatched_trade_path = reports_dir / "unmatched_trades.csv"
    with unmatched_trade_path.open("w", newline="", encoding="utf-8") as f:
        if unmatched_trade:
            writer = csv.DictWriter(f, fieldnames=list(unmatched_trade[0].keys()))
            writer.writeheader()
            writer.writerows(unmatched_trade)
        else:
            f.write("")

    summary = {
        "run_id": args.run_id,
        "png_dir": str(png_dir),
        "trades_jsonl": str(trades_path),
        "out_dir": str(out_dir),
        "matched_png": matched,
        "unmatched_png": len(unmatched_png),
        "unmatched_trade": len(unmatched_trade),
        "clusters": len(cluster_acc),
    }
    with (reports_dir / "summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("=== visual_audit_bucketizer 完成 ===")
    print(f"run_id         : {args.run_id}")
    print(f"png dir        : {png_dir}")
    print(f"trades jsonl   : {trades_path}")
    print(f"out dir        : {out_dir}")
    print(f"matched png    : {matched}")
    print(f"unmatched png  : {len(unmatched_png)}")
    print(f"unmatched trade: {len(unmatched_trade)}")
    print(f"clusters       : {len(cluster_acc)}")


if __name__ == "__main__":
    main()
