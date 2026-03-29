#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Snapback-ABC_BINDEX C前因子几何审计
基于 sim_trades.jsonl 中 context.a_time / b_time / c_time
回填 ab_bars / bc_bars / bc_ab_ratio 到 detail/tagged csv。

口径（基于当前已确认事实）：
- 允许 a_time <= b_time < c_time
- bar_ms 默认 60000（1m）
- ab_bars = (b_time - a_time) / bar_ms
- bc_bars = (c_time - b_time) / bar_ms
- ab_bars == 0 时，bc_ab_ratio 置空
- 时间差若不是整 bar，直接 fail-fast
- 若 detail/tagged 中找不到对应 (a_time,b_time,c_time) 记录，直接 fail-fast
"""
from __future__ import annotations

import argparse
import csv
import json
import os
from typing import Dict, List, Tuple, Any

TIME_KEY = ("a_time", "b_time", "c_time")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Enrich geometry fields from context times in sim_trades jsonl.")
    p.add_argument("--trades-jsonl", required=True)
    p.add_argument("--detail-csv", required=True)
    p.add_argument("--detail-out-csv", required=True)
    p.add_argument("--tagged-csv", required=False, default="")
    p.add_argument("--tagged-out-csv", required=False, default="")
    p.add_argument("--summary-json", required=True)
    p.add_argument("--bar-ms", type=int, default=60000)
    return p.parse_args()


def ensure_parent(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def read_jsonl(path: str) -> List[dict]:
    rows: List[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise SystemExit(f"第 {i} 行 jsonl 解析失败: {e}")
    if not rows:
        raise SystemExit("trades jsonl 为空。")
    return rows


def compute_geometry_from_trade(trade: dict, idx: int, bar_ms: int) -> Tuple[Tuple[int, int, int], int, int, Any]:
    ctx = trade.get("context")
    if not isinstance(ctx, dict):
        raise SystemExit(f"第 {idx} 条 trade 缺少 context。")

    missing = [k for k in TIME_KEY if k not in ctx]
    if missing:
        raise SystemExit(f"第 {idx} 条 trade context 缺少字段: {', '.join(missing)}")

    a_time = ctx["a_time"]
    b_time = ctx["b_time"]
    c_time = ctx["c_time"]

    if not all(isinstance(v, int) for v in (a_time, b_time, c_time)):
        raise SystemExit(f"第 {idx} 条 trade 时间字段不是 int: a_time={a_time}, b_time={b_time}, c_time={c_time}")

    # 基于当前已确认事实：允许 a_time == b_time，但要求 b_time < c_time
    if not (a_time <= b_time < c_time):
        raise SystemExit(
            f"第 {idx} 条 trade 时间顺序错误: a_time={a_time}, b_time={b_time}, c_time={c_time}"
        )

    ab_delta = b_time - a_time
    bc_delta = c_time - b_time

    if ab_delta % bar_ms != 0 or bc_delta % bar_ms != 0:
        raise SystemExit(
            f"第 {idx} 条 trade 时间差不是整 bar: "
            f"ab_delta={ab_delta}, bc_delta={bc_delta}, bar_ms={bar_ms}"
        )

    ab_bars = ab_delta // bar_ms
    bc_bars = bc_delta // bar_ms
    bc_ab_ratio = "" if ab_bars == 0 else (bc_bars / ab_bars)

    return (a_time, b_time, c_time), ab_bars, bc_bars, bc_ab_ratio


def build_lookup(trades: List[dict], bar_ms: int) -> Dict[Tuple[int, int, int], Tuple[int, int, Any]]:
    lookup: Dict[Tuple[int, int, int], Tuple[int, int, Any]] = {}
    for idx, trade in enumerate(trades, start=1):
        key, ab_bars, bc_bars, ratio = compute_geometry_from_trade(trade, idx, bar_ms)
        if key in lookup:
            raise SystemExit(f"发现重复时间键 (a_time,b_time,c_time): {key}")
        lookup[key] = (ab_bars, bc_bars, ratio)
    return lookup


def read_csv(path: str) -> Tuple[List[dict], List[str]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise SystemExit(f"csv 无表头: {path}")
        rows = list(reader)
        return rows, list(reader.fieldnames)


def parse_int_field(row: dict, field: str, path: str, row_idx: int) -> int:
    if field not in row or row[field] == "":
        raise SystemExit(f"{os.path.basename(path)} 第 {row_idx} 行缺少字段 {field}")
    try:
        return int(float(row[field]))
    except ValueError:
        raise SystemExit(f"{os.path.basename(path)} 第 {row_idx} 行字段 {field} 不能转为 int: {row[field]}")


def enrich_rows(rows: List[dict], path: str, lookup: Dict[Tuple[int, int, int], Tuple[int, int, Any]]) -> int:
    matched = 0
    for i, row in enumerate(rows, start=2):  # header = row 1
        for field in TIME_KEY:
            if field not in row:
                raise SystemExit(f"{os.path.basename(path)} 缺少字段 {field}")
        key = tuple(parse_int_field(row, field, path, i) for field in TIME_KEY)
        if key not in lookup:
            raise SystemExit(
                f"{os.path.basename(path)} 第 {i} 行找不到对应时间键: "
                f"a_time={key[0]}, b_time={key[1]}, c_time={key[2]}"
            )
        ab_bars, bc_bars, ratio = lookup[key]
        row["ab_bars"] = str(ab_bars)
        row["bc_bars"] = str(bc_bars)
        row["bc_ab_ratio"] = "" if ratio == "" else repr(ratio)
        matched += 1
    return matched


def write_csv(path: str, rows: List[dict], original_fieldnames: List[str]) -> None:
    fieldnames = list(original_fieldnames)
    for f in ["ab_bars", "bc_bars", "bc_ab_ratio"]:
        if f not in fieldnames:
            fieldnames.append(f)

    ensure_parent(path)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    args = parse_args()
    trades = read_jsonl(args.trades_jsonl)
    lookup = build_lookup(trades, args.bar_ms)

    detail_rows, detail_fields = read_csv(args.detail_csv)
    detail_matched = enrich_rows(detail_rows, args.detail_csv, lookup)
    write_csv(args.detail_out_csv, detail_rows, detail_fields)

    tagged_rows_count = 0
    if args.tagged_csv or args.tagged_out_csv:
        if not (args.tagged_csv and args.tagged_out_csv):
            raise SystemExit("tagged-csv 与 tagged-out-csv 必须同时提供，或同时不提供。")
        tagged_rows, tagged_fields = read_csv(args.tagged_csv)
        tagged_rows_count = enrich_rows(tagged_rows, args.tagged_csv, lookup)
        write_csv(args.tagged_out_csv, tagged_rows, tagged_fields)

    summary = {
        "trades_jsonl": args.trades_jsonl,
        "detail_csv": args.detail_csv,
        "detail_out_csv": args.detail_out_csv,
        "tagged_csv": args.tagged_csv,
        "tagged_out_csv": args.tagged_out_csv,
        "summary_json": args.summary_json,
        "bar_ms": args.bar_ms,
        "trade_rows": len(trades),
        "lookup_rows": len(lookup),
        "detail_rows": len(detail_rows),
        "detail_matched_rows": detail_matched,
        "tagged_rows": tagged_rows_count if args.tagged_csv else 0,
        "formula": {
            "ab_bars": "(b_time - a_time) / bar_ms",
            "bc_bars": "(c_time - b_time) / bar_ms",
            "bc_ab_ratio": "bc_bars / ab_bars, empty when ab_bars == 0",
        },
        "time_order_rule": "a_time <= b_time < c_time",
    }

    ensure_parent(args.summary_json)
    with open(args.summary_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("=== enrich_geometry_from_times_v3 完成 ===")
    print(f"trades jsonl  : {args.trades_jsonl}")
    print(f"detail out csv: {args.detail_out_csv}")
    if args.tagged_out_csv:
        print(f"tagged out csv: {args.tagged_out_csv}")
    print(f"summary json  : {args.summary_json}")
    print(f"trade rows    : {len(trades)}")
    print(f"detail rows   : {len(detail_rows)}")
    if args.tagged_csv:
        print(f"tagged rows   : {tagged_rows_count}")


if __name__ == "__main__":
    main()
