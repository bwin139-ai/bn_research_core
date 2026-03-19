#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Snapback-ABC_BINDEX C前因子几何审计
基于 sim_trades 中已有的 a_time / b_time / c_time 计算：
- ab_bars
- bc_bars
- bc_ab_ratio
并回填到审计明细表（detail csv）和/或样本标签表（tagged csv）。

设计原则：
- 只基于 C 点之前已存在的时间锚点
- fail-fast：时间缺失、顺序错误、非整 bar 间隔，直接报错退出
- 不修改原文件；输出到新文件
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
from typing import Dict, List, Optional, Tuple

BAR_MS_DEFAULT = 60_000


def load_jsonl(path: str) -> List[dict]:
    rows: List[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValueError(f"JSONL 解析失败: {path}:{lineno}: {e}") from e
    if not rows:
        raise ValueError(f"输入 trades jsonl 为空: {path}")
    return rows


def read_csv(path: str) -> Tuple[List[str], List[dict]]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        if not fieldnames:
            raise ValueError(f"CSV 缺少表头: {path}")
        rows = list(reader)
    if not rows:
        raise ValueError(f"输入 csv 为空: {path}")
    return fieldnames, rows


def ensure_parent(path: str) -> None:
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)


def must_int(value, field_name: str, idx: int) -> int:
    if value is None or value == "":
        raise ValueError(f"第 {idx} 条 trade 缺少字段 {field_name}")
    try:
        return int(value)
    except Exception as e:
        raise ValueError(f"第 {idx} 条 trade 字段 {field_name} 不是有效整数: {value}") from e


def compute_geometry(a_time: int, b_time: int, c_time: int, bar_ms: int) -> Tuple[int, int, float]:
    if not (a_time < b_time < c_time):
        raise ValueError(
            f"时间顺序非法: a_time={a_time}, b_time={b_time}, c_time={c_time}，要求 a_time < b_time < c_time"
        )
    ab_delta = b_time - a_time
    bc_delta = c_time - b_time
    if ab_delta % bar_ms != 0:
        raise ValueError(f"ab 时间差不是整 bar: ab_delta={ab_delta}, bar_ms={bar_ms}")
    if bc_delta % bar_ms != 0:
        raise ValueError(f"bc 时间差不是整 bar: bc_delta={bc_delta}, bar_ms={bar_ms}")

    ab_bars = ab_delta // bar_ms
    bc_bars = bc_delta // bar_ms
    if ab_bars <= 0:
        raise ValueError(f"ab_bars 非法: {ab_bars}")
    if bc_bars <= 0:
        raise ValueError(f"bc_bars 非法: {bc_bars}")

    bc_ab_ratio = bc_bars / ab_bars
    return ab_bars, bc_bars, bc_ab_ratio


def build_trade_geometry_map(trades: List[dict], bar_ms: int) -> Dict[Tuple[str, str, str], Tuple[int, int, float]]:
    out: Dict[Tuple[str, str, str], Tuple[int, int, float]] = {}

    for idx, trade in enumerate(trades, 1):
        a_time = must_int(trade.get("a_time"), "a_time", idx)
        b_time = must_int(trade.get("b_time"), "b_time", idx)
        c_time = must_int(trade.get("c_time"), "c_time", idx)

        ab_bars, bc_bars, bc_ab_ratio = compute_geometry(a_time, b_time, c_time, bar_ms)

        key = (str(a_time), str(b_time), str(c_time))
        if key in out:
            prev = out[key]
            curr = (ab_bars, bc_bars, bc_ab_ratio)
            if prev != curr:
                raise ValueError(
                    f"重复时间键对应不同几何结果: key={key}, prev={prev}, curr={curr}"
                )
        out[key] = (ab_bars, bc_bars, bc_ab_ratio)

    if not out:
        raise ValueError("未构建出任何几何映射。")

    return out


def enrich_rows(
    rows: List[dict],
    fieldnames: List[str],
    trade_geom: Dict[Tuple[str, str, str], Tuple[int, int, float]],
    csv_name: str,
) -> Tuple[List[str], List[dict], dict]:
    required = ["a_time", "b_time", "c_time"]
    for col in required:
        if col not in fieldnames:
            raise ValueError(f"{csv_name} 缺少必需字段: {col}")

    new_fields = list(fieldnames)
    for col in ["ab_bars", "bc_bars", "bc_ab_ratio"]:
        if col not in new_fields:
            new_fields.append(col)

    matched = 0
    unmatched = 0
    out_rows: List[dict] = []

    for i, row in enumerate(rows, 1):
        key = (str(row.get("a_time", "")).strip(), str(row.get("b_time", "")).strip(), str(row.get("c_time", "")).strip())
        if "" in key:
            raise ValueError(f"{csv_name} 第 {i} 行缺少 a_time/b_time/c_time，无法匹配几何字段")

        if key not in trade_geom:
            unmatched += 1
            raise ValueError(f"{csv_name} 第 {i} 行无法在 trades jsonl 中匹配几何字段: key={key}")

        ab_bars, bc_bars, bc_ab_ratio = trade_geom[key]
        new_row = dict(row)
        new_row["ab_bars"] = str(ab_bars)
        new_row["bc_bars"] = str(bc_bars)
        new_row["bc_ab_ratio"] = f"{bc_ab_ratio:.12f}"
        out_rows.append(new_row)
        matched += 1

    summary = {
        "rows": len(rows),
        "matched_rows": matched,
        "unmatched_rows": unmatched,
    }
    return new_fields, out_rows, summary


def write_csv(path: str, fieldnames: List[str], rows: List[dict]) -> None:
    ensure_parent(path)
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_json(path: str, payload: dict) -> None:
    ensure_parent(path)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="基于 a_time/b_time/c_time 回填 ab_bars/bc_bars/bc_ab_ratio")
    p.add_argument("--trades-jsonl", required=True, help="sim_trades.<RUNID>.jsonl")
    p.add_argument("--detail-csv", required=True, help="审计明细 csv（输入）")
    p.add_argument("--detail-out-csv", required=True, help="审计明细 csv（输出，新增几何字段）")
    p.add_argument("--tagged-csv", help="样本标签 csv（输入，可选）")
    p.add_argument("--tagged-out-csv", help="样本标签 csv（输出，可选）")
    p.add_argument("--summary-json", required=True, help="摘要 json 输出")
    p.add_argument("--bar-ms", type=int, default=BAR_MS_DEFAULT, help="bar 时长毫秒数，默认 60000（1m）")
    return p.parse_args()


def main() -> int:
    args = parse_args()

    if bool(args.tagged_csv) != bool(args.tagged_out_csv):
        raise ValueError("tagged-csv 与 tagged-out-csv 必须同时提供，或同时不提供。")

    trades = load_jsonl(args.trades_jsonl)
    trade_geom = build_trade_geometry_map(trades, args.bar_ms)

    detail_fields, detail_rows = read_csv(args.detail_csv)
    detail_new_fields, detail_out_rows, detail_summary = enrich_rows(
        detail_rows, detail_fields, trade_geom, "detail_csv"
    )
    write_csv(args.detail_out_csv, detail_new_fields, detail_out_rows)

    tagged_summary: Optional[dict] = None
    if args.tagged_csv:
        tagged_fields, tagged_rows = read_csv(args.tagged_csv)
        tagged_new_fields, tagged_out_rows, tagged_summary = enrich_rows(
            tagged_rows, tagged_fields, trade_geom, "tagged_csv"
        )
        write_csv(args.tagged_out_csv, tagged_new_fields, tagged_out_rows)

    summary = {
        "trades_jsonl": args.trades_jsonl,
        "detail_csv": args.detail_csv,
        "detail_out_csv": args.detail_out_csv,
        "tagged_csv": args.tagged_csv or "",
        "tagged_out_csv": args.tagged_out_csv or "",
        "bar_ms": args.bar_ms,
        "trade_rows": len(trades),
        "trade_geometry_keys": len(trade_geom),
        "detail": detail_summary,
        "tagged": tagged_summary,
    }
    write_json(args.summary_json, summary)

    print("=== enrich_geometry_from_times 完成 ===")
    print(f"trades jsonl  : {args.trades_jsonl}")
    print(f"detail out csv: {args.detail_out_csv}")
    if args.tagged_out_csv:
        print(f"tagged out csv: {args.tagged_out_csv}")
    print(f"summary json  : {args.summary_json}")
    print(f"trade rows    : {len(trades)}")
    print(f"detail rows   : {detail_summary['rows']}")
    if tagged_summary:
        print(f"tagged rows   : {tagged_summary['rows']}")
    print(f"bar_ms        : {args.bar_ms}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(str(e), file=sys.stderr)
        raise SystemExit(1)
