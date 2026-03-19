#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Snapback-ABC_BINDEX C前因子几何审计
基于 sim_trades.jsonl 中 context.a_time / context.b_time / context.c_time
计算 ab_bars / bc_bars / bc_ab_ratio，并回填到 detail / tagged 审计表。

规则：
- 只接受 1m bar 间隔（默认 60000 ms）
- fail-fast：
  * 缺少 context 或 a_time/b_time/c_time -> 报错退出
  * 时间顺序不满足 a < b < c -> 报错退出
  * 时间差不是整 bar_ms -> 报错退出
  * detail/tagged 中找不到匹配记录 -> 报错退出
"""
import argparse
import csv
import json
from pathlib import Path
from typing import Dict, Tuple, List

BAR_MS_DEFAULT = 60000

TradeKey = Tuple[str, int, int, int]  # (symbol, a_time, b_time, c_time)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--trades-jsonl", required=True)
    p.add_argument("--detail-csv", required=True)
    p.add_argument("--detail-out-csv", required=True)
    p.add_argument("--tagged-csv")
    p.add_argument("--tagged-out-csv")
    p.add_argument("--summary-json", required=True)
    p.add_argument("--bar-ms", type=int, default=BAR_MS_DEFAULT)
    return p.parse_args()


def require_columns(fieldnames, required: List[str], file_label: str):
    missing = [c for c in required if c not in fieldnames]
    if missing:
        raise SystemExit(f"{file_label} 缺少必需字段: {', '.join(missing)}")


def load_trade_geometry(trades_jsonl: Path, bar_ms: int) -> Dict[TradeKey, Dict[str, float]]:
    lookup: Dict[TradeKey, Dict[str, float]] = {}
    rows = 0
    with trades_jsonl.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            rows += 1
            try:
                obj = json.loads(line)
            except Exception as e:
                raise SystemExit(f"第 {line_no} 行 JSON 解析失败: {e}")

            symbol = obj.get("symbol")
            if not symbol:
                raise SystemExit(f"第 {line_no} 条 trade 缺少字段 symbol")

            ctx = obj.get("context")
            if not isinstance(ctx, dict):
                raise SystemExit(f"第 {line_no} 条 trade 缺少字段 context 或类型错误")

            for key in ("a_time", "b_time", "c_time"):
                if key not in ctx:
                    raise SystemExit(f"第 {line_no} 条 trade 的 context 缺少字段 {key}")

            a_time = ctx["a_time"]
            b_time = ctx["b_time"]
            c_time = ctx["c_time"]

            if not all(isinstance(v, int) for v in (a_time, b_time, c_time)):
                raise SystemExit(f"第 {line_no} 条 trade 的 a_time/b_time/c_time 不是 int 毫秒时间戳")

            if not (a_time < b_time < c_time):
                raise SystemExit(
                    f"第 {line_no} 条 trade 时间顺序错误: a_time={a_time}, b_time={b_time}, c_time={c_time}"
                )

            ab_delta = b_time - a_time
            bc_delta = c_time - b_time
            if ab_delta % bar_ms != 0 or bc_delta % bar_ms != 0:
                raise SystemExit(
                    f"第 {line_no} 条 trade 时间差不是整 bar_ms: "
                    f"ab_delta={ab_delta}, bc_delta={bc_delta}, bar_ms={bar_ms}"
                )

            ab_bars = ab_delta // bar_ms
            bc_bars = bc_delta // bar_ms
            if ab_bars <= 0 or bc_bars <= 0:
                raise SystemExit(
                    f"第 {line_no} 条 trade bars 非正数: ab_bars={ab_bars}, bc_bars={bc_bars}"
                )

            key = (symbol, a_time, b_time, c_time)
            if key in lookup:
                raise SystemExit(
                    f"发现重复 trade key: symbol={symbol}, a_time={a_time}, b_time={b_time}, c_time={c_time}"
                )

            lookup[key] = {
                "ab_bars": ab_bars,
                "bc_bars": bc_bars,
                "bc_ab_ratio": bc_bars / ab_bars,
            }

    if rows == 0:
        raise SystemExit("trades jsonl 为空。")
    return lookup


def enrich_csv(
    input_csv: Path,
    output_csv: Path,
    lookup: Dict[TradeKey, Dict[str, float]],
    file_label: str,
) -> Dict[str, int]:
    with input_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise SystemExit(f"{file_label} 为空或表头缺失。")
        required = ["symbol", "a_time", "b_time", "c_time"]
        require_columns(reader.fieldnames, required, file_label)

        fieldnames = list(reader.fieldnames)
        for new_col in ("ab_bars", "bc_bars", "bc_ab_ratio"):
            if new_col not in fieldnames:
                fieldnames.append(new_col)

        rows = list(reader)

    matched = 0
    for idx, row in enumerate(rows, start=1):
        try:
            key = (
                row["symbol"],
                int(row["a_time"]),
                int(row["b_time"]),
                int(row["c_time"]),
            )
        except Exception as e:
            raise SystemExit(f"{file_label} 第 {idx} 行无法解析 symbol/a_time/b_time/c_time: {e}")

        geom = lookup.get(key)
        if geom is None:
            raise SystemExit(
                f"{file_label} 第 {idx} 行未在 trades lookup 中找到匹配: "
                f"symbol={row['symbol']}, a_time={row['a_time']}, b_time={row['b_time']}, c_time={row['c_time']}"
            )

        row["ab_bars"] = str(int(geom["ab_bars"]))
        row["bc_bars"] = str(int(geom["bc_bars"]))
        row["bc_ab_ratio"] = f"{geom['bc_ab_ratio']:.12f}"
        matched += 1

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return {"rows": len(rows), "matched": matched}


def main():
    args = parse_args()
    trades_jsonl = Path(args.trades_jsonl)
    detail_csv = Path(args.detail_csv)
    detail_out_csv = Path(args.detail_out_csv)
    tagged_csv = Path(args.tagged_csv) if args.tagged_csv else None
    tagged_out_csv = Path(args.tagged_out_csv) if args.tagged_out_csv else None
    summary_json = Path(args.summary_json)

    if bool(tagged_csv) != bool(tagged_out_csv):
        raise SystemExit("--tagged-csv 与 --tagged-out-csv 必须同时提供或同时省略。")

    lookup = load_trade_geometry(trades_jsonl, args.bar_ms)
    detail_stats = enrich_csv(detail_csv, detail_out_csv, lookup, "detail csv")

    tagged_stats = None
    if tagged_csv and tagged_out_csv:
        tagged_stats = enrich_csv(tagged_csv, tagged_out_csv, lookup, "tagged csv")

    summary = {
        "task_name": "Snapback-ABC_BINDEX C前因子几何审计",
        "trades_jsonl": str(trades_jsonl),
        "detail_csv": str(detail_csv),
        "detail_out_csv": str(detail_out_csv),
        "tagged_csv": str(tagged_csv) if tagged_csv else "",
        "tagged_out_csv": str(tagged_out_csv) if tagged_out_csv else "",
        "bar_ms": args.bar_ms,
        "trade_lookup_rows": len(lookup),
        "detail_rows": detail_stats["rows"],
        "detail_matched_rows": detail_stats["matched"],
        "tagged_rows": tagged_stats["rows"] if tagged_stats else 0,
        "tagged_matched_rows": tagged_stats["matched"] if tagged_stats else 0,
    }

    summary_json.parent.mkdir(parents=True, exist_ok=True)
    summary_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=== enrich_geometry_from_times 完成 ===")
    print(f"trades jsonl  : {trades_jsonl}")
    print(f"detail out csv: {detail_out_csv}")
    if tagged_out_csv:
        print(f"tagged out csv: {tagged_out_csv}")
    print(f"summary json  : {summary_json}")
    print(f"trade lookup  : {len(lookup)}")
    print(f"detail rows   : {detail_stats['rows']}")
    if tagged_stats:
        print(f"tagged rows   : {tagged_stats['rows']}")


if __name__ == "__main__":
    main()
