#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Snapback-ABC_BINDEX C前因子几何审计
从 sim_trades.jsonl 直接导出 geometry audit source csv。

只基于 trade 顶层字段 + trade["context"] 字段生成分析源表，
避免依赖已丢失关键键的下游 audit/detail csv。

fail-fast:
- 记录缺少 context / a_time / b_time / c_time 直接报错
- 时间顺序必须满足 a_time <= b_time < c_time
- 时间差必须是整 1m bar
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

BAR_MS = 60_000


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                raise SystemExit(f"{path} 第 {lineno} 行 JSON 解析失败: {e}")
            if not isinstance(obj, dict):
                raise SystemExit(f"{path} 第 {lineno} 行不是 JSON object。")
            rows.append(obj)
    if not rows:
        raise SystemExit(f"{path} 为空，无法导出几何审计源表。")
    return rows


def require_int(obj: Dict[str, Any], key: str, where: str) -> int:
    if key not in obj:
        raise SystemExit(f"{where} 缺少字段 {key}")
    val = obj[key]
    if not isinstance(val, int):
        raise SystemExit(f"{where} 字段 {key} 不是 int: {val!r}")
    return val


def require_any(obj: Dict[str, Any], key: str, where: str) -> Any:
    if key not in obj:
        raise SystemExit(f"{where} 缺少字段 {key}")
    return obj[key]


def maybe_get(obj: Dict[str, Any], key: str) -> Any:
    return obj.get(key, "")


def compute_bars(a_time: int, b_time: int, c_time: int, where: str) -> tuple[int, int, Any]:
    if not (a_time <= b_time < c_time):
        raise SystemExit(
            f"{where} 时间顺序错误: a_time={a_time}, b_time={b_time}, c_time={c_time}"
        )
    ab_ms = b_time - a_time
    bc_ms = c_time - b_time
    if ab_ms % BAR_MS != 0 or bc_ms % BAR_MS != 0:
        raise SystemExit(
            f"{where} 不是整 1m bar 间隔: "
            f"ab_ms={ab_ms}, bc_ms={bc_ms}, BAR_MS={BAR_MS}"
        )
    ab_bars = ab_ms // BAR_MS
    bc_bars = bc_ms // BAR_MS
    bc_ab_ratio = "" if ab_bars == 0 else (bc_bars / ab_bars)
    return ab_bars, bc_bars, bc_ab_ratio


def build_row(trade: Dict[str, Any], idx: int) -> Dict[str, Any]:
    where = f"第 {idx} 条 trade"
    context = trade.get("context")
    if not isinstance(context, dict):
        raise SystemExit(f"{where} 缺少字段 context 或 context 不是 object。")

    a_time = require_int(context, "a_time", where)
    b_time = require_int(context, "b_time", where)
    c_time = require_int(context, "c_time", where)
    ab_bars, bc_bars, bc_ab_ratio = compute_bars(a_time, b_time, c_time, where)

    row: Dict[str, Any] = {
        # 顶层主键 / 时间
        "symbol": require_any(trade, "symbol", where),
        "signal_time": require_any(trade, "signal_time", where),
        "entry_time": require_any(trade, "entry_time", where),
        "exit_time": require_any(trade, "exit_time", where),
        "signal_time_bj": maybe_get(trade, "signal_time_bj"),
        "entry_time_bj": maybe_get(trade, "entry_time_bj"),
        "exit_time_bj": maybe_get(trade, "exit_time_bj"),
        # 顶层交易结果
        "signal_price": maybe_get(trade, "signal_price"),
        "entry_price": maybe_get(trade, "entry_price"),
        "exit_price": maybe_get(trade, "exit_price"),
        "pnl_pct": require_any(trade, "pnl_pct", where),
        "reason": require_any(trade, "reason", where),
        # context 环境/结构字段
        "chg_24h": require_any(context, "chg_24h", where),
        "drop_window_chg": require_any(context, "drop_window_chg", where),
        "drop_pct": require_any(context, "drop_pct", where),
        "rebound_ratio": require_any(context, "rebound_ratio", where),
        "vol_ratio": maybe_get(context, "vol_ratio"),
        "vol_24h": maybe_get(context, "vol_24h"),
        "recent_high_price": maybe_get(context, "recent_high_price"),
        "trigger_name": maybe_get(context, "trigger_name"),
        "selected_tp_pct": maybe_get(context, "selected_tp_pct"),
        "tp_tier": maybe_get(context, "tp_tier"),
        # 几何时间锚点
        "s_time": maybe_get(context, "s_time"),
        "a_time": a_time,
        "b_time": b_time,
        "c_time": c_time,
        # 价格锚点
        "s_close": maybe_get(context, "s_close"),
        "a_high_price": maybe_get(context, "a_high_price"),
        "b_contract_price": maybe_get(context, "b_contract_price"),
        "b_index_price": maybe_get(context, "b_index_price"),
        "c_price": maybe_get(context, "c_price"),
        # 新增几何字段
        "ab_bars": ab_bars,
        "bc_bars": bc_bars,
        "bc_ab_ratio": bc_ab_ratio,
    }
    return row


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        raise SystemExit("没有可写入的行。")
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_summary(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="从 sim_trades.jsonl 导出 geometry audit source csv")
    p.add_argument("--trades-jsonl", required=True, help="sim_trades jsonl 路径")
    p.add_argument("--run-id", default="", help="run_id")
    p.add_argument("--out-csv", required=True, help="输出 geometry_audit_source.csv")
    p.add_argument("--summary-json", required=True, help="输出 summary json")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    trades_path = Path(args.trades_jsonl)
    out_csv = Path(args.out_csv)
    summary_json = Path(args.summary_json)

    trades = load_jsonl(trades_path)
    rows = [build_row(trade, idx + 1) for idx, trade in enumerate(trades)]
    write_csv(out_csv, rows)

    ab_zero_count = sum(1 for r in rows if r["ab_bars"] == 0)
    summary = {
        "run_id": args.run_id,
        "trades_jsonl": str(trades_path),
        "out_csv": str(out_csv),
        "rows": len(rows),
        "ab_zero_count": ab_zero_count,
        "fields": list(rows[0].keys()),
    }
    write_summary(summary_json, summary)

    print("=== export_geometry_audit_source 完成 ===")
    print(f"run_id       : {args.run_id}")
    print(f"trades jsonl : {trades_path}")
    print(f"out csv      : {out_csv}")
    print(f"summary json : {summary_json}")
    print(f"rows         : {len(rows)}")
    print(f"ab_zero      : {ab_zero_count}")


if __name__ == "__main__":
    main()
