#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
统计 sim_trades 中 A 点与 S 点重合的笔数。
默认只需要 --run-id，自动推导 input/output 路径。

判定规则：
1) 优先比较 context.a_time 与 context.s_time
2) 若毫秒/秒/字符串混用，统一规范到 epoch ms 再比较
3) 同时输出：
   - exact_same_count: A_time == S_time
   - same_bar_count:   A/S 落在同一根 1m bar（对毫秒取 floor 到分钟）
"""

import argparse
import csv
import json
import os
import statistics
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _first_not_none(*vals):
    for v in vals:
        if v is not None and v != "":
            return v
    return None


def _to_epoch_ms(value: Any) -> Optional[int]:
    if value is None or value == "":
        return None

    if isinstance(value, (int, float)):
        iv = int(value)
        # 13位视为毫秒，10位左右视为秒
        if abs(iv) >= 10**12:
            return iv
        if abs(iv) >= 10**9:
            return iv * 1000
        return None

    s = str(value).strip()
    if not s:
        return None

    # 纯数字字符串
    if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
        iv = int(s)
        if abs(iv) >= 10**12:
            return iv
        if abs(iv) >= 10**9:
            return iv * 1000
        return None

    # ISO 时间
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def _minute_floor_ms(ms: Optional[int]) -> Optional[int]:
    if ms is None:
        return None
    return (ms // 60000) * 60000


def load_rows(jsonl_path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception as e:
                raise RuntimeError(f"JSONL 解析失败 line={line_no}: {e}") from e

            ctx = obj.get("context") if isinstance(obj.get("context"), dict) else {}

            a_raw = _first_not_none(
                ctx.get("a_time_ms"), ctx.get("a_ts_ms"), ctx.get("a_time"), ctx.get("a_ts")
            )
            s_raw = _first_not_none(
                ctx.get("s_time_ms"), ctx.get("s_ts_ms"), ctx.get("s_time"), ctx.get("s_ts")
            )

            a_ms = _to_epoch_ms(a_raw)
            s_ms = _to_epoch_ms(s_raw)

            row = {
                "symbol": _first_not_none(obj.get("symbol"), ctx.get("symbol"), ""),
                "signal_time": _first_not_none(obj.get("signal_time"), ctx.get("signal_time"), ""),
                "a_time_raw": a_raw,
                "s_time_raw": s_raw,
                "a_time_ms": a_ms,
                "s_time_ms": s_ms,
                "exact_same": (a_ms is not None and s_ms is not None and a_ms == s_ms),
                "same_bar": (
                    a_ms is not None
                    and s_ms is not None
                    and _minute_floor_ms(a_ms) == _minute_floor_ms(s_ms)
                ),
                "ab_bars": _first_not_none(obj.get("ab_bars"), ctx.get("ab_bars")),
                "bc_bars": _first_not_none(obj.get("bc_bars"), ctx.get("bc_bars")),
                "rebound_ratio": _first_not_none(obj.get("rebound_ratio"), ctx.get("rebound_ratio")),
                "pnl_pct": obj.get("pnl_pct"),
                "reason": _first_not_none(obj.get("reason"), obj.get("exit_reason"), ""),
            }
            rows.append(row)
    return rows


def write_csv(path: str, fieldnames: List[str], rows: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow(row)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True, help="运行标识；未显式传路径时，会按 run-id 自动推导默认路径")
    ap.add_argument("--trades-jsonl", required=False, help="sim_trades JSONL；默认 output/state/sim_trades.{run_id}.jsonl")
    ap.add_argument("--out-dir", required=False, help="输出目录；默认 output/state/as_overlap_audit.{run_id}")
    args = ap.parse_args()

    if not args.trades_jsonl:
        args.trades_jsonl = os.path.join("output", "state", f"sim_trades.{args.run_id}.jsonl")
    if not args.out_dir:
        args.out_dir = os.path.join("output", "state", f"as_overlap_audit.{args.run_id}")

    if not os.path.exists(args.trades_jsonl):
        raise RuntimeError(f"sim_trades 文件不存在: {args.trades_jsonl}")
    os.makedirs(args.out_dir, exist_ok=True)

    rows = load_rows(args.trades_jsonl)
    if not rows:
        raise RuntimeError("未读取到任何 trade")

    exact_rows = [r for r in rows if r["exact_same"]]
    same_bar_rows = [r for r in rows if r["same_bar"]]

    summary = {
        "run_id": args.run_id,
        "trades_jsonl": args.trades_jsonl,
        "total_trades": len(rows),
        "valid_a_s_both_present": sum(1 for r in rows if r["a_time_ms"] is not None and r["s_time_ms"] is not None),
        "exact_same_count": len(exact_rows),
        "exact_same_ratio": (len(exact_rows) / len(rows)) if rows else None,
        "same_bar_count": len(same_bar_rows),
        "same_bar_ratio": (len(same_bar_rows) / len(rows)) if rows else None,
    }

    write_csv(
        os.path.join(args.out_dir, "a_s_exact_same_samples.csv"),
        [
            "symbol", "signal_time", "a_time_raw", "s_time_raw", "a_time_ms", "s_time_ms",
            "ab_bars", "bc_bars", "rebound_ratio", "pnl_pct", "reason"
        ],
        exact_rows,
    )
    write_csv(
        os.path.join(args.out_dir, "a_s_same_bar_samples.csv"),
        [
            "symbol", "signal_time", "a_time_raw", "s_time_raw", "a_time_ms", "s_time_ms",
            "ab_bars", "bc_bars", "rebound_ratio", "pnl_pct", "reason"
        ],
        same_bar_rows,
    )
    with open(os.path.join(args.out_dir, "a_s_overlap_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("=== A/S overlap audit 完成 ===")
    for k, v in summary.items():
        print(f"{k:24s}: {v}")
    print("written                 : a_s_exact_same_samples.csv, a_s_same_bar_samples.csv, a_s_overlap_summary.json")


if __name__ == "__main__":
    main()
