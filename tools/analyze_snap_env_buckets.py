#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
对 Snapback trades 做环境分桶审计：
1) 按 24hChg 分桶
2) 按 VolR(vol_ratio) 分桶

输出每个桶的：
- trades
- win_rate
- avg_pnl_pct
- median_pnl_pct
- sl_rate
- ts_rate
- tp_rate
- avg_drop_pct
- avg_rebound_ratio
- avg_volr

用法示例：
python tools/analyze_snap_env_buckets.py \
  --trades output/state/sim_trades.SNAP_V4.1_30D_P6_0316T0953_ALL.jsonl --label 0953 \
  --trades output/state/sim_trades.SNAP_V4.1_30D_P6_0316T1536_ALL.jsonl --label 1536 \
  --trades output/state/sim_trades.SNAP_V4.1_30D_P6_0316T1715_ALL.jsonl --label 1715
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
        if math.isnan(x) or math.isinf(x):
            return None
        return x
    except Exception:
        return None


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                rows.append(json.loads(s))
            except Exception as e:
                raise RuntimeError(f"解析失败: {path} 第 {line_no} 行: {e}") from e
    return rows


def _ctx_get(ctx: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in ctx and ctx[k] is not None:
            return ctx[k]
    return None


def _bucket_24h_chg(v: Optional[float]) -> str:
    if v is None:
        return "NA"
    if v < -20:
        return "<-20"
    if v < 0:
        return "[-20,0)"
    if v < 20:
        return "[0,20)"
    return ">=20"


def _bucket_volr(v: Optional[float]) -> str:
    if v is None:
        return "NA"
    if v < 8:
        return "<8"
    if v < 10:
        return "[8,10)"
    if v < 12:
        return "[10,12)"
    if v < 15:
        return "[12,15)"
    return ">=15"


def _reason_bucket(reason: str) -> str:
    reason = (reason or "").upper()
    if reason == "STOP_LOSS":
        return "SL"
    if reason == "TIME_STOP":
        return "TS"
    if reason == "TAKE_PROFIT":
        return "TP"
    if reason == "BREAKEVEN_STOP":
        return "BS"
    return "OTHER"


def _median_or_na(xs: List[float]) -> Optional[float]:
    if not xs:
        return None
    return statistics.median(xs)


def _mean_or_na(xs: List[float]) -> Optional[float]:
    if not xs:
        return None
    return sum(xs) / len(xs)


def _fmt_pct(v: Optional[float]) -> str:
    if v is None:
        return "NA"
    return f"{v:.2f}"


def _fmt_num(v: Optional[float]) -> str:
    if v is None:
        return "NA"
    return f"{v:.2f}"


def _extract_trade_row(tr: Dict[str, Any]) -> Dict[str, Any]:
    ctx = tr.get("context") or {}

    pnl_pct = _safe_float(
        tr.get("pnl_pct", tr.get("profit_pct", tr.get("realized_pnl_pct")))
    )

    chg_24h = _safe_float(
        _ctx_get(ctx, "chg_24h_pct", "change_24h_pct", "24hChg", "day_change_pct")
    )
    volr = _safe_float(
        _ctx_get(ctx, "vol_ratio", "volr", "VolR", "volume_ratio")
    )
    drop_pct = _safe_float(
        _ctx_get(ctx, "drop_pct", "dropPct")
    )
    rebound_ratio = _safe_float(
        _ctx_get(ctx, "rebound_ratio", "reboundRatio")
    )

    return {
        "reason": _reason_bucket(str(tr.get("reason", ""))),
        "pnl_pct": pnl_pct,
        "chg_24h": chg_24h,
        "volr": volr,
        "drop_pct": drop_pct,
        "rebound_ratio": rebound_ratio,
    }


def _build_bucket_stats(rows: Iterable[Dict[str, Any]], bucket_key: str) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        bucket = row[bucket_key]
        grouped.setdefault(bucket, []).append(row)

    bucket_order_24h = ["<-20", "[-20,0)", "[0,20)", ">=20", "NA"]
    bucket_order_volr = ["<8", "[8,10)", "[10,12)", "[12,15)", ">=15", "NA"]

    order = bucket_order_24h if bucket_key == "bucket_24h" else bucket_order_volr

    out: List[Dict[str, Any]] = []
    for bucket in order:
        rs = grouped.get(bucket, [])
        if not rs:
            continue

        pnl_list = [x["pnl_pct"] for x in rs if x["pnl_pct"] is not None]
        drop_list = [x["drop_pct"] for x in rs if x["drop_pct"] is not None]
        rebound_list = [x["rebound_ratio"] for x in rs if x["rebound_ratio"] is not None]
        volr_list = [x["volr"] for x in rs if x["volr"] is not None]

        total = len(rs)
        win_cnt = sum(1 for x in rs if (x["pnl_pct"] is not None and x["pnl_pct"] > 0))
        sl_cnt = sum(1 for x in rs if x["reason"] == "SL")
        ts_cnt = sum(1 for x in rs if x["reason"] == "TS")
        tp_cnt = sum(1 for x in rs if x["reason"] == "TP")
        bs_cnt = sum(1 for x in rs if x["reason"] == "BS")

        out.append(
            {
                "bucket": bucket,
                "trades": total,
                "win_rate": 100.0 * win_cnt / total,
                "avg_pnl_pct": _mean_or_na(pnl_list),
                "median_pnl_pct": _median_or_na(pnl_list),
                "sl_rate": 100.0 * sl_cnt / total,
                "ts_rate": 100.0 * ts_cnt / total,
                "tp_rate": 100.0 * tp_cnt / total,
                "bs_rate": 100.0 * bs_cnt / total,
                "avg_drop_pct": _mean_or_na(drop_list),
                "avg_rebound_ratio": _mean_or_na(rebound_list),
                "avg_volr": _mean_or_na(volr_list),
            }
        )
    return out


def _print_table(title: str, rows: List[Dict[str, Any]]) -> None:
    print("\n" + "=" * 100)
    print(title)
    print("=" * 100)
    header = (
        f"{'bucket':<10}"
        f"{'trades':>8}"
        f"{'win%':>8}"
        f"{'avg_pnl%':>12}"
        f"{'med_pnl%':>12}"
        f"{'sl%':>8}"
        f"{'ts%':>8}"
        f"{'tp%':>8}"
        f"{'bs%':>8}"
        f"{'avg_drop%':>12}"
        f"{'avg_reb':>10}"
        f"{'avg_volr':>10}"
    )
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"{r['bucket']:<10}"
            f"{r['trades']:>8d}"
            f"{_fmt_pct(r['win_rate']):>8}"
            f"{_fmt_pct(r['avg_pnl_pct']):>12}"
            f"{_fmt_pct(r['median_pnl_pct']):>12}"
            f"{_fmt_pct(r['sl_rate']):>8}"
            f"{_fmt_pct(r['ts_rate']):>8}"
            f"{_fmt_pct(r['tp_rate']):>8}"
            f"{_fmt_pct(r['bs_rate']):>8}"
            f"{_fmt_pct(r['avg_drop_pct']):>12}"
            f"{_fmt_num(r['avg_rebound_ratio']):>10}"
            f"{_fmt_num(r['avg_volr']):>10}"
        )


def analyze_one(path: Path, label: str) -> None:
    trades = _load_jsonl(path)
    rows: List[Dict[str, Any]] = []

    for tr in trades:
        row = _extract_trade_row(tr)
        row["bucket_24h"] = _bucket_24h_chg(row["chg_24h"])
        row["bucket_volr"] = _bucket_volr(row["volr"])
        rows.append(row)

    pnl_list = [x["pnl_pct"] for x in rows if x["pnl_pct"] is not None]
    print("\n" + "#" * 100)
    print(f"[{label}] {path}")
    print(f"trades={len(rows)} | avg_pnl_pct={_fmt_pct(_mean_or_na(pnl_list))} | median_pnl_pct={_fmt_pct(_median_or_na(pnl_list))}")
    print("#" * 100)

    stats_24h = _build_bucket_stats(rows, "bucket_24h")
    stats_volr = _build_bucket_stats(rows, "bucket_volr")

    _print_table(f"[{label}] 按 24hChg 分桶", stats_24h)
    _print_table(f"[{label}] 按 VolR 分桶", stats_volr)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze Snapback environment buckets from trades JSONL.")
    parser.add_argument(
        "--trades",
        action="append",
        required=True,
        help="Path to trades jsonl. Can be repeated.",
    )
    parser.add_argument(
        "--label",
        action="append",
        required=True,
        help="Label for each trades file. Must align with --trades order.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if len(args.trades) != len(args.label):
        raise SystemExit("--trades 和 --label 数量必须一致")

    for trades_path, label in zip(args.trades, args.label):
        analyze_one(Path(trades_path), label)


if __name__ == "__main__":
    main()