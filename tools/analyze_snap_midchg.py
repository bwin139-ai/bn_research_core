#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
统计 Snapback 的中观 N 分钟涨跌幅(mid_chg_pct)与离场结果的关系。

默认定义：
    mid_chg_pct = (anchor_price / close_N_minutes_ago - 1) * 100

anchor 可选：
- c     : 使用 C 点价格 / C 点时间
- entry : 使用 entry_price / entry_time

用途：
1) 验证 “15mChg < 0 是否更符合 Snapback 世界观”
2) 先做统计，不先改策略逻辑
3) 支持粗分桶（<0 / >=0）与细分桶

示例：
python3 tools/analyze_snap_midchg.py \
  --trades output/state/sim_trades.SNAP_V4.2_30D_P6_0316T2216_ALL.jsonl \
  --kline-root data/klines_1m \
  --minutes 15 \
  --anchor c
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


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


def _ctx_first(ctx: Dict[str, Any], *keys: str) -> Any:
    for k in keys:
        if k in ctx and ctx[k] is not None:
            return ctx[k]
    return None


def _to_dt(value: Any) -> Optional[pd.Timestamp]:
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)) and not pd.isna(value):
            return pd.to_datetime(int(value), unit="ms", utc=True)
    except Exception:
        pass
    try:
        return pd.to_datetime(value, utc=True)
    except Exception:
        return None


def _reason_short(reason: str) -> str:
    reason = (reason or "").upper()
    if reason == "TAKE_PROFIT":
        return "TP"
    if reason == "STOP_LOSS":
        return "SL"
    if reason == "TIME_STOP":
        return "TS"
    if reason == "BREAKEVEN_STOP":
        return "BS"
    return "OTHER"


def _find_parquet(kline_root: Path, symbol: str) -> Path:
    direct = kline_root / f"{symbol}.parquet"
    if direct.exists():
        return direct

    candidates = list(kline_root.rglob(f"{symbol}.parquet"))
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        candidates = sorted(candidates, key=lambda p: (len(str(p)), str(p)))
        return candidates[0]

    raise FileNotFoundError(f"未找到 {symbol}.parquet (root={kline_root})")


def _load_symbol_df(cache: Dict[str, pd.DataFrame], kline_root: Path, symbol: str) -> pd.DataFrame:
    if symbol in cache:
        return cache[symbol]

    path = _find_parquet(kline_root, symbol)
    df = pd.read_parquet(path)

    if "open_time_ms" in df.columns:
        idx = pd.to_datetime(df["open_time_ms"].astype("int64"), unit="ms", utc=True)
    elif "open_time" in df.columns:
        idx = pd.to_datetime(df["open_time"], utc=True)
    else:
        raise RuntimeError(f"{path} 缺少 open_time_ms / open_time")

    df = df.copy()
    df.index = idx
    cache[symbol] = df
    return df


def _nearest_close(df: pd.DataFrame, ts: pd.Timestamp) -> Optional[float]:
    if df.empty:
        return None
    try:
        loc = df.index.get_indexer([ts], method="nearest")[0]
        if loc < 0:
            return None
        return _safe_float(df.iloc[loc]["close"])
    except Exception:
        return None


def _extract_midchg_row(
    tr: Dict[str, Any],
    kline_root: Path,
    cache: Dict[str, pd.DataFrame],
    minutes: int,
    anchor_mode: str,
) -> Optional[Dict[str, Any]]:
    symbol = tr.get("symbol")
    if not symbol:
        return None

    ctx = tr.get("context") or {}

    entry_time = _to_dt(tr.get("entry_time"))
    entry_price = _safe_float(tr.get("entry_price"))

    c_time = _to_dt(_ctx_first(ctx, "c_time_ms", "c_ts_ms", "c_time", "c_ts"))
    c_price = _safe_float(_ctx_first(ctx, "c_price", "c_px", "point_c_price"))

    if anchor_mode == "entry":
        anchor_ts = entry_time
        anchor_price = entry_price
    else:
        anchor_ts = c_time or entry_time
        anchor_price = c_price if c_price is not None else entry_price

    if anchor_ts is None or anchor_price is None:
        return None

    prev_ts = anchor_ts - pd.Timedelta(minutes=minutes)

    df = _load_symbol_df(cache, kline_root, symbol)
    prev_close = _nearest_close(df, prev_ts)
    if prev_close is None or prev_close == 0:
        return None

    mid_chg_pct = (float(anchor_price) / float(prev_close) - 1.0) * 100.0

    chg_24h_pct = _safe_float(_ctx_first(ctx, "chg_24h_pct"))
    if chg_24h_pct is None:
        raw_24h = _safe_float(_ctx_first(ctx, "chg_24h"))
        chg_24h_pct = raw_24h * 100.0 if raw_24h is not None else None

    return {
        "symbol": symbol,
        "reason": _reason_short(str(tr.get("reason", ""))),
        "pnl_pct": _safe_float(tr.get("pnl_pct")),
        "mid_chg_pct": mid_chg_pct,
        "anchor_ts": anchor_ts,
        "anchor_price": float(anchor_price),
        "prev_close": float(prev_close),
        "chg_24h_pct": chg_24h_pct,
        "volr": _safe_float(_ctx_first(ctx, "vol_ratio", "vol_r", "volR", "volume_ratio")),
        "drop_pct": _safe_float(_ctx_first(ctx, "drop_pct")),
        "rebound_ratio": _safe_float(_ctx_first(ctx, "rebound_ratio")),
    }


def _bucket_zero(v: Optional[float]) -> str:
    if v is None:
        return "NA"
    return "<0" if v < 0 else ">=0"


def _bucket_fine(v: Optional[float]) -> str:
    if v is None:
        return "NA"
    if v < -10:
        return "<-10"
    if v < -5:
        return "[-10,-5)"
    if v < 0:
        return "[-5,0)"
    if v < 5:
        return "[0,5)"
    if v < 10:
        return "[5,10)"
    return ">=10"


def _mean(xs: List[float]) -> Optional[float]:
    return (sum(xs) / len(xs)) if xs else None


def _median(xs: List[float]) -> Optional[float]:
    return statistics.median(xs) if xs else None


def _fmt(v: Optional[float], digits: int = 2) -> str:
    if v is None:
        return "NA"
    return f"{v:.{digits}f}"


def _build_stats(rows: List[Dict[str, Any]], key: str, order: List[str]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        grouped.setdefault(r[key], []).append(r)

    out = []
    for bucket in order:
        rs = grouped.get(bucket, [])
        if not rs:
            continue

        total = len(rs)
        pnl = [x["pnl_pct"] for x in rs if x["pnl_pct"] is not None]
        mid = [x["mid_chg_pct"] for x in rs if x["mid_chg_pct"] is not None]
        volr = [x["volr"] for x in rs if x["volr"] is not None]
        drop = [x["drop_pct"] for x in rs if x["drop_pct"] is not None]
        reb = [x["rebound_ratio"] for x in rs if x["rebound_ratio"] is not None]

        tp = sum(1 for x in rs if x["reason"] == "TP")
        sl = sum(1 for x in rs if x["reason"] == "SL")
        ts = sum(1 for x in rs if x["reason"] == "TS")
        bs = sum(1 for x in rs if x["reason"] == "BS")
        win = sum(1 for x in rs if x["pnl_pct"] is not None and x["pnl_pct"] > 0)

        out.append({
            "bucket": bucket,
            "trades": total,
            "win_rate": 100.0 * win / total,
            "avg_pnl_pct": _mean(pnl),
            "median_pnl_pct": _median(pnl),
            "tp_rate": 100.0 * tp / total,
            "sl_rate": 100.0 * sl / total,
            "ts_rate": 100.0 * ts / total,
            "bs_rate": 100.0 * bs / total,
            "avg_midchg_pct": _mean(mid),
            "avg_volr": _mean(volr),
            "avg_drop_pct": _mean(drop),
            "avg_rebound_ratio": _mean(reb),
        })
    return out


def _print_table(title: str, rows: List[Dict[str, Any]]) -> None:
    print("\n" + "=" * 118)
    print(title)
    print("=" * 118)
    header = (
        f"{'bucket':<12}"
        f"{'trades':>8}"
        f"{'win%':>8}"
        f"{'avg_pnl%':>12}"
        f"{'med_pnl%':>12}"
        f"{'tp%':>8}"
        f"{'sl%':>8}"
        f"{'ts%':>8}"
        f"{'bs%':>8}"
        f"{'avg_mid%':>11}"
        f"{'avg_volr':>10}"
        f"{'avg_drop%':>11}"
        f"{'avg_reb':>10}"
    )
    print(header)
    print("-" * len(header))
    for r in rows:
        print(
            f"{r['bucket']:<12}"
            f"{r['trades']:>8d}"
            f"{_fmt(r['win_rate']):>8}"
            f"{_fmt(r['avg_pnl_pct']):>12}"
            f"{_fmt(r['median_pnl_pct']):>12}"
            f"{_fmt(r['tp_rate']):>8}"
            f"{_fmt(r['sl_rate']):>8}"
            f"{_fmt(r['ts_rate']):>8}"
            f"{_fmt(r['bs_rate']):>8}"
            f"{_fmt(r['avg_midchg_pct']):>11}"
            f"{_fmt(r['avg_volr']):>10}"
            f"{_fmt(r['avg_drop_pct']):>11}"
            f"{_fmt(r['avg_rebound_ratio']):>10}"
        )


def _print_examples(title: str, rows: List[Dict[str, Any]], predicate, limit: int = 8) -> None:
    print("\n" + "-" * 118)
    print(title)
    print("-" * 118)
    count = 0
    for r in rows:
        if not predicate(r):
            continue
        print(
            f"{r['reason']:<5} "
            f"{r['symbol']:<14} "
            f"mid_chg={_fmt(r['mid_chg_pct']):>7}% "
            f"pnl={_fmt(r['pnl_pct']):>7}% "
            f"24hChg={_fmt(r['chg_24h_pct']):>7}% "
            f"VolR={_fmt(r['volr']):>6}"
        )
        count += 1
        if count >= limit:
            break
    if count == 0:
        print("(none)")


def main() -> None:
    ap = argparse.ArgumentParser(description="Analyze relation between Snapback exits and N-minute mid-change.")
    ap.add_argument("--trades", required=True, help="Path to sim_trades jsonl")
    ap.add_argument("--kline-root", required=True, help="Root dir of 1m klines parquet")
    ap.add_argument("--minutes", type=int, default=15, help="Lookback minutes for mid change")
    ap.add_argument("--anchor", choices=["c", "entry"], default="c", help="Use C point or entry point as anchor")
    args = ap.parse_args()

    trades_path = Path(args.trades)
    kline_root = Path(args.kline_root)

    trades = _load_jsonl(trades_path)
    cache: Dict[str, pd.DataFrame] = {}
    rows: List[Dict[str, Any]] = []
    skipped = 0
    for tr in trades:
        row = _extract_midchg_row(tr, kline_root, cache, args.minutes, args.anchor)
        if row is None:
            skipped += 1
            continue
        row["bucket_zero"] = _bucket_zero(row["mid_chg_pct"])
        row["bucket_fine"] = _bucket_fine(row["mid_chg_pct"])
        rows.append(row)

    pnl = [x["pnl_pct"] for x in rows if x["pnl_pct"] is not None]
    print("\n" + "#" * 118)
    print(f"trades_used={len(rows)} | skipped={skipped} | minutes={args.minutes} | anchor={args.anchor}")
    print(f"avg_pnl_pct={_fmt(_mean(pnl), 4)} | median_pnl_pct={_fmt(_median(pnl), 4)}")
    print("#" * 118)

    zero_stats = _build_stats(rows, "bucket_zero", ["<0", ">=0", "NA"])
    fine_stats = _build_stats(rows, "bucket_fine", ["<-10", "[-10,-5)", "[-5,0)", "[0,5)", "[5,10)", ">=10", "NA"])

    _print_table(f"按 {args.minutes}mChg 与 0 分界分桶", zero_stats)
    _print_table(f"按 {args.minutes}mChg 细分桶", fine_stats)

    rows_sorted = sorted(rows, key=lambda x: (x["mid_chg_pct"], x["pnl_pct"] if x["pnl_pct"] is not None else -999))
    _print_examples("负中观涨跌幅且 TP 的样本（前几笔）", rows_sorted, lambda r: r["mid_chg_pct"] < 0 and r["reason"] == "TP")
    _print_examples("正中观涨跌幅且 SL 的样本（前几笔）", rows_sorted, lambda r: r["mid_chg_pct"] >= 0 and r["reason"] == "SL")


if __name__ == "__main__":
    main()
