#!/usr/bin/env python3
import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


@dataclass
class TradeRow:
    symbol: str
    signal_time: int
    signal_time_bj: str
    entry_time: int
    exit_time: int
    pnl_pct: float
    reason: str
    market_total_24h_vol: float
    market_vol_bucket: str


def load_jsonl(path: Path) -> List[dict]:
    rows: List[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception as e:
                raise RuntimeError(f"JSON parse failed at line {lineno}: {e}") from e
    return rows


def bj_str_from_ms(ms: int) -> str:
    return pd.to_datetime(ms, unit="ms", utc=True).tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S")


def infer_ms(v: pd.Series) -> pd.Series:
    ts_num = pd.to_numeric(v, errors="coerce")
    if ts_num.isna().all():
        ts_dt = pd.to_datetime(v, errors="coerce", utc=True)
        if ts_dt.isna().all():
            raise RuntimeError("cannot infer timestamp series")
        ts_num = ts_dt.view("int64") // 10**6
    if float(ts_num.max()) < 10**12:
        ts_num = ts_num * 1000
    return ts_num.astype("int64")


def list_symbol_dirs(kline_root: Path) -> List[Path]:
    dirs = [p for p in kline_root.iterdir() if p.is_dir()]
    return sorted(dirs, key=lambda p: p.name)


def months_needed(target_ts_list: List[int]) -> List[str]:
    if not target_ts_list:
        return []
    start = pd.to_datetime(min(target_ts_list), unit="ms", utc=True).tz_convert("Asia/Shanghai")
    end = pd.to_datetime(max(target_ts_list), unit="ms", utc=True).tz_convert("Asia/Shanghai")
    start = (start - pd.Timedelta(days=2)).to_period("M").to_timestamp()
    end = end.to_period("M").to_timestamp()
    months = pd.period_range(start=start, end=end, freq="M")
    return [str(m) for m in months]


def load_symbol_months(symbol_dir: Path, month_keys: List[str]) -> Optional[pd.DataFrame]:
    parts = []
    for mk in month_keys:
        p_parquet = symbol_dir / f"{mk}.parquet"
        p_csv = symbol_dir / f"{mk}.csv"
        if p_parquet.exists():
            df = pd.read_parquet(p_parquet)
        elif p_csv.exists():
            df = pd.read_csv(p_csv)
        else:
            continue
        col_map = {str(c).lower(): c for c in df.columns}
        ts_col = None
        for cand in ["open_time_ms", "open_time", "ts", "timestamp", "time", "t"]:
            if cand in col_map:
                ts_col = col_map[cand]
                break
        if ts_col is None:
            raise RuntimeError(f"missing timestamp column in {symbol_dir}")
        if "quote_asset_volume" not in col_map:
            raise RuntimeError(f"missing quote_asset_volume in {symbol_dir}")
        ts = infer_ms(df[ts_col])
        qav = pd.to_numeric(df[col_map["quote_asset_volume"]], errors="coerce")
        out = pd.DataFrame({"quote_asset_volume": qav.values}, index=pd.Index(ts, name="open_time"))
        out = out.sort_index()
        out = out[~out.index.duplicated(keep="first")]
        out = out.dropna(subset=["quote_asset_volume"])
        parts.append(out)

    if not parts:
        return None

    df_all = pd.concat(parts, axis=0).sort_index()
    df_all = df_all[~df_all.index.duplicated(keep="first")]
    return df_all


def compute_market_total_24h_vol_for_targets(kline_root: Path, target_ts_list: List[int]) -> Dict[int, float]:
    if not target_ts_list:
        return {}
    target_ts_list = sorted(set(int(x) for x in target_ts_list))
    month_keys = months_needed(target_ts_list)
    accum: Dict[int, float] = {int(ts): 0.0 for ts in target_ts_list}

    symbol_dirs = list_symbol_dirs(kline_root)
    if not symbol_dirs:
        raise RuntimeError(f"no symbol directories found under {kline_root}")

    for symbol_dir in symbol_dirs:
        df = load_symbol_months(symbol_dir, month_keys)
        if df is None or df.empty:
            continue

        qav = df["quote_asset_volume"].astype(float)
        roll = qav.rolling(window=1440, min_periods=1).sum()

        idx = roll.index.to_numpy()
        vals = roll.to_numpy()

        for ts in target_ts_list:
            pos = idx.searchsorted(ts, side="right") - 1
            if pos >= 0:
                accum[ts] += float(vals[pos])

    return accum


def bucket_by_quantiles(values: List[float], labels: List[str]) -> Tuple[List[str], Dict[str, float]]:
    if not values:
        return [], {}
    s = pd.Series(values, dtype="float64")
    try:
        buckets = pd.qcut(s.rank(method="first"), q=len(labels), labels=labels)
    except Exception:
        buckets = pd.Series([labels[0]] * len(values))
    thresholds = {
        "min": float(s.min()),
        "p25": float(s.quantile(0.25)),
        "p50": float(s.quantile(0.50)),
        "p75": float(s.quantile(0.75)),
        "max": float(s.max()),
    }
    return [str(x) for x in buckets.tolist()], thresholds


def build_summary(rows: List[TradeRow], thresholds: Dict[str, float]) -> dict:
    by_reason: Dict[str, List[TradeRow]] = defaultdict(list)
    by_bucket: Dict[str, List[TradeRow]] = defaultdict(list)
    by_month_reason: Dict[str, Dict[str, List[TradeRow]]] = defaultdict(lambda: defaultdict(list))

    for r in rows:
        by_reason[r.reason].append(r)
        by_bucket[r.market_vol_bucket].append(r)
        by_month_reason[r.signal_time_bj[:7]][r.reason].append(r)

    def summarize_group(group: List[TradeRow]) -> dict:
        n = len(group)
        return {
            "trade_count": n,
            "avg_market_total_24h_vol": round(sum(x.market_total_24h_vol for x in group) / n, 6) if n else 0.0,
            "avg_pnl_pct": round(sum(x.pnl_pct for x in group) / n, 6) if n else 0.0,
            "win_rate": round(sum(1 for x in group if x.pnl_pct > 0) / n, 6) if n else 0.0,
        }

    summary = {
        "overall": {
            "trade_count": len(rows),
            "avg_market_total_24h_vol": round(sum(x.market_total_24h_vol for x in rows) / len(rows), 6) if rows else 0.0,
            "avg_pnl_pct": round(sum(x.pnl_pct for x in rows) / len(rows), 6) if rows else 0.0,
            "reason_counts": dict(Counter(x.reason for x in rows)),
            "market_vol_thresholds": thresholds,
        },
        "by_reason": {k: summarize_group(v) for k, v in sorted(by_reason.items())},
        "by_market_vol_bucket": {k: summarize_group(v) for k, v in sorted(by_bucket.items())},
        "monthly_reason_summary": [],
    }

    for month in sorted(by_month_reason.keys()):
        for reason in sorted(by_month_reason[month].keys()):
            group = by_month_reason[month][reason]
            item = {"month": month, "reason": reason}
            item.update(summarize_group(group))
            summary["monthly_reason_summary"].append(item)

    return summary


def write_csv(rows: List[TradeRow], out_path: Path) -> None:
    headers = [
        "symbol", "signal_time", "signal_time_bj", "entry_time", "exit_time",
        "pnl_pct", "reason", "market_total_24h_vol", "market_vol_bucket"
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            writer.writerow({
                "symbol": r.symbol,
                "signal_time": r.signal_time,
                "signal_time_bj": r.signal_time_bj,
                "entry_time": r.entry_time,
                "exit_time": r.exit_time,
                "pnl_pct": r.pnl_pct,
                "reason": r.reason,
                "market_total_24h_vol": r.market_total_24h_vol,
                "market_vol_bucket": r.market_vol_bucket,
            })


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare TP/SL/TS against total market 24h quote volume.")
    ap.add_argument("--sim-trades", required=True, help="Path to sim_trades.*.jsonl")
    ap.add_argument("--kline-root", required=True, help="Root dir like data/klines_1m")
    ap.add_argument("--out-dir", required=True, help="Output dir")
    args = ap.parse_args()

    sim_trades_path = Path(args.sim_trades)
    kline_root = Path(args.kline_root)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    trades = load_jsonl(sim_trades_path)
    target_ts = [int(x["signal_time"]) for x in trades]
    market_map = compute_market_total_24h_vol_for_targets(kline_root, target_ts)

    labels = ["LOW", "MID_LOW", "MID_HIGH", "HIGH"]
    ordered_vals = [float(market_map[int(x["signal_time"])]) for x in trades]
    bucket_labels, thresholds = bucket_by_quantiles(ordered_vals, labels)

    rows: List[TradeRow] = []
    for trade, bucket in zip(trades, bucket_labels):
        rows.append(
            TradeRow(
                symbol=str(trade["symbol"]).upper(),
                signal_time=int(trade["signal_time"]),
                signal_time_bj=bj_str_from_ms(int(trade["signal_time"])),
                entry_time=int(trade.get("entry_time", trade["signal_time"])),
                exit_time=int(trade.get("exit_time", trade.get("signal_time"))),
                pnl_pct=float(trade["pnl_pct"]),
                reason=str(trade["reason"]),
                market_total_24h_vol=float(market_map[int(trade["signal_time"])]),
                market_vol_bucket=bucket,
            )
        )

    summary = build_summary(rows, thresholds)

    summary_path = out_dir / "market_total_24h_vol_vs_trade_outcomes_summary.json"
    trades_csv = out_dir / "market_total_24h_vol_vs_trade_outcomes_trades.csv"

    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv(rows, trades_csv)

    print("=== market total 24h volume vs trade outcomes audit done ===")
    print(f"sim_trades              : {sim_trades_path}")
    print(f"kline_root              : {kline_root}")
    print(f"trade_count             : {len(rows)}")
    print(f"summary_json            : {summary_path}")
    print(f"trades_csv              : {trades_csv}")


if __name__ == "__main__":
    main()
