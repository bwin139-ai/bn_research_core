#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow.parquet as pq


@dataclass
class ProbeResult:
    data_dir: str
    symbol: str
    signal_time_ms: int
    row_count: int
    matched: bool
    prev_bar: Optional[Dict[str, Any]]
    curr_bar: Optional[Dict[str, Any]]
    highest_price: Optional[float]
    current_close: Optional[float]
    drop_pct: Optional[float]
    chg_24h: Optional[float]
    vol_24h: Optional[float]
    vol_climax: Optional[float]
    vol_baseline: Optional[float]
    vol_ratio: Optional[float]
    debug: Dict[str, Any]


def load_json(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def month_keys_for_range(start_ms: int, end_ms: int) -> List[str]:
    out: List[str] = []
    cur = pd.Timestamp(start_ms, unit="ms", tz="UTC").to_period("M")
    end = pd.Timestamp(end_ms, unit="ms", tz="UTC").to_period("M")
    while cur <= end:
        out.append(str(cur).replace("-", ""))
        cur += 1
    return out


def read_symbol_window(data_dir: str, symbol: str, start_ms: int, end_ms: int) -> pd.DataFrame:
    sym_dir = os.path.join(data_dir, symbol)
    if not os.path.isdir(sym_dir):
        raise FileNotFoundError(f"symbol dir not found: {sym_dir}")

    month_keys = month_keys_for_range(start_ms, end_ms)
    frames: List[pd.DataFrame] = []
    for mk in month_keys:
        fpath = os.path.join(sym_dir, f"{mk}.parquet")
        if not os.path.exists(fpath):
            continue
        tbl = pq.read_table(fpath)
        df = tbl.to_pandas()
        frames.append(df)

    if not frames:
        return pd.DataFrame(columns=["open_time_ms", "open", "high", "low", "close", "quote_asset_volume"])

    df = pd.concat(frames, ignore_index=True)
    df = df.sort_values("open_time_ms").drop_duplicates(subset=["open_time_ms"], keep="last").reset_index(drop=True)
    df = df[(df["open_time_ms"] >= start_ms) & (df["open_time_ms"] <= end_ms)].reset_index(drop=True)
    return df


def maybe_downcast_float32(df: pd.DataFrame, enabled: bool) -> pd.DataFrame:
    if not enabled:
        return df
    out = df.copy()
    float_cols = out.select_dtypes(include=["float64"]).columns.tolist()
    for col in float_cols:
        out[col] = out[col].astype("float32")
    return out


def series_scalar(x: Any) -> Optional[float]:
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    return float(x)


def bar_to_dict(row: Optional[pd.Series]) -> Optional[Dict[str, Any]]:
    if row is None:
        return None
    keys = ["open_time_ms", "open", "high", "low", "close", "quote_asset_volume"]
    out: Dict[str, Any] = {}
    for k in keys:
        if k not in row:
            continue
        v = row[k]
        if k == "open_time_ms":
            out[k] = int(v)
        else:
            out[k] = series_scalar(v)
    return out


def compute_probe(data_dir: str, symbol: str, signal_time_ms: int, config: Dict[str, Any], downcast_float32: bool) -> ProbeResult:
    drop_window = int(config.get("drop_window", 240))
    vol_window = int(config.get("vol_window", 24))
    vol_ma_window = int(config.get("vol_ma_window", 20))

    # Need enough history for 24h metrics and drop window. Use a generous margin.
    start_ms = signal_time_ms - max((24 * 60 + vol_ma_window + 5), (drop_window + 5)) * 60_000
    end_ms = signal_time_ms

    df = read_symbol_window(data_dir, symbol, start_ms, end_ms)
    df = maybe_downcast_float32(df, enabled=downcast_float32)

    if df.empty:
        return ProbeResult(
            data_dir=data_dir,
            symbol=symbol,
            signal_time_ms=signal_time_ms,
            row_count=0,
            matched=False,
            prev_bar=None,
            curr_bar=None,
            highest_price=None,
            current_close=None,
            drop_pct=None,
            chg_24h=None,
            vol_24h=None,
            vol_climax=None,
            vol_baseline=None,
            vol_ratio=None,
            debug={"reason": "empty_df"},
        )

    df = df.sort_values("open_time_ms").reset_index(drop=True)
    df["chg_24h"] = df["close"] / df["close"].shift(24 * 60) - 1.0
    df["vol_24h"] = df["quote_asset_volume"].rolling(24 * 60, min_periods=24 * 60).sum()
    df["vol_climax"] = df["quote_asset_volume"].rolling(vol_window, min_periods=vol_window).sum()
    df["vol_baseline"] = df["quote_asset_volume"].rolling(vol_ma_window, min_periods=vol_ma_window).mean() * vol_window
    df["vol_ratio"] = df["vol_climax"] / df["vol_baseline"].replace(0, pd.NA)
    df["lowest_ndays"] = df["low"].rolling(drop_window, min_periods=drop_window).min()

    idx_list = df.index[df["open_time_ms"] == signal_time_ms].tolist()
    if not idx_list:
        near = df.tail(3)[["open_time_ms", "close"]].to_dict(orient="records")
        return ProbeResult(
            data_dir=data_dir,
            symbol=symbol,
            signal_time_ms=signal_time_ms,
            row_count=len(df),
            matched=False,
            prev_bar=None,
            curr_bar=None,
            highest_price=None,
            current_close=None,
            drop_pct=None,
            chg_24h=None,
            vol_24h=None,
            vol_climax=None,
            vol_baseline=None,
            vol_ratio=None,
            debug={"reason": "signal_time_not_found", "tail": near},
        )

    idx = idx_list[0]
    curr = df.iloc[idx]
    prev = df.iloc[idx - 1] if idx > 0 else None

    start_idx = max(0, idx - drop_window + 1)
    recent_drop_df = df.iloc[start_idx : idx + 1]
    highest_price = recent_drop_df["high"].max() if not recent_drop_df.empty else math.nan
    current_close = curr["close"]
    drop_pct = (highest_price - current_close) / highest_price if highest_price and not pd.isna(highest_price) else math.nan

    return ProbeResult(
        data_dir=data_dir,
        symbol=symbol,
        signal_time_ms=signal_time_ms,
        row_count=len(df),
        matched=True,
        prev_bar=bar_to_dict(prev),
        curr_bar=bar_to_dict(curr),
        highest_price=series_scalar(highest_price),
        current_close=series_scalar(current_close),
        drop_pct=series_scalar(drop_pct),
        chg_24h=series_scalar(curr.get("chg_24h")),
        vol_24h=series_scalar(curr.get("vol_24h")),
        vol_climax=series_scalar(curr.get("vol_climax")),
        vol_baseline=series_scalar(curr.get("vol_baseline")),
        vol_ratio=series_scalar(curr.get("vol_ratio")),
        debug={
            "drop_window": drop_window,
            "vol_window": vol_window,
            "vol_ma_window": vol_ma_window,
            "start_idx": int(start_idx),
            "idx": int(idx),
            "recent_drop_rows": int(len(recent_drop_df)),
            "recent_drop_first_open_time_ms": int(recent_drop_df.iloc[0]["open_time_ms"]) if len(recent_drop_df) else None,
            "recent_drop_last_open_time_ms": int(recent_drop_df.iloc[-1]["open_time_ms"]) if len(recent_drop_df) else None,
        },
    )


def to_jsonable(result: ProbeResult) -> Dict[str, Any]:
    return {
        "data_dir": result.data_dir,
        "symbol": result.symbol,
        "signal_time_ms": result.signal_time_ms,
        "row_count": result.row_count,
        "matched": result.matched,
        "prev_bar": result.prev_bar,
        "curr_bar": result.curr_bar,
        "highest_price": result.highest_price,
        "current_close": result.current_close,
        "drop_pct": result.drop_pct,
        "chg_24h": result.chg_24h,
        "vol_24h": result.vol_24h,
        "vol_climax": result.vol_climax,
        "vol_baseline": result.vol_baseline,
        "vol_ratio": result.vol_ratio,
        "debug": result.debug,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--old-data-dir", required=True)
    ap.add_argument("--new-data-dir", required=True)
    ap.add_argument("--symbol", required=True)
    ap.add_argument("--signal-time-ms", required=True, type=int)
    ap.add_argument("--config", required=True)
    ap.add_argument("--downcast-float32", choices=["on", "off"], default="on")
    ap.add_argument("--out", default="")
    args = ap.parse_args()

    config = load_json(args.config)
    downcast = args.downcast_float32 == "on"

    old_res = compute_probe(args.old_data_dir, args.symbol, args.signal_time_ms, config, downcast)
    new_res = compute_probe(args.new_data_dir, args.symbol, args.signal_time_ms, config, downcast)

    out = {
        "symbol": args.symbol,
        "signal_time_ms": args.signal_time_ms,
        "config_path": args.config,
        "downcast_float32": downcast,
        "old": to_jsonable(old_res),
        "new": to_jsonable(new_res),
        "diff": {
            "current_close": None if old_res.current_close is None or new_res.current_close is None else (new_res.current_close - old_res.current_close),
            "highest_price": None if old_res.highest_price is None or new_res.highest_price is None else (new_res.highest_price - old_res.highest_price),
            "drop_pct": None if old_res.drop_pct is None or new_res.drop_pct is None else (new_res.drop_pct - old_res.drop_pct),
            "chg_24h": None if old_res.chg_24h is None or new_res.chg_24h is None else (new_res.chg_24h - old_res.chg_24h),
            "vol_24h": None if old_res.vol_24h is None or new_res.vol_24h is None else (new_res.vol_24h - old_res.vol_24h),
            "vol_climax": None if old_res.vol_climax is None or new_res.vol_climax is None else (new_res.vol_climax - old_res.vol_climax),
            "vol_baseline": None if old_res.vol_baseline is None or new_res.vol_baseline is None else (new_res.vol_baseline - old_res.vol_baseline),
            "vol_ratio": None if old_res.vol_ratio is None or new_res.vol_ratio is None else (new_res.vol_ratio - old_res.vol_ratio),
        },
    }

    text = json.dumps(out, ensure_ascii=False, indent=2)
    if args.out:
        os.makedirs(os.path.dirname(args.out), exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(text)
            f.write("\n")
    print(text)


if __name__ == "__main__":
    main()
