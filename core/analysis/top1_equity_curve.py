from __future__ import annotations

import argparse
import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, List

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd


class EquityVizError(RuntimeError):
    pass


@dataclass(frozen=True)
class SimTrade:
    symbol: str
    signal_time: int
    entry_time: int
    exit_time: int
    signal_price: float
    entry_price: float
    exit_price: float
    pnl_pct: float
    reason: str


DEFAULT_INDEX_WEIGHTS: Dict[str, float] = {
    "BTCUSDT": 0.56,
    "ETHUSDT": 0.24,
    "BNBUSDT": 0.12,
    "SOLUSDT": 0.08,
}


def iter_jsonl(path: str) -> Iterable[dict]:
    if not os.path.exists(path):
        raise EquityVizError(f"jsonl not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        for ln, line in enumerate(f, start=1):
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception as e:
                raise EquityVizError(f"json parse failed: {path}:{ln}: {e}") from e
            if not isinstance(obj, dict):
                raise EquityVizError(f"jsonl row must be object: {path}:{ln}")
            yield obj


def load_top1_trades(path: str) -> List[SimTrade]:
    out: List[SimTrade] = []
    for obj in iter_jsonl(path):
        required = [
            "symbol",
            "signal_time",
            "entry_time",
            "exit_time",
            "signal_price",
            "entry_price",
            "exit_price",
            "pnl_pct",
            "reason",
        ]
        missing = [k for k in required if k not in obj]
        if missing:
            raise EquityVizError(f"missing keys {missing} in {path}")
        out.append(
            SimTrade(
                symbol=str(obj["symbol"]).upper(),
                signal_time=int(obj["signal_time"]),
                entry_time=int(obj["entry_time"]),
                exit_time=int(obj["exit_time"]),
                signal_price=float(obj["signal_price"]),
                entry_price=float(obj["entry_price"]),
                exit_price=float(obj["exit_price"]),
                pnl_pct=float(obj["pnl_pct"]),
                reason=str(obj["reason"]),
            )
        )
    if not out:
        raise EquityVizError(f"no trades found: {path}")
    out.sort(key=lambda x: (x.exit_time, x.entry_time, x.signal_time, x.symbol))
    return out


def month_keys_between_utc(t_min_ms: int, t_max_ms: int) -> List[str]:
    if t_max_ms < t_min_ms:
        raise EquityVizError("invalid time range: t_max < t_min")
    dt0 = datetime.fromtimestamp(t_min_ms / 1000, tz=timezone.utc)
    dt1 = datetime.fromtimestamp(t_max_ms / 1000, tz=timezone.utc)
    y, m = dt0.year, dt0.month
    out: List[str] = []
    while True:
        out.append(f"{y:04d}-{m:02d}")
        if (y, m) == (dt1.year, dt1.month):
            return out
        m += 1
        if m == 13:
            y += 1
            m = 1


def load_symbol_1m_closes(
    *,
    kline_root: str,
    symbol: str,
    t_min_ms: int,
    t_max_ms: int,
) -> pd.DataFrame:
    months = month_keys_between_utc(t_min_ms, t_max_ms)
    paths: List[str] = []
    for mk in months:
        p = os.path.join(kline_root, symbol, f"{mk}.parquet")
        if not os.path.exists(p):
            raise EquityVizError(f"missing parquet: {p}")
        paths.append(p)

    frames: List[pd.DataFrame] = []
    for p in paths:
        try:
            df = pd.read_parquet(p, columns=["open_time_ms", "close"])
        except Exception as e:
            raise EquityVizError(f"failed to read parquet: {p}: {e}") from e
        frames.append(df)

    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all.sort_values("open_time_ms").drop_duplicates(
        "open_time_ms", keep="last"
    )
    df_all = df_all[
        (df_all["open_time_ms"] >= t_min_ms) & (df_all["open_time_ms"] <= t_max_ms)
    ].copy()
    if df_all.empty:
        raise EquityVizError(f"empty close series after slice: {symbol}")
    df_all["open_time_ms"] = df_all["open_time_ms"].astype("int64")
    df_all["close"] = df_all["close"].astype("float64")
    return df_all


def build_equity_curve(
    trades: List[SimTrade],
    *,
    initial_equity: float,
    fee_side: float,
) -> pd.DataFrame:
    rows: List[dict] = []
    gross = float(initial_equity)
    net = float(initial_equity)
    round_trip_fee = 2.0 * float(fee_side)

    for i, t in enumerate(trades, start=1):
        gross_r = float(t.pnl_pct)
        net_r = float(t.pnl_pct) - round_trip_fee

        # 采用单利逻辑：每次交易的本金固定为 initial_equity
        gross += float(initial_equity) * gross_r
        net += float(initial_equity) * net_r

        if gross <= 0 or net <= 0:
            raise EquityVizError(
                f"equity <= 0 after trade #{i}: symbol={t.symbol} gross={gross} net={net}"
            )

        rows.append(
            {
                "trade_no": i,
                "symbol": t.symbol,
                "exit_time": int(t.exit_time),
                "exit_dt": pd.to_datetime(int(t.exit_time), unit="ms", utc=True),
                "pnl_pct": float(t.pnl_pct),
                "gross_return_pct": gross_r * 100.0,
                "net_return_pct": net_r * 100.0,
                "equity_gross": gross,
                "equity_net": net,
                "reason": t.reason,
            }
        )
    return pd.DataFrame(rows)


def build_crypto_index_series(
    *,
    exit_times_ms: List[int],
    kline_root: str,
    weights: Dict[str, float],
    initial_equity: float,
) -> pd.Series:
    if not exit_times_ms:
        raise EquityVizError("exit_times_ms is empty")
    if not math.isclose(sum(weights.values()), 1.0, rel_tol=0.0, abs_tol=1e-9):
        raise EquityVizError(
            f"index weights must sum to 1.0, got {sum(weights.values())}"
        )

    t_min_ms = int(min(exit_times_ms))
    t_max_ms = int(max(exit_times_ms))

    merged: pd.DataFrame | None = None
    base_close: Dict[str, float] = {}

    for symbol, weight in weights.items():
        df = load_symbol_1m_closes(
            kline_root=kline_root,
            symbol=symbol,
            t_min_ms=t_min_ms,
            t_max_ms=t_max_ms,
        )
        df = df.rename(columns={"close": f"close_{symbol}"})
        base_close[symbol] = float(df.iloc[0][f"close_{symbol}"])
        merged = (
            df if merged is None else merged.merge(df, on="open_time_ms", how="outer")
        )

    if merged is None or merged.empty:
        raise EquityVizError("failed to build crypto index series")

    merged = merged.sort_values("open_time_ms").ffill().dropna().copy()
    if merged.empty:
        raise EquityVizError("crypto index series empty after forward-fill")

    idx_val = pd.Series(0.0, index=merged.index, dtype="float64")
    for symbol, weight in weights.items():
        col = f"close_{symbol}"
        idx_val += (merged[col] / base_close[symbol]) * float(weight)

    merged["index_equity"] = idx_val * float(initial_equity)
    merged["ts"] = pd.to_datetime(merged["open_time_ms"], unit="ms", utc=True)

    exit_df = pd.DataFrame(
        {
            "exit_time": [int(x) for x in exit_times_ms],
            "exit_dt": pd.to_datetime(
                [int(x) for x in exit_times_ms], unit="ms", utc=True
            ),
        }
    ).sort_values("exit_time")

    out = pd.merge_asof(
        exit_df,
        merged[["open_time_ms", "index_equity", "ts"]].sort_values("open_time_ms"),
        left_on="exit_time",
        right_on="open_time_ms",
        direction="backward",
    )
    if out["index_equity"].isna().any():
        raise EquityVizError("index merge_asof produced NaN values")
    return pd.Series(out["index_equity"].to_list(), index=exit_df["exit_dt"])


def plot_equity_curve(
    *,
    eq_df: pd.DataFrame,
    crypto_index: pd.Series,
    initial_equity: float,
    fee_side: float,
    weights: Dict[str, float],
    out_path: str,
    title_prefix: str = "Sim equity curve",
) -> None:
    weight_label = "/".join(f"{k[:-4]}{int(v * 100):d}" for k, v in weights.items())

    fig, ax = plt.subplots(figsize=(18, 8))
    ax.plot(
        eq_df["exit_dt"], eq_df["equity_gross"], label="Equity (gross)", linewidth=2.4
    )
    ax.plot(
        eq_df["exit_dt"],
        eq_df["equity_net"],
        label="Equity (net after fees)",
        linewidth=2.4,
    )
    ax.plot(
        crypto_index.index,
        crypto_index.values,
        "--",
        label=f"Crypto index ({weight_label})",
        linewidth=1.8,
    )

    ax.set_title(
        f"{title_prefix} (initial={initial_equity:.2f} USDT, fee/side={fee_side * 100:.4f}%)",
        fontsize=24,
    )
    ax.set_xlabel("Exit time", fontsize=18)
    ax.set_ylabel("Equity (USDT)", fontsize=18)
    ax.grid(True, alpha=0.25)
    ax.legend(loc="best", fontsize=14)
    ax.tick_params(axis="both", labelsize=12)

    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
    fig.tight_layout()
    fig.savefig(out_path, dpi=160, bbox_inches="tight", pad_inches=0.08)
    plt.close(fig)


def parse_weights(s: str) -> Dict[str, float]:
    parts = [x.strip() for x in s.split(",") if x.strip()]
    if not parts:
        raise EquityVizError("empty weights")
    out: Dict[str, float] = {}
    for part in parts:
        if ":" not in part:
            raise EquityVizError(f"invalid weight item: {part}")
        symbol, weight = part.split(":", 1)
        symbol = symbol.strip().upper()
        if not symbol.endswith("USDT"):
            symbol = f"{symbol}USDT"
        out[symbol] = float(weight.strip())
    total = sum(out.values())
    if total <= 0:
        raise EquityVizError("weights sum must be > 0")
    return {k: v / total for k, v in out.items()}


def build_summary(eq_df: pd.DataFrame, *, initial_equity: float) -> dict:
    gross = eq_df["equity_gross"]
    net = eq_df["equity_net"]
    pnl_pct = eq_df["pnl_pct"]
    return {
        "trades": int(len(eq_df)),
        "win_rate": float((pnl_pct > 0).mean()),
        "final_gross": float(gross.iloc[-1]),
        "final_net": float(net.iloc[-1]),
        "gross_return_pct": float(
            (gross.iloc[-1] / float(initial_equity) - 1.0) * 100.0
        ),
        "net_return_pct": float((net.iloc[-1] / float(initial_equity) - 1.0) * 100.0),
        "start_exit_bj": eq_df["exit_dt"]
        .iloc[0]
        .tz_convert("Asia/Shanghai")
        .strftime("%Y-%m-%d %H:%M"),
        "end_exit_bj": eq_df["exit_dt"]
        .iloc[-1]
        .tz_convert("Asia/Shanghai")
        .strftime("%Y-%m-%d %H:%M"),
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Top1 sim trades equity curve with crypto index"
    )
    p.add_argument("--trades", required=True, help="Path to sim_trades.Top1*.jsonl")
    p.add_argument(
        "--kline-root", default="data/klines_1m", help="Root dir of 1m parquet data"
    )
    p.add_argument(
        "--initial-equity", type=float, default=100.0, help="Initial equity in USDT"
    )
    p.add_argument(
        "--fee-side",
        type=float,
        default=0.0005,
        help="Fee per side, e.g. 0.0005 = 0.05%%",
    )
    p.add_argument(
        "--index-weights",
        default="BTC:0.56,ETH:0.24,BNB:0.12,SOL:0.08",
        help="Example: BTC:0.56,ETH:0.24,BNB:0.12,SOL:0.08",
    )
    p.add_argument("--out", required=True, help="PNG output path")
    p.add_argument("--summary-out", default=None, help="Optional json summary path")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    trades = load_top1_trades(args.trades)
    weights = parse_weights(args.index_weights)
    eq_df = build_equity_curve(
        trades,
        initial_equity=float(args.initial_equity),
        fee_side=float(args.fee_side),
    )
    crypto_index = build_crypto_index_series(
        exit_times_ms=eq_df["exit_time"].astype("int64").tolist(),
        kline_root=args.kline_root,
        weights=weights,
        initial_equity=float(args.initial_equity),
    )
    plot_equity_curve(
        eq_df=eq_df,
        crypto_index=crypto_index,
        initial_equity=float(args.initial_equity),
        fee_side=float(args.fee_side),
        weights=weights,
        out_path=args.out,
    )

    if args.summary_out:
        summary = build_summary(eq_df, initial_equity=float(args.initial_equity))
        os.makedirs(os.path.dirname(args.summary_out) or ".", exist_ok=True)
        with open(args.summary_out, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

    print(f"[OK] wrote: {args.out}")
    if args.summary_out:
        print(f"[OK] wrote: {args.summary_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
