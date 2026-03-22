#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

UTC = timezone.utc
DEFAULT_INDEX_WEIGHTS = {
    "BTCUSDT": 0.56,
    "ETHUSDT": 0.24,
    "BNBUSDT": 0.12,
    "SOLUSDT": 0.08,
}


def round2(value: float) -> float:
    return round(float(value), 2)


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def prepare_rows(trades: List[Dict[str, Any]], fee_side: float) -> List[Dict[str, Any]]:
    fee_ratio_round_trip = fee_side * 2.0
    out: List[Dict[str, Any]] = []
    for row in trades:
        if row.get("pnl_pct") is None:
            continue
        exit_ms = 0
        for key in ("exit_time", "entry_time", "signal_time"):
            v = row.get(key)
            if isinstance(v, (int, float)):
                exit_ms = int(v)
                break
        gross_ratio = float(row["pnl_pct"])
        net_ratio = gross_ratio - fee_ratio_round_trip
        out.append(
            {
                "exit_time_ms": exit_ms,
                "dt": datetime.fromtimestamp(exit_ms / 1000.0, tz=UTC),
                "gross_ratio": gross_ratio,
                "net_ratio": net_ratio,
            }
        )
    out.sort(key=lambda x: x["exit_time_ms"])
    return out


def build_curves(rows: List[Dict[str, Any]], initial_equity: float):
    simple_gross = [initial_equity]
    simple_net = [initial_equity]
    compound_gross = [initial_equity]
    compound_net = [initial_equity]
    for r in rows:
        simple_gross.append(simple_gross[-1] + initial_equity * r["gross_ratio"])
        simple_net.append(simple_net[-1] + initial_equity * r["net_ratio"])
        compound_gross.append(compound_gross[-1] * max(0.0, 1.0 + r["gross_ratio"]))
        compound_net.append(compound_net[-1] * max(0.0, 1.0 + r["net_ratio"]))
    return simple_gross, simple_net, compound_gross, compound_net


def calc_max_drawdown(curve: List[float], times_ms: List[int]) -> Dict[str, Any]:
    if not curve:
        return {
            "amount_usdt": 0.00,
            "pct": 0.00,
            "days": 0.00,
            "trades": 0,
            "peak_index": 0,
            "trough_index": 0,
        }

    peak_val = curve[0]
    peak_idx = 0
    best_peak_idx = 0
    best_trough_idx = 0
    best_amount = 0.0

    for i, val in enumerate(curve):
        if val > peak_val:
            peak_val = val
            peak_idx = i
        draw_amount = peak_val - val
        if draw_amount > best_amount:
            best_amount = draw_amount
            best_peak_idx = peak_idx
            best_trough_idx = i

    peak_value = curve[best_peak_idx]
    trough_value = curve[best_trough_idx]
    draw_amount = peak_value - trough_value
    draw_pct = (draw_amount / peak_value * 100.0) if peak_value > 0 else 0.0
    days = 0.0
    if times_ms:
        days = max(0.0, (times_ms[best_trough_idx] - times_ms[best_peak_idx]) / 1000.0 / 86400.0)

    return {
        "amount_usdt": round2(draw_amount),
        "pct": round2(draw_pct),
        "days": round2(days),
        "trades": max(0, best_trough_idx - best_peak_idx),
        "peak_index": best_peak_idx,
        "trough_index": best_trough_idx,
    }


def load_symbol_series(kline_root: Path, symbol: str, months: List[str]) -> pd.DataFrame:
    dfs = []
    sym_dir = kline_root / symbol
    for month in months:
        fp = sym_dir / f"{month}.parquet"
        if fp.exists():
            dfs.append(
                pd.read_parquet(fp, columns=["open_time_ms", "close"]).rename(columns={"close": symbol})
            )
    if not dfs:
        return pd.DataFrame(columns=["open_time_ms", symbol])
    return pd.concat(dfs, ignore_index=True).sort_values("open_time_ms").drop_duplicates("open_time_ms", keep="last")


def build_index_series(exit_times_ms: List[int], kline_root: Path, initial_equity: float):
    if not exit_times_ms:
        return []
    months = sorted({datetime.fromtimestamp(ms / 1000.0, tz=UTC).strftime("%Y-%m") for ms in exit_times_ms})
    merged = pd.DataFrame({"open_time_ms": exit_times_ms}).sort_values("open_time_ms")
    for sym in DEFAULT_INDEX_WEIGHTS:
        sdf = load_symbol_series(kline_root, sym, months)
        if sdf.empty:
            return []
        merged = pd.merge_asof(merged, sdf.sort_values("open_time_ms"), on="open_time_ms", direction="backward")
    weighted = []
    for _, row in merged.iterrows():
        try:
            weighted.append(sum(float(row[sym]) * w for sym, w in DEFAULT_INDEX_WEIGHTS.items()))
        except Exception:
            weighted.append(None)
    last = None
    for i, v in enumerate(weighted):
        if v is None:
            weighted[i] = last
        else:
            last = v
    first = next((v for v in weighted if v is not None and v > 0), None)
    if not first:
        return []
    return [initial_equity * (float(v) / first) if v is not None else initial_equity for v in weighted]


def build_equity_payload(rows: List[Dict[str, Any]], initial_equity: float) -> Dict[str, Any]:
    simple_gross, simple_net, compound_gross, compound_net = build_curves(rows, initial_equity)
    times_ms = [rows[0]["exit_time_ms"] if rows else 0] + [r["exit_time_ms"] for r in rows]

    simple_gross_return_pct = sum(r["gross_ratio"] for r in rows) * 100.0
    simple_net_return_pct = sum(r["net_ratio"] for r in rows) * 100.0
    compound_gross_return_pct = ((compound_gross[-1] / initial_equity) - 1.0) * 100.0 if compound_gross else 0.0
    compound_net_return_pct = ((compound_net[-1] / initial_equity) - 1.0) * 100.0 if compound_net else 0.0

    return {
        "equity_initial_usdt": round2(initial_equity),
        "final_equity_simple_gross_usdt": round2(simple_gross[-1] if simple_gross else initial_equity),
        "final_equity_simple_net_usdt": round2(simple_net[-1] if simple_net else initial_equity),
        "final_equity_compound_gross_usdt": round2(compound_gross[-1] if compound_gross else initial_equity),
        "final_equity_compound_net_usdt": round2(compound_net[-1] if compound_net else initial_equity),
        "return_simple_gross_pct": round2(simple_gross_return_pct),
        "return_simple_net_pct": round2(simple_net_return_pct),
        "return_compound_gross_pct": round2(compound_gross_return_pct),
        "return_compound_net_pct": round2(compound_net_return_pct),
        "max_drawdown": {
            "simple_gross": calc_max_drawdown(simple_gross, times_ms),
            "simple_net": calc_max_drawdown(simple_net, times_ms),
            "compound_gross": calc_max_drawdown(compound_gross, times_ms),
            "compound_net": calc_max_drawdown(compound_net, times_ms),
        },
        "_curves": {
            "simple_gross": simple_gross,
            "simple_net": simple_net,
            "compound_gross": compound_gross,
            "compound_net": compound_net,
        },
    }


def fmt_dd(dd: Dict[str, Any]) -> str:
    return f"{dd['amount_usdt']:.2f} USDT ({dd['pct']:.2f}%) / {dd['days']:.2f}d / {dd['trades']}t"


def plot_curve(
    out_path: Path,
    title: str,
    times: List[datetime],
    gross_curve: List[float],
    net_curve: List[float],
    gross_return_pct: float,
    net_return_pct: float,
    gross_dd: Dict[str, Any],
    net_dd: Dict[str, Any],
    index_curve: List[float],
    initial_equity: float,
    fee_side: float,
):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(18, 8))
    ax.plot(times, gross_curve[1:], label="Equity (gross)")
    ax.plot(times, net_curve[1:], label="Equity (net after fees)")
    if index_curve:
        ax.plot(times, index_curve, "--", label="Crypto index (BTC56/ETH24/BNB12/SOL08)")
    ax.set_xlabel("Exit time")
    ax.set_ylabel("Equity (USDT)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.grid(True, alpha=0.3)
    subtitle = (
        f"Gross={gross_return_pct:.2f}% | Net={net_return_pct:.2f}%    "
        f"MaxDD gross[{fmt_dd(gross_dd)}]    MaxDD net[{fmt_dd(net_dd)}]"
    )
    ax.set_title(
        f"{title} (initial={initial_equity:.2f} USDT, fee/side={fee_side * 100.0:.2f}%)\n{subtitle}"
    )
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def resolve_paths_from_run_id(run_id: str, state_dir: Path):
    return {
        "trades": state_dir / f"sim_trades.{run_id}.jsonl",
        "summary": state_dir / f"sim_summary.{run_id}.json",
        "simple_out": state_dir / f"sim_curve_simple.{run_id}.png",
        "compound_out": state_dir / f"sim_curve_compound.{run_id}.png",
        "summary_out": state_dir / f"sim_equity.{run_id}.json",
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--state-dir", default="output/state")
    ap.add_argument("--trades", default=None)
    ap.add_argument("--summary", default=None)
    ap.add_argument("--simple-out", default=None)
    ap.add_argument("--compound-out", default=None)
    ap.add_argument("--summary-out", default=None)
    ap.add_argument("--kline-root", default="data/klines_1m")
    ap.add_argument("--initial-equity", type=float, default=100.0)
    ap.add_argument("--fee-side", type=float, default=0.0005)
    args = ap.parse_args()

    state_dir = Path(args.state_dir)
    if args.run_id:
        paths = resolve_paths_from_run_id(args.run_id, state_dir)
        trades_path = paths["trades"]
        summary_path = paths["summary"]
        simple_out = paths["simple_out"]
        compound_out = paths["compound_out"]
        summary_out = paths["summary_out"]
    else:
        missing = [
            name for name, value in {
                "--trades": args.trades,
                "--simple-out": args.simple_out,
                "--compound-out": args.compound_out,
                "--summary-out": args.summary_out,
            }.items() if not value
        ]
        if missing:
            raise SystemExit(f"missing required arguments: {', '.join(missing)}")
        trades_path = Path(args.trades)
        summary_path = Path(args.summary) if args.summary else None
        simple_out = Path(args.simple_out)
        compound_out = Path(args.compound_out)
        summary_out = Path(args.summary_out)

    summary = {}
    if summary_path and summary_path.exists():
        summary = json.loads(summary_path.read_text(encoding="utf-8"))
    initial_equity = float(summary.get("equity_initial_usdt", summary.get("equity_initial", args.initial_equity)))
    fee_side = float(summary.get("fee_side", args.fee_side))

    rows = prepare_rows(load_jsonl(trades_path), fee_side)
    payload = build_equity_payload(rows, initial_equity)
    curves = payload.pop("_curves")

    times = [r["dt"] for r in rows]
    index_curve = build_index_series([r["exit_time_ms"] for r in rows], Path(args.kline_root), initial_equity)

    plot_curve(
        simple_out,
        "Sim equity curve",
        times,
        curves["simple_gross"],
        curves["simple_net"],
        payload["return_simple_gross_pct"],
        payload["return_simple_net_pct"],
        payload["max_drawdown"]["simple_gross"],
        payload["max_drawdown"]["simple_net"],
        index_curve,
        initial_equity,
        fee_side,
    )
    plot_curve(
        compound_out,
        "Sim compound equity curve",
        times,
        curves["compound_gross"],
        curves["compound_net"],
        payload["return_compound_gross_pct"],
        payload["return_compound_net_pct"],
        payload["max_drawdown"]["compound_gross"],
        payload["max_drawdown"]["compound_net"],
        index_curve,
        initial_equity,
        fee_side,
    )

    summary_out.write_text(
        json.dumps(
            {
                **payload,
                "fee_side_pct": round2(fee_side * 100.0),
                "simple_png": str(simple_out),
                "compound_png": str(compound_out),
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
