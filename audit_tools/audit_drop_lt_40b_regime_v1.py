#!/usr/bin/env python3
import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple


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


def load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        raise RuntimeError(f"Failed to load json from {path}: {e}") from e


def summarize_trades(rows: List[dict]) -> dict:
    n = len(rows)
    pnl_sum = sum(float(r["pnl_pct"]) for r in rows)
    win_count = sum(1 for r in rows if float(r["pnl_pct"]) > 0)
    loss_count = sum(1 for r in rows if float(r["pnl_pct"]) < 0)
    flat_count = sum(1 for r in rows if float(r["pnl_pct"]) == 0)
    reason_counts = Counter(str(r.get("reason", "")) for r in rows)
    return {
        "trade_count": n,
        "pnl_sum_pct": round(pnl_sum, 6),
        "avg_pnl_pct": round(pnl_sum / n, 6) if n else 0.0,
        "win_count": win_count,
        "loss_count": loss_count,
        "flat_count": flat_count,
        "win_rate": round(win_count / n, 6) if n else 0.0,
        "loss_rate": round(loss_count / n, 6) if n else 0.0,
        "reason_counts": dict(reason_counts),
        "reason_rates": {k: round(v / n, 6) for k, v in sorted(reason_counts.items())} if n else {},
    }


def summarize_monthly(rows: List[dict]) -> List[dict]:
    by_month: Dict[str, List[dict]] = defaultdict(list)
    for r in rows:
        bj = str(r.get("signal_time_bj") or "")
        month = bj[:7] if len(bj) >= 7 else "UNKNOWN"
        by_month[month].append(r)

    out: List[dict] = []
    for month in sorted(by_month.keys()):
        group = by_month[month]
        summary = summarize_trades(group)
        out.append({
            "month": month,
            "trade_count": summary["trade_count"],
            "pnl_sum_pct": summary["pnl_sum_pct"],
            "avg_pnl_pct": summary["avg_pnl_pct"],
            "win_rate": summary["win_rate"],
            "take_profit": int(summary["reason_counts"].get("TAKE_PROFIT", 0)),
            "stop_loss": int(summary["reason_counts"].get("STOP_LOSS", 0)),
            "time_stop": int(summary["reason_counts"].get("TIME_STOP", 0)),
        })
    return out


def write_csv(rows: List[dict], out_path: Path) -> None:
    headers = [
        "symbol",
        "signal_time",
        "signal_time_bj",
        "entry_time",
        "exit_time",
        "pnl_pct",
        "reason",
        "market_total_24h_vol",
        "market_vol_bucket",
        "kept_after_filter",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            payload = {k: row.get(k, "") for k in headers}
            writer.writerow(payload)


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit the effect of dropping <40B regime trades.")
    ap.add_argument("--market-vol-trades", required=True, help="Path to market_total_24h_vol_vs_trade_outcomes_trades.csv")
    ap.add_argument("--threshold", type=float, default=40_000_000_000.0, help="Absolute lower bound of market_total_24h_vol to keep")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    args = ap.parse_args()

    trades_path = Path(args.market_vol_trades)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows: List[dict] = []
    with trades_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["signal_time"] = int(float(row["signal_time"])) if row.get("signal_time") else None
            row["entry_time"] = int(float(row["entry_time"])) if row.get("entry_time") else None
            row["exit_time"] = int(float(row["exit_time"])) if row.get("exit_time") else None
            row["pnl_pct"] = float(row["pnl_pct"]) if row.get("pnl_pct") else 0.0
            row["market_total_24h_vol"] = float(row["market_total_24h_vol"]) if row.get("market_total_24h_vol") else 0.0
            rows.append(row)

    baseline_rows = list(rows)
    filtered_rows = [r for r in rows if float(r["market_total_24h_vol"]) >= float(args.threshold)]

    for row in baseline_rows:
        row["kept_after_filter"] = bool(float(row["market_total_24h_vol"]) >= float(args.threshold))

    summary = {
        "filter_rule": {
            "market_total_24h_vol_min": float(args.threshold),
            "market_total_24h_vol_min_billions": round(float(args.threshold) / 1_000_000_000.0, 3),
        },
        "baseline": summarize_trades(baseline_rows),
        "filtered": summarize_trades(filtered_rows),
        "delta_filtered_minus_baseline": {},
        "baseline_monthly": summarize_monthly(baseline_rows),
        "filtered_monthly": summarize_monthly(filtered_rows),
        "dropped_trade_count": int(len(baseline_rows) - len(filtered_rows)),
        "dropped_ratio": round((len(baseline_rows) - len(filtered_rows)) / len(baseline_rows), 6) if baseline_rows else 0.0,
    }

    base = summary["baseline"]
    filt = summary["filtered"]
    summary["delta_filtered_minus_baseline"] = {
        "trade_count": int(filt["trade_count"] - base["trade_count"]),
        "pnl_sum_pct": round(float(filt["pnl_sum_pct"]) - float(base["pnl_sum_pct"]), 6),
        "avg_pnl_pct": round(float(filt["avg_pnl_pct"]) - float(base["avg_pnl_pct"]), 6),
        "win_rate": round(float(filt["win_rate"]) - float(base["win_rate"]), 6),
    }

    summary_path = out_dir / "drop_lt_40b_regime_summary.json"
    trades_csv_path = out_dir / "drop_lt_40b_regime_trades.csv"

    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    write_csv(baseline_rows, trades_csv_path)

    print("=== drop <40B regime audit done ===")
    print(f"market_vol_trades       : {trades_path}")
    print(f"threshold               : {args.threshold}")
    print(f"baseline_trade_count    : {summary['baseline']['trade_count']}")
    print(f"filtered_trade_count    : {summary['filtered']['trade_count']}")
    print(f"dropped_trade_count     : {summary['dropped_trade_count']}")
    print(f"summary_json            : {summary_path}")
    print(f"trades_csv              : {trades_csv_path}")


if __name__ == "__main__":
    main()
