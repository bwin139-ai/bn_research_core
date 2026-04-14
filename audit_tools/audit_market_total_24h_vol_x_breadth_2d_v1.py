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


def load_market_vol_trades_csv(path: Path) -> List[dict]:
    rows: List[dict] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            row["signal_time"] = int(float(row["signal_time"])) if row.get("signal_time") else None
            row["entry_time"] = int(float(row["entry_time"])) if row.get("entry_time") else None
            row["exit_time"] = int(float(row["exit_time"])) if row.get("exit_time") else None
            row["pnl_pct"] = float(row["pnl_pct"]) if row.get("pnl_pct") else 0.0
            row["market_total_24h_vol"] = float(row["market_total_24h_vol"]) if row.get("market_total_24h_vol") else 0.0
            rows.append(row)
    return rows


def build_candidate_count_map(candidate_audit_rows: List[dict]) -> Dict[int, int]:
    """
    candidate_pool_audit 落盘 bar_ts 是 C+1 分钟的 signal_time 口径，
    可直接与 sim_trades.signal_time 对齐。
    对重复 bar_ts，要求 candidate_count 一致；若不一致则报错。
    """
    out: Dict[int, int] = {}
    for row in candidate_audit_rows:
        signal_time = int(row["bar_ts"])
        candidate_count = int(row["candidate_count"])
        prev = out.get(signal_time)
        if prev is not None and prev != candidate_count:
            raise RuntimeError(
                f"candidate_count conflict at signal_time={signal_time}: {prev} vs {candidate_count}"
            )
        out[signal_time] = candidate_count
    return out


def market_vol_bucket(v: float) -> str:
    if v < 40_000_000_000:
        return "LT_40B"
    if v < 60_000_000_000:
        return "B40_60"
    if v < 80_000_000_000:
        return "B60_80"
    return "GE_80B"


def breadth_bucket(candidate_count: int) -> str:
    if candidate_count == 1:
        return "S"
    if candidate_count > 10:
        return "M_GT_10"
    if candidate_count > 1:
        return "M"
    return "UNKNOWN"


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


def summarize_by_month(rows: List[dict]) -> List[dict]:
    by_month: Dict[str, List[dict]] = defaultdict(list)
    for r in rows:
        bj = str(r.get("signal_time_bj") or "")
        month = bj[:7] if len(bj) >= 7 else "UNKNOWN"
        by_month[month].append(r)

    out: List[dict] = []
    for month in sorted(by_month.keys()):
        s = summarize_trades(by_month[month])
        out.append({
            "month": month,
            "trade_count": s["trade_count"],
            "pnl_sum_pct": s["pnl_sum_pct"],
            "avg_pnl_pct": s["avg_pnl_pct"],
            "win_rate": s["win_rate"],
            "take_profit": int(s["reason_counts"].get("TAKE_PROFIT", 0)),
            "stop_loss": int(s["reason_counts"].get("STOP_LOSS", 0)),
            "time_stop": int(s["reason_counts"].get("TIME_STOP", 0)),
        })
    return out


def write_enriched_csv(rows: List[dict], out_path: Path) -> None:
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
        "candidate_count",
        "breadth_bucket",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in headers})


def main() -> None:
    ap = argparse.ArgumentParser(description="2D audit: market_total_24h_vol x S/M/M_GT_10")
    ap.add_argument("--market-vol-trades", required=True, help="Path to market_total_24h_vol_vs_trade_outcomes_trades.csv")
    ap.add_argument("--candidate-audit", required=True, help="Path to snapback_candidate_pool_audit.jsonl")
    ap.add_argument("--out-dir", required=True, help="Output dir")
    args = ap.parse_args()

    market_vol_trades_path = Path(args.market_vol_trades)
    candidate_audit_path = Path(args.candidate_audit)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    trade_rows = load_market_vol_trades_csv(market_vol_trades_path)
    candidate_audit_rows = load_jsonl(candidate_audit_path)
    candidate_count_map = build_candidate_count_map(candidate_audit_rows)

    enriched_rows: List[dict] = []
    missing_candidate_count_rows: List[dict] = []

    for row in trade_rows:
        signal_time = int(row["signal_time"])
        candidate_count = candidate_count_map.get(signal_time)
        if candidate_count is None:
            missing_candidate_count_rows.append({
                "signal_time": signal_time,
                "signal_time_bj": row.get("signal_time_bj"),
                "symbol": row.get("symbol"),
            })
            continue

        item = dict(row)
        item["candidate_count"] = int(candidate_count)
        item["market_vol_bucket"] = market_vol_bucket(float(item["market_total_24h_vol"]))
        item["breadth_bucket"] = breadth_bucket(int(candidate_count))
        enriched_rows.append(item)

    overall = summarize_trades(enriched_rows)

    by_2d: List[dict] = []
    by_market_then_breadth: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    for row in enriched_rows:
        key = (str(row["market_vol_bucket"]), str(row["breadth_bucket"]))
        by_market_then_breadth[key].append(row)

    ordered_market_buckets = ["LT_40B", "B40_60", "B60_80", "GE_80B"]
    ordered_breadth_buckets = ["S", "M", "M_GT_10"]

    for mv in ordered_market_buckets:
        for br in ordered_breadth_buckets:
            group = by_market_then_breadth.get((mv, br), [])
            s = summarize_trades(group)
            by_2d.append({
                "market_vol_bucket": mv,
                "breadth_bucket": br,
                "trade_count": s["trade_count"],
                "pnl_sum_pct": s["pnl_sum_pct"],
                "avg_pnl_pct": s["avg_pnl_pct"],
                "win_rate": s["win_rate"],
                "take_profit": int(s["reason_counts"].get("TAKE_PROFIT", 0)),
                "stop_loss": int(s["reason_counts"].get("STOP_LOSS", 0)),
                "time_stop": int(s["reason_counts"].get("TIME_STOP", 0)),
            })

    by_breadth: Dict[str, List[dict]] = defaultdict(list)
    by_market: Dict[str, List[dict]] = defaultdict(list)
    for row in enriched_rows:
        by_breadth[str(row["breadth_bucket"])].append(row)
        by_market[str(row["market_vol_bucket"])].append(row)

    summary = {
        "input": {
            "market_vol_trades": str(market_vol_trades_path),
            "candidate_audit": str(candidate_audit_path),
        },
        "overall": overall,
        "missing_candidate_count_trade_count": len(missing_candidate_count_rows),
        "by_market_vol_bucket": {
            k: summarize_trades(v) for k, v in ordered_dict(by_market, ordered_market_buckets).items()
        },
        "by_breadth_bucket": {
            k: summarize_trades(v) for k, v in ordered_dict(by_breadth, ordered_breadth_buckets).items()
        },
        "by_2d_bucket": by_2d,
        "monthly_overall": summarize_by_month(enriched_rows),
        "monthly_by_breadth_bucket": build_monthly_by_bucket(enriched_rows, "breadth_bucket", ordered_breadth_buckets),
        "monthly_by_market_vol_bucket": build_monthly_by_bucket(enriched_rows, "market_vol_bucket", ordered_market_buckets),
        "missing_candidate_count_rows": missing_candidate_count_rows[:50],
    }

    summary_path = out_dir / "market_total_24h_vol_x_breadth_2d_summary.json"
    trades_csv_path = out_dir / "market_total_24h_vol_x_breadth_2d_trades.csv"

    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    write_enriched_csv(enriched_rows, trades_csv_path)

    print("=== market_total_24h_vol x breadth 2D audit done ===")
    print(f"market_vol_trades               : {market_vol_trades_path}")
    print(f"candidate_audit                 : {candidate_audit_path}")
    print(f"trade_count_used                : {len(enriched_rows)}")
    print(f"missing_candidate_count_trades  : {len(missing_candidate_count_rows)}")
    print(f"summary_json                    : {summary_path}")
    print(f"trades_csv                      : {trades_csv_path}")


def ordered_dict(src: Dict[str, List[dict]], order: List[str]) -> Dict[str, List[dict]]:
    out: Dict[str, List[dict]] = {}
    for k in order:
        out[k] = src.get(k, [])
    for k in sorted(src.keys()):
        if k not in out:
            out[k] = src[k]
    return out


def build_monthly_by_bucket(rows: List[dict], field: str, order: List[str]) -> List[dict]:
    by_month_bucket: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    for r in rows:
        bj = str(r.get("signal_time_bj") or "")
        month = bj[:7] if len(bj) >= 7 else "UNKNOWN"
        bucket = str(r.get(field) or "UNKNOWN")
        by_month_bucket[(month, bucket)].append(r)

    months = sorted({str(r.get("signal_time_bj") or "")[:7] if len(str(r.get("signal_time_bj") or "")) >= 7 else "UNKNOWN" for r in rows})
    out: List[dict] = []
    for month in months:
        for bucket in order:
            group = by_month_bucket.get((month, bucket), [])
            s = summarize_trades(group)
            out.append({
                "month": month,
                field: bucket,
                "trade_count": s["trade_count"],
                "pnl_sum_pct": s["pnl_sum_pct"],
                "avg_pnl_pct": s["avg_pnl_pct"],
                "win_rate": s["win_rate"],
                "take_profit": int(s["reason_counts"].get("TAKE_PROFIT", 0)),
                "stop_loss": int(s["reason_counts"].get("STOP_LOSS", 0)),
                "time_stop": int(s["reason_counts"].get("TIME_STOP", 0)),
            })
    return out


if __name__ == "__main__":
    main()
