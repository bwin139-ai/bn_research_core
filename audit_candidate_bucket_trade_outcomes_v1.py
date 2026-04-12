#!/usr/bin/env python3
import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Tuple


EXPECTED_REASONS = ["TAKE_PROFIT", "STOP_LOSS", "TIME_STOP"]


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
                raise RuntimeError(f"JSON parse failed at line {lineno} in {path}: {e}") from e
    return rows


def dedupe_candidate_rows(rows: List[dict]) -> Tuple[List[dict], List[dict], int, int]:
    by_bar: Dict[int, List[dict]] = defaultdict(list)
    for row in rows:
        if "bar_ts" not in row:
            raise RuntimeError("candidate audit row missing bar_ts")
        by_bar[int(row["bar_ts"])] .append(row)

    unique_rows: List[dict] = []
    dup_samples: List[dict] = []
    duplicate_groups = 0
    duplicate_extra_rows = 0

    for bar_ts in sorted(by_bar.keys()):
        group = by_bar[bar_ts]
        first = group[0]
        if len(group) > 1:
            duplicate_groups += 1
            duplicate_extra_rows += len(group) - 1
            dup_samples.append({
                "bar_ts": bar_ts,
                "bar_bj": first.get("bar_bj"),
                "duplicate_rows": len(group),
                "all_rows_identical": all(x == first for x in group[1:]),
            })
        unique_rows.append(first)

    return unique_rows, dup_samples, duplicate_groups, duplicate_extra_rows


def month_from_bj(s: str) -> str:
    return str(s)[:7]


def build_bucket_map(candidate_rows: List[dict]) -> Dict[Tuple[str, int], dict]:
    out: Dict[Tuple[str, int], dict] = {}
    for row in candidate_rows:
        cands = row.get("candidates_sorted_by_drop_pct") or []
        if not cands:
            continue
        top1 = cands[0]
        symbol = str(top1.get("symbol", "")).strip().upper()
        if not symbol:
            continue
        # on_kline_close uses c_time=current_time_ms, while signal_time is current_time_ms + 60000
        signal_time = int(row["bar_ts"]) + 60000
        out[(symbol, signal_time)] = {
            "bucket": "S" if int(row["candidate_count"]) == 1 else "M",
            "candidate_count": int(row["candidate_count"]),
            "bar_ts": int(row["bar_ts"]),
            "bar_bj": row.get("bar_bj"),
            "top3_symbols": ", ".join(str(x.get("symbol")) for x in cands[:3]),
            "top1_drop_pct": top1.get("drop_pct"),
        }
    return out


def normalize_reason(reason: str) -> str:
    r = str(reason or "").strip().upper()
    if not r:
        return "UNKNOWN"
    return r


def build_trade_rows(trades: List[dict], bucket_map: Dict[Tuple[str, int], dict]) -> Tuple[List[dict], List[dict]]:
    enriched: List[dict] = []
    unmatched: List[dict] = []
    for t in trades:
        symbol = str(t.get("symbol", "")).strip().upper()
        signal_time = int(t["signal_time"])
        meta = bucket_map.get((symbol, signal_time))
        if meta is None:
            unmatched.append({
                "symbol": symbol,
                "signal_time": signal_time,
                "signal_time_bj": t.get("signal_time_bj"),
            })
            continue
        row = dict(t)
        row["bucket"] = meta["bucket"]
        row["candidate_count"] = meta["candidate_count"]
        row["bar_ts"] = meta["bar_ts"]
        row["bar_bj"] = meta["bar_bj"]
        row["top3_symbols"] = meta["top3_symbols"]
        row["top1_drop_pct"] = meta["top1_drop_pct"]
        row["reason_norm"] = normalize_reason(t.get("reason"))
        enriched.append(row)
    return enriched, unmatched


def summarize_bucket(rows: List[dict]) -> dict:
    trade_count = len(rows)
    pnl_sum = sum(float(r.get("pnl_pct", 0.0) or 0.0) for r in rows)
    pnl_avg = pnl_sum / trade_count if trade_count else 0.0
    win_count = sum(1 for r in rows if float(r.get("pnl_pct", 0.0) or 0.0) > 0)
    loss_count = sum(1 for r in rows if float(r.get("pnl_pct", 0.0) or 0.0) < 0)
    flat_count = trade_count - win_count - loss_count
    reason_counter = Counter(normalize_reason(r.get("reason")) for r in rows)

    summary = {
        "trade_count": trade_count,
        "pnl_sum_pct": round(pnl_sum, 6),
        "avg_pnl_pct": round(pnl_avg, 6),
        "win_count": win_count,
        "loss_count": loss_count,
        "flat_count": flat_count,
        "win_rate": round(win_count / trade_count, 6) if trade_count else 0.0,
        "loss_rate": round(loss_count / trade_count, 6) if trade_count else 0.0,
        "reason_counts": dict(reason_counter),
        "reason_rates": {
            k: round(reason_counter.get(k, 0) / trade_count, 6) if trade_count else 0.0
            for k in sorted(set(list(reason_counter.keys()) + EXPECTED_REASONS))
        },
    }
    return summary


def build_monthly(rows: List[dict]) -> List[dict]:
    bucket_month: Dict[Tuple[str, str], List[dict]] = defaultdict(list)
    for r in rows:
        m = month_from_bj(str(r.get("signal_time_bj", "")))
        bucket_month[(r["bucket"], m)].append(r)

    out: List[dict] = []
    for (bucket, month), group in sorted(bucket_month.items()):
        s = summarize_bucket(group)
        out.append({
            "bucket": bucket,
            "month": month,
            "trade_count": s["trade_count"],
            "pnl_sum_pct": s["pnl_sum_pct"],
            "avg_pnl_pct": s["avg_pnl_pct"],
            "win_rate": s["win_rate"],
            "take_profit": s["reason_counts"].get("TAKE_PROFIT", 0),
            "stop_loss": s["reason_counts"].get("STOP_LOSS", 0),
            "time_stop": s["reason_counts"].get("TIME_STOP", 0),
        })
    return out


def build_summary(candidate_rows: List[dict], enriched_trades: List[dict], unmatched_trades: List[dict], duplicate_groups: int, duplicate_extra_rows: int) -> dict:
    candidate_bucket_counts = Counter("S" if int(r["candidate_count"]) == 1 else "M" for r in candidate_rows)
    candidate_swallowed = {
        "S": 0,
        "M": sum(max(0, int(r["candidate_count"]) - 1) for r in candidate_rows if int(r["candidate_count"]) > 1),
    }

    s_rows = [r for r in enriched_trades if r["bucket"] == "S"]
    m_rows = [r for r in enriched_trades if r["bucket"] == "M"]

    summary = {
        "candidate_rounds": {
            "total": len(candidate_rows),
            "S": candidate_bucket_counts.get("S", 0),
            "M": candidate_bucket_counts.get("M", 0),
            "M_ratio": round(candidate_bucket_counts.get("M", 0) / len(candidate_rows), 6) if candidate_rows else 0.0,
            "M_swallowed_candidates_total": candidate_swallowed["M"],
            "duplicate_groups": duplicate_groups,
            "duplicate_extra_rows": duplicate_extra_rows,
        },
        "trade_join": {
            "matched_trade_count": len(enriched_trades),
            "unmatched_trade_count": len(unmatched_trades),
        },
        "bucket_trade_summary": {
            "S": summarize_bucket(s_rows),
            "M": summarize_bucket(m_rows),
        },
        "monthly_trade_summary": build_monthly(enriched_trades),
    }
    return summary


def write_trade_csv(rows: List[dict], out_path: Path) -> None:
    headers = [
        "bucket", "candidate_count", "symbol", "signal_time", "signal_time_bj",
        "entry_time", "entry_time_bj", "exit_time", "exit_time_bj",
        "entry_price", "exit_price", "pnl_pct", "reason", "bar_ts", "bar_bj",
        "top1_drop_pct", "top3_symbols"
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in sorted(rows, key=lambda x: (x["bucket"], int(x["signal_time"]), str(x["symbol"]))):
            writer.writerow({h: r.get(h, "") for h in headers})


def write_monthly_csv(rows: List[dict], out_path: Path) -> None:
    headers = [
        "bucket", "month", "trade_count", "pnl_sum_pct", "avg_pnl_pct", "win_rate",
        "take_profit", "stop_loss", "time_stop"
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in rows:
            writer.writerow({h: r.get(h, "") for h in headers})


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit S(single) vs M(multiple) trade outcomes from snapback candidate pool + sim trades.")
    ap.add_argument("--candidate-audit", required=True, help="Path to snapback_candidate_pool_audit.jsonl")
    ap.add_argument("--sim-trades", required=True, help="Path to sim_trades.*.jsonl")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    args = ap.parse_args()

    candidate_path = Path(args.candidate_audit)
    trades_path = Path(args.sim_trades)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    candidate_rows_raw = load_jsonl(candidate_path)
    candidate_rows, dup_samples, duplicate_groups, duplicate_extra_rows = dedupe_candidate_rows(candidate_rows_raw)
    trades = load_jsonl(trades_path)

    bucket_map = build_bucket_map(candidate_rows)
    enriched_trades, unmatched_trades = build_trade_rows(trades, bucket_map)
    summary = build_summary(candidate_rows, enriched_trades, unmatched_trades, duplicate_groups, duplicate_extra_rows)

    summary_path = out_dir / "candidate_bucket_trade_outcomes_summary.json"
    trades_csv_path = out_dir / "candidate_bucket_trade_outcomes_trades.csv"
    monthly_csv_path = out_dir / "candidate_bucket_trade_outcomes_monthly.csv"
    unmatched_path = out_dir / "candidate_bucket_trade_outcomes_unmatched.json"
    dup_path = out_dir / "candidate_bucket_trade_outcomes_duplicates.json"

    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    write_trade_csv(enriched_trades, trades_csv_path)
    write_monthly_csv(summary["monthly_trade_summary"], monthly_csv_path)
    unmatched_path.write_text(json.dumps(unmatched_trades, ensure_ascii=False, indent=2), encoding="utf-8")
    dup_path.write_text(json.dumps(dup_samples, ensure_ascii=False, indent=2), encoding="utf-8")

    s = summary["bucket_trade_summary"]["S"]
    m = summary["bucket_trade_summary"]["M"]
    print("=== candidate bucket trade outcomes audit done ===")
    print(f"candidate_audit         : {candidate_path}")
    print(f"sim_trades              : {trades_path}")
    print(f"candidate_rounds_total  : {summary['candidate_rounds']['total']}")
    print(f"S_rounds                : {summary['candidate_rounds']['S']}")
    print(f"M_rounds                : {summary['candidate_rounds']['M']}")
    print(f"M_swallowed_total       : {summary['candidate_rounds']['M_swallowed_candidates_total']}")
    print(f"matched_trade_count     : {summary['trade_join']['matched_trade_count']}")
    print(f"unmatched_trade_count   : {summary['trade_join']['unmatched_trade_count']}")
    print(f"S_trade_count           : {s['trade_count']}")
    print(f"S_pnl_sum_pct           : {s['pnl_sum_pct']}")
    print(f"S_avg_pnl_pct           : {s['avg_pnl_pct']}")
    print(f"M_trade_count           : {m['trade_count']}")
    print(f"M_pnl_sum_pct           : {m['pnl_sum_pct']}")
    print(f"M_avg_pnl_pct           : {m['avg_pnl_pct']}")
    print(f"summary_json            : {summary_path}")
    print(f"trades_csv              : {trades_csv_path}")
    print(f"monthly_csv             : {monthly_csv_path}")
    print(f"unmatched_json          : {unmatched_path}")
    print(f"duplicates_json         : {dup_path}")


if __name__ == "__main__":
    main()
