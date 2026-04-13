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
        by_bar[int(row["bar_ts"])].append(row)

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


def multiplicity_bucket(candidate_count: int) -> str:
    if candidate_count <= 0:
        raise RuntimeError(f"invalid candidate_count: {candidate_count}")
    if candidate_count == 1:
        return "S"
    if candidate_count == 2:
        return "M_2"
    if 3 <= candidate_count <= 5:
        return "M_3_5"
    if 6 <= candidate_count <= 10:
        return "M_6_10"
    return "M_GT_10"


def top_symbols(row: dict, topn: int) -> str:
    cands = row.get("candidates_sorted_by_drop_pct") or []
    return ", ".join(str(x.get("symbol")) for x in cands[:topn])


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
        signal_time = int(row["bar_ts"]) + 60000
        cc = int(row["candidate_count"])
        out[(symbol, signal_time)] = {
            "bucket": multiplicity_bucket(cc),
            "candidate_count": cc,
            "bar_ts": int(row["bar_ts"]),
            "bar_bj": row.get("bar_bj"),
            "top3_symbols": top_symbols(row, 3),
            "top5_symbols": top_symbols(row, 5),
            "top1_drop_pct": top1.get("drop_pct"),
        }
    return out


def normalize_reason(reason: str) -> str:
    r = str(reason or "").strip().upper()
    return r if r else "UNKNOWN"


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
        row.update(meta)
        row["reason_norm"] = normalize_reason(t.get("reason"))
        enriched.append(row)
    return enriched, unmatched


def summarize_bucket(rows: List[dict]) -> dict:
    trade_count = len(rows)
    pnl_sum = sum(float(r.get("pnl_pct", 0.0) or 0.0) for r in rows)
    win_count = sum(1 for r in rows if float(r.get("pnl_pct", 0.0) or 0.0) > 0)
    loss_count = sum(1 for r in rows if float(r.get("pnl_pct", 0.0) or 0.0) < 0)
    flat_count = trade_count - win_count - loss_count
    reason_counter = Counter(normalize_reason(r.get("reason")) for r in rows)
    avg_pnl = pnl_sum / trade_count if trade_count else 0.0
    return {
        "trade_count": trade_count,
        "pnl_sum_pct": round(pnl_sum, 6),
        "avg_pnl_pct": round(avg_pnl, 6),
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
    candidate_bucket_counts = Counter(multiplicity_bucket(int(r["candidate_count"])) for r in candidate_rows)
    swallowed_total = sum(max(0, int(r["candidate_count"]) - 1) for r in candidate_rows if int(r["candidate_count"]) > 1)

    ordered_buckets = ["S", "M_2", "M_3_5", "M_6_10", "M_GT_10"]
    bucket_rows = {b: [r for r in enriched_trades if r["bucket"] == b] for b in ordered_buckets}

    return {
        "candidate_rounds": {
            "total": len(candidate_rows),
            "bucket_counts": {b: candidate_bucket_counts.get(b, 0) for b in ordered_buckets},
            "duplicate_groups": duplicate_groups,
            "duplicate_extra_rows": duplicate_extra_rows,
            "swallowed_candidates_total": swallowed_total,
        },
        "trade_join": {
            "matched_trade_count": len(enriched_trades),
            "unmatched_trade_count": len(unmatched_trades),
        },
        "bucket_trade_summary": {
            b: summarize_bucket(bucket_rows[b]) for b in ordered_buckets
        },
        "monthly_trade_summary": build_monthly(enriched_trades),
    }


def write_trade_csv(rows: List[dict], out_path: Path) -> None:
    headers = [
        "bucket", "candidate_count", "symbol", "signal_time", "signal_time_bj",
        "entry_time", "entry_time_bj", "exit_time", "exit_time_bj",
        "entry_price", "exit_price", "pnl_pct", "reason", "bar_ts", "bar_bj",
        "top1_drop_pct", "top3_symbols", "top5_symbols"
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in sorted(rows, key=lambda x: (str(x["bucket"]), int(x["signal_time"]), str(x["symbol"]))):
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
    ap = argparse.ArgumentParser(description="Audit snapback trade outcomes by multiplicity buckets from candidate pool + sim trades.")
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
    monthly_rows = build_monthly(enriched_trades)

    summary_path = out_dir / "candidate_bucket_trade_outcomes_by_multiplicity_summary.json"
    trades_csv_path = out_dir / "candidate_bucket_trade_outcomes_by_multiplicity_trades.csv"
    monthly_csv_path = out_dir / "candidate_bucket_trade_outcomes_by_multiplicity_monthly.csv"
    unmatched_path = out_dir / "candidate_bucket_trade_outcomes_by_multiplicity_unmatched.json"
    dup_path = out_dir / "candidate_bucket_trade_outcomes_by_multiplicity_duplicates.json"

    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    write_trade_csv(enriched_trades, trades_csv_path)
    write_monthly_csv(monthly_rows, monthly_csv_path)
    unmatched_path.write_text(json.dumps(unmatched_trades, ensure_ascii=False, indent=2), encoding="utf-8")
    dup_path.write_text(json.dumps(dup_samples, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=== candidate bucket trade outcomes by multiplicity audit done ===")
    print(f"candidate_audit        : {candidate_path}")
    print(f"sim_trades             : {trades_path}")
    print(f"candidate_rounds_total : {summary['candidate_rounds']['total']}")
    print(f"bucket_counts          : {summary['candidate_rounds']['bucket_counts']}")
    print(f"matched_trade_count    : {summary['trade_join']['matched_trade_count']}")
    print(f"unmatched_trade_count  : {summary['trade_join']['unmatched_trade_count']}")
    print(f"summary_json           : {summary_path}")
    print(f"trades_csv             : {trades_csv_path}")
    print(f"monthly_csv            : {monthly_csv_path}")
    print(f"unmatched_json         : {unmatched_path}")
    print(f"duplicates_json        : {dup_path}")


if __name__ == "__main__":
    main()
