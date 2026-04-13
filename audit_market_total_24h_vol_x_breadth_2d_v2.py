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


def build_candidate_rows_map(candidate_audit_rows: List[dict]) -> Dict[int, List[dict]]:
    out: Dict[int, List[dict]] = defaultdict(list)
    for row in candidate_audit_rows:
        signal_time = int(row["bar_ts"])
        item = dict(row)
        item["candidate_count"] = int(item["candidate_count"])
        symbols = []
        for c in item.get("candidates_sorted_by_drop_pct") or []:
            sym = str(c.get("symbol") or "").upper().strip()
            if sym:
                symbols.append(sym)
        item["_candidate_symbols"] = symbols
        out[signal_time].append(item)
    return dict(out)


def resolve_candidate_row_for_trade(rows: List[dict], trade_symbol: str) -> Tuple[dict, str]:
    symbol_u = str(trade_symbol).upper().strip()
    if len(rows) == 1:
        return rows[0], "single_row"

    containing = [r for r in rows if symbol_u in set(r.get("_candidate_symbols") or [])]
    if len(containing) == 1:
        return containing[0], "matched_symbol_unique"

    if containing:
        counts = {int(r["candidate_count"]) for r in containing}
        if len(counts) == 1:
            return containing[0], "matched_symbol_same_count"
        # 倾向保守：同一 trade symbol 出现在多条冲突记录里时，取 candidate_count 更大的那条
        containing_sorted = sorted(
            containing,
            key=lambda r: (
                -int(r["candidate_count"]),
                -len(r.get("_candidate_symbols") or []),
            )
        )
        return containing_sorted[0], "matched_symbol_conflict_choose_max_count"

    # 如果 trade_symbol 不在任何冲突记录里，按 candidate_count 众数取；再按更大 count 兜底
    count_counter = Counter(int(r["candidate_count"]) for r in rows)
    modal_count, _ = sorted(count_counter.items(), key=lambda kv: (-kv[1], -kv[0]))[0]
    modal_rows = [r for r in rows if int(r["candidate_count"]) == int(modal_count)]
    if len(modal_rows) == 1:
        return modal_rows[0], "modal_count_unique"
    modal_rows = sorted(modal_rows, key=lambda r: -len(r.get("_candidate_symbols") or []))
    return modal_rows[0], "modal_count_tie_choose_richest"


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
        "candidate_resolution_method",
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in headers})


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
    candidate_rows_map = build_candidate_rows_map(candidate_audit_rows)

    enriched_rows: List[dict] = []
    missing_candidate_count_rows: List[dict] = []
    conflicted_signal_times: Dict[int, dict] = {}

    for row in trade_rows:
        signal_time = int(row["signal_time"])
        candidate_rows = candidate_rows_map.get(signal_time)
        if not candidate_rows:
            missing_candidate_count_rows.append({
                "signal_time": signal_time,
                "signal_time_bj": row.get("signal_time_bj"),
                "symbol": row.get("symbol"),
            })
            continue

        chosen_row, resolution_method = resolve_candidate_row_for_trade(candidate_rows, str(row.get("symbol") or ""))
        if len(candidate_rows) > 1:
            conflicted_signal_times[signal_time] = {
                "signal_time": signal_time,
                "signal_time_bj": row.get("signal_time_bj"),
                "trade_symbol": row.get("symbol"),
                "resolution_method": resolution_method,
                "candidate_counts_seen": sorted({int(x["candidate_count"]) for x in candidate_rows}),
                "row_count": len(candidate_rows),
            }

        item = dict(row)
        item["candidate_count"] = int(chosen_row["candidate_count"])
        item["market_vol_bucket"] = market_vol_bucket(float(item["market_total_24h_vol"]))
        item["breadth_bucket"] = breadth_bucket(int(item["candidate_count"]))
        item["candidate_resolution_method"] = resolution_method
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
        "conflicted_signal_time_count": len(conflicted_signal_times),
        "resolution_method_counts": dict(Counter(str(r.get("candidate_resolution_method") or "") for r in enriched_rows)),
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
        "conflicted_signal_times_preview": list(sorted(conflicted_signal_times.values(), key=lambda x: x["signal_time"]))[:50],
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
    print(f"conflicted_signal_time_count    : {len(conflicted_signal_times)}")
    print(f"summary_json                    : {summary_path}")
    print(f"trades_csv                      : {trades_csv_path}")


if __name__ == "__main__":
    main()
