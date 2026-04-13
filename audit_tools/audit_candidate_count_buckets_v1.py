#!/usr/bin/env python3
import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, List, Tuple


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


def dedupe_by_bar_ts(rows: List[dict]) -> Tuple[List[dict], List[dict], int, int]:
    seen: Dict[int, dict] = {}
    dup_samples: List[dict] = []
    duplicate_groups = 0
    duplicate_extra_rows = 0

    by_bar: Dict[int, List[dict]] = defaultdict(list)
    for row in rows:
        by_bar[int(row["bar_ts"])].append(row)

    for bar_ts, group in sorted(by_bar.items()):
        if len(group) > 1:
            duplicate_groups += 1
            duplicate_extra_rows += len(group) - 1
            first = group[0]
            identical = all(x == first for x in group[1:])
            dup_samples.append({
                "bar_ts": bar_ts,
                "bar_bj": first.get("bar_bj"),
                "duplicate_rows": len(group),
                "all_rows_identical": identical,
            })
        seen[bar_ts] = group[0]

    unique_rows = [seen[k] for k in sorted(seen.keys())]
    return unique_rows, dup_samples, duplicate_groups, duplicate_extra_rows


def bucket_label(n: int) -> str:
    if n == 1:
        return "eq_1"
    if n == 2:
        return "eq_2"
    if 3 <= n <= 5:
        return "3_to_5"
    if 6 <= n <= 10:
        return "6_to_10"
    return "gt_10"


def top3_symbols(row: dict) -> str:
    cands = row.get("candidates_sorted_by_drop_pct") or []
    return ", ".join(str(x.get("symbol")) for x in cands[:3])


def month_from_bar_bj(bar_bj: str) -> str:
    return str(bar_bj)[:7]


def build_summary(rows: List[dict], original_count: int, duplicate_groups: int, duplicate_extra_rows: int) -> dict:
    total_unique = len(rows)
    count_eq_1 = sum(1 for r in rows if int(r["candidate_count"]) == 1)
    count_gt_1 = sum(1 for r in rows if int(r["candidate_count"]) > 1)
    swallowed = sum(max(0, int(r["candidate_count"]) - 1) for r in rows if int(r["candidate_count"]) > 1)

    bucket_counter = Counter(bucket_label(int(r["candidate_count"])) for r in rows)
    monthly_counter: Dict[str, Counter] = defaultdict(Counter)
    for r in rows:
        m = month_from_bar_bj(r["bar_bj"])
        n = int(r["candidate_count"])
        monthly_counter[m]["total_rounds"] += 1
        monthly_counter[m]["eq_1"] += int(n == 1)
        monthly_counter[m]["gt_1"] += int(n > 1)
        monthly_counter[m]["swallowed_candidates"] += max(0, n - 1)

    top_multi = sorted(
        [r for r in rows if int(r["candidate_count"]) > 1],
        key=lambda x: (-int(x["candidate_count"]), int(x["bar_ts"]))
    )

    return {
        "original_row_count": original_count,
        "deduped_row_count": total_unique,
        "duplicate_groups": duplicate_groups,
        "duplicate_extra_rows": duplicate_extra_rows,
        "candidate_count_eq_1_rounds": count_eq_1,
        "candidate_count_gt_1_rounds": count_gt_1,
        "candidate_count_gt_1_ratio": round(count_gt_1 / total_unique, 6) if total_unique else 0.0,
        "swallowed_candidates_total": swallowed,
        "bucket_counts": dict(bucket_counter),
        "monthly_counts": [
            {
                "month": m,
                **dict(monthly_counter[m]),
            }
            for m in sorted(monthly_counter.keys())
        ],
        "top_multi_rounds": [
            {
                "bar_ts": int(r["bar_ts"]),
                "bar_bj": r["bar_bj"],
                "candidate_count": int(r["candidate_count"]),
                "top3_symbols": top3_symbols(r),
            }
            for r in top_multi[:30]
        ],
    }


def write_csv(rows: List[dict], out_path: Path) -> None:
    headers = [
        "bar_ts", "bar_bj", "candidate_count", "bucket",
        "top1_symbol", "top1_drop_pct", "top3_symbols"
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for r in sorted(rows, key=lambda x: int(x["bar_ts"])):
            cands = r.get("candidates_sorted_by_drop_pct") or []
            top1 = cands[0] if cands else {}
            writer.writerow({
                "bar_ts": int(r["bar_ts"]),
                "bar_bj": r["bar_bj"],
                "candidate_count": int(r["candidate_count"]),
                "bucket": bucket_label(int(r["candidate_count"])),
                "top1_symbol": top1.get("symbol", ""),
                "top1_drop_pct": top1.get("drop_pct", ""),
                "top3_symbols": top3_symbols(r),
            })


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit snapback candidate_count buckets from candidate pool audit JSONL.")
    ap.add_argument("--input", required=True, help="Path to snapback_candidate_pool_audit.jsonl")
    ap.add_argument("--out-dir", required=True, help="Directory for outputs")
    args = ap.parse_args()

    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = load_jsonl(input_path)
    unique_rows, dup_samples, duplicate_groups, duplicate_extra_rows = dedupe_by_bar_ts(rows)
    summary = build_summary(unique_rows, len(rows), duplicate_groups, duplicate_extra_rows)

    summary_path = out_dir / "candidate_count_bucket_summary.json"
    csv_path = out_dir / "candidate_count_bucket_rounds.csv"
    dup_path = out_dir / "candidate_count_bucket_duplicates.json"

    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    write_csv(unique_rows, csv_path)
    dup_path.write_text(json.dumps(dup_samples, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=== candidate_count buckets audit done ===")
    print(f"input                 : {input_path}")
    print(f"original_row_count    : {len(rows)}")
    print(f"deduped_row_count     : {len(unique_rows)}")
    print(f"duplicate_groups      : {duplicate_groups}")
    print(f"duplicate_extra_rows  : {duplicate_extra_rows}")
    print(f"eq_1_rounds           : {summary['candidate_count_eq_1_rounds']}")
    print(f"gt_1_rounds           : {summary['candidate_count_gt_1_rounds']}")
    print(f"gt_1_ratio            : {summary['candidate_count_gt_1_ratio']}")
    print(f"swallowed_total       : {summary['swallowed_candidates_total']}")
    print(f"summary_json          : {summary_path}")
    print(f"rounds_csv            : {csv_path}")
    print(f"duplicates_json       : {dup_path}")


if __name__ == "__main__":
    main()
