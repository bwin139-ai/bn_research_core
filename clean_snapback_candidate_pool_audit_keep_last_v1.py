#!/usr/bin/env python3
import argparse
import json
from collections import defaultdict
from pathlib import Path


def load_jsonl(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            row["_lineno"] = lineno
            rows.append(row)
    return rows


def write_jsonl(rows, path: Path):
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            payload = {k: v for k, v in row.items() if not str(k).startswith("_")}
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def main():
    ap = argparse.ArgumentParser(description="Dedup snapback_candidate_pool_audit.jsonl by bar_ts, keep last row")
    ap.add_argument("--input", required=True, help="Path to snapback_candidate_pool_audit.jsonl")
    ap.add_argument("--out-dir", required=True, help="Output dir")
    args = ap.parse_args()

    input_path = Path(args.input)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = load_jsonl(input_path)

    grouped = defaultdict(list)
    for row in rows:
        grouped[int(row["bar_ts"])].append(row)

    cleaned_rows = []
    duplicate_groups = 0
    duplicate_extra_rows = 0
    conflicts = []

    for bar_ts in sorted(grouped.keys()):
        group = grouped[bar_ts]
        chosen = max(group, key=lambda r: int(r["_lineno"]))  # 保留最后出现的一条
        cleaned_rows.append(chosen)

        if len(group) > 1:
            duplicate_groups += 1
            duplicate_extra_rows += len(group) - 1
            conflicts.append({
                "bar_ts": bar_ts,
                "bar_bj": chosen.get("bar_bj"),
                "row_count": len(group),
                "kept_lineno": int(chosen["_lineno"]),
                "dropped_linenos": [int(r["_lineno"]) for r in group if int(r["_lineno"]) != int(chosen["_lineno"])],
                "candidate_counts_seen": [int(r.get("candidate_count") or 0) for r in group],
            })

    cleaned_rows = sorted(cleaned_rows, key=lambda r: int(r["bar_ts"]))

    summary = {
        "input_path": str(input_path),
        "original_row_count": len(rows),
        "cleaned_row_count": len(cleaned_rows),
        "duplicate_groups": duplicate_groups,
        "duplicate_extra_rows": duplicate_extra_rows,
        "dedup_rule": "group by bar_ts, keep last row, drop earlier rows",
    }

    cleaned_path = out_dir / "snapback_candidate_pool_audit.cleaned_keep_last.jsonl"
    summary_path = out_dir / "snapback_candidate_pool_audit.cleaned_keep_last.summary.json"
    conflicts_path = out_dir / "snapback_candidate_pool_audit.cleaned_keep_last.conflicts.json"

    write_jsonl(cleaned_rows, cleaned_path)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    conflicts_path.write_text(json.dumps(conflicts, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=== candidate_pool_audit keep-last clean done ===")
    print(f"input                  : {input_path}")
    print(f"original_row_count     : {len(rows)}")
    print(f"cleaned_row_count      : {len(cleaned_rows)}")
    print(f"duplicate_groups       : {duplicate_groups}")
    print(f"duplicate_extra_rows   : {duplicate_extra_rows}")
    print(f"cleaned_jsonl          : {cleaned_path}")
    print(f"summary_json           : {summary_path}")
    print(f"conflicts_json         : {conflicts_path}")


if __name__ == "__main__":
    main()
