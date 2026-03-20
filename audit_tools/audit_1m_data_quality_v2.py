#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import json
import os
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

try:
    import pyarrow.parquet as pq
except Exception as e:
    raise SystemExit("Missing dependency: pyarrow. Install with: pip install -U pyarrow") from e

INTERVAL_MS = 60_000
BJT = timezone(timedelta(hours=8))
REQUIRED_CONTRACT_COLS = ["open_time_ms", "open", "high", "low", "close", "quote_asset_volume"]
IDX_COLS = ["high_idx", "low_idx", "close_idx"]
ALL_REQUIRED_COLS = REQUIRED_CONTRACT_COLS + IDX_COLS


@dataclass
class ContractGapSegment:
    symbol: str
    gap_start_bjt: str
    gap_end_bjt: str
    missing_rows: int
    prev_bar_bjt: str
    next_bar_bjt: str


@dataclass
class IdxMissingSegment:
    symbol: str
    idx_status: str
    seg_start_bjt: str
    seg_end_bjt: str
    missing_rows: int


@dataclass
class SchemaIssue:
    symbol: str
    file_path: str
    issue_type: str
    missing_columns: str
    detail: str


@dataclass
class SymbolStatus:
    symbol: str
    shard_count: int
    total_rows: int
    first_bar_bjt: str
    last_bar_bjt: str
    has_contract_gap: bool
    contract_gap_segments: int
    contract_gap_rows: int
    has_duplicate_ts: bool
    duplicate_ts_rows: int
    has_non_monotonic_ts: bool
    non_monotonic_pairs: int
    idx_status: str
    idx_missing_segments: int
    idx_missing_rows: int
    schema_issue_count: int
    stale_tail: bool
    severity: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit 1m contract continuity and idx data quality")
    p.add_argument("--data-dir", required=True, help="Root directory of per-symbol parquet shards")
    p.add_argument("--out-dir", required=True, help="Directory for audit outputs")
    p.add_argument("--tail-target-bjt", default="", help='Expected latest bar in Beijing time, e.g. "2026-03-20 23:59"')
    return p.parse_args()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def ms_to_bjt_str(ms: Optional[int]) -> str:
    if ms is None:
        return ""
    dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc).astimezone(BJT)
    return dt.strftime("%Y-%m-%d %H:%M")


def parse_tail_target_bjt(s: str) -> Optional[int]:
    s = (s or "").strip()
    if not s:
        return None
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=BJT)
    return int(dt.astimezone(timezone.utc).timestamp() * 1000)


def list_symbol_dirs(data_dir: str) -> List[str]:
    symbols = []
    for name in sorted(os.listdir(data_dir)):
        p = os.path.join(data_dir, name)
        if os.path.isdir(p):
            symbols.append(name)
    return symbols


def list_parquet_files(symbol_dir: str) -> List[str]:
    files = []
    for root, _, names in os.walk(symbol_dir):
        for n in names:
            if n.endswith(".parquet"):
                files.append(os.path.join(root, n))
    files.sort()
    return files


def load_symbol_rows(symbol: str, files: List[str]) -> Tuple[List[int], List[bool], List[SchemaIssue], int]:
    open_times: List[int] = []
    idx_row_complete: List[bool] = []
    issues: List[SchemaIssue] = []
    row_count = 0

    for fp in files:
        try:
            pf = pq.ParquetFile(fp)
            schema_names = list(pf.schema_arrow.names)
        except Exception as e:
            issues.append(SchemaIssue(symbol, fp, "PARQUET_READ_ERROR", "", str(e)))
            continue

        missing_contract = [c for c in REQUIRED_CONTRACT_COLS if c not in schema_names]
        missing_idx = [c for c in IDX_COLS if c not in schema_names]
        if missing_contract or missing_idx:
            issue_type = "SCHEMA_MISSING_COLUMNS"
            issues.append(
                SchemaIssue(
                    symbol=symbol,
                    file_path=fp,
                    issue_type=issue_type,
                    missing_columns=",".join(missing_contract + missing_idx),
                    detail=f"schema={','.join(schema_names)}",
                )
            )

        try:
            cols_to_read = [c for c in ALL_REQUIRED_COLS if c in schema_names]
            tbl = pq.read_table(fp, columns=cols_to_read)
        except Exception as e:
            issues.append(SchemaIssue(symbol, fp, "PARQUET_TABLE_READ_ERROR", "", str(e)))
            continue

        names = set(tbl.column_names)
        if "open_time_ms" not in names:
            continue

        ot = tbl.column("open_time_ms").to_pylist()
        n = len(ot)
        row_count += n
        hi = tbl.column("high_idx").to_pylist() if "high_idx" in names else [None] * n
        lo = tbl.column("low_idx").to_pylist() if "low_idx" in names else [None] * n
        cl = tbl.column("close_idx").to_pylist() if "close_idx" in names else [None] * n

        open_times.extend(int(x) for x in ot if x is not None)
        idx_row_complete.extend([(hi[i] is not None and lo[i] is not None and cl[i] is not None) for i in range(n)])

    return open_times, idx_row_complete, issues, row_count


def build_contract_gaps(symbol: str, sorted_times: List[int]) -> Tuple[List[ContractGapSegment], int, int, int]:
    if not sorted_times:
        return [], 0, 0, 0
    gaps: List[ContractGapSegment] = []
    duplicate_rows = 0
    non_mono = 0

    for prev_ms, next_ms in zip(sorted_times[:-1], sorted_times[1:]):
        delta = next_ms - prev_ms
        if delta == 0:
            duplicate_rows += 1
        elif delta < 0:
            non_mono += 1
        elif delta > INTERVAL_MS:
            missing_rows = (delta // INTERVAL_MS) - 1
            gap_start = prev_ms + INTERVAL_MS
            gap_end = next_ms - INTERVAL_MS
            gaps.append(
                ContractGapSegment(
                    symbol=symbol,
                    gap_start_bjt=ms_to_bjt_str(gap_start),
                    gap_end_bjt=ms_to_bjt_str(gap_end),
                    missing_rows=int(missing_rows),
                    prev_bar_bjt=ms_to_bjt_str(prev_ms),
                    next_bar_bjt=ms_to_bjt_str(next_ms),
                )
            )
    return gaps, duplicate_rows, non_mono, sum(g.missing_rows for g in gaps)


def build_idx_segments(symbol: str, open_times_sorted: List[int], idx_complete_sorted: List[bool], idx_status: str) -> Tuple[List[IdxMissingSegment], int]:
    segments: List[IdxMissingSegment] = []
    missing_rows_total = 0
    if not open_times_sorted or not idx_complete_sorted or len(open_times_sorted) != len(idx_complete_sorted):
        return segments, missing_rows_total

    start_i = None
    for i, ok in enumerate(idx_complete_sorted):
        if not ok and start_i is None:
            start_i = i
        elif ok and start_i is not None:
            seg_start_ms = open_times_sorted[start_i]
            seg_end_ms = open_times_sorted[i - 1]
            rows = i - start_i
            segments.append(IdxMissingSegment(symbol, idx_status, ms_to_bjt_str(seg_start_ms), ms_to_bjt_str(seg_end_ms), rows))
            missing_rows_total += rows
            start_i = None
    if start_i is not None:
        seg_start_ms = open_times_sorted[start_i]
        seg_end_ms = open_times_sorted[-1]
        rows = len(open_times_sorted) - start_i
        segments.append(IdxMissingSegment(symbol, idx_status, ms_to_bjt_str(seg_start_ms), ms_to_bjt_str(seg_end_ms), rows))
        missing_rows_total += rows
    return segments, missing_rows_total


def classify_idx_status(row_count: int, idx_complete_count: int, schema_issues: List[SchemaIssue]) -> str:
    schema_missing_idx = any((iss.issue_type == "SCHEMA_MISSING_COLUMNS" and any(col in iss.missing_columns.split(",") for col in IDX_COLS)) for iss in schema_issues)
    if row_count == 0:
        return "NO_ROWS"
    if idx_complete_count == row_count:
        return "FULL"
    if idx_complete_count == 0:
        return "SCHEMA_MISSING" if schema_missing_idx else "ALL_MISSING"
    return "PARTIAL_MISSING"


def write_csv(path: str, rows: List[dict], fieldnames: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def main() -> None:
    args = parse_args()
    ensure_dir(args.out_dir)
    tail_target_ms = parse_tail_target_bjt(args.tail_target_bjt)

    symbols = list_symbol_dirs(args.data_dir)
    symbol_status_rows: List[dict] = []
    contract_gap_rows: List[dict] = []
    idx_missing_rows: List[dict] = []
    schema_issue_rows: List[dict] = []

    summary = {
        "data_dir": args.data_dir,
        "out_dir": args.out_dir,
        "symbols_audited": len(symbols),
        "tail_target_bjt": args.tail_target_bjt,
        "classification_counts": {},
        "severity_counts": {},
        "symbols_with_contract_gaps": 0,
        "symbols_with_duplicate_ts": 0,
        "symbols_with_non_monotonic_ts": 0,
        "symbols_with_schema_issues": 0,
        "symbols_stale_tail": 0,
        "symbols_all_missing_idx": [],
        "symbols_partial_missing_idx": [],
        "symbols_with_other_problems": [],
    }

    for symbol in symbols:
        files = list_parquet_files(os.path.join(args.data_dir, symbol))
        open_times, idx_complete, issues, row_count = load_symbol_rows(symbol, files)

        pairs = list(zip(open_times, idx_complete))
        pairs.sort(key=lambda x: x[0])
        sorted_times = [x[0] for x in pairs]
        sorted_idx_complete = [x[1] for x in pairs]

        gaps, duplicate_rows, non_mono, gap_rows = build_contract_gaps(symbol, sorted_times)
        idx_complete_count = sum(1 for x in sorted_idx_complete if x)
        idx_status = classify_idx_status(row_count=len(sorted_times), idx_complete_count=idx_complete_count, schema_issues=issues)
        idx_segments, idx_missing_total = build_idx_segments(symbol, sorted_times, sorted_idx_complete, idx_status)

        first_ms = sorted_times[0] if sorted_times else None
        last_ms = sorted_times[-1] if sorted_times else None
        stale_tail = bool(tail_target_ms is not None and last_ms is not None and last_ms < tail_target_ms)

        has_schema_issues = len(issues) > 0
        has_contract_gap = len(gaps) > 0
        has_duplicate_ts = duplicate_rows > 0
        has_non_monotonic = non_mono > 0

        severity = "OK"
        if has_contract_gap or has_duplicate_ts or has_non_monotonic or idx_status in {"ALL_MISSING", "SCHEMA_MISSING", "NO_ROWS"} or has_schema_issues:
            severity = "FATAL"
        elif idx_status == "PARTIAL_MISSING" or stale_tail:
            severity = "WARNING"

        status = SymbolStatus(
            symbol=symbol,
            shard_count=len(files),
            total_rows=len(sorted_times),
            first_bar_bjt=ms_to_bjt_str(first_ms),
            last_bar_bjt=ms_to_bjt_str(last_ms),
            has_contract_gap=has_contract_gap,
            contract_gap_segments=len(gaps),
            contract_gap_rows=gap_rows,
            has_duplicate_ts=has_duplicate_ts,
            duplicate_ts_rows=duplicate_rows,
            has_non_monotonic_ts=has_non_monotonic,
            non_monotonic_pairs=non_mono,
            idx_status=idx_status,
            idx_missing_segments=len(idx_segments),
            idx_missing_rows=idx_missing_total,
            schema_issue_count=len(issues),
            stale_tail=stale_tail,
            severity=severity,
        )
        symbol_status_rows.append(asdict(status))
        contract_gap_rows.extend(asdict(x) for x in gaps)
        idx_missing_rows.extend(asdict(x) for x in idx_segments)
        schema_issue_rows.extend(asdict(x) for x in issues)

        summary["classification_counts"][idx_status] = summary["classification_counts"].get(idx_status, 0) + 1
        summary["severity_counts"][severity] = summary["severity_counts"].get(severity, 0) + 1
        if has_contract_gap:
            summary["symbols_with_contract_gaps"] += 1
        if has_duplicate_ts:
            summary["symbols_with_duplicate_ts"] += 1
        if has_non_monotonic:
            summary["symbols_with_non_monotonic_ts"] += 1
        if has_schema_issues:
            summary["symbols_with_schema_issues"] += 1
        if stale_tail:
            summary["symbols_stale_tail"] += 1
        if idx_status == "ALL_MISSING":
            summary["symbols_all_missing_idx"].append(symbol)
        elif idx_status == "PARTIAL_MISSING":
            summary["symbols_partial_missing_idx"].append(symbol)
        if severity != "OK" and idx_status not in {"ALL_MISSING", "PARTIAL_MISSING"}:
            summary["symbols_with_other_problems"].append(symbol)

    write_csv(
        os.path.join(args.out_dir, "symbol_status.csv"),
        symbol_status_rows,
        [
            "symbol", "shard_count", "total_rows", "first_bar_bjt", "last_bar_bjt",
            "has_contract_gap", "contract_gap_segments", "contract_gap_rows",
            "has_duplicate_ts", "duplicate_ts_rows",
            "has_non_monotonic_ts", "non_monotonic_pairs",
            "idx_status", "idx_missing_segments", "idx_missing_rows",
            "schema_issue_count", "stale_tail", "severity",
        ],
    )
    write_csv(
        os.path.join(args.out_dir, "contract_gap_segments.csv"),
        contract_gap_rows,
        ["symbol", "gap_start_bjt", "gap_end_bjt", "missing_rows", "prev_bar_bjt", "next_bar_bjt"],
    )
    write_csv(
        os.path.join(args.out_dir, "idx_missing_segments.csv"),
        idx_missing_rows,
        ["symbol", "idx_status", "seg_start_bjt", "seg_end_bjt", "missing_rows"],
    )
    write_csv(
        os.path.join(args.out_dir, "schema_issues.csv"),
        schema_issue_rows,
        ["symbol", "file_path", "issue_type", "missing_columns", "detail"],
    )
    with open(os.path.join(args.out_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("=" * 80)
    print("1m 数据质量审计完成")
    print("=" * 80)
    print(f"data_dir                 : {args.data_dir}")
    print(f"out_dir                  : {args.out_dir}")
    print(f"symbols_audited          : {len(symbols)}")
    print(f"tail_target_bjt          : {args.tail_target_bjt or '(not set)'}")
    print("classification_counts    :")
    for k, v in sorted(summary["classification_counts"].items()):
        print(f"  - {k}: {v}")
    print("severity_counts          :")
    for k, v in sorted(summary["severity_counts"].items()):
        print(f"  - {k}: {v}")
    print(f"symbols_with_contract_gaps     : {summary['symbols_with_contract_gaps']}")
    print(f"symbols_with_duplicate_ts      : {summary['symbols_with_duplicate_ts']}")
    print(f"symbols_with_non_monotonic_ts  : {summary['symbols_with_non_monotonic_ts']}")
    print(f"symbols_with_schema_issues     : {summary['symbols_with_schema_issues']}")
    print(f"symbols_stale_tail             : {summary['symbols_stale_tail']}")
    print(f"all_missing_idx_symbols        : {len(summary['symbols_all_missing_idx'])}")
    print(f"partial_missing_idx_symbols    : {len(summary['symbols_partial_missing_idx'])}")


if __name__ == "__main__":
    main()
