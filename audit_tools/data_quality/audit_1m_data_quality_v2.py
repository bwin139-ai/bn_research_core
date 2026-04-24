#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import pyarrow.parquet as pq
except Exception as e:
    raise SystemExit("Missing dependency: pyarrow. Install with: pip install -U pyarrow") from e

INTERVAL_MS = 60_000
HOUR_MS = 3_600_000
BJT = timezone(timedelta(hours=8))
REQUIRED_CONTRACT_COLS = ["open_time_ms", "open", "high", "low", "close", "quote_asset_volume"]
IDX_COLS = ["high_idx", "low_idx", "close_idx"]
ALL_REQUIRED_COLS = REQUIRED_CONTRACT_COLS + IDX_COLS
RECENT_COMPLETE_TAIL_MIN_ROWS = 1440
BASELINE_VERSION = 1
BASELINE_POLICY_NORMAL = "NORMAL"
BASELINE_POLICY_IDX_PREFIX_OK = "IDX_PREFIX_OK"
BASELINE_POLICY_PRE_MARKET_NO_IDX = "PRE_MARKET_NO_IDX"
BASELINE_POLICY_REVIEW_REQUIRED = "REVIEW_REQUIRED"


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
    first_bar_ms: Optional[int]
    last_bar_ms: Optional[int]
    first_bar_bjt: str
    last_bar_bjt: str
    first_idx_bar_ms: Optional[int]
    last_idx_bar_ms: Optional[int]
    first_idx_bar_bjt: str
    last_idx_bar_bjt: str
    idx_complete_rows: int
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
    recent_complete_tail_rows: int
    recent_complete_tail_hours: float
    historical_idx_only_normalized: bool
    idx_prefix_only_normalized: bool
    stale_tail: bool
    tail_lag_hours: float
    suspected_delisted: bool
    confirmed_delisted: bool
    exclude_from_formal_universe: bool
    exclude_reason: str
    baseline_policy: str
    severity: str


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit 1m contract continuity and idx data quality")
    p.add_argument("--data-dir", required=True, help="Root directory of per-symbol parquet shards")
    p.add_argument("--out-dir", required=True, help="Directory for audit outputs")
    p.add_argument("--tail-target-bjt", default="", help='Expected latest bar in Beijing time, e.g. "2026-03-20 23:59"')
    p.add_argument(
        "--delisted-threshold-hours",
        type=int,
        default=72,
        help="If tail lag >= this threshold, list symbol as suspected delisted (default: 72)",
    )
    p.add_argument(
        "--confirmed-delisted-file",
        default="",
        help="Optional txt file, one symbol per line, for manually confirmed delisted symbols",
    )
    p.add_argument(
        "--audit-mode",
        choices=["full", "incremental"],
        default="full",
        help="full: audit all history; incremental: only audit rows after baseline watermark",
    )
    p.add_argument(
        "--baseline-file",
        default="",
        help="Optional baseline watermark json. Required for incremental mode.",
    )
    p.add_argument(
        "--write-baseline-file",
        default="",
        help="Optional output path to write/update a klines_1m audit baseline after this run.",
    )
    p.add_argument(
        "--baseline-pre-market-symbols",
        default="",
        help="Optional comma-separated symbols to label as PRE_MARKET_NO_IDX in the written baseline.",
    )
    p.add_argument(
        "--baseline-note",
        default="",
        help="Optional free-form note written into the baseline file.",
    )
    return p.parse_args()


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def ensure_parent(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


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


def load_symbol_set(path: str) -> Set[str]:
    path = (path or "").strip()
    if not path:
        return set()
    out: Set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            s = raw.strip()
            if not s or s.startswith("#"):
                continue
            out.add(s.upper())
    return out


def parse_symbol_csv(raw: str) -> Set[str]:
    items = set()
    for item in (raw or "").split(","):
        s = item.strip().upper()
        if s:
            items.add(s)
    return items


def load_baseline(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    symbols = payload.get("symbols")
    if not isinstance(symbols, dict):
        raise ValueError(f"baseline file missing symbols map: {path}")
    return payload


def baseline_entry_for_symbol(baseline: Optional[Dict[str, Any]], symbol: str) -> Optional[Dict[str, Any]]:
    if not baseline:
        return None
    entry = (baseline.get("symbols") or {}).get(str(symbol).upper())
    return entry if isinstance(entry, dict) else None


def load_symbol_rows(
    symbol: str,
    files: List[str],
    *,
    min_open_time_ms: Optional[int] = None,
) -> Tuple[List[int], List[bool], List[SchemaIssue], int]:
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
            issues.append(
                SchemaIssue(
                    symbol=symbol,
                    file_path=fp,
                    issue_type="SCHEMA_MISSING_COLUMNS",
                    missing_columns=",".join(missing_contract + missing_idx),
                    detail=f"schema={','.join(schema_names)}",
                )
            )

        try:
            cols_to_read = [c for c in ALL_REQUIRED_COLS if c in schema_names]
            filters = [("open_time_ms", ">=", int(min_open_time_ms))] if min_open_time_ms is not None and "open_time_ms" in schema_names else None
            tbl = pq.read_table(fp, columns=cols_to_read, filters=filters)
        except Exception as e:
            issues.append(SchemaIssue(symbol, fp, "PARQUET_TABLE_READ_ERROR", "", str(e)))
            continue

        names = set(tbl.column_names)
        if "open_time_ms" not in names:
            continue

        ot = tbl.column("open_time_ms").to_pylist()
        n = len(ot)
        if n <= 0:
            continue
        row_count += n
        hi = tbl.column("high_idx").to_pylist() if "high_idx" in names else [None] * n
        lo = tbl.column("low_idx").to_pylist() if "low_idx" in names else [None] * n
        cl = tbl.column("close_idx").to_pylist() if "close_idx" in names else [None] * n

        for i in range(n):
            t = ot[i]
            if t is None:
                continue
            open_times.append(int(t))
            idx_row_complete.append(hi[i] is not None and lo[i] is not None and cl[i] is not None)

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
    schema_missing_idx = any(
        iss.issue_type == "SCHEMA_MISSING_COLUMNS" and any(col in iss.missing_columns.split(",") for col in IDX_COLS)
        for iss in schema_issues
    )
    if row_count == 0:
        return "NO_ROWS"
    if idx_complete_count == row_count:
        return "FULL"
    if idx_complete_count == 0:
        return "SCHEMA_MISSING" if schema_missing_idx else "ALL_MISSING"
    return "PARTIAL_MISSING"


def compute_recent_complete_tail(open_times_sorted: List[int], idx_complete_sorted: List[bool]) -> Tuple[int, float]:
    if not open_times_sorted or not idx_complete_sorted or len(open_times_sorted) != len(idx_complete_sorted):
        return 0, 0.0

    i = len(idx_complete_sorted) - 1
    while i >= 0 and idx_complete_sorted[i]:
        i -= 1

    trailing_rows = len(idx_complete_sorted) - 1 - i
    if trailing_rows <= 1:
        return trailing_rows, 0.0
    trailing_hours = round((open_times_sorted[-1] - open_times_sorted[i + 1]) / HOUR_MS, 2)
    return trailing_rows, trailing_hours


def first_last_complete_idx_ms(open_times_sorted: List[int], idx_complete_sorted: List[bool]) -> Tuple[Optional[int], Optional[int], int]:
    idx_times = [ms for ms, ok in zip(open_times_sorted, idx_complete_sorted) if ok]
    if not idx_times:
        return None, None, 0
    return idx_times[0], idx_times[-1], len(idx_times)


def is_idx_prefix_only_normalized(open_times_sorted: List[int], idx_complete_sorted: List[bool]) -> bool:
    if not open_times_sorted or not idx_complete_sorted or len(open_times_sorted) != len(idx_complete_sorted):
        return False
    try:
        first_ok_idx = idx_complete_sorted.index(True)
    except ValueError:
        return False
    return all(not ok for ok in idx_complete_sorted[:first_ok_idx]) and all(idx_complete_sorted[first_ok_idx:])


def write_csv(path: str, rows: List[dict], fieldnames: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_symbol_txt(path: str, symbols: List[str]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for s in sorted(set(symbols)):
            f.write(s + "\n")


def baseline_policy_for_symbol(status: SymbolStatus, pre_market_symbols: Set[str]) -> str:
    symbol = str(status.symbol).upper()
    if symbol in pre_market_symbols:
        return BASELINE_POLICY_PRE_MARKET_NO_IDX
    if status.idx_status == "FULL" and status.severity == "OK":
        return BASELINE_POLICY_NORMAL
    if status.idx_prefix_only_normalized and status.severity == "OK":
        return BASELINE_POLICY_IDX_PREFIX_OK
    return BASELINE_POLICY_REVIEW_REQUIRED


def write_baseline_file(
    path: str,
    *,
    args: argparse.Namespace,
    symbol_statuses: List[SymbolStatus],
    pre_market_symbols: Set[str],
) -> None:
    ensure_parent(path)
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    payload = {
        "baseline_version": BASELINE_VERSION,
        "created_utc_ms": now_ms,
        "created_bjt": ms_to_bjt_str(now_ms),
        "data_dir": args.data_dir,
        "source_out_dir": args.out_dir,
        "source_audit_mode": args.audit_mode,
        "tail_target_bjt": args.tail_target_bjt,
        "note": args.baseline_note,
        "pre_market_symbols": sorted(pre_market_symbols),
        "symbols": {},
    }
    for status in sorted(symbol_statuses, key=lambda x: x.symbol):
        policy = baseline_policy_for_symbol(status, pre_market_symbols)
        payload["symbols"][status.symbol.upper()] = {
            "policy": policy,
            "severity": status.severity,
            "idx_status": status.idx_status,
            "idx_prefix_only_normalized": bool(status.idx_prefix_only_normalized),
            "historical_idx_only_normalized": bool(status.historical_idx_only_normalized),
            "last_contract_open_time_ms": status.last_bar_ms,
            "last_contract_open_time_bjt": status.last_bar_bjt,
            "last_idx_open_time_ms": status.last_idx_bar_ms,
            "last_idx_open_time_bjt": status.last_idx_bar_bjt,
            "first_idx_open_time_ms": status.first_idx_bar_ms,
            "first_idx_open_time_bjt": status.first_idx_bar_bjt,
            "schema_issue_count": int(status.schema_issue_count),
            "contract_gap_rows": int(status.contract_gap_rows),
            "duplicate_ts_rows": int(status.duplicate_ts_rows),
            "non_monotonic_pairs": int(status.non_monotonic_pairs),
        }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def main() -> None:
    args = parse_args()
    ensure_dir(args.out_dir)
    tail_target_ms = parse_tail_target_bjt(args.tail_target_bjt)
    confirmed_delisted = load_symbol_set(args.confirmed_delisted_file)
    pre_market_symbols = parse_symbol_csv(args.baseline_pre_market_symbols)
    baseline_payload: Optional[Dict[str, Any]] = None
    if args.audit_mode == "incremental":
        if not args.baseline_file.strip():
            raise SystemExit("--baseline-file is required when --audit-mode=incremental")
        baseline_payload = load_baseline(args.baseline_file)
    elif args.baseline_file.strip() and os.path.exists(args.baseline_file):
        baseline_payload = load_baseline(args.baseline_file)

    symbols = list_symbol_dirs(args.data_dir)
    symbol_status_rows: List[dict] = []
    contract_gap_rows: List[dict] = []
    idx_missing_rows: List[dict] = []
    schema_issue_rows: List[dict] = []
    symbol_statuses: List[SymbolStatus] = []

    summary: Dict[str, Any] = {
        "data_dir": args.data_dir,
        "out_dir": args.out_dir,
        "audit_mode": args.audit_mode,
        "baseline_file": args.baseline_file,
        "symbols_discovered": len(symbols),
        "symbols_audited": 0,
        "symbols_skipped_no_new_rows": 0,
        "tail_target_bjt": args.tail_target_bjt,
        "delisted_threshold_hours": args.delisted_threshold_hours,
        "confirmed_delisted_file": args.confirmed_delisted_file,
        "classification_counts": {},
        "severity_counts": {},
        "symbols_with_contract_gaps": 0,
        "symbols_with_duplicate_ts": 0,
        "symbols_with_non_monotonic_ts": 0,
        "symbols_with_schema_issues": 0,
        "symbols_stale_tail": 0,
        "symbols_all_missing_idx": [],
        "symbols_partial_missing_idx": [],
        "symbols_idx_prefix_only_normalized": [],
        "symbols_schema_or_other_fatal": [],
        "symbols_suspected_delisted": [],
        "symbols_confirmed_delisted": [],
        "symbols_formal_exclude": [],
        "symbols_warning_review": [],
        "symbols_pre_market_no_idx": [],
    }

    for symbol in symbols:
        files = list_parquet_files(os.path.join(args.data_dir, symbol))
        baseline_entry = baseline_entry_for_symbol(baseline_payload, symbol)
        min_open_time_ms = None
        last_contract_from_baseline = None
        baseline_policy = BASELINE_POLICY_REVIEW_REQUIRED
        if baseline_entry:
            last_contract_from_baseline = baseline_entry.get("last_contract_open_time_ms")
            if last_contract_from_baseline is not None:
                min_open_time_ms = max(0, int(last_contract_from_baseline) - INTERVAL_MS)
            baseline_policy = str(baseline_entry.get("policy") or BASELINE_POLICY_REVIEW_REQUIRED).strip().upper()

        open_times, idx_complete, issues, row_count = load_symbol_rows(symbol, files, min_open_time_ms=min_open_time_ms)
        pairs = list(zip(open_times, idx_complete))
        pairs.sort(key=lambda x: x[0])
        sorted_times = [x[0] for x in pairs]
        sorted_idx_complete = [x[1] for x in pairs]

        if args.audit_mode == "incremental" and last_contract_from_baseline is not None:
            new_rows = [ms for ms in sorted_times if ms > int(last_contract_from_baseline)]
            if not new_rows:
                summary["symbols_skipped_no_new_rows"] += 1
                continue

        summary["symbols_audited"] += 1

        gaps, duplicate_rows, non_mono, gap_rows = build_contract_gaps(symbol, sorted_times)
        first_idx_ms, last_idx_ms, idx_complete_count = first_last_complete_idx_ms(sorted_times, sorted_idx_complete)
        idx_status = classify_idx_status(row_count=len(sorted_times), idx_complete_count=idx_complete_count, schema_issues=issues)
        idx_segments, idx_missing_total = build_idx_segments(symbol, sorted_times, sorted_idx_complete, idx_status)
        recent_complete_tail_rows, recent_complete_tail_hours = compute_recent_complete_tail(sorted_times, sorted_idx_complete)
        idx_prefix_only_normalized = bool(
            idx_complete_count > 0
            and is_idx_prefix_only_normalized(sorted_times, sorted_idx_complete)
        )

        first_ms = sorted_times[0] if sorted_times else None
        last_ms = sorted_times[-1] if sorted_times else None
        stale_tail = bool(tail_target_ms is not None and last_ms is not None and last_ms < tail_target_ms)
        tail_lag_hours = 0.0
        if tail_target_ms is not None and last_ms is not None and last_ms < tail_target_ms:
            tail_lag_hours = round((tail_target_ms - last_ms) / HOUR_MS, 2)
        suspected_delisted = bool(stale_tail and tail_lag_hours >= float(args.delisted_threshold_hours))
        confirmed = symbol.upper() in confirmed_delisted
        has_schema_issues = len(issues) > 0
        has_contract_gap = len(gaps) > 0
        has_duplicate_ts = duplicate_rows > 0
        has_non_monotonic = non_mono > 0
        historical_idx_only_normalized = bool(
            idx_status == "PARTIAL_MISSING"
            and recent_complete_tail_rows >= RECENT_COMPLETE_TAIL_MIN_ROWS
            and not has_contract_gap
            and not has_duplicate_ts
            and not has_non_monotonic
            and not stale_tail
        )
        idx_prefix_only_normalized = bool(
            idx_prefix_only_normalized
            and not has_contract_gap
            and not has_duplicate_ts
            and not has_non_monotonic
            and not stale_tail
        )

        severity = "OK"
        if baseline_policy == BASELINE_POLICY_PRE_MARKET_NO_IDX and idx_complete_count == 0:
            severity = "WARNING"
        elif has_contract_gap or has_duplicate_ts or has_non_monotonic or idx_status in {"ALL_MISSING", "SCHEMA_MISSING", "NO_ROWS"}:
            severity = "FATAL"
        elif idx_status == "PARTIAL_MISSING":
            severity = "OK" if (historical_idx_only_normalized or idx_prefix_only_normalized) else "WARNING"
        elif has_schema_issues:
            severity = "OK" if idx_prefix_only_normalized else "FATAL"
        elif stale_tail:
            severity = "WARNING"

        exclude_reasons = []
        if severity == "FATAL":
            exclude_reasons.append("FATAL_DATA_QUALITY")
        if confirmed:
            exclude_reasons.append("CONFIRMED_DELISTED")
        exclude_from_formal_universe = len(exclude_reasons) > 0

        status = SymbolStatus(
            symbol=symbol,
            shard_count=len(files),
            total_rows=len(sorted_times),
            first_bar_ms=first_ms,
            last_bar_ms=last_ms,
            first_bar_bjt=ms_to_bjt_str(first_ms),
            last_bar_bjt=ms_to_bjt_str(last_ms),
            first_idx_bar_ms=first_idx_ms,
            last_idx_bar_ms=last_idx_ms,
            first_idx_bar_bjt=ms_to_bjt_str(first_idx_ms),
            last_idx_bar_bjt=ms_to_bjt_str(last_idx_ms),
            idx_complete_rows=idx_complete_count,
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
            recent_complete_tail_rows=recent_complete_tail_rows,
            recent_complete_tail_hours=recent_complete_tail_hours,
            historical_idx_only_normalized=historical_idx_only_normalized,
            idx_prefix_only_normalized=idx_prefix_only_normalized,
            stale_tail=stale_tail,
            tail_lag_hours=tail_lag_hours,
            suspected_delisted=suspected_delisted,
            confirmed_delisted=confirmed,
            exclude_from_formal_universe=exclude_from_formal_universe,
            exclude_reason="|".join(exclude_reasons),
            baseline_policy=baseline_policy_for_symbol(
                SymbolStatus(
                    symbol=symbol,
                    shard_count=len(files),
                    total_rows=len(sorted_times),
                    first_bar_ms=first_ms,
                    last_bar_ms=last_ms,
                    first_bar_bjt=ms_to_bjt_str(first_ms),
                    last_bar_bjt=ms_to_bjt_str(last_ms),
                    first_idx_bar_ms=first_idx_ms,
                    last_idx_bar_ms=last_idx_ms,
                    first_idx_bar_bjt=ms_to_bjt_str(first_idx_ms),
                    last_idx_bar_bjt=ms_to_bjt_str(last_idx_ms),
                    idx_complete_rows=idx_complete_count,
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
                    recent_complete_tail_rows=recent_complete_tail_rows,
                    recent_complete_tail_hours=recent_complete_tail_hours,
                    historical_idx_only_normalized=historical_idx_only_normalized,
                    idx_prefix_only_normalized=idx_prefix_only_normalized,
                    stale_tail=stale_tail,
                    tail_lag_hours=tail_lag_hours,
                    suspected_delisted=suspected_delisted,
                    confirmed_delisted=confirmed,
                    exclude_from_formal_universe=exclude_from_formal_universe,
                    exclude_reason="|".join(exclude_reasons),
                    baseline_policy="",
                    severity=severity,
                ),
                pre_market_symbols,
            ),
            severity=severity,
        )
        symbol_statuses.append(status)
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
        if idx_prefix_only_normalized:
            summary["symbols_idx_prefix_only_normalized"].append(symbol)
        if severity == "FATAL" and idx_status not in {"ALL_MISSING", "PARTIAL_MISSING"}:
            summary["symbols_schema_or_other_fatal"].append(symbol)
        if suspected_delisted:
            summary["symbols_suspected_delisted"].append(symbol)
        if confirmed:
            summary["symbols_confirmed_delisted"].append(symbol)
        if exclude_from_formal_universe:
            summary["symbols_formal_exclude"].append(symbol)
        if severity == "WARNING" or suspected_delisted:
            summary["symbols_warning_review"].append(symbol)
        if status.baseline_policy == BASELINE_POLICY_PRE_MARKET_NO_IDX:
            summary["symbols_pre_market_no_idx"].append(symbol)

    write_csv(
        os.path.join(args.out_dir, "symbol_status.csv"),
        symbol_status_rows,
        [
            "symbol", "shard_count", "total_rows",
            "first_bar_ms", "last_bar_ms", "first_bar_bjt", "last_bar_bjt",
            "first_idx_bar_ms", "last_idx_bar_ms", "first_idx_bar_bjt", "last_idx_bar_bjt",
            "idx_complete_rows",
            "has_contract_gap", "contract_gap_segments", "contract_gap_rows",
            "has_duplicate_ts", "duplicate_ts_rows",
            "has_non_monotonic_ts", "non_monotonic_pairs",
            "idx_status", "idx_missing_segments", "idx_missing_rows",
            "schema_issue_count", "recent_complete_tail_rows", "recent_complete_tail_hours",
            "historical_idx_only_normalized", "idx_prefix_only_normalized",
            "stale_tail", "tail_lag_hours",
            "suspected_delisted", "confirmed_delisted",
            "exclude_from_formal_universe", "exclude_reason",
            "baseline_policy", "severity",
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

    write_symbol_txt(os.path.join(args.out_dir, "symbols_suspected_delisted.txt"), summary["symbols_suspected_delisted"])
    write_symbol_txt(os.path.join(args.out_dir, "symbols_confirmed_delisted.txt"), summary["symbols_confirmed_delisted"])
    write_symbol_txt(os.path.join(args.out_dir, "symbols_formal_exclude.txt"), summary["symbols_formal_exclude"])
    write_symbol_txt(os.path.join(args.out_dir, "symbols_warning_review.txt"), summary["symbols_warning_review"])

    with open(os.path.join(args.out_dir, "summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    if args.write_baseline_file.strip():
        write_baseline_file(
            args.write_baseline_file,
            args=args,
            symbol_statuses=symbol_statuses,
            pre_market_symbols=pre_market_symbols,
        )

    print("=" * 80)
    print("1m 数据质量审计完成")
    print("=" * 80)
    print(f"data_dir                 : {args.data_dir}")
    print(f"out_dir                  : {args.out_dir}")
    print(f"audit_mode               : {args.audit_mode}")
    print(f"baseline_file            : {args.baseline_file or '(not set)'}")
    print(f"symbols_discovered       : {summary['symbols_discovered']}")
    print(f"symbols_audited          : {summary['symbols_audited']}")
    print(f"symbols_skipped_no_new   : {summary['symbols_skipped_no_new_rows']}")
    print(f"tail_target_bjt          : {args.tail_target_bjt or '(not set)'}")
    print(f"delisted_threshold_hours : {args.delisted_threshold_hours}")
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
    print(f"idx_prefix_only_normalized     : {len(summary['symbols_idx_prefix_only_normalized'])}")
    print(f"pre_market_no_idx_symbols      : {len(summary['symbols_pre_market_no_idx'])}")
    print(f"suspected_delisted_symbols     : {len(summary['symbols_suspected_delisted'])}")
    print(f"confirmed_delisted_symbols     : {len(summary['symbols_confirmed_delisted'])}")
    print(f"formal_exclude_symbols         : {len(summary['symbols_formal_exclude'])}")
    if args.write_baseline_file.strip():
        print(f"baseline_written_to            : {args.write_baseline_file}")


if __name__ == "__main__":
    main()
