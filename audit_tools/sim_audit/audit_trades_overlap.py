#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

STRICT_TOP_FIELDS = [
    "symbol",
    "signal_time",
    "entry_time",
    "exit_time",
    "reason",
    "signal_time_bj",
    "entry_time_bj",
    "exit_time_bj",
]

STRICT_CONTEXT_FIELDS = [
    "trigger_type",
]

FLOAT_TOP_FIELDS = [
    "signal_price",
    "entry_price",
    "exit_price",
    "pnl_pct",
]

FLOAT_CONTEXT_FIELDS = [
    "chg_24h",
    "vol_24h",
    "drop_pct",
    "vol_ratio",
]

TIME_KEYS_CANDIDATES = [
    "exit_time",
    "entry_time",
    "signal_time",
    "exit_time_ms",
    "entry_time_ms",
    "signal_time_ms",
]


@dataclass
class TimeInfo:
    key: str
    value: float


def _read_jsonl(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValueError(f"JSON decode error in {path}:{line_no}: {e}") from e
    if not rows:
        raise ValueError(f"No rows found in {path}")
    return rows


def _to_epoch_seconds(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        av = abs(v)
        if av >= 1e14:
            return v / 1_000_000.0
        if av >= 1e11:
            return v / 1000.0
        return v
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        if s.isdigit() or (s.startswith("-") and s[1:].isdigit()):
            return _to_epoch_seconds(int(s))
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.timestamp()
        except ValueError:
            return None
    return None


def _fmt_epoch_seconds(sec: Optional[float]) -> Optional[str]:
    if sec is None:
        return None
    return datetime.fromtimestamp(sec, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _detect_time_key(rows: List[Dict[str, Any]]) -> str:
    sample = rows[0]
    for key in TIME_KEYS_CANDIDATES:
        if key in sample and _to_epoch_seconds(sample.get(key)) is not None:
            return key
    raise ValueError(f"Could not detect time key from candidates: {TIME_KEYS_CANDIDATES}")


def _extract_time(row: Dict[str, Any], key: str) -> TimeInfo:
    sec = _to_epoch_seconds(row.get(key))
    if sec is None:
        raise ValueError(f"Row missing/invalid time field {key}: {row}")
    return TimeInfo(key=key, value=sec)


def _filter_overlap(rows: List[Dict[str, Any]], time_key: str, start_sec: float, end_sec: float) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        t = _extract_time(row, time_key).value
        if start_sec <= t <= end_sec:
            out.append(row)
    return out


def _tail_after(rows: List[Dict[str, Any]], time_key: str, end_sec: float) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        t = _extract_time(row, time_key).value
        if t > end_sec:
            out.append(row)
    return out


def _strict_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
    ctx = row.get("context") or {}
    vals: List[Any] = []
    for field in STRICT_TOP_FIELDS:
        vals.append(row.get(field))
    for field in STRICT_CONTEXT_FIELDS:
        vals.append(ctx.get(field))
    return tuple(vals)


def _counter_diff_count(a: Counter, b: Counter) -> int:
    total = 0
    for key, count in (a - b).items():
        total += count
    return total


def _safe_isclose(a: Any, b: Any, abs_tol: float, rel_tol: float) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    if isinstance(a, bool) or isinstance(b, bool):
        return a == b
    if not isinstance(a, (int, float)) or not isinstance(b, (int, float)):
        return a == b
    return math.isclose(float(a), float(b), abs_tol=abs_tol, rel_tol=rel_tol)


def _float_fields_diff(
    old_row: Dict[str, Any],
    new_row: Dict[str, Any],
    abs_tol: float,
    rel_tol: float,
) -> Dict[str, Dict[str, Any]]:
    diffs: Dict[str, Dict[str, Any]] = {}

    for field in FLOAT_TOP_FIELDS:
        ov = old_row.get(field)
        nv = new_row.get(field)
        if not _safe_isclose(ov, nv, abs_tol=abs_tol, rel_tol=rel_tol):
            diffs[field] = _make_float_diff_payload(ov, nv)

    old_ctx = old_row.get("context") or {}
    new_ctx = new_row.get("context") or {}
    for field in FLOAT_CONTEXT_FIELDS:
        ov = old_ctx.get(field)
        nv = new_ctx.get(field)
        if not _safe_isclose(ov, nv, abs_tol=abs_tol, rel_tol=rel_tol):
            diffs[f"context.{field}"] = _make_float_diff_payload(ov, nv)

    return diffs


def _make_float_diff_payload(old_value: Any, new_value: Any) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "old": old_value,
        "new": new_value,
        "abs_diff": None,
        "rel_diff": None,
    }
    if isinstance(old_value, (int, float)) and isinstance(new_value, (int, float)):
        o = float(old_value)
        n = float(new_value)
        abs_diff = abs(n - o)
        denom = max(abs(o), abs(n), 1e-30)
        payload["abs_diff"] = abs_diff
        payload["rel_diff"] = abs_diff / denom
    return payload


def _update_float_stats(
    stats: Dict[str, Dict[str, Any]],
    old_row: Dict[str, Any],
    new_row: Dict[str, Any],
    idx: int,
) -> None:
    for field in FLOAT_TOP_FIELDS:
        _update_one_float_stat(stats, field, old_row.get(field), new_row.get(field), idx)

    old_ctx = old_row.get("context") or {}
    new_ctx = new_row.get("context") or {}
    for field in FLOAT_CONTEXT_FIELDS:
        _update_one_float_stat(stats, f"context.{field}", old_ctx.get(field), new_ctx.get(field), idx)


def _update_one_float_stat(
    stats: Dict[str, Dict[str, Any]],
    field: str,
    old_value: Any,
    new_value: Any,
    idx: int,
) -> None:
    item = stats.setdefault(
        field,
        {
            "present_both_count": 0,
            "equal_within_tol_count": 0,
            "diff_count": 0,
            "max_abs_diff": None,
            "max_rel_diff": None,
            "max_abs_diff_at_index": None,
            "max_rel_diff_at_index": None,
            "first_diff_index": None,
            "first_diff_old": None,
            "first_diff_new": None,
        },
    )

    if old_value is None or new_value is None:
        return
    if not isinstance(old_value, (int, float)) or not isinstance(new_value, (int, float)):
        return

    o = float(old_value)
    n = float(new_value)
    abs_diff = abs(n - o)
    rel_diff = abs_diff / max(abs(o), abs(n), 1e-30)
    item["present_both_count"] += 1

    if abs_diff == 0.0:
        item["equal_within_tol_count"] += 1
    else:
        item["diff_count"] += 1
        if item["first_diff_index"] is None:
            item["first_diff_index"] = idx
            item["first_diff_old"] = old_value
            item["first_diff_new"] = new_value

    if item["max_abs_diff"] is None or abs_diff > item["max_abs_diff"]:
        item["max_abs_diff"] = abs_diff
        item["max_abs_diff_at_index"] = idx
    if item["max_rel_diff"] is None or rel_diff > item["max_rel_diff"]:
        item["max_rel_diff"] = rel_diff
        item["max_rel_diff_at_index"] = idx


def _finalize_float_stats(stats: Dict[str, Dict[str, Any]], abs_tol: float, rel_tol: float) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for field, item in stats.items():
        present = item["present_both_count"]
        diff_count = 0
        first_idx = None
        first_old = None
        first_new = None
        max_abs = item["max_abs_diff"]
        max_rel = item["max_rel_diff"]

        # Reinterpret diff_count in tolerance terms.
        # If max diff is within tolerance, all are within tolerance.
        # Otherwise keep the raw diff_count from exact comparisons only as an indicator is misleading,
        # so recompute through stored maxima is impossible. Here we use first_diff_* + maxima for reporting,
        # and expose within_tolerance based on thresholds.
        within_tolerance = True
        if max_abs is not None and max_rel is not None:
            within_tolerance = max_abs <= abs_tol or max_rel <= rel_tol
        if not within_tolerance:
            diff_count = item["diff_count"]
            first_idx = item["first_diff_index"]
            first_old = item["first_diff_old"]
            first_new = item["first_diff_new"]

        out[field] = {
            "present_both_count": present,
            "within_tolerance": within_tolerance,
            "max_abs_diff": max_abs,
            "max_rel_diff": max_rel,
            "max_abs_diff_at_index": item["max_abs_diff_at_index"],
            "max_rel_diff_at_index": item["max_rel_diff_at_index"],
            "first_diff_index": first_idx,
            "first_diff_old": first_old,
            "first_diff_new": first_new,
            "abs_tol": abs_tol,
            "rel_tol": rel_tol,
        }
    return out


def _first_mismatch(
    old_rows: List[Dict[str, Any]],
    new_rows: List[Dict[str, Any]],
    abs_tol: float,
    rel_tol: float,
) -> Optional[Dict[str, Any]]:
    n = min(len(old_rows), len(new_rows))
    for idx in range(n):
        old_row = old_rows[idx]
        new_row = new_rows[idx]
        if _strict_key(old_row) != _strict_key(new_row):
            return {
                "index": idx,
                "type": "strict_key_mismatch",
                "old": old_row,
                "new": new_row,
            }
        float_diffs = _float_fields_diff(old_row, new_row, abs_tol=abs_tol, rel_tol=rel_tol)
        if float_diffs:
            return {
                "index": idx,
                "type": "float_field_mismatch",
                "float_diffs": float_diffs,
                "old": old_row,
                "new": new_row,
            }
    if len(old_rows) != len(new_rows):
        return {
            "index": n,
            "type": "length_mismatch",
            "old_len": len(old_rows),
            "new_len": len(new_rows),
        }
    return None


def _reason_counts(rows: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    c = Counter()
    for row in rows:
        c[str(row.get("reason"))] += 1
    return dict(c)


def _sum_field(rows: Iterable[Dict[str, Any]], field: str) -> Optional[float]:
    vals: List[float] = []
    for row in rows:
        v = row.get(field)
        if isinstance(v, (int, float)):
            vals.append(float(v))
    if not vals:
        return None
    return float(sum(vals))


def _top_symbols(rows: Iterable[Dict[str, Any]], n: int = 10) -> Dict[str, int]:
    c = Counter()
    for row in rows:
        c[str(row.get("symbol"))] += 1
    return dict(c.most_common(n))


def audit(old_trades: str, new_trades: str, abs_tol: float, rel_tol: float) -> Dict[str, Any]:
    old_rows = _read_jsonl(old_trades)
    new_rows = _read_jsonl(new_trades)

    old_time_key = _detect_time_key(old_rows)
    new_time_key = _detect_time_key(new_rows)

    old_min = min(_extract_time(r, old_time_key).value for r in old_rows)
    old_max = max(_extract_time(r, old_time_key).value for r in old_rows)
    new_min = min(_extract_time(r, new_time_key).value for r in new_rows)
    new_max = max(_extract_time(r, new_time_key).value for r in new_rows)

    overlap_start = max(old_min, new_min)
    overlap_end = min(old_max, new_max)
    if overlap_start > overlap_end:
        raise ValueError("No overlapping time range between old and new trades")

    old_overlap = _filter_overlap(old_rows, old_time_key, overlap_start, overlap_end)
    new_overlap = _filter_overlap(new_rows, new_time_key, overlap_start, overlap_end)
    tail_new = _tail_after(new_rows, new_time_key, overlap_end)

    old_keys = [_strict_key(r) for r in old_overlap]
    new_keys = [_strict_key(r) for r in new_overlap]
    same_order_key_seq = old_keys == new_keys

    old_counter = Counter(old_keys)
    new_counter = Counter(new_keys)

    float_stats_raw: Dict[str, Dict[str, Any]] = {}
    pair_count = min(len(old_overlap), len(new_overlap))
    for idx in range(pair_count):
        _update_float_stats(float_stats_raw, old_overlap[idx], new_overlap[idx], idx)
    float_field_report = _finalize_float_stats(float_stats_raw, abs_tol=abs_tol, rel_tol=rel_tol)

    report: Dict[str, Any] = {
        "old_path": old_trades,
        "new_path": new_trades,
        "old_total_count": len(old_rows),
        "new_total_count": len(new_rows),
        "old_time_key": old_time_key,
        "new_time_key": new_time_key,
        "overlap_start": _fmt_epoch_seconds(overlap_start),
        "overlap_end": _fmt_epoch_seconds(overlap_end),
        "old_overlap_count": len(old_overlap),
        "new_overlap_count": len(new_overlap),
        "same_order_key_seq": same_order_key_seq,
        "only_old_count": _counter_diff_count(old_counter, new_counter),
        "only_new_count": _counter_diff_count(new_counter, old_counter),
        "reason_counts_old": _reason_counts(old_overlap),
        "reason_counts_new": _reason_counts(new_overlap),
        "old_overlap_pnl_pct_sum": _sum_field(old_overlap, "pnl_pct"),
        "new_overlap_pnl_pct_sum": _sum_field(new_overlap, "pnl_pct"),
        "overlap_pnl_pct_sum_delta": None,
        "strict_schema": {
            "top_fields": STRICT_TOP_FIELDS,
            "context_fields": STRICT_CONTEXT_FIELDS,
        },
        "float_schema": {
            "top_fields": FLOAT_TOP_FIELDS,
            "context_fields": FLOAT_CONTEXT_FIELDS,
            "abs_tol": abs_tol,
            "rel_tol": rel_tol,
        },
        "float_field_report": float_field_report,
        "tail_new_count": len(tail_new),
        "tail_new_pnl_pct_sum": _sum_field(tail_new, "pnl_pct"),
        "tail_new_first_time": _fmt_epoch_seconds(min((_extract_time(r, new_time_key).value for r in tail_new), default=None)),
        "tail_new_last_time": _fmt_epoch_seconds(max((_extract_time(r, new_time_key).value for r in tail_new), default=None)),
        "tail_new_symbols_top10": _top_symbols(tail_new, n=10),
        "first_mismatch": _first_mismatch(old_overlap, new_overlap, abs_tol=abs_tol, rel_tol=rel_tol),
    }

    old_sum = report["old_overlap_pnl_pct_sum"]
    new_sum = report["new_overlap_pnl_pct_sum"]
    if old_sum is not None and new_sum is not None:
        report["overlap_pnl_pct_sum_delta"] = new_sum - old_sum

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit overlapping trades with strict fields + float tolerance fields")
    parser.add_argument("--old-trades", required=True)
    parser.add_argument("--new-trades", required=True)
    parser.add_argument("--abs-tol", type=float, default=1e-8)
    parser.add_argument("--rel-tol", type=float, default=1e-8)
    parser.add_argument("--report-out", default=None)
    args = parser.parse_args()

    report = audit(
        old_trades=args.old_trades,
        new_trades=args.new_trades,
        abs_tol=args.abs_tol,
        rel_tol=args.rel_tol,
    )

    text = json.dumps(report, ensure_ascii=False, indent=2)
    print(text)
    if args.report_out:
        out_path = Path(args.report_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(text + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
