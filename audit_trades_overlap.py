#!/usr/bin/env python3
import argparse
import json
import math
import os
from collections import Counter
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Tuple

TIME_CANDIDATES = [
    "exit_time_ms",
    "close_time_ms",
    "exit_ts_ms",
    "exit_timestamp_ms",
    "exit_time",
    "close_time",
    "exit_ts",
    "timestamp",
    "ts",
    "entry_time_ms",
    "open_time_ms",
    "entry_time",
]

STRICT_KEY_CANDIDATES = [
    "symbol",
    "side",
    "position_side",
    "entry_time_ms",
    "exit_time_ms",
    "entry_time",
    "exit_time",
    "entry_reason",
    "exit_reason",
    "reason",
    "signal",
]

FLOAT_CANDIDATES = [
    "entry_price",
    "exit_price",
    "pnl",
    "net_pnl",
    "gross_pnl",
    "return_pct",
    "roi",
    "fee",
    "fees",
]

REASON_CANDIDATES = ["reason", "exit_reason", "entry_reason", "signal"]


@dataclass
class TradeRow:
    idx: int
    raw: Dict[str, Any]
    time_key: str
    time_value: float


def _load_jsonl(path: str) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as exc:
                raise SystemExit(f"JSON decode error in {path}:{lineno}: {exc}")
    return rows


def _pick_time_key(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        raise SystemExit("No trades found; cannot infer time field.")
    sample_keys = set()
    for row in rows[:20]:
        sample_keys.update(row.keys())
    for key in TIME_CANDIDATES:
        if key in sample_keys:
            # Make sure enough values are usable
            ok = 0
            for row in rows[:20]:
                v = row.get(key)
                if isinstance(v, (int, float)):
                    ok += 1
                elif isinstance(v, str) and v:
                    ok += 1
            if ok >= max(1, min(5, len(rows[:20]))):
                return key
    raise SystemExit(
        "Unable to infer time field. Pass --time-key explicitly after checking your JSONL schema."
    )


def _to_epoch_like(v: Any) -> float:
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            raise ValueError("empty time string")
        # Try plain numeric first
        try:
            return float(s)
        except ValueError:
            pass
        from datetime import datetime
        s2 = s.replace("Z", "+00:00")
        return datetime.fromisoformat(s2).timestamp()
    raise ValueError(f"unsupported time value: {type(v).__name__}")


def _materialize(rows: List[Dict[str, Any]], time_key: str) -> List[TradeRow]:
    out: List[TradeRow] = []
    for i, row in enumerate(rows):
        if time_key not in row:
            raise SystemExit(f"Row {i} missing time key {time_key}")
        out.append(TradeRow(idx=i, raw=row, time_key=time_key, time_value=_to_epoch_like(row[time_key])))
    return out


def _infer_overlap(
    old_rows: List[TradeRow],
    new_rows: List[TradeRow],
    compare_start: Optional[float],
    compare_end: Optional[float],
) -> Tuple[float, float]:
    old_min = min(r.time_value for r in old_rows)
    old_max = max(r.time_value for r in old_rows)
    new_min = min(r.time_value for r in new_rows)
    new_max = max(r.time_value for r in new_rows)
    start = max(old_min, new_min) if compare_start is None else compare_start
    end = min(old_max, new_max) if compare_end is None else compare_end
    if end < start:
        raise SystemExit("No overlap between old and new trades in the chosen compare window.")
    return start, end


def _filter_rows(rows: List[TradeRow], start: float, end: float) -> List[TradeRow]:
    return [r for r in rows if start <= r.time_value <= end]


def _build_semantic_key(row: Dict[str, Any], extra_keys: Optional[List[str]] = None) -> Tuple[Any, ...]:
    keys = list(STRICT_KEY_CANDIDATES)
    if extra_keys:
        for k in extra_keys:
            if k not in keys:
                keys.append(k)
    return tuple(row.get(k) for k in keys)


def _pick_reason(row: Dict[str, Any]) -> Optional[str]:
    for k in REASON_CANDIDATES:
        v = row.get(k)
        if v is not None:
            return str(v)
    return None


def _float_diffs(a: Dict[str, Any], b: Dict[str, Any], abs_tol: float, rel_tol: float) -> Dict[str, Dict[str, float]]:
    out: Dict[str, Dict[str, float]] = {}
    keys = set(a.keys()) & set(b.keys())
    for k in keys:
        if k not in FLOAT_CANDIDATES:
            continue
        va, vb = a.get(k), b.get(k)
        if isinstance(va, (int, float)) and isinstance(vb, (int, float)):
            if not math.isclose(float(va), float(vb), abs_tol=abs_tol, rel_tol=rel_tol):
                out[k] = {
                    "old": float(va),
                    "new": float(vb),
                    "delta": float(vb) - float(va),
                }
    return out


def _counter(rows: Iterable[TradeRow]) -> Counter:
    c = Counter()
    for r in rows:
        reason = _pick_reason(r.raw)
        if reason is not None:
            c[reason] += 1
    return c


def _sum_field(rows: Iterable[TradeRow], names: List[str]) -> Optional[float]:
    total = 0.0
    found = False
    for r in rows:
        for n in names:
            v = r.raw.get(n)
            if isinstance(v, (int, float)):
                total += float(v)
                found = True
                break
    return total if found else None


def _first_mismatch(
    old_rows: List[TradeRow],
    new_rows: List[TradeRow],
    abs_tol: float,
    rel_tol: float,
    extra_keys: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    n = min(len(old_rows), len(new_rows))
    for i in range(n):
        a = old_rows[i].raw
        b = new_rows[i].raw
        ka = _build_semantic_key(a, extra_keys)
        kb = _build_semantic_key(b, extra_keys)
        float_diffs = _float_diffs(a, b, abs_tol=abs_tol, rel_tol=rel_tol)
        if ka != kb or float_diffs:
            return {
                "index": i,
                "semantic_key_equal": ka == kb,
                "old_semantic_key": ka,
                "new_semantic_key": kb,
                "float_diffs": float_diffs,
                "old": a,
                "new": b,
            }
    if len(old_rows) != len(new_rows):
        return {
            "index": n,
            "semantic_key_equal": False,
            "old": old_rows[n].raw if len(old_rows) > n else None,
            "new": new_rows[n].raw if len(new_rows) > n else None,
            "reason": "different_lengths",
        }
    return None


def _fmt_time_like(value: float, original_key: str) -> Any:
    # Keep same scale for ms-like keys; otherwise emit ISO for readability.
    if original_key.endswith("_ms"):
        return int(value)
    from datetime import datetime, timezone
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def audit(
    old_path: str,
    new_path: str,
    time_key: Optional[str],
    compare_start: Optional[str],
    compare_end: Optional[str],
    abs_tol: float,
    rel_tol: float,
    extra_keys: Optional[List[str]],
) -> Dict[str, Any]:
    old_raw = _load_jsonl(old_path)
    new_raw = _load_jsonl(new_path)

    old_time_key = time_key or _pick_time_key(old_raw)
    new_time_key = time_key or _pick_time_key(new_raw)

    old_rows = _materialize(old_raw, old_time_key)
    new_rows = _materialize(new_raw, new_time_key)

    compare_start_v = _to_epoch_like(compare_start) if compare_start else None
    compare_end_v = _to_epoch_like(compare_end) if compare_end else None
    overlap_start, overlap_end = _infer_overlap(old_rows, new_rows, compare_start_v, compare_end_v)

    old_overlap = _filter_rows(old_rows, overlap_start, overlap_end)
    new_overlap = _filter_rows(new_rows, overlap_start, overlap_end)
    new_tail = [r for r in new_rows if r.time_value > overlap_end]

    old_keys = [_build_semantic_key(r.raw, extra_keys) for r in old_overlap]
    new_keys = [_build_semantic_key(r.raw, extra_keys) for r in new_overlap]

    old_counter = Counter(old_keys)
    new_counter = Counter(new_keys)
    only_old = sum((old_counter - new_counter).values())
    only_new = sum((new_counter - old_counter).values())

    mismatch = _first_mismatch(old_overlap, new_overlap, abs_tol, rel_tol, extra_keys)

    pnl_names = ["pnl", "net_pnl", "gross_pnl"]
    old_pnl = _sum_field(old_overlap, pnl_names)
    new_pnl = _sum_field(new_overlap, pnl_names)
    tail_pnl = _sum_field(new_tail, pnl_names)

    report = {
        "old_path": old_path,
        "new_path": new_path,
        "old_total_count": len(old_rows),
        "new_total_count": len(new_rows),
        "old_time_key": old_time_key,
        "new_time_key": new_time_key,
        "overlap_start": _fmt_time_like(overlap_start, old_time_key),
        "overlap_end": _fmt_time_like(overlap_end, old_time_key),
        "old_overlap_count": len(old_overlap),
        "new_overlap_count": len(new_overlap),
        "same_order_key_seq": old_keys == new_keys,
        "only_old_count": only_old,
        "only_new_count": only_new,
        "reason_counts_old": dict(_counter(old_overlap)),
        "reason_counts_new": dict(_counter(new_overlap)),
        "old_overlap_pnl_sum": old_pnl,
        "new_overlap_pnl_sum": new_pnl,
        "overlap_pnl_sum_delta": (new_pnl - old_pnl) if old_pnl is not None and new_pnl is not None else None,
        "tail_new_count": len(new_tail),
        "tail_new_pnl_sum": tail_pnl,
        "first_mismatch": mismatch,
    }
    if new_tail:
        report["tail_new_first_time"] = _fmt_time_like(min(r.time_value for r in new_tail), new_time_key)
        report["tail_new_last_time"] = _fmt_time_like(max(r.time_value for r in new_tail), new_time_key)
        report["tail_new_symbols_top10"] = dict(Counter(str(r.raw.get("symbol")) for r in new_tail).most_common(10))
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit old/new trades JSONL over overlap window with per-trade comparison.")
    ap.add_argument("--old-trades", required=True)
    ap.add_argument("--new-trades", required=True)
    ap.add_argument("--time-key", default=None, help="Optional explicit time field used by both files.")
    ap.add_argument("--compare-start", default=None, help="Optional compare start (epoch / epoch_ms / ISO8601).")
    ap.add_argument("--compare-end", default=None, help="Optional compare end (epoch / epoch_ms / ISO8601).")
    ap.add_argument("--abs-tol", type=float, default=1e-9)
    ap.add_argument("--rel-tol", type=float, default=1e-9)
    ap.add_argument("--extra-key", action="append", default=[], help="Additional strict key field(s) to include in semantic comparison.")
    ap.add_argument("--report-out", default=None)
    args = ap.parse_args()

    report = audit(
        old_path=args.old_trades,
        new_path=args.new_trades,
        time_key=args.time_key,
        compare_start=args.compare_start,
        compare_end=args.compare_end,
        abs_tol=args.abs_tol,
        rel_tol=args.rel_tol,
        extra_keys=args.extra_key,
    )
    text = json.dumps(report, ensure_ascii=False, indent=2)
    print(text)
    if args.report_out:
        with open(args.report_out, "w", encoding="utf-8") as f:
            f.write(text + "\n")


if __name__ == "__main__":
    main()
