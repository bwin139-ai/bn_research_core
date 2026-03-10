#!/usr/bin/env python3
import argparse
import hashlib
import json
import math
import os
import re
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

PHASE_PATTERNS = {
    "start": re.compile(r"\[start\]"),
    "panel_ready": re.compile(r"\[load_panel\].*done|\[panel_ready\]|panel_ready", re.IGNORECASE),
    "data_ready": re.compile(r"\[data_ready\]|data_ready", re.IGNORECASE),
    "dict_ready": re.compile(r"\[build_dict\].*done|\[dict_ready\]|dict_ready", re.IGNORECASE),
    "first_signal": re.compile(r"first_signal", re.IGNORECASE),
    "done": re.compile(r"\[done\]| done$|completed|finished", re.IGNORECASE),
}
TS_PREFIX = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})")


def md5_file(path: str) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def load_trades(path: str) -> List[Dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        raw = f.read().strip()
    if not raw:
        return []

    # Try full-file JSON first, but fall back to JSONL if the file contains
    # multiple top-level JSON objects (common .jsonl layout).
    if raw[0] in "[{":
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            if isinstance(parsed.get("trades"), list):
                return parsed["trades"]
            raise ValueError(f"Unsupported JSON object format in {path}")

    # JSONL
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception as e:
                raise ValueError(f"Invalid JSON on line {i} in {path}: {e}")
            out.append(obj)
    return out


def trade_key(tr: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        tr.get("symbol"),
        tr.get("signal_time"),
        tr.get("entry_time"),
        tr.get("exit_time"),
        tr.get("reason"),
    )


def canonical_price(v: Any, digits: int = 12) -> Optional[float]:
    if v is None:
        return None
    try:
        return round(float(v), digits)
    except Exception:
        return None


def skeleton_equal(a: Dict[str, Any], b: Dict[str, Any], price_tol: float, pnl_tol: float) -> Tuple[bool, Dict[str, Any]]:
    diff = {}
    for field in ["symbol", "signal_time", "entry_time", "exit_time", "reason"]:
        if a.get(field) != b.get(field):
            diff[field] = {"old": a.get(field), "new": b.get(field)}
    for field in ["entry_price", "exit_price", "signal_price"]:
        av = a.get(field)
        bv = b.get(field)
        if av is None and bv is None:
            continue
        if av is None or bv is None or abs(float(av) - float(bv)) > price_tol:
            diff[field] = {"old": av, "new": bv}
    av = a.get("pnl_pct")
    bv = b.get("pnl_pct")
    if av is None and bv is None:
        pass
    elif av is None or bv is None or abs(float(av) - float(bv)) > pnl_tol:
        diff["pnl_pct"] = {"old": av, "new": bv}
    return (len(diff) == 0, diff)


def context_float_diffs(a: Dict[str, Any], b: Dict[str, Any], tol: float) -> Dict[str, Any]:
    ca = a.get("context") or {}
    cb = b.get("context") or {}
    keys = sorted(set(ca.keys()) | set(cb.keys()))
    out = {}
    for k in keys:
        av = ca.get(k)
        bv = cb.get(k)
        if isinstance(av, (int, float)) and isinstance(bv, (int, float)):
            d = float(bv) - float(av)
            if abs(d) > tol:
                out[k] = {"old": av, "new": bv, "delta": d}
        elif av != bv:
            out[k] = {"old": av, "new": bv}
    return out


def parse_dt(s: str) -> Optional[datetime]:
    try:
        return datetime.strptime(s, "%Y-%m-%d %H:%M:%S,%f")
    except Exception:
        return None


def parse_log(path: str) -> Dict[str, Any]:
    phase_ts: Dict[str, str] = {}
    lines = 0
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            lines += 1
            m = TS_PREFIX.match(line)
            if not m:
                continue
            ts_str = m.group(1)
            for phase, pat in PHASE_PATTERNS.items():
                if phase in phase_ts:
                    continue
                if pat.search(line):
                    phase_ts[phase] = ts_str
    phase_secs = {}
    ordered = ["start", "panel_ready", "data_ready", "dict_ready", "first_signal", "done"]
    parsed = {k: parse_dt(v) for k, v in phase_ts.items()}
    for a, b, name in [
        ("start", "panel_ready", "load_panel"),
        ("panel_ready", "data_ready", "panel_to_data_ready"),
        ("data_ready", "dict_ready", "build_dict"),
        ("dict_ready", "first_signal", "to_first_signal"),
        ("start", "done", "total_to_done"),
    ]:
        if parsed.get(a) and parsed.get(b):
            phase_secs[name] = round((parsed[b] - parsed[a]).total_seconds(), 3)
    return {"path": path, "lines": lines, "phase_ts": phase_ts, "phase_secs": phase_secs}


def compare_trades(old_trades: List[Dict[str, Any]], new_trades: List[Dict[str, Any]], price_tol: float, pnl_tol: float, ctx_tol: float) -> Dict[str, Any]:
    old_by_key = {trade_key(t): t for t in old_trades}
    new_by_key = {trade_key(t): t for t in new_trades}
    old_keys = list(old_by_key.keys())
    new_keys = list(new_by_key.keys())
    only_old = [k for k in old_keys if k not in new_by_key]
    only_new = [k for k in new_keys if k not in old_by_key]
    common = [k for k in old_keys if k in new_by_key]

    skeleton_mismatches = []
    context_mismatches = []
    first_context_diff = None
    for k in common:
        a = old_by_key[k]
        b = new_by_key[k]
        ok, diff = skeleton_equal(a, b, price_tol=price_tol, pnl_tol=pnl_tol)
        if not ok:
            skeleton_mismatches.append({"key": k, "diff": diff})
        ctx_diff = context_float_diffs(a, b, ctx_tol)
        if ctx_diff:
            item = {"key": k, "context_diff": ctx_diff}
            context_mismatches.append(item)
            if first_context_diff is None:
                first_context_diff = item

    same_order_key_seq = [trade_key(t) for t in old_trades] == [trade_key(t) for t in new_trades]
    reason_counts_old = Counter(t.get("reason") for t in old_trades)
    reason_counts_new = Counter(t.get("reason") for t in new_trades)
    pnl_sum_old = sum(float(t.get("pnl_pct", 0.0) or 0.0) for t in old_trades)
    pnl_sum_new = sum(float(t.get("pnl_pct", 0.0) or 0.0) for t in new_trades)

    return {
        "old_lines": len(old_trades),
        "new_lines": len(new_trades),
        "same_order_key_seq": same_order_key_seq,
        "only_old": only_old[:10],
        "only_new": only_new[:10],
        "only_old_count": len(only_old),
        "only_new_count": len(only_new),
        "common_count": len(common),
        "skeleton_mismatch_count": len(skeleton_mismatches),
        "first_skeleton_mismatch": skeleton_mismatches[0] if skeleton_mismatches else None,
        "context_mismatch_count": len(context_mismatches),
        "first_context_mismatch": first_context_diff,
        "reason_counts_old": dict(reason_counts_old),
        "reason_counts_new": dict(reason_counts_new),
        "pnl_sum_old": pnl_sum_old,
        "pnl_sum_new": pnl_sum_new,
        "pnl_sum_delta": pnl_sum_new - pnl_sum_old,
        "trade_skeleton_recognized_as_same": (
            len(only_old) == 0 and len(only_new) == 0 and len(skeleton_mismatches) == 0
        ),
    }


def compare_logs(old_info: Dict[str, Any], new_info: Dict[str, Any]) -> Dict[str, Any]:
    names = sorted(set(old_info["phase_secs"].keys()) | set(new_info["phase_secs"].keys()))
    summary = {}
    for name in names:
        ov = old_info["phase_secs"].get(name)
        nv = new_info["phase_secs"].get(name)
        if ov is None or nv is None:
            summary[name] = {"old": ov, "new": nv, "gain_pct": None}
        else:
            gain = ((ov - nv) / ov * 100.0) if ov else None
            summary[name] = {"old": ov, "new": nv, "gain_pct": round(gain, 2) if gain is not None else None}
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare old Snapback baseline vs new baseline with business-oriented trade skeleton checks.")
    ap.add_argument("--old-log", required=True)
    ap.add_argument("--new-log", required=True)
    ap.add_argument("--old-trades", required=True)
    ap.add_argument("--new-trades", required=True)
    ap.add_argument("--price-tol", type=float, default=1e-12)
    ap.add_argument("--pnl-tol", type=float, default=1e-12)
    ap.add_argument("--context-float-tol", type=float, default=0.0)
    ap.add_argument("--out-json", default="")
    args = ap.parse_args()

    old_trades = load_trades(args.old_trades)
    new_trades = load_trades(args.new_trades)
    old_log = parse_log(args.old_log)
    new_log = parse_log(args.new_log)

    trade_cmp = compare_trades(old_trades, new_trades, args.price_tol, args.pnl_tol, args.context_float_tol)
    log_cmp = compare_logs(old_log, new_log)

    result = {
        "input": {
            "old_log": args.old_log,
            "new_log": args.new_log,
            "old_trades": args.old_trades,
            "new_trades": args.new_trades,
            "old_log_md5": md5_file(args.old_log) if os.path.exists(args.old_log) else None,
            "new_log_md5": md5_file(args.new_log) if os.path.exists(args.new_log) else None,
            "old_trades_md5": md5_file(args.old_trades) if os.path.exists(args.old_trades) else None,
            "new_trades_md5": md5_file(args.new_trades) if os.path.exists(args.new_trades) else None,
        },
        "trade_consistency": trade_cmp,
        "log_performance": log_cmp,
        "raw_phase_timestamps": {
            "old": old_log["phase_ts"],
            "new": new_log["phase_ts"],
        },
    }

    print("===== INPUT FILES =====")
    for k, v in result["input"].items():
        print(f"{k}: {v}")

    print("\n===== BUSINESS BASELINE CHECK =====")
    print(f"old_lines: {trade_cmp['old_lines']}")
    print(f"new_lines: {trade_cmp['new_lines']}")
    print(f"trade_skeleton_recognized_as_same: {trade_cmp['trade_skeleton_recognized_as_same']}")
    print(f"same_order_key_seq: {trade_cmp['same_order_key_seq']}")
    print(f"only_old_count: {trade_cmp['only_old_count']}")
    print(f"only_new_count: {trade_cmp['only_new_count']}")
    print(f"skeleton_mismatch_count: {trade_cmp['skeleton_mismatch_count']}")
    print(f"context_mismatch_count: {trade_cmp['context_mismatch_count']}")
    print(f"pnl_sum_old: {trade_cmp['pnl_sum_old']}")
    print(f"pnl_sum_new: {trade_cmp['pnl_sum_new']}")
    print(f"pnl_sum_delta: {trade_cmp['pnl_sum_delta']}")
    print(f"reason_counts_old: {trade_cmp['reason_counts_old']}")
    print(f"reason_counts_new: {trade_cmp['reason_counts_new']}")
    if trade_cmp["first_skeleton_mismatch"]:
        print("first_skeleton_mismatch:")
        print(json.dumps(trade_cmp["first_skeleton_mismatch"], ensure_ascii=False, indent=2))
    if trade_cmp["first_context_mismatch"]:
        print("first_context_mismatch:")
        print(json.dumps(trade_cmp["first_context_mismatch"], ensure_ascii=False, indent=2))

    print("\n===== PERFORMANCE SUMMARY =====")
    for name, item in log_cmp.items():
        print(f"[{name}]")
        print(f"  old : {item['old']}")
        print(f"  new : {item['new']}")
        print(f"  gain: {item['gain_pct']}")

    print("\n===== RAW PHASE TIMESTAMPS =====")
    all_keys = sorted(set(old_log["phase_ts"].keys()) | set(new_log["phase_ts"].keys()))
    for k in all_keys:
        print(f"{k:16s} old={old_log['phase_ts'].get(k)} | new={new_log['phase_ts'].get(k)}")

    if args.out_json:
        with open(args.out_json, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        print(f"\nWrote JSON summary to: {args.out_json}")


if __name__ == "__main__":
    main()
