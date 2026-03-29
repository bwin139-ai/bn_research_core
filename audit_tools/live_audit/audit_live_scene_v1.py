#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

KEY_EVENTS = [
    "signal_detected",
    "execution_plan_ready",
    "leverage_ensured",
    "leverage_ensure_failed",
    "entry_fill_observed",
    "exit_price_plan",
    "entry_submitted",
    "entry_submit_failed",
    "tp_submitted",
    "tp_submit_failed",
    "sl_submitted",
    "sl_submit_failed",
    "entry_immediate_bracket_check",
    "entry_immediate_bracket_verify_failed",
    "entry_immediate_bracket_incomplete",
    "entry_immediate_bracket_complete",
    "critical_bracket_gap_after_entry",
    "entry_pending_waiting_fill",
    "entry_filled_recovered_to_open_trade",
    "entry_recovery_bracket_verify_failed",
    "entry_recovery_bracket_incomplete",
    "tp_recreated",
    "tp_recreate_failed",
    "sl_recreated",
    "sl_recreate_failed",
    "orphan_exchange_activity",
    "orphan_exchange_position",
    "orphan_exchange_open_orders",
    "signal_scan_skipped_orphan_exchange_activity",
]


def _safe_float(v: Any) -> float | None:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _ts_key(row: dict[str, Any]) -> tuple[int, str]:
    ts = row.get("bar_ts")
    try:
        return int(ts), str(row.get("event") or "")
    except Exception:
        return 0, str(row.get("event") or "")


@dataclass
class AuditEvent:
    line_no: int
    raw: dict[str, Any]

    @property
    def event(self) -> str:
        return str(self.raw.get("event") or "")

    @property
    def symbol(self) -> str:
        return str(self.raw.get("symbol") or "").upper().strip()

    @property
    def order_root(self) -> str:
        return str(self.raw.get("order_root") or "").strip()

    @property
    def bar_bj(self) -> str:
        return str(self.raw.get("bar_bj") or "")



def load_events(path: Path, symbols: set[str] | None, day: str | None) -> list[AuditEvent]:
    out: list[AuditEvent] = []
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            symbol = str(row.get("symbol") or "").upper().strip()
            if symbols and symbol and symbol not in symbols:
                continue
            bar_bj = str(row.get("bar_bj") or "")
            if day and (not bar_bj.startswith(day)):
                continue
            out.append(AuditEvent(i, row))
    out.sort(key=lambda x: _ts_key(x.raw))
    return out


def build_groups(events: list[AuditEvent]) -> dict[str, list[AuditEvent]]:
    groups: dict[str, list[AuditEvent]] = defaultdict(list)
    fallback_counter = 0
    for ev in events:
        key = ev.order_root
        if not key:
            key = f"NO_ROOT::{ev.symbol or 'UNKNOWN'}::{fallback_counter:06d}"
            fallback_counter += 1
        groups[key].append(ev)
    for arr in groups.values():
        arr.sort(key=lambda x: _ts_key(x.raw))
    return dict(sorted(groups.items(), key=lambda kv: _ts_key(kv[1][0].raw) if kv[1] else (0, "")))


def summarize_group(order_root: str, events: list[AuditEvent]) -> dict[str, Any]:
    symbol = next((e.symbol for e in events if e.symbol), "")
    event_map: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for ev in events:
        event_map[ev.event].append(ev.raw)

    signal_rows = event_map.get("signal_detected", [])
    signal_snapshot = (signal_rows[0].get("signal_snapshot") if signal_rows else None) or {}

    entry_fill_row = (event_map.get("entry_fill_observed") or [{}])[0]
    exit_plan_row = (event_map.get("exit_price_plan") or [{}])[0]
    bracket_row = (event_map.get("entry_immediate_bracket_check") or [{}])[0]

    entry_fill = _safe_float(entry_fill_row.get("entry_fill_price"))
    selected_tp_pct = _safe_float(exit_plan_row.get("selected_tp_pct"))
    resolved_tp = _safe_float(exit_plan_row.get("resolved_tp_price"))
    signal_tp = _safe_float(exit_plan_row.get("signal_tp_price"))
    signal_sl = _safe_float(exit_plan_row.get("signal_sl_price"))
    resolved_sl = _safe_float(exit_plan_row.get("resolved_sl_price"))
    expected_tp_from_fill = None
    tp_gap = None
    if entry_fill and selected_tp_pct and selected_tp_pct > 0:
        expected_tp_from_fill = entry_fill * (1.0 + selected_tp_pct)
    if expected_tp_from_fill is not None and resolved_tp is not None:
        tp_gap = resolved_tp - expected_tp_from_fill

    orphan_rows = event_map.get("orphan_exchange_activity", [])
    critical_rows = event_map.get("critical_bracket_gap_after_entry", [])

    summary = {
        "order_root": order_root,
        "symbol": symbol,
        "first_bar_bj": events[0].bar_bj if events else "",
        "events": [e.event for e in events],
        "signal": {
            "signal_time": signal_snapshot.get("signal_time_bj") or signal_snapshot.get("signal_time"),
            "current_price": _safe_float(signal_snapshot.get("current_price")),
            "signal_tp_price": _safe_float(signal_snapshot.get("tp_price")),
            "signal_sl_price": _safe_float(signal_snapshot.get("sl_price")),
            "params_selected_take_profit_pct": _safe_float(((signal_snapshot.get("params") or {}) if isinstance(signal_snapshot.get("params"), dict) else {}).get("selected_take_profit_pct")),
            "params_selected_tp_pct": _safe_float(((signal_snapshot.get("params") or {}) if isinstance(signal_snapshot.get("params"), dict) else {}).get("selected_tp_pct")),
            "context_selected_tp_pct": _safe_float(((signal_snapshot.get("context") or {}) if isinstance(signal_snapshot.get("context"), dict) else {}).get("selected_tp_pct")),
        },
        "entry_fill": {
            "entry_fill_price": entry_fill,
            "entry_fill_price_source": entry_fill_row.get("entry_fill_price_source"),
        },
        "exit_price_plan": {
            "selected_tp_pct": selected_tp_pct,
            "signal_tp_price": signal_tp,
            "signal_sl_price": signal_sl,
            "resolved_tp_price": resolved_tp,
            "resolved_tp_price_source": exit_plan_row.get("resolved_tp_price_source"),
            "resolved_sl_price": resolved_sl,
            "expected_tp_from_fill": expected_tp_from_fill,
            "tp_gap_vs_expected": tp_gap,
            "tp_semantics_match": (abs(tp_gap) < 1e-12) if tp_gap is not None else None,
        },
        "submit_status": {
            "tp_submit_ok": bool((event_map.get("tp_submitted") or [])) and not bool((event_map.get("tp_submit_failed") or [])),
            "sl_submit_ok": bool((event_map.get("sl_submitted") or [])) and not bool((event_map.get("sl_submit_failed") or [])),
            "tp_submit_failed_reason": ((event_map.get("tp_submit_failed") or [{}])[0].get("exchange_snapshot") or {}).get("reason"),
            "sl_submit_failed_reason": ((event_map.get("sl_submit_failed") or [{}])[0].get("exchange_snapshot") or {}).get("reason"),
        },
        "entry_immediate_bracket_check": {
            "tp_submit_ok": bracket_row.get("tp_submit_ok"),
            "sl_submit_ok": bracket_row.get("sl_submit_ok"),
            "tp_bound_initial": bracket_row.get("tp_bound_initial"),
            "sl_bound_initial": bracket_row.get("sl_bound_initial"),
        },
        "entry_result": {
            "has_entry_immediate_bracket_complete": bool(event_map.get("entry_immediate_bracket_complete")),
            "has_entry_pending_waiting_fill": bool(event_map.get("entry_pending_waiting_fill")),
            "has_critical_bracket_gap_after_entry": bool(critical_rows),
            "critical_bracket_gap_reason": (critical_rows[0].get("reason") if critical_rows else None),
            "has_orphan_exchange_activity": bool(orphan_rows),
        },
        "raw_event_line_nos": {ev.event: ev.line_no for ev in events},
    }
    return summary


def print_group_summary(summary: dict[str, Any]) -> None:
    print("=" * 100)
    print(f"symbol      : {summary['symbol']}")
    print(f"order_root  : {summary['order_root']}")
    print(f"first_bar_bj: {summary['first_bar_bj']}")
    print(f"events      : {' -> '.join(summary['events'])}")

    sig = summary["signal"]
    print("\n[signal]")
    print(f"signal_time                : {sig['signal_time']}")
    print(f"current_price              : {sig['current_price']}")
    print(f"signal_tp_price            : {sig['signal_tp_price']}")
    print(f"signal_sl_price            : {sig['signal_sl_price']}")
    print(f"params.selected_take_profit_pct : {sig['params_selected_take_profit_pct']}")
    print(f"params.selected_tp_pct          : {sig['params_selected_tp_pct']}")
    print(f"context.selected_tp_pct         : {sig['context_selected_tp_pct']}")

    fill = summary["entry_fill"]
    plan = summary["exit_price_plan"]
    print("\n[tp/sl semantics]")
    print(f"entry_fill_price           : {fill['entry_fill_price']}")
    print(f"entry_fill_price_source    : {fill['entry_fill_price_source']}")
    print(f"selected_tp_pct            : {plan['selected_tp_pct']}")
    print(f"resolved_tp_price          : {plan['resolved_tp_price']}")
    print(f"resolved_tp_price_source   : {plan['resolved_tp_price_source']}")
    print(f"expected_tp_from_fill      : {plan['expected_tp_from_fill']}")
    print(f"tp_gap_vs_expected         : {plan['tp_gap_vs_expected']}")
    print(f"tp_semantics_match         : {plan['tp_semantics_match']}")
    print(f"resolved_sl_price          : {plan['resolved_sl_price']}")

    submit = summary["submit_status"]
    bracket = summary["entry_immediate_bracket_check"]
    result = summary["entry_result"]
    print("\n[submit / bracket]")
    print(f"tp_submit_ok               : {submit['tp_submit_ok']}")
    print(f"sl_submit_ok               : {submit['sl_submit_ok']}")
    print(f"tp_submit_failed_reason    : {submit['tp_submit_failed_reason']}")
    print(f"sl_submit_failed_reason    : {submit['sl_submit_failed_reason']}")
    print(f"tp_bound_initial           : {bracket['tp_bound_initial']}")
    print(f"sl_bound_initial           : {bracket['sl_bound_initial']}")
    print(f"entry_immediate_complete   : {result['has_entry_immediate_bracket_complete']}")
    print(f"entry_pending_wait_fill    : {result['has_entry_pending_waiting_fill']}")
    print(f"critical_bracket_gap       : {result['has_critical_bracket_gap_after_entry']}")
    print(f"critical_gap_reason        : {result['critical_bracket_gap_reason']}")
    print(f"orphan_exchange_activity   : {result['has_orphan_exchange_activity']}")

    print("\n[line refs in audit jsonl]")
    for k, v in sorted(summary["raw_event_line_nos"].items(), key=lambda kv: kv[1]):
        print(f"{v:>8}  {k}")



def main() -> int:
    parser = argparse.ArgumentParser(description="审计 Snapback live 现场执行链路")
    parser.add_argument("--audit-file", required=True, help="例如 state/live_audit/snapback_mybwin139.jsonl")
    parser.add_argument("--symbols", nargs="*", default=[], help="例如 PIPPINUSDT 4USDT")
    parser.add_argument("--day", default=None, help="北京日期过滤，例如 2026-03-29")
    parser.add_argument("--show-all-events", action="store_true", help="不只看关键事件，显示该组全部事件")
    parser.add_argument("--json-out", default=None, help="可选，输出完整 summary json")
    args = parser.parse_args()

    audit_path = Path(args.audit_file)
    if not audit_path.exists():
        raise SystemExit(f"audit file not found: {audit_path}")

    symbols = {s.upper().strip() for s in args.symbols if s.strip()} or None
    events = load_events(audit_path, symbols=symbols, day=args.day)
    if not events:
        print("未命中任何事件")
        return 0

    if not args.show_all_events:
        events = [e for e in events if e.event in KEY_EVENTS]
        if not events:
            print("命中了 symbol/day，但关键事件为空")
            return 0

    groups = build_groups(events)
    summaries = [summarize_group(order_root, arr) for order_root, arr in groups.items()]

    print(f"audit_file : {audit_path}")
    print(f"symbols    : {sorted(symbols) if symbols else 'ALL'}")
    print(f"day        : {args.day or 'ALL'}")
    print(f"group_count: {len(summaries)}")
    for summary in summaries:
        print_group_summary(summary)

    if args.json_out:
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(summaries, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\njson_out: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
