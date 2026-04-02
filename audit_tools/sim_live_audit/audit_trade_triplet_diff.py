#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
from typing import Any

from audit_tools.live_audit.audit_live_scene_v1 import build_groups, load_events

BJ_TZ = dt.timezone(dt.timedelta(hours=8))


def _bj_to_ms(s: str) -> int:
    dt_bj = dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=BJ_TZ)
    return int(dt_bj.timestamp() * 1000)


def _to_bj(ms: int | None) -> str | None:
    if ms is None:
        return None
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc).astimezone(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _normalize_symbol(s: Any) -> str:
    return str(s or "").upper().strip()


def _safe_int(v: Any) -> int | None:
    if v in (None, ""):
        return None
    try:
        return int(v)
    except Exception:
        return None


def _safe_float(v: Any) -> float | None:
    if v in (None, ""):
        return None
    try:
        return float(v)
    except Exception:
        return None


def _compare_values(a: Any, b: Any, tol: float = 1e-9) -> tuple[bool | None, Any, Any]:
    fa = _safe_float(a)
    fb = _safe_float(b)
    if fa is not None and fb is not None:
        return abs(fa - fb) <= tol, fa, fb
    if a is None or b is None:
        return None, a, b
    return a == b, a, b


def _weighted_avg_price(rows: list[dict[str, Any]]) -> float | None:
    total_qty = 0.0
    total_quote = 0.0
    for row in rows:
        qty = _safe_float(row.get("qty"))
        price = _safe_float(row.get("price"))
        quote_qty = _safe_float(row.get("quote_qty"))
        if qty is None or qty <= 0:
            continue
        if quote_qty is None and price is not None:
            quote_qty = qty * price
        if quote_qty is None:
            continue
        total_qty += qty
        total_quote += quote_qty
    if total_qty <= 0:
        return None
    return total_quote / total_qty


def _sum_float(rows: list[dict[str, Any]], key: str) -> float | None:
    vals = [_safe_float(r.get(key)) for r in rows]
    vals = [x for x in vals if x is not None]
    if not vals:
        return None
    return sum(vals)


def _load_jsonl_rows(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except json.JSONDecodeError:
                continue
            if isinstance(obj, dict):
                out.append(obj)
    return out


def _row_c_time_ms(row: dict[str, Any]) -> int | None:
    direct = _safe_int(row.get("c_time"))
    if direct is not None:
        return direct
    ctx = row.get("context") or {}
    return _safe_int(ctx.get("c_time"))


def _find_sim_trade(path: Path, symbol: str, c_time_ms: int) -> dict[str, Any] | None:
    for row in _load_jsonl_rows(path):
        if _normalize_symbol(row.get("symbol")) != symbol:
            continue
        if _row_c_time_ms(row) == c_time_ms:
            return row
    return None


def _find_live_signal(path: Path, symbol: str, c_time_ms: int) -> dict[str, Any] | None:
    for row in _load_jsonl_rows(path):
        if _normalize_symbol(row.get("symbol")) != symbol:
            continue
        if _row_c_time_ms(row) == c_time_ms:
            return row
    return None


def _find_live_trade(path: Path, symbol: str, c_time_ms: int, live_signal: dict[str, Any] | None = None) -> dict[str, Any] | None:
    target_digest = str((live_signal or {}).get("signal_digest") or "").strip()
    for row in _load_jsonl_rows(path):
        if _normalize_symbol(row.get("symbol")) != symbol:
            continue
        if _row_c_time_ms(row) == c_time_ms:
            return row
        if target_digest and str(row.get("signal_digest") or "").strip() == target_digest:
            return row
    return None


def _group_for_target_from_audit(path: Path, symbol: str, c_time_ms: int) -> dict[str, Any] | None:
    day = _to_bj(c_time_ms)
    day_prefix = day[:10] if day else None
    events = load_events(path, symbols={symbol}, day=day_prefix)
    if not events:
        return None
    groups = build_groups(events)
    target_signal_time = c_time_ms + 60000
    best: tuple[int, dict[str, Any]] | None = None
    for order_root, arr in groups.items():
        signal_snapshot = None
        for ev in arr:
            if str(ev.raw.get("event") or "") == "signal_detected":
                signal_snapshot = ev.raw.get("signal_snapshot") or {}
                break
        if not isinstance(signal_snapshot, dict):
            continue
        ctx = signal_snapshot.get("context") or {}
        ctx_c_time = _safe_int(ctx.get("c_time"))
        sig_ms = _safe_int(signal_snapshot.get("signal_time")) or _safe_int(signal_snapshot.get("signal_time_ts"))
        if ctx_c_time is not None and ctx_c_time == c_time_ms:
            return {"order_root": order_root, "events": [e.raw for e in arr]}
        if sig_ms is not None:
            diff = abs(sig_ms - target_signal_time)
            if best is None or diff < best[0]:
                best = (diff, {"order_root": order_root, "events": [e.raw for e in arr]})
    return best[1] if best and best[0] <= 120000 else None


def _find_bn_orders(path: Path, symbol: str, live_trade: dict[str, Any] | None) -> list[dict[str, Any]]:
    if live_trade is None:
        return []
    order_root = str(live_trade.get("order_root") or "").strip()
    client_ids = {
        str(live_trade.get("entry_client_order_id") or "").strip(),
        str(live_trade.get("tp_order_client_id") or "").strip(),
        str(live_trade.get("sl_order_client_id") or "").strip(),
        str(live_trade.get("time_stop_client_order_id") or "").strip(),
        str(live_trade.get("exit_order_client_id") or "").strip(),
    }
    client_ids.discard("")
    out = []
    for row in _load_jsonl_rows(path):
        if _normalize_symbol(row.get("symbol")) != symbol:
            continue
        if order_root and str(row.get("order_root") or "").strip() == order_root:
            out.append(row)
            continue
        if str(row.get("client_order_id") or "").strip() in client_ids:
            out.append(row)
            continue
        if live_trade.get("exit_order_exchange_id") is not None and row.get("exchange_order_id") == live_trade.get("exit_order_exchange_id"):
            out.append(row)
            continue
    dedup = {}
    for row in out:
        key = (row.get("exchange_order_id"), row.get("client_order_id"))
        dedup[key] = row
    return list(dedup.values())


def _find_bn_fills(path: Path, symbol: str, order_ids: set[Any]) -> list[dict[str, Any]]:
    if not order_ids:
        return []
    out = []
    for row in _load_jsonl_rows(path):
        if _normalize_symbol(row.get("symbol")) != symbol:
            continue
        if row.get("order_id") in order_ids:
            out.append(row)
    return out


def _find_bn_income(path: Path | None, symbol: str, start_ms: int | None, end_ms: int | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    out = []
    for row in _load_jsonl_rows(path):
        if _normalize_symbol(row.get("symbol")) != symbol:
            continue
        event_ms = _safe_int(row.get("event_ms"))
        if start_ms is not None and event_ms is not None and event_ms < start_ms:
            continue
        if end_ms is not None and event_ms is not None and event_ms > end_ms:
            continue
        out.append(row)
    return out


def _find_bn_position_facts(path: Path | None, symbol: str, collected_ms: int | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    rows = []
    for row in _load_jsonl_rows(path):
        if _normalize_symbol(row.get("symbol")) != symbol:
            continue
        rows.append(row)
    if collected_ms is None or not rows:
        return rows[-3:]
    rows.sort(key=lambda r: abs((_safe_int(r.get("collected_ms")) or 0) - collected_ms))
    return rows[:3]


def _leg_order(rows: list[dict[str, Any]], leg: str) -> dict[str, Any] | None:
    leg_u = str(leg).upper().strip()
    for row in rows:
        if str(row.get("leg") or "").upper().strip() == leg_u:
            return row
    return None


def _determine_bn_exit_reason(exit_order: dict[str, Any] | None) -> str | None:
    if not exit_order:
        return None
    leg = str(exit_order.get("leg") or "").upper().strip()
    if leg == "TP":
        return "TAKE_PROFIT"
    if leg == "SL":
        return "STOP_LOSS"
    if leg in {"TS", "TIME_STOP"}:
        return "TIME_STOP"
    return None


def _classify_layer_status(report: dict[str, Any]) -> dict[str, str]:
    comp = report.get("comparison") or {}
    signal_checks = comp.get("signal_checks") or {}
    price_checks = comp.get("price_checks") or {}

    signal_unknown = any(v.get("same") is None for v in signal_checks.values()) if signal_checks else True
    signal_ok = bool(signal_checks) and all(v.get("same") is True for v in signal_checks.values())
    price_unknown = any(v.get("same") is None for v in price_checks.values()) if price_checks else True

    if signal_ok:
        signal_layer = "OK"
    elif signal_unknown:
        signal_layer = "PARTIAL"
    else:
        signal_layer = "DIFF"

    live_bn_entry = price_checks.get("live_entry_vs_bn_entry") or {}
    live_bn_exit = price_checks.get("live_exit_vs_bn_exit") or {}
    live_reason = comp.get("live_reason")
    bn_reason = comp.get("bn_reason")

    if live_bn_entry.get("same") is True and live_bn_exit.get("same") is True and live_reason == bn_reason:
        execution_layer = "OK"
    elif live_bn_entry.get("same") is None or live_bn_exit.get("same") is None or live_reason is None or bn_reason is None:
        execution_layer = "PARTIAL"
    else:
        execution_layer = "DIFF"

    sim_ref = comp.get("sim_reason")
    if sim_ref is None:
        reference_layer = "NO_SIM_REFERENCE"
    elif sim_ref == live_reason:
        reference_layer = "SIM_REF_ALIGNED"
    else:
        reference_layer = "SIM_REF_DIFF"

    return {
        "signal_layer_status": signal_layer,
        "execution_layer_status": execution_layer,
        "sim_reference_status": reference_layer,
    }


def _summary_lines(report: dict[str, Any]) -> str:
    lines: list[str] = []
    target = report["target"]
    lines.append("=== sim/live/bn trade triplet diff ===")
    lines.append(f"symbol: {target['symbol']}")
    lines.append(f"c_time_ms: {target['c_time_ms']}")
    lines.append(f"c_time_bj: {target['c_time_bj']}")
    lines.append("")

    matched = report["matched_records"]
    lines.append("[matched records]")
    for k in ("sim_trade", "live_signal", "live_trade", "live_scene", "bn_orders", "bn_fills", "bn_income", "bn_position_facts"):
        lines.append(f"{k}: {matched[k]}")
    lines.append("")

    layer_status = report.get("layer_status") or {}
    if layer_status:
        lines.append("[layer status]")
        for k, v in layer_status.items():
            lines.append(f"{k}: {v}")
        lines.append("")

    live_signal = report.get("live_signal") or {}
    if live_signal:
        lines.append("[live signal]")
        for k in ("run_id", "projection_schema_version", "strategy_name", "signal_time", "signal_time_bj", "c_time", "c_time_bj", "tp_tier", "selected_tp_pct", "signal_digest"):
            lines.append(f"{k}: {live_signal.get(k)}")
        lines.append("")

    live = report.get("live_trade") or {}
    if live:
        lines.append("[live trade]")
        for k in ("run_id", "projection_schema_version", "strategy_name", "order_root", "reason", "entry_price", "exit_price", "entry_price_source", "exit_price_source", "entry_time_source", "resolved_tp_price", "resolved_tp_price_source", "resolved_sl_price", "selected_tp_pct", "tp_tier", "entry_client_order_id", "entry_exchange_order_id", "tp_order_exchange_id", "sl_order_exchange_id", "time_stop_exchange_order_id", "exit_order_client_id", "exit_order_exchange_id", "exit_order_leg"):
            lines.append(f"{k}: {live.get(k)}")
        lines.append("")

    bn = report.get("bn_trade_facts") or {}
    if bn:
        lines.append("[bn trade facts]")
        for k in ("entry_order_id", "entry_client_order_id", "entry_avg_fill_price", "entry_fill_qty", "exit_order_id", "exit_client_order_id", "exit_avg_fill_price", "exit_fill_qty", "exit_reason_from_bn_order", "income_sum", "commission_sum", "realized_pnl_sum"):
            lines.append(f"{k}: {bn.get(k)}")
        lines.append("")

    comp = report.get("comparison") or {}
    lines.append("[reason comparison]")
    lines.append(f"sim_reason: {comp.get('sim_reason')}")
    lines.append(f"live_reason: {comp.get('live_reason')}")
    lines.append(f"bn_reason  : {comp.get('bn_reason')}")
    lines.append("")

    lines.append("[signal comparison]")
    for key, payload in (comp.get("signal_checks") or {}).items():
        lines.append(f"{key}: same={payload.get('same')} | left={payload.get('left')} | right={payload.get('right')}")
    lines.append("")

    lines.append("[price comparison]")
    for key, payload in (comp.get("price_checks") or {}).items():
        lines.append(f"{key}: same={payload.get('same')} | left={payload.get('left')} | right={payload.get('right')}")
    lines.append("")

    lines.append("[timing comparison]")
    for key, payload in (comp.get("timing_checks") or {}).items():
        lines.append(f"{key}: {payload}")
    lines.append("")
    return "\n".join(lines) + "\n"


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit one trade across sim / live / bn truth layers.")
    p.add_argument("--sim-trades", required=True)
    p.add_argument("--live-signals", required=True)
    p.add_argument("--live-trades", required=True)
    p.add_argument("--live-audit-file", required=True)
    p.add_argument("--bn-orders", required=True)
    p.add_argument("--bn-fills", required=True)
    p.add_argument("--bn-income", default="")
    p.add_argument("--bn-position-facts", default="")
    p.add_argument("--symbol", required=True)
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--c-time-ms", type=int)
    group.add_argument("--c-time-bj")
    p.add_argument("--out-dir", default="output/sim_live_audit")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    symbol = _normalize_symbol(args.symbol)
    c_time_ms = int(args.c_time_ms) if args.c_time_ms is not None else _bj_to_ms(args.c_time_bj)

    sim_trade = _find_sim_trade(Path(args.sim_trades), symbol, c_time_ms)
    live_signal = _find_live_signal(Path(args.live_signals), symbol, c_time_ms)
    live_trade = _find_live_trade(Path(args.live_trades), symbol, c_time_ms, live_signal=live_signal)
    live_scene_group = _group_for_target_from_audit(Path(args.live_audit_file), symbol, c_time_ms)

    bn_orders = _find_bn_orders(Path(args.bn_orders), symbol, live_trade)
    order_ids = {row.get("exchange_order_id") for row in bn_orders if row.get("exchange_order_id") is not None}
    bn_fills = _find_bn_fills(Path(args.bn_fills), symbol, order_ids)

    income_path = Path(args.bn_income) if args.bn_income else None
    posfacts_path = Path(args.bn_position_facts) if args.bn_position_facts else None

    time_start = None
    time_end = None
    if live_trade:
        time_start = _safe_int(live_trade.get("signal_time")) or c_time_ms
        time_end = _safe_int(live_trade.get("exit_time")) or ((time_start or c_time_ms) + 3600_000)
    elif live_signal:
        time_start = _safe_int(live_signal.get("signal_time")) or c_time_ms
        time_end = (time_start or c_time_ms) + 3600_000
    else:
        time_start = c_time_ms
        time_end = c_time_ms + 3600_000

    bn_income = _find_bn_income(income_path, symbol, time_start, time_end)
    bn_position_facts = _find_bn_position_facts(posfacts_path, symbol, _safe_int(live_trade.get("exit_time")) if live_trade else None)

    entry_order = _leg_order(bn_orders, "EN")
    exit_order = None
    if live_trade and live_trade.get("exit_order_exchange_id") is not None:
        for row in bn_orders:
            if row.get("exchange_order_id") == live_trade.get("exit_order_exchange_id"):
                exit_order = row
                break
    if exit_order is None:
        reason = str((live_trade or {}).get("reason") or "").upper().strip()
        if reason == "TAKE_PROFIT":
            exit_order = _leg_order(bn_orders, "TP")
        elif reason == "STOP_LOSS":
            exit_order = _leg_order(bn_orders, "SL")
        elif reason == "TIME_STOP":
            exit_order = _leg_order(bn_orders, "TS")

    entry_fills = [r for r in bn_fills if entry_order and r.get("order_id") == entry_order.get("exchange_order_id")]
    exit_fills = [r for r in bn_fills if exit_order and r.get("order_id") == exit_order.get("exchange_order_id")]

    bn_trade_facts = {
        "entry_order_id": entry_order.get("exchange_order_id") if entry_order else None,
        "entry_client_order_id": entry_order.get("client_order_id") if entry_order else None,
        "entry_avg_fill_price": _weighted_avg_price(entry_fills),
        "entry_fill_qty": _sum_float(entry_fills, "qty"),
        "exit_order_id": exit_order.get("exchange_order_id") if exit_order else None,
        "exit_client_order_id": exit_order.get("client_order_id") if exit_order else None,
        "exit_avg_fill_price": _weighted_avg_price(exit_fills),
        "exit_fill_qty": _sum_float(exit_fills, "qty"),
        "exit_reason_from_bn_order": _determine_bn_exit_reason(exit_order),
        "income_sum": _sum_float(bn_income, "income"),
        "commission_sum": _sum_float(bn_fills, "commission"),
        "realized_pnl_sum": _sum_float(bn_fills, "realized_pnl"),
    }

    sim_reason = sim_trade.get("reason") if sim_trade else None
    live_reason = live_trade.get("reason") if live_trade else None
    bn_reason = bn_trade_facts.get("exit_reason_from_bn_order")

    signal_checks = {
        "live_signal_vs_live_trade_signal_digest": dict(zip(("same", "left", "right"), _compare_values((live_signal or {}).get("signal_digest"), (live_trade or {}).get("signal_digest")))),
        "live_signal_vs_live_trade_selected_tp_pct": dict(zip(("same", "left", "right"), _compare_values((live_signal or {}).get("selected_tp_pct"), (live_trade or {}).get("selected_tp_pct")))),
        "live_signal_vs_live_trade_tp_tier": dict(zip(("same", "left", "right"), _compare_values((live_signal or {}).get("tp_tier"), (live_trade or {}).get("tp_tier"), tol=0.0))),
        "live_signal_vs_live_trade_c_time": dict(zip(("same", "left", "right"), _compare_values((live_signal or {}).get("c_time"), (live_trade or {}).get("c_time")))),
    }

    price_checks = {
        "sim_entry_vs_live_entry": dict(zip(("same","left","right"), _compare_values(sim_trade.get("entry_price") if sim_trade else None, live_trade.get("entry_price") if live_trade else None))),
        "live_entry_vs_bn_entry": dict(zip(("same","left","right"), _compare_values(live_trade.get("entry_price") if live_trade else None, bn_trade_facts.get("entry_avg_fill_price")))),
        "sim_exit_vs_live_exit": dict(zip(("same","left","right"), _compare_values(sim_trade.get("exit_price") if sim_trade else None, live_trade.get("exit_price") if live_trade else None))),
        "live_exit_vs_bn_exit": dict(zip(("same","left","right"), _compare_values(live_trade.get("exit_price") if live_trade else None, bn_trade_facts.get("exit_avg_fill_price")))),
    }

    timing_checks = {
        "sim_signal_time": _safe_int(sim_trade.get("signal_time")) if sim_trade else None,
        "live_signal_projection_time": _safe_int(live_signal.get("signal_time")) if live_signal else None,
        "live_trade_signal_time": _safe_int(live_trade.get("signal_time")) if live_trade else None,
        "live_exit_time": _safe_int(live_trade.get("exit_time")) if live_trade else None,
        "bn_income_rows": len(bn_income),
        "bn_position_facts_rows": len(bn_position_facts),
    }

    report = {
        "target": {"symbol": symbol, "c_time_ms": c_time_ms, "c_time_bj": _to_bj(c_time_ms)},
        "matched_records": {
            "sim_trade": sim_trade is not None,
            "live_signal": live_signal is not None,
            "live_trade": live_trade is not None,
            "live_scene": live_scene_group is not None,
            "bn_orders": len(bn_orders),
            "bn_fills": len(bn_fills),
            "bn_income": len(bn_income),
            "bn_position_facts": len(bn_position_facts),
        },
        "sim_trade": sim_trade,
        "live_signal": live_signal,
        "live_trade": live_trade,
        "live_scene_group": live_scene_group,
        "bn_orders": bn_orders,
        "bn_fills": bn_fills,
        "bn_income": bn_income,
        "bn_position_facts": bn_position_facts,
        "bn_trade_facts": bn_trade_facts,
        "comparison": {
            "sim_reason": sim_reason,
            "live_reason": live_reason,
            "bn_reason": bn_reason,
            "signal_checks": signal_checks,
            "price_checks": price_checks,
            "timing_checks": timing_checks,
        },
    }
    report["layer_status"] = _classify_layer_status(report)

    slug = f"{symbol}.{c_time_ms}"
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_json(out_dir / f"{slug}.trade_triplet.report.json", report)
    summary = _summary_lines(report)
    (out_dir / f"{slug}.trade_triplet.summary.txt").write_text(summary, encoding="utf-8")
    print(summary, end="")
    print(f"wrote: {out_dir / (slug + '.trade_triplet.summary.txt')}")


if __name__ == "__main__":
    main()
