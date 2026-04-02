#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

BJ_TZ = dt.timezone(dt.timedelta(hours=8))
INPUT_METRICS = ["chg_24h", "vol_24h"]
STRUCTURE_METRICS = [
    "drop_pct",
    "drop_window_chg",
    "vol_ratio",
    "s_time",
    "s_close",
    "a_time",
    "a_high_price",
    "ab_bars",
    "b_time",
    "bc_bars",
    "c_time",
    "c_price",
    "b_contract_price",
    "b_index_price",
    "basis_b_pct",
    "rebound_ratio",
]


def _bj_to_ms(s: str) -> int:
    dt_bj = dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=BJ_TZ)
    return int(dt_bj.timestamp() * 1000)


def _to_bj(ms: int | None) -> str | None:
    if ms is None:
        return None
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc).astimezone(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class MatchResult:
    sim_signal: dict[str, Any] | None
    sim_trade: dict[str, Any] | None
    live_signal: dict[str, Any] | None
    live_trade: dict[str, Any] | None
    live_stage3: dict[str, Any] | None
    live_stage5: dict[str, Any] | None


def _load_jsonl_first_match(path: Path, predicate, prefer_key: str | None = None, prefer_value: str | None = None) -> dict[str, Any] | None:
    first_match: dict[str, Any] | None = None
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not predicate(row):
                continue
            if prefer_key is None:
                return row
            if first_match is None:
                first_match = row
            if prefer_value is not None and str(row.get(prefer_key, "")) == prefer_value:
                return row
    return first_match


def _normalize_symbol(s: Any) -> str:
    return str(s or "").upper().strip()


def _safe_int(v: Any) -> int | None:
    if v in (None, ""):
        return None
    try:
        return int(v)
    except Exception:
        return None


def _safe_num(x: Any) -> float | int | None:
    if x is None:
        return None
    try:
        v = float(x)
    except Exception:
        return None
    if abs(v - round(v)) < 1e-12:
        return int(round(v))
    return v


def _compare_values(a: Any, b: Any, tol: float = 1e-6) -> tuple[bool, Any, Any]:
    na = _safe_num(a)
    nb = _safe_num(b)
    if na is not None and nb is not None:
        if abs(float(na) - float(nb)) <= tol:
            return True, na, nb
        return False, na, nb
    return a == b, a, b


def _signal_context(row: dict[str, Any]) -> dict[str, Any]:
    ctx = row.get("context")
    return ctx if isinstance(ctx, dict) else {}


def _signal_c_time(row: dict[str, Any]) -> int | None:
    direct = _safe_int(row.get("c_time"))
    if direct is not None:
        return direct
    return _safe_int(_signal_context(row).get("c_time"))


def _signal_selected_tp_pct(row: dict[str, Any]) -> Any:
    direct = row.get("selected_tp_pct")
    if direct not in (None, ""):
        return direct
    return _signal_context(row).get("selected_tp_pct")


def _signal_tp_tier(row: dict[str, Any]) -> Any:
    direct = row.get("tp_tier")
    if direct not in (None, ""):
        return direct
    return _signal_context(row).get("tp_tier")


def _load_sim_signal(path: Path, symbol: str, c_time_ms: int) -> dict[str, Any] | None:
    return _load_jsonl_first_match(path, lambda r: _normalize_symbol(r.get("symbol")) == symbol and _signal_c_time(r) == c_time_ms)


def _load_sim_trade(path: Path, symbol: str, c_time_ms: int) -> dict[str, Any] | None:
    return _load_jsonl_first_match(path, lambda r: _normalize_symbol(r.get("symbol")) == symbol and _signal_c_time(r) == c_time_ms)


def _load_live_signal(path: Path, symbol: str, c_time_ms: int) -> dict[str, Any] | None:
    return _load_jsonl_first_match(path, lambda r: _normalize_symbol(r.get("symbol")) == symbol and _signal_c_time(r) == c_time_ms)


def _load_live_trade(path: Path, symbol: str, c_time_ms: int, live_signal: dict[str, Any] | None = None) -> dict[str, Any] | None:
    target_digest = str((live_signal or {}).get("signal_digest") or "").strip()
    return _load_jsonl_first_match(
        path,
        lambda r: _normalize_symbol(r.get("symbol")) == symbol and (_signal_c_time(r) == c_time_ms or (target_digest and str(r.get("signal_digest") or "").strip() == target_digest)),
    )


def _load_live_stage3(path: Path, symbol: str, c_time_ms: int) -> dict[str, Any] | None:
    def pred(r: dict[str, Any]) -> bool:
        if _normalize_symbol(r.get("symbol")) != symbol:
            return False
        c_bar_ts = _safe_int(r.get("c_bar_ts"))
        if c_bar_ts is not None:
            return c_bar_ts == c_time_ms
        bar_ts = _safe_int(r.get("bar_ts"))
        return bar_ts is not None and bar_ts - 60000 == c_time_ms

    return _load_jsonl_first_match(path, pred, prefer_key="audit_label", prefer_value="candidate")


def _load_live_stage5(path: Path, symbol: str, c_time_ms: int) -> dict[str, Any] | None:
    return _load_jsonl_first_match(path, lambda r: _normalize_symbol(r.get("symbol")) == symbol and _safe_int(r.get("c_time")) == c_time_ms, prefer_key="logic_selected", prefer_value="True")


def _get_stage3_latest_bar(stage3: dict[str, Any] | None) -> dict[str, Any] | None:
    if not stage3:
        return None
    hb = stage3.get("history_bars") or []
    if not hb:
        return None
    return hb[-1]


def _build_comparison(sim_signal: dict[str, Any] | None, sim_trade: dict[str, Any] | None, live_signal: dict[str, Any] | None, live_trade: dict[str, Any] | None, live_stage3: dict[str, Any] | None, live_stage5: dict[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {
        "top_level": {},
        "input_metrics": {},
        "structure_metrics": {},
        "signal_vs_entry": {},
        "live_signal_vs_trade": {},
        "live_stage5_flags": {},
    }
    if not sim_signal:
        return out

    sim_ctx = _signal_context(sim_signal)
    live_sig = live_signal or {}
    live_ctx = _signal_context(live_sig)
    live3_last = _get_stage3_latest_bar(live_stage3) or {}
    live5 = live_stage5 or {}

    top_pairs = {
        "symbol": (sim_signal.get("symbol"), live_sig.get("symbol")),
        "signal_time": (sim_signal.get("signal_time"), live_sig.get("signal_time")),
        "signal_time_bj": (sim_signal.get("signal_time_bj"), live_sig.get("signal_time_bj")),
        "c_time": (_signal_c_time(sim_signal), _signal_c_time(live_sig)),
        "current_price": (sim_signal.get("current_price"), live_sig.get("current_price")),
        "tp_price": (sim_signal.get("tp_price"), live_sig.get("tp_price")),
        "sl_price": (sim_signal.get("sl_price"), live_sig.get("sl_price")),
        "tp_tier": (_signal_tp_tier(sim_signal), _signal_tp_tier(live_sig)),
        "selected_tp_pct": (_signal_selected_tp_pct(sim_signal), _signal_selected_tp_pct(live_sig)),
    }
    for k, (a, b) in top_pairs.items():
        same, va, vb = _compare_values(a, b)
        out["top_level"][k] = {"same": same, "sim": va, "live": vb}

    if live_stage3 is None:
        out["input_metrics"]["status"] = {"same": None, "sim": "OPTIONAL", "live": "MISSING_STAGE3_ROW"}
        for key in INPUT_METRICS:
            out["input_metrics"][key] = {"same": None, "sim": _safe_num(sim_ctx.get(key)), "live": None}
    else:
        for key in INPUT_METRICS:
            same, va, vb = _compare_values(sim_ctx.get(key), live3_last.get(key))
            out["input_metrics"][key] = {"same": same, "sim": va, "live": vb}

    for key in STRUCTURE_METRICS:
        same, va, vb = _compare_values(sim_ctx.get(key), live_ctx.get(key))
        out["structure_metrics"][key] = {"same": same, "sim": va, "live": vb}

    if sim_trade:
        signal_time = _safe_int(sim_signal.get("signal_time"))
        entry_time = _safe_int(sim_trade.get("entry_time"))
        out["signal_vs_entry"]["entry_time_equals_signal_time"] = {
            "same": signal_time == entry_time,
            "signal_time": signal_time,
            "entry_time": entry_time,
        }
        out["signal_vs_entry"]["entry_time_minus_signal_time_ms"] = (entry_time - signal_time) if (signal_time is not None and entry_time is not None) else None

    if live_trade:
        checks = {
            "signal_digest": ((live_signal or {}).get("signal_digest"), live_trade.get("signal_digest")),
            "selected_tp_pct": (_signal_selected_tp_pct(live_sig), live_trade.get("selected_tp_pct")),
            "tp_tier": (_signal_tp_tier(live_sig), live_trade.get("tp_tier")),
            "c_time": (_signal_c_time(live_sig), _signal_c_time(live_trade)),
        }
        for key, (a, b) in checks.items():
            same, va, vb = _compare_values(a, b, tol=0.0 if key in {"signal_digest", "tp_tier"} else 1e-6)
            out["live_signal_vs_trade"][key] = {"same": same, "live_signal": va, "live_trade": vb}

    if live_stage5:
        out["live_stage5_flags"] = {
            "logic_selected": bool(live5.get("logic_selected")),
            "audit_selected": bool(live5.get("audit_selected")),
            "audit_selected_symbol": live5.get("audit_selected_symbol"),
            "candidate_rank": live5.get("candidate_rank"),
            "is_candidate": live5.get("is_candidate"),
            "stage5_pass": live5.get("stage5_pass"),
            "fail_reason": live5.get("fail_reason"),
        }
    return out


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _first_diff(metric_dict: dict[str, dict[str, Any]]) -> str | None:
    for k, payload in metric_dict.items():
        if not payload.get("same", False):
            return k
    return None


def _build_summary(symbol: str, c_time_ms: int, match: MatchResult, comparison: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("=== sim/live signal snapshot diff ===")
    lines.append(f"symbol: {symbol}")
    lines.append(f"c_time_ms: {c_time_ms}")
    lines.append(f"c_time_bj: {_to_bj(c_time_ms)}")
    lines.append("")

    lines.append("[matched records]")
    lines.append(f"sim_signal : {'YES' if match.sim_signal else 'NO'}")
    lines.append(f"sim_trade  : {'YES' if match.sim_trade else 'NO'}")
    lines.append(f"live_signal: {'YES' if match.live_signal else 'NO'}")
    lines.append(f"live_trade : {'YES' if match.live_trade else 'NO'}")
    lines.append(f"live_stage3: {'YES' if match.live_stage3 else 'NO'}")
    lines.append(f"live_stage5: {'YES' if match.live_stage5 else 'NO'}")
    lines.append("")

    lines.append("[top level comparison]")
    for key, payload in comparison.get("top_level", {}).items():
        lines.append(f"{key}: same={payload['same']} | sim={payload['sim']} | live={payload['live']}")
    lines.append("")

    lines.append("[input metric comparison]")
    for key, payload in comparison.get("input_metrics", {}).items():
        if key == "status":
            lines.append(f"status: same={payload['same']} | sim={payload['sim']} | live={payload['live']}")
        else:
            lines.append(f"{key}: same={payload['same']} | sim={payload['sim']} | live={payload['live']}")
    lines.append("")
    lines.append(f"first_input_diff: {_first_diff({k:v for k,v in comparison.get('input_metrics', {}).items() if k != 'status'}) or 'NONE'}")
    lines.append("")

    lines.append("[structure metric comparison]")
    for key, payload in comparison.get("structure_metrics", {}).items():
        lines.append(f"{key}: same={payload['same']} | sim={payload['sim']} | live={payload['live']}")
    lines.append("")
    lines.append(f"first_structure_diff: {_first_diff(comparison.get('structure_metrics', {})) or 'NONE'}")
    lines.append("")

    if comparison.get("live_signal_vs_trade"):
        lines.append("[live signal vs trade]")
        for key, payload in comparison["live_signal_vs_trade"].items():
            lines.append(f"{key}: same={payload['same']} | live_signal={payload['live_signal']} | live_trade={payload['live_trade']}")
        lines.append("")

    if comparison.get("live_stage5_flags"):
        lines.append("[live stage5 flags]")
        for key, value in comparison["live_stage5_flags"].items():
            lines.append(f"{key}: {value}")
        lines.append("")

    if comparison.get("signal_vs_entry"):
        lines.append("[sim signal vs entry]")
        eq = comparison["signal_vs_entry"].get("entry_time_equals_signal_time") or {}
        lines.append(f"entry_time_equals_signal_time: {eq.get('same')} | signal_time={eq.get('signal_time')} | entry_time={eq.get('entry_time')}")
        lines.append(f"entry_time_minus_signal_time_ms: {comparison['signal_vs_entry'].get('entry_time_minus_signal_time_ms')}")
        lines.append("")

    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit one sim/live signal snapshot by symbol + c_time.")
    p.add_argument("--sim-signals", required=True, help="Path to sim_signals.*.jsonl")
    p.add_argument("--live-signals", required=True, help="Path to live_signals.*.jsonl")
    p.add_argument("--sim-trades", default="", help="Path to sim_trades.*.jsonl")
    p.add_argument("--live-trades", default="", help="Path to live_trades.*.jsonl")
    p.add_argument("--live-stage3", default="", help="Optional path to live stage3_enriched jsonl")
    p.add_argument("--live-stage5", default="", help="Optional path to live stage5_structure_audit jsonl")
    p.add_argument("--symbol", required=True, help="Target symbol, e.g. DUSKUSDT")
    group = p.add_mutually_exclusive_group(required=True)
    group.add_argument("--c-time-ms", type=int, help="C bar open_time_ms")
    group.add_argument("--c-time-bj", help='C bar bj time, e.g. "2026-03-26 07:46:00"')
    p.add_argument("--out-dir", default="output/sim_live_audit", help="Output directory")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    symbol = _normalize_symbol(args.symbol)
    c_time_ms = int(args.c_time_ms) if args.c_time_ms is not None else _bj_to_ms(args.c_time_bj)

    sim_signal = _load_sim_signal(Path(args.sim_signals), symbol, c_time_ms)
    sim_trade = _load_sim_trade(Path(args.sim_trades), symbol, c_time_ms) if args.sim_trades else None
    live_signal = _load_live_signal(Path(args.live_signals), symbol, c_time_ms)
    live_trade = _load_live_trade(Path(args.live_trades), symbol, c_time_ms, live_signal=live_signal) if args.live_trades else None
    live_stage3 = _load_live_stage3(Path(args.live_stage3), symbol, c_time_ms) if args.live_stage3 else None
    live_stage5 = _load_live_stage5(Path(args.live_stage5), symbol, c_time_ms) if args.live_stage5 else None

    match = MatchResult(sim_signal=sim_signal, sim_trade=sim_trade, live_signal=live_signal, live_trade=live_trade, live_stage3=live_stage3, live_stage5=live_stage5)
    comparison = _build_comparison(sim_signal, sim_trade, live_signal, live_trade, live_stage3, live_stage5)

    slug = f"{symbol}.{c_time_ms}"
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    _write_json(out_dir / f"{slug}.sim_signal.json", sim_signal)
    _write_json(out_dir / f"{slug}.sim_trade.json", sim_trade)
    _write_json(out_dir / f"{slug}.live_signal.json", live_signal)
    _write_json(out_dir / f"{slug}.live_trade.json", live_trade)
    _write_json(out_dir / f"{slug}.live_stage3.json", live_stage3)
    _write_json(out_dir / f"{slug}.live_stage5.json", live_stage5)
    _write_json(out_dir / f"{slug}.comparison.json", comparison)

    summary = _build_summary(symbol, c_time_ms, match, comparison)
    (out_dir / f"{slug}.summary.txt").write_text(summary, encoding="utf-8")
    print(summary, end="")
    print(f"wrote: {out_dir / (slug + '.summary.txt')}")


if __name__ == "__main__":
    main()
