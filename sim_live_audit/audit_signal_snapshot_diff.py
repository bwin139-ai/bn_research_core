#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


BJ_TZ = dt.timezone(dt.timedelta(hours=8))


def _to_bj(ms: int | None) -> str | None:
    if ms is None:
        return None
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc).astimezone(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")


@dataclass
class MatchResult:
    sim_signal: dict[str, Any] | None
    sim_trade: dict[str, Any] | None
    live_stage3: dict[str, Any] | None
    live_stage5: dict[str, Any] | None


def _load_jsonl_first_match(
    path: Path,
    predicate,
    prefer_key: str | None = None,
    prefer_value: str | None = None,
) -> dict[str, Any] | None:
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


def _get_c_time_from_sim_row(row: dict[str, Any]) -> int | None:
    ctx = row.get("context") or {}
    v = ctx.get("c_time")
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


def _load_sim_signal(path: Path, symbol: str, c_time_ms: int) -> dict[str, Any] | None:
    return _load_jsonl_first_match(
        path,
        lambda r: _normalize_symbol(r.get("symbol")) == symbol and _get_c_time_from_sim_row(r) == c_time_ms,
    )


def _load_sim_trade(path: Path, symbol: str, c_time_ms: int) -> dict[str, Any] | None:
    return _load_jsonl_first_match(
        path,
        lambda r: _normalize_symbol(r.get("symbol")) == symbol and _get_c_time_from_sim_row(r) == c_time_ms,
    )


def _load_live_stage3(path: Path, symbol: str, c_time_ms: int) -> dict[str, Any] | None:
    def pred(r: dict[str, Any]) -> bool:
        if _normalize_symbol(r.get("symbol")) != symbol:
            return False
        c_bar_ts = r.get("c_bar_ts")
        if c_bar_ts is not None:
            try:
                return int(c_bar_ts) == c_time_ms
            except Exception:
                return False
        bar_ts = r.get("bar_ts")
        try:
            return int(bar_ts) - 60000 == c_time_ms
        except Exception:
            return False

    return _load_jsonl_first_match(path, pred, prefer_key="audit_label", prefer_value="candidate")


def _load_live_stage5(path: Path, symbol: str, c_time_ms: int) -> dict[str, Any] | None:
    def pred(r: dict[str, Any]) -> bool:
        if _normalize_symbol(r.get("symbol")) != symbol:
            return False
        try:
            return int(r.get("c_time")) == c_time_ms
        except Exception:
            return False

    return _load_jsonl_first_match(path, pred)


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


def _get_stage3_latest_bar(stage3: dict[str, Any] | None) -> dict[str, Any] | None:
    if not stage3:
        return None
    hb = stage3.get("history_bars") or []
    if not hb:
        return None
    return hb[-1]


def _build_comparison(
    sim_signal: dict[str, Any] | None,
    sim_trade: dict[str, Any] | None,
    live_stage3: dict[str, Any] | None,
    live_stage5: dict[str, Any] | None,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "top_level": {},
        "input_metrics": {},
        "structure_metrics": {},
        "signal_vs_entry": {},
    }
    if not sim_signal:
        return out

    sim_ctx = sim_signal.get("context") or {}
    live5 = live_stage5 or {}
    live3_last = _get_stage3_latest_bar(live_stage3) or {}

    top_pairs = {
        "symbol": (sim_signal.get("symbol"), live5.get("symbol") or (live_stage3 or {}).get("symbol")),
        "sim_signal_time": (sim_signal.get("signal_time"), None),
        "live_signal_time": (None, live5.get("signal_time_ts") or (live_stage3 or {}).get("signal_time_ts")),
        "c_time": (sim_ctx.get("c_time"), live5.get("c_time") or (live_stage3 or {}).get("c_bar_ts")),
        "tp_tier": (sim_ctx.get("tp_tier"), live5.get("tp_tier")),
        "selected_tp_pct": (sim_ctx.get("selected_tp_pct"), live5.get("selected_tp_pct")),
    }
    for k, (a, b) in top_pairs.items():
        same, va, vb = _compare_values(a, b)
        out["top_level"][k] = {"same": same, "sim": va, "live": vb}

    input_metric_sources = {
        "chg_24h": (sim_ctx.get("chg_24h"), live3_last.get("chg_24h")),
        "vol_24h": (sim_ctx.get("vol_24h"), live3_last.get("vol_24h")),
    }
    for key, (a, b) in input_metric_sources.items():
        same, va, vb = _compare_values(a, b)
        out["input_metrics"][key] = {"same": same, "sim": va, "live": vb}

    metric_keys = [
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
    for key in metric_keys:
        same, va, vb = _compare_values(sim_ctx.get(key), live5.get(key))
        out["structure_metrics"][key] = {"same": same, "sim": va, "live": vb}

    if sim_trade:
        signal_time = sim_signal.get("signal_time")
        entry_time = sim_trade.get("entry_time")
        same, va, vb = _compare_values(signal_time, entry_time)
        out["signal_vs_entry"]["entry_time_equals_signal_time"] = {
            "same": same,
            "signal_time": va,
            "entry_time": vb,
        }
        out["signal_vs_entry"]["entry_time_minus_signal_time_ms"] = None
        try:
            out["signal_vs_entry"]["entry_time_minus_signal_time_ms"] = int(entry_time) - int(signal_time)
        except Exception:
            pass

    return out


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _build_summary(symbol: str, c_time_ms: int, match: MatchResult, comparison: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("=== sim/live signal snapshot diff ===")
    lines.append(f"symbol: {symbol}")
    lines.append(f"c_time_ms: {c_time_ms}")
    lines.append(f"c_time_bj: {_to_bj(c_time_ms)}")
    lines.append("")

    lines.append("[matched records]")
    lines.append(f"sim_signal: {'YES' if match.sim_signal else 'NO'}")
    lines.append(f"sim_trade : {'YES' if match.sim_trade else 'NO'}")
    lines.append(f"live_stage3: {'YES' if match.live_stage3 else 'NO'}")
    lines.append(f"live_stage5: {'YES' if match.live_stage5 else 'NO'}")
    lines.append("")

    if match.live_stage3:
        hb = match.live_stage3.get("history_bars") or []
        lines.append("[live stage3 history_bars]")
        lines.append(f"history_count: {len(hb)}")
        if hb:
            lines.append(f"first_bar_ms: {hb[0].get('open_time_ms')} | bj: {hb[0].get('open_time_bj')}")
            lines.append(f"last_bar_ms : {hb[-1].get('open_time_ms')} | bj: {hb[-1].get('open_time_bj')}")
        lines.append("")

    lines.append("[input metric comparison]")
    first_input_diff = None
    for key, payload in comparison.get("input_metrics", {}).items():
        lines.append(f"{key}: same={payload['same']} | sim={payload['sim']} | live={payload['live']}")
        if first_input_diff is None and not payload["same"]:
            first_input_diff = key
    lines.append("")
    lines.append(f"first_input_diff: {first_input_diff or 'NONE'}")
    lines.append("")

    lines.append("[structure metric comparison]")
    first_diff = None
    for key, payload in comparison.get("structure_metrics", {}).items():
        lines.append(f"{key}: same={payload['same']} | sim={payload['sim']} | live={payload['live']}")
        if first_diff is None and not payload["same"]:
            first_diff = key
    lines.append("")
    lines.append(f"first_structure_diff: {first_diff or 'NONE'}")
    lines.append("")

    sve = comparison.get("signal_vs_entry", {})
    if sve:
        lines.append("[sim signal vs entry]")
        eq = sve.get("entry_time_equals_signal_time") or {}
        lines.append(
            f"entry_time_equals_signal_time: {eq.get('same')} | signal_time={eq.get('signal_time')} | entry_time={eq.get('entry_time')}"
        )
        lines.append(f"entry_time_minus_signal_time_ms: {sve.get('entry_time_minus_signal_time_ms')}")
        lines.append("")

    return "\n".join(lines) + "\n"


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Audit one sim/live signal snapshot by symbol + c_time.")
    p.add_argument("--sim-signals", required=True, help="Path to sim_signals.*.jsonl")
    p.add_argument("--sim-trades", default="", help="Path to sim_trades.*.jsonl")
    p.add_argument("--live-stage3", required=True, help="Path to live stage3_enriched jsonl")
    p.add_argument("--live-stage5", required=True, help="Path to live stage5_structure_audit jsonl")
    p.add_argument("--symbol", required=True, help="Target symbol, e.g. DUSKUSDT")
    p.add_argument("--c-time-ms", required=True, type=int, help="C bar open_time_ms")
    p.add_argument("--out-dir", default="output/sim_live_audit", help="Output directory")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    symbol = _normalize_symbol(args.symbol)
    c_time_ms = int(args.c_time_ms)

    sim_signal = _load_sim_signal(Path(args.sim_signals), symbol, c_time_ms)
    sim_trade = _load_sim_trade(Path(args.sim_trades), symbol, c_time_ms) if args.sim_trades else None
    live_stage3 = _load_live_stage3(Path(args.live_stage3), symbol, c_time_ms)
    live_stage5 = _load_live_stage5(Path(args.live_stage5), symbol, c_time_ms)

    match = MatchResult(
        sim_signal=sim_signal,
        sim_trade=sim_trade,
        live_stage3=live_stage3,
        live_stage5=live_stage5,
    )
    comparison = _build_comparison(sim_signal, sim_trade, live_stage3, live_stage5)

    slug = f"{symbol}.{c_time_ms}"
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    _write_json(out_dir / f"{slug}.sim_signal.json", sim_signal)
    _write_json(out_dir / f"{slug}.sim_trade.json", sim_trade)
    _write_json(out_dir / f"{slug}.live_stage3.json", live_stage3)
    _write_json(out_dir / f"{slug}.live_stage5.json", live_stage5)
    _write_json(out_dir / f"{slug}.comparison.json", comparison)

    summary = _build_summary(symbol, c_time_ms, match, comparison)
    (out_dir / f"{slug}.summary.txt").write_text(summary, encoding="utf-8")
    print(summary, end="")
    print(f"wrote: {out_dir / (slug + '.summary.txt')}")


if __name__ == "__main__":
    main()
