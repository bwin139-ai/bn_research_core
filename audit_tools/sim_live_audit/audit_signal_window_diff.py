#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import re
from pathlib import Path
from typing import Any, Iterable
import hashlib

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
SIGNAL_SUMMARY_FIELDS = [
    "signal_time",
    "signal_time_bj",
    "current_price",
    "tp_price",
    "sl_price",
    "selected_tp_pct",
    "tp_tier",
]

TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d{3}\b")
STAGE3_SYMBOL_RE = re.compile(r'"symbol"\s*:\s*"([^"]+)"')
STAGE3_C_BAR_TS_RE = re.compile(r'"c_bar_ts"\s*:\s*(\d+)')
RUNNER_STARTED_TOKEN = "[Snapback-Live] runner started"
RUNNER_ERROR_TOKEN = "[Snapback-Live] runner error"
STARTUP_BLOCKED_TOKEN = "startup blocked: reconcile/orphan/state error detected"
FILTERED_KEYBOARD_INTERRUPT = "[FILTERED] KeyboardInterrupt traceback removed"


def _to_bj(ms: int | None) -> str | None:
    if ms is None:
        return None
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc).astimezone(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _parse_bj_datetime_to_ms(text: str | None) -> int | None:
    if text is None:
        return None
    s = str(text).strip()
    if not s:
        return None
    patterns = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
    ]
    last_err = None
    for p in patterns:
        try:
            dt_obj = dt.datetime.strptime(s, p).replace(tzinfo=BJ_TZ)
            return int(dt_obj.timestamp() * 1000)
        except Exception as e:
            last_err = e
    raise ValueError(f"invalid Beijing datetime: {text!r}") from last_err




def _bj_date_text(ms: int | None) -> str | None:
    if ms is None:
        return None
    return dt.datetime.fromtimestamp(ms / 1000.0, tz=dt.timezone.utc).astimezone(BJ_TZ).strftime("%Y-%m-%d")


def _iter_bj_dates_inclusive(start_ms: int | None, end_ms: int | None) -> list[str]:
    if start_ms is None or end_ms is None:
        raise SystemExit("stage3 daily mode requires both start and end c_time")
    start_date = dt.datetime.fromtimestamp(start_ms / 1000.0, tz=dt.timezone.utc).astimezone(BJ_TZ).date()
    end_date = dt.datetime.fromtimestamp(end_ms / 1000.0, tz=dt.timezone.utc).astimezone(BJ_TZ).date()
    if end_date < start_date:
        raise SystemExit("end c_time must be >= start c_time")
    out: list[str] = []
    cur = start_date
    while cur <= end_date:
        out.append(cur.strftime("%Y-%m-%d"))
        cur += dt.timedelta(days=1)
    return out


def _resolve_stage3_daily_paths(stage3_dir: Path | None, account: str | None, start_ms: int | None, end_ms: int | None) -> list[Path]:
    if stage3_dir is None and not account:
        return []
    if stage3_dir is None or not account:
        raise SystemExit("Use --stage3-dir together with --account, or omit both")
    if not stage3_dir.exists():
        raise SystemExit(f"stage3 dir not found: {stage3_dir}")
    if not stage3_dir.is_dir():
        raise SystemExit(f"stage3 dir is not a directory: {stage3_dir}")
    account_norm = str(account).strip()
    if not account_norm:
        raise SystemExit("account must not be empty")
    dates = _iter_bj_dates_inclusive(start_ms, end_ms)
    paths: list[Path] = []
    missing: list[str] = []
    for d in dates:
        p = stage3_dir / f"snapback_{account_norm}.stage3_enriched.{d}.jsonl"
        if p.exists() and p.is_file():
            paths.append(p)
        else:
            missing.append(str(p))
    if missing:
        missing_text = "\n".join(missing)
        raise SystemExit(f"missing stage3 daily files:\n{missing_text}")
    return paths


def _stage3_daily_signature(paths: list[Path]) -> str:
    if not paths:
        return ""
    joined = "|".join(str(p) for p in paths)
    return hashlib.md5(joined.encode("utf-8")).hexdigest()

def _normalize_symbol(s: Any) -> str:
    return str(s or "").upper().strip()


def _safe_int(x: Any) -> int | None:
    if x in (None, ""):
        return None
    try:
        return int(x)
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


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
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
                yield obj


def _stage3_prefilter_key_from_line(line: str) -> tuple[str, int] | None:
    sym_m = STAGE3_SYMBOL_RE.search(line)
    ts_m = STAGE3_C_BAR_TS_RE.search(line)
    if not sym_m or not ts_m:
        return None
    try:
        return _normalize_symbol(sym_m.group(1)), int(ts_m.group(1))
    except Exception:
        return None


def _signal_context(row: dict[str, Any]) -> dict[str, Any]:
    ctx = row.get("context")
    return ctx if isinstance(ctx, dict) else {}


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


def _signal_c_time(row: dict[str, Any]) -> int | None:
    direct = _safe_int(row.get("c_time"))
    if direct is not None:
        return direct
    return _safe_int(_signal_context(row).get("c_time"))


def _signal_time_ms(row: dict[str, Any]) -> int | None:
    for key in ("signal_time", "signal_time_ts", "bar_ts"):
        value = _safe_int(row.get(key))
        if value is not None:
            return value
    c_time = _signal_c_time(row)
    if c_time is None:
        return None
    return c_time + 60000


def _sim_key(row: dict[str, Any]) -> tuple[str, int] | None:
    sym = _normalize_symbol(row.get("symbol"))
    c_time = _signal_c_time(row)
    if not sym or c_time is None:
        return None
    return sym, c_time


def _live_signal_key(row: dict[str, Any]) -> tuple[str, int] | None:
    sym = _normalize_symbol(row.get("symbol"))
    c_time = _signal_c_time(row)
    if not sym or c_time is None:
        return None
    return sym, c_time


def _parse_log_ts_ms(line: str) -> int | None:
    m = TS_RE.match(line)
    if not m:
        return None
    dt_obj = dt.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=BJ_TZ)
    return int(dt_obj.timestamp() * 1000)


def _merge_windows(windows: list[tuple[int, int]]) -> list[tuple[int, int]]:
    if not windows:
        return []
    merged: list[tuple[int, int]] = []
    for start_ms, end_ms in sorted(windows):
        if not merged:
            merged.append((start_ms, end_ms))
            continue
        prev_start, prev_end = merged[-1]
        if start_ms <= prev_end:
            merged[-1] = (prev_start, max(prev_end, end_ms))
        else:
            merged.append((start_ms, end_ms))
    return merged


def _build_live_uptime_windows(path: Path | None) -> list[tuple[int, int]]:
    if path is None:
        return []
    windows: list[tuple[int, int]] = []
    active_start_ms: int | None = None
    last_ts_ms: int | None = None

    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        ts_ms = _parse_log_ts_ms(line)
        if ts_ms is not None:
            last_ts_ms = ts_ms

        if RUNNER_STARTED_TOKEN in line:
            if active_start_ms is not None and last_ts_ms is not None and last_ts_ms >= active_start_ms:
                windows.append((active_start_ms, last_ts_ms))
            active_start_ms = ts_ms
            continue

        if RUNNER_ERROR_TOKEN in line or STARTUP_BLOCKED_TOKEN in line:
            if active_start_ms is not None:
                end_ms = ts_ms if ts_ms is not None else last_ts_ms
                if end_ms is not None and end_ms >= active_start_ms:
                    windows.append((active_start_ms, end_ms))
                active_start_ms = None
            continue

        if FILTERED_KEYBOARD_INTERRUPT in line:
            if active_start_ms is not None and last_ts_ms is not None and last_ts_ms >= active_start_ms:
                windows.append((active_start_ms, last_ts_ms))
            active_start_ms = None
            continue

    if active_start_ms is not None and last_ts_ms is not None and last_ts_ms >= active_start_ms:
        windows.append((active_start_ms, last_ts_ms))
    return _merge_windows(windows)


def _signal_time_in_windows(signal_time_ms: int | None, windows: list[tuple[int, int]]) -> bool:
    if signal_time_ms is None:
        return False
    if not windows:
        return True
    for start_ms, end_ms in windows:
        if start_ms <= signal_time_ms <= end_ms:
            return True
    return False


def _in_window(c_time: int, start_ms: int | None, end_ms: int | None) -> bool:
    if start_ms is not None and c_time < start_ms:
        return False
    if end_ms is not None and c_time > end_ms:
        return False
    return True


def _build_stage3_index(
    paths: list[Path],
    start_ms: int | None,
    end_ms: int | None,
    symbols: set[str] | None,
    target_keys: set[tuple[str, int]] | None = None,
) -> dict[tuple[str, int], dict[str, Any]]:
    out: dict[tuple[str, int], dict[str, Any]] = {}
    if not paths:
        return out
    if target_keys is not None and not target_keys:
        return out
    for path in paths:
        with path.open("r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line:
                    continue
                fast_key = _stage3_prefilter_key_from_line(line)
                if fast_key is None:
                    continue
                if target_keys is not None and fast_key not in target_keys:
                    continue
                sym, c_bar_ts = fast_key
                if symbols and sym not in symbols:
                    continue
                if not _in_window(c_bar_ts, start_ms, end_ms):
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(row, dict):
                    continue
                sym = _normalize_symbol(row.get("symbol"))
                c_bar_ts = _safe_int(row.get("c_bar_ts"))
                if not sym or c_bar_ts is None:
                    continue
                if symbols and sym not in symbols:
                    continue
                if not _in_window(c_bar_ts, start_ms, end_ms):
                    continue
                if target_keys is not None and (sym, c_bar_ts) not in target_keys:
                    continue
                out[(sym, c_bar_ts)] = row
    return out


def _build_stage5_index(path: Path | None, start_ms: int | None, end_ms: int | None, symbols: set[str] | None, uptime_windows: list[tuple[int, int]] | None = None) -> dict[tuple[str, int], dict[str, Any]]:
    out: dict[tuple[str, int], dict[str, Any]] = {}
    if path is None:
        return out
    active_windows = list(uptime_windows or [])
    for row in _iter_jsonl(path):
        key = _live_signal_key(row)
        if key is None:
            continue
        sym, c_time = key
        if symbols and sym not in symbols:
            continue
        if not _in_window(c_time, start_ms, end_ms):
            continue
        if active_windows and not _signal_time_in_windows(_signal_time_ms(row), active_windows):
            continue
        out[key] = row
    return out


def _build_sim_signal_index(path: Path, start_ms: int | None, end_ms: int | None, symbols: set[str] | None) -> dict[tuple[str, int], dict[str, Any]]:
    out: dict[tuple[str, int], dict[str, Any]] = {}
    for row in _iter_jsonl(path):
        key = _sim_key(row)
        if key is None:
            continue
        sym, c_time = key
        if symbols and sym not in symbols:
            continue
        if not _in_window(c_time, start_ms, end_ms):
            continue
        out[key] = row
    return out


def _build_live_signal_index(path: Path, start_ms: int | None, end_ms: int | None, symbols: set[str] | None, uptime_windows: list[tuple[int, int]] | None = None) -> dict[tuple[str, int], dict[str, Any]]:
    out: dict[tuple[str, int], dict[str, Any]] = {}
    active_windows = list(uptime_windows or [])
    for row in _iter_jsonl(path):
        key = _live_signal_key(row)
        if key is None:
            continue
        sym, c_time = key
        if symbols and sym not in symbols:
            continue
        if not _in_window(c_time, start_ms, end_ms):
            continue
        if active_windows and not _signal_time_in_windows(_signal_time_ms(row), active_windows):
            continue
        out[key] = row
    return out


def _build_sim_trade_index(path: Path | None, start_ms: int | None, end_ms: int | None, symbols: set[str] | None) -> dict[tuple[str, int], dict[str, Any]]:
    out: dict[tuple[str, int], dict[str, Any]] = {}
    if path is None:
        return out
    for row in _iter_jsonl(path):
        key = _sim_key(row)
        if key is None:
            continue
        sym, c_time = key
        if symbols and sym not in symbols:
            continue
        if not _in_window(c_time, start_ms, end_ms):
            continue
        out[key] = row
    return out


def _get_stage3_latest_bar(stage3: dict[str, Any] | None) -> dict[str, Any] | None:
    if not stage3:
        return None
    hb = stage3.get("history_bars") or []
    if not hb:
        return None
    return hb[-1]


def _history_count(stage3: dict[str, Any] | None) -> int | None:
    if not stage3:
        return None
    hb = stage3.get("history_bars") or []
    try:
        return len(hb)
    except Exception:
        return None


def _first_diff(metric_dict: dict[str, dict[str, Any]]) -> str | None:
    for k, payload in metric_dict.items():
        if not payload.get("same", False):
            return k
    return None


def _signal_summary(row: dict[str, Any], prefix: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for field in SIGNAL_SUMMARY_FIELDS:
        if field == "selected_tp_pct":
            out[f"{prefix}_{field}"] = _safe_num(_signal_selected_tp_pct(row))
        elif field == "tp_tier":
            out[f"{prefix}_{field}"] = _signal_tp_tier(row)
        else:
            out[f"{prefix}_{field}"] = row.get(field)
    return out


def _compare_match(
    key: tuple[str, int],
    sim_signal: dict[str, Any],
    live_signal: dict[str, Any],
    sim_trade: dict[str, Any] | None,
    live_stage3: dict[str, Any] | None,
    live_stage5: dict[str, Any] | None,
    *,
    input_metrics_enabled: bool,
) -> dict[str, Any]:
    symbol, c_time = key
    sim_ctx = _signal_context(sim_signal)
    live_ctx = _signal_context(live_signal)
    live3_last = _get_stage3_latest_bar(live_stage3) or {}
    row: dict[str, Any] = {
        "symbol": symbol,
        "c_time_ms": c_time,
        "c_time_bj": _to_bj(c_time),
        "sim_signal_time": _signal_time_ms(sim_signal),
        "live_signal_time": _signal_time_ms(live_signal),
        "sim_signal_time_bj": _to_bj(_signal_time_ms(sim_signal)),
        "live_signal_time_bj": _to_bj(_signal_time_ms(live_signal)),
        "sim_trade_present": sim_trade is not None,
        "live_stage3_present": live_stage3 is not None,
        "live_stage5_present": live_stage5 is not None,
        "live_history_count": _history_count(live_stage3),
    }
    row.update(_signal_summary(sim_signal, "sim"))
    row.update(_signal_summary(live_signal, "live"))

    input_comp: dict[str, dict[str, Any]] = {}
    if input_metrics_enabled:
        for k in INPUT_METRICS:
            same, va, vb = _compare_values(sim_ctx.get(k), live3_last.get(k))
            input_comp[k] = {"same": same, "sim": va, "live": vb}
            row[f"input_{k}_same"] = same
            row[f"input_{k}_sim"] = va
            row[f"input_{k}_live"] = vb
        row["input_metrics_status"] = "OK" if live_stage3 is not None else "MISSING_STAGE3_ROW"
        row["first_input_diff"] = _first_diff(input_comp) if live_stage3 is not None else "MISSING_STAGE3_ROW"
    else:
        for k in INPUT_METRICS:
            row[f"input_{k}_same"] = None
            row[f"input_{k}_sim"] = _safe_num(sim_ctx.get(k))
            row[f"input_{k}_live"] = None
        row["input_metrics_status"] = "SKIPPED_NO_STAGE3"
        row["first_input_diff"] = "SKIPPED_NO_STAGE3"

    structure_comp: dict[str, dict[str, Any]] = {}
    all_structure_same = True
    for k in STRUCTURE_METRICS:
        same, va, vb = _compare_values(sim_ctx.get(k), live_ctx.get(k))
        structure_comp[k] = {"same": same, "sim": va, "live": vb}
        row[f"struct_{k}_same"] = same
        row[f"struct_{k}_sim"] = va
        row[f"struct_{k}_live"] = vb
        if not same:
            all_structure_same = False

    row["all_structure_same"] = all_structure_same
    row["first_structure_diff"] = _first_diff(structure_comp)

    if live_stage5 is not None:
        row["stage5_logic_selected"] = bool(live_stage5.get("logic_selected"))
        row["stage5_audit_selected"] = bool(live_stage5.get("audit_selected"))
        row["stage5_candidate_rank"] = live_stage5.get("candidate_rank")
        row["stage5_fail_reason"] = live_stage5.get("fail_reason")
    else:
        row["stage5_logic_selected"] = None
        row["stage5_audit_selected"] = None
        row["stage5_candidate_rank"] = None
        row["stage5_fail_reason"] = None

    if sim_trade is not None:
        signal_time = _signal_time_ms(sim_signal)
        entry_time = _safe_int(sim_trade.get("entry_time"))
        row["entry_time_equals_signal_time"] = signal_time == entry_time
        row["entry_time_minus_signal_time_ms"] = (entry_time - signal_time) if (signal_time is not None and entry_time is not None) else None
        row["trade_reason"] = sim_trade.get("reason")
        row["trade_pnl_pct"] = _safe_num(sim_trade.get("pnl_pct"))
    else:
        row["entry_time_equals_signal_time"] = None
        row["entry_time_minus_signal_time_ms"] = None
        row["trade_reason"] = None
        row["trade_pnl_pct"] = None
    return row


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Compare sim/live signals over a c_time window.")
    ap.add_argument("--sim-signals", required=True)
    ap.add_argument("--sim-trades", default="")
    ap.add_argument("--live-signals", required=True)
    ap.add_argument("--stage3-dir", default="", help="Directory containing snapback_<account>.stage3_enriched.YYYY-MM-DD.jsonl")
    ap.add_argument("--account", default="", help="Account name used to resolve stage3 daily files")
    ap.add_argument("--live-stage5", default="")
    ap.add_argument("--live-log", default="", help="Cleaned live console log used to build uptime windows")
    ap.add_argument("--start-c-time-ms", type=int, default=None)
    ap.add_argument("--end-c-time-ms", type=int, default=None)
    ap.add_argument("--start-c-time-bj", default="", help="Beijing start c_time, e.g. '2026-03-26 07:46:00'")
    ap.add_argument("--end-c-time-bj", default="", help="Beijing end c_time, e.g. '2026-03-26 09:13:00'")
    ap.add_argument("--symbols", default="", help="Comma-separated symbols to include")
    ap.add_argument("--out-dir", default="output/sim_live_audit")
    args = ap.parse_args()

    if args.start_c_time_ms is not None and args.start_c_time_bj:
        raise SystemExit("Use only one of --start-c-time-ms or --start-c-time-bj")
    if args.end_c_time_ms is not None and args.end_c_time_bj:
        raise SystemExit("Use only one of --end-c-time-ms or --end-c-time-bj")

    start_c_time_ms = args.start_c_time_ms
    end_c_time_ms = args.end_c_time_ms
    if start_c_time_ms is None and args.start_c_time_bj:
        start_c_time_ms = _parse_bj_datetime_to_ms(args.start_c_time_bj)
    if end_c_time_ms is None and args.end_c_time_bj:
        end_c_time_ms = _parse_bj_datetime_to_ms(args.end_c_time_bj)

    symbols = {_normalize_symbol(x) for x in args.symbols.split(",") if _normalize_symbol(x)} or None

    sim_signals_path = Path(args.sim_signals)
    sim_trades_path = Path(args.sim_trades) if args.sim_trades else None
    live_signals_path = Path(args.live_signals)
    stage3_dir_path = Path(args.stage3_dir) if args.stage3_dir else None
    account_name = str(args.account).strip() if args.account else ""
    live_stage5_path = Path(args.live_stage5) if args.live_stage5 else None
    live_log_path = Path(args.live_log) if args.live_log else None

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    uptime_windows = _build_live_uptime_windows(live_log_path)
    stage3_paths = _resolve_stage3_daily_paths(stage3_dir_path, account_name or None, start_c_time_ms, end_c_time_ms)
    input_metrics_enabled = bool(stage3_paths)

    sim_signals = _build_sim_signal_index(sim_signals_path, start_c_time_ms, end_c_time_ms, symbols)
    sim_trades = _build_sim_trade_index(sim_trades_path, start_c_time_ms, end_c_time_ms, symbols)
    live_signals = _build_live_signal_index(live_signals_path, start_c_time_ms, end_c_time_ms, symbols, uptime_windows=uptime_windows)

    sim_keys = set(sim_signals.keys())
    live_keys = set(live_signals.keys())
    matched_keys = sorted(sim_keys & live_keys)
    matched_key_set = set(matched_keys)
    sim_only_keys = sorted(sim_keys - live_keys)
    live_only_keys = sorted(live_keys - sim_keys)

    live_stage3 = _build_stage3_index(stage3_paths, start_c_time_ms, end_c_time_ms, symbols, target_keys=matched_key_set)
    live_stage5 = _build_stage5_index(live_stage5_path, start_c_time_ms, end_c_time_ms, symbols, uptime_windows=uptime_windows)

    matched_rows = [
        _compare_match(
            k,
            sim_signals[k],
            live_signals[k],
            sim_trades.get(k),
            live_stage3.get(k),
            live_stage5.get(k),
            input_metrics_enabled=input_metrics_enabled,
        )
        for k in matched_keys
    ]
    sim_only_rows = []
    for k in sim_only_keys:
        sym, c_time = k
        r = sim_signals[k]
        sim_only_rows.append({
            "symbol": sym,
            "c_time_ms": c_time,
            "c_time_bj": _to_bj(c_time),
            "signal_time": _signal_time_ms(r),
            "signal_time_bj": _to_bj(_signal_time_ms(r)),
            "current_price": r.get("current_price"),
            "tp_price": r.get("tp_price"),
            "sl_price": r.get("sl_price"),
            "tp_tier": _signal_tp_tier(r),
            "selected_tp_pct": _safe_num(_signal_selected_tp_pct(r)),
        })
    live_only_rows = []
    for k in live_only_keys:
        sym, c_time = k
        r = live_signals[k]
        live_only_rows.append({
            "symbol": sym,
            "c_time_ms": c_time,
            "c_time_bj": _to_bj(c_time),
            "signal_time": _signal_time_ms(r),
            "signal_time_bj": _to_bj(_signal_time_ms(r)),
            "current_price": r.get("current_price"),
            "tp_price": r.get("tp_price"),
            "sl_price": r.get("sl_price"),
            "tp_tier": _signal_tp_tier(r),
            "selected_tp_pct": _safe_num(_signal_selected_tp_pct(r)),
        })

    first_input_counts: dict[str, int] = {}
    first_structure_counts: dict[str, int] = {}
    strict_matched_count = 0
    for row in matched_rows:
        if input_metrics_enabled:
            fi = row.get("first_input_diff") or "NONE"
            first_input_counts[fi] = first_input_counts.get(fi, 0) + 1
        fs = row.get("first_structure_diff") or "NONE"
        first_structure_counts[fs] = first_structure_counts.get(fs, 0) + 1
        if row.get("all_structure_same"):
            strict_matched_count += 1

    summary = {
        "start_c_time_ms": start_c_time_ms,
        "start_c_time_bj": _to_bj(start_c_time_ms),
        "end_c_time_ms": end_c_time_ms,
        "end_c_time_bj": _to_bj(end_c_time_ms),
        "symbols_filter": sorted(symbols) if symbols else [],
        "live_log_path": str(live_log_path) if live_log_path else "",
        "stage3_dir": str(stage3_dir_path) if stage3_dir_path else "",
        "account": account_name,
        "stage3_daily_file_count": len(stage3_paths),
        "stage3_target_key_count": len(matched_key_set),
        "stage3_daily_files": [str(p) for p in stage3_paths],
        "stage3_daily_signature_md5": _stage3_daily_signature(stage3_paths),
        "uptime_window_count": len(uptime_windows),
        "uptime_windows": [{"start_ms": s, "start_bj": _to_bj(s), "end_ms": e, "end_bj": _to_bj(e)} for s, e in uptime_windows],
        "input_metrics_enabled": input_metrics_enabled,
        "sim_signal_count": len(sim_keys),
        "live_signal_count": len(live_keys),
        "matched_count": len(matched_keys),
        "strict_structure_matched_count": strict_matched_count,
        "sim_only_count": len(sim_only_keys),
        "live_only_count": len(live_only_keys),
        "first_input_diff_counts": dict(sorted(first_input_counts.items())) if input_metrics_enabled else {},
        "first_structure_diff_counts": dict(sorted(first_structure_counts.items())),
    }

    stem = f"window.{start_c_time_ms or 'MIN'}_{end_c_time_ms or 'MAX'}"
    if symbols:
        stem += "." + "_".join(sorted(symbols))

    _write_json(out_dir / f"{stem}.summary.json", summary)
    _write_csv(out_dir / f"{stem}.matched.csv", matched_rows)
    _write_csv(out_dir / f"{stem}.sim_only.csv", sim_only_rows)
    _write_csv(out_dir / f"{stem}.live_only.csv", live_only_rows)

    lines = []
    lines.append("=== sim/live signal window diff ===")
    lines.append(f"start_c_time_ms: {start_c_time_ms}")
    lines.append(f"start_c_time_bj: {_to_bj(start_c_time_ms)}")
    lines.append(f"end_c_time_ms: {end_c_time_ms}")
    lines.append(f"end_c_time_bj: {_to_bj(end_c_time_ms)}")
    lines.append("")
    lines.append("[counts]")
    lines.append(f"sim_signal_count: {len(sim_keys)}")
    lines.append(f"live_signal_count: {len(live_keys)}")
    lines.append(f"matched_count: {len(matched_keys)}")
    lines.append(f"strict_structure_matched_count: {strict_matched_count}")
    lines.append(f"sim_only_count: {len(sim_only_keys)}")
    lines.append(f"live_only_count: {len(live_only_keys)}")
    lines.append("")
    lines.append("[uptime]")
    lines.append(f"live_log_path: {str(live_log_path) if live_log_path else ''}")
    lines.append(f"uptime_window_count: {len(uptime_windows)}")
    lines.append("")
    lines.append("[stage3]")
    lines.append(f"stage3_dir: {str(stage3_dir_path) if stage3_dir_path else ''}")
    lines.append(f"account: {account_name}")
    lines.append(f"stage3_daily_file_count: {len(stage3_paths)}")
    lines.append(f"stage3_target_key_count: {len(matched_key_set)}")
    lines.append("")
    lines.append("[first input diff counts]")
    if input_metrics_enabled:
        for k, v in sorted(first_input_counts.items()):
            lines.append(f"{k}: {v}")
    else:
        lines.append("SKIPPED_NO_STAGE3")
    lines.append("")
    lines.append("[first structure diff counts]")
    for k, v in sorted(first_structure_counts.items()):
        lines.append(f"{k}: {v}")
    (out_dir / f"{stem}.summary.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")

    print("=== sim/live signal window diff ===")
    print(f"start_c_time_ms: {start_c_time_ms}")
    print(f"start_c_time_bj: {_to_bj(start_c_time_ms)}")
    print(f"end_c_time_ms: {end_c_time_ms}")
    print(f"end_c_time_bj: {_to_bj(end_c_time_ms)}")
    print("")
    print("[counts]")
    print(f"sim_signal_count: {len(sim_keys)}")
    print(f"live_signal_count: {len(live_keys)}")
    print(f"matched_count: {len(matched_keys)}")
    print(f"strict_structure_matched_count: {strict_matched_count}")
    print(f"sim_only_count: {len(sim_only_keys)}")
    print(f"live_only_count: {len(live_only_keys)}")
    print("")
    print("[uptime]")
    print(f"live_log_path: {str(live_log_path) if live_log_path else ''}")
    print(f"uptime_window_count: {len(uptime_windows)}")
    print("")
    print("[stage3]")
    print(f"stage3_dir: {str(stage3_dir_path) if stage3_dir_path else ''}")
    print(f"account: {account_name}")
    print(f"stage3_daily_file_count: {len(stage3_paths)}")
    print(f"stage3_target_key_count: {len(matched_key_set)}")
    print(f"input_metrics_enabled: {input_metrics_enabled}")
    print("")
    print(f"wrote: {out_dir / (stem + '.summary.json')}")
    print(f"wrote: {out_dir / (stem + '.matched.csv')}")
    print(f"wrote: {out_dir / (stem + '.sim_only.csv')}")
    print(f"wrote: {out_dir / (stem + '.live_only.csv')}")


if __name__ == "__main__":
    main()
