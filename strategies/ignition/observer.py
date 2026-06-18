from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

BJ = timezone(timedelta(hours=8))


def _now_bj() -> str:
    return datetime.now(tz=BJ).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")


def _load_json(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"IGN config missing: {path}")
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError(f"IGN config must be object: {path}")
    return data


def _require_mapping(cfg: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    if key not in cfg:
        raise KeyError(f"IGN config missing section: {key}")
    value = cfg[key]
    if not isinstance(value, Mapping):
        raise TypeError(f"IGN config section must be object: {key}")
    return value


def _require_bool(cfg: Mapping[str, Any], key: str) -> bool:
    if key not in cfg:
        raise KeyError(f"IGN config missing bool: {key}")
    value = cfg[key]
    if not isinstance(value, bool):
        raise TypeError(f"IGN config {key} must be bool")
    return value


def _require_str(cfg: Mapping[str, Any], key: str) -> str:
    if key not in cfg:
        raise KeyError(f"IGN config missing string: {key}")
    value = str(cfg[key]).strip()
    if not value:
        raise ValueError(f"IGN config string empty: {key}")
    return value


def _require_int(cfg: Mapping[str, Any], key: str, *, min_value: int | None = None) -> int:
    if key not in cfg:
        raise KeyError(f"IGN config missing int: {key}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"IGN config {key} must be int")
    try:
        out = int(value)
    except Exception as e:
        raise TypeError(f"IGN config {key} must be int") from e
    if min_value is not None and out < int(min_value):
        raise ValueError(f"IGN config {key} must be >= {min_value}")
    return out


def _require_float(cfg: Mapping[str, Any], key: str) -> float:
    if key not in cfg:
        raise KeyError(f"IGN config missing number: {key}")
    value = cfg[key]
    if isinstance(value, bool):
        raise TypeError(f"IGN config {key} must be number")
    try:
        return float(value)
    except Exception as e:
        raise TypeError(f"IGN config {key} must be number") from e


def load_config(path: str) -> dict[str, Any]:
    data = _load_json(path)
    hub = _require_mapping(data, "hub")
    runtime = _require_mapping(data, "runtime")
    structure = _require_mapping(data, "structure")
    early_signal = _require_mapping(data, "early_signal")
    cfg = {
        "enabled": _require_bool(data, "enabled"),
        "account": _require_str(data, "account"),
        "notify_enabled": _require_bool(data, "notify_enabled"),
        "hub": {
            "max_age_secs": _require_int(hub, "max_age_secs", min_value=1),
        },
        "runtime": {
            "loop": _require_bool(runtime, "loop"),
            "interval_secs": _require_int(runtime, "interval_secs", min_value=1),
            "top_n": _require_int(runtime, "top_n", min_value=1),
            "audit_top_n": _require_int(runtime, "audit_top_n", min_value=1),
            "alert_cooldown_secs": _require_int(runtime, "alert_cooldown_secs", min_value=0),
        },
        "structure": {
            "history_window_mins": _require_int(structure, "history_window_mins", min_value=180),
            "min_total_return_180m": _require_float(structure, "min_total_return_180m"),
            "min_positive_segment_count": _require_int(structure, "min_positive_segment_count", min_value=1),
            "min_segment_return": _require_float(structure, "min_segment_return"),
            "max_drawdown_180m": _require_float(structure, "max_drawdown_180m"),
            "max_near_high_drawdown": _require_float(structure, "max_near_high_drawdown"),
            "large_range_bar_pct": _require_float(structure, "large_range_bar_pct"),
            "max_large_range_count": _require_int(structure, "max_large_range_count", min_value=0),
            "large_red_body_pct": _require_float(structure, "large_red_body_pct"),
            "max_large_red_count_60m": _require_int(structure, "max_large_red_count_60m", min_value=0),
            "min_volume_boost_30m": _require_float(structure, "min_volume_boost_30m"),
            "min_low_lift_count": _require_int(structure, "min_low_lift_count", min_value=0),
            "min_structure_score": _require_float(structure, "min_structure_score"),
        },
        "early_signal": {
            "enabled": _require_bool(early_signal, "enabled"),
            "min_total_return_180m": _require_float(early_signal, "min_total_return_180m"),
            "max_total_return_180m": _require_float(early_signal, "max_total_return_180m"),
            "min_recent_return_30m": _require_float(early_signal, "min_recent_return_30m"),
            "min_positive_segment_count": _require_int(early_signal, "min_positive_segment_count", min_value=1),
            "min_volume_boost_30m": _require_float(early_signal, "min_volume_boost_30m"),
            "max_drawdown_180m": _require_float(early_signal, "max_drawdown_180m"),
            "max_near_high_drawdown": _require_float(early_signal, "max_near_high_drawdown"),
            "max_large_red_count_60m": _require_int(early_signal, "max_large_red_count_60m", min_value=0),
            "min_low_lift_count": _require_int(early_signal, "min_low_lift_count", min_value=0),
            "min_structure_score": _require_float(early_signal, "min_structure_score"),
        },
    }
    if int(cfg["structure"]["history_window_mins"]) != 180:
        raise ValueError("IGN first observer version requires structure.history_window_mins == 180")
    return cfg


def _require_columns(df: Any, symbol: str) -> None:
    required = ["open", "high", "low", "close", "quote_asset_volume"]
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise KeyError(f"IGN missing columns for {symbol}: {missing}")


def _max_close_drawdown(closes: Any) -> float:
    peaks = closes.cummax()
    dd = closes / peaks - 1.0
    return abs(float(dd.min()))


def _safe_return(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        raise ValueError("price denominator must be positive")
    return float(numerator / denominator - 1.0)


def _chunk_low_lift_count(lows: Any) -> int:
    chunks = [
        lows.iloc[0:60],
        lows.iloc[60:120],
        lows.iloc[120:150],
        lows.iloc[150:180],
    ]
    chunk_lows = [float(chunk.min()) for chunk in chunks if not chunk.empty]
    count = 0
    for prev, cur in zip(chunk_lows, chunk_lows[1:]):
        if cur >= prev:
            count += 1
    return int(count)


def analyze_symbol_frame(symbol: str, df: Any, structure_cfg: Mapping[str, Any]) -> dict[str, Any]:
    import pandas as pd

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"IGN full_df[{symbol}] must be DataFrame")
    _require_columns(df, symbol)
    need = int(structure_cfg["history_window_mins"])
    if len(df) < need:
        return {
            "symbol": symbol,
            "passed": False,
            "reject_reasons": [f"history_insufficient:{len(df)}<{need}"],
        }
    window = df.sort_index().tail(need).copy()
    if len(window) != 180:
        return {
            "symbol": symbol,
            "passed": False,
            "reject_reasons": [f"history_window_not_180:{len(window)}"],
        }
    opens = window["open"].astype(float)
    highs = window["high"].astype(float)
    lows = window["low"].astype(float)
    closes = window["close"].astype(float)
    quote_vol = window["quote_asset_volume"].astype(float)
    if (opens <= 0).any() or (highs <= 0).any() or (lows <= 0).any() or (closes <= 0).any():
        return {
            "symbol": symbol,
            "passed": False,
            "reject_reasons": ["non_positive_price"],
        }

    r_30m = _safe_return(float(closes.iloc[-1]), float(closes.iloc[-31]))
    r_30_60m = _safe_return(float(closes.iloc[-31]), float(closes.iloc[-61]))
    r_60_120m = _safe_return(float(closes.iloc[-61]), float(closes.iloc[-121]))
    r_120_180m = _safe_return(float(closes.iloc[-121]), float(opens.iloc[0]))
    r_180m = _safe_return(float(closes.iloc[-1]), float(opens.iloc[0]))
    segments = [r_30m, r_30_60m, r_60_120m, r_120_180m]
    positive_segment_count = sum(1 for x in segments if x > 0)

    max_drawdown = _max_close_drawdown(closes)
    high_180m = float(highs.max())
    near_high_drawdown = float((high_180m - float(closes.iloc[-1])) / high_180m)
    bar_ranges = highs / lows - 1.0
    large_range_count = int((bar_ranges > float(structure_cfg["large_range_bar_pct"])).sum())
    red_body = opens.tail(60) / closes.tail(60) - 1.0
    large_red_count_60m = int(((closes.tail(60) < opens.tail(60)) & (red_body > float(structure_cfg["large_red_body_pct"]))).sum())
    recent_30m_vol = float(quote_vol.tail(30).sum())
    previous_150m_avg_30m_vol = float(quote_vol.head(150).sum()) / 5.0
    volume_boost_30m = recent_30m_vol / previous_150m_avg_30m_vol if previous_150m_avg_30m_vol > 0 else 0.0
    low_lift_count = _chunk_low_lift_count(lows)

    reject_reasons: list[str] = []
    if r_180m < float(structure_cfg["min_total_return_180m"]):
        reject_reasons.append("total_return_below_min")
    if positive_segment_count < int(structure_cfg["min_positive_segment_count"]):
        reject_reasons.append("positive_segment_count_below_min")
    if min(segments) < float(structure_cfg["min_segment_return"]):
        reject_reasons.append("segment_return_too_weak")
    if max_drawdown > float(structure_cfg["max_drawdown_180m"]):
        reject_reasons.append("drawdown_too_large")
    if near_high_drawdown > float(structure_cfg["max_near_high_drawdown"]):
        reject_reasons.append("not_near_180m_high")
    if large_range_count > int(structure_cfg["max_large_range_count"]):
        reject_reasons.append("large_range_count_too_high")
    if large_red_count_60m > int(structure_cfg["max_large_red_count_60m"]):
        reject_reasons.append("large_red_count_too_high")
    if volume_boost_30m < float(structure_cfg["min_volume_boost_30m"]):
        reject_reasons.append("volume_boost_below_min")
    if low_lift_count < int(structure_cfg["min_low_lift_count"]):
        reject_reasons.append("low_lift_count_below_min")

    score = 0.0
    score += min(30.0, max(0.0, r_180m / float(structure_cfg["min_total_return_180m"])) * 20.0)
    score += min(20.0, positive_segment_count * 5.0)
    score += min(15.0, max(0.0, volume_boost_30m / float(structure_cfg["min_volume_boost_30m"])) * 10.0)
    score += max(0.0, 15.0 * (1.0 - max_drawdown / max(float(structure_cfg["max_drawdown_180m"]), 1e-9)))
    score += max(0.0, 10.0 * (1.0 - near_high_drawdown / max(float(structure_cfg["max_near_high_drawdown"]), 1e-9)))
    score += min(10.0, low_lift_count * 3.5)
    score = round(float(score), 2)

    if score < float(structure_cfg["min_structure_score"]):
        reject_reasons.append("structure_score_below_min")

    first_ts = int(window.index[0])
    last_ts = int(window.index[-1])
    return {
        "symbol": symbol,
        "passed": not reject_reasons,
        "first_bar_ts": first_ts,
        "first_bar_bj": _fmt_bj_from_ms(first_ts),
        "latest_bar_ts": last_ts,
        "latest_bar_bj": _fmt_bj_from_ms(last_ts),
        "last_close": round(float(closes.iloc[-1]), 10),
        "high_180m": round(high_180m, 10),
        "r_30m": round(r_30m, 6),
        "r_30_60m": round(r_30_60m, 6),
        "r_60_120m": round(r_60_120m, 6),
        "r_120_180m": round(r_120_180m, 6),
        "r_180m": round(r_180m, 6),
        "positive_segment_count": int(positive_segment_count),
        "max_drawdown_180m": round(max_drawdown, 6),
        "near_high_drawdown": round(near_high_drawdown, 6),
        "large_range_count": int(large_range_count),
        "large_red_count_60m": int(large_red_count_60m),
        "volume_boost_30m": round(float(volume_boost_30m), 6),
        "low_lift_count": int(low_lift_count),
        "structure_score": score,
        "reject_reasons": reject_reasons,
    }


def _evaluate_early_signal(row: Mapping[str, Any], early_cfg: Mapping[str, Any]) -> list[str]:
    if not bool(early_cfg["enabled"]):
        return ["early_signal_disabled"]
    required = [
        "r_180m",
        "r_30m",
        "positive_segment_count",
        "volume_boost_30m",
        "max_drawdown_180m",
        "near_high_drawdown",
        "large_red_count_60m",
        "low_lift_count",
        "structure_score",
    ]
    missing = [key for key in required if key not in row]
    if missing:
        return [f"early_missing_metrics:{','.join(missing)}"]

    reject_reasons: list[str] = []
    if float(row["r_180m"]) < float(early_cfg["min_total_return_180m"]):
        reject_reasons.append("early_total_return_below_min")
    if float(row["r_180m"]) > float(early_cfg["max_total_return_180m"]):
        reject_reasons.append("early_total_return_above_max")
    if float(row["r_30m"]) < float(early_cfg["min_recent_return_30m"]):
        reject_reasons.append("early_recent_return_below_min")
    if int(row["positive_segment_count"]) < int(early_cfg["min_positive_segment_count"]):
        reject_reasons.append("early_positive_segment_count_below_min")
    if float(row["volume_boost_30m"]) < float(early_cfg["min_volume_boost_30m"]):
        reject_reasons.append("early_volume_boost_below_min")
    if float(row["max_drawdown_180m"]) > float(early_cfg["max_drawdown_180m"]):
        reject_reasons.append("early_drawdown_too_large")
    if float(row["near_high_drawdown"]) > float(early_cfg["max_near_high_drawdown"]):
        reject_reasons.append("early_not_near_180m_high")
    if int(row["large_red_count_60m"]) > int(early_cfg["max_large_red_count_60m"]):
        reject_reasons.append("early_large_red_count_too_high")
    if int(row["low_lift_count"]) < int(early_cfg["min_low_lift_count"]):
        reject_reasons.append("early_low_lift_count_below_min")
    if float(row["structure_score"]) < float(early_cfg["min_structure_score"]):
        reject_reasons.append("early_structure_score_below_min")
    return reject_reasons


def _reject_summary(results: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in results:
        for reason in row.get("reject_reasons") or []:
            key = str(reason).split(":", 1)[0]
            out[key] = out.get(key, 0) + 1
    return dict(sorted(out.items(), key=lambda item: (-item[1], item[0])))


def _alert_state_path(account: str) -> Path:
    return Path(PROJECT_ROOT) / "state" / "live" / f"ignition_observer_alerts.{account}.json"


def _load_alert_state(account: str) -> dict[str, Any]:
    path = _alert_state_path(account)
    if not path.exists():
        return {"alerts": {}}
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, dict):
        raise TypeError(f"IGN alert state must be object: {path}")
    alerts = data.get("alerts")
    if not isinstance(alerts, dict):
        raise TypeError(f"IGN alert state alerts must be object: {path}")
    return data


def _save_alert_state(account: str, state: Mapping[str, Any]) -> None:
    path = _alert_state_path(account)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, sort_keys=True)
    os.replace(tmp, path)


def _apply_alert_cooldown(
    account: str,
    layer: str,
    candidates: list[dict[str, Any]],
    *,
    cooldown_secs: int,
    now_ms: int,
) -> tuple[list[dict[str, Any]], int]:
    if cooldown_secs <= 0 or not candidates:
        return candidates, 0
    state = _load_alert_state(account)
    alerts = state["alerts"]
    cooldown_ms = int(cooldown_secs) * 1000
    filtered: list[dict[str, Any]] = []
    suppressed = 0
    for item in candidates:
        symbol = str(item["symbol"]).upper().strip()
        key = f"{layer}:{symbol}"
        last_ms = int(alerts.get(key, 0) or 0)
        if last_ms > 0 and now_ms - last_ms < cooldown_ms:
            suppressed += 1
            continue
        filtered.append(item)
        alerts[key] = int(now_ms)
    state["updated_bj"] = _now_bj()
    _save_alert_state(account, state)
    return filtered, suppressed


def _summary_lines(label: str, account: str, candidates: list[dict[str, Any]], scan_id: str, top_n: int) -> list[str]:
    lines = [f"{label} candidates | account={account} | scan_id={scan_id}"]
    for item in candidates[:top_n]:
        lines.append(
            f"{item['symbol']} score={item['structure_score']} "
            f"r180={item['r_180m']:.2%} r30={item['r_30m']:.2%} "
            f"volx={item['volume_boost_30m']:.2f} dd={item['max_drawdown_180m']:.2%}"
        )
    return lines


def _notify_candidates(enabled: bool, account: str, candidates: list[dict[str, Any]], scan_id: str, top_n: int) -> None:
    if not enabled or not candidates:
        return
    from core.message_bridge import send_to_bot

    send_to_bot("\n".join(_summary_lines("🔥 [IGN]", account, candidates, scan_id, top_n)), label="ign")


def _notify_early_candidates(enabled: bool, account: str, candidates: list[dict[str, Any]], scan_id: str, top_n: int) -> None:
    if not enabled or not candidates:
        return
    from core.message_bridge import send_to_bot

    send_to_bot("\n".join(_summary_lines("🌱 [IGN_EARLY]", account, candidates, scan_id, top_n)), label="ign_early")


def scan_once(cfg: Mapping[str, Any]) -> dict[str, Any]:
    if not bool(cfg["enabled"]):
        raise RuntimeError("IGN observer disabled by config")
    from core.live.audit_log import append_stage_record
    from core.live.market_data_hub import load_finalized_candidate_inputs_from_hub

    account = str(cfg["account"]).strip()
    payload = load_finalized_candidate_inputs_from_hub(account, max_age_secs=int(cfg["hub"]["max_age_secs"]))
    if not isinstance(payload, dict):
        raise TypeError("IGN hub payload must be dict")
    full_df = payload.get("full_df")
    if not isinstance(full_df, dict):
        raise KeyError("IGN hub payload missing full_df")

    structure_cfg = cfg["structure"]
    scan_id = uuid.uuid4().hex[:12]
    results: list[dict[str, Any]] = []
    for symbol, frame in sorted(full_df.items()):
        try:
            row = analyze_symbol_frame(str(symbol).upper().strip(), frame, structure_cfg)
            early_reject_reasons = _evaluate_early_signal(row, cfg["early_signal"])
            row["early_passed"] = not early_reject_reasons
            row["early_reject_reasons"] = early_reject_reasons
            results.append(row)
        except Exception as e:
            results.append({
                "symbol": str(symbol).upper().strip(),
                "passed": False,
                "early_passed": False,
                "reject_reasons": [f"analysis_error:{e}"],
                "early_reject_reasons": [f"analysis_error:{e}"],
            })

    passed = sorted(
        [row for row in results if bool(row.get("passed"))],
        key=lambda row: (float(row.get("structure_score") or 0.0), float(row.get("r_180m") or 0.0)),
        reverse=True,
    )
    early_passed = sorted(
        [row for row in results if bool(row.get("early_passed")) and not bool(row.get("passed"))],
        key=lambda row: (float(row.get("structure_score") or 0.0), float(row.get("r_180m") or 0.0)),
        reverse=True,
    )
    rejected = [row for row in results if not bool(row.get("passed"))]
    early_rejected = [row for row in results if not bool(row.get("early_passed"))]
    top_n = int(cfg["runtime"]["top_n"])
    audit_top_n = int(cfg["runtime"]["audit_top_n"])
    now_ms = int(time.time() * 1000)
    alert_cooldown_secs = int(cfg["runtime"]["alert_cooldown_secs"])
    notify_enabled = bool(cfg["notify_enabled"])
    if notify_enabled:
        notify_passed, notify_suppressed = _apply_alert_cooldown(
            account,
            "IGN",
            passed,
            cooldown_secs=alert_cooldown_secs,
            now_ms=now_ms,
        )
        notify_early_passed, notify_early_suppressed = _apply_alert_cooldown(
            account,
            "IGN_EARLY",
            early_passed,
            cooldown_secs=alert_cooldown_secs,
            now_ms=now_ms,
        )
    else:
        notify_passed = passed
        notify_early_passed = early_passed
        notify_suppressed = 0
        notify_early_suppressed = 0
    summary = {
        "scan_id": scan_id,
        "strategy_name": "IGN",
        "strategy_family": "momentum_ignition",
        "account": account,
        "scan_bj": _now_bj(),
        "latest_closed_bar_ts": int(payload.get("latest_closed_bar_ts") or 0),
        "latest_closed_bar_bj": str(payload.get("latest_closed_bar_bj") or ""),
        "symbol_count": int(len(results)),
        "passed_count": int(len(passed)),
        "early_passed_count": int(len(early_passed)),
        "notify_passed_count": int(len(notify_passed)),
        "notify_early_passed_count": int(len(notify_early_passed)),
        "alert_cooldown_secs": int(alert_cooldown_secs),
        "alert_suppressed_count": int(notify_suppressed),
        "early_alert_suppressed_count": int(notify_early_suppressed),
        "top_candidates": passed[:top_n],
        "top_early_candidates": early_passed[:top_n],
        "rejected_summary": _reject_summary(rejected),
        "early_rejected_summary": _reject_summary([
            {"reject_reasons": row.get("early_reject_reasons") or []}
            for row in early_rejected
        ]),
        "audit_top_rejected": sorted(
            rejected,
            key=lambda row: float(row.get("structure_score") or 0.0),
            reverse=True,
        )[:audit_top_n],
    }
    append_stage_record(account, "ignition_observer", summary)
    _notify_candidates(notify_enabled, account, notify_passed, scan_id, top_n)
    _notify_early_candidates(notify_enabled, account, notify_early_passed, scan_id, top_n)
    return summary


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ignition / IGN observer: scan hub 1m bars, audit only")
    parser.add_argument("--config", required=True)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--notify", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging()
    cfg = load_config(args.config)
    if args.loop:
        cfg["runtime"]["loop"] = True
    if args.notify:
        cfg["notify_enabled"] = True
    while True:
        summary = scan_once(cfg)
        logging.info(
            "IGN scan finished | account=%s | symbols=%s | passed=%s | early=%s | notify=%s/%s | suppressed=%s/%s | c_bar=%s",
            summary["account"],
            summary["symbol_count"],
            summary["passed_count"],
            summary["early_passed_count"],
            summary["notify_passed_count"],
            summary["notify_early_passed_count"],
            summary["alert_suppressed_count"],
            summary["early_alert_suppressed_count"],
            summary["latest_closed_bar_bj"],
        )
        if not bool(cfg["runtime"]["loop"]):
            break
        time.sleep(int(cfg["runtime"]["interval_secs"]))


if __name__ == "__main__":
    main()
