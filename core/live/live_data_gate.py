from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from filelock import FileLock

from core.live.market_data_hub import load_finalized_candidate_inputs_from_hub
from core.runtime_state import get_state_dir

BJ = timezone(timedelta(hours=8))
FINALIZED_PAYLOAD_CB_DEADLINE_SECS = 50
FINALIZED_PAYLOAD_WAIT_POLL_SECS = 1.0
FINALIZED_PAYLOAD_NOT_READY_SUMMARY_SECS = 3600

_NOT_READY_SUMMARY_LAST_EMIT_UTC_MS: dict[tuple[str, str], int] = {}


def fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")


def now_utc_ms() -> int:
    return int(time.time() * 1000)


def bj_day_from_ms(ts_ms: int | None) -> str:
    value = int(ts_ms) if ts_ms is not None else now_utc_ms()
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%Y-%m-%d")


def json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, set):
        return sorted(value)
    if isinstance(value, tuple):
        return list(value)
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


def append_jsonl(path: Path, record: Mapping[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(path) + ".lock")
    with lock:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(dict(record), ensure_ascii=False, default=json_default, separators=(",", ":")) + "\n")
            f.flush()
            os.fsync(f.fileno())
    return path


def payload_int(payload: Mapping[str, Any] | None, key: str) -> int | None:
    if not isinstance(payload, Mapping):
        return None
    raw = payload.get(key)
    if raw in (None, ""):
        return None
    try:
        return int(raw)
    except Exception:
        return None


def finalized_payload_mismatch_reason(
    payload: Mapping[str, Any] | None,
    *,
    expected_latest_closed_bar_ts: int,
    expected_signal_time_ts: int,
) -> str:
    if not isinstance(payload, Mapping):
        return "payload_missing"
    payload_latest_closed_bar_ts = payload_int(payload, "latest_closed_bar_ts")
    if payload_latest_closed_bar_ts != int(expected_latest_closed_bar_ts):
        return (
            "latest_closed_bar_ts_mismatch"
            f"(payload={payload_latest_closed_bar_ts},expected={int(expected_latest_closed_bar_ts)})"
        )
    payload_signal_time_ts = payload_int(payload, "signal_time_ts")
    if payload_signal_time_ts != int(expected_signal_time_ts):
        return (
            "signal_time_ts_mismatch"
            f"(payload={payload_signal_time_ts},expected={int(expected_signal_time_ts)})"
        )
    if not isinstance(payload.get("finalize_summary"), Mapping):
        return "finalize_summary_missing"
    return ""


def wait_finalized_candidate_inputs_for_snapshot(
    account: str,
    *,
    expected_latest_closed_bar_ts: int,
    expected_signal_time_ts: int,
    deadline_secs: int = FINALIZED_PAYLOAD_CB_DEADLINE_SECS,
    poll_secs: float = FINALIZED_PAYLOAD_WAIT_POLL_SECS,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    deadline_utc_ms = int(expected_signal_time_ts) + int(deadline_secs * 1000)
    attempts = 0
    first_attempt_utc_ms: int | None = None
    last_attempt_utc_ms: int | None = None
    last_reason = ""
    last_payload_latest_closed_bar_ts: int | None = None
    last_payload_signal_time_ts: int | None = None
    last_payload_published_bj: str | None = None
    expected_latest_closed_bar_ts = int(expected_latest_closed_bar_ts)
    expected_signal_time_ts = int(expected_signal_time_ts)

    while True:
        attempts += 1
        attempt_utc_ms = now_utc_ms()
        if first_attempt_utc_ms is None:
            first_attempt_utc_ms = attempt_utc_ms
        last_attempt_utc_ms = attempt_utc_ms
        try:
            payload = load_finalized_candidate_inputs_from_hub(account, max_age_secs=None)
            last_payload_latest_closed_bar_ts = payload_int(payload, "latest_closed_bar_ts")
            last_payload_signal_time_ts = payload_int(payload, "signal_time_ts")
            last_payload_published_bj = str(payload.get("published_bj") or "") if isinstance(payload, Mapping) else None
            last_reason = finalized_payload_mismatch_reason(
                payload,
                expected_latest_closed_bar_ts=expected_latest_closed_bar_ts,
                expected_signal_time_ts=expected_signal_time_ts,
            )
            if not last_reason:
                return payload, {
                    "ok": True,
                    "attempts": int(attempts),
                    "first_attempt_utc_ms": first_attempt_utc_ms,
                    "first_attempt_bj": fmt_bj_from_ms(first_attempt_utc_ms),
                    "last_attempt_utc_ms": last_attempt_utc_ms,
                    "last_attempt_bj": fmt_bj_from_ms(last_attempt_utc_ms),
                    "deadline_utc_ms": int(deadline_utc_ms),
                    "deadline_bj": fmt_bj_from_ms(deadline_utc_ms),
                    "last_reason": "",
                    "expected_latest_closed_bar_ts": expected_latest_closed_bar_ts,
                    "expected_signal_time_ts": expected_signal_time_ts,
                    "last_payload_latest_closed_bar_ts": last_payload_latest_closed_bar_ts,
                    "last_payload_signal_time_ts": last_payload_signal_time_ts,
                    "last_payload_published_bj": last_payload_published_bj,
                }
        except Exception as exc:
            last_reason = f"hub_candidate_payload_unavailable: {exc}"

        current_utc_ms = now_utc_ms()
        if current_utc_ms >= deadline_utc_ms:
            return None, {
                "ok": False,
                "attempts": int(attempts),
                "first_attempt_utc_ms": first_attempt_utc_ms,
                "first_attempt_bj": fmt_bj_from_ms(first_attempt_utc_ms),
                "last_attempt_utc_ms": last_attempt_utc_ms,
                "last_attempt_bj": fmt_bj_from_ms(last_attempt_utc_ms),
                "deadline_utc_ms": int(deadline_utc_ms),
                "deadline_bj": fmt_bj_from_ms(deadline_utc_ms),
                "last_reason": last_reason,
                "expected_latest_closed_bar_ts": expected_latest_closed_bar_ts,
                "expected_signal_time_ts": expected_signal_time_ts,
                "last_payload_latest_closed_bar_ts": last_payload_latest_closed_bar_ts,
                "last_payload_signal_time_ts": last_payload_signal_time_ts,
                "last_payload_published_bj": last_payload_published_bj,
            }

        sleep_secs = min(float(poll_secs), max(0.0, (deadline_utc_ms - current_utc_ms) / 1000.0))
        if sleep_secs > 0:
            time.sleep(sleep_secs)


def expected_snapshot_from_signal_check_epoch(signal_check_epoch: float) -> tuple[int, int]:
    signal_time = datetime.fromtimestamp(float(signal_check_epoch), tz=timezone.utc).replace(second=0, microsecond=0)
    signal_time_ts = int(signal_time.timestamp() * 1000)
    latest_closed_bar_ts = signal_time_ts - 60_000
    return latest_closed_bar_ts, signal_time_ts


def finalized_payload_not_ready_audit_path(
    *,
    strategy_name: str,
    account: str,
    day_bj: str | None = None,
) -> Path:
    strategy_key = str(strategy_name).strip().replace("/", "_")
    account_key = str(account).strip()
    if not strategy_key:
        raise ValueError("strategy_name must not be empty")
    if not account_key:
        raise ValueError("account must not be empty")
    day_key = str(day_bj or "").strip() or bj_day_from_ms(None)
    return (
        get_state_dir()
        / "live_audit"
        / "live_data_gate"
        / strategy_key
        / account_key
        / day_key
        / "finalized_payload_not_ready.jsonl"
    )


def _not_ready_audit_paths_for_window(
    *,
    strategy_name: str,
    account: str,
    start_utc_ms: int,
    end_utc_ms: int,
) -> list[Path]:
    start_day = bj_day_from_ms(int(start_utc_ms))
    end_day = bj_day_from_ms(int(end_utc_ms))
    days = [start_day] if start_day == end_day else [start_day, end_day]
    return [
        finalized_payload_not_ready_audit_path(strategy_name=strategy_name, account=account, day_bj=day)
        for day in days
    ]


def _read_not_ready_events_for_window(
    *,
    strategy_name: str,
    account: str,
    start_utc_ms: int,
    end_utc_ms: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in _not_ready_audit_paths_for_window(
        strategy_name=strategy_name,
        account=account,
        start_utc_ms=start_utc_ms,
        end_utc_ms=end_utc_ms,
    ):
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except Exception:
                continue
            if not isinstance(payload, dict):
                continue
            ts = payload_int(payload, "collected_utc_ms")
            if ts is None or ts < int(start_utc_ms) or ts > int(end_utc_ms):
                continue
            rows.append(payload)
    return rows


def maybe_log_finalized_payload_not_ready_summary(
    *,
    strategy_name: str,
    strategy_label: str,
    account: str,
    logger: Any,
    now_ms: int | None = None,
    interval_secs: int = FINALIZED_PAYLOAD_NOT_READY_SUMMARY_SECS,
    window_secs: int = FINALIZED_PAYLOAD_NOT_READY_SUMMARY_SECS,
) -> None:
    now_value = int(now_ms) if now_ms is not None else now_utc_ms()
    key = (str(strategy_name).strip(), str(account).strip())
    last_emit = _NOT_READY_SUMMARY_LAST_EMIT_UTC_MS.get(key)
    if last_emit is None:
        _NOT_READY_SUMMARY_LAST_EMIT_UTC_MS[key] = now_value
        return
    if now_value - int(last_emit) < int(interval_secs) * 1000:
        return
    _NOT_READY_SUMMARY_LAST_EMIT_UTC_MS[key] = now_value
    start_ms = now_value - int(window_secs) * 1000
    events = _read_not_ready_events_for_window(
        strategy_name=strategy_name,
        account=account,
        start_utc_ms=start_ms,
        end_utc_ms=now_value,
    )
    reason_counts: dict[str, int] = {}
    latest_signal_time_bj = None
    for event in events:
        reason = str(event.get("reason") or "unknown")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        signal_time_bj = event.get("expected_signal_time_bj")
        if signal_time_bj:
            latest_signal_time_bj = str(signal_time_bj)
    logger.info(
        "[%s] finalized payload not ready summary | account=%s | window_mins=%s | count=%s | by_reason=%s | latest_signal_time=%s",
        strategy_label,
        account,
        int(int(window_secs) / 60),
        len(events),
        json.dumps(reason_counts, ensure_ascii=False, sort_keys=True),
        latest_signal_time_bj,
    )


def record_finalized_payload_not_ready_event(
    *,
    strategy_name: str,
    strategy_label: str,
    account: str,
    run_id: str,
    loop_iteration: int | None,
    expected_latest_closed_bar_ts: int | None,
    expected_signal_time_ts: int | None,
    candidate_payload_wait: Mapping[str, Any],
    projection_path: str | Path | None,
    logger: Any,
) -> Path:
    now_ms = now_utc_ms()
    wait_payload = dict(candidate_payload_wait)
    reason = str(wait_payload.get("last_reason") or "not_ready")
    expected_latest_closed = int(expected_latest_closed_bar_ts or 0) or None
    expected_signal = int(expected_signal_time_ts or 0) or None
    record = {
        "schema_version": 1,
        "event": "finalized_payload_not_ready",
        "strategy_name": str(strategy_name).strip(),
        "account": str(account).strip(),
        "run_id": str(run_id),
        "loop_iteration": loop_iteration,
        "collected_utc_ms": int(now_ms),
        "collected_bj": fmt_bj_from_ms(now_ms),
        "reason": reason,
        "expected_latest_closed_bar_ts": expected_latest_closed,
        "expected_latest_closed_bar_bj": fmt_bj_from_ms(expected_latest_closed),
        "expected_signal_time_ts": expected_signal,
        "expected_signal_time_bj": fmt_bj_from_ms(expected_signal),
        "attempts": payload_int(wait_payload, "attempts"),
        "deadline_bj": wait_payload.get("deadline_bj"),
        "last_payload_latest_closed_bar_ts": payload_int(wait_payload, "last_payload_latest_closed_bar_ts"),
        "last_payload_latest_closed_bar_bj": fmt_bj_from_ms(payload_int(wait_payload, "last_payload_latest_closed_bar_ts")),
        "last_payload_signal_time_ts": payload_int(wait_payload, "last_payload_signal_time_ts"),
        "last_payload_signal_time_bj": fmt_bj_from_ms(payload_int(wait_payload, "last_payload_signal_time_ts")),
        "last_payload_published_bj": wait_payload.get("last_payload_published_bj"),
        "projection_path": str(projection_path) if projection_path is not None else None,
        "candidate_payload_wait": wait_payload,
    }
    path = append_jsonl(
        finalized_payload_not_ready_audit_path(
            strategy_name=strategy_name,
            account=account,
            day_bj=bj_day_from_ms(now_ms),
        ),
        record,
    )
    maybe_log_finalized_payload_not_ready_summary(
        strategy_name=strategy_name,
        strategy_label=strategy_label,
        account=account,
        logger=logger,
        now_ms=now_ms,
    )
    return path
