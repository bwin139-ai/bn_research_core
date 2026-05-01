from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from core.live.market_data_hub import load_finalized_candidate_inputs_from_hub

BJ = timezone(timedelta(hours=8))
FINALIZED_PAYLOAD_CB_DEADLINE_SECS = 50
FINALIZED_PAYLOAD_WAIT_POLL_SECS = 1.0


def fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")


def now_utc_ms() -> int:
    return int(time.time() * 1000)


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
