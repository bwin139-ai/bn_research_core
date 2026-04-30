from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from filelock import FileLock

from core.runtime_state import get_state_dir

_BJ = ZoneInfo("Asia/Shanghai")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_bj(dt: datetime) -> str:
    return dt.astimezone(_BJ).strftime("%Y-%m-%d %H:%M:%S")


def get_live_audit_dir() -> Path:
    path = get_state_dir() / "live_audit"
    path.mkdir(parents=True, exist_ok=True)
    return path


def get_live_audit_path(account: str, *, day_bj: str | None = None) -> Path:
    account_key = str(account).strip()
    if not account_key:
        raise ValueError("account must not be empty")
    day_key = str(day_bj or '').strip() or _bj_date_key(_now_utc())
    return get_live_audit_dir() / f"snapback_{account_key}.{day_key}.jsonl"


def _strategy_file_key(strategy_name: str) -> str:
    key = str(strategy_name).strip().lower().replace("-", "_")
    if not key:
        raise ValueError("strategy_name must not be empty")
    if not all(ch.isalnum() or ch == "_" for ch in key):
        raise ValueError(f"strategy_name contains unsupported path chars: {strategy_name!r}")
    return key


def get_strategy_live_audit_path(account: str, strategy_name: str, *, day_bj: str | None = None) -> Path:
    account_key = str(account).strip()
    if not account_key:
        raise ValueError("account must not be empty")
    day_key = str(day_bj or '').strip() or _bj_date_key(_now_utc())
    return get_live_audit_dir() / f"{_strategy_file_key(strategy_name)}_{account_key}.{day_key}.jsonl"


def get_stage_audit_dir() -> Path:
    path = get_live_audit_dir() / "stage_audit"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _bj_date_key(dt: datetime) -> str:
    return dt.astimezone(_BJ).strftime("%Y-%m-%d")


def get_stage_audit_path(account: str, stage: str, *, day_bj: str | None = None) -> Path:
    account_key = str(account).strip()
    if not account_key:
        raise ValueError("account must not be empty")
    stage_key = str(stage).strip()
    if not stage_key:
        raise ValueError("stage must not be empty")
    day_key = str(day_bj or '').strip()
    if not day_key:
        raise ValueError("day_bj must not be empty for stage audit")
    return get_stage_audit_dir() / f"snapback_{account_key}.{stage_key}.{day_key}.jsonl"


def _build_record(account: str, event: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    now = _now_utc()
    record: dict[str, Any] = {
        "ts_utc": now.isoformat(),
        "ts_bj": _fmt_bj(now),
        "account": str(account),
        "event": str(event),
        "level": "INFO",
        "run_mode": "live",
    }
    if payload:
        record.update(payload)
    return record


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, set):
        return sorted(_json_default(v) for v in value)
    module = type(value).__module__
    if module.startswith("numpy"):
        try:
            return value.item()
        except Exception:
            pass
    if module.startswith("pandas"):
        try:
            if hasattr(value, "to_pydatetime"):
                return value.to_pydatetime().isoformat()
        except Exception:
            pass
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return str(value)


def _append_json_record(path: Path, record: dict[str, Any]) -> Path:
    lock = FileLock(str(path) + ".lock")
    with lock:
        with path.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False, default=_json_default) + "\n")
            f.flush()
            os.fsync(f.fileno())
    return path


def append_audit_record(account: str, event: str, payload: dict[str, Any] | None = None) -> Path:
    record = _build_record(account, event, payload)
    path = get_live_audit_path(account, day_bj=str(record["ts_bj"])[:10])
    return _append_json_record(path, record)


def append_strategy_audit_record(
    account: str,
    strategy_name: str,
    event: str,
    payload: dict[str, Any] | None = None,
) -> Path:
    record = _build_record(account, event, payload)
    record["strategy_name"] = str(strategy_name).strip()
    path = get_strategy_live_audit_path(account, strategy_name, day_bj=str(record["ts_bj"])[:10])
    return _append_json_record(path, record)


def append_stage_record(account: str, stage: str, payload: dict[str, Any] | None = None) -> Path:
    now = _now_utc()
    record: dict[str, Any] = {
        "ts_utc": now.isoformat(),
        "ts_bj": _fmt_bj(now),
        "account": str(account),
        "run_mode": "live",
        "stage": str(stage),
    }
    if payload:
        record.update(payload)
    path = get_stage_audit_path(
        account,
        stage,
        day_bj=_bj_date_key(now),
    )
    return _append_json_record(path, record)


def write_runner_started(account: str, payload: dict[str, Any] | None = None) -> Path:
    return append_audit_record(account, "runner_started", payload)


def write_runner_heartbeat(account: str, payload: dict[str, Any] | None = None) -> Path:
    return append_audit_record(account, "runner_heartbeat", payload)


def write_event(account: str, event: str, payload: dict[str, Any] | None = None) -> Path:
    return append_audit_record(account, event, payload)


def write_strategy_event(account: str, strategy_name: str, event: str, payload: dict[str, Any] | None = None) -> Path:
    return append_strategy_audit_record(account, strategy_name, event, payload)
