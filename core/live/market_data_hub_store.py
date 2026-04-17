from __future__ import annotations

import json
import os
import pickle
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from core.live.audit_log import get_live_audit_dir

_BJ = timezone(timedelta(hours=8))
_HUB_DIRNAME = "market_data_hub"


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(_BJ).strftime("%Y-%m-%d %H:%M:%S")


def _hub_dir() -> Path:
    path = get_live_audit_dir() / _HUB_DIRNAME
    path.mkdir(parents=True, exist_ok=True)
    return path


def _account_dir(account: str) -> Path:
    account_key = str(account).strip() or "unknown"
    path = _hub_dir() / account_key
    path.mkdir(parents=True, exist_ok=True)
    return path


def _current_dir(account: str) -> Path:
    path = _account_dir(account) / "current"
    path.mkdir(parents=True, exist_ok=True)
    return path


def _daily_dir(account: str, day_bj: str | None) -> Path:
    day_key = str(day_bj or "").strip() or datetime.now(timezone.utc).astimezone(_BJ).strftime("%Y-%m-%d")
    path = _account_dir(account) / "daily" / day_key
    path.mkdir(parents=True, exist_ok=True)
    return path


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    unique_suffix = f".{os.getpid()}.{time.time_ns()}.tmp"
    tmp_path = path.with_name(path.name + unique_suffix)
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")
    os.replace(tmp_path, path)


def write_current_snapshot(account: str, name: str, payload: dict[str, Any]) -> Path:
    path = _current_dir(account) / f"{str(name).strip()}.json"
    _atomic_write_json(path, payload)
    return path


def append_daily_snapshot(account: str, name: str, payload: dict[str, Any], *, day_bj: str | None = None) -> Path:
    path = _daily_dir(account, day_bj) / f"{str(name).strip()}.jsonl"
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        f.flush()
        os.fsync(f.fileno())
    return path



def _atomic_write_bytes(path: Path, data: bytes) -> None:
    unique_suffix = f".{os.getpid()}.{time.time_ns()}.tmp"
    tmp_path = path.with_name(path.name + unique_suffix)
    tmp_path.write_bytes(data)
    os.replace(tmp_path, path)


def _current_json_path(account: str, name: str) -> Path:
    return _current_dir(account) / f"{str(name).strip()}.json"


def _current_pickle_path(account: str, name: str) -> Path:
    return _current_dir(account) / f"{str(name).strip()}.pkl"


def write_current_pickle(account: str, name: str, payload: Any) -> Path:
    path = _current_pickle_path(account, name)
    _atomic_write_bytes(path, pickle.dumps(payload, protocol=pickle.HIGHEST_PROTOCOL))
    return path


def read_current_snapshot(account: str, name: str) -> dict[str, Any] | None:
    path = _current_json_path(account, name)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def read_current_pickle(account: str, name: str) -> Any:
    path = _current_pickle_path(account, name)
    if not path.exists():
        return None
    try:
        return pickle.loads(path.read_bytes())
    except Exception:
        return None


def has_current_pickle(account: str, name: str) -> bool:
    return _current_pickle_path(account, name).exists()


def has_current_snapshot(account: str, name: str) -> bool:
    return _current_json_path(account, name).exists()
