from __future__ import annotations

import argparse
import glob
import json
import logging
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from core.message_bridge import send_to_bot
from core.runtime_state import load_json_file, save_json_file_atomic

BJ = ZoneInfo("Asia/Shanghai")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_bj(dt: datetime) -> str:
    return dt.astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")


def _load_config(path: Path) -> dict[str, Any]:
    cfg = load_json_file(path)
    if not isinstance(cfg, dict):
        raise ValueError("process monitor config must be a JSON object")
    for field in (
        "schema_version",
        "notify_label",
        "log_path",
        "state_path",
        "default_alert_repeat_secs",
        "checks",
    ):
        if field not in cfg:
            raise ValueError(f"process monitor config missing required field: {field}")
    if int(cfg["schema_version"]) != 1:
        raise ValueError("process monitor config schema_version must be 1")
    if not isinstance(cfg["checks"], list) or not cfg["checks"]:
        raise ValueError("process monitor config checks must be a non-empty list")
    for idx, check in enumerate(cfg["checks"]):
        _validate_check(check, idx)
    return cfg


def _validate_check(check: Any, idx: int) -> None:
    if not isinstance(check, dict):
        raise ValueError(f"checks[{idx}] must be a JSON object")
    for field in ("name", "type", "min_count", "max_count", "match_all"):
        if field not in check:
            raise ValueError(f"checks[{idx}] missing required field: {field}")
    if str(check["type"]) != "process":
        raise ValueError(f"checks[{idx}] type must be process")
    if not isinstance(check["match_all"], list) or not check["match_all"]:
        raise ValueError(f"checks[{idx}].match_all must be a non-empty list")
    if int(check["min_count"]) < 0:
        raise ValueError(f"checks[{idx}].min_count must be >= 0")
    if int(check["max_count"]) < int(check["min_count"]):
        raise ValueError(f"checks[{idx}].max_count must be >= min_count")
    heartbeat = check.get("heartbeat")
    if heartbeat is None:
        return
    if not isinstance(heartbeat, dict):
        raise ValueError(f"checks[{idx}].heartbeat must be a JSON object")
    for field in ("path_glob", "timestamp_field", "timestamp_type", "max_age_secs"):
        if field not in heartbeat:
            raise ValueError(f"checks[{idx}].heartbeat missing required field: {field}")
    if str(heartbeat["timestamp_type"]) not in {"utc_ms", "bj_datetime"}:
        raise ValueError(f"checks[{idx}].heartbeat.timestamp_type is invalid")
    if int(heartbeat["max_age_secs"]) <= 0:
        raise ValueError(f"checks[{idx}].heartbeat.max_age_secs must be > 0")


def _process_table() -> list[dict[str, Any]]:
    proc = subprocess.run(
        ["ps", "-eo", "pid=,ppid=,stat=,args="],
        check=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    rows: list[dict[str, Any]] = []
    self_pid = os.getpid()
    for line in proc.stdout.splitlines():
        parts = line.strip().split(None, 3)
        if len(parts) != 4:
            continue
        try:
            pid = int(parts[0])
            ppid = int(parts[1])
        except ValueError:
            continue
        if pid == self_pid:
            continue
        rows.append({"pid": pid, "ppid": ppid, "stat": parts[2], "args": parts[3]})
    return rows


def _match_processes(processes: list[dict[str, Any]], fragments: list[Any]) -> list[dict[str, Any]]:
    needles = [str(fragment) for fragment in fragments]
    matched: list[dict[str, Any]] = []
    for proc in processes:
        args = str(proc.get("args") or "")
        if all(needle in args for needle in needles):
            matched.append(proc)
    return matched


def _heartbeat_timestamp(path: Path, field: str, timestamp_type: str) -> datetime:
    data = load_json_file(path)
    if not isinstance(data, dict):
        raise ValueError(f"heartbeat file is not a JSON object: {path}")
    if field not in data:
        raise ValueError(f"heartbeat file missing field {field}: {path}")
    raw = data[field]
    if timestamp_type == "utc_ms":
        return datetime.fromtimestamp(int(raw) / 1000, tz=timezone.utc)
    if timestamp_type == "bj_datetime":
        parsed = datetime.strptime(str(raw), "%Y-%m-%d %H:%M:%S")
        return parsed.replace(tzinfo=BJ).astimezone(timezone.utc)
    raise ValueError(f"unsupported heartbeat timestamp_type: {timestamp_type}")


def _check_heartbeat(heartbeat_cfg: dict[str, Any], now: datetime) -> dict[str, Any]:
    paths = [Path(p) for p in glob.glob(str(heartbeat_cfg["path_glob"]))]
    if not paths:
        return {"ok": False, "reason": "heartbeat_missing", "path_glob": heartbeat_cfg["path_glob"]}
    newest = max(paths, key=lambda p: p.stat().st_mtime)
    ts = _heartbeat_timestamp(
        newest,
        str(heartbeat_cfg["timestamp_field"]),
        str(heartbeat_cfg["timestamp_type"]),
    )
    age_secs = max(0, int((now - ts).total_seconds()))
    max_age_secs = int(heartbeat_cfg["max_age_secs"])
    return {
        "ok": age_secs <= max_age_secs,
        "reason": "heartbeat_ok" if age_secs <= max_age_secs else "heartbeat_stale",
        "path": str(newest),
        "updated_bj": _fmt_bj(ts),
        "age_secs": age_secs,
        "max_age_secs": max_age_secs,
    }


def _run_checks(cfg: dict[str, Any]) -> dict[str, Any]:
    now = _now_utc()
    processes = _process_table()
    results: list[dict[str, Any]] = []
    for check in cfg["checks"]:
        matched = _match_processes(processes, check["match_all"])
        count = len(matched)
        min_count = int(check["min_count"])
        max_count = int(check["max_count"])
        process_ok = min_count <= count <= max_count
        result: dict[str, Any] = {
            "name": str(check["name"]),
            "type": "process",
            "ok": process_ok,
            "reason": "process_ok" if process_ok else "process_count_mismatch",
            "count": count,
            "min_count": min_count,
            "max_count": max_count,
            "matched": [
                {
                    "pid": int(proc["pid"]),
                    "ppid": int(proc["ppid"]),
                    "stat": str(proc["stat"]),
                    "args": str(proc["args"]),
                }
                for proc in matched
            ],
        }
        if process_ok and check.get("heartbeat") is not None:
            heartbeat = _check_heartbeat(check["heartbeat"], now)
            result["heartbeat"] = heartbeat
            if not bool(heartbeat["ok"]):
                result["ok"] = False
                result["reason"] = str(heartbeat["reason"])
        results.append(result)
    failed = [item for item in results if not bool(item["ok"])]
    return {
        "schema_version": 1,
        "checked_utc": now.isoformat(),
        "checked_bj": _fmt_bj(now),
        "status": "ok" if not failed else "warning",
        "failed_count": len(failed),
        "checks": results,
    }


def _append_log(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        f.flush()
        os.fsync(f.fileno())


def _load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": 1, "checks": {}}
    state = load_json_file(path)
    if not isinstance(state, dict):
        raise ValueError(f"process monitor state is not a JSON object: {path}")
    state.setdefault("schema_version", 1)
    state.setdefault("checks", {})
    return state


def _detail_signature(result: dict[str, Any]) -> str:
    heartbeat = result.get("heartbeat")
    stable_heartbeat = None
    if isinstance(heartbeat, dict):
        stable_heartbeat = {
            "reason": heartbeat.get("reason"),
            "path": heartbeat.get("path"),
            "path_glob": heartbeat.get("path_glob"),
            "updated_bj": heartbeat.get("updated_bj"),
            "max_age_secs": heartbeat.get("max_age_secs"),
        }
    payload = {
        "reason": result.get("reason"),
        "count": result.get("count"),
        "matched_pids": [item.get("pid") for item in result.get("matched") or []],
        "heartbeat": stable_heartbeat,
    }
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def _format_issue(result: dict[str, Any]) -> str:
    name = str(result["name"])
    reason = str(result["reason"])
    if reason == "process_count_mismatch":
        return (
            f"{name}: count={result['count']} "
            f"expected={result['min_count']}..{result['max_count']}"
        )
    heartbeat = result.get("heartbeat")
    if isinstance(heartbeat, dict):
        if reason == "heartbeat_missing":
            return f"{name}: heartbeat missing ({heartbeat.get('path_glob')})"
        if reason == "heartbeat_stale":
            return (
                f"{name}: heartbeat stale age={heartbeat.get('age_secs')}s "
                f"threshold={heartbeat.get('max_age_secs')}s"
            )
    return f"{name}: {reason}"


def _notifications(
    cfg: dict[str, Any],
    state: dict[str, Any],
    report: dict[str, Any],
) -> list[str]:
    now_ts = int(time.time())
    repeat_secs = int(cfg["default_alert_repeat_secs"])
    state_checks = state.setdefault("checks", {})
    messages: list[str] = []
    for result in report["checks"]:
        name = str(result["name"])
        ok = bool(result["ok"])
        status = "ok" if ok else "warning"
        signature = _detail_signature(result)
        previous = state_checks.get(name) or {}
        previous_status = str(previous.get("status") or "")
        previous_signature = str(previous.get("signature") or "")
        last_notify_ts = int(previous.get("last_notify_ts") or 0)
        notify = False
        if not ok:
            notify = (
                previous_status != "warning"
                or previous_signature != signature
                or now_ts - last_notify_ts >= repeat_secs
            )
        elif previous_status == "warning":
            notify = True
        if notify:
            if ok:
                messages.append(
                    "[ProcessMonitor] recovered\n"
                    f"check: {name}\n"
                    f"time: {report['checked_bj']}"
                )
            else:
                messages.append(
                    "[ProcessMonitor] warning\n"
                    f"time: {report['checked_bj']}\n"
                    f"{_format_issue(result)}"
                )
            last_notify_ts = now_ts
        state_checks[name] = {
            "status": status,
            "signature": signature,
            "last_notify_ts": last_notify_ts,
            "updated_ts": now_ts,
            "updated_bj": report["checked_bj"],
        }
    return messages


def run_once(config_path: Path) -> dict[str, Any]:
    cfg = _load_config(config_path)
    report = _run_checks(cfg)
    log_path = Path(str(cfg["log_path"]))
    state_path = Path(str(cfg["state_path"]))
    _append_log(log_path, report)
    state = _load_state(state_path)
    for message in _notifications(cfg, state, report):
        send_to_bot(message, label=str(cfg["notify_label"]))
        logging.info("[ProcessMonitor] queued notification: %s", message)
    save_json_file_atomic(state_path, state)
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Monitor expected production processes.")
    parser.add_argument("--config", required=True)
    parser.add_argument("--loop", action="store_true")
    parser.add_argument("--interval-secs", type=int, default=60)
    args = parser.parse_args()
    if args.interval_secs <= 0:
        raise ValueError("--interval-secs must be > 0")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    config_path = Path(args.config)
    while True:
        report = run_once(config_path)
        logging.info(
            "[ProcessMonitor] checked status=%s failed_count=%s",
            report["status"],
            report["failed_count"],
        )
        if not args.loop:
            return
        time.sleep(args.interval_secs)


if __name__ == "__main__":
    main()
