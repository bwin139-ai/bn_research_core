from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from filelock import FileLock

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_BJ = timezone(timedelta(hours=8))
_SHARED_DIR = PROJECT_ROOT / 'output' / 'shared_market'
_BAN_UNTIL_FILENAME = 'binance_rest_ban_until.shared.json'
_QUOTA_STATE_FILENAME = 'binance_rest_quota.shared.json'
_USAGE_LEDGER_DIRNAME = 'binance_rest_usage'
_REQUEST_WEIGHT_LIMIT_1M = 2400
_REQUEST_WEIGHT_GUARD_THRESHOLD_1M = 2200


def _now_ms() -> int:
    return int(time.time() * 1000)


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(_BJ).strftime('%Y-%m-%d %H:%M:%S')


def _ban_until_path() -> Path:
    _SHARED_DIR.mkdir(parents=True, exist_ok=True)
    return _SHARED_DIR / _BAN_UNTIL_FILENAME


def _quota_state_path() -> Path:
    _SHARED_DIR.mkdir(parents=True, exist_ok=True)
    return _SHARED_DIR / _QUOTA_STATE_FILENAME


def _usage_ledger_path(day_bj: str | None = None) -> Path:
    day_key = str(day_bj or '').strip() or _fmt_bj_from_ms(_now_ms())[:10]
    path = _SHARED_DIR / _USAGE_LEDGER_DIRNAME / day_key / 'binance_rest_usage.jsonl'
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp_path = path.with_name(f'{path.name}.{os.getpid()}.{time.time_ns()}.tmp')
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False) + '\n', encoding='utf-8')
    os.replace(tmp_path, path)


def _read_json_dict(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _extract_header_int(headers: Any, name: str) -> int | None:
    if headers is None:
        return None
    target = str(name or '').strip().lower()
    if not target:
        return None
    try:
        items = headers.items()
    except Exception:
        return None
    for key, value in items:
        if str(key or '').strip().lower() != target:
            continue
        try:
            return int(str(value).strip())
        except Exception:
            return None
    return None


def _nonnegative_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except Exception:
        return None
    if parsed < 0:
        return None
    return parsed


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def append_binance_rest_usage_record(
    *,
    source: str,
    request_status: str,
    priority: str | None = None,
    account: str | None = None,
    method: str | None = None,
    endpoint: str | None = None,
    used_weight_1m: int | None = None,
    used_weight_1m_delta: int | None = None,
    order_count_10s: int | None = None,
    order_count_1m: int | None = None,
    minute_bucket_utc: int | None = None,
    observed_utc_ms: int | None = None,
    reject_code: str | None = None,
    threshold: int | None = None,
    weight_limit_1m: int | None = None,
    reason: str | None = None,
) -> Path | None:
    now_ms = _now_ms()
    record = {
        'schema_version': 1,
        'recorded_utc_ms': int(now_ms),
        'recorded_bj': _fmt_bj_from_ms(int(now_ms)),
        'observed_utc_ms': int(observed_utc_ms) if observed_utc_ms is not None else int(now_ms),
        'observed_bj': _fmt_bj_from_ms(int(observed_utc_ms) if observed_utc_ms is not None else int(now_ms)),
        'minute_bucket_utc': int(minute_bucket_utc) if minute_bucket_utc is not None else int((observed_utc_ms or now_ms) // 60000),
        'source': str(source),
        'request_status': str(request_status),
        'priority': str(priority or ''),
        'account': str(account or ''),
        'method': str(method or ''),
        'endpoint': str(endpoint or ''),
        'used_weight_1m': int(used_weight_1m) if used_weight_1m is not None else None,
        'used_weight_1m_delta': int(used_weight_1m_delta) if used_weight_1m_delta is not None else None,
        'order_count_10s': int(order_count_10s) if order_count_10s is not None else None,
        'order_count_1m': int(order_count_1m) if order_count_1m is not None else None,
        'reject_code': str(reject_code or ''),
        'threshold': int(threshold) if threshold is not None else None,
        'weight_limit_1m': int(weight_limit_1m) if weight_limit_1m is not None else None,
        'reason': str(reason or ''),
    }
    path = _usage_ledger_path(str(record['recorded_bj'])[:10])
    try:
        lock = FileLock(str(path) + '.lock')
        with lock:
            with path.open('a', encoding='utf-8') as f:
                f.write(json.dumps(record, ensure_ascii=False, default=_json_default) + '\n')
                f.flush()
                os.fsync(f.fileno())
        return path
    except Exception:
        return None


def _usage_ledger_paths_for_window(now_ms: int, window_minutes: int) -> list[Path]:
    window_ms = max(1, int(window_minutes)) * 60_000
    start_ms = int(now_ms) - window_ms
    days: set[str] = set()
    for ts_ms in (start_ms, int(now_ms)):
        day_key = _fmt_bj_from_ms(ts_ms)[:10]
        days.add(day_key)
    return [_usage_ledger_path(day_key) for day_key in sorted(days)]


def read_binance_rest_usage_summary(
    *,
    window_minutes: int = 30,
    now_ms: int | None = None,
) -> dict[str, Any]:
    current_ms = int(now_ms) if now_ms is not None else _now_ms()
    window_mins = max(1, int(window_minutes))
    start_ms = current_ms - window_mins * 60_000

    rows: list[dict[str, Any]] = []
    for path in _usage_ledger_paths_for_window(current_ms, window_mins):
        if not path.exists():
            continue
        try:
            with path.open('r', encoding='utf-8') as f:
                for line in f:
                    try:
                        row = json.loads(line)
                    except Exception:
                        continue
                    if not isinstance(row, dict):
                        continue
                    try:
                        recorded_ms = int(row.get('recorded_utc_ms') or 0)
                    except Exception:
                        recorded_ms = 0
                    if recorded_ms < start_ms or recorded_ms > current_ms:
                        continue
                    rows.append(row)
        except Exception:
            continue

    rows.sort(key=lambda x: int(x.get('recorded_utc_ms') or 0))
    request_count = len(rows)
    status_counts: dict[str, int] = {}
    priority_counts: dict[str, int] = {}
    method_counts: dict[str, int] = {}
    endpoint_counts: dict[str, int] = {}
    source_counts: dict[str, int] = {}
    minute_weight_max: dict[int, int] = {}
    minute_delta_sum: dict[int, int] = {}
    latest_used_weight_1m: int | None = None
    latest_observed_bj = ''
    latest_source = ''
    order_count_10s_max = 0
    order_count_1m_max = 0

    for row in rows:
        status = str(row.get('request_status') or 'unknown')
        priority = str(row.get('priority') or 'UNKNOWN')
        method = str(row.get('method') or 'UNKNOWN')
        endpoint = str(row.get('endpoint') or 'UNKNOWN')
        source = str(row.get('source') or 'UNKNOWN')
        status_counts[status] = status_counts.get(status, 0) + 1
        priority_counts[priority] = priority_counts.get(priority, 0) + 1
        method_counts[method] = method_counts.get(method, 0) + 1
        endpoint_counts[endpoint] = endpoint_counts.get(endpoint, 0) + 1
        source_counts[source] = source_counts.get(source, 0) + 1

        try:
            minute_bucket = int(row.get('minute_bucket_utc'))
        except Exception:
            minute_bucket = int(int(row.get('recorded_utc_ms') or 0) // 60_000)

        used_raw = row.get('used_weight_1m')
        if used_raw is not None:
            try:
                used_value = int(used_raw)
            except Exception:
                used_value = None
            if used_value is not None and used_value >= 0:
                minute_weight_max[minute_bucket] = max(int(minute_weight_max.get(minute_bucket, 0)), used_value)
                latest_used_weight_1m = used_value
                latest_observed_bj = str(row.get('observed_bj') or '')
                latest_source = source

        delta_raw = row.get('used_weight_1m_delta')
        if delta_raw is not None:
            try:
                minute_delta_sum[minute_bucket] = int(minute_delta_sum.get(minute_bucket, 0)) + max(0, int(delta_raw))
            except Exception:
                pass

        try:
            order_count_10s_max = max(order_count_10s_max, int(row.get('order_count_10s') or 0))
        except Exception:
            pass
        try:
            order_count_1m_max = max(order_count_1m_max, int(row.get('order_count_1m') or 0))
        except Exception:
            pass

    def _top_counts(data: dict[str, int], limit: int = 8) -> list[dict[str, Any]]:
        return [
            {'key': key, 'count': int(count)}
            for key, count in sorted(data.items(), key=lambda item: (-int(item[1]), str(item[0])))[:limit]
        ]

    minute_weights = list(minute_weight_max.values())
    delta_values = list(minute_delta_sum.values())
    return {
        'schema_version': 1,
        'window_minutes': int(window_mins),
        'start_utc_ms': int(start_ms),
        'start_bj': _fmt_bj_from_ms(int(start_ms)),
        'end_utc_ms': int(current_ms),
        'end_bj': _fmt_bj_from_ms(int(current_ms)),
        'request_count': int(request_count),
        'ok_count': int(status_counts.get('ok', 0)),
        'error_count': int(status_counts.get('error', 0)),
        'rejected_by_gateway_count': int(status_counts.get('rejected_by_gateway', 0)),
        'status_counts': dict(sorted(status_counts.items())),
        'priority_counts': dict(sorted(priority_counts.items())),
        'method_counts_top': _top_counts(method_counts),
        'endpoint_counts_top': _top_counts(endpoint_counts),
        'source_counts_top': _top_counts(source_counts),
        'observed_minute_count': int(len(minute_weight_max)),
        'used_weight_1m_total_by_minute_max': int(sum(minute_weights)),
        'used_weight_1m_peak': int(max(minute_weights)) if minute_weights else 0,
        'used_weight_1m_delta_sum_observed': int(sum(delta_values)),
        'latest_used_weight_1m': latest_used_weight_1m,
        'latest_observed_bj': latest_observed_bj,
        'latest_source': latest_source,
        'order_count_10s_max': int(order_count_10s_max),
        'order_count_1m_max': int(order_count_1m_max),
    }


def read_binance_rest_quota_state() -> dict[str, Any] | None:
    return _read_json_dict(_quota_state_path())


def record_binance_rest_quota(
    *,
    source: str,
    headers: Any,
    server_time_utc_ms: int | None = None,
    priority: str | None = None,
    account: str | None = None,
    method: str | None = None,
    endpoint: str | None = None,
    request_status: str = 'ok',
) -> dict[str, Any] | None:
    used_weight_1m = _nonnegative_int(_extract_header_int(headers, 'X-MBX-USED-WEIGHT-1M'))
    order_count_10s = _nonnegative_int(_extract_header_int(headers, 'X-MBX-ORDER-COUNT-10S'))
    order_count_1m = _nonnegative_int(_extract_header_int(headers, 'X-MBX-ORDER-COUNT-1M'))
    if used_weight_1m is None and order_count_10s is None and order_count_1m is None:
        return None

    now_ms = _now_ms()
    observed_ms = int(server_time_utc_ms) if server_time_utc_ms is not None else now_ms
    minute_bucket_utc = int(observed_ms // 60000)
    prev = read_binance_rest_quota_state() or {}
    prev_used_weight_1m = _nonnegative_int(prev.get('used_weight_1m'))
    prev_minute_bucket_utc = _nonnegative_int(prev.get('minute_bucket_utc'))
    used_weight_1m_delta = None
    if (
        used_weight_1m is not None
        and prev_used_weight_1m is not None
        and prev_minute_bucket_utc == minute_bucket_utc
    ):
        try:
            used_weight_1m_delta = max(0, int(used_weight_1m) - int(prev_used_weight_1m))
        except Exception:
            used_weight_1m_delta = None
    state_used_weight_1m = used_weight_1m
    if state_used_weight_1m is None and prev_minute_bucket_utc == minute_bucket_utc:
        state_used_weight_1m = prev_used_weight_1m

    payload = {
        'schema_version': 1,
        'source': str(source),
        'recorded_utc_ms': int(now_ms),
        'recorded_bj': _fmt_bj_from_ms(int(now_ms)),
        'observed_utc_ms': int(observed_ms),
        'observed_bj': _fmt_bj_from_ms(int(observed_ms)),
        'minute_bucket_utc': int(minute_bucket_utc),
        'used_weight_1m': int(state_used_weight_1m) if state_used_weight_1m is not None else None,
        'used_weight_1m_delta': int(used_weight_1m_delta) if used_weight_1m_delta is not None else None,
        'order_count_10s': int(order_count_10s) if order_count_10s is not None else None,
        'order_count_1m': int(order_count_1m) if order_count_1m is not None else None,
        'weight_limit_1m': int(prev.get('weight_limit_1m') or _REQUEST_WEIGHT_LIMIT_1M),
        'weight_guard_threshold_1m': int(prev.get('weight_guard_threshold_1m') or _REQUEST_WEIGHT_GUARD_THRESHOLD_1M),
        'guard_sleep_count_total': int(prev.get('guard_sleep_count_total') or 0),
        'guard_sleep_secs_total': float(prev.get('guard_sleep_secs_total') or 0.0),
        'guard_sleep_last_utc_ms': prev.get('guard_sleep_last_utc_ms'),
        'guard_sleep_last_bj': prev.get('guard_sleep_last_bj'),
        'ban_count_total': int(prev.get('ban_count_total') or 0),
        'ban_last_utc_ms': prev.get('ban_last_utc_ms'),
        'ban_last_bj': prev.get('ban_last_bj'),
    }
    try:
        _atomic_write_json(_quota_state_path(), payload)
    except Exception:
        return None
    append_binance_rest_usage_record(
        source=source,
        request_status=request_status,
        priority=priority,
        account=account,
        method=method,
        endpoint=endpoint,
        used_weight_1m=used_weight_1m,
        used_weight_1m_delta=used_weight_1m_delta,
        order_count_10s=order_count_10s,
        order_count_1m=order_count_1m,
        minute_bucket_utc=minute_bucket_utc,
        observed_utc_ms=observed_ms,
        weight_limit_1m=int(payload.get('weight_limit_1m') or _REQUEST_WEIGHT_LIMIT_1M),
    )
    return payload


def _update_quota_state(mutator) -> dict[str, Any]:
    current = read_binance_rest_quota_state() or {
        'schema_version': 1,
        'recorded_utc_ms': _now_ms(),
        'recorded_bj': _fmt_bj_from_ms(_now_ms()),
        'weight_limit_1m': _REQUEST_WEIGHT_LIMIT_1M,
        'weight_guard_threshold_1m': _REQUEST_WEIGHT_GUARD_THRESHOLD_1M,
        'guard_sleep_count_total': 0,
        'guard_sleep_secs_total': 0.0,
        'ban_count_total': 0,
    }
    payload = mutator(dict(current))
    payload['schema_version'] = 1
    _atomic_write_json(_quota_state_path(), payload)
    return payload


def record_binance_rest_ban(
    *,
    ban_until_utc_ms: int,
    source: str,
    status_code: int | None = None,
    reason: str | None = None,
    url: str | None = None,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now_ms = _now_ms()
    payload = {
        'schema_version': 1,
        'source': str(source),
        'status_code': int(status_code) if status_code is not None else None,
        'reason': str(reason or ''),
        'url': str(url or ''),
        'params': dict(params or {}),
        'recorded_utc_ms': now_ms,
        'recorded_bj': _fmt_bj_from_ms(now_ms),
        'ban_until_utc_ms': int(ban_until_utc_ms),
        'ban_until_bj': _fmt_bj_from_ms(int(ban_until_utc_ms)),
    }
    _atomic_write_json(_ban_until_path(), payload)
    def _mutate_quota_state(current: dict[str, Any]) -> dict[str, Any]:
        current['ban_count_total'] = int(current.get('ban_count_total') or 0) + 1
        current['ban_last_utc_ms'] = int(now_ms)
        current['ban_last_bj'] = _fmt_bj_from_ms(int(now_ms))
        current.setdefault('weight_limit_1m', _REQUEST_WEIGHT_LIMIT_1M)
        current.setdefault('weight_guard_threshold_1m', _REQUEST_WEIGHT_GUARD_THRESHOLD_1M)
        current.setdefault('guard_sleep_count_total', 0)
        current.setdefault('guard_sleep_secs_total', 0.0)
        return current
    try:
        _update_quota_state(_mutate_quota_state)
    except Exception:
        pass
    return payload


def read_active_binance_rest_ban(*, now_ms: int | None = None) -> dict[str, Any] | None:
    path = _ban_until_path()
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    try:
        ban_until_utc_ms = int(payload.get('ban_until_utc_ms') or 0)
    except Exception:
        return None
    current_ms = _now_ms() if now_ms is None else int(now_ms)
    if ban_until_utc_ms <= current_ms:
        return None
    return payload


def sleep_if_binance_rest_banned(*, source: str, pad_secs: float = 1.0) -> float:
    payload = read_active_binance_rest_ban()
    if payload is None:
        return 0.0
    ban_until_utc_ms = int(payload['ban_until_utc_ms'])
    sleep_s = max(0.0, (ban_until_utc_ms - _now_ms()) / 1000.0) + max(0.0, float(pad_secs))
    if sleep_s > 0.0:
        print(
            f'[binance_rest_ban_guard] source={source} sleep={sleep_s:.1f}s '
            f'until_bj={payload.get("ban_until_bj")}',
            flush=True,
        )
        time.sleep(sleep_s)
    return sleep_s


def sleep_if_binance_rest_quota_near_limit(
    *,
    source: str,
    weight_guard_threshold_1m: int = _REQUEST_WEIGHT_GUARD_THRESHOLD_1M,
    weight_limit_1m: int = _REQUEST_WEIGHT_LIMIT_1M,
    pad_secs: float = 1.0,
) -> float:
    payload = read_binance_rest_quota_state()
    if payload is None:
        return 0.0
    used_weight_1m = payload.get('used_weight_1m')
    minute_bucket_utc = payload.get('minute_bucket_utc')
    if used_weight_1m is None or minute_bucket_utc is None:
        return 0.0
    now_ms = _now_ms()
    current_minute_bucket_utc = int(now_ms // 60000)
    if int(minute_bucket_utc) != current_minute_bucket_utc:
        return 0.0
    threshold = max(1, int(weight_guard_threshold_1m))
    limit = max(threshold, int(weight_limit_1m))
    if int(used_weight_1m) < threshold:
        return 0.0
    next_minute_ms = int((current_minute_bucket_utc + 1) * 60000)
    sleep_s = max(0.0, (next_minute_ms - now_ms) / 1000.0) + max(0.0, float(pad_secs))
    if sleep_s <= 0.0:
        return 0.0
    def _mutate_quota_state(current: dict[str, Any]) -> dict[str, Any]:
        current['weight_limit_1m'] = int(limit)
        current['weight_guard_threshold_1m'] = int(threshold)
        current['guard_sleep_count_total'] = int(current.get('guard_sleep_count_total') or 0) + 1
        current['guard_sleep_secs_total'] = float(current.get('guard_sleep_secs_total') or 0.0) + float(sleep_s)
        current['guard_sleep_last_utc_ms'] = int(now_ms)
        current['guard_sleep_last_bj'] = _fmt_bj_from_ms(int(now_ms))
        return current
    try:
        _update_quota_state(_mutate_quota_state)
    except Exception:
        pass
    print(
        f'[binance_rest_quota_guard] source={source} used_weight_1m={int(used_weight_1m)}/{limit} '
        f'sleep={sleep_s:.1f}s until_bj={_fmt_bj_from_ms(next_minute_ms)}',
        flush=True,
    )
    time.sleep(sleep_s)
    return sleep_s
