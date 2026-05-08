from __future__ import annotations

import time
from typing import Any, Mapping

from core.live.rate_limit_guard import (
    append_binance_rest_usage_record,
    read_active_binance_rest_ban,
    read_binance_rest_quota_state,
    record_binance_rest_quota,
)

REQUEST_PRIORITY_LOW = "LOW"
REQUEST_PRIORITY_NORMAL = "NORMAL"
REQUEST_PRIORITY_HIGH = "HIGH"
REQUEST_PRIORITY_CRITICAL = "CRITICAL"

_WEIGHT_LIMIT_1M = 2400
_THRESHOLD_LOW_NORMAL = 2000
_THRESHOLD_HIGH = 2300
_THRESHOLD_CRITICAL = 2350

_VALID_PRIORITIES = {
    REQUEST_PRIORITY_LOW,
    REQUEST_PRIORITY_NORMAL,
    REQUEST_PRIORITY_HIGH,
    REQUEST_PRIORITY_CRITICAL,
}


class BinanceRestGatewayRejected(RuntimeError):
    def __init__(
        self,
        *,
        code: str,
        source: str,
        priority: str,
        used_weight_1m: int | None,
        threshold: int | None,
        reason: str,
    ) -> None:
        super().__init__(f"{code}: source={source} priority={priority} reason={reason}")
        self.code = code
        self.source = source
        self.priority = priority
        self.used_weight_1m = used_weight_1m
        self.threshold = threshold
        self.reason = reason


def normalize_priority(priority: str | None) -> str:
    value = str(priority or REQUEST_PRIORITY_NORMAL).upper().strip()
    if value not in _VALID_PRIORITIES:
        raise ValueError(f"unsupported Binance REST priority: {priority!r}")
    return value


def _threshold_for_priority(priority: str) -> int:
    value = normalize_priority(priority)
    if value in {REQUEST_PRIORITY_LOW, REQUEST_PRIORITY_NORMAL}:
        return _THRESHOLD_LOW_NORMAL
    if value == REQUEST_PRIORITY_HIGH:
        return _THRESHOLD_HIGH
    if value == REQUEST_PRIORITY_CRITICAL:
        return _THRESHOLD_CRITICAL
    raise ValueError(f"unsupported Binance REST priority: {priority!r}")


def _reject(
    *,
    code: str,
    source: str,
    priority: str,
    account: str | None,
    method: str | None,
    endpoint: str | None,
    used_weight_1m: int | None,
    threshold: int | None,
    reason: str,
) -> None:
    append_binance_rest_usage_record(
        source=source,
        request_status="rejected_by_gateway",
        priority=priority,
        account=account,
        method=method,
        endpoint=endpoint,
        used_weight_1m=used_weight_1m,
        reject_code=code,
        threshold=threshold,
        weight_limit_1m=_WEIGHT_LIMIT_1M,
        reason=reason,
    )
    raise BinanceRestGatewayRejected(
        code=code,
        source=source,
        priority=priority,
        used_weight_1m=used_weight_1m,
        threshold=threshold,
        reason=reason,
    )


def assert_gateway_allows_request(
    *,
    source: str,
    priority: str | None = None,
    account: str | None = None,
    method: str | None = None,
    endpoint: str | None = None,
) -> dict[str, Any]:
    source_key = str(source or "").strip()
    if not source_key:
        raise ValueError("source must not be empty")
    priority_key = normalize_priority(priority)

    ban = read_active_binance_rest_ban()
    if isinstance(ban, dict):
        _reject(
            code="BN_REST_GATE_BAN_WINDOW_ACTIVE",
            source=source_key,
            priority=priority_key,
            account=account,
            method=method,
            endpoint=endpoint,
            used_weight_1m=None,
            threshold=None,
            reason=f"ban_until_bj={ban.get('ban_until_bj')}",
        )

    quota = read_binance_rest_quota_state()
    if not isinstance(quota, dict):
        return {
            "ok": True,
            "priority": priority_key,
            "used_weight_1m": None,
            "threshold": _threshold_for_priority(priority_key),
        }

    used_raw = quota.get("used_weight_1m")
    minute_bucket_utc = quota.get("minute_bucket_utc")
    if used_raw is None or minute_bucket_utc is None:
        return {
            "ok": True,
            "priority": priority_key,
            "used_weight_1m": None,
            "threshold": _threshold_for_priority(priority_key),
        }
    try:
        used_weight_1m = int(used_raw)
    except Exception:
        return {
            "ok": True,
            "priority": priority_key,
            "used_weight_1m": None,
            "threshold": _threshold_for_priority(priority_key),
        }
    try:
        observed_minute_bucket = int(minute_bucket_utc)
    except Exception:
        observed_minute_bucket = -1
    current_minute_bucket = int(time.time() * 1000) // 60000
    if observed_minute_bucket != current_minute_bucket:
        return {
            "ok": True,
            "priority": priority_key,
            "used_weight_1m": used_weight_1m,
            "threshold": _threshold_for_priority(priority_key),
            "stale_quota_snapshot": True,
        }

    threshold = _threshold_for_priority(priority_key)
    if used_weight_1m >= threshold:
        if priority_key in {REQUEST_PRIORITY_LOW, REQUEST_PRIORITY_NORMAL}:
            code = "BN_REST_GATE_LOW_NORMAL_QUOTA_CLOSED"
        elif priority_key == REQUEST_PRIORITY_HIGH:
            code = "BN_REST_GATE_HIGH_QUOTA_CLOSED"
        else:
            code = "BN_REST_GATE_CRITICAL_QUOTA_CLOSED"
        _reject(
            code=code,
            source=source_key,
            priority=priority_key,
            account=account,
            method=method,
            endpoint=endpoint,
            used_weight_1m=used_weight_1m,
            threshold=threshold,
            reason=f"used_weight_1m={used_weight_1m} threshold={threshold}",
        )

    return {
        "ok": True,
        "priority": priority_key,
        "used_weight_1m": used_weight_1m,
        "threshold": threshold,
    }


def call_client_method(
    account: str,
    *,
    source: str,
    method_name: str,
    priority: str | None = None,
    server_time_field: str | None = None,
    **params: Any,
) -> Any:
    priority_key = normalize_priority(priority)
    assert_gateway_allows_request(
        source=source,
        priority=priority_key,
        account=account,
        method=method_name,
        endpoint=method_name,
    )
    from core.live.binance_client import get_client

    client = get_client(account)
    method = getattr(client, method_name)
    payload = method(**params)
    response = getattr(client, "response", None)
    server_time_utc_ms = None
    server_time_key = str(server_time_field or "").strip()
    if server_time_key and isinstance(payload, Mapping):
        try:
            server_time_utc_ms = int(payload.get(server_time_key) or 0) or None
        except Exception:
            server_time_utc_ms = None
    record_binance_rest_quota(
        source=source,
        headers=getattr(response, "headers", None),
        server_time_utc_ms=server_time_utc_ms,
        priority=priority_key,
        account=account,
        method=method_name,
        endpoint=method_name,
    )
    return payload


def call_futures_public(
    account: str,
    *,
    source: str,
    endpoint: str,
    params: Mapping[str, Any] | None = None,
    priority: str | None = None,
) -> Any:
    priority_key = normalize_priority(priority)
    assert_gateway_allows_request(
        source=source,
        priority=priority_key,
        account=account,
        method="GET",
        endpoint=endpoint,
    )
    from core.live.binance_client import get_client

    client = get_client(account)
    raw_method = getattr(client, "_request_futures_api", None)
    if not callable(raw_method):
        raise RuntimeError("python-binance client missing _request_futures_api")
    data = {k: v for k, v in dict(params or {}).items() if v is not None}
    payload = raw_method("get", endpoint, data=data)
    response = getattr(client, "response", None)
    record_binance_rest_quota(
        source=source,
        headers=getattr(response, "headers", None),
        priority=priority_key,
        account=account,
        method="GET",
        endpoint=endpoint,
    )
    return payload
