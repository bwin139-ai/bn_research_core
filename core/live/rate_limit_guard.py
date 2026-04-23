from __future__ import annotations

import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
_BJ = timezone(timedelta(hours=8))
_SHARED_DIR = PROJECT_ROOT / 'output' / 'shared_market'
_BAN_UNTIL_FILENAME = 'binance_rest_ban_until.shared.json'


def _now_ms() -> int:
    return int(time.time() * 1000)


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(_BJ).strftime('%Y-%m-%d %H:%M:%S')


def _ban_until_path() -> Path:
    _SHARED_DIR.mkdir(parents=True, exist_ok=True)
    return _SHARED_DIR / _BAN_UNTIL_FILENAME


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    tmp_path = path.with_name(f'{path.name}.{os.getpid()}.{time.time_ns()}.tmp')
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False) + '\n', encoding='utf-8')
    os.replace(tmp_path, path)


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
