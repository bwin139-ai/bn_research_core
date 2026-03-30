from __future__ import annotations

import time
import uuid
from typing import Any

BROKER_ID = '7Qv8Kw2S'
MAX_CLIENT_ORDER_ID_LEN = 36


def make_order_root() -> str:
    return f"{time.strftime('%m%d%H%M%S')}_{uuid.uuid4().hex[:6]}"


def build_client_order_id(*, broker_id: str = BROKER_ID, strat: str, leg: str, root: str) -> str:
    broker = str(broker_id).strip()
    strat_code = str(strat).upper().strip()
    leg_code = str(leg).upper().strip()
    root_value = str(root).strip()
    if not broker:
        raise ValueError('broker_id must not be empty')
    if not strat_code:
        raise ValueError('strat must not be empty')
    if not leg_code:
        raise ValueError('leg must not be empty')
    if not root_value:
        raise ValueError('root must not be empty')
    client_order_id = f'x-{broker}_{strat_code}_{leg_code}_{root_value}'
    return client_order_id[:MAX_CLIENT_ORDER_ID_LEN]


def parse_client_order_id(client_order_id: str | None, *, broker_id: str = BROKER_ID) -> dict[str, Any]:
    raw = str(client_order_id or '').strip()
    out: dict[str, Any] = {
        'client_order_id': raw,
        'recognized': False,
        'classification': 'UNKNOWN',
        'broker_id': None,
        'strat': None,
        'leg': None,
        'root': None,
    }
    if not raw:
        return out
    if not raw.startswith('x-'):
        out['classification'] = 'MANUAL_OR_EXTERNAL'
        return out
    payload = raw[2:]
    parts = payload.split('_', 3)
    if len(parts) != 4:
        return out
    broker_value, strat_value, leg_value, root_value = parts
    out.update({
        'broker_id': broker_value or None,
        'strat': str(strat_value or '').upper().strip() or None,
        'leg': str(leg_value or '').upper().strip() or None,
        'root': root_value or None,
    })
    if broker_value == str(broker_id).strip():
        out['recognized'] = True
        out['classification'] = 'SYSTEM'
        return out
    out['classification'] = 'MANUAL_OR_EXTERNAL'
    return out


def classify_client_order_id(client_order_id: str | None, *, broker_id: str = BROKER_ID) -> str:
    return str(parse_client_order_id(client_order_id, broker_id=broker_id).get('classification') or 'UNKNOWN')


def is_system_client_order_id(client_order_id: str | None, *, broker_id: str = BROKER_ID) -> bool:
    return bool(parse_client_order_id(client_order_id, broker_id=broker_id).get('recognized'))
