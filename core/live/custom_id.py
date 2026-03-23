from __future__ import annotations

import time
import uuid

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
