from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from core.live.custom_id import BROKER_ID, classify_client_order_id, parse_client_order_id

BJ = timezone(timedelta(hours=8))


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')


def _symbol(value: Any) -> str:
    return str(value or '').upper().strip()


def _raw_int(value: Any) -> int | None:
    if value in (None, ''):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _raw_float(value: Any) -> float | None:
    if value in (None, ''):
        return None
    try:
        return float(value)
    except Exception:
        return None


def build_raw_record(
    *,
    dataset: str,
    account: str,
    symbol: str,
    sync_run_id: str,
    collected_ms: int,
    source: str,
    raw_payload: dict[str, Any],
) -> dict[str, Any]:
    return {
        '_meta': {
            'dataset': dataset,
            'account': str(account).strip(),
            'symbol': _symbol(symbol),
            'sync_run_id': str(sync_run_id).strip(),
            'collected_ms': int(collected_ms),
            'collected_bj': _fmt_bj_from_ms(int(collected_ms)),
            'source': str(source).strip(),
        },
        **dict(raw_payload or {}),
    }


def normalize_order_record(
    order_row: dict[str, Any],
    *,
    account: str,
    sync_run_id: str,
    collected_ms: int,
) -> dict[str, Any]:
    client_order_id = order_row.get('client_order_id')
    parsed = parse_client_order_id(client_order_id, broker_id=BROKER_ID)
    event_ms = _raw_int(order_row.get('update_time_ms')) or _raw_int(order_row.get('time_ms'))
    return {
        'kind': 'bn_order',
        'account': str(account).strip(),
        'run_mode': 'bn_sync',
        'sync_run_id': str(sync_run_id).strip(),
        'collected_ms': int(collected_ms),
        'collected_bj': _fmt_bj_from_ms(int(collected_ms)),
        'event_ms': event_ms,
        'event_bj': _fmt_bj_from_ms(event_ms),
        'symbol': _symbol(order_row.get('symbol')),
        'exchange_order_id': order_row.get('order_id'),
        'client_order_id': client_order_id,
        'type': order_row.get('type'),
        'status': order_row.get('status'),
        'side': order_row.get('side'),
        'position_side': order_row.get('position_side'),
        'price': _raw_float(order_row.get('price')),
        'avg_price': _raw_float(order_row.get('avg_price')),
        'orig_qty': _raw_float(order_row.get('orig_qty')),
        'executed_qty': _raw_float(order_row.get('executed_qty')),
        'cum_quote': _raw_float(order_row.get('cum_quote')),
        'stop_price': _raw_float(order_row.get('stop_price')),
        'time_ms': _raw_int(order_row.get('time_ms')),
        'time_bj': _fmt_bj_from_ms(_raw_int(order_row.get('time_ms'))),
        'update_time_ms': _raw_int(order_row.get('update_time_ms')),
        'update_time_bj': _fmt_bj_from_ms(_raw_int(order_row.get('update_time_ms'))),
        'working_type': order_row.get('working_type'),
        'orig_type': order_row.get('orig_type'),
        'reduce_only': bool(order_row.get('reduce_only', False)),
        'close_position': bool(order_row.get('close_position', False)),
        'system_origin': classify_client_order_id(client_order_id, broker_id=BROKER_ID),
        'strategy': parsed.get('strat'),
        'leg': parsed.get('leg'),
        'order_root': parsed.get('root'),
        'broker_id': parsed.get('broker_id'),
        'recognized_client_order_id': bool(parsed.get('recognized')),
        'raw_order_ref': {
            'symbol': _symbol(order_row.get('symbol')),
            'order_id': order_row.get('order_id'),
            'client_order_id': client_order_id,
        },
    }


def normalize_fill_record(
    trade_row: dict[str, Any],
    *,
    account: str,
    sync_run_id: str,
    collected_ms: int,
) -> dict[str, Any]:
    client_order_id = trade_row.get('client_order_id')
    parsed = parse_client_order_id(client_order_id, broker_id=BROKER_ID)
    event_ms = _raw_int(trade_row.get('time_ms'))
    return {
        'kind': 'bn_fill',
        'account': str(account).strip(),
        'run_mode': 'bn_sync',
        'sync_run_id': str(sync_run_id).strip(),
        'collected_ms': int(collected_ms),
        'collected_bj': _fmt_bj_from_ms(int(collected_ms)),
        'event_ms': event_ms,
        'event_bj': _fmt_bj_from_ms(event_ms),
        'symbol': _symbol(trade_row.get('symbol')),
        'trade_id': trade_row.get('trade_id'),
        'order_id': trade_row.get('order_id'),
        'client_order_id': client_order_id,
        'side': trade_row.get('side'),
        'position_side': trade_row.get('position_side'),
        'price': _raw_float(trade_row.get('price')),
        'qty': _raw_float(trade_row.get('qty')),
        'quote_qty': _raw_float(trade_row.get('quote_qty')),
        'commission': _raw_float(trade_row.get('commission')),
        'commission_asset': trade_row.get('commission_asset'),
        'realized_pnl': _raw_float(trade_row.get('realized_pnl')),
        'maker': bool(trade_row.get('maker', False)),
        'buyer': trade_row.get('buyer'),
        'system_origin': classify_client_order_id(client_order_id, broker_id=BROKER_ID),
        'strategy': parsed.get('strat'),
        'leg': parsed.get('leg'),
        'order_root': parsed.get('root'),
        'broker_id': parsed.get('broker_id'),
        'recognized_client_order_id': bool(parsed.get('recognized')),
        'raw_trade_ref': {
            'symbol': _symbol(trade_row.get('symbol')),
            'trade_id': trade_row.get('trade_id'),
            'order_id': trade_row.get('order_id'),
        },
    }
