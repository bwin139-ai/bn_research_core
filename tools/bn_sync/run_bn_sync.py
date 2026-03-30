#!/usr/bin/env python3
from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from core.bn_sync.checkpoint import (
    get_symbol_watermark,
    load_checkpoint,
    save_checkpoint,
    update_symbol_watermark,
)
from core.bn_sync.normalize import (
    build_raw_record,
    normalize_fill_record,
    normalize_order_record,
)
from core.bn_sync.storage import append_jsonl_unique
from core.live.binance_exec import get_account_trades, get_all_orders

BJ = timezone(timedelta(hours=8))


def _now_ms() -> int:
    return int(time.time() * 1000)


def _fmt_bj_from_ms(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime('%Y-%m-%d %H:%M:%S')


def _default_sync_run_id(account: str) -> str:
    return f"BNSYNC_{str(account).upper().strip()}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"


def _parse_symbols(value: str) -> list[str]:
    out: list[str] = []
    for raw in str(value or '').split(','):
        symbol = str(raw).upper().strip()
        if symbol:
            out.append(symbol)
    deduped: list[str] = []
    seen: set[str] = set()
    for symbol in out:
        if symbol in seen:
            continue
        seen.add(symbol)
        deduped.append(symbol)
    if not deduped:
        raise SystemExit('symbols must not be empty')
    return deduped


def _compute_window_start_ms(
    checkpoint_payload: dict[str, Any],
    *,
    dataset: str,
    symbol: str,
    explicit_start_ms: int | None,
    now_ms: int,
    lookback_mins: int,
    overlap_secs: int,
) -> int:
    if explicit_start_ms is not None:
        return int(explicit_start_ms)
    watermark = get_symbol_watermark(checkpoint_payload, dataset, symbol)
    if watermark is not None:
        return max(0, int(watermark) - int(overlap_secs) * 1000)
    return int(now_ms) - int(lookback_mins) * 60 * 1000


def _record_key_for_raw_order(record: dict[str, Any]) -> str:
    meta = record.get('_meta') or {}
    order_id = record.get('orderId')
    client_order_id = record.get('clientOrderId')
    return f"{meta.get('account')}|ORDER|{meta.get('symbol')}|{order_id}|{client_order_id}"


def _record_key_for_norm_order(record: dict[str, Any]) -> str:
    return f"{record.get('account')}|ORDER|{record.get('symbol')}|{record.get('exchange_order_id')}|{record.get('client_order_id')}"


def _record_key_for_raw_fill(record: dict[str, Any]) -> str:
    meta = record.get('_meta') or {}
    trade_id = record.get('id')
    order_id = record.get('orderId')
    return f"{meta.get('account')}|FILL|{meta.get('symbol')}|{trade_id}|{order_id}"


def _record_key_for_norm_fill(record: dict[str, Any]) -> str:
    return f"{record.get('account')}|FILL|{record.get('symbol')}|{record.get('trade_id')}|{record.get('order_id')}"


def _max_event_ms(rows: list[dict[str, Any]], *, candidates: list[str]) -> int | None:
    out: int | None = None
    for row in rows:
        for key in candidates:
            value = row.get(key)
            if value in (None, ''):
                continue
            try:
                ms = int(value)
            except Exception:
                continue
            if out is None or ms > out:
                out = ms
    return out


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description='Incremental Binance truth sync for orders and fills.')
    p.add_argument('--account', required=True)
    p.add_argument('--symbols', required=True, help='Comma-separated symbols, e.g. BTCUSDT,ETHUSDT')
    p.add_argument('--out-dir', default='output/bn_sync')
    p.add_argument('--state-dir', default='state/bn_sync')
    p.add_argument('--lookback-mins', type=int, default=1440)
    p.add_argument('--overlap-secs', type=int, default=300)
    p.add_argument('--limit', type=int, default=1000)
    p.add_argument('--start-ms', type=int, default=None)
    p.add_argument('--end-ms', type=int, default=None)
    p.add_argument('--run-id', default='')
    return p.parse_args()


def main() -> None:
    args = parse_args()
    account = str(args.account).strip()
    symbols = _parse_symbols(args.symbols)
    sync_run_id = str(args.run_id).strip() or _default_sync_run_id(account)
    now_ms = _now_ms()
    end_ms = int(args.end_ms) if args.end_ms is not None else now_ms

    out_dir = Path(args.out_dir)
    state_dir = Path(args.state_dir)
    raw_dir = out_dir / 'raw'
    norm_dir = out_dir / 'normalized'
    index_dir = state_dir / 'index'

    orders_checkpoint_path = state_dir / f'{account}.orders.checkpoint.json'
    fills_checkpoint_path = state_dir / f'{account}.fills.checkpoint.json'
    orders_checkpoint = load_checkpoint(orders_checkpoint_path)
    fills_checkpoint = load_checkpoint(fills_checkpoint_path)

    raw_orders_path = raw_dir / f'bn_raw_orders.{account}.jsonl'
    raw_fills_path = raw_dir / f'bn_raw_fills.{account}.jsonl'
    norm_orders_path = norm_dir / f'bn_orders.{account}.jsonl'
    norm_fills_path = norm_dir / f'bn_fills.{account}.jsonl'

    raw_orders_index = index_dir / f'bn_raw_orders.{account}.keys.txt'
    raw_fills_index = index_dir / f'bn_raw_fills.{account}.keys.txt'
    norm_orders_index = index_dir / f'bn_orders.{account}.keys.txt'
    norm_fills_index = index_dir / f'bn_fills.{account}.keys.txt'

    orders_total = 0
    fills_total = 0

    for symbol in symbols:
        orders_start_ms = _compute_window_start_ms(
            orders_checkpoint,
            dataset='orders',
            symbol=symbol,
            explicit_start_ms=args.start_ms,
            now_ms=now_ms,
            lookback_mins=args.lookback_mins,
            overlap_secs=args.overlap_secs,
        )
        fills_start_ms = _compute_window_start_ms(
            fills_checkpoint,
            dataset='fills',
            symbol=symbol,
            explicit_start_ms=args.start_ms,
            now_ms=now_ms,
            lookback_mins=args.lookback_mins,
            overlap_secs=args.overlap_secs,
        )

        orders_res = get_all_orders(
            account,
            symbol,
            start_time_ms=orders_start_ms,
            end_time_ms=end_ms,
            limit=args.limit,
        )
        if not orders_res.get('ok'):
            raise SystemExit(f"orders sync failed for {symbol}: {orders_res.get('reason')}")

        fills_res = get_account_trades(
            account,
            symbol,
            start_time_ms=fills_start_ms,
            end_time_ms=end_ms,
            limit=args.limit,
        )
        if not fills_res.get('ok'):
            raise SystemExit(f"fills sync failed for {symbol}: {fills_res.get('reason')}")

        collected_ms = _now_ms()
        order_rows = list(orders_res.get('data') or [])
        fill_rows = list(fills_res.get('data') or [])

        raw_order_records = [
            build_raw_record(
                dataset='orders',
                account=account,
                symbol=symbol,
                sync_run_id=sync_run_id,
                collected_ms=collected_ms,
                source='futures_get_all_orders',
                raw_payload=row.get('raw') or {},
            )
            for row in order_rows
        ]
        raw_fill_records = [
            build_raw_record(
                dataset='fills',
                account=account,
                symbol=symbol,
                sync_run_id=sync_run_id,
                collected_ms=collected_ms,
                source='futures_account_trades',
                raw_payload=row.get('raw') or {},
            )
            for row in fill_rows
        ]

        norm_order_records = [
            normalize_order_record(row, account=account, sync_run_id=sync_run_id, collected_ms=collected_ms)
            for row in order_rows
        ]
        norm_fill_records = [
            normalize_fill_record(row, account=account, sync_run_id=sync_run_id, collected_ms=collected_ms)
            for row in fill_rows
        ]

        raw_orders_append = append_jsonl_unique(raw_orders_path, raw_orders_index, raw_order_records, key_fn=_record_key_for_raw_order)
        raw_fills_append = append_jsonl_unique(raw_fills_path, raw_fills_index, raw_fill_records, key_fn=_record_key_for_raw_fill)
        norm_orders_append = append_jsonl_unique(norm_orders_path, norm_orders_index, norm_order_records, key_fn=_record_key_for_norm_order)
        norm_fills_append = append_jsonl_unique(norm_fills_path, norm_fills_index, norm_fill_records, key_fn=_record_key_for_norm_fill)

        orders_total += int(norm_orders_append['appended'])
        fills_total += int(norm_fills_append['appended'])

        max_order_ms = _max_event_ms(order_rows, candidates=['update_time_ms', 'time_ms'])
        max_fill_ms = _max_event_ms(fill_rows, candidates=['time_ms'])

        orders_checkpoint = update_symbol_watermark(
            orders_checkpoint,
            'orders',
            symbol,
            last_event_ms=max_order_ms or end_ms,
            extra={
                'sync_run_id': sync_run_id,
                'last_start_ms': orders_start_ms,
                'last_end_ms': end_ms,
                'last_collected_ms': collected_ms,
                'last_collected_bj': _fmt_bj_from_ms(collected_ms),
            },
        )
        fills_checkpoint = update_symbol_watermark(
            fills_checkpoint,
            'fills',
            symbol,
            last_event_ms=max_fill_ms or end_ms,
            extra={
                'sync_run_id': sync_run_id,
                'last_start_ms': fills_start_ms,
                'last_end_ms': end_ms,
                'last_collected_ms': collected_ms,
                'last_collected_bj': _fmt_bj_from_ms(collected_ms),
            },
        )

        print(f"[{symbol}] orders_raw={raw_orders_append['appended']} orders_norm={norm_orders_append['appended']} fills_raw={raw_fills_append['appended']} fills_norm={norm_fills_append['appended']}")

    save_checkpoint(orders_checkpoint_path, orders_checkpoint)
    save_checkpoint(fills_checkpoint_path, fills_checkpoint)

    print('=== bn sync done ===')
    print(f'account      : {account}')
    print(f'sync_run_id  : {sync_run_id}')
    print(f'symbol_count : {len(symbols)}')
    print(f'orders_added : {orders_total}')
    print(f'fills_added  : {fills_total}')
    print(f'raw_orders   : {raw_orders_path}')
    print(f'raw_fills    : {raw_fills_path}')
    print(f'norm_orders  : {norm_orders_path}')
    print(f'norm_fills   : {norm_fills_path}')
    print(f'orders_ckpt  : {orders_checkpoint_path}')
    print(f'fills_ckpt   : {fills_checkpoint_path}')


if __name__ == '__main__':
    main()
