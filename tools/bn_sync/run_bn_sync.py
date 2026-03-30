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
    normalize_income_record,
    normalize_order_record,
    normalize_position_fact_record,
    normalize_position_snapshot_record,
)
from core.bn_sync.storage import append_jsonl_unique
from core.live.binance_exec import (
    get_account_trades,
    get_all_orders,
    get_income_history,
    get_position_snapshots,
)

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


def _record_key_for_raw_income(record: dict[str, Any]) -> str:
    meta = record.get('_meta') or {}
    tran_id = record.get('tranId')
    trade_id = record.get('tradeId')
    income_type = record.get('incomeType')
    time_ms = record.get('time')
    return f"{meta.get('account')}|INCOME|{meta.get('symbol')}|{tran_id}|{trade_id}|{income_type}|{time_ms}"


def _record_key_for_norm_income(record: dict[str, Any]) -> str:
    return f"{record.get('account')}|INCOME|{record.get('symbol')}|{record.get('tran_id')}|{record.get('trade_id')}|{record.get('income_type')}|{record.get('event_ms')}"


def _record_key_for_raw_position_snapshot(record: dict[str, Any]) -> str:
    meta = record.get('_meta') or {}
    position_side = record.get('positionSide')
    collected_ms = (meta.get('collected_ms') if isinstance(meta, dict) else None)
    return f"{meta.get('account')}|POSSNAP|{meta.get('symbol')}|{position_side}|{collected_ms}"


def _record_key_for_norm_position_snapshot(record: dict[str, Any]) -> str:
    return f"{record.get('account')}|POSSNAP|{record.get('symbol')}|{record.get('position_side')}|{record.get('collected_ms')}"


def _record_key_for_norm_position_fact(record: dict[str, Any]) -> str:
    return f"{record.get('account')}|POSFACT|{record.get('symbol')}|{record.get('position_side')}|{record.get('collected_ms')}"


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
    p = argparse.ArgumentParser(description='Incremental Binance truth sync for orders, fills, income and position snapshots.')
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
    p.add_argument('--include-zero-positions', action='store_true')
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
    income_checkpoint_path = state_dir / f'{account}.income.checkpoint.json'
    orders_checkpoint = load_checkpoint(orders_checkpoint_path)
    fills_checkpoint = load_checkpoint(fills_checkpoint_path)
    income_checkpoint = load_checkpoint(income_checkpoint_path)

    raw_orders_path = raw_dir / f'bn_raw_orders.{account}.jsonl'
    raw_fills_path = raw_dir / f'bn_raw_fills.{account}.jsonl'
    raw_income_path = raw_dir / f'bn_raw_income.{account}.jsonl'
    raw_position_snapshots_path = raw_dir / f'bn_raw_position_snapshots.{account}.jsonl'

    norm_orders_path = norm_dir / f'bn_orders.{account}.jsonl'
    norm_fills_path = norm_dir / f'bn_fills.{account}.jsonl'
    norm_income_path = norm_dir / f'bn_income.{account}.jsonl'
    norm_position_snapshots_path = norm_dir / f'bn_position_snapshots.{account}.jsonl'
    norm_position_facts_path = norm_dir / f'bn_position_facts.{account}.jsonl'

    raw_orders_index = index_dir / f'bn_raw_orders.{account}.keys.txt'
    raw_fills_index = index_dir / f'bn_raw_fills.{account}.keys.txt'
    raw_income_index = index_dir / f'bn_raw_income.{account}.keys.txt'
    raw_position_snapshots_index = index_dir / f'bn_raw_position_snapshots.{account}.keys.txt'

    norm_orders_index = index_dir / f'bn_orders.{account}.keys.txt'
    norm_fills_index = index_dir / f'bn_fills.{account}.keys.txt'
    norm_income_index = index_dir / f'bn_income.{account}.keys.txt'
    norm_position_snapshots_index = index_dir / f'bn_position_snapshots.{account}.keys.txt'
    norm_position_facts_index = index_dir / f'bn_position_facts.{account}.keys.txt'

    positions_collected_ms = _now_ms()
    positions_snapshot_res = get_position_snapshots(account, include_zero=bool(args.include_zero_positions))
    if not positions_snapshot_res.get('ok'):
        raise SystemExit(f"position snapshots sync failed: {positions_snapshot_res.get('reason')}")
    positions_rows_all = list(positions_snapshot_res.get('data') or [])

    orders_total = 0
    fills_total = 0
    income_total = 0
    position_snapshots_total = 0
    position_facts_total = 0

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
        income_start_ms = _compute_window_start_ms(
            income_checkpoint,
            dataset='income',
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

        income_res = get_income_history(
            account,
            symbol,
            start_time_ms=income_start_ms,
            end_time_ms=end_ms,
            limit=args.limit,
        )
        if not income_res.get('ok'):
            raise SystemExit(f"income sync failed for {symbol}: {income_res.get('reason')}")

        collected_ms = _now_ms()
        order_rows = list(orders_res.get('data') or [])
        fill_rows = list(fills_res.get('data') or [])
        income_rows = list(income_res.get('data') or [])
        position_rows = [row for row in positions_rows_all if str(row.get('symbol') or '').upper().strip() == symbol]

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
        raw_income_records = [
            build_raw_record(
                dataset='income',
                account=account,
                symbol=symbol,
                sync_run_id=sync_run_id,
                collected_ms=collected_ms,
                source='futures_income_history',
                raw_payload=row.get('raw') or {},
            )
            for row in income_rows
        ]
        raw_position_snapshot_records = [
            build_raw_record(
                dataset='position_snapshots',
                account=account,
                symbol=symbol,
                sync_run_id=sync_run_id,
                collected_ms=positions_collected_ms,
                source='futures_position_information',
                raw_payload=row.get('raw') or {},
            )
            for row in position_rows
        ]

        norm_order_records = [
            normalize_order_record(row, account=account, sync_run_id=sync_run_id, collected_ms=collected_ms)
            for row in order_rows
        ]
        norm_fill_records = [
            normalize_fill_record(row, account=account, sync_run_id=sync_run_id, collected_ms=collected_ms)
            for row in fill_rows
        ]
        norm_income_records = [
            normalize_income_record(row, account=account, sync_run_id=sync_run_id, collected_ms=collected_ms)
            for row in income_rows
        ]
        norm_position_snapshot_records = [
            normalize_position_snapshot_record(
                row,
                account=account,
                sync_run_id=sync_run_id,
                collected_ms=positions_collected_ms,
            )
            for row in position_rows
        ]
        norm_position_fact_records = [
            normalize_position_fact_record(
                row,
                account=account,
                sync_run_id=sync_run_id,
                collected_ms=positions_collected_ms,
            )
            for row in position_rows
        ]

        raw_orders_append = append_jsonl_unique(raw_orders_path, raw_orders_index, raw_order_records, key_fn=_record_key_for_raw_order)
        raw_fills_append = append_jsonl_unique(raw_fills_path, raw_fills_index, raw_fill_records, key_fn=_record_key_for_raw_fill)
        raw_income_append = append_jsonl_unique(raw_income_path, raw_income_index, raw_income_records, key_fn=_record_key_for_raw_income)
        raw_position_snapshots_append = append_jsonl_unique(
            raw_position_snapshots_path,
            raw_position_snapshots_index,
            raw_position_snapshot_records,
            key_fn=_record_key_for_raw_position_snapshot,
        )

        norm_orders_append = append_jsonl_unique(norm_orders_path, norm_orders_index, norm_order_records, key_fn=_record_key_for_norm_order)
        norm_fills_append = append_jsonl_unique(norm_fills_path, norm_fills_index, norm_fill_records, key_fn=_record_key_for_norm_fill)
        norm_income_append = append_jsonl_unique(norm_income_path, norm_income_index, norm_income_records, key_fn=_record_key_for_norm_income)
        norm_position_snapshots_append = append_jsonl_unique(
            norm_position_snapshots_path,
            norm_position_snapshots_index,
            norm_position_snapshot_records,
            key_fn=_record_key_for_norm_position_snapshot,
        )
        norm_position_facts_append = append_jsonl_unique(
            norm_position_facts_path,
            norm_position_facts_index,
            norm_position_fact_records,
            key_fn=_record_key_for_norm_position_fact,
        )

        orders_total += int(norm_orders_append['appended'])
        fills_total += int(norm_fills_append['appended'])
        income_total += int(norm_income_append['appended'])
        position_snapshots_total += int(norm_position_snapshots_append['appended'])
        position_facts_total += int(norm_position_facts_append['appended'])

        max_order_ms = _max_event_ms(order_rows, candidates=['update_time_ms', 'time_ms'])
        max_fill_ms = _max_event_ms(fill_rows, candidates=['time_ms'])
        max_income_ms = _max_event_ms(income_rows, candidates=['time_ms'])

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
        income_checkpoint = update_symbol_watermark(
            income_checkpoint,
            'income',
            symbol,
            last_event_ms=max_income_ms or end_ms,
            extra={
                'sync_run_id': sync_run_id,
                'last_start_ms': income_start_ms,
                'last_end_ms': end_ms,
                'last_collected_ms': collected_ms,
                'last_collected_bj': _fmt_bj_from_ms(collected_ms),
            },
        )

        print(
            f"[{symbol}] "
            f"orders_raw={raw_orders_append['appended']} orders_norm={norm_orders_append['appended']} "
            f"fills_raw={raw_fills_append['appended']} fills_norm={norm_fills_append['appended']} "
            f"income_raw={raw_income_append['appended']} income_norm={norm_income_append['appended']} "
            f"possnap_raw={raw_position_snapshots_append['appended']} possnap_norm={norm_position_snapshots_append['appended']} "
            f"posfact_norm={norm_position_facts_append['appended']}"
        )

    save_checkpoint(orders_checkpoint_path, orders_checkpoint)
    save_checkpoint(fills_checkpoint_path, fills_checkpoint)
    save_checkpoint(income_checkpoint_path, income_checkpoint)

    print('=== bn sync done ===')
    print(f'account      : {account}')
    print(f'sync_run_id  : {sync_run_id}')
    print(f'symbol_count : {len(symbols)}')
    print(f'orders_added : {orders_total}')
    print(f'fills_added  : {fills_total}')
    print(f'income_added : {income_total}')
    print(f'possnap_added: {position_snapshots_total}')
    print(f'posfact_added: {position_facts_total}')
    print(f'raw_orders   : {raw_orders_path}')
    print(f'raw_fills    : {raw_fills_path}')
    print(f'raw_income   : {raw_income_path}')
    print(f'raw_possnap  : {raw_position_snapshots_path}')
    print(f'norm_orders  : {norm_orders_path}')
    print(f'norm_fills   : {norm_fills_path}')
    print(f'norm_income  : {norm_income_path}')
    print(f'norm_possnap : {norm_position_snapshots_path}')
    print(f'norm_posfact : {norm_position_facts_path}')
    print(f'orders_ckpt  : {orders_checkpoint_path}')
    print(f'fills_ckpt   : {fills_checkpoint_path}')
    print(f'income_ckpt  : {income_checkpoint_path}')


if __name__ == '__main__':
    main()
