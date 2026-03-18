#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from statistics import median
from typing import Any


def load_jsonl(path: Path):
    rows = []
    with path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def pct(x: Any):
    try:
        return float(x) * 100.0
    except Exception:
        return None


def get_context(trade: dict) -> dict:
    return trade.get('context', {}) or {}


def get_reason(trade: dict) -> str:
    return str(trade.get('reason') or trade.get('exit_reason') or '')


def get_pnl_pct(trade: dict):
    for k in ('pnl_pct', 'net_pnl_pct', 'profit_pct'):
        if k in trade and trade[k] is not None:
            return float(trade[k])
    ctx = get_context(trade)
    for k in ('pnl_pct', 'net_pnl_pct', 'profit_pct'):
        if k in ctx and ctx[k] is not None:
            return float(ctx[k])
    return None


def summarize(rows: list[dict]) -> dict:
    pnls = [r['_pnl_pct'] for r in rows if r['_pnl_pct'] is not None]
    reasons: dict[str, int] = {}
    wins = 0
    for r in rows:
        reason = r['_reason']
        reasons[reason] = reasons.get(reason, 0) + 1
        if (r['_pnl_pct'] or 0) > 0:
            wins += 1
    return {
        'count': len(rows),
        'pnl_pct_sum': round(sum(pnls), 12) if pnls else 0.0,
        'avg_pnl_pct': round(sum(pnls) / len(pnls), 12) if pnls else None,
        'median_pnl_pct': round(median(pnls), 12) if pnls else None,
        'win_rate': round(wins / len(rows), 12) if rows else None,
        'reason_counts': reasons,
    }


def write_csv(path: Path, rows: list[dict]):
    cols = [
        'symbol', 'reason', 'pnl_pct', 'chg_24h_pct', 'drop_window_chg_pct',
        'a_time', 'b_time', 'c_time', 's_time'
    ]
    with path.open('w', encoding='utf-8', newline='') as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            ctx = get_context(r)
            w.writerow({
                'symbol': r.get('symbol'),
                'reason': r['_reason'],
                'pnl_pct': r['_pnl_pct'],
                'chg_24h_pct': round((r['_chg_24h'] or 0) * 100, 6) if r['_chg_24h'] is not None else None,
                'drop_window_chg_pct': round((r['_drop_window_chg'] or 0) * 100, 6) if r['_drop_window_chg'] is not None else None,
                'a_time': ctx.get('a_time'),
                'b_time': ctx.get('b_time'),
                'c_time': ctx.get('c_time'),
                's_time': ctx.get('s_time'),
            })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--run-id', required=True)
    ap.add_argument('--summary-json', required=True)
    ap.add_argument('--kept-csv', required=True)
    ap.add_argument('--removed-csv', required=True)
    args = ap.parse_args()

    root = Path('output/state')
    trades_path = root / f'sim_trades.{args.run_id}.jsonl'
    summary_path = root / f'sim_summary.{args.run_id}.json'
    trades = load_jsonl(trades_path)

    enriched = []
    for t in trades:
        ctx = get_context(t)
        t['_chg_24h'] = None if ctx.get('chg_24h') is None else float(ctx.get('chg_24h'))
        t['_drop_window_chg'] = None if ctx.get('drop_window_chg') is None else float(ctx.get('drop_window_chg'))
        t['_pnl_pct'] = get_pnl_pct(t)
        t['_reason'] = get_reason(t)
        enriched.append(t)

    removed = [t for t in enriched if (t['_chg_24h'] is not None and t['_drop_window_chg'] is not None and t['_chg_24h'] > 0 and t['_drop_window_chg'] > 0)]
    kept = [t for t in enriched if t not in removed]

    before = summarize(enriched)
    after = summarize(kept)
    removed_stats = summarize(removed)

    worst_before = sorted([t for t in enriched if t['_pnl_pct'] is not None], key=lambda x: x['_pnl_pct'])[:20]
    worst_after = sorted([t for t in kept if t['_pnl_pct'] is not None], key=lambda x: x['_pnl_pct'])[:20]

    summary = {
        'run_id': args.run_id,
        'trades_path': str(trades_path),
        'sim_summary_path': str(summary_path),
        'filter_rule': 'remove when chg_24h > 0 and drop_window_chg > 0',
        'before': before,
        'after': after,
        'removed': removed_stats,
        'delta': {
            'count': after['count'] - before['count'],
            'pnl_pct_sum': round((after['pnl_pct_sum'] or 0) - (before['pnl_pct_sum'] or 0), 12),
        },
        'removed_reason_counts': removed_stats['reason_counts'],
        'worst_before_top20': [
            {'symbol': t.get('symbol'), 'reason': t['_reason'], 'pnl_pct': t['_pnl_pct'], 'chg_24h_pct': pct(t['_chg_24h']), 'drop_window_chg_pct': pct(t['_drop_window_chg'])}
            for t in worst_before
        ],
        'worst_after_top20': [
            {'symbol': t.get('symbol'), 'reason': t['_reason'], 'pnl_pct': t['_pnl_pct'], 'chg_24h_pct': pct(t['_chg_24h']), 'drop_window_chg_pct': pct(t['_drop_window_chg'])}
            for t in worst_after
        ],
    }

    Path(args.summary_json).parent.mkdir(parents=True, exist_ok=True)
    Path(args.kept_csv).parent.mkdir(parents=True, exist_ok=True)
    Path(args.removed_csv).parent.mkdir(parents=True, exist_ok=True)

    with Path(args.summary_json).open('w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    write_csv(Path(args.kept_csv), kept)
    write_csv(Path(args.removed_csv), removed)

    print('=== audit_direction_a_quadrant_filter 完成 ===')
    print(f'run_id       : {args.run_id}')
    print(f'trades       : {trades_path}')
    print(f'summary json : {args.summary_json}')
    print(f'kept csv     : {args.kept_csv}')
    print(f'removed csv  : {args.removed_csv}')
    print(f'before count : {before["count"]}')
    print(f'after count  : {after["count"]}')
    print(f'removed count: {removed_stats["count"]}')


if __name__ == '__main__':
    main()
