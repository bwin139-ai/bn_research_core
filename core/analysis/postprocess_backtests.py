#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from strategies.run_backtest import build_extended_summary_metrics, _extract_fee_side  # type: ignore


def load_jsonl(path: Path) -> List[dict]:
    rows: List[dict] = []
    if not path.exists():
        return rows
    with path.open('r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description='Build merged backtest summary and optional equity curves.')
    ap.add_argument('--run-id', required=True)
    ap.add_argument('--state-dir', default='output/state')
    ap.add_argument('--merge-meta', required=True)
    ap.add_argument('--kline-root', default='data/klines_1m')
    ap.add_argument('--initial-equity', type=float, default=100.0)
    ap.add_argument('--fee-side', type=float, default=0.0005)
    ap.add_argument('--equity-script', default='core/analysis/sim_equity_curves.py')
    ap.add_argument('--build-equity', action='store_true')
    args = ap.parse_args()

    state_dir = Path(args.state_dir)
    run_id = args.run_id
    merge_meta_path = Path(args.merge_meta)
    merged_trades = state_dir / f'sim_trades.{run_id}.jsonl'
    merged_signals = state_dir / f'sim_signals.{run_id}.jsonl'
    merged_summary = state_dir / f'sim_summary.{run_id}.json'

    with merge_meta_path.open('r', encoding='utf-8') as f:
        merge_meta = json.load(f)

    config_path = Path(merge_meta['config_path'])
    run_config: Dict[str, Any] | None = None
    if config_path.exists():
        with config_path.open('r', encoding='utf-8') as f:
            run_config = json.load(f)

    trades = load_jsonl(merged_trades)
    signals = load_jsonl(merged_signals)
    fee_side = _extract_fee_side(run_config or {}) if run_config is not None else args.fee_side
    metrics = build_extended_summary_metrics(trades, fee_side=fee_side, initial_equity=args.initial_equity)
    metrics['signals_count'] = len(signals)

    out = {
        'summary_scope': 'ALL',
        'generated_by': 'core/analysis/postprocess_backtests.py',
        'strategy_name': merge_meta['strategy_name'],
        'run_id': run_id,
        'start': merge_meta['start'],
        'end': merge_meta['end'],
        'batch_days': merge_meta['batch_days'],
        'max_parallel': merge_meta['max_parallel'],
        'batch_count': merge_meta['batch_count'],
        'success_count': merge_meta['success_count'],
        'failed_count': merge_meta['failed_count'],
        'wall_clock_seconds': merge_meta['wall_clock_seconds'],
        'batch_run_ids': merge_meta['batch_run_ids'],
        'batch_summaries': merge_meta['batch_summaries'],
        'config_path': str(config_path),
        'run_config': run_config,
        'artifacts': dict(merge_meta.get('artifacts', {})),
        **metrics,
    }

    if args.build_equity:
        cmd = [
            sys.executable,
            args.equity_script,
            '--run-id', run_id,
            '--state-dir', str(state_dir),
            '--kline-root', args.kline_root,
            '--initial-equity', str(args.initial_equity),
            '--fee-side', str(fee_side),
        ]
        subprocess.run(cmd, check=True)
        out['artifacts'].update({
            'equity_curve_simple_png': str(state_dir / f'sim_curve_simple.{run_id}.png'),
            'equity_curve_compound_png': str(state_dir / f'sim_curve_compound.{run_id}.png'),
            'equity_summary_json': str(state_dir / f'sim_equity.{run_id}.json'),
        })

    with merged_summary.open('w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(f'Wrote merged summary: {merged_summary}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
