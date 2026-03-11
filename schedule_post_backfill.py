#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
from argparse import Namespace
from pathlib import Path


def load_schedule_module(repo_root: Path):
    target = repo_root / 'schedule_backtests.py'
    spec = importlib.util.spec_from_file_location('schedule_backtests_runtime', target)
    if spec is None or spec.loader is None:
        raise RuntimeError(f'failed to load module from {target}')
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def derive_scheduler_name(summary_path: Path, summary: dict) -> str:
    m = re.match(r'^scheduler_(.+)\.summary\.json$', summary_path.name)
    if m:
        return m.group(1)
    tasks = summary.get('tasks', [])
    if tasks:
        run_id = tasks[0].get('run_id', '')
        m2 = re.match(r'(.+)_B\d{2}_\d{8}_\d{8}$', run_id)
        if m2:
            return m2.group(1)
    raise RuntimeError('cannot derive scheduler_name from summary filename or task run_id')


def main() -> int:
    ap = argparse.ArgumentParser(description='Backfill post-processing for an already-finished schedule_backtests run.')
    ap.add_argument('--summary-json', required=True, help='Path to scheduler_*.summary.json')
    ap.add_argument('--repo-root', default='.', help='Repo root containing schedule_backtests.py')
    ap.add_argument('--post-merge', action='store_true', help='Run merge/summary/viz post-processing')
    ap.add_argument('--build-equity', action='store_true', help='Run equity generation after merge')
    ap.add_argument('--equity-script', default='core/analysis/top1_equity_curve.py')
    ap.add_argument('--kline-root', default='data/klines_1m')
    ap.add_argument('--equity-initial', type=float, default=100.0)
    ap.add_argument('--equity-fee-side', type=float, default=0.0005)
    args = ap.parse_args()

    if not (args.post_merge or args.build_equity):
        ap.error('at least one of --post-merge or --build-equity must be provided')

    repo_root = Path(args.repo_root).resolve()
    summary_path = Path(args.summary_json).resolve()

    mod = load_schedule_module(repo_root)

    with summary_path.open('r', encoding='utf-8') as f:
        scheduler_summary = json.load(f)

    if scheduler_summary.get('failed_count', 0):
        print('POST_BACKFILL_ABORT failed_count>0')
        return 1

    scheduler_name = derive_scheduler_name(summary_path, scheduler_summary)
    scheduler_log = repo_root / scheduler_summary['logs_dir'] / f'scheduler_{scheduler_name}.log'

    tasks = [mod.Task(**item) for item in scheduler_summary.get('tasks', [])]
    finished = scheduler_summary.get('finished', [])

    runtime_args = Namespace(
        strategy=scheduler_summary['strategy'],
        start=scheduler_summary['start'],
        end=scheduler_summary['end'],
        batch_days=scheduler_summary['batch_days'],
        max_parallel=scheduler_summary['max_parallel'],
        python_bin=scheduler_summary.get('python_bin', '/root/service_env/bin/python'),
        config=scheduler_summary['config'],
        out_dir=scheduler_summary['out_dir'],
        logs_dir=scheduler_summary['logs_dir'],
        post_merge=args.post_merge,
        build_equity=args.build_equity,
        equity_script=args.equity_script,
        kline_root=args.kline_root,
        equity_initial=args.equity_initial,
        equity_fee_side=args.equity_fee_side,
    )

    artifacts, errors = mod.run_post_processing(
        args=runtime_args,
        scheduler_name=scheduler_name,
        tasks=tasks,
        finished=finished,
        scheduler_summary=scheduler_summary,
        scheduler_log=scheduler_log,
    )

    scheduler_summary['artifacts'] = artifacts
    if errors:
        scheduler_summary['artifacts_errors'] = errors

    with summary_path.open('w', encoding='utf-8') as f:
        json.dump(scheduler_summary, f, ensure_ascii=False, indent=2)

    print(f'POST_BACKFILL_DONE artifacts={len(artifacts)} errors={len(errors)} summary={summary_path}')
    if errors:
        for err in errors:
            print(f'ERROR: {err}')
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main())
