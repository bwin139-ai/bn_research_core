#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from core.message_bridge import send_to_bot

UTC = timezone.utc


def parse_iso_utc(s: str) -> datetime:
    dt = datetime.fromisoformat(s.replace('Z', '+00:00'))
    if dt.tzinfo is None:
        raise ValueError(f'timestamp must include timezone: {s}')
    return dt.astimezone(UTC)


def fmt_dt(dt: datetime) -> str:
    return dt.astimezone(UTC).isoformat().replace('+00:00', 'Z')


def ymd(dt: datetime) -> str:
    return dt.astimezone(UTC).strftime('%Y%m%d')


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def now_tag() -> str:
    return datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')


def notify_message(notify_label: Optional[str], text: str) -> None:
    if not notify_label:
        return
    try:
        send_to_bot(text, label=notify_label)
    except Exception:
        # best-effort only: scheduler semantics must not depend on notifications
        pass


@dataclass
class Task:
    batch_id: int
    start: str
    end: str
    run_id: str
    log_path: str
    cmd: List[str]


@dataclass
class RunningTask:
    task: Task
    proc: subprocess.Popen
    started_at: float



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


def merge_jsonl_files(paths: List[Path], out_path: Path) -> int:
    total = 0
    ensure_dir(out_path.parent)
    with out_path.open('w', encoding='utf-8') as out:
        for path in paths:
            if not path.exists():
                continue
            with path.open('r', encoding='utf-8') as f:
                for line in f:
                    if not line.strip():
                        continue
                    out.write(line if line.endswith('\n') else line + '\n')
                    total += 1
    return total


def merge_viz_dirs(paths: List[Path], out_dir: Path) -> int:
    ensure_dir(out_dir)
    copied = 0
    for path in paths:
        if not path.exists() or not path.is_dir():
            continue
        for png in sorted(path.glob('*.png')):
            target = out_dir / png.name
            if target.exists():
                stem = png.stem
                suffix = png.suffix
                idx = 1
                while True:
                    alt = out_dir / f"{stem}__dup{idx}{suffix}"
                    if not alt.exists():
                        target = alt
                        break
                    idx += 1
            shutil.copy2(png, target)
            copied += 1
    return copied


def resolve_config_path(config_arg: str) -> Path:
    raw = Path(config_arg)
    if raw.exists():
        return raw
    strat_path = Path('strategies') / config_arg
    if strat_path.exists():
        return strat_path
    return raw


def build_all_summary(
    args: argparse.Namespace,
    runset: str,
    tasks: List[Task],
    finished: List[dict],
    scheduler_summary: dict,
    merged_trades_path: Optional[Path],
    merged_signals_path: Optional[Path],
    artifacts: Dict[str, str],
) -> dict:
    trades = load_jsonl(merged_trades_path) if merged_trades_path else []
    signals = load_jsonl(merged_signals_path) if merged_signals_path else []

    reason_counts: Dict[str, int] = {}
    symbols = set()
    pnl_pct_sum = 0.0
    pnl_pct_present = False
    for row in trades:
        sym = row.get('symbol')
        if sym:
            symbols.add(sym)
        reason = row.get('reason')
        if reason:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        if isinstance(row.get('pnl_pct'), (int, float)):
            pnl_pct_sum += float(row['pnl_pct'])
            pnl_pct_present = True

    config_path = resolve_config_path(args.config)
    run_config: Dict[str, Any] | None = None
    if config_path.exists():
        with config_path.open('r', encoding='utf-8') as f:
            run_config = json.load(f)

    return {
        'summary_scope': 'ALL',
        'generated_by': 'schedule_backtests.py',
        'strategy_name': args.strategy,
        'run_id': f'{runset}_ALL',
        'start': args.start,
        'end': args.end,
        'batch_days': args.batch_days,
        'max_parallel': args.max_parallel,
        'batch_count': len(tasks),
        'success_count': scheduler_summary['success_count'],
        'failed_count': scheduler_summary['failed_count'],
        'wall_clock_seconds': scheduler_summary['wall_clock_seconds'],
        'signals_count': len(signals),
        'total_trades': len(trades),
        'symbols_count': len(symbols),
        'reason_counts': reason_counts,
        'pnl_pct_sum': round(pnl_pct_sum, 12) if pnl_pct_present else None,
        'batch_run_ids': [t.run_id for t in tasks],
        'batch_summaries': [str(Path(args.out_dir) / f'sim_summary.{t.run_id}.json') for t in tasks],
        'config_path': str(config_path),
        'run_config': run_config,
        'artifacts': artifacts,
    }


def run_post_processing(
    args: argparse.Namespace,
    scheduler_name: str,
    tasks: List[Task],
    finished: List[dict],
    scheduler_summary: dict,
    scheduler_log: Path,
    notify_label: Optional[str] = None,
) -> tuple[Dict[str, str], List[str]]:
    artifacts: Dict[str, str] = {}
    errors: List[str] = []

    enabled_merge = args.post_merge or args.build_equity
    if not enabled_merge:
        return artifacts, errors

    state_dir = Path(args.out_dir)
    runset = scheduler_name

    append_line(scheduler_log, f'POST_MERGE_START runset={runset}')
    print(f'POST_MERGE_START runset={runset}')

    trade_paths = [state_dir / f'sim_trades.{t.run_id}.jsonl' for t in tasks]
    signal_paths = [state_dir / f'sim_signals.{t.run_id}.jsonl' for t in tasks]
    viz_dirs = [state_dir / f'sim_viz_{t.run_id}' for t in tasks]

    merged_trades = state_dir / f'sim_trades.{runset}_ALL.jsonl'
    merged_signals = state_dir / f'sim_signals.{runset}_ALL.jsonl'
    merged_viz_dir = state_dir / f'sim_viz_{runset}_ALL'
    merged_summary = state_dir / f'sim_summary.{runset}_ALL.json'

    try:
        trades_count = merge_jsonl_files(trade_paths, merged_trades)
        artifacts['merged_trades'] = str(merged_trades)
        signals_count = merge_jsonl_files(signal_paths, merged_signals)
        artifacts['merged_signals'] = str(merged_signals)
        viz_count = merge_viz_dirs(viz_dirs, merged_viz_dir)
        artifacts['merged_viz_dir'] = str(merged_viz_dir)

        all_summary = build_all_summary(
            args=args,
            runset=runset,
            tasks=tasks,
            finished=finished,
            scheduler_summary=scheduler_summary,
            merged_trades_path=merged_trades,
            merged_signals_path=merged_signals,
            artifacts=artifacts.copy(),
        )
        with merged_summary.open('w', encoding='utf-8') as f:
            json.dump(all_summary, f, ensure_ascii=False, indent=2)
        artifacts['merged_summary'] = str(merged_summary)

        append_line(
            scheduler_log,
            f'POST_MERGE_DONE runset={runset} trades={trades_count} signals={signals_count} viz_pngs={viz_count} merged_summary={merged_summary}',
        )
        print(f'POST_MERGE_DONE runset={runset} trades={trades_count} signals={signals_count} viz_pngs={viz_count}')
    except Exception as e:
        msg = f'post-merge failed: {e}'
        errors.append(msg)
        append_line(scheduler_log, f'POST_MERGE_FAIL runset={runset} error={e}')
        print(f'POST_MERGE_FAIL runset={runset} error={e}')
        notify_message(notify_label, f'[SCHED] post-merge FAIL | strategy={args.strategy} | runset={runset} | error={e}')
        return artifacts, errors

    if args.build_equity:
        append_line(scheduler_log, f'POST_EQUITY_START runset={runset}')
        print(f'POST_EQUITY_START runset={runset}')
        equity_png = state_dir / f'sim_curve.{runset}_ALL.png'
        equity_json = state_dir / f'sim_equity.{runset}_ALL.json'
        logs_dir = Path(args.logs_dir)
        ensure_dir(logs_dir)
        equity_log = logs_dir / f'equity_{runset}_ALL.log'
        cmd = [
            args.python_bin,
            args.equity_script,
            '--trades', str(merged_trades),
            '--kline-root', args.kline_root,
            '--initial-equity', str(args.equity_initial),
            '--fee-side', str(args.equity_fee_side),
            '--out', str(equity_png),
            '--summary-out', str(equity_json),
        ]
        try:
            with equity_log.open('w', encoding='utf-8') as f:
                proc = subprocess.run(cmd, stdout=f, stderr=subprocess.STDOUT, check=False)
            if proc.returncode != 0:
                raise RuntimeError(f'equity script rc={proc.returncode}; see {equity_log}')
            artifacts['equity_curve_png'] = str(equity_png)
            artifacts['equity_summary_json'] = str(equity_json)
            artifacts['equity_log'] = str(equity_log)
            append_line(scheduler_log, f'POST_EQUITY_DONE runset={runset} out={equity_png} summary={equity_json}')
            print(f'POST_EQUITY_DONE runset={runset} out={equity_png}')
            notify_message(notify_label, f'[SCHED] build-equity DONE | strategy={args.strategy} | runset={runset} | out={equity_png}')
        except Exception as e:
            msg = f'build-equity failed: {e}'
            errors.append(msg)
            append_line(scheduler_log, f'POST_EQUITY_FAIL runset={runset} error={e}')
            print(f'POST_EQUITY_FAIL runset={runset} error={e}')
            notify_message(notify_label, f'[SCHED] build-equity FAIL | strategy={args.strategy} | runset={runset} | error={e}')

    if artifacts.get('merged_summary'):
        merged_summary_path = Path(artifacts['merged_summary'])
        with merged_summary_path.open('r', encoding='utf-8') as f:
            all_summary = json.load(f)
        all_summary['artifacts'] = artifacts
        with merged_summary_path.open('w', encoding='utf-8') as f:
            json.dump(all_summary, f, ensure_ascii=False, indent=2)

    return artifacts, errors


def build_batches(start: datetime, end: datetime, batch_days: int) -> List[tuple[datetime, datetime]]:
    if end <= start:
        raise ValueError('--end must be later than --start')
    if batch_days <= 0:
        raise ValueError('--batch-days must be > 0')

    out: List[tuple[datetime, datetime]] = []
    cur = start
    step = timedelta(days=batch_days)
    while cur < end:
        nxt = min(cur + step, end)
        out.append((cur, nxt))
        cur = nxt
    return out


def build_tasks(args: argparse.Namespace) -> List[Task]:
    start = parse_iso_utc(args.start)
    end = parse_iso_utc(args.end)
    batches = build_batches(start, end, args.batch_days)

    run_prefix = args.run_prefix or f"{args.strategy.upper()}_{args.batch_days}D_{now_tag()}"
    logs_dir = Path(args.logs_dir)
    ensure_dir(logs_dir)

    tasks: List[Task] = []
    for idx, (b_start, b_end) in enumerate(batches, start=1):
        run_id = f"{run_prefix}_B{idx:02d}_{ymd(b_start)}_{ymd(b_end)}"
        log_path = str(logs_dir / f"{run_id}.console.log")
        cmd = [
            args.python_bin,
            'strategies/run_backtest.py',
            '--strategy', args.strategy,
            '--start', fmt_dt(b_start),
            '--end', fmt_dt(b_end),
            '--kline-window', str(args.kline_window),
            '--config', args.config,
            '--out-dir', args.out_dir,
            '--run-id', run_id,
        ]
        tasks.append(Task(
            batch_id=idx,
            start=fmt_dt(b_start),
            end=fmt_dt(b_end),
            run_id=run_id,
            log_path=log_path,
            cmd=cmd,
        ))
    return tasks


def append_line(path: Path, text: str) -> None:
    with path.open('a', encoding='utf-8') as f:
        f.write(text + '\n')


def launch_task(task: Task, scheduler_log: Path) -> RunningTask:
    log_f = open(task.log_path, 'a', encoding='utf-8')
    proc = subprocess.Popen(task.cmd, stdout=log_f, stderr=subprocess.STDOUT)
    started_at_ts = time.time()
    started_at_dt = datetime.now(UTC)
    rt = RunningTask(task=task, proc=proc, started_at=started_at_ts)
    stamp = fmt_dt(started_at_dt)
    append_line(
        scheduler_log,
        f"[{stamp}] START batch={task.batch_id:02d} pid={proc.pid} run_id={task.run_id} start={task.start} end={task.end} log={task.log_path}",
    )
    print(f"[{stamp}] START batch={task.batch_id:02d} pid={proc.pid} run_id={task.run_id}")
    return rt


def poll_running(
    running: List[RunningTask],
    scheduler_log: Path,
    finished: List[dict],
    strategy: str,
    notify_label: Optional[str] = None,
) -> List[RunningTask]:
    keep: List[RunningTask] = []
    for rt in running:
        rc = rt.proc.poll()
        if rc is None:
            keep.append(rt)
            continue
        elapsed = time.time() - rt.started_at
        finished_at_dt = datetime.now(UTC)
        rec = {
            'batch_id': rt.task.batch_id,
            'run_id': rt.task.run_id,
            'return_code': rc,
            'elapsed_seconds': round(elapsed, 3),
            'log_path': rt.task.log_path,
            'start': rt.task.start,
            'end': rt.task.end,
            'started_at': fmt_dt(datetime.fromtimestamp(rt.started_at, tz=UTC)),
            'finished_at': fmt_dt(finished_at_dt),
        }
        finished.append(rec)
        status = 'DONE' if rc == 0 else 'FAIL'
        stamp = fmt_dt(finished_at_dt)
        append_line(
            scheduler_log,
            f"[{stamp}] {status} batch={rt.task.batch_id:02d} rc={rc} elapsed={elapsed:.1f}s run_id={rt.task.run_id} log={rt.task.log_path}",
        )
        print(f"[{stamp}] {status} batch={rt.task.batch_id:02d} rc={rc} elapsed={elapsed:.1f}s run_id={rt.task.run_id}")
        notify_message(
            notify_label,
            f'[SCHED] {status} | strategy={strategy} | batch={rt.task.batch_id:02d} | run_id={rt.task.run_id} | rc={rc} | elapsed={elapsed:.1f}s | start={rt.task.start} | end={rt.task.end}',
        )
    return keep


def make_summary(args: argparse.Namespace, tasks: List[Task], finished: List[dict], wall_clock_seconds: float) -> dict:
    failed = [x for x in finished if x['return_code'] != 0]
    return {
        'strategy': args.strategy,
        'start': args.start,
        'end': args.end,
        'batch_days': args.batch_days,
        'max_parallel': args.max_parallel,
        'python_bin': args.python_bin,
        'config': args.config,
        'out_dir': args.out_dir,
        'logs_dir': args.logs_dir,
        'tasks': [asdict(t) for t in tasks],
        'finished': finished,
        'success_count': len(finished) - len(failed),
        'failed_count': len(failed),
        'failed_run_ids': [x['run_id'] for x in failed],
        'wall_clock_seconds': round(wall_clock_seconds, 3),
    }


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description='Minimal backtest batch scheduler with dynamic refill.')
    ap.add_argument('--strategy', required=True)
    ap.add_argument('--start', required=True, help='ISO8601 with timezone, e.g. 2025-04-18T00:00:00+00:00')
    ap.add_argument('--end', required=True, help='ISO8601 with timezone, e.g. 2026-04-18T00:00:00+00:00')
    ap.add_argument('--batch-days', type=int, required=True)
    ap.add_argument('--max-parallel', type=int, required=True)
    ap.add_argument('--kline-window', type=int, required=True)
    ap.add_argument('--config', required=True)
    ap.add_argument('--out-dir', required=True)
    ap.add_argument('--python-bin', default='/root/service_env/bin/python')
    ap.add_argument('--run-prefix', default=None)
    ap.add_argument('--logs-dir', default='output/logs')
    ap.add_argument('--summary-json', default=None)
    ap.add_argument('--poll-seconds', type=float, default=2.0)
    ap.add_argument('--dry-run', action='store_true')
    ap.add_argument('--post-merge', action='store_true')
    ap.add_argument('--build-equity', action='store_true')
    ap.add_argument('--post-only', action='store_true')
    ap.add_argument('--equity-script', default='core/analysis/top1_equity_curve.py')
    ap.add_argument('--kline-root', default='data/klines_1m')
    ap.add_argument('--equity-initial', type=float, default=100.0)
    ap.add_argument('--equity-fee-side', type=float, default=0.0005)
    ap.add_argument('--notify-label', default=None, help='queue label for scheduler notifications, e.g. admin or global')
    args = ap.parse_args()
    if args.max_parallel <= 0:
        ap.error('--max-parallel must be > 0')
    return args


def main() -> int:
    args = parse_args()
    tasks = build_tasks(args)
    scheduler_name = args.run_prefix or f"{args.strategy.upper()}_{args.batch_days}D"
    logs_dir = Path(args.logs_dir)
    ensure_dir(logs_dir)
    state_dir = Path(args.out_dir)
    ensure_dir(state_dir)
    scheduler_log = logs_dir / f"scheduler_{scheduler_name}.log"
    summary_json = Path(args.summary_json) if args.summary_json else state_dir / f"scheduler_{scheduler_name}.summary.json"

    append_line(scheduler_log, f"PLAN total_batches={len(tasks)} max_parallel={args.max_parallel}")
    for t in tasks:
        line = f"PLAN batch={t.batch_id:02d} start={t.start} end={t.end} run_id={t.run_id} log={t.log_path}"
        append_line(scheduler_log, line)
        print(line)
        if args.dry_run:
            print('CMD ' + ' '.join(t.cmd))

    if args.dry_run:
        return 0

    if args.post_only:
        if not summary_json.exists():
            raise FileNotFoundError(f'summary json not found for post-only mode: {summary_json}')
        with summary_json.open('r', encoding='utf-8') as f:
            summary = json.load(f)
        finished = summary.get('finished', [])
        if summary.get('failed_count'):
            raise RuntimeError('post-only mode requires failed_count == 0')
        artifacts, errors = run_post_processing(
            args=args,
            scheduler_name=scheduler_name,
            tasks=tasks,
            finished=finished,
            scheduler_summary=summary,
            scheduler_log=scheduler_log,
            notify_label=args.notify_label,
        )
        summary['artifacts'] = artifacts
        summary['artifacts_errors'] = errors
        with summary_json.open('w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        print(f"Wrote summary: {summary_json}")
        return 1 if errors else 0

    pending = list(tasks)
    running: List[RunningTask] = []
    finished: List[dict] = []
    t0 = time.time()

    while pending or running:
        while pending and len(running) < args.max_parallel:
            task = pending.pop(0)
            running.append(launch_task(task, scheduler_log))
        time.sleep(args.poll_seconds)
        running = poll_running(running, scheduler_log, finished, strategy=args.strategy, notify_label=args.notify_label)

    wall = time.time() - t0
    summary = make_summary(args, tasks, finished, wall)
    with summary_json.open('w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    artifacts = {}
    artifacts_errors: List[str] = []
    if summary['failed_count'] == 0 and (args.post_merge or args.build_equity):
        artifacts, artifacts_errors = run_post_processing(
            args=args,
            scheduler_name=scheduler_name,
            tasks=tasks,
            finished=finished,
            scheduler_summary=summary,
            scheduler_log=scheduler_log,
            notify_label=args.notify_label,
        )
        summary['artifacts'] = artifacts
        summary['artifacts_errors'] = artifacts_errors
        with summary_json.open('w', encoding='utf-8') as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

    append_line(
        scheduler_log,
        f"SUMMARY success={summary['success_count']} failed={summary['failed_count']} wall_clock={summary['wall_clock_seconds']}s summary_json={summary_json}",
    )
    print(f"SUMMARY success={summary['success_count']} failed={summary['failed_count']} wall_clock={summary['wall_clock_seconds']}s")
    print(f"Wrote summary: {summary_json}")
    notify_message(
        args.notify_label,
        f"[SCHED] SUMMARY | strategy={args.strategy} | runset={scheduler_name} | success={summary['success_count']} | failed={summary['failed_count']} | wall_clock={summary['wall_clock_seconds']}s | summary={summary_json}",
    )
    return 1 if summary['failed_count'] or artifacts_errors else 0


if __name__ == '__main__':
    sys.exit(main())
