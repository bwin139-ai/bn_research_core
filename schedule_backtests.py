#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional

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
    rt = RunningTask(task=task, proc=proc, started_at=time.time())
    append_line(
        scheduler_log,
        f"START batch={task.batch_id:02d} pid={proc.pid} run_id={task.run_id} start={task.start} end={task.end} log={task.log_path}",
    )
    print(f"START batch={task.batch_id:02d} pid={proc.pid} run_id={task.run_id}")
    return rt


def poll_running(running: List[RunningTask], scheduler_log: Path, finished: List[dict]) -> List[RunningTask]:
    keep: List[RunningTask] = []
    for rt in running:
        rc = rt.proc.poll()
        if rc is None:
            keep.append(rt)
            continue
        elapsed = time.time() - rt.started_at
        rec = {
            'batch_id': rt.task.batch_id,
            'run_id': rt.task.run_id,
            'return_code': rc,
            'elapsed_seconds': round(elapsed, 3),
            'log_path': rt.task.log_path,
            'start': rt.task.start,
            'end': rt.task.end,
        }
        finished.append(rec)
        status = 'DONE' if rc == 0 else 'FAIL'
        append_line(
            scheduler_log,
            f"{status} batch={rt.task.batch_id:02d} rc={rc} elapsed={elapsed:.1f}s run_id={rt.task.run_id} log={rt.task.log_path}",
        )
        print(f"{status} batch={rt.task.batch_id:02d} rc={rc} elapsed={elapsed:.1f}s run_id={rt.task.run_id}")
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

    pending = list(tasks)
    running: List[RunningTask] = []
    finished: List[dict] = []
    t0 = time.time()

    while pending or running:
        while pending and len(running) < args.max_parallel:
            task = pending.pop(0)
            running.append(launch_task(task, scheduler_log))
        time.sleep(args.poll_seconds)
        running = poll_running(running, scheduler_log, finished)

    wall = time.time() - t0
    summary = make_summary(args, tasks, finished, wall)
    with summary_json.open('w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    append_line(
        scheduler_log,
        f"SUMMARY success={summary['success_count']} failed={summary['failed_count']} wall_clock={summary['wall_clock_seconds']}s summary_json={summary_json}",
    )
    print(f"SUMMARY success={summary['success_count']} failed={summary['failed_count']} wall_clock={summary['wall_clock_seconds']}s")
    print(f"Wrote summary: {summary_json}")
    return 1 if summary['failed_count'] else 0


if __name__ == '__main__':
    sys.exit(main())
