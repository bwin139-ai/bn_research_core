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
BJ_TZ = timezone(timedelta(hours=8))
DEFAULT_FEE_SIDE = 0.0005


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


def short_mmdd(iso_utc: str) -> str:
    return parse_iso_utc(iso_utc).strftime('%m-%d')


def fmt_seconds_cn(seconds: float) -> str:
    return f'{seconds:.0f}秒'


def _safe_float(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None:
            return default
        if isinstance(v, str) and not v.strip():
            return default
        return float(v)
    except Exception:
        return default


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
    # Rebuild merged viz directory from scratch to avoid stale PNGs from previous runs.
    if out_dir.exists():
        shutil.rmtree(out_dir)
    ensure_dir(out_dir)
    copied = 0
    for path in paths:
        if not path.exists() or not path.is_dir():
            continue
        for src in path.glob('*.png'):
            dst = out_dir / src.name
            if dst.exists():
                stem = src.stem
                suffix = src.suffix
                idx = 2
                while True:
                    alt = out_dir / f'{stem}__dup{idx}{suffix}'
                    if not alt.exists():
                        dst = alt
                        break
                    idx += 1
            shutil.copy2(src, dst)
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


def normalize_run_backtest_config_arg(config_arg: str) -> str:
    # strategies/run_backtest.py resolves --config relative to the strategies/ directory.
    # Accept both spring/config.json and strategies/spring/config.json at scheduler level.
    path = Path(config_arg)
    parts = path.parts
    if parts and parts[0] == 'strategies':
        if len(parts) == 1:
            raise ValueError('--config cannot be just "strategies"')
        return str(Path(*parts[1:]))
    return config_arg


def supports_candidate_audit(strategy: str) -> bool:
    return str(strategy or '').strip() == 'snapback'


def supports_spring_decision_audit(strategy: str) -> bool:
    return str(strategy or '').strip() == 'spring-sabc'


def build_merge_meta(
    args: argparse.Namespace,
    runset: str,
    tasks: List[Task],
    scheduler_summary: dict,
    artifacts: Dict[str, str],
) -> dict:
    return {
        'summary_scope': 'ALL_META',
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
        'batch_run_ids': [t.run_id for t in tasks],
        'batch_summaries': [str(Path(args.out_dir) / f'sim_summary.{t.run_id}.json') for t in tasks],
        'config_path': str(resolve_config_path(args.config)),
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
    candidate_audit_enabled = supports_candidate_audit(args.strategy)
    candidate_audit_paths = [state_dir / f'snapback_candidate_pool_audit.{t.run_id}.jsonl' for t in tasks] if candidate_audit_enabled else []
    spring_decision_audit_enabled = supports_spring_decision_audit(args.strategy)
    spring_decision_audit_paths = [state_dir / f'spring_decision_audit.{t.run_id}.jsonl' for t in tasks] if spring_decision_audit_enabled else []
    viz_dirs = [state_dir / f'sim_viz_{t.run_id}' for t in tasks]

    merged_trades = state_dir / f'sim_trades.{runset}_ALL.jsonl'
    merged_signals = state_dir / f'sim_signals.{runset}_ALL.jsonl'
    merged_candidate_audit = state_dir / f'snapback_candidate_pool_audit.{runset}_ALL.jsonl' if candidate_audit_enabled else None
    merged_spring_decision_audit = state_dir / f'spring_decision_audit.{runset}_ALL.jsonl' if spring_decision_audit_enabled else None
    merged_viz_dir = state_dir / f'sim_viz_{runset}_ALL'
    merged_merge_meta = state_dir / f'sim_merge_meta.{runset}_ALL.json'
    merged_summary = state_dir / f'sim_summary.{runset}_ALL.json'

    try:
        trades_count = merge_jsonl_files(trade_paths, merged_trades)
        artifacts['merged_trades'] = str(merged_trades)
        signals_count = merge_jsonl_files(signal_paths, merged_signals)
        artifacts['merged_signals'] = str(merged_signals)
        candidate_audit_count = 0
        if candidate_audit_enabled and merged_candidate_audit is not None:
            candidate_audit_count = merge_jsonl_files(candidate_audit_paths, merged_candidate_audit)
            artifacts['merged_candidate_audit'] = str(merged_candidate_audit)
        spring_decision_audit_count = 0
        if spring_decision_audit_enabled and merged_spring_decision_audit is not None:
            spring_decision_audit_count = merge_jsonl_files(spring_decision_audit_paths, merged_spring_decision_audit)
            artifacts['merged_spring_decision_audit'] = str(merged_spring_decision_audit)
        viz_count = merge_viz_dirs(viz_dirs, merged_viz_dir)
        artifacts['merged_viz_dir'] = str(merged_viz_dir)

        merge_meta = build_merge_meta(
            args=args,
            runset=runset,
            tasks=tasks,
            scheduler_summary=scheduler_summary,
            artifacts=artifacts.copy(),
        )
        merge_meta.update({
            'trades_count': trades_count,
            'signals_count': signals_count,
            'candidate_audit_count': candidate_audit_count,
            'spring_decision_audit_count': spring_decision_audit_count,
            'viz_png_count': viz_count,
        })
        with merged_merge_meta.open('w', encoding='utf-8') as f:
            json.dump(merge_meta, f, ensure_ascii=False, indent=2)
        artifacts['merged_merge_meta'] = str(merged_merge_meta)

        append_line(
            scheduler_log,
            f'POST_MERGE_DONE runset={runset} trades={trades_count} signals={signals_count} candidate_audits={candidate_audit_count} spring_decision_audits={spring_decision_audit_count} viz_pngs={viz_count} merge_meta={merged_merge_meta}',
        )
        print(f'POST_MERGE_DONE runset={runset} trades={trades_count} signals={signals_count} candidate_audits={candidate_audit_count} spring_decision_audits={spring_decision_audit_count} viz_pngs={viz_count}')
        if candidate_audit_enabled:
            merge_notify = f'汇总完成｜{args.strategy}\n交易：{trades_count}｜信号：{signals_count}｜候选池：{candidate_audit_count}｜图表：{viz_count}'
        elif spring_decision_audit_enabled:
            merge_notify = f'汇总完成｜{args.strategy}\n交易：{trades_count}｜信号：{signals_count}｜Spring审计：{spring_decision_audit_count}｜图表：{viz_count}'
        else:
            merge_notify = f'汇总完成｜{args.strategy}\n交易：{trades_count}｜信号：{signals_count}｜图表：{viz_count}'
        notify_message(notify_label, merge_notify)
    except Exception as e:
        msg = f'post-merge failed: {e}'
        errors.append(msg)
        append_line(scheduler_log, f'POST_MERGE_FAIL runset={runset} error={e}')
        print(f'POST_MERGE_FAIL runset={runset} error={e}')
        notify_message(
            notify_label,
            f'汇总失败｜{args.strategy}\n原因：{e}',
        )
        return artifacts, errors

    append_line(scheduler_log, f'POST_ANALYSIS_START runset={runset}')
    print(f'POST_ANALYSIS_START runset={runset}')
    try:
        cmd = [
            args.python_bin,
            args.postprocess_script,
            '--run-id', f'{runset}_ALL',
            '--state-dir', str(state_dir),
            '--merge-meta', str(merged_merge_meta),
            '--kline-root', args.kline_root,
            '--initial-equity', str(args.equity_initial),
            '--fee-side', str(args.equity_fee_side),
        ]
        if args.build_equity:
            cmd.append('--build-equity')
        subprocess.run(cmd, check=True)

        artifacts['merged_summary'] = str(merged_summary)
        if args.build_equity:
            artifacts['equity_curve_simple_png'] = str(state_dir / f'sim_curve_simple.{runset}_ALL.png')
            artifacts['equity_curve_compound_png'] = str(state_dir / f'sim_curve_compound.{runset}_ALL.png')
            artifacts['equity_summary_json'] = str(state_dir / f'sim_equity.{runset}_ALL.json')

        append_line(scheduler_log, f'POST_ANALYSIS_DONE runset={runset} merged_summary={merged_summary}')
        print(f'POST_ANALYSIS_DONE runset={runset} merged_summary={merged_summary}')
        if args.build_equity:
            notify_message(
                notify_label,
                f'绩效分析完成｜{args.strategy}\nSummary：{merged_summary.name}\n单利：sim_curve_simple.{runset}_ALL.png\n复利：sim_curve_compound.{runset}_ALL.png',
            )
        else:
            notify_message(
                notify_label,
                f'绩效分析完成｜{args.strategy}\nSummary：{merged_summary.name}',
            )
    except Exception as e:
        msg = f'post-analysis failed: {e}'
        errors.append(msg)
        append_line(scheduler_log, f'POST_ANALYSIS_FAIL runset={runset} error={e}')
        print(f'POST_ANALYSIS_FAIL runset={runset} error={e}')
        notify_message(
            notify_label,
            f'绩效分析失败｜{args.strategy}\n原因：{e}',
        )

    if artifacts.get('merged_summary'):
        merged_summary_path = Path(artifacts['merged_summary'])
        if merged_summary_path.exists():
            with merged_summary_path.open('r', encoding='utf-8') as f:
                all_summary = json.load(f)
            all_summary['artifacts'] = artifacts
            with merged_summary_path.open('w', encoding='utf-8') as f:
                json.dump(all_summary, f, ensure_ascii=False, indent=2)

    return artifacts, errors

def build_batches(start_dt: datetime, end_dt: datetime, batch_days: int) -> List[tuple[datetime, datetime]]:
    if end_dt <= start_dt:
        raise ValueError('end must be > start')
    if batch_days <= 0:
        raise ValueError('batch_days must be > 0')
    batches: List[tuple[datetime, datetime]] = []
    cursor = start_dt
    while cursor < end_dt:
        nxt = min(cursor + timedelta(days=batch_days), end_dt)
        batches.append((cursor, nxt))
        cursor = nxt
    return batches


def build_tasks(args: argparse.Namespace) -> List[Task]:
    start_dt = parse_iso_utc(args.start)
    end_dt = parse_iso_utc(args.end)
    batches = build_batches(start_dt, end_dt, args.batch_days)
    out_dir = Path(args.out_dir)
    ensure_dir(out_dir)
    logs_dir = Path(args.logs_dir)
    ensure_dir(logs_dir)

    backtest_config = normalize_run_backtest_config_arg(args.config)

    tasks: List[Task] = []
    for i, (b_start, b_end) in enumerate(batches, start=1):
        run_id = f"{args.run_prefix or args.strategy.upper()}_{short_mmdd(fmt_dt(b_start))}_{short_mmdd(fmt_dt(b_end))}_B{i:02d}"
        log_path = logs_dir / f"{run_id}.console.log"
        cmd = [
            args.python_bin,
            "strategies/run_backtest.py",
            "--strategy", args.strategy,
            "--start", fmt_dt(b_start),
            "--end", fmt_dt(b_end),
            "--kline-window", str(args.kline_window),
            "--run-id", run_id,
            "--config", backtest_config,
            "--out-dir", args.out_dir,
        ]
        tasks.append(
            Task(
                batch_id=i,
                start=fmt_dt(b_start),
                end=fmt_dt(b_end),
                run_id=run_id,
                log_path=str(log_path),
                cmd=cmd,
            )
        )
    return tasks


def append_line(path: Path, line: str) -> None:
    ensure_dir(path.parent)
    with path.open('a', encoding='utf-8') as f:
        f.write(line.rstrip() + '\n')


def launch_task(task: Task, scheduler_log: Path) -> RunningTask:
    ensure_dir(Path(task.log_path).parent)
    logf = open(task.log_path, 'w', encoding='utf-8')
    proc = subprocess.Popen(
        task.cmd,
        stdout=logf,
        stderr=subprocess.STDOUT,
        text=True,
    )
    line = f"LAUNCH batch={task.batch_id:02d} run_id={task.run_id} pid={proc.pid} start={task.start} end={task.end} log={task.log_path}"
    append_line(scheduler_log, line)
    print(line)
    return RunningTask(task=task, proc=proc, started_at=time.time())


def poll_running(
    running: List[RunningTask],
    scheduler_log: Path,
    finished: List[dict],
    strategy: Optional[str] = None,
    notify_label: Optional[str] = None,
) -> List[RunningTask]:
    keep: List[RunningTask] = []
    for rt in running:
        rc = rt.proc.poll()
        if rc is None:
            keep.append(rt)
            continue
        elapsed = round(time.time() - rt.started_at, 3)
        result = {
            'batch_id': rt.task.batch_id,
            'run_id': rt.task.run_id,
            'return_code': rc,
            'elapsed_seconds': elapsed,
            'log_path': rt.task.log_path,
            'start': rt.task.start,
            'end': rt.task.end,
        }
        finished.append(result)
        line = f"DONE batch={rt.task.batch_id:02d} run_id={rt.task.run_id} rc={rc} elapsed={elapsed}s log={rt.task.log_path}"
        append_line(scheduler_log, line)
        print(line)
        status_text = '成功' if rc == 0 else '失败'
        if strategy:
            notify_message(
                notify_label,
                f"批次{status_text}｜{strategy}\nrun_id：{rt.task.run_id}\n耗时：{fmt_seconds_cn(elapsed)}\n返回码：{rc}",
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
    ap.add_argument('--strategy', required=True, choices=['snapback', 'spring-sabc'])
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
    ap.add_argument('--equity-script', default='core/analysis/sim_equity_curves.py')
    ap.add_argument('--postprocess-script', default='core/analysis/postprocess_backtests.py')
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

    append_line(
        scheduler_log,
        f"START scheduler={scheduler_name} strategy={args.strategy} total_batches={len(tasks)} max_parallel={args.max_parallel}",
    )
    print(f"SCHEDULER {scheduler_name} batches={len(tasks)} max_parallel={args.max_parallel}")

    for t in tasks:
        line = f"PLAN batch={t.batch_id:02d} run_id={t.run_id} start={t.start} end={t.end} log={t.log_path}"
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
        f"回测总结｜{args.strategy}\n成功：{summary['success_count']}｜失败：{summary['failed_count']}｜总耗时：{fmt_seconds_cn(summary['wall_clock_seconds'])}",
    )
    return 1 if summary['failed_count'] or artifacts_errors else 0


if __name__ == '__main__':
    sys.exit(main())
