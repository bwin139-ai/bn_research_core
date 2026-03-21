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

import matplotlib.pyplot as plt

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


def _extract_exit_time_ms(row: Dict[str, Any]) -> int:
    for key in ('exit_time', 'entry_time', 'signal_time'):
        v = row.get(key)
        if isinstance(v, (int, float)):
            return int(v)
    return 0


def _extract_fee_side_from_config(run_config: Dict[str, Any] | None, fallback: float) -> float:
    if not isinstance(run_config, dict):
        return fallback
    candidates = [
        run_config.get('fee_side'),
        run_config.get('backtest', {}).get('fee_side') if isinstance(run_config.get('backtest'), dict) else None,
        run_config.get('runtime', {}).get('fee_side') if isinstance(run_config.get('runtime'), dict) else None,
        run_config.get('sim', {}).get('fee_side') if isinstance(run_config.get('sim'), dict) else None,
    ]
    for v in candidates:
        fv = _safe_float(v, None)
        if fv is not None:
            return fv
    return fallback


def _prepare_trade_rows(rows: List[Dict[str, Any]], fee_side: float) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    fee_frac = fee_side * 2.0
    for row in rows:
        gross_pct = _safe_float(row.get('pnl_pct'), None)
        if gross_pct is None:
            continue
        out.append({
            'symbol': str(row.get('symbol') or ''),
            'exit_time_ms': _extract_exit_time_ms(row),
            'gross_pct': float(gross_pct),
            'net_pct': float(gross_pct) - fee_frac,
            'reason': row.get('reason'),
        })
    out.sort(key=lambda x: (x['exit_time_ms'], x['symbol']))
    return out


def _build_equity_curves(rows: List[Dict[str, Any]], initial_equity: float) -> Dict[str, List[float]]:
    simple_gross = [initial_equity]
    simple_net = [initial_equity]
    compound_gross = [initial_equity]
    compound_net = [initial_equity]
    for row in rows:
        gp = row['gross_pct']
        npct = row['net_pct']
        simple_gross.append(simple_gross[-1] + initial_equity * gp)
        simple_net.append(simple_net[-1] + initial_equity * npct)
        compound_gross.append(compound_gross[-1] * max(0.0, 1.0 + gp))
        compound_net.append(compound_net[-1] * max(0.0, 1.0 + npct))
    return {
        'simple_gross': simple_gross,
        'simple_net': simple_net,
        'compound_gross': compound_gross,
        'compound_net': compound_net,
    }


def _calc_max_drawdown(curve: List[float], times_ms: List[int]) -> Dict[str, Any]:
    peak_val = curve[0] if curve else 0.0
    peak_idx = 0
    best = {
        'days': 0.0,
        'trades': 0,
        'amount': 0.0,
        'pct': 0.0,
        'peak_index': 0,
        'trough_index': 0,
    }
    for i, val in enumerate(curve):
        if val > peak_val:
            peak_val = val
            peak_idx = i
        draw_amount = peak_val - val
        draw_pct = (draw_amount / peak_val) if peak_val > 0 else 0.0
        if draw_pct > best['pct']:
            peak_time = times_ms[peak_idx] if peak_idx < len(times_ms) else 0
            trough_time = times_ms[i] if i < len(times_ms) else peak_time
            days = max(0.0, (trough_time - peak_time) / 1000.0 / 86400.0)
            best = {
                'days': round(days, 6),
                'trades': max(0, i - peak_idx),
                'amount': round(draw_amount, 12),
                'pct': round(draw_pct * 100.0, 12),
                'peak_index': peak_idx,
                'trough_index': i,
            }
    return best


def _build_monthly_stats(rows: List[Dict[str, Any]], initial_equity: float) -> List[Dict[str, Any]]:
    monthly: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        dt = datetime.fromtimestamp(row['exit_time_ms'] / 1000.0, tz=UTC).astimezone(BJ_TZ)
        key = dt.strftime('%Y-%m')
        item = monthly.setdefault(key, {
            'month': key,
            'trade_count': 0,
            'win_count': 0,
            'loss_count': 0,
            'flat_count': 0,
            'gross_pnl_pct_sum': 0.0,
            'net_pnl_pct_sum': 0.0,
            'gross_pnl_amount_simple_100': 0.0,
            'net_pnl_amount_simple_100': 0.0,
        })
        item['trade_count'] += 1
        if row['gross_pct'] > 0:
            item['win_count'] += 1
        elif row['gross_pct'] < 0:
            item['loss_count'] += 1
        else:
            item['flat_count'] += 1
        item['gross_pnl_pct_sum'] += row['gross_pct']
        item['net_pnl_pct_sum'] += row['net_pct']
        item['gross_pnl_amount_simple_100'] += initial_equity * row['gross_pct']
        item['net_pnl_amount_simple_100'] += initial_equity * row['net_pct']
    out = []
    for key in sorted(monthly):
        item = monthly[key]
        out.append({
            'month': item['month'],
            'trade_count': item['trade_count'],
            'win_count': item['win_count'],
            'loss_count': item['loss_count'],
            'flat_count': item['flat_count'],
            'net_pnl': round(item['net_pnl_amount_simple_100'], 12),
            'gross_pnl_pct_sum': round(item['gross_pnl_pct_sum'], 12),
            'net_pnl_pct_sum': round(item['net_pnl_pct_sum'], 12),
            'gross_pnl_amount_simple_100': round(item['gross_pnl_amount_simple_100'], 12),
            'net_pnl_amount_simple_100': round(item['net_pnl_amount_simple_100'], 12),
        })
    return out


def build_extended_metrics(trades: List[Dict[str, Any]], fee_side: float, initial_equity: float) -> Dict[str, Any]:
    rows = _prepare_trade_rows(trades, fee_side)
    reason_counts: Dict[str, int] = {}
    symbols = set()
    for row in rows:
        if row['symbol']:
            symbols.add(row['symbol'])
        reason = row.get('reason')
        if reason:
            reason_counts[str(reason)] = reason_counts.get(str(reason), 0) + 1
    curves = _build_equity_curves(rows, initial_equity)
    times_ms = [rows[0]['exit_time_ms'] if rows else 0] + [r['exit_time_ms'] for r in rows]
    simple_gross_pct = sum(r['gross_pct'] for r in rows)
    simple_net_pct = sum(r['net_pct'] for r in rows)
    compound_gross_pct = ((curves['compound_gross'][-1] / initial_equity) - 1.0) if curves['compound_gross'] else 0.0
    compound_net_pct = ((curves['compound_net'][-1] / initial_equity) - 1.0) if curves['compound_net'] else 0.0
    max_drawdown = {
        'simple_gross': _calc_max_drawdown(curves['simple_gross'], times_ms),
        'simple_net': _calc_max_drawdown(curves['simple_net'], times_ms),
        'compound_gross': _calc_max_drawdown(curves['compound_gross'], times_ms),
        'compound_net': _calc_max_drawdown(curves['compound_net'], times_ms),
    }
    return {
        'signals_count': None,
        'total_trades': len(rows),
        'symbols_count': len(symbols),
        'reason_counts': reason_counts,
        'pnl_pct_sum': round(simple_gross_pct, 12),
        'pnl_pct_sum_net_fee': round(simple_net_pct, 12),
        'compound_return_pct': round(compound_gross_pct, 12),
        'compound_return_pct_net_fee': round(compound_net_pct, 12),
        'max_drawdown': max_drawdown,
        'monthly_stats': _build_monthly_stats(rows, initial_equity),
        'equity_curves': curves,
        'equity_initial': initial_equity,
        'fee_side': fee_side,
    }


def _format_dd(dd: Dict[str, Any]) -> str:
    return (
        f"回撤: {dd['amount']:.2f} ({dd['pct']:.2f}%) / {dd['days']:.1f}天 / {dd['trades']}笔"
    )


def _plot_curve(path: Path, title: str, x_vals: List[int], gross_curve: List[float], net_curve: List[float], gross_profit_pct: float, net_profit_pct: float, gross_dd: Dict[str, Any], net_dd: Dict[str, Any]) -> None:
    ensure_dir(path.parent)
    fig, ax = plt.subplots(figsize=(14, 7))
    ax.plot(x_vals, gross_curve, label='不扣手续费')
    ax.plot(x_vals, net_curve, label='扣手续费')
    ax.set_xlabel('交易笔序')
    ax.set_ylabel('权益')
    subtitle = (
        f"累计盈利 不扣费={gross_profit_pct * 100.0:.4f}% | 扣费={net_profit_pct * 100.0:.4f}%    "
        f"最大回撤 不扣费[{_format_dd(gross_dd)}]    扣费[{_format_dd(net_dd)}]"
    )
    ax.set_title(f"{title}\n{subtitle}")
    ax.grid(True, alpha=0.3)
    ax.legend()
    fig.tight_layout()
    fig.savefig(path, dpi=150)
    plt.close(fig)


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

    config_path = resolve_config_path(args.config)
    run_config: Dict[str, Any] | None = None
    if config_path.exists():
        with config_path.open('r', encoding='utf-8') as f:
            run_config = json.load(f)

    metrics = build_extended_metrics(
        trades=trades,
        fee_side=_extract_fee_side_from_config(run_config, args.equity_fee_side),
        initial_equity=args.equity_initial,
    )
    metrics['signals_count'] = len(signals)

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
        'batch_run_ids': [t.run_id for t in tasks],
        'batch_summaries': [str(Path(args.out_dir) / f'sim_summary.{t.run_id}.json') for t in tasks],
        'config_path': str(config_path),
        'run_config': run_config,
        'artifacts': artifacts,
        **{k: v for k, v in metrics.items() if k != 'equity_curves'},
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
        notify_message(
            notify_label,
            f'汇总完成｜{args.strategy}\n交易：{trades_count}｜信号：{signals_count}｜图表：{viz_count}',
        )
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

    if args.build_equity:
        append_line(scheduler_log, f'POST_EQUITY_START runset={runset}')
        print(f'POST_EQUITY_START runset={runset}')
        try:
            with merged_summary.open('r', encoding='utf-8') as f:
                all_summary = json.load(f)
            curves = build_extended_metrics(
                trades=load_jsonl(merged_trades),
                fee_side=all_summary.get('fee_side', args.equity_fee_side),
                initial_equity=all_summary.get('equity_initial', args.equity_initial),
            )
            x_vals = list(range(len(curves['equity_curves']['simple_gross'])))
            simple_png = state_dir / f'sim_curve_simple.{runset}_ALL.png'
            compound_png = state_dir / f'sim_curve_compound.{runset}_ALL.png'
            equity_json = state_dir / f'sim_equity.{runset}_ALL.json'
            _plot_curve(
                simple_png,
                '单利资金曲线',
                x_vals,
                curves['equity_curves']['simple_gross'],
                curves['equity_curves']['simple_net'],
                curves['pnl_pct_sum'],
                curves['pnl_pct_sum_net_fee'],
                curves['max_drawdown']['simple_gross'],
                curves['max_drawdown']['simple_net'],
            )
            _plot_curve(
                compound_png,
                '复利资金曲线',
                x_vals,
                curves['equity_curves']['compound_gross'],
                curves['equity_curves']['compound_net'],
                curves['compound_return_pct'],
                curves['compound_return_pct_net_fee'],
                curves['max_drawdown']['compound_gross'],
                curves['max_drawdown']['compound_net'],
            )
            equity_summary = {k: v for k, v in curves.items() if k != 'equity_curves'}
            with equity_json.open('w', encoding='utf-8') as f:
                json.dump(equity_summary, f, ensure_ascii=False, indent=2)
            artifacts['equity_curve_simple_png'] = str(simple_png)
            artifacts['equity_curve_compound_png'] = str(compound_png)
            artifacts['equity_summary_json'] = str(equity_json)
            append_line(scheduler_log, f'POST_EQUITY_DONE runset={runset} simple={simple_png} compound={compound_png} summary={equity_json}')
            print(f'POST_EQUITY_DONE runset={runset} simple={simple_png} compound={compound_png}')
            notify_message(
                notify_label,
                f'资金曲线已生成｜{args.strategy}\n单利：{simple_png.name}\n复利：{compound_png.name}',
            )
        except Exception as e:
            msg = f'build-equity failed: {e}'
            errors.append(msg)
            append_line(scheduler_log, f'POST_EQUITY_FAIL runset={runset} error={e}')
            print(f'POST_EQUITY_FAIL runset={runset} error={e}')
            notify_message(
                notify_label,
                f'资金曲线生成失败｜{args.strategy}\n原因：{e}',
            )

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
        if rc == 0:
            msg = (
                f'回测完成｜{strategy}｜第{rt.task.batch_id:02d}批\n'
                f'区间：{short_mmdd(rt.task.start)} ~ {short_mmdd(rt.task.end)}\n'
                f'耗时：{fmt_seconds_cn(elapsed)}'
            )
        else:
            msg = (
                f'回测失败｜{strategy}｜第{rt.task.batch_id:02d}批\n'
                f'区间：{short_mmdd(rt.task.start)} ~ {short_mmdd(rt.task.end)}\n'
                f'返回码：{rc}｜耗时：{fmt_seconds_cn(elapsed)}'
            )
        notify_message(notify_label, msg)
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
        f"回测总结｜{args.strategy}\n成功：{summary['success_count']}｜失败：{summary['failed_count']}｜总耗时：{fmt_seconds_cn(summary['wall_clock_seconds'])}",
    )
    return 1 if summary['failed_count'] or artifacts_errors else 0


if __name__ == '__main__':
    sys.exit(main())
