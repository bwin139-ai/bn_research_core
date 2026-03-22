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


def round2(value: float) -> float:
    return round(float(value), 2)


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


def resolve_merge_meta_path(run_id: str, state_dir: Path, explicit: str | None) -> Path:
    if explicit:
        return Path(explicit)
    return state_dir / f"sim_merge_meta.{run_id}.json"


def load_meta_fallback(run_id: str, state_dir: Path, explicit_merge_meta: str | None) -> Dict[str, Any]:
    merge_meta_path = resolve_merge_meta_path(run_id, state_dir, explicit_merge_meta)
    if merge_meta_path.exists():
        with merge_meta_path.open('r', encoding='utf-8') as f:
            meta = json.load(f)
        meta["_meta_source"] = "merge_meta"
        meta["_meta_path"] = str(merge_meta_path)
        return meta

    summary_path = state_dir / f"sim_summary.{run_id}.json"
    if summary_path.exists():
        with summary_path.open('r', encoding='utf-8') as f:
            old = json.load(f)
        meta = {
            "strategy_name": old.get("strategy_name") or old.get("strategy"),
            "run_id": old.get("run_id", run_id),
            "start": old.get("start"),
            "end": old.get("end"),
            "batch_days": old.get("batch_days"),
            "max_parallel": old.get("max_parallel"),
            "batch_count": old.get("batch_count"),
            "success_count": old.get("success_count"),
            "failed_count": old.get("failed_count"),
            "wall_clock_seconds": old.get("wall_clock_seconds"),
            "batch_run_ids": old.get("batch_run_ids", []),
            "batch_summaries": old.get("batch_summaries", []),
            "config_path": old.get("config_path") or old.get("config"),
            "artifacts": dict(old.get("artifacts", {})),
        }
        meta["_meta_source"] = "summary_fallback"
        meta["_meta_path"] = str(summary_path)
        return meta

    raise FileNotFoundError(
        f"merge meta not found: {merge_meta_path}; fallback summary also not found: {summary_path}"
    )


def normalize_monthly_stats(monthly_stats: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in monthly_stats:
        out.append(
            {
                "month": row.get("month"),
                "trade_count": int(row.get("trade_count", 0)),
                "win_count": int(row.get("win_count", 0)),
                "loss_count": int(row.get("loss_count", 0)),
                "flat_count": int(row.get("flat_count", 0)),
                "net_pnl_usdt": round2(row.get("net_pnl", 0.0)),
                "gross_return_pct": round2(float(row.get("gross_pnl_pct_sum", 0.0)) * 100.0),
                "net_return_pct": round2(float(row.get("net_pnl_pct_sum", 0.0)) * 100.0),
                "gross_pnl_usdt_simple_100": round2(row.get("gross_pnl_amount_simple_100", 0.0)),
                "net_pnl_usdt_simple_100": round2(row.get("net_pnl_amount_simple_100", 0.0)),
            }
        )
    return out


def build_normalized_metrics(trades: List[Dict[str, Any]], fee_side: float, initial_equity: float) -> Dict[str, Any]:
    raw = build_extended_summary_metrics(trades, fee_side=fee_side, initial_equity=initial_equity)
    try:
        from core.analysis.sim_equity_curves import build_equity_payload, prepare_rows  # type: ignore

        equity_metrics = build_equity_payload(prepare_rows(trades, fee_side), initial_equity)
    except Exception as exc:
        raise RuntimeError(f"failed to build normalized equity metrics: {exc}") from exc

    return {
        "equity_initial_usdt": round2(initial_equity),
        "fee_side_pct": round2(fee_side * 100.0),
        "trade_count": len(trades),
        "signals_count": 0,
        "final_equity_simple_gross_usdt": equity_metrics["final_equity_simple_gross_usdt"],
        "final_equity_simple_net_usdt": equity_metrics["final_equity_simple_net_usdt"],
        "final_equity_compound_gross_usdt": equity_metrics["final_equity_compound_gross_usdt"],
        "final_equity_compound_net_usdt": equity_metrics["final_equity_compound_net_usdt"],
        "return_simple_gross_pct": equity_metrics["return_simple_gross_pct"],
        "return_simple_net_pct": equity_metrics["return_simple_net_pct"],
        "return_compound_gross_pct": equity_metrics["return_compound_gross_pct"],
        "return_compound_net_pct": equity_metrics["return_compound_net_pct"],
        "max_drawdown": dict(equity_metrics["max_drawdown"]),
        "monthly_stats": normalize_monthly_stats(raw.get("monthly_stats", [])),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description='Build merged backtest summary and optional equity curves.')
    ap.add_argument('--run-id', required=True)
    ap.add_argument('--state-dir', default='output/state')
    ap.add_argument('--merge-meta', default=None, help='Optional. Defaults to state-dir/sim_merge_meta.<RUN_ID>.json; falls back to sim_summary.<RUN_ID>.json')
    ap.add_argument('--kline-root', default='data/klines_1m')
    ap.add_argument('--initial-equity', type=float, default=100.0)
    ap.add_argument('--fee-side', type=float, default=0.0005)
    ap.add_argument('--equity-script', default='core/analysis/sim_equity_curves.py')
    ap.add_argument('--build-equity', action='store_true')
    args = ap.parse_args()

    state_dir = Path(args.state_dir)
    run_id = args.run_id
    merged_trades = state_dir / f'sim_trades.{run_id}.jsonl'
    merged_signals = state_dir / f'sim_signals.{run_id}.jsonl'
    merged_summary = state_dir / f'sim_summary.{run_id}.json'

    if not merged_trades.exists():
        raise FileNotFoundError(f'merged trades not found: {merged_trades}')
    if not merged_signals.exists():
        raise FileNotFoundError(f'merged signals not found: {merged_signals}')

    merge_meta = load_meta_fallback(run_id, state_dir, args.merge_meta)

    config_path = Path(merge_meta['config_path']) if merge_meta.get('config_path') else Path()
    run_config: Dict[str, Any] | None = None
    if config_path and config_path.exists():
        with config_path.open('r', encoding='utf-8') as f:
            run_config = json.load(f)

    trades = load_jsonl(merged_trades)
    signals = load_jsonl(merged_signals)
    fee_side = _extract_fee_side(run_config or {}) if run_config is not None else args.fee_side
    metrics = build_normalized_metrics(trades, fee_side=fee_side, initial_equity=args.initial_equity)
    metrics['signals_count'] = len(signals)

    out = {
        'summary_scope': 'ALL',
        'generated_by': 'core/analysis/postprocess_backtests.py',
        'strategy_name': merge_meta.get('strategy_name'),
        'run_id': run_id,
        'start': merge_meta.get('start'),
        'end': merge_meta.get('end'),
        'batch_days': merge_meta.get('batch_days'),
        'max_parallel': merge_meta.get('max_parallel'),
        'batch_count': merge_meta.get('batch_count'),
        'success_count': merge_meta.get('success_count'),
        'failed_count': merge_meta.get('failed_count'),
        'wall_clock_seconds': merge_meta.get('wall_clock_seconds'),
        'batch_run_ids': merge_meta.get('batch_run_ids', []),
        'batch_summaries': merge_meta.get('batch_summaries', []),
        'config_path': str(config_path) if config_path else None,
        'run_config': run_config,
        'artifacts': dict(merge_meta.get('artifacts', {})),
        'meta_source': merge_meta.get('_meta_source'),
        'meta_path': merge_meta.get('_meta_path'),
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
