#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, List

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

BJ_TZ = timezone(timedelta(hours=8))
DEFAULT_FEE_SIDE = 0.0005


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


def _safe_float(v: object, default: float | None = None) -> float | None:
    try:
        if v is None:
            return default
        if isinstance(v, str) and not v.strip():
            return default
        return float(v)
    except Exception:
        return default


def _extract_fee_side(config: Dict[str, Any]) -> float:
    candidates = [
        config.get("fee_side"),
        config.get("backtest", {}).get("fee_side") if isinstance(config.get("backtest"), dict) else None,
        config.get("runtime", {}).get("fee_side") if isinstance(config.get("runtime"), dict) else None,
        config.get("sim", {}).get("fee_side") if isinstance(config.get("sim"), dict) else None,
    ]
    for v in candidates:
        fv = _safe_float(v, None)
        if fv is not None:
            return fv
    return DEFAULT_FEE_SIDE


def _pnl_usdt_from_trade(trade: Dict[str, object], initial_equity: float) -> float:
    notional = trade.get('position_notional_usdt')
    try:
        notional_usdt = float(notional)
    except Exception:
        notional_usdt = float(initial_equity)
    if notional_usdt <= 0:
        notional_usdt = float(initial_equity)
    return notional_usdt * float(trade['pnl_pct'])


def _hold_minutes_from_trade(trade: Dict[str, object]) -> float:
    entry_time = int(trade['entry_time'])
    exit_time = int(trade['exit_time'])
    return (exit_time - entry_time) / 60000.0


def _avg_or_none(values: List[float]) -> float | None:
    if not values:
        return None
    return round2(sum(values) / len(values))


def _max_or_none(values: List[float]) -> float | None:
    if not values:
        return None
    return round2(max(values))


def build_exit_stats(trades: List[Dict[str, object]], initial_equity: float) -> Dict[str, object]:
    take_profit_pnls: List[float] = []
    take_profit_holds: List[float] = []
    stop_loss_pnls: List[float] = []
    stop_loss_holds: List[float] = []
    timeout_pnls: List[float] = []
    timeout_holds: List[float] = []

    for trade in trades:
        reason = str(trade['reason'])
        pnl_usdt = _pnl_usdt_from_trade(trade, initial_equity=initial_equity)
        hold_minutes = _hold_minutes_from_trade(trade)

        if reason == 'TAKE_PROFIT':
            take_profit_pnls.append(pnl_usdt)
            take_profit_holds.append(hold_minutes)
        elif reason == 'STOP_LOSS':
            stop_loss_pnls.append(abs(pnl_usdt))
            stop_loss_holds.append(hold_minutes)
        elif reason == 'TIME_STOP':
            timeout_pnls.append(pnl_usdt)
            timeout_holds.append(hold_minutes)

    return {
        'take_profit_count': len(take_profit_pnls),
        'average_take_profit_usdt': _avg_or_none(take_profit_pnls),
        'average_take_profit_hold_minutes': _avg_or_none(take_profit_holds),
        'max_take_profit_hold_minutes': _max_or_none(take_profit_holds),
        'stop_loss_count': len(stop_loss_pnls),
        'average_stop_loss_usdt': _avg_or_none(stop_loss_pnls),
        'average_stop_loss_hold_minutes': _avg_or_none(stop_loss_holds),
        'max_stop_loss_hold_minutes': _max_or_none(stop_loss_holds),
        'timeout_count': len(timeout_pnls),
        'average_timeout_pnl_usdt': _avg_or_none(timeout_pnls),
        'average_timeout_hold_minutes': _avg_or_none(timeout_holds),
        'max_timeout_hold_minutes': _max_or_none(timeout_holds),
    }


def count_signal_symbols(signals: List[Dict[str, object]]) -> int:
    symbols = {str(row['symbol']).strip() for row in signals if str(row['symbol']).strip()}
    return len(symbols)


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


def build_monthly_stats(rows: List[Dict[str, Any]], initial_equity: float) -> List[Dict[str, Any]]:
    monthly: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        dt = datetime.fromtimestamp(row["exit_time_ms"] / 1000.0, tz=timezone.utc).astimezone(BJ_TZ)
        key = dt.strftime("%Y-%m")
        item = monthly.setdefault(
            key,
            {
                "month": key,
                "trade_count": 0,
                "win_count": 0,
                "loss_count": 0,
                "flat_count": 0,
                "gross_pnl_usdt": 0.0,
                "net_pnl_usdt": 0.0,
            },
        )
        item["trade_count"] += 1
        if float(row["gross_amount_usdt"]) > 0:
            item["win_count"] += 1
        elif float(row["gross_amount_usdt"]) < 0:
            item["loss_count"] += 1
        else:
            item["flat_count"] += 1
        item["gross_pnl_usdt"] += float(row["gross_amount_usdt"])
        item["net_pnl_usdt"] += float(row["net_amount_usdt"])

    out: List[Dict[str, Any]] = []
    for key in sorted(monthly.keys()):
        item = monthly[key]
        gross_pnl_usdt = float(item["gross_pnl_usdt"])
        net_pnl_usdt = float(item["net_pnl_usdt"])
        out.append(
            {
                "month": item["month"],
                "trade_count": int(item["trade_count"]),
                "win_count": int(item["win_count"]),
                "loss_count": int(item["loss_count"]),
                "flat_count": int(item["flat_count"]),
                "net_pnl_usdt": round2(net_pnl_usdt),
                "gross_return_pct": round2(gross_pnl_usdt / initial_equity * 100.0),
                "net_return_pct": round2(net_pnl_usdt / initial_equity * 100.0),
                "gross_pnl_usdt_simple_100": round2(gross_pnl_usdt),
                "net_pnl_usdt_simple_100": round2(net_pnl_usdt),
            }
        )
    return out


def build_normalized_metrics(trades: List[Dict[str, object]], fee_side: float, initial_equity: float, show_gross: bool = False) -> Dict[str, object]:
    try:
        from core.analysis.sim_equity_curves import build_equity_payload, prepare_rows  # type: ignore

        rows = prepare_rows(trades, fee_side, initial_equity)
        equity_metrics = build_equity_payload(rows, initial_equity, show_gross=show_gross)
    except Exception as exc:
        raise RuntimeError(f"failed to build normalized equity metrics: {exc}") from exc

    out = {
        "equity_initial_usdt": round2(initial_equity),
        "fee_side_pct": round2(fee_side * 100.0),
        "trade_count": len(trades),
        "final_equity_simple_net_usdt": equity_metrics["final_equity_simple_net_usdt"],
        "final_equity_compound_net_usdt": equity_metrics["final_equity_compound_net_usdt"],
        "return_simple_net_pct": equity_metrics["return_simple_net_pct"],
        "return_compound_net_pct": equity_metrics["return_compound_net_pct"],
        "max_drawdown": dict(equity_metrics["max_drawdown"]),
        "monthly_stats": build_monthly_stats(rows, initial_equity),
    }
    if show_gross:
        out.update(
            {
                "final_equity_simple_gross_usdt": equity_metrics["final_equity_simple_gross_usdt"],
                "final_equity_compound_gross_usdt": equity_metrics["final_equity_compound_gross_usdt"],
                "return_simple_gross_pct": equity_metrics["return_simple_gross_pct"],
                "return_compound_gross_pct": equity_metrics["return_compound_gross_pct"],
            }
        )
    return out


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
    ap.add_argument('--show-gross', action='store_true')
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
    run_config: Dict[str, object] | None = None
    if config_path and config_path.exists():
        with config_path.open('r', encoding='utf-8') as f:
            run_config = json.load(f)

    trades = load_jsonl(merged_trades)
    signals = load_jsonl(merged_signals)
    fee_side = _extract_fee_side(run_config or {}) if run_config is not None else args.fee_side
    metrics = build_normalized_metrics(trades, fee_side=fee_side, initial_equity=args.initial_equity, show_gross=args.show_gross)
    exit_stats = build_exit_stats(trades, initial_equity=args.initial_equity)

    metrics = {
        **metrics,
        'signals_count': len(signals),
        'symbols_count': count_signal_symbols(signals),
        **exit_stats,
    }

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
        if args.show_gross:
            cmd.append('--show-gross')
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
