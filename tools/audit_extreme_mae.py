#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
极端 MAE 审计工具（最小版）
用途：
1. 从 sim_trades.<RUNID>.jsonl 中筛出高风险样本
2. 计算几个关键衍生指标，帮助人工图审
3. 导出 csv / json，便于后续逐笔审计
"""

from __future__ import annotations

import argparse
import csv
import json
import math
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pyarrow.parquet as pq

DEFAULT_STATE_DIR = Path("output/state")
DEFAULT_DATA_DIR = Path("data/klines_1m")


def find_trades_file(run_id: str, state_dir: Path) -> Path:
    p = state_dir / f"sim_trades.{run_id}.jsonl"
    if not p.exists():
        raise FileNotFoundError(f"未找到交易文件: {p}")
    return p


def safe_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if math.isnan(x) or math.isinf(x):
        return None
    return x


def pct(v: Optional[float]) -> Optional[float]:
    return None if v is None else v * 100.0


def pct_identity(v: Optional[float]) -> Optional[float]:
    return v


def first_present(*values: Any) -> Any:
    for v in values:
        if v is not None:
            return v
    return None


def safe_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def pct_change(base: Optional[float], target: Optional[float]) -> Optional[float]:
    if base is None or target is None or abs(base) <= 1e-12:
        return None
    return (target / base - 1.0) * 100.0


def bars_between(start_ms: Optional[int], end_ms: Optional[int]) -> Optional[int]:
    if start_ms is None or end_ms is None:
        return None
    diff_ms = end_ms - start_ms
    if diff_ms < 0:
        return None
    return int(round(diff_ms / 60000.0))


def extract_abc_geometry(row: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Any]:
    a_time = safe_int(first_present(
        context.get("a_time"),
        context.get("a_time_ms"),
        context.get("a_open_time"),
        context.get("a_open_time_ms"),
        context.get("a_ts"),
    ))
    b_time = safe_int(first_present(
        context.get("b_time"),
        context.get("b_time_ms"),
        context.get("b_open_time"),
        context.get("b_open_time_ms"),
        context.get("b_ts"),
    ))
    c_time = safe_int(first_present(
        context.get("c_time"),
        context.get("c_time_ms"),
        context.get("c_open_time"),
        context.get("c_open_time_ms"),
        context.get("c_ts"),
        row.get("entry_time"),
    ))

    a_price = safe_float(first_present(
        context.get("a_high_price"),
        context.get("a_price"),
        context.get("a_contract_price"),
        context.get("a_index_price"),
    ))
    b_contract_price = safe_float(context.get("b_contract_price"))
    b_index_price = safe_float(context.get("b_index_price"))
    c_price = safe_float(first_present(
        context.get("c_price"),
        context.get("c_close_price"),
        row.get("entry_price"),
    ))

    ab_bars = bars_between(a_time, b_time)
    bc_bars = bars_between(b_time, c_time)
    ac_bars = bars_between(a_time, c_time)

    ab_drop_to_b_index_pct = pct_change(a_price, b_index_price)
    ab_drop_to_b_contract_pct = pct_change(a_price, b_contract_price)
    bc_rebound_from_b_index_pct = pct_change(b_index_price, c_price)
    bc_rebound_from_b_contract_pct = pct_change(b_contract_price, c_price)

    bc_vs_ab_ratio_index: Optional[float] = None
    if bc_rebound_from_b_index_pct is not None and ab_drop_to_b_index_pct is not None and abs(ab_drop_to_b_index_pct) > 1e-12:
        bc_vs_ab_ratio_index = abs(bc_rebound_from_b_index_pct) / abs(ab_drop_to_b_index_pct)

    bc_vs_ab_ratio_contract: Optional[float] = None
    if bc_rebound_from_b_contract_pct is not None and ab_drop_to_b_contract_pct is not None and abs(ab_drop_to_b_contract_pct) > 1e-12:
        bc_vs_ab_ratio_contract = abs(bc_rebound_from_b_contract_pct) / abs(ab_drop_to_b_contract_pct)

    return {
        "a_time": a_time,
        "b_time": b_time,
        "c_time": c_time,
        "c_price": c_price,
        "ab_bars": ab_bars,
        "bc_bars": bc_bars,
        "ac_bars": ac_bars,
        "ab_drop_to_b_index_pct": ab_drop_to_b_index_pct,
        "ab_drop_to_b_contract_pct": ab_drop_to_b_contract_pct,
        "bc_rebound_from_b_index_pct": bc_rebound_from_b_index_pct,
        "bc_rebound_from_b_contract_pct": bc_rebound_from_b_contract_pct,
        "bc_vs_ab_ratio_index": bc_vs_ab_ratio_index,
        "bc_vs_ab_ratio_contract": bc_vs_ab_ratio_contract,
    }


def calc_mfe_mae(data_dir: Path, symbol: str, entry_time: int, exit_time: int, entry_price: float) -> Tuple[Optional[float], Optional[float], str]:
    if entry_price <= 0:
        return None, None, "missing_or_invalid_entry_price"

    sym_dir = data_dir / symbol
    if not sym_dir.is_dir():
        return None, None, "symbol_dir_not_found"

    pq_files = sorted(sym_dir.glob("*.parquet"))
    if not pq_files:
        return None, None, "parquet_not_found"

    try:
        df = pq.read_table([str(x) for x in pq_files]).to_pandas()
    except Exception:
        return None, None, "parquet_read_failed"

    if "open_time_ms" not in df.columns or "high" not in df.columns or "low" not in df.columns:
        return None, None, "required_columns_missing"

    hold_df = df[(df["open_time_ms"] >= entry_time) & (df["open_time_ms"] <= exit_time)]
    if hold_df.empty:
        return None, None, "hold_window_empty"

    try:
        high_max = float(hold_df["high"].max())
        low_min = float(hold_df["low"].min())
    except Exception:
        return None, None, "high_low_calc_failed"

    mfe_pct = (high_max / entry_price - 1.0) * 100.0
    mae_pct = (low_min / entry_price - 1.0) * 100.0
    return mfe_pct, mae_pct, "recomputed_from_klines"


def read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError as e:
                raise ValueError(f"JSONL 第 {i} 行解析失败: {e}") from e
    return rows


def _safe_int_ms(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _load_symbol_df(data_dir: Path, symbol: str):
    sym_dir = data_dir / symbol
    if not sym_dir.is_dir():
        return None
    pq_files = sorted(sym_dir.glob("*.parquet"))
    if not pq_files:
        return None
    try:
        df = pq.read_table([str(x) for x in pq_files]).to_pandas()
    except Exception:
        return None
    if "open_time_ms" not in df.columns:
        return None
    return df


def _closest_row_time_ms(df, col: str, target: Optional[float]) -> Optional[int]:
    if df is None or target is None or col not in df.columns or df.empty:
        return None
    try:
        s = df[col].astype(float)
        idx = (s - float(target)).abs().idxmin()
        return int(df.loc[idx, "open_time_ms"])
    except Exception:
        return None


def _bars_between_ms(start_ms: Optional[int], end_ms: Optional[int], bar_ms: int = 60_000) -> Optional[int]:
    if start_ms is None or end_ms is None:
        return None
    if end_ms < start_ms:
        return None
    try:
        return int((end_ms - start_ms) // bar_ms)
    except Exception:
        return None


def _derive_abc_geometry(
    row: Dict[str, Any],
    context: Dict[str, Any],
    params: Dict[str, Any],
    data_dir: Path,
) -> Dict[str, Any]:
    a_time = _safe_int_ms(context.get("a_time"))
    b_time = _safe_int_ms(context.get("b_time"))
    c_time = _safe_int_ms(context.get("c_time", row.get("entry_time") or row.get("entry_time_bj")))

    a_high_price = safe_float(context.get("a_high_price"))
    b_contract_price = safe_float(context.get("b_contract_price"))
    b_index_price = safe_float(context.get("b_index_price"))
    c_price = safe_float(context.get("c_price", row.get("entry_price")))

    need_fill_times = a_time is None or b_time is None
    symbol = str(row.get("symbol") or "")
    drop_window_mins = _safe_int_ms(
        context.get("drop_window_mins", params.get("drop_window_mins"))
    )

    if need_fill_times and symbol and c_time is not None and drop_window_mins and drop_window_mins > 0:
        df = _load_symbol_df(data_dir, symbol)
        if df is not None:
            try:
                start_ms = c_time - int(drop_window_mins) * 60_000
                win = df[(df["open_time_ms"] >= start_ms) & (df["open_time_ms"] <= c_time)].copy()
            except Exception:
                win = None

            if win is not None and not win.empty:
                if b_time is None:
                    if b_index_price is not None and "low_idx" in win.columns:
                        b_time = _closest_row_time_ms(win, "low_idx", b_index_price)
                    if b_time is None and b_contract_price is not None and "low" in win.columns:
                        b_time = _closest_row_time_ms(win, "low", b_contract_price)

                if a_time is None:
                    a_win = win
                    if b_time is not None:
                        try:
                            a_win = win[win["open_time_ms"] <= b_time].copy()
                        except Exception:
                            a_win = win
                    if a_win is not None and not a_win.empty:
                        if a_high_price is not None and "high" in a_win.columns:
                            a_time = _closest_row_time_ms(a_win, "high", a_high_price)
                        if a_time is None and "high" in a_win.columns:
                            try:
                                idx = a_win["high"].astype(float).idxmax()
                                a_time = int(a_win.loc[idx, "open_time_ms"])
                            except Exception:
                                pass

    ab_bars = _bars_between_ms(a_time, b_time)
    bc_bars = _bars_between_ms(b_time, c_time)
    ac_bars = _bars_between_ms(a_time, c_time)

    ab_drop_to_b_index_pct = None
    if a_high_price is not None and b_index_price is not None and a_high_price > 0:
        ab_drop_to_b_index_pct = (1.0 - b_index_price / a_high_price) * 100.0

    ab_drop_to_b_contract_pct = None
    if a_high_price is not None and b_contract_price is not None and a_high_price > 0:
        ab_drop_to_b_contract_pct = (1.0 - b_contract_price / a_high_price) * 100.0

    bc_rebound_from_b_index_pct = None
    if c_price is not None and b_index_price is not None and b_index_price > 0:
        bc_rebound_from_b_index_pct = (c_price / b_index_price - 1.0) * 100.0

    bc_rebound_from_b_contract_pct = None
    if c_price is not None and b_contract_price is not None and b_contract_price > 0:
        bc_rebound_from_b_contract_pct = (c_price / b_contract_price - 1.0) * 100.0

    bc_vs_ab_ratio_index = None
    if (
        bc_rebound_from_b_index_pct is not None
        and ab_drop_to_b_index_pct is not None
        and abs(ab_drop_to_b_index_pct) > 1e-12
    ):
        bc_vs_ab_ratio_index = bc_rebound_from_b_index_pct / ab_drop_to_b_index_pct

    bc_vs_ab_ratio_contract = None
    if (
        bc_rebound_from_b_contract_pct is not None
        and ab_drop_to_b_contract_pct is not None
        and abs(ab_drop_to_b_contract_pct) > 1e-12
    ):
        bc_vs_ab_ratio_contract = bc_rebound_from_b_contract_pct / ab_drop_to_b_contract_pct

    return {
        "a_time": a_time,
        "b_time": b_time,
        "c_time": c_time,
        "c_price": c_price,
        "ab_bars": ab_bars,
        "bc_bars": bc_bars,
        "ac_bars": ac_bars,
        "ab_drop_to_b_index_pct": ab_drop_to_b_index_pct,
        "ab_drop_to_b_contract_pct": ab_drop_to_b_contract_pct,
        "bc_rebound_from_b_index_pct": bc_rebound_from_b_index_pct,
        "bc_rebound_from_b_contract_pct": bc_rebound_from_b_contract_pct,
        "bc_vs_ab_ratio_index": bc_vs_ab_ratio_index,
        "bc_vs_ab_ratio_contract": bc_vs_ab_ratio_contract,
    }


def parse_trade(row: Dict[str, Any], data_dir: Path) -> Dict[str, Any]:
    context = row.get("context") or {}
    params = row.get("params") or {}

    pnl_pct_raw = safe_float(row.get("pnl_pct"))
    mfe_raw = safe_float(row.get("mfe_pct"))
    mae_raw = safe_float(row.get("mae_pct"))
    selected_tp_raw = safe_float(
        context.get("selected_tp_pct", params.get("selected_take_profit_pct"))
    )

    pnl_pct_v = pct(pnl_pct_raw)
    selected_tp_v = pct(selected_tp_raw)

    mfe_mae_source = "trade_fields"
    mfe_mae_note = "ok"
    if mfe_raw is not None and mae_raw is not None:
        mfe_v = pct(mfe_raw)
        mae_v = pct(mae_raw)
    else:
        entry_time_raw = row.get("entry_time")
        exit_time_raw = row.get("exit_time")
        entry_price_raw = safe_float(row.get("entry_price"))
        symbol = str(row.get("symbol") or "")
        if entry_time_raw is None or exit_time_raw is None or entry_price_raw is None or not symbol:
            mfe_v = None
            mae_v = None
            mfe_mae_source = "unavailable"
            mfe_mae_note = "missing_trade_fields_for_recompute"
        else:
            try:
                entry_time = int(entry_time_raw)
                exit_time = int(exit_time_raw)
            except (TypeError, ValueError):
                mfe_v = None
                mae_v = None
                mfe_mae_source = "unavailable"
                mfe_mae_note = "invalid_trade_time"
            else:
                mfe_v, mae_v, mfe_mae_note = calc_mfe_mae(
                    data_dir=data_dir,
                    symbol=symbol,
                    entry_time=entry_time,
                    exit_time=exit_time,
                    entry_price=entry_price_raw,
                )
                mfe_mae_source = (
                    "recomputed_from_klines" if mfe_v is not None and mae_v is not None else "unavailable"
                )

    mae_to_loss_ratio: Optional[float] = None
    if mae_v is not None and pnl_pct_v is not None and pnl_pct_v < 0 and abs(pnl_pct_v) > 1e-12:
        mae_to_loss_ratio = abs(mae_v) / abs(pnl_pct_v)

    mfe_to_tp_ratio: Optional[float] = None
    if mfe_v is not None and selected_tp_v is not None and selected_tp_v > 1e-12:
        mfe_to_tp_ratio = mfe_v / selected_tp_v

    abc_geo = _derive_abc_geometry(row=row, context=context, params=params, data_dir=data_dir)

    return {
        "symbol": row.get("symbol"),
        "entry_time": row.get("entry_time") or row.get("entry_time_bj"),
        "exit_time": row.get("exit_time") or row.get("exit_time_bj"),
        "reason": row.get("exit_reason") or row.get("reason"),
        "hold_mins": row.get("hold_minutes"),
        "trigger_name": context.get("trigger_name"),
        "tp_tier": context.get("tp_tier"),
        "pnl_pct": pnl_pct_v,
        "mfe_pct": mfe_v,
        "mae_pct": mae_v,
        "mae_to_loss_ratio": mae_to_loss_ratio,
        "mfe_to_tp_ratio": mfe_to_tp_ratio,
        "selected_tp_pct": selected_tp_v,
        "drop_pct": pct(safe_float(context.get("drop_pct"))),
        "rebound_ratio": pct(safe_float(context.get("rebound_ratio"))),
        "vol_ratio": safe_float(context.get("vol_ratio")),
        "wick_ratio": pct(safe_float(context.get("wick_ratio"))),
        "basis_spike_pct": pct(safe_float(context.get("basis_spike_pct"))),
        "basis_close_pct": pct(safe_float(context.get("basis_close_pct"))),
        "a_high_price": safe_float(context.get("a_high_price")),
        "b_contract_price": safe_float(context.get("b_contract_price")),
        "b_index_price": safe_float(context.get("b_index_price")),
        "entry_price": safe_float(row.get("entry_price")),
        "exit_price": safe_float(row.get("exit_price")),
        **abc_geo,
        "mfe_mae_source": mfe_mae_source,
        "mfe_mae_note": mfe_mae_note,
    }

def is_high_risk(
    t: Dict[str, Any],
    mae_threshold_pct: float,
    mae_to_loss_ratio_threshold: float,
    low_mfe_threshold_pct: float,
) -> bool:
    mae_pct = t.get("mae_pct")
    mfe_pct = t.get("mfe_pct")
    reason = (t.get("reason") or "").upper()

    if mae_pct is not None and mae_pct <= mae_threshold_pct:
        return True

    if t.get("mae_to_loss_ratio") is not None and t["mae_to_loss_ratio"] >= mae_to_loss_ratio_threshold:
        return True

    if reason in {"STOP_LOSS", "TIME_STOP"} and mfe_pct is not None and mfe_pct < low_mfe_threshold_pct:
        return True

    if reason == "STOP_LOSS" and mae_pct is not None and mae_pct <= -5.0:
        return True

    return False


def sort_key(row: Dict[str, Any]) -> tuple:
    ratio = row.get("mae_to_loss_ratio")
    mae = row.get("mae_pct")
    mfe = row.get("mfe_pct")
    return (
        -(ratio if ratio is not None else -1),
        (mae if mae is not None else 9999),
        (mfe if mfe is not None else 9999),
    )


def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    fields = [
        "symbol", "entry_time", "exit_time", "reason", "hold_mins", "trigger_name", "tp_tier",
        "pnl_pct", "mfe_pct", "mae_pct", "mae_to_loss_ratio", "mfe_to_tp_ratio", "selected_tp_pct",
        "drop_pct", "rebound_ratio", "vol_ratio", "wick_ratio", "basis_spike_pct", "basis_close_pct",
        "a_high_price", "b_contract_price", "b_index_price", "entry_price", "exit_price",
        "a_time", "b_time", "c_time", "c_price",
        "ab_bars", "bc_bars", "ac_bars",
        "ab_drop_to_b_index_pct", "ab_drop_to_b_contract_pct",
        "bc_rebound_from_b_index_pct", "bc_rebound_from_b_contract_pct",
        "bc_vs_ab_ratio_index", "bc_vs_ab_ratio_contract",
        "mfe_mae_source", "mfe_mae_note",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fields})


def write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def fmt(x: Optional[float], ndigits: int = 2) -> str:
    return "N/A" if x is None else f"{x:.{ndigits}f}"


def print_top(rows: List[Dict[str, Any]], top_n: int) -> None:
    print("\n===== 极端 MAE 审计 Top 样本 =====")
    for i, r in enumerate(rows[:top_n], 1):
        print(
            f"{i:02d}. {r['symbol']}"
            f" | reason={r.get('reason')}"
            f" | pnl={fmt(r.get('pnl_pct'))}%"
            f" | mfe={fmt(r.get('mfe_pct'))}%"
            f" | mae={fmt(r.get('mae_pct'))}%"
            f" | mae/loss={fmt(r.get('mae_to_loss_ratio'))}"
            f" | trigger={r.get('trigger_name')}"
            f" | tier={r.get('tp_tier')}"
            f" | bc_idx={fmt(r.get('bc_rebound_from_b_index_pct'))}%"
            f" | bc_bars={r.get('bc_bars') if r.get('bc_bars') is not None else 'N/A'}"
            f" | bc/ab={fmt(r.get('bc_vs_ab_ratio_index'))}"
            f" | entry={r.get('entry_time')}"
        )


def build_summary(all_rows: List[Dict[str, Any]], risk_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    reason_counts: Dict[str, int] = {}
    trigger_counts: Dict[str, int] = {}
    tier_counts: Dict[str, int] = {}
    mfe_mae_source_counts: Dict[str, int] = {}
    for r in risk_rows:
        reason = str(r.get("reason") or "UNKNOWN")
        trigger = str(r.get("trigger_name") or "UNKNOWN")
        tier = str(r.get("tp_tier") or "UNKNOWN")
        source = str(r.get("mfe_mae_source") or "UNKNOWN")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        trigger_counts[trigger] = trigger_counts.get(trigger, 0) + 1
        tier_counts[tier] = tier_counts.get(tier, 0) + 1
        mfe_mae_source_counts[source] = mfe_mae_source_counts.get(source, 0) + 1

    worst_mae = min((r["mae_pct"] for r in risk_rows if r.get("mae_pct") is not None), default=None)
    worst_ratio = max((r["mae_to_loss_ratio"] for r in risk_rows if r.get("mae_to_loss_ratio") is not None), default=None)

    return {
        "total_trades": len(all_rows),
        "risk_sample_count": len(risk_rows),
        "reason_counts": reason_counts,
        "trigger_counts": trigger_counts,
        "tp_tier_counts": tier_counts,
        "mfe_mae_source_counts": mfe_mae_source_counts,
        "worst_mae_pct": worst_mae,
        "max_mae_to_loss_ratio": worst_ratio,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Snapback 极端 MAE 审计工具（最小版）")
    ap.add_argument("--run-id", help="回测 RUNID，例如 SNAP_V2.4_30D_P5_0314T1212_ALL")
    ap.add_argument("--trades", help="直接指定 sim_trades.jsonl 文件路径")
    ap.add_argument("--state-dir", default=str(DEFAULT_STATE_DIR), help="状态目录，默认 output/state")
    ap.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR), help="K线数据目录，默认 data/klines_1m")
    ap.add_argument("--mae-threshold-pct", type=float, default=-8.0, help="极深 MAE 阈值，默认 -8.0")
    ap.add_argument("--mae-loss-ratio-threshold", type=float, default=3.0, help="mae/loss 放大倍数阈值，默认 3.0")
    ap.add_argument("--low-mfe-threshold-pct", type=float, default=1.0, help="低 MFE 阈值，默认 1.0")
    ap.add_argument("--top-n", type=int, default=20, help="控制台输出前 N 条，默认 20")
    ap.add_argument("--out-csv", help="导出 csv 路径")
    ap.add_argument("--summary-out", help="导出 summary json 路径")
    args = ap.parse_args()

    state_dir = Path(args.state_dir)
    data_dir = Path(args.data_dir)
    if args.trades:
        trades_path = Path(args.trades)
        run_id = trades_path.stem.replace("sim_trades.", "")
    elif args.run_id:
        trades_path = find_trades_file(args.run_id, state_dir)
        run_id = args.run_id
    else:
        raise SystemExit("必须提供 --run-id 或 --trades")

    all_rows = [parse_trade(r, data_dir=data_dir) for r in read_jsonl(trades_path)]
    risk_rows = [
        r for r in all_rows
        if is_high_risk(
            r,
            mae_threshold_pct=args.mae_threshold_pct,
            mae_to_loss_ratio_threshold=args.mae_loss_ratio_threshold,
            low_mfe_threshold_pct=args.low_mfe_threshold_pct,
        )
    ]
    risk_rows.sort(key=sort_key)

    out_csv = Path(args.out_csv) if args.out_csv else state_dir / f"extreme_mae_audit.{run_id}.csv"
    summary_out = Path(args.summary_out) if args.summary_out else state_dir / f"extreme_mae_audit.{run_id}.summary.json"

    write_csv(out_csv, risk_rows)
    summary = build_summary(all_rows, risk_rows)
    summary["run_id"] = run_id
    summary["trades_path"] = str(trades_path)
    summary["csv_path"] = str(out_csv)
    summary["data_dir"] = str(data_dir)
    write_json(summary_out, summary)

    print(f"已读取交易数: {len(all_rows)}")
    print(f"高风险样本数: {len(risk_rows)}")
    print(f"CSV 输出: {out_csv}")
    print(f"Summary 输出: {summary_out}")
    print(f"最深 MAE: {fmt(summary.get('worst_mae_pct'))}%")
    print(f"最大 mae/loss 倍数: {fmt(summary.get('max_mae_to_loss_ratio'))}")
    print_top(risk_rows, top_n=args.top_n)


if __name__ == "__main__":
    main()
