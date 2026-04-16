import argparse
import json
import logging
import math
import os
import shutil
import sys
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List
import types
from pathlib import Path

import numpy as np
import pandas as pd


class NumpyEncoder(json.JSONEncoder):
    """处理 Numpy/Pandas 数据类型的 JSON 序列化器"""

    def default(self, obj):
        if isinstance(obj, (np.integer, np.floating, np.bool_)):
            return obj.item()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# 🧠 策略大脑将根据命令行参数动态导入，实现引擎复用

from core.analysis.analyzer import PerformanceAnalyzer  # noqa: E402
from core.analysis.visualizer import StrategyVisualizerMatplotlib  # noqa: E402
from core.config_loader import StrategyConfig  # noqa: E402
from core.engine.broker import Order, VirtualBroker  # noqa: E402
from core.engine.data_feeder import CrossSectionalFeeder  # noqa: E402

BJ_TZ = timezone(timedelta(hours=8))
EQUITY_INITIAL = 100.0
DEFAULT_FEE_SIDE = 0.0005


def setup_logging(log_file: str):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def _safe_float(v, default=None):
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


def _extract_exit_time_ms(trade: Dict[str, Any]) -> int:
    for key in ("exit_time", "entry_time", "signal_time"):
        v = trade.get(key)
        if isinstance(v, (int, float)):
            return int(v)
    return 0


def _extract_symbol(trade: Dict[str, Any]) -> str:
    v = trade.get("symbol")
    return str(v) if v is not None else ""


def _prepare_trade_rows(trade_history: List[Dict[str, Any]], fee_side: float) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    fee_frac = fee_side * 2.0
    for t in trade_history:
        gross_pct = _safe_float(t.get("pnl_pct"), None)
        if gross_pct is None:
            continue
        exit_ms = _extract_exit_time_ms(t)
        rows.append(
            {
                "symbol": _extract_symbol(t),
                "exit_time_ms": exit_ms,
                "gross_pct": float(gross_pct),
                "net_pct": float(gross_pct) - fee_frac,
            }
        )
    rows.sort(key=lambda x: (x["exit_time_ms"], x["symbol"]))
    return rows


def _build_equity_curves(rows: List[Dict[str, Any]], initial_equity: float) -> Dict[str, List[float]]:
    simple_gross = [initial_equity]
    simple_net = [initial_equity]
    compound_gross = [initial_equity]
    compound_net = [initial_equity]
    for row in rows:
        gp = row["gross_pct"]
        npct = row["net_pct"]
        simple_gross.append(simple_gross[-1] + initial_equity * gp)
        simple_net.append(simple_net[-1] + initial_equity * npct)
        compound_gross.append(compound_gross[-1] * max(0.0, 1.0 + gp))
        compound_net.append(compound_net[-1] * max(0.0, 1.0 + npct))
    return {
        "simple_gross": simple_gross,
        "simple_net": simple_net,
        "compound_gross": compound_gross,
        "compound_net": compound_net,
    }


def _calc_max_drawdown(curve: List[float], times_ms: List[int]) -> Dict[str, Any]:
    if not curve:
        return {
            "days": 0.0,
            "trades": 0,
            "amount": 0.0,
            "pct": 0.0,
            "peak_index": 0,
            "trough_index": 0,
        }
    peak_val = curve[0]
    peak_idx = 0
    best = {
        "days": 0.0,
        "trades": 0,
        "amount": 0.0,
        "pct": 0.0,
        "peak_index": 0,
        "trough_index": 0,
    }
    for i, val in enumerate(curve):
        if val > peak_val:
            peak_val = val
            peak_idx = i
        draw_amount = peak_val - val
        draw_pct = (draw_amount / peak_val) if peak_val > 0 else 0.0
        if draw_pct > best["pct"]:
            peak_time = times_ms[peak_idx] if peak_idx < len(times_ms) else 0
            trough_time = times_ms[i] if i < len(times_ms) else peak_time
            days = max(0.0, (trough_time - peak_time) / 1000.0 / 86400.0)
            best = {
                "days": round(days, 6),
                "trades": max(0, i - peak_idx),
                "amount": round(draw_amount, 12),
                "pct": round(draw_pct * 100.0, 12),
                "peak_index": peak_idx,
                "trough_index": i,
            }
    return best


def _build_monthly_stats(rows: List[Dict[str, Any]], initial_equity: float) -> List[Dict[str, Any]]:
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
                "gross_pnl_pct_sum": 0.0,
                "net_pnl_pct_sum": 0.0,
                "gross_pnl_amount_simple_100": 0.0,
                "net_pnl_amount_simple_100": 0.0,
            },
        )
        item["trade_count"] += 1
        if row["gross_pct"] > 0:
            item["win_count"] += 1
        elif row["gross_pct"] < 0:
            item["loss_count"] += 1
        else:
            item["flat_count"] += 1
        item["gross_pnl_pct_sum"] += row["gross_pct"]
        item["net_pnl_pct_sum"] += row["net_pct"]
        item["gross_pnl_amount_simple_100"] += initial_equity * row["gross_pct"]
        item["net_pnl_amount_simple_100"] += initial_equity * row["net_pct"]
    out = []
    for key in sorted(monthly.keys()):
        item = monthly[key]
        out.append(
            {
                "month": item["month"],
                "trade_count": item["trade_count"],
                "win_count": item["win_count"],
                "loss_count": item["loss_count"],
                "flat_count": item["flat_count"],
                "net_pnl": round(item["net_pnl_amount_simple_100"], 12),
                "gross_pnl_pct_sum": round(item["gross_pnl_pct_sum"], 12),
                "net_pnl_pct_sum": round(item["net_pnl_pct_sum"], 12),
                "gross_pnl_amount_simple_100": round(item["gross_pnl_amount_simple_100"], 12),
                "net_pnl_amount_simple_100": round(item["net_pnl_amount_simple_100"], 12),
            }
        )
    return out




def _build_candidate_audit_run_id(run_id: str) -> str:
    return str(run_id or '').strip()


def _candidate_audit_path(base_dir: str, run_id: str) -> str:
    rid = _build_candidate_audit_run_id(run_id)
    if rid:
        return os.path.join(base_dir, f"snapback_candidate_pool_audit.{rid}.jsonl")
    return os.path.join(base_dir, 'snapback_candidate_pool_audit.jsonl')


def _patch_candidate_pool_audit_writer(strategy: Any, audit_path: str) -> None:
    target_path = Path(audit_path)

    def _patched_append_candidate_pool_audit(self, current_time_ms: int, candidates: List[Dict[str, Any]], *, market_total_24h_vol: float) -> None:
        target_path.parent.mkdir(parents=True, exist_ok=True)

        def _to_jsonable(value: Any) -> Any:
            if isinstance(value, dict):
                return {str(k): _to_jsonable(v) for k, v in value.items()}
            if isinstance(value, (list, tuple)):
                return [_to_jsonable(v) for v in value]
            if isinstance(value, pd.Timestamp):
                return int(value.value // 10**6)
            if value is None:
                return None
            if pd.isna(value):
                return None
            if hasattr(value, 'item'):
                try:
                    return value.item()
                except Exception:
                    pass
            return value

        sorted_candidates = sorted(candidates, key=lambda x: x['drop_pct'], reverse=True)
        payload_candidates: List[Dict[str, Any]] = []
        for rank, candidate in enumerate(sorted_candidates, start=1):
            item = _to_jsonable(candidate)
            item['rank_by_drop_pct'] = rank
            payload_candidates.append(item)

        bar_bj = (pd.to_datetime(current_time_ms, unit='ms') + pd.Timedelta(hours=8)).strftime('%Y-%m-%d %H:%M:%S')
        payload = {
            'bar_ts': int(current_time_ms),
            'bar_bj': bar_bj,
            'market_total_24h_vol': float(market_total_24h_vol),
            'market_total_24h_vol_min': float(getattr(self, 'market_total_24h_vol_min', 0.0)),
            'candidate_count': len(payload_candidates),
            'candidates_sorted_by_drop_pct': payload_candidates,
        }

        with target_path.open('a', encoding='utf-8') as f:
            f.write(json.dumps(payload, ensure_ascii=False) + '\n')

    strategy._append_candidate_pool_audit = types.MethodType(_patched_append_candidate_pool_audit, strategy)

def _apply_spring_logging_hooks(strategy: Any, broker: VirtualBroker) -> None:
    original_on_kline_close = broker.on_kline_close

    def _patched_on_kline_close(current_time_ms: int, cross_section) -> None:
        prev_trade_count = len(broker.trade_history)
        prev_disable_level = logging.root.manager.disable
        logging.disable(logging.CRITICAL)
        try:
            original_on_kline_close(current_time_ms, cross_section)
        finally:
            logging.disable(prev_disable_level)
        new_trades = broker.trade_history[prev_trade_count:]
        for trade in new_trades:
            if "signal_time" in trade and trade["signal_time"]:
                trade["signal_time_bj"] = (
                    pd.to_datetime(trade["signal_time"], unit="ms") + pd.Timedelta(hours=8)
                ).strftime("%Y-%m-%d %H:%M")
            if "entry_time" in trade and trade["entry_time"]:
                trade["entry_time_bj"] = (
                    pd.to_datetime(trade["entry_time"], unit="ms") + pd.Timedelta(hours=8)
                ).strftime("%Y-%m-%d %H:%M")
            if "exit_time" in trade and trade["exit_time"]:
                trade["exit_time_bj"] = (
                    pd.to_datetime(trade["exit_time"], unit="ms") + pd.Timedelta(hours=8)
                ).strftime("%Y-%m-%d %H:%M")
            logging.info(strategy.build_exit_log(trade))

    broker.on_kline_close = _patched_on_kline_close


def build_extended_summary_metrics(trade_history: List[Dict[str, Any]], fee_side: float, initial_equity: float = EQUITY_INITIAL) -> Dict[str, Any]:
    rows = _prepare_trade_rows(trade_history, fee_side)
    times_ms = [rows[0]["exit_time_ms"] if rows else 0] + [r["exit_time_ms"] for r in rows]
    curves = _build_equity_curves(rows, initial_equity)
    simple_gross_sum_pct = sum(r["gross_pct"] for r in rows)
    simple_net_sum_pct = sum(r["net_pct"] for r in rows)
    compound_gross_pct = ((curves["compound_gross"][-1] / initial_equity) - 1.0) if curves["compound_gross"] else 0.0
    compound_net_pct = ((curves["compound_net"][-1] / initial_equity) - 1.0) if curves["compound_net"] else 0.0
    return {
        "fee_side": fee_side,
        "pnl_pct_sum": round(simple_gross_sum_pct, 12),
        "pnl_pct_sum_net_fee": round(simple_net_sum_pct, 12),
        "compound_return_pct": round(compound_gross_pct, 12),
        "compound_return_pct_net_fee": round(compound_net_pct, 12),
        "max_drawdown": {
            "simple_gross": _calc_max_drawdown(curves["simple_gross"], times_ms),
            "simple_net": _calc_max_drawdown(curves["simple_net"], times_ms),
            "compound_gross": _calc_max_drawdown(curves["compound_gross"], times_ms),
            "compound_net": _calc_max_drawdown(curves["compound_net"], times_ms),
        },
        "monthly_stats": _build_monthly_stats(rows, initial_equity),
    }



def _spring_reason_short(reason: Any) -> str:
    value = str(reason or "UNKNOWN").upper()
    if value == "TAKE_PROFIT":
        return "TP"
    if value == "STOP_LOSS":
        return "SL"
    if value == "TIME_STOP":
        return "TS"
    return value


def _spring_bj_from_ms(ts_ms: Any, fmt: str = "%Y-%m-%d %H:%M") -> str:
    try:
        return datetime.fromtimestamp(int(ts_ms) / 1000.0, tz=timezone.utc).astimezone(BJ_TZ).strftime(fmt)
    except Exception:
        return "NA"


def _spring_price_text(value: Any) -> str:
    fv = _safe_float(value, None)
    if fv is None:
        return "NA"
    afv = abs(fv)
    if afv >= 100:
        s = f"{fv:.2f}"
    elif afv >= 1:
        s = f"{fv:.4f}"
    elif afv >= 0.01:
        s = f"{fv:.6f}"
    else:
        s = f"{fv:.8f}"
    return s.rstrip("0").rstrip(".") if "." in s else s


def _spring_pct_text(value: Any) -> str:
    fv = _safe_float(value, None)
    if fv is None:
        return "NA"
    return f"{fv * 100.0:.2f}%"


def _spring_compact_volume(value: Any) -> str:
    fv = _safe_float(value, None)
    if fv is None:
        return "NA"
    if abs(fv) >= 1_000_000_000:
        return f"{fv / 1_000_000_000:.2f}B"
    if abs(fv) >= 1_000_000:
        return f"{fv / 1_000_000:.1f}M"
    if abs(fv) >= 1_000:
        return f"{fv / 1_000:.1f}K"
    return f"{fv:.0f}"


def _spring_symbol_df(feeder_df: pd.DataFrame, symbol: str) -> pd.DataFrame:
    if not isinstance(feeder_df, pd.DataFrame) or feeder_df.empty:
        return pd.DataFrame()
    if isinstance(feeder_df.index, pd.MultiIndex):
        if "symbol" in feeder_df.index.names:
            try:
                out = feeder_df.xs(symbol, level="symbol").copy()
            except Exception:
                return pd.DataFrame()
        else:
            try:
                out = feeder_df.xs(symbol, level=-1).copy()
            except Exception:
                return pd.DataFrame()
    else:
        out = feeder_df.copy()
    try:
        out.index = pd.Index([int(x) for x in out.index])
    except Exception:
        return pd.DataFrame()
    return out.sort_index()


def _spring_row_at_or_before(df: pd.DataFrame, ts_ms: int) -> Optional[pd.Series]:
    if df.empty:
        return None
    idx = df.index
    try:
        pos = idx.searchsorted(int(ts_ms), side="right") - 1
    except Exception:
        return None
    if pos < 0 or pos >= len(df):
        return None
    return df.iloc[int(pos)]


def _spring_candle_window(feeder_df: pd.DataFrame, trade: Dict[str, Any]) -> pd.DataFrame:
    symbol = str(trade.get("symbol") or "")
    context = dict(trade.get("context") or {})
    df = _spring_symbol_df(feeder_df, symbol)
    if df.empty:
        return df
    c_time = int(context.get("c_time_ms") or trade.get("signal_time") or trade.get("entry_time"))
    pattern_bars = int(context.get("pattern_window_bars") or 60)
    s_time = c_time - max(0, pattern_bars - 1) * 60_000
    exit_time = int(trade.get("exit_time") or trade.get("entry_time") or c_time)
    start_ms = s_time - 60 * 60_000
    end_ms = exit_time + 15 * 60_000
    return df[(df.index >= start_ms) & (df.index <= end_ms)].copy()


def _spring_marker_points(trade: Dict[str, Any], window_df: pd.DataFrame) -> List[Dict[str, Any]]:
    context = dict(trade.get("context") or {})
    c_time = int(context.get("c_time_ms") or trade.get("signal_time") or trade.get("entry_time"))
    pattern_bars = int(context.get("pattern_window_bars") or 60)
    s_time = c_time - max(0, pattern_bars - 1) * 60_000
    s_row = _spring_row_at_or_before(window_df, s_time)
    s_price = _safe_float(s_row.get("close") if s_row is not None else None, None)
    return [
        {"label": "S", "time": s_time, "price": s_price},
        {"label": "A", "time": int(context.get("a_time_ms")), "price": _safe_float(context.get("a_close"), None)},
        {"label": "B", "time": int(context.get("b_time_ms")), "price": _safe_float(context.get("b_close"), None)},
        {"label": "C", "time": int(context.get("c_time_ms")), "price": _safe_float(context.get("c_close"), None)},
        {"label": "E", "time": int(trade.get("exit_time")), "price": _safe_float(trade.get("exit_price"), None)},
    ]


def _plot_spring_trade_kline_mpl(trade: Dict[str, Any], feeder_df: pd.DataFrame, output_dir: str) -> None:
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle

    symbol = str(trade.get("symbol") or "UNKNOWN")
    context = dict(trade.get("context") or {})
    window_df = _spring_candle_window(feeder_df, trade)
    if window_df.empty:
        logging.warning(f"Spring复盘图跳过：{symbol} 没有可用K线窗口")
        return

    required_cols = {"open", "high", "low", "close"}
    if not required_cols.issubset(set(window_df.columns)):
        logging.warning(f"Spring复盘图跳过：{symbol} 缺少OHLC字段")
        return

    vol_col = "quote_asset_volume" if "quote_asset_volume" in window_df.columns else ("volume" if "volume" in window_df.columns else None)
    xs = np.arange(len(window_df))
    ts_to_x = {int(ts): i for i, ts in enumerate(window_df.index.tolist())}

    fig, (ax_price, ax_vol) = plt.subplots(
        2,
        1,
        figsize=(12, 8),
        sharex=True,
        gridspec_kw={"height_ratios": [3.2, 1.0]},
    )

    width = 0.6
    for i, (_, row) in enumerate(window_df.iterrows()):
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])
        color = "#15803d" if c >= o else "#dc2626"
        ax_price.vlines(i, l, h, color=color, linewidth=0.8, alpha=0.85)
        body_low = min(o, c)
        body_h = max(abs(c - o), max(abs(h - l) * 0.015, 1e-12))
        ax_price.add_patch(Rectangle((i - width / 2.0, body_low), width, body_h, facecolor=color, edgecolor=color, linewidth=0.6, alpha=0.85))
        if vol_col:
            ax_vol.bar(i, float(row[vol_col]), width=0.75, color="#6b7280", alpha=0.85)

    markers = _spring_marker_points(trade, window_df)
    price_span = max(float(window_df["high"].max()) - float(window_df["low"].min()), 1e-12)
    for point in markers:
        ts = int(point["time"])
        if ts not in ts_to_x:
            continue
        price = _safe_float(point.get("price"), None)
        if price is None:
            continue
        x = ts_to_x[ts]
        ax_price.scatter([x], [price], s=30, zorder=5)
        ax_price.text(x, price + price_span * 0.025, point["label"], fontsize=9, fontweight="bold", ha="center", va="bottom")

    pnl = _safe_float(trade.get("pnl_pct"), 0.0) or 0.0
    hold_mins = 0
    entry_time = _safe_float(trade.get("entry_time"), None)
    exit_time = _safe_float(trade.get("exit_time"), None)
    if entry_time is not None and exit_time is not None and exit_time >= entry_time:
        hold_mins = int(round((exit_time - entry_time) / 60000.0))
    reason = _spring_reason_short(trade.get("reason"))
    signal_time_bj = str(trade.get("signal_time_bj") or _spring_bj_from_ms(trade.get("signal_time")))
    ax_price.set_title(f"{signal_time_bj} | {symbol} | PnL: {pnl * 100.0:.2f}% | {reason}({hold_mins}m)", fontsize=14, fontweight="bold")
    ax_price.set_ylabel("Price")
    ax_vol.set_ylabel("Quote Vol" if vol_col == "quote_asset_volume" else "Volume")
    ax_price.grid(True, linestyle="--", alpha=0.35)
    ax_vol.grid(True, linestyle="--", alpha=0.25)
    ax_price.tick_params(axis="x", labelbottom=False)
    ax_vol.tick_params(axis="x", labelbottom=False)
    ax_vol.set_xticklabels([])

    def _pt(label: str) -> Dict[str, Any]:
        return next((p for p in markers if p["label"] == label), {})

    s = _pt("S")
    a = _pt("A")
    b = _pt("B")
    c = _pt("C")
    e = _pt("E")
    line1 = (
        f"S {_spring_bj_from_ms(s.get('time'), '%H:%M')} @ {_spring_price_text(s.get('price'))} | "
        f"A {_spring_bj_from_ms(a.get('time'), '%H:%M')} @ {_spring_price_text(a.get('price'))} | "
        f"B {_spring_bj_from_ms(b.get('time'), '%H:%M')} @ {_spring_price_text(b.get('price'))} | "
        f"C {_spring_bj_from_ms(c.get('time'), '%H:%M')} @ {_spring_price_text(c.get('price'))} | "
        f"E {_spring_bj_from_ms(e.get('time'), '%H:%M')} @ {_spring_price_text(e.get('price'))}"
    )
    line2 = (
        f"abBars {int(context.get('ab_bars', 0))} | bcBars {int(context.get('bc_bars', 0))} | "
        f"bc/ab {_safe_float(context.get('bc_over_ab_bars'), 0.0):.2f} | "
        f"abChg {_spring_pct_text(context.get('ab_chg_pct'))} | "
        f"rebound {_spring_pct_text(context.get('rebound_ratio'))} | "
        f"volR {_safe_float(context.get('vol_ratio'), 0.0):.2f}"
    )
    line3 = (
        f"A close/high {_spring_price_text(context.get('a_close'))}/{_spring_price_text(context.get('a_high'))} | "
        f"B close/low {_spring_price_text(context.get('b_close'))}/{_spring_price_text(context.get('b_low'))} | "
        f"C close {_spring_price_text(context.get('c_close'))}"
    )
    line4 = (
        f"24hChg {_spring_pct_text(context.get('chg_24h'))} | "
        f"24hVol {_spring_compact_volume(context.get('vol_24h'))} | "
        f"score_order {int(context.get('score_order', 0))} | score {int(context.get('score', 0))}"
    )
    fig.text(0.5, 0.075, line1, ha="center", va="center", fontsize=10, family="monospace")
    fig.text(0.5, 0.050, line2, ha="center", va="center", fontsize=10, family="monospace")
    fig.text(0.5, 0.025, line3, ha="center", va="center", fontsize=10, family="monospace")
    fig.text(0.5, 0.005, line4, ha="center", va="center", fontsize=10, family="monospace")
    plt.tight_layout(rect=[0.03, 0.10, 0.98, 0.94])

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    sig_bj = _spring_bj_from_ms(trade.get("signal_time"), "%Y%m%d_%H%M")
    filename = f"SPRING_{sig_bj}_{symbol}_{reason}.png"
    fig.savefig(out_path / filename, dpi=150)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="结构策略回测引擎")
    parser.add_argument(
        "--start", required=True, help="ISO格式开始时间，如 2025-04-18T00:00:00+00:00"
    )
    parser.add_argument(
        "--end", required=True, help="ISO格式结束时间，如 2026-03-03T00:00:00+00:00"
    )
    parser.add_argument("--config", default="config.json", help="策略配置文件路径")
    parser.add_argument("--out-dir", default="state", help="回测结果输出基础目录")
    parser.add_argument(
        "--run-id", default="default", help="运行实例ID，用于文件命名隔离"
    )
    parser.add_argument(
        "--kline-window", type=int, default=800, help="复盘图表展示的1分钟K线总数量"
    )
    parser.add_argument(
        "--strategy",
        choices=["spring-sabc", "snapback"],
        default="snapback",
        help="选择要运行的策略大脑 (默认: snapback)",
    )
    parser.add_argument("--audit-start-bj", default="", help="取证开始时间，北京时间，格式: YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--audit-end-bj", default="", help="取证结束时间，北京时间，格式: YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--audit-symbols", default="", help="取证品种，逗号分隔，例如 RIVERUSDT,VVVUSDT")
    parser.add_argument("--audit-history-window-bars", type=int, default=120, help="取证输出的历史bars数量")
    parser.add_argument("--audit-out-dir", default="", help="取证输出目录，默认跟随 --out-dir")
    args = parser.parse_args()

    audit_symbols = {
        s.strip().upper()
        for s in str(args.audit_symbols or "").split(",")
        if s.strip()
    }
    audit_enabled = bool(args.audit_start_bj and args.audit_end_bj and audit_symbols)
    audit_start_ms = None
    audit_end_ms = None
    audit_out_path = None
    if audit_enabled:
        try:
            audit_start_dt = datetime.strptime(args.audit_start_bj, "%Y-%m-%d %H:%M:%S").replace(tzinfo=BJ_TZ)
            audit_end_dt = datetime.strptime(args.audit_end_bj, "%Y-%m-%d %H:%M:%S").replace(tzinfo=BJ_TZ)
        except ValueError as e:
            raise SystemExit(f"取证时间解析失败，请使用北京时间格式 YYYY-MM-DD HH:MM:SS: {e}")
        audit_start_ms = int(audit_start_dt.astimezone(timezone.utc).timestamp() * 1000)
        audit_end_ms = int(audit_end_dt.astimezone(timezone.utc).timestamp() * 1000)
        if audit_end_ms < audit_start_ms:
            raise SystemExit("取证时间窗口非法: audit_end_bj 早于 audit_start_bj")

    def _bj_from_ms(ts_ms: int | None) -> str | None:
        if ts_ms is None:
            return None
        return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ_TZ).strftime("%Y-%m-%d %H:%M:%S")

    def _json_safe(value: Any) -> Any:
        if isinstance(value, (np.integer, np.floating, np.bool_)):
            return value.item()
        if isinstance(value, np.ndarray):
            return value.tolist()
        if isinstance(value, pd.Timestamp):
            if pd.isna(value):
                return None
            if value.tzinfo is None:
                value = value.tz_localize(timezone.utc)
            return value.isoformat()
        try:
            if pd.isna(value):
                return None
        except Exception:
            pass
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, dict):
            return {str(k): _json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [_json_safe(v) for v in value]
        return value

    def _series_snapshot(row: Any) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        if row is None:
            return out
        try:
            items = row.items()
        except Exception:
            return out
        for k, v in items:
            out[str(k)] = _json_safe(v)
        return out

    def _history_records(df: Any, bars: int) -> List[Dict[str, Any]]:
        if df is None:
            return []
        try:
            if df.empty:
                return []
        except Exception:
            return []
        hist = df.tail(max(1, int(bars)))
        out: List[Dict[str, Any]] = []
        for open_time_ms, row in hist.iterrows():
            ts_ms = int(open_time_ms)
            rec = {"open_time_ms": ts_ms, "open_time_bj": _bj_from_ms(ts_ms)}
            rec.update(_series_snapshot(row))
            out.append(rec)
        return out

    # 初始化目录和日志
    out_dir = os.path.join(PROJECT_ROOT, args.out_dir)
    os.makedirs(out_dir, exist_ok=True)
    if audit_enabled:
        audit_dir = os.path.join(PROJECT_ROOT, args.audit_out_dir) if str(args.audit_out_dir or "").strip() else out_dir
        os.makedirs(audit_dir, exist_ok=True)
        audit_out_path = os.path.join(audit_dir, f"sim_forensic.{args.run_id}.jsonl")
        if os.path.exists(audit_out_path):
            os.remove(audit_out_path)
    log_file = os.path.join(PROJECT_ROOT, "output", "logs", f"sim.{args.run_id}.log")
    setup_logging(log_file)

    logging.info("=" * 60)
    logging.info(f"🚀 启动 {args.strategy.upper()} 策略仿真引擎 (RUNID: {args.run_id})")
    logging.info("=" * 60)

    # 1. 加载配置
    config_path = os.path.join(os.path.dirname(__file__), args.config)
    try:
        config = StrategyConfig.load(config_path)

        # --- 逻辑健壮性防线：启动前强制核对现场参数 ---
        print("\n" + "=" * 60)
        print(f"🚨 [逻辑校验] 正在从以下路径加载配置: {config_path}")
        print(f"🚨 [内存参数] 实际读入的 Key 列表: {list(config.keys())}")
        print("=" * 60 + "\n")
    except Exception as e:
        logging.error(f"配置加载失败: {e}")
        sys.exit(1)

    # 2. 解析时间
    try:
        start_dt = datetime.fromisoformat(args.start)
        end_dt = datetime.fromisoformat(args.end)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
    except ValueError as e:
        logging.error(f"时间格式解析失败，请确保使用严格的 ISO8601 格式: {e}")
        sys.exit(1)

    # 3. 初始化基础设施
    data_dir = os.path.join(PROJECT_ROOT, "data", "klines_1m")
    try:
        if args.strategy == "snapback":
            feeder_ndays_lowest = max(
                1,
                math.ceil(
                    config["runtime"]["max_history_window_mins"] / (24 * 60)
                ),
            )
        elif args.strategy == "spring-sabc":
            feeder_ndays_lowest = max(
                1,
                math.ceil(
                    config["runtime"]["max_history_window_mins"] / (24 * 60)
                ),
            )
        else:
            raise KeyError(f"【铁律违背】不支持的策略类型: {args.strategy!r}")

        feeder = CrossSectionalFeeder(
            config=config,
            data_dir=data_dir,
            start_time_ms=start_ms,
            end_time_ms=end_ms,
            ndays_lowest=feeder_ndays_lowest,
        )
        timestamps = feeder.get_timestamps()
        logging.info(
            f"数据加载完毕，时间范围: {start_dt} 至 {end_dt}，共 {len(timestamps)} 根K线"
        )
    except Exception as e:
        logging.error(f"数据源初始化失败: {e}")
        sys.exit(1)

    broker = VirtualBroker(config=config)

    # 🧠 动态挂载策略大脑
    if args.strategy == "snapback":
        from strategies.snapback.logic import WashoutSnapbackStrategy

        strategy = WashoutSnapbackStrategy(config=config)
        _patch_candidate_pool_audit_writer(
            strategy,
            _candidate_audit_path(out_dir, args.run_id),
        )
    elif args.strategy == "spring-sabc":
        from strategies.spring.logic import SpringSABCStrategy

        strategy = SpringSABCStrategy(config=config)
        _apply_spring_logging_hooks(strategy, broker)
    else:
        logging.error(f"❌ 不支持的策略类型: {args.strategy}")
        sys.exit(1)

    signals_history = []

    # ==========================================
    # 🚀 [基因改造] 预计算：拆解全表为极速字典
    # ==========================================
    logging.info("⚙️ 正在将多重索引大表预先拆解为 O(1) 寻址字典，请稍候...")
    df_dict = {
        sym: df.reset_index(level="symbol", drop=True).sort_index()
        for sym, df in feeder.df.groupby(level="symbol")
    }
    logging.info(
        f"✅ 缓存字典建立完毕，共收录 {len(df_dict)} 个交易标的，开始极速步进！"
    )
    # ==========================================

    # 4. 时间驱动循环
    logging.info("引擎点火，开始时间步进...")
    for i, ts in enumerate(timestamps):
        cross_section = feeder.get_cross_section(ts)

        # 4.1 驱动撮合引擎：snapback 保持原有顺序；spring-sabc 需要先在 CB 发单，再在同一 CB 撮合。
        if args.strategy != "spring-sabc":
            broker.on_kline_close(ts, cross_section)

        # 4.2 获取当前活动标的，传给大脑做环境感知
        active_symbols = set(broker.active_orders.keys()) | set(
            broker.active_positions.keys()
        )

        # 4.3 大脑思考，输出信号快照 (若无信号则返回 None)
        signal = strategy.on_kline_close(
            ts, cross_section, active_symbols, full_df=df_dict
        )

        structure_audit_map = {}
        if audit_enabled and hasattr(strategy, "audit_symbols_at_kline_close"):
            structure_audit_map = strategy.audit_symbols_at_kline_close(
                ts,
                cross_section,
                active_symbols,
                full_df=df_dict,
                target_symbols=audit_symbols,
            )

        if audit_enabled and audit_start_ms is not None and audit_end_ms is not None and audit_out_path:
            if audit_start_ms <= int(ts) <= audit_end_ms:
                signal_symbol = str((signal or {}).get("symbol") or "").upper().strip() if signal else ""
                signal_digest = None
                if signal:
                    signal_digest = {
                        "symbol": signal_symbol or None,
                        "signal_time": int(signal["signal_time"]) if signal.get("signal_time") is not None else None,
                        "current_price": _json_safe(signal.get("current_price")),
                        "tp_price": _json_safe(signal.get("tp_price")),
                        "sl_price": _json_safe(signal.get("sl_price")),
                    }
                with open(audit_out_path, "a", encoding="utf-8") as f:
                    for audit_symbol in sorted(audit_symbols):
                        symbol_df = df_dict.get(audit_symbol)
                        row = cross_section.loc[audit_symbol] if audit_symbol in cross_section.index else None
                        forensic_row = {
                            "run_id": args.run_id,
                            "strategy": args.strategy,
                            "bar_ts": int(ts),
                            "bar_bj": _bj_from_ms(int(ts)),
                            "symbol": audit_symbol,
                            "in_cross_section": bool(audit_symbol in cross_section.index),
                            "in_active_symbols": bool(audit_symbol in active_symbols),
                            "selected_signal_symbol": signal_symbol or None,
                            "selected_signal_digest": signal_digest,
                            "cross_snapshot": _series_snapshot(row),
                            "structure_audit": structure_audit_map.get(audit_symbol, {}),
                            "history_bars": _history_records(symbol_df.loc[:ts], args.audit_history_window_bars) if symbol_df is not None else [],
                        }
                        f.write(json.dumps(forensic_row, ensure_ascii=False, cls=NumpyEncoder) + "\n")

        if signal:
            signals_history.append(signal)
            if args.strategy == "spring-sabc":
                logging.info(strategy.build_entry_log(signal))
            # 4.4 回测入口作为"桥梁"，根据信号向撮合引擎发单
            # signal_time 已按策略语义记为 CB（观察/开仓发生时刻）
            signal_time_ms = int(signal["signal_time"])
            order = Order(
                symbol=signal["symbol"],
                create_time_ms=signal_time_ms,
                signal_time_ms=signal_time_ms,
                signal_price=signal["current_price"],
                context=signal.get("context", {}),
            )
            order.tp_price = signal["tp_price"]
            order.sl_price = signal["sl_price"]
            broker.active_orders[signal["symbol"]] = order

        if args.strategy == "spring-sabc":
            broker.on_kline_close(ts, cross_section)

    # 5. 盘后结算与落盘
    trade_history = broker.trade_history
    trades_out = os.path.join(out_dir, f"sim_trades.{args.run_id}.jsonl")
    signals_out = os.path.join(out_dir, f"sim_signals.{args.run_id}.jsonl")

    # 信号快照落盘
    with open(signals_out, "w", encoding="utf-8") as f:
        for s in signals_history:
            f.write(json.dumps(s, cls=NumpyEncoder) + "\n")

    # 成交记录落盘 (并附加北京时间)
    with open(trades_out, "w", encoding="utf-8") as f:
        for t in trade_history:
            if "signal_time" in t and t["signal_time"]:
                t["signal_time_bj"] = (
                    pd.to_datetime(t["signal_time"], unit="ms") + pd.Timedelta(hours=8)
                ).strftime("%Y-%m-%d %H:%M")
            t["entry_time_bj"] = (
                pd.to_datetime(t["entry_time"], unit="ms") + pd.Timedelta(hours=8)
            ).strftime("%Y-%m-%d %H:%M")
            t["exit_time_bj"] = (
                pd.to_datetime(t["exit_time"], unit="ms") + pd.Timedelta(hours=8)
            ).strftime("%Y-%m-%d %H:%M")
            f.write(json.dumps(t, cls=NumpyEncoder) + "\n")

    if not trade_history:
        logging.warning("本次回测无交易产生。")
        sys.exit(0)

    logging.info(
        f"生成业绩报告... 共 {len(trade_history)} 笔交易，发出 {len(signals_history)} 次信号"
    )
    analyzer = PerformanceAnalyzer(
        trade_history=trade_history, config=config, feeder_df=feeder.df
    )
    report = analyzer.generate_report()
    extended = build_extended_summary_metrics(trade_history, fee_side=_extract_fee_side(config), initial_equity=EQUITY_INITIAL)

    summary_out = os.path.join(out_dir, f"sim_summary.{args.run_id}.json")
    with open(summary_out, "w", encoding="utf-8") as f:
        # 核心改进：将本次运行的原始配置 config 完整保留在 summary 开头，确保实验可追溯
        safe_report = {"run_config": config}

        # 合并绩效报告字段 (过滤掉不可序列化的 DataFrame)
        for k, v in report.items():
            if k not in ["trades_df", "benchmark_series"]:
                safe_report[k] = v
        safe_report.update(extended)

        json.dump(safe_report, f, indent=2, ensure_ascii=False, cls=NumpyEncoder)
    # 6. 可视化导出
    viz_dir = os.path.join(out_dir, f"sim_viz_{args.run_id}")
    if os.path.exists(viz_dir):
        shutil.rmtree(viz_dir)
    os.makedirs(viz_dir, exist_ok=True)
    if args.strategy == "spring-sabc":
        for trade in trade_history:
            _plot_spring_trade_kline_mpl(trade=trade, feeder_df=feeder.df, output_dir=viz_dir)
    else:
        visualizer = StrategyVisualizerMatplotlib(output_dir=viz_dir)
        for trade in trade_history:
            visualizer.plot_trade_kline_mpl(
                trade=trade, feeder_df=feeder.df, window_mins_1m=args.kline_window
            )

    logging.info("=" * 60)
    logging.info("回测完成！")
    logging.info(f"信号快照: {signals_out}")
    logging.info(f"交易明细: {trades_out}")
    logging.info(f"业绩摘要: {summary_out}")
    if audit_enabled and audit_out_path:
        logging.info(f"取证现场: {audit_out_path}")
    logging.info(f"高清复盘图目录: {viz_dir}")
    logging.info("=" * 60)


if __name__ == "__main__":
    main()
