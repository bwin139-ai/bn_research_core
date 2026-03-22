#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path

EXPECTED_MD5 = "30819cecc4fd20116f336bb88f494025"

TOOL_CONTENT = r'''#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

UTC = timezone.utc
DEFAULT_INDEX_WEIGHTS = {
    "BTCUSDT": 0.56,
    "ETHUSDT": 0.24,
    "BNBUSDT": 0.12,
    "SOLUSDT": 0.08,
}


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def prepare_rows(trades: List[Dict[str, Any]], fee_side: float) -> List[Dict[str, Any]]:
    fee_frac = fee_side * 2.0
    out: List[Dict[str, Any]] = []
    for row in trades:
        if row.get("pnl_pct") is None:
            continue
        exit_ms = 0
        for key in ("exit_time", "entry_time", "signal_time"):
            v = row.get(key)
            if isinstance(v, (int, float)):
                exit_ms = int(v)
                break
        out.append(
            {
                "exit_time_ms": exit_ms,
                "dt": datetime.fromtimestamp(exit_ms / 1000.0, tz=UTC),
                "gross_pct": float(row["pnl_pct"]),
                "net_pct": float(row["pnl_pct"]) - fee_frac,
            }
        )
    out.sort(key=lambda x: x["exit_time_ms"])
    return out


def build_curves(rows: List[Dict[str, Any]], initial_equity: float):
    sg = [initial_equity]
    sn = [initial_equity]
    cg = [initial_equity]
    cn = [initial_equity]
    for r in rows:
        sg.append(sg[-1] + initial_equity * r["gross_pct"])
        sn.append(sn[-1] + initial_equity * r["net_pct"])
        cg.append(cg[-1] * max(0.0, 1.0 + r["gross_pct"]))
        cn.append(cn[-1] * max(0.0, 1.0 + r["net_pct"]))
    return sg, sn, cg, cn


def calc_max_drawdown(curve, times_ms):
    peak_val = curve[0] if curve else 0.0
    peak_idx = 0
    best = {"days": 0.0, "trades": 0, "amount": 0.0, "pct": 0.0}
    for i, val in enumerate(curve):
        if val > peak_val:
            peak_val = val
            peak_idx = i
        draw_amount = peak_val - val
        draw_pct = (draw_amount / peak_val) if peak_val > 0 else 0.0
        if draw_pct > best["pct"]:
            days = max(0.0, (times_ms[i] - times_ms[peak_idx]) / 1000.0 / 86400.0)
            best = {
                "days": round(days, 6),
                "trades": max(0, i - peak_idx),
                "amount": round(draw_amount, 12),
                "pct": round(draw_pct * 100.0, 12),
            }
    return best


def load_symbol_series(kline_root: Path, symbol: str, months: List[str]) -> pd.DataFrame:
    dfs = []
    sym_dir = kline_root / symbol
    for m in months:
        fp = sym_dir / f"{m}.parquet"
        if fp.exists():
            dfs.append(pd.read_parquet(fp, columns=["open_time_ms", "close"]).rename(columns={"close": symbol}))
    if not dfs:
        return pd.DataFrame(columns=["open_time_ms", symbol])
    return pd.concat(dfs, ignore_index=True).sort_values("open_time_ms").drop_duplicates("open_time_ms", keep="last")


def build_index_series(exit_times_ms: List[int], kline_root: Path, initial_equity: float):
    if not exit_times_ms:
        return []
    months = sorted({datetime.fromtimestamp(ms / 1000.0, tz=UTC).strftime("%Y-%m") for ms in exit_times_ms})
    merged = pd.DataFrame({"open_time_ms": exit_times_ms}).sort_values("open_time_ms")
    for sym in DEFAULT_INDEX_WEIGHTS:
        sdf = load_symbol_series(kline_root, sym, months)
        if sdf.empty:
            return []
        merged = pd.merge_asof(merged, sdf.sort_values("open_time_ms"), on="open_time_ms", direction="backward")
    weighted = []
    for _, row in merged.iterrows():
        try:
            weighted.append(sum(float(row[sym]) * w for sym, w in DEFAULT_INDEX_WEIGHTS.items()))
        except Exception:
            weighted.append(None)
    last = None
    for i, v in enumerate(weighted):
        if v is None:
            weighted[i] = last
        else:
            last = v
    first = next((v for v in weighted if v is not None and v > 0), None)
    if not first:
        return []
    return [initial_equity * (float(v) / first) if v is not None else initial_equity for v in weighted]


def fmt_dd(dd):
    return f"{dd['amount']:.2f} ({dd['pct']:.2f}%) / {dd['days']:.1f}d / {dd['trades']}t"


def plot_curve(out_path, title, times, gross_curve, net_curve, gross_profit_pct, net_profit_pct, gross_dd, net_dd, index_curve, initial_equity, fee_side):
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(18, 8))
    ax.plot(times, gross_curve[1:], label="Equity (gross)")
    ax.plot(times, net_curve[1:], label="Equity (net after fees)")
    if index_curve:
        ax.plot(times, index_curve, "--", label="Crypto index (BTC56/ETH24/BNB12/SOL08)")
    ax.set_xlabel("Exit time")
    ax.set_ylabel("Equity (USDT)")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax.grid(True, alpha=0.3)
    subtitle = f"Gross={gross_profit_pct*100:.4f}% | Net={net_profit_pct*100:.4f}%    MaxDD gross[{fmt_dd(gross_dd)}]    MaxDD net[{fmt_dd(net_dd)}]"
    ax.set_title(f"{title} (initial={initial_equity:.2f} USDT, fee/side={fee_side*100:.4f}%)\n{subtitle}")
    ax.legend(loc="upper left")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", required=True)
    ap.add_argument("--summary", default=None)
    ap.add_argument("--simple-out", required=True)
    ap.add_argument("--compound-out", required=True)
    ap.add_argument("--summary-out", required=True)
    ap.add_argument("--kline-root", default="data/klines_1m")
    ap.add_argument("--initial-equity", type=float, default=100.0)
    ap.add_argument("--fee-side", type=float, default=0.0005)
    args = ap.parse_args()

    summary = {}
    if args.summary and Path(args.summary).exists():
        summary = json.loads(Path(args.summary).read_text(encoding="utf-8"))
    initial_equity = float(summary.get("equity_initial", args.initial_equity))
    fee_side = float(summary.get("fee_side", args.fee_side))

    rows = prepare_rows(load_jsonl(Path(args.trades)), fee_side)
    sg, sn, cg, cn = build_curves(rows, initial_equity)
    times_ms = [rows[0]["exit_time_ms"] if rows else 0] + [r["exit_time_ms"] for r in rows]
    times = [r["dt"] for r in rows]
    gp = sum(r["gross_pct"] for r in rows)
    npct = sum(r["net_pct"] for r in rows)
    cgp = ((cg[-1] / initial_equity) - 1.0) if cg else 0.0
    cnp = ((cn[-1] / initial_equity) - 1.0) if cn else 0.0
    dds = {
        "simple_gross": calc_max_drawdown(sg, times_ms),
        "simple_net": calc_max_drawdown(sn, times_ms),
        "compound_gross": calc_max_drawdown(cg, times_ms),
        "compound_net": calc_max_drawdown(cn, times_ms),
    }
    idx_curve = build_index_series([r["exit_time_ms"] for r in rows], Path(args.kline_root), initial_equity)

    plot_curve(args.simple_out, "Sim equity curve", times, sg, sn, gp, npct, dds["simple_gross"], dds["simple_net"], idx_curve, initial_equity, fee_side)
    plot_curve(args.compound_out, "Sim compound equity curve", times, cg, cn, cgp, cnp, dds["compound_gross"], dds["compound_net"], idx_curve, initial_equity, fee_side)

    Path(args.summary_out).write_text(json.dumps({
        "equity_initial": initial_equity,
        "fee_side": fee_side,
        "pnl_pct_sum": round(gp, 12),
        "pnl_pct_sum_net_fee": round(npct, 12),
        "compound_return_pct": round(cgp, 12),
        "compound_return_pct_net_fee": round(cnp, 12),
        "max_drawdown": dds,
        "simple_png": args.simple_out,
        "compound_png": args.compound_out,
    }, ensure_ascii=False, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
'''


def md5_text(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--target", default="schedule_backtests.py")
    args = ap.parse_args()

    p = Path(args.target)
    orig = p.read_text(encoding="utf-8")
    actual = md5_text(orig)
    if actual != EXPECTED_MD5:
        raise SystemExit(f"fingerprint mismatch for schedule_backtests.py\n  expected md5: {EXPECTED_MD5}\n  actual md5  : {actual}")

    text = orig.replace("\nimport matplotlib.pyplot as plt\n", "\n")
    text = text.replace(
        "    ap.add_argument('--equity-script', default='core/analysis/top1_equity_curve.py')\n",
        "    ap.add_argument('--equity-script', default='core/analysis/sim_equity_curves.py')\n",
        1,
    )

    old = '''    if args.build_equity:
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
'''

    new = '''    if args.build_equity:
        append_line(scheduler_log, f'POST_EQUITY_START runset={runset}')
        print(f'POST_EQUITY_START runset={runset}')
        try:
            simple_png = state_dir / f'sim_curve_simple.{runset}_ALL.png'
            compound_png = state_dir / f'sim_curve_compound.{runset}_ALL.png'
            equity_json = state_dir / f'sim_equity.{runset}_ALL.json'
            cmd = [
                args.python_bin,
                args.equity_script,
                '--trades', str(merged_trades),
                '--summary', str(merged_summary),
                '--simple-out', str(simple_png),
                '--compound-out', str(compound_png),
                '--summary-out', str(equity_json),
                '--kline-root', args.kline_root,
                '--initial-equity', str(args.equity_initial),
                '--fee-side', str(args.equity_fee_side),
            ]
            subprocess.run(cmd, check=True)
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
'''

    if old not in text:
        raise SystemExit("cannot find build_equity block to replace")
    text = text.replace(old, new, 1)

    bak = p.with_suffix(p.suffix + ".bak_4025")
    bak.write_text(orig, encoding="utf-8")
    p.write_text(text, encoding="utf-8")

    tp = Path("core/analysis/sim_equity_curves.py")
    tp.parent.mkdir(parents=True, exist_ok=True)
    tp.write_text(TOOL_CONTENT, encoding="utf-8")

    print(f"patched: {p}")
    print(f"backup : {bak}")
    print(f"created: {tp}")


if __name__ == "__main__":
    main()
