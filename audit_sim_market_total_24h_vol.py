#!/usr/bin/env python3
import argparse
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

BJ = timezone(timedelta(hours=8))

def bj(ms):
    if ms is None:
        return None
    return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")

def load_jsonl(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append((lineno, json.loads(line)))
            except Exception as e:
                raise RuntimeError(f"JSONL 解析失败: {path} line={lineno} err={e}")
    return rows

def infer_candidate_audit_path(trades_path: Path):
    name = trades_path.name
    if not name.startswith("sim_trades.") or not name.endswith(".jsonl"):
        return None
    run_id = name[len("sim_trades."):-len(".jsonl")]
    return trades_path.parent / f"snapback_candidate_pool_audit.{run_id}.jsonl"

def symbol_in_candidates(audit_row, symbol: str):
    cands = audit_row.get("candidates_sorted_by_drop_pct") or []
    for idx, item in enumerate(cands, 1):
        if str(item.get("symbol", "")).upper() == symbol.upper():
            out = dict(item)
            out["_rank_by_drop_pct_from_list"] = idx
            return out
    return None

def candidate_match_score(trade, audit_row, candidate_item):
    score = 0
    bar_ts = audit_row.get("bar_ts")
    signal_time = trade.get("signal_time")
    c_time = trade.get("c_time")
    entry_time = trade.get("entry_time")

    if candidate_item is not None:
        score += 1000
    if signal_time is not None and bar_ts == signal_time:
        score += 200
    if c_time is not None and bar_ts == c_time:
        score += 160
    if signal_time is not None and bar_ts == signal_time - 60000:
        score += 120
    if c_time is not None and bar_ts == c_time + 60000:
        score += 120
    if entry_time is not None and abs(int(bar_ts) - int(entry_time)) <= 60000:
        score += 20
    if signal_time is not None and bar_ts is not None:
        score -= min(abs(int(bar_ts) - int(signal_time)) // 1000, 180)
    return score

def best_candidate_audit_match(trade, audit_rows):
    symbol = str(trade.get("symbol", "")).upper()
    ranked = []
    for lineno, row in audit_rows:
        cand = symbol_in_candidates(row, symbol)
        if cand is None:
            continue
        score = candidate_match_score(trade, row, cand)
        ranked.append((score, lineno, row, cand))
    if not ranked:
        return None
    ranked.sort(key=lambda x: (-x[0], abs(int(x[2].get("bar_ts", 0)) - int(trade.get("signal_time", 0) or 0)), x[1]))
    return ranked[0]

def parse_args():
    p = argparse.ArgumentParser(description="审计 sim 信号对应的 market_total_24h_vol")
    p.add_argument("--trades", required=True, help="sim_trades.<RUNID>.jsonl 路径")
    p.add_argument("--candidate-audit", default="", help="snapback_candidate_pool_audit.<RUNID>.jsonl 路径；不传则尝试同目录自动推断")
    p.add_argument("--symbols", default="", help="只审计指定 symbol，逗号分隔，例如 SIRENUSDT,ONUSDT")
    p.add_argument("--limit", type=int, default=20, help="最多输出多少笔 trade，默认 20")
    return p.parse_args()

def main():
    args = parse_args()
    trades_path = Path(args.trades)
    if not trades_path.exists():
        raise SystemExit(f"trades 文件不存在: {trades_path}")

    candidate_audit_path = Path(args.candidate_audit) if args.candidate_audit else infer_candidate_audit_path(trades_path)
    if candidate_audit_path is None or not candidate_audit_path.exists():
        raise SystemExit("candidate audit 文件不存在；请显式传 --candidate-audit")

    trades_rows = load_jsonl(trades_path)
    audit_rows = load_jsonl(candidate_audit_path)

    symbol_filter = {x.strip().upper() for x in args.symbols.split(",") if x.strip()}
    selected_trades = []
    for lineno, trade in trades_rows:
        sym = str(trade.get("symbol", "")).upper()
        if symbol_filter and sym not in symbol_filter:
            continue
        selected_trades.append((lineno, trade))

    if not selected_trades:
        raise SystemExit("没有匹配到任何 trade")

    print("=" * 100)
    print("SIM signal 对应 market_total_24h_vol 审计")
    print(f"trades         : {trades_path}")
    print(f"candidate_audit: {candidate_audit_path}")
    print(f"trade_count    : {len(selected_trades)}")
    print("=" * 100)

    count = 0
    for lineno, trade in selected_trades:
        if count >= args.limit:
            break
        match = best_candidate_audit_match(trade, audit_rows)
        print(f"\n[trade line {lineno}] {trade.get('symbol')}")
        print(f"  signal_time : {trade.get('signal_time')} | {bj(trade.get('signal_time'))}")
        print(f"  c_time      : {trade.get('c_time')} | {bj(trade.get('c_time'))}")
        print(f"  entry_time  : {trade.get('entry_time')} | {bj(trade.get('entry_time'))}")
        print(f"  entry_price : {trade.get('entry_price')}")
        print(f"  exit_price  : {trade.get('exit_price')}")
        print(f"  reason      : {trade.get('reason')}")
        if match is None:
            print("  candidate_audit_match : NOT FOUND")
            continue

        score, audit_lineno, audit_row, cand = match
        print(f"  candidate_audit line  : {audit_lineno}")
        print(f"  match_score           : {score}")
        print(f"  audit_bar_ts          : {audit_row.get('bar_ts')} | {audit_row.get('bar_bj')}")
        print(f"  market_total_24h_vol  : {audit_row.get('market_total_24h_vol')}")
        print(f"  market_total_24h_min  : {audit_row.get('market_total_24h_vol_min')}")
        print(f"  candidate_count       : {audit_row.get('candidate_count')}")
        print(f"  rank_by_drop_pct      : {cand.get('_rank_by_drop_pct_from_list')}")
        print(f"  cand.drop_pct         : {cand.get('drop_pct')}")
        print(f"  cand.vol_ratio        : {cand.get('vol_ratio')}")
        print(f"  cand.rebound_ratio    : {cand.get('rebound_ratio')}")
        print(f"  cand.selected_tp_pct  : {cand.get('selected_tp_pct')}")
        print(f"  cand.tp_tier          : {cand.get('tp_tier')}")
        count += 1

if __name__ == "__main__":
    main()
