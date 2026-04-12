#!/usr/bin/env python3
import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd


@dataclass
class ReplayResult:
    symbol: str
    bar_ts: int
    bar_bj: str
    signal_time: int
    signal_time_bj: str
    entry_time: int
    entry_time_bj: str
    exit_time: int
    exit_time_bj: str
    entry_price: float
    exit_price: float
    tp_price: float
    sl_price: float
    pnl_pct: float
    reason: str
    rank_drop_pct: int
    rank_vol_ratio: int
    rank_score: int
    combo_rank: int
    weight: float
    candidate_count: int
    drop_pct: float
    vol_ratio: float
    selected_tp_pct: float
    tp_tier: str
    b_index_price: float


def load_jsonl(path: Path) -> List[dict]:
    rows: List[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception as e:
                raise RuntimeError(f"JSON parse failed at line {lineno}: {e}") from e
    return rows


def dedupe_by_bar_ts(rows: List[dict]) -> Tuple[List[dict], List[dict], int, int]:
    seen: Dict[int, dict] = {}
    dup_samples: List[dict] = []
    duplicate_groups = 0
    duplicate_extra_rows = 0
    by_bar: Dict[int, List[dict]] = defaultdict(list)
    for row in rows:
        by_bar[int(row["bar_ts"])].append(row)
    for bar_ts, group in sorted(by_bar.items()):
        if len(group) > 1:
            duplicate_groups += 1
            duplicate_extra_rows += len(group) - 1
            first = group[0]
            identical = all(x == first for x in group[1:])
            dup_samples.append({
                "bar_ts": bar_ts,
                "bar_bj": first.get("bar_bj"),
                "duplicate_rows": len(group),
                "all_rows_identical": identical,
            })
        seen[bar_ts] = group[0]
    unique_rows = [seen[k] for k in sorted(seen.keys())]
    return unique_rows, dup_samples, duplicate_groups, duplicate_extra_rows


def bj_str_from_ms(ms: int) -> str:
    return (pd.to_datetime(ms, unit="ms", utc=True).tz_convert("Asia/Shanghai").strftime("%Y-%m-%d %H:%M:%S"))


def load_config(config_path: Optional[Path]) -> Tuple[Optional[int], Optional[float]]:
    if config_path is None:
        return None, None
    data = json.loads(config_path.read_text(encoding="utf-8"))
    ts_cfg = data["exit_policy"]["time_stop"]
    return int(ts_cfg["max_hold_mins"]), float(ts_cfg["min_profit_pct"])


class KlineStore:
    def __init__(self, root: Path):
        self.root = root
        self.path_cache: Dict[str, List[Path]] = {}
        self.df_cache: Dict[str, pd.DataFrame] = {}

    def _find_paths(self, symbol: str) -> List[Path]:
        symbol_u = symbol.upper()
        if symbol_u in self.path_cache:
            return self.path_cache[symbol_u]

        symbol_dir = self.root / symbol_u
        candidates: List[Path] = []
        if symbol_dir.is_dir():
            for ext in ("parquet", "csv", "feather"):
                candidates.extend(sorted(symbol_dir.glob(f"*.{ext}")))

        if not candidates:
            for ext in ("parquet", "csv", "feather"):
                candidates.extend(sorted(self.root.rglob(f"{symbol_u}.{ext}")))
                candidates.extend(sorted(self.root.rglob(f"{symbol_u.lower()}.{ext}")))

        if not candidates:
            raise FileNotFoundError(f"kline file not found for symbol={symbol_u} under {self.root}")

        self.path_cache[symbol_u] = candidates
        return candidates

    def _normalize_df(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        col_map = {str(c).lower(): c for c in df.columns}
        ts_col = None
        for cand in ["open_time_ms", "open_time", "ts", "timestamp", "time", "t"]:
            if cand in col_map:
                ts_col = col_map[cand]
                break
        if ts_col is not None:
            ts = df[ts_col]
        else:
            if isinstance(df.index, pd.DatetimeIndex):
                ts = pd.Series((df.index.view("int64") // 10**6), index=df.index)
            else:
                ts = pd.Series(df.index, index=df.index)

        ts_num = pd.to_numeric(ts, errors="coerce")
        if ts_num.isna().all():
            ts_dt = pd.to_datetime(ts, errors="coerce", utc=True)
            if ts_dt.isna().all():
                raise RuntimeError(f"cannot determine timestamp column for {symbol}")
            ts_num = (ts_dt.view("int64") // 10**6)

        if ts_num.max() < 10**12:
            ts_num = ts_num * 1000

        needed = {}
        for want in ["open", "high", "low", "close"]:
            if want not in col_map:
                raise RuntimeError(f"missing column {want} in kline file for {symbol}")
            needed[want] = pd.to_numeric(df[col_map[want]], errors="coerce")

        out = pd.DataFrame(needed)
        out.index = pd.Index(ts_num.astype("int64"), name="open_time")
        out = out.sort_index()
        out = out[~out.index.duplicated(keep="first")]
        out = out.dropna(subset=["open", "high", "low", "close"])
        return out

    def get(self, symbol: str) -> pd.DataFrame:
        symbol_u = symbol.upper()
        if symbol_u in self.df_cache:
            return self.df_cache[symbol_u]

        frames: List[pd.DataFrame] = []
        for path in self._find_paths(symbol_u):
            if path.suffix.lower() == ".parquet":
                df = pd.read_parquet(path)
            elif path.suffix.lower() == ".csv":
                df = pd.read_csv(path)
            elif path.suffix.lower() == ".feather":
                df = pd.read_feather(path)
            else:
                raise RuntimeError(f"unsupported file type: {path}")
            frames.append(self._normalize_df(df, symbol_u))

        if not frames:
            raise RuntimeError(f"empty kline set for symbol={symbol_u}")

        norm = pd.concat(frames).sort_index()
        norm = norm[~norm.index.duplicated(keep="first")]
        self.df_cache[symbol_u] = norm
        return norm


def add_ranks(cands: List[dict]) -> List[dict]:
    out = [dict(x) for x in cands]
    by_drop = sorted(out, key=lambda x: (-float(x["drop_pct"]), -float(x["vol_ratio"]), str(x["symbol"])))
    for i, c in enumerate(by_drop, 1):
        c["rank_drop_pct"] = i
    by_vol = sorted(out, key=lambda x: (-float(x["vol_ratio"]), -float(x["drop_pct"]), str(x["symbol"])))
    for i, c in enumerate(by_vol, 1):
        c["rank_vol_ratio"] = i
    out = sorted(
        out,
        key=lambda x: (
            int(x["rank_drop_pct"]) + int(x["rank_vol_ratio"]),
            -float(x["drop_pct"]),
            -float(x["vol_ratio"]),
            str(x["symbol"]),
        ),
    )
    for i, c in enumerate(out, 1):
        c["rank_score"] = int(c["rank_drop_pct"]) + int(c["rank_vol_ratio"])
        c["combo_rank"] = i
    return out


def replay_one_candidate(
    cand: dict,
    bar_ts: int,
    bar_bj: str,
    store: KlineStore,
    max_hold_mins: int,
    time_stop_min_profit_pct: float,
    same_bar_policy: str,
    weight: float,
    candidate_count: int,
) -> ReplayResult:
    symbol = str(cand["symbol"]).upper()
    signal_time = int(bar_ts) + 60_000
    df = store.get(symbol)

    if signal_time not in df.index:
        pos = df.index.searchsorted(signal_time, side="left")
        if pos >= len(df.index):
            raise RuntimeError(f"signal_time not found and no later bar for {symbol} @ {signal_time}")
        entry_time = int(df.index[pos])
    else:
        entry_time = signal_time

    entry_bar = df.loc[entry_time]
    entry_price = float(entry_bar["open"])
    tp_pct = float(cand["selected_tp_pct"])
    tp_price = entry_price * (1.0 + tp_pct)
    sl_price = float(cand["b_index_price"])

    start_pos = df.index.get_loc(entry_time)
    if not isinstance(start_pos, int):
        start_pos = int(start_pos.start)

    for i in range(start_pos, len(df.index)):
        ts = int(df.index[i])
        bar = df.iloc[i]
        high = float(bar["high"])
        low = float(bar["low"])
        close = float(bar["close"])
        elapsed_mins = int((ts - entry_time) // 60_000)

        hit_tp = high >= tp_price
        hit_sl = low <= sl_price

        if hit_tp and hit_sl:
            if same_bar_policy == "tp_first":
                exit_price = tp_price
                reason = "TAKE_PROFIT"
            else:
                exit_price = sl_price
                reason = "STOP_LOSS"
            exit_time = ts
            break
        if hit_sl:
            exit_price = sl_price
            reason = "STOP_LOSS"
            exit_time = ts
            break
        if hit_tp:
            exit_price = tp_price
            reason = "TAKE_PROFIT"
            exit_time = ts
            break

        if elapsed_mins >= max_hold_mins:
            pnl_now = (close / entry_price) - 1.0
            if pnl_now < time_stop_min_profit_pct:
                exit_price = close
                reason = "TIME_STOP"
                exit_time = ts
                break
    else:
        ts = int(df.index[-1])
        close = float(df.iloc[-1]["close"])
        exit_price = close
        reason = "DATA_END"
        exit_time = ts

    pnl_pct = (exit_price / entry_price) - 1.0
    return ReplayResult(
        symbol=symbol,
        bar_ts=int(bar_ts),
        bar_bj=str(bar_bj),
        signal_time=signal_time,
        signal_time_bj=bj_str_from_ms(signal_time),
        entry_time=entry_time,
        entry_time_bj=bj_str_from_ms(entry_time),
        exit_time=exit_time,
        exit_time_bj=bj_str_from_ms(exit_time),
        entry_price=entry_price,
        exit_price=float(exit_price),
        tp_price=float(tp_price),
        sl_price=float(sl_price),
        pnl_pct=float(pnl_pct),
        reason=str(reason),
        rank_drop_pct=int(cand["rank_drop_pct"]),
        rank_vol_ratio=int(cand["rank_vol_ratio"]),
        rank_score=int(cand["rank_score"]),
        combo_rank=int(cand["combo_rank"]),
        weight=float(weight),
        candidate_count=int(candidate_count),
        drop_pct=float(cand["drop_pct"]),
        vol_ratio=float(cand["vol_ratio"]),
        selected_tp_pct=float(cand["selected_tp_pct"]),
        tp_tier=str(cand.get("tp_tier", "")),
        b_index_price=float(cand["b_index_price"]),
    )


def summarize_results(results: List[ReplayResult], top_n: int, duplicate_groups: int, duplicate_extra_rows: int) -> dict:
    leg_counter = Counter(r.reason for r in results)
    round_map: Dict[int, List[ReplayResult]] = defaultdict(list)
    for r in results:
        round_map[r.bar_ts].append(r)

    round_rows = []
    for bar_ts, group in sorted(round_map.items()):
        weighted_pnl = sum(r.weight * r.pnl_pct for r in group)
        round_rows.append({
            "bar_ts": bar_ts,
            "bar_bj": group[0].bar_bj,
            "leg_count": len(group),
            "weighted_round_pnl_pct": weighted_pnl,
            "win_leg_count": sum(1 for r in group if r.pnl_pct > 0),
            "loss_leg_count": sum(1 for r in group if r.pnl_pct < 0),
            "top_symbols": ", ".join(r.symbol for r in sorted(group, key=lambda x: x.combo_rank)),
        })

    round_win_count = sum(1 for x in round_rows if x["weighted_round_pnl_pct"] > 0)
    round_loss_count = sum(1 for x in round_rows if x["weighted_round_pnl_pct"] < 0)
    round_flat_count = sum(1 for x in round_rows if x["weighted_round_pnl_pct"] == 0)

    monthly = defaultdict(lambda: {
        "trade_count": 0,
        "pnl_sum_pct": 0.0,
        "avg_pnl_pct": 0.0,
        "win_rate": 0.0,
        "take_profit": 0,
        "stop_loss": 0,
        "time_stop": 0,
        "data_end": 0,
        "round_count": 0,
        "round_weighted_pnl_sum_pct": 0.0,
    })

    for r in results:
        m = r.bar_bj[:7]
        monthly[m]["trade_count"] += 1
        monthly[m]["pnl_sum_pct"] += r.pnl_pct
        if r.pnl_pct > 0:
            monthly[m].setdefault("win_count", 0)
            monthly[m]["win_count"] += 1
        if r.reason == "TAKE_PROFIT":
            monthly[m]["take_profit"] += 1
        elif r.reason == "STOP_LOSS":
            monthly[m]["stop_loss"] += 1
        elif r.reason == "TIME_STOP":
            monthly[m]["time_stop"] += 1
        elif r.reason == "DATA_END":
            monthly[m]["data_end"] += 1
    for rr in round_rows:
        m = rr["bar_bj"][:7]
        monthly[m]["round_count"] += 1
        monthly[m]["round_weighted_pnl_sum_pct"] += rr["weighted_round_pnl_pct"]
    for m in monthly:
        tc = monthly[m]["trade_count"]
        monthly[m]["avg_pnl_pct"] = monthly[m]["pnl_sum_pct"] / tc if tc else 0.0
        monthly[m]["win_rate"] = monthly[m].get("win_count", 0) / tc if tc else 0.0

    summary = {
        "replay_scope": {
            "candidate_rounds_total": len(round_rows),
            "selected_legs_total": len(results),
            "top_n": top_n,
            "duplicate_groups": duplicate_groups,
            "duplicate_extra_rows": duplicate_extra_rows,
        },
        "leg_summary": {
            "trade_count": len(results),
            "pnl_sum_pct": round(sum(r.pnl_pct for r in results), 6),
            "avg_pnl_pct": round(sum(r.pnl_pct for r in results) / len(results), 6) if results else 0.0,
            "win_count": sum(1 for r in results if r.pnl_pct > 0),
            "loss_count": sum(1 for r in results if r.pnl_pct < 0),
            "flat_count": sum(1 for r in results if r.pnl_pct == 0),
            "win_rate": round(sum(1 for r in results if r.pnl_pct > 0) / len(results), 6) if results else 0.0,
            "reason_counts": dict(leg_counter),
            "reason_rates": {k: round(v / len(results), 6) for k, v in sorted(leg_counter.items())} if results else {},
        },
        "round_summary": {
            "round_count": len(round_rows),
            "weighted_pnl_sum_pct": round(sum(x["weighted_round_pnl_pct"] for x in round_rows), 6),
            "avg_weighted_round_pnl_pct": round(sum(x["weighted_round_pnl_pct"] for x in round_rows) / len(round_rows), 6) if round_rows else 0.0,
            "win_count": round_win_count,
            "loss_count": round_loss_count,
            "flat_count": round_flat_count,
            "win_rate": round(round_win_count / len(round_rows), 6) if round_rows else 0.0,
        },
        "monthly_summary": [
            {
                "month": m,
                **{k: (round(v, 6) if isinstance(v, float) else v) for k, v in monthly[m].items() if k != "win_count"},
            }
            for m in sorted(monthly.keys())
        ],
        "top_rounds": sorted(round_rows, key=lambda x: (-x["weighted_round_pnl_pct"], x["bar_ts"]))[:20],
        "worst_rounds": sorted(round_rows, key=lambda x: (x["weighted_round_pnl_pct"], x["bar_ts"]))[:20],
    }
    return summary, round_rows


def write_trades_csv(results: List[ReplayResult], out_path: Path) -> None:
    rows = [r.__dict__ for r in results]
    headers = list(rows[0].keys()) if rows else []
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def write_rounds_csv(rows: List[dict], out_path: Path) -> None:
    headers = [
        "bar_ts", "bar_bj", "leg_count", "weighted_round_pnl_pct",
        "win_leg_count", "loss_leg_count", "top_symbols"
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    ap = argparse.ArgumentParser(description="Replay M_GT_10 top-N equal-weight combo using candidate audit + 1m klines.")
    ap.add_argument("--candidate-audit", required=True, help="Path to snapback_candidate_pool_audit.jsonl")
    ap.add_argument("--kline-root", required=True, help="Root dir containing 1m kline files, e.g. data/klines_1m")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--top-n", type=int, default=5, help="Top N candidates per M_GT_10 round")
    ap.add_argument("--candidate-threshold", type=int, default=10, help="Use rounds with candidate_count > threshold")
    ap.add_argument("--same-bar-policy", choices=["sl_first", "tp_first"], default="sl_first")
    ap.add_argument("--config", help="Optional strategy config json to load max_hold_mins and time_stop min_profit_pct")
    ap.add_argument("--max-hold-mins", type=int, help="Override max_hold_mins")
    ap.add_argument("--time-stop-min-profit-pct", type=float, help="Override time_stop min_profit_pct, e.g. 0.0")
    args = ap.parse_args()

    cfg_max_hold, cfg_ts_min_profit = load_config(Path(args.config)) if args.config else (None, None)
    max_hold_mins = args.max_hold_mins if args.max_hold_mins is not None else cfg_max_hold
    ts_min_profit = args.time_stop_min_profit_pct if args.time_stop_min_profit_pct is not None else cfg_ts_min_profit
    if max_hold_mins is None or ts_min_profit is None:
        raise RuntimeError("must provide either --config or both --max-hold-mins and --time-stop-min-profit-pct")

    input_path = Path(args.candidate_audit)
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = load_jsonl(input_path)
    unique_rows, dup_samples, duplicate_groups, duplicate_extra_rows = dedupe_by_bar_ts(rows)
    target_rows = [r for r in unique_rows if int(r["candidate_count"]) > int(args.candidate_threshold)]

    store = KlineStore(Path(args.kline_root))
    results: List[ReplayResult] = []

    for row in target_rows:
        ranked = add_ranks(list(row.get("candidates_sorted_by_drop_pct") or []))
        selected = ranked[: args.top_n]
        if not selected:
            continue
        weight = 1.0 / len(selected)
        for cand in selected:
            results.append(
                replay_one_candidate(
                    cand=cand,
                    bar_ts=int(row["bar_ts"]),
                    bar_bj=str(row["bar_bj"]),
                    store=store,
                    max_hold_mins=int(max_hold_mins),
                    time_stop_min_profit_pct=float(ts_min_profit),
                    same_bar_policy=args.same_bar_policy,
                    weight=weight,
                    candidate_count=int(row["candidate_count"]),
                )
            )

    summary, round_rows = summarize_results(results, args.top_n, duplicate_groups, duplicate_extra_rows)

    summary_path = out_dir / "m_gt_10_topn_combo_replay_summary.json"
    trades_csv = out_dir / "m_gt_10_topn_combo_replay_trades.csv"
    rounds_csv = out_dir / "m_gt_10_topn_combo_replay_rounds.csv"
    dup_json = out_dir / "m_gt_10_topn_combo_replay_duplicates.json"

    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    write_trades_csv(results, trades_csv)
    write_rounds_csv(round_rows, rounds_csv)
    dup_json.write_text(json.dumps(dup_samples, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=== m_gt_10 topN combo replay done ===")
    print(f"candidate_audit         : {input_path}")
    print(f"kline_root              : {args.kline_root}")
    print(f"candidate_threshold     : > {args.candidate_threshold}")
    print(f"top_n                   : {args.top_n}")
    print(f"same_bar_policy         : {args.same_bar_policy}")
    print(f"max_hold_mins           : {max_hold_mins}")
    print(f"time_stop_min_profit_pct: {ts_min_profit}")
    print(f"target_rounds           : {len(target_rows)}")
    print(f"selected_legs_total     : {len(results)}")
    print(f"summary_json            : {summary_path}")
    print(f"trades_csv              : {trades_csv}")
    print(f"rounds_csv              : {rounds_csv}")
    print(f"duplicates_json         : {dup_json}")


if __name__ == "__main__":
    main()
