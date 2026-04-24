#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional

import pandas as pd
import pyarrow.parquet as pq

REQUIRED_IDX_COLS = ["high_idx", "low_idx", "close_idx"]


@dataclass
class ShardAuditRow:
    symbol: str
    month: str
    file_path: str
    rows: int
    has_high_idx_col: bool
    has_low_idx_col: bool
    has_close_idx_col: bool
    missing_high_idx: int
    missing_low_idx: int
    missing_close_idx: int
    missing_all_three: int
    missing_ratio_all_three: float
    first_open_time_ms: Optional[int]
    last_open_time_ms: Optional[int]


@dataclass
class SymbolSummary:
    symbol: str
    shard_count: int
    total_rows: int
    total_missing_all_three: int
    missing_ratio_all_three: float
    shards_missing_any: int
    shards_missing_all_three: int
    shards_missing_columns: int
    first_open_time_ms: Optional[int]
    last_open_time_ms: Optional[int]
    classification: str


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Audit high_idx/low_idx/close_idx completeness in data/klines_1m parquet shards"
    )
    ap.add_argument("--data-dir", default="data/klines_1m", help="root dir of contract parquet shards")
    ap.add_argument("--out-csv", default="output/state/idx_completeness_audit.csv", help="per-shard CSV output path")
    ap.add_argument("--summary-json", default="output/state/idx_completeness_audit.summary.json", help="summary JSON output path")
    ap.add_argument("--symbols", default="", help="optional comma-separated symbols filter")
    ap.add_argument("--top-n", type=int, default=30, help="top N symbols in summary rankings")
    return ap.parse_args()


def ensure_parent(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)


def list_symbol_dirs(data_dir: str, symbols_filter: Optional[set[str]] = None) -> List[str]:
    if not os.path.isdir(data_dir):
        raise FileNotFoundError(f"data dir not found: {data_dir}")
    out: List[str] = []
    for name in sorted(os.listdir(data_dir)):
        path = os.path.join(data_dir, name)
        if not os.path.isdir(path):
            continue
        if symbols_filter and name.upper() not in symbols_filter:
            continue
        out.append(name)
    return out


def audit_shard(symbol: str, file_path: str) -> ShardAuditRow:
    pf = pq.ParquetFile(file_path)
    names = set(pf.schema_arrow.names)
    has_cols = {col: col in names for col in REQUIRED_IDX_COLS}

    cols_to_read = ["open_time_ms"] + [col for col in REQUIRED_IDX_COLS if has_cols[col]]
    tbl = pq.read_table(file_path, columns=cols_to_read)
    df = tbl.to_pandas()

    row_count = len(df)
    first_open = int(df["open_time_ms"].iloc[0]) if row_count > 0 else None
    last_open = int(df["open_time_ms"].iloc[-1]) if row_count > 0 else None

    missing_counts: Dict[str, int] = {}
    for col in REQUIRED_IDX_COLS:
        if has_cols[col]:
            missing_counts[col] = int(df[col].isna().sum())
        else:
            missing_counts[col] = row_count

    if row_count > 0:
        present_series = []
        for col in REQUIRED_IDX_COLS:
            if has_cols[col]:
                present_series.append(df[col].notna())
            else:
                present_series.append(pd.Series([False] * row_count, index=df.index))
        any_present = present_series[0] | present_series[1] | present_series[2]
        missing_all_three = int((~any_present).sum())
    else:
        missing_all_three = 0

    month = os.path.basename(file_path).replace(".parquet", "")
    return ShardAuditRow(
        symbol=symbol,
        month=month,
        file_path=file_path,
        rows=row_count,
        has_high_idx_col=has_cols["high_idx"],
        has_low_idx_col=has_cols["low_idx"],
        has_close_idx_col=has_cols["close_idx"],
        missing_high_idx=missing_counts["high_idx"],
        missing_low_idx=missing_counts["low_idx"],
        missing_close_idx=missing_counts["close_idx"],
        missing_all_three=missing_all_three,
        missing_ratio_all_three=(missing_all_three / row_count) if row_count > 0 else 0.0,
        first_open_time_ms=first_open,
        last_open_time_ms=last_open,
    )


def classify_symbol(total_rows: int, total_missing: int, shards_missing_columns: int, shards_missing_any: int) -> str:
    if total_rows <= 0:
        return "EMPTY"
    if shards_missing_columns > 0:
        return "OLD_SCHEMA_OR_BROKEN"
    if total_missing == 0:
        return "COMPLETE"
    if total_missing == total_rows:
        return "NO_IDX_ALL_ROWS"
    if shards_missing_any == 1 and total_missing < min(20, total_rows):
        return "TINY_GAP"
    return "PARTIAL_GAP"


def build_symbol_summaries(rows: List[ShardAuditRow]) -> List[SymbolSummary]:
    by_symbol: Dict[str, List[ShardAuditRow]] = {}
    for row in rows:
        by_symbol.setdefault(row.symbol, []).append(row)

    summaries: List[SymbolSummary] = []
    for symbol, parts in sorted(by_symbol.items()):
        parts = sorted(parts, key=lambda x: x.month)
        total_rows = sum(x.rows for x in parts)
        total_missing = sum(x.missing_all_three for x in parts)
        shards_missing_any = sum(1 for x in parts if x.missing_all_three > 0)
        shards_missing_all_three = sum(1 for x in parts if x.rows > 0 and x.missing_all_three == x.rows)
        shards_missing_columns = sum(
            1
            for x in parts
            if not (x.has_high_idx_col and x.has_low_idx_col and x.has_close_idx_col)
        )
        first_open = min((x.first_open_time_ms for x in parts if x.first_open_time_ms is not None), default=None)
        last_open = max((x.last_open_time_ms for x in parts if x.last_open_time_ms is not None), default=None)
        summaries.append(
            SymbolSummary(
                symbol=symbol,
                shard_count=len(parts),
                total_rows=total_rows,
                total_missing_all_three=total_missing,
                missing_ratio_all_three=(total_missing / total_rows) if total_rows > 0 else 0.0,
                shards_missing_any=shards_missing_any,
                shards_missing_all_three=shards_missing_all_three,
                shards_missing_columns=shards_missing_columns,
                first_open_time_ms=first_open,
                last_open_time_ms=last_open,
                classification=classify_symbol(total_rows, total_missing, shards_missing_columns, shards_missing_any),
            )
        )
    return summaries


def to_ms_records(symbol_summaries: List[SymbolSummary], top_n: int) -> dict:
    total_symbols = len(symbol_summaries)
    total_rows = sum(x.total_rows for x in symbol_summaries)
    total_missing = sum(x.total_missing_all_three for x in symbol_summaries)

    by_class: Dict[str, int] = {}
    for item in symbol_summaries:
        by_class[item.classification] = by_class.get(item.classification, 0) + 1

    top_missing_ratio = sorted(
        [x for x in symbol_summaries if x.total_rows > 0],
        key=lambda x: (x.missing_ratio_all_three, x.total_missing_all_three),
        reverse=True,
    )[:top_n]
    top_missing_rows = sorted(
        symbol_summaries,
        key=lambda x: (x.total_missing_all_three, x.missing_ratio_all_three),
        reverse=True,
    )[:top_n]

    return {
        "total_symbols": total_symbols,
        "total_rows": total_rows,
        "total_missing_all_three": total_missing,
        "global_missing_ratio_all_three": (total_missing / total_rows) if total_rows > 0 else 0.0,
        "classification_counts": by_class,
        "symbols_complete": [x.symbol for x in symbol_summaries if x.classification == "COMPLETE"],
        "symbols_no_idx_all_rows": [x.symbol for x in symbol_summaries if x.classification == "NO_IDX_ALL_ROWS"],
        "symbols_partial_gap": [x.symbol for x in symbol_summaries if x.classification == "PARTIAL_GAP"],
        "symbols_old_schema_or_broken": [x.symbol for x in symbol_summaries if x.classification == "OLD_SCHEMA_OR_BROKEN"],
        "top_missing_ratio": [asdict(x) for x in top_missing_ratio],
        "top_missing_rows": [asdict(x) for x in top_missing_rows],
    }


def main() -> None:
    args = parse_args()
    symbols_filter = None
    if args.symbols.strip():
        symbols_filter = {x.strip().upper() for x in args.symbols.split(",") if x.strip()}

    symbol_dirs = list_symbol_dirs(args.data_dir, symbols_filter)
    shard_rows: List[ShardAuditRow] = []

    for symbol in symbol_dirs:
        sym_dir = os.path.join(args.data_dir, symbol)
        files = sorted(
            os.path.join(sym_dir, fn)
            for fn in os.listdir(sym_dir)
            if fn.endswith(".parquet")
        )
        for fp in files:
            shard_rows.append(audit_shard(symbol, fp))

    shard_df = pd.DataFrame([asdict(x) for x in shard_rows])
    ensure_parent(args.out_csv)
    if not shard_df.empty:
        shard_df = shard_df.sort_values(["symbol", "month"]).reset_index(drop=True)
    shard_df.to_csv(args.out_csv, index=False)

    symbol_summaries = build_symbol_summaries(shard_rows)
    summary = to_ms_records(symbol_summaries, args.top_n)
    summary["data_dir"] = args.data_dir
    summary["out_csv"] = args.out_csv
    summary["symbols_audited"] = len(symbol_dirs)

    ensure_parent(args.summary_json)
    with open(args.summary_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

    print("=" * 80)
    print("idx 完整性审计完成")
    print("=" * 80)
    print(f"data_dir           : {args.data_dir}")
    print(f"symbols_audited    : {len(symbol_dirs)}")
    print(f"per_shard_csv      : {args.out_csv}")
    print(f"summary_json       : {args.summary_json}")
    print(f"total_rows         : {summary['total_rows']}")
    print(f"total_missing_all3 : {summary['total_missing_all_three']}")
    print(f"global_missing_pct : {summary['global_missing_ratio_all_three'] * 100:.4f}%")
    print("classification_cnt :")
    for k, v in sorted(summary["classification_counts"].items()):
        print(f"  - {k}: {v}")

    print("\nTop missing ratio symbols:")
    for item in summary["top_missing_ratio"][: min(10, len(summary["top_missing_ratio"]))]:
        print(
            f"  - {item['symbol']}: missing_ratio={item['missing_ratio_all_three'] * 100:.2f}% "
            f"missing_rows={item['total_missing_all_three']} classification={item['classification']}"
        )


if __name__ == "__main__":
    main()
