#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
tools/prune_klines_schema.py

用途：
- 从旧 10 列 1m kline parquet 库中，只抽取 6 个有效字段
- 写入新的 6 列 parquet 库目录
- 默认不修改源目录，属于非破坏式迁移

默认保留 6 列：
1) open_time_ms
2) open
3) high
4) low
5) close
6) quote_asset_volume

删除 4 列：
- volume
- taker_buy_quote_asset_volume
- close_time_ms
- trades
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


KEEP_COLUMNS = [
    "open_time_ms",
    "open",
    "high",
    "low",
    "close",
    "quote_asset_volume",
]

TARGET_SCHEMA = pa.schema([
    ("open_time_ms", pa.int64()),
    ("open", pa.float64()),
    ("high", pa.float64()),
    ("low", pa.float64()),
    ("close", pa.float64()),
    ("quote_asset_volume", pa.float64()),
])


def human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(n)
    for unit in units:
        if x < 1024.0 or unit == units[-1]:
            return f"{x:.2f}{unit}"
        x /= 1024.0
    return f"{n}B"


def iter_parquet_files(root: Path):
    for p in sorted(root.rglob("*.parquet")):
        if p.is_file():
            yield p


def ensure_required_columns(table: pa.Table, src_file: Path) -> None:
    cols = set(table.column_names)
    missing = [c for c in KEEP_COLUMNS if c not in cols]
    if missing:
        raise ValueError(f"missing columns in {src_file}: {missing}")


def cast_table_to_target_schema(table: pa.Table, src_file: Path) -> pa.Table:
    """
    只保留 KEEP_COLUMNS，并强制 cast 到统一 schema，避免不同 shard 的 dtype 漂移。
    """
    ensure_required_columns(table, src_file)

    table = table.select(KEEP_COLUMNS)

    arrays = []
    for field in TARGET_SCHEMA:
        col = table[field.name]
        try:
            casted = col.cast(field.type)
        except Exception as e:
            raise ValueError(
                f"cast failed in {src_file}, column={field.name}, "
                f"from={col.type} to={field.type}: {e}"
            ) from e
        arrays.append(casted)

    return pa.Table.from_arrays(arrays, schema=TARGET_SCHEMA)


def convert_one_file(src_file: Path, src_root: Path, dst_root: Path, dry_run: bool) -> tuple[int, int]:
    rel = src_file.relative_to(src_root)
    dst_file = dst_root / rel
    dst_file.parent.mkdir(parents=True, exist_ok=True)

    src_size = src_file.stat().st_size

    table = pq.read_table(src_file)
    table = cast_table_to_target_schema(table, src_file)

    if not dry_run:
        pq.write_table(
            table,
            dst_file,
            compression="zstd",
            use_dictionary=False,
        )

    dst_size = dst_file.stat().st_size if (not dry_run and dst_file.exists()) else 0
    return src_size, dst_size


def main() -> int:
    parser = argparse.ArgumentParser(description="Prune kline parquet schema from 10 columns to 6 columns.")
    parser.add_argument(
        "--src-root",
        required=True,
        help="源目录，例如 /root/BN_strategy/data/klines_1m",
    )
    parser.add_argument(
        "--dst-root",
        required=True,
        help="目标目录，例如 /root/bn_research_core/data/klines_1m",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="只检查并统计，不写文件",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="只处理前 N 个 parquet 文件，0 表示全部处理",
    )
    args = parser.parse_args()

    src_root = Path(args.src_root).resolve()
    dst_root = Path(args.dst_root).resolve()

    if not src_root.exists():
        print(f"[ERROR] src root not found: {src_root}")
        return 1

    files = list(iter_parquet_files(src_root))
    if not files:
        print(f"[ERROR] no parquet files found under: {src_root}")
        return 1

    if args.limit > 0:
        files = files[:args.limit]

    print("===== PRUNE KLINES SCHEMA =====")
    print(f"src_root : {src_root}")
    print(f"dst_root : {dst_root}")
    print(f"dry_run  : {args.dry_run}")
    print(f"files    : {len(files)}")
    print(f"keep     : {KEEP_COLUMNS}")
    print("================================")

    ok = 0
    fail = 0
    total_src_size = 0
    total_dst_size = 0

    for i, src_file in enumerate(files, 1):
        try:
            src_size, dst_size = convert_one_file(
                src_file=src_file,
                src_root=src_root,
                dst_root=dst_root,
                dry_run=args.dry_run,
            )
            total_src_size += src_size
            total_dst_size += dst_size
            ok += 1

            rel = src_file.relative_to(src_root)
            print(f"[OK] {i}/{len(files)} {rel}")
        except Exception as e:
            fail += 1
            rel = src_file.relative_to(src_root)
            print(f"[FAIL] {i}/{len(files)} {rel} :: {e}")

    print("================================")
    print(f"ok files       : {ok}")
    print(f"fail files     : {fail}")
    print(f"src total size : {human_bytes(total_src_size)}")
    if not args.dry_run:
        print(f"dst total size : {human_bytes(total_dst_size)}")
        if total_src_size > 0:
            ratio = (total_dst_size / total_src_size) * 100.0
            print(f"size ratio     : {ratio:.2f}%")
    print("================================")

    return 0 if fail == 0 else 2


if __name__ == "__main__":
    sys.exit(main())