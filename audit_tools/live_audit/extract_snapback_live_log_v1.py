#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterable

TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d{3}\b")
NOHUP_LINE = "nohup: ignoring input"
TRACEBACK_START = "Traceback (most recent call last):"
KEYBOARD_INTERRUPT = "KeyboardInterrupt"


@dataclass
class Record:
    ts: datetime | None
    lines: list[str] = field(default_factory=list)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="按北京时间裁剪 snapback_live.console.log，并自动去掉 KeyboardInterrupt 相关低价值 traceback。"
    )
    p.add_argument("--input", required=True, help="原始日志文件路径")
    p.add_argument("--output", required=True, help="输出文件路径")
    p.add_argument("--start-bj", help='起始北京时间，格式如 "2026-03-29 11:00" 或 "2026-03-29 11:00:00"')
    p.add_argument("--end-bj", help='结束北京时间，格式如 "2026-03-29 16:00" 或 "2026-03-29 16:00:00"')
    p.add_argument("--keep-nohup", action="store_true", help="保留 nohup: ignoring input 行")
    p.add_argument(
        "--keep-keyboardinterrupt-marker",
        action="store_true",
        help="删除 KeyboardInterrupt traceback 后，保留一行简短标记",
    )
    return p.parse_args()


def parse_bj_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue
    raise SystemExit(f"invalid datetime format: {value}")


def extract_ts(line: str) -> datetime | None:
    m = TS_RE.match(line)
    if not m:
        return None
    return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")


def iter_records(lines: Iterable[str]) -> list[Record]:
    records: list[Record] = []
    current: Record | None = None
    for raw in lines:
        line = raw.rstrip("\n")
        ts = extract_ts(line)
        if ts is not None:
            current = Record(ts=ts, lines=[line])
            records.append(current)
            continue
        if current is None:
            current = Record(ts=None, lines=[line])
            records.append(current)
            continue
        current.lines.append(line)
    return records


def record_in_range(record: Record, start_dt: datetime | None, end_dt: datetime | None) -> bool:
    if record.ts is None:
        return False
    if start_dt is not None and record.ts < start_dt:
        return False
    if end_dt is not None and record.ts > end_dt:
        return False
    return True


def strip_keyboardinterrupt_traceback(
    lines: list[str], *,
    keep_marker: bool,
) -> tuple[list[str], bool]:
    if TRACEBACK_START not in lines:
        return lines, False

    tb_idx = None
    for i, line in enumerate(lines):
        if line == TRACEBACK_START:
            tb_idx = i
            break
    if tb_idx is None:
        return lines, False

    tail_nonempty = None
    for i in range(len(lines) - 1, -1, -1):
        if lines[i].strip():
            tail_nonempty = lines[i].strip()
            break

    if tail_nonempty != KEYBOARD_INTERRUPT:
        return lines, False

    kept = lines[:tb_idx]
    if keep_marker:
        kept.append("[FILTERED] KeyboardInterrupt traceback removed")
    return kept, True


def clean_record(
    record: Record,
    *,
    keep_nohup: bool,
    keep_kbi_marker: bool,
) -> tuple[list[str], bool]:
    lines = list(record.lines)

    if not keep_nohup:
        lines = [x for x in lines if x.strip() != NOHUP_LINE]

    lines, dropped_kbi = strip_keyboardinterrupt_traceback(
        lines,
        keep_marker=keep_kbi_marker,
    )

    while lines and not lines[-1].strip():
        lines.pop()

    return lines, dropped_kbi


def main() -> int:
    args = parse_args()
    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise SystemExit(f"input file not found: {input_path}")

    start_dt = parse_bj_dt(args.start_bj)
    end_dt = parse_bj_dt(args.end_bj)
    if start_dt and end_dt and start_dt > end_dt:
        raise SystemExit("start-bj must be <= end-bj")

    raw_lines = input_path.read_text(encoding="utf-8", errors="replace").splitlines()
    records = iter_records(raw_lines)

    kept_lines: list[str] = []
    kept_record_count = 0
    dropped_kbi_count = 0

    for record in records:
        if not record_in_range(record, start_dt, end_dt):
            continue

        cleaned, dropped_kbi = clean_record(
            record,
            keep_nohup=args.keep_nohup,
            keep_kbi_marker=args.keep_keyboardinterrupt_marker,
        )
        if dropped_kbi:
            dropped_kbi_count += 1
        if not cleaned:
            continue

        kept_lines.extend(cleaned)
        kept_record_count += 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(kept_lines) + ("\n" if kept_lines else ""), encoding="utf-8")

    print(f"input_file           : {input_path}")
    print(f"output_file          : {output_path}")
    print(f"start_bj             : {args.start_bj}")
    print(f"end_bj               : {args.end_bj}")
    print(f"records_kept         : {kept_record_count}")
    print(f"keyboardinterrupt_tb : {dropped_kbi_count}")
    print(f"keep_nohup           : {args.keep_nohup}")
    print(f"keep_kbi_marker      : {args.keep_keyboardinterrupt_marker}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
