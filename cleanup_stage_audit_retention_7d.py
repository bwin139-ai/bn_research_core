#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

BJ = timezone(timedelta(hours=8))


@dataclass
class FileStats:
    path: Path
    total_lines: int = 0
    kept_lines: int = 0
    dropped_lines: int = 0
    parse_errors: int = 0
    oldest_kept_bj: str | None = None
    newest_kept_bj: str | None = None
    cutoff_bj: str | None = None
    original_size: int = 0
    new_size: int = 0


def _fmt_size(num: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    val = float(num)
    for unit in units:
        if val < 1024.0 or unit == units[-1]:
            return f"{val:.1f}{unit}"
        val /= 1024.0
    return f"{num}B"


def _parse_bj_text(text: str | None) -> datetime | None:
    if not text:
        return None
    text = str(text).strip()
    fmts = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]
    for fmt in fmts:
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=BJ)
        except Exception:
            pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(BJ)
    except Exception:
        return None


def _extract_row_dt_bj(row: dict[str, Any]) -> datetime | None:
    for key in ("bar_bj", "signal_time_bj", "c_bar_bj", "open_time_bj"):
        dt = _parse_bj_text(row.get(key))
        if dt is not None:
            return dt

    for key in ("bar_ts", "signal_time_ts", "signal_time", "c_bar_ts", "open_time_ms"):
        raw = row.get(key)
        try:
            ts = int(raw)
        except Exception:
            ts = None
        if ts and ts > 0:
            if ts > 10_000_000_000:
                return datetime.fromtimestamp(ts / 1000.0, tz=timezone.utc).astimezone(BJ)
            return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(BJ)
    return None


def _filter_file(path: Path, cutoff_dt_bj: datetime, apply_changes: bool, keep_backup: bool) -> FileStats:
    stats = FileStats(path=path)
    stats.cutoff_bj = cutoff_dt_bj.strftime("%Y-%m-%d %H:%M:%S")
    stats.original_size = path.stat().st_size

    tmp_path = path.with_suffix(path.suffix + ".retention_tmp")
    backup_path = path.with_suffix(path.suffix + ".bak_before_retention")

    with path.open("r", encoding="utf-8") as src, tmp_path.open("w", encoding="utf-8") as dst:
        for line in src:
            stats.total_lines += 1
            try:
                row = json.loads(line)
            except Exception:
                stats.parse_errors += 1
                continue

            dt_bj = _extract_row_dt_bj(row)
            if dt_bj is None:
                stats.dropped_lines += 1
                continue

            if dt_bj >= cutoff_dt_bj:
                dst.write(json.dumps(row, ensure_ascii=False) + "\n")
                stats.kept_lines += 1
                bj_text = dt_bj.strftime("%Y-%m-%d %H:%M:%S")
                if stats.oldest_kept_bj is None or bj_text < stats.oldest_kept_bj:
                    stats.oldest_kept_bj = bj_text
                if stats.newest_kept_bj is None or bj_text > stats.newest_kept_bj:
                    stats.newest_kept_bj = bj_text
            else:
                stats.dropped_lines += 1

    stats.new_size = tmp_path.stat().st_size if tmp_path.exists() else 0

    if not apply_changes:
        tmp_path.unlink(missing_ok=True)
        return stats

    if keep_backup:
        shutil.move(str(path), str(backup_path))
    else:
        path.unlink(missing_ok=True)
    shutil.move(str(tmp_path), str(path))
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Retain only recent N days for large stage_audit JSONL files (BJ timezone)."
    )
    parser.add_argument("paths", nargs="+", help="Target JSONL files")
    parser.add_argument("--days", type=int, default=7, help="Retention days in BJ timezone (default: 7)")
    parser.add_argument("--apply", action="store_true", help="Actually rewrite files; default is dry-run")
    parser.add_argument("--no-backup", action="store_true", help="Do not keep .bak_before_retention backup")
    args = parser.parse_args()

    now_bj = datetime.now(timezone.utc).astimezone(BJ)
    cutoff_dt_bj = now_bj - timedelta(days=int(args.days))

    print("=== stage_audit retention ===")
    print(f"now_bj    : {now_bj.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"cutoff_bj : {cutoff_dt_bj.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"mode      : {'APPLY' if args.apply else 'DRY-RUN'}")
    print(f"backup    : {not args.no_backup}")
    print("")

    total_before = 0
    total_after = 0

    for raw in args.paths:
        path = Path(raw)
        if not path.exists():
            print(f"[SKIP] missing: {path}")
            continue

        stats = _filter_file(
            path=path,
            cutoff_dt_bj=cutoff_dt_bj,
            apply_changes=bool(args.apply),
            keep_backup=not bool(args.no_backup),
        )

        total_before += stats.original_size
        total_after += stats.new_size

        print(f"[FILE] {stats.path}")
        print(f"  total_lines    : {stats.total_lines}")
        print(f"  kept_lines     : {stats.kept_lines}")
        print(f"  dropped_lines  : {stats.dropped_lines}")
        print(f"  parse_errors   : {stats.parse_errors}")
        print(f"  oldest_kept_bj : {stats.oldest_kept_bj}")
        print(f"  newest_kept_bj : {stats.newest_kept_bj}")
        print(f"  original_size  : {stats.original_size} ({_fmt_size(stats.original_size)})")
        print(f"  new_size       : {stats.new_size} ({_fmt_size(stats.new_size)})")
        if stats.original_size > 0:
            saved = max(stats.original_size - stats.new_size, 0)
            print(f"  reclaimable    : {saved} ({_fmt_size(saved)})")
        if args.apply and (not args.no_backup):
            print(f"  backup_path    : {stats.path.with_suffix(stats.path.suffix + '.bak_before_retention')}")
        print("")

    if total_before > 0:
        saved_total = max(total_before - total_after, 0)
        print("=== summary ===")
        print(f"total_before : {total_before} ({_fmt_size(total_before)})")
        print(f"total_after  : {total_after} ({_fmt_size(total_after)})")
        print(f"reclaimable  : {saved_total} ({_fmt_size(saved_total)})")


if __name__ == "__main__":
    main()
