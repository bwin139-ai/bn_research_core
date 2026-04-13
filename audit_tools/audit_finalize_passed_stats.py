#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _to_int(v: Any) -> int | None:
    try:
        if v is None or v == "":
            return None
        return int(v)
    except Exception:
        return None


def _to_float(v: Any) -> float | None:
    try:
        if v is None or v == "":
            return None
        return float(v)
    except Exception:
        return None


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, 1):
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception as e:
                raise RuntimeError(f"JSONL 解析失败: {path} line={line_no} err={e}") from e
            if isinstance(obj, dict):
                rows.append(obj)
    return rows


def _candidate_count(rec: dict[str, Any]) -> int | None:
    for key in (
        "candidate_symbol_count_before_finalize",
        "candidate_symbols_count",
    ):
        v = _to_int(rec.get(key))
        if v is not None:
            return v
    return None


def _all_passed_elapsed_ms_from_record(rec: dict[str, Any]) -> int | None:
    """
    优先使用逐秒收敛 schema 的精确字段。
    若缺失，则尝试基于旧 schema 做近似：
      - finalize_elapsed_ms 存在
      - candidate_count == candidate_symbol_count_after_finalize
      - verify_failed_count == 0
      - skipped_due_deadline != True
    这种近似仅代表“本轮 finalize 完成且未丢 symbol”，
    不等价于真正的“逐秒收敛全员 passed 时间”。
    """
    precise_keys = (
        "finalize_all_passed_elapsed_ms",
        "all_passed_elapsed_ms",
    )
    for key in precise_keys:
        v = _to_int(rec.get(key))
        if v is not None:
            return v

    finalize_summary = rec.get("finalize_summary")
    if isinstance(finalize_summary, dict):
        for key in precise_keys:
            v = _to_int(finalize_summary.get(key))
            if v is not None:
                return v

    # fallback / approximate from old schema
    finalize_elapsed_ms = _to_int(rec.get("finalize_elapsed_ms"))
    if finalize_elapsed_ms is None and isinstance(finalize_summary, dict):
        finalize_elapsed_ms = _to_int(finalize_summary.get("finalize_elapsed_ms"))
    if finalize_elapsed_ms is None:
        return None

    candidate_before = _to_int(rec.get("candidate_symbol_count_before_finalize"))
    candidate_after = _to_int(rec.get("candidate_symbol_count_after_finalize"))
    verify_failed_count = _to_int(rec.get("finalize_verify_failed_count"))
    skipped_due_deadline = rec.get("skipped_due_deadline")
    if skipped_due_deadline is None and isinstance(finalize_summary, dict):
        skipped_due_deadline = finalize_summary.get("skipped_due_deadline")

    if (
        candidate_before is not None
        and candidate_after is not None
        and verify_failed_count is not None
        and candidate_before == candidate_after
        and verify_failed_count == 0
        and bool(skipped_due_deadline) is False
    ):
        return finalize_elapsed_ms

    return None


def _deadline_hit_from_record(rec: dict[str, Any]) -> bool:
    keys = (
        "finalize_deadline_hit",
        "deadline_hit",
        "skipped_due_deadline",
    )
    for key in keys:
        v = rec.get(key)
        if isinstance(v, bool):
            return v
    finalize_summary = rec.get("finalize_summary")
    if isinstance(finalize_summary, dict):
        for key in keys:
            v = finalize_summary.get(key)
            if isinstance(v, bool):
                return v
    # 兼容明确 timeout count 的未来 schema
    timeout_count = _to_int(rec.get("timeout_not_finalized_count"))
    if timeout_count is None and isinstance(finalize_summary, dict):
        timeout_count = _to_int(finalize_summary.get("timeout_not_finalized_count"))
    return (timeout_count or 0) > 0


def _record_c_bar_bj(rec: dict[str, Any]) -> str | None:
    for key in ("c_bar_bj", "latest_closed_bar_bj", "bar_bj"):
        v = rec.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _record_account(rec: dict[str, Any]) -> str | None:
    v = rec.get("account")
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None


def _collect_finalize_records(rows: list[dict[str, Any]], account: str | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for rec in rows:
        if account:
            rec_account = _record_account(rec)
            if rec_account and rec_account != account:
                continue
        event = rec.get("event")
        stage = rec.get("stage")
        if event == "c_bar_finalize_summary" or stage == "stage0_run_once_perf":
            # stage0_run_once_perf 需要至少带 finalize 字段才纳入
            if event == "c_bar_finalize_summary":
                out.append(rec)
                continue
            if _to_int(rec.get("finalize_elapsed_ms")) is not None or isinstance(rec.get("finalize_summary"), dict):
                out.append(rec)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(
        description="统计 live finalize 全员 passed 时间与 deadline 次数"
    )
    parser.add_argument(
        "--audit-jsonl",
        default="state/live_audit/snapback_mybwin139.jsonl",
        help="live audit JSONL 路径",
    )
    parser.add_argument(
        "--account",
        default="mybwin139",
        help="账户名；为空则不过滤 account",
    )
    parser.add_argument(
        "--start-c-bar-bj",
        default="",
        help='起始 c_bar_bj，例: "2026-04-08 20:00:00"',
    )
    parser.add_argument(
        "--end-c-bar-bj",
        default="",
        help='结束 c_bar_bj，例: "2026-04-09 11:00:00"',
    )
    parser.add_argument(
        "--out-json",
        default="output/sim_live_audit/finalize_passed_stats.json",
        help="输出 summary json 路径",
    )
    args = parser.parse_args()

    audit_path = Path(args.audit_jsonl)
    if not audit_path.exists():
        raise SystemExit(f"文件不存在: {audit_path}")

    rows = _load_jsonl(audit_path)
    finalize_rows = _collect_finalize_records(rows, args.account or None)

    start_bj = (args.start_c_bar_bj or "").strip()
    end_bj = (args.end_c_bar_bj or "").strip()

    filtered: list[dict[str, Any]] = []
    for rec in finalize_rows:
        c_bar_bj = _record_c_bar_bj(rec)
        if not c_bar_bj:
            continue
        if start_bj and c_bar_bj < start_bj:
            continue
        if end_bj and c_bar_bj > end_bj:
            continue
        filtered.append(rec)

    all_passed_rows: list[dict[str, Any]] = []
    deadline_count = 0
    approximate_mode_count = 0

    per_bar_rows: list[dict[str, Any]] = []
    for rec in filtered:
        c_bar_bj = _record_c_bar_bj(rec)
        elapsed_ms = _all_passed_elapsed_ms_from_record(rec)
        deadline_hit = _deadline_hit_from_record(rec)
        if deadline_hit:
            deadline_count += 1

        exact = False
        for key in ("finalize_all_passed_elapsed_ms", "all_passed_elapsed_ms"):
            if _to_int(rec.get(key)) is not None:
                exact = True
        if not exact and isinstance(rec.get("finalize_summary"), dict):
            fs = rec["finalize_summary"]
            for key in ("finalize_all_passed_elapsed_ms", "all_passed_elapsed_ms"):
                if _to_int(fs.get(key)) is not None:
                    exact = True

        row = {
            "c_bar_bj": c_bar_bj,
            "candidate_count": _candidate_count(rec),
            "all_passed_elapsed_ms": elapsed_ms,
            "all_passed_elapsed_secs": round(elapsed_ms / 1000.0, 3) if elapsed_ms is not None else None,
            "deadline_hit": deadline_hit,
            "exact_schema": exact,
        }
        per_bar_rows.append(row)

        if elapsed_ms is not None:
            all_passed_rows.append(row)
            if not exact:
                approximate_mode_count += 1

    elapsed_list = [int(x["all_passed_elapsed_ms"]) for x in all_passed_rows if x["all_passed_elapsed_ms"] is not None]

    min_ms = min(elapsed_list) if elapsed_list else None
    max_ms = max(elapsed_list) if elapsed_list else None
    avg_ms = round(sum(elapsed_list) / len(elapsed_list), 3) if elapsed_list else None

    summary = {
        "audit_jsonl": str(audit_path),
        "account": args.account or None,
        "start_c_bar_bj": start_bj or None,
        "end_c_bar_bj": end_bj or None,
        "finalize_record_count": len(filtered),
        "all_passed_round_count": len(all_passed_rows),
        "deadline_count": deadline_count,
        "min_all_passed_elapsed_ms": min_ms,
        "max_all_passed_elapsed_ms": max_ms,
        "avg_all_passed_elapsed_ms": avg_ms,
        "min_all_passed_elapsed_secs": round(min_ms / 1000.0, 3) if min_ms is not None else None,
        "max_all_passed_elapsed_secs": round(max_ms / 1000.0, 3) if max_ms is not None else None,
        "avg_all_passed_elapsed_secs": round(float(avg_ms) / 1000.0, 3) if avg_ms is not None else None,
        "approximate_mode_count": approximate_mode_count,
        "note": (
            "若 approximate_mode_count > 0，表示当前现场缺少逐秒收敛 schema 的精确字段；"
            "脚本退化使用旧 schema 的 finalize_elapsed_ms 做近似统计。"
        ),
        "per_bar_rows": per_bar_rows,
    }

    out_path = Path(args.out_json)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print("=== finalize passed stats ===")
    print(f"audit_jsonl: {audit_path}")
    print(f"account: {args.account or ''}")
    print(f"start_c_bar_bj: {start_bj or ''}")
    print(f"end_c_bar_bj: {end_bj or ''}")
    print(f"finalize_record_count: {len(filtered)}")
    print(f"all_passed_round_count: {len(all_passed_rows)}")
    print(f"deadline_count: {deadline_count}")
    print(f"min_all_passed_elapsed_ms: {min_ms}")
    print(f"max_all_passed_elapsed_ms: {max_ms}")
    print(f"avg_all_passed_elapsed_ms: {avg_ms}")
    print(f"approximate_mode_count: {approximate_mode_count}")
    print(f"wrote: {out_path}")


if __name__ == "__main__":
    main()
