#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

BJ_TZ_OFFSET = "+08:00"


@dataclass(frozen=True)
class Finding:
    kind: str
    severity: str
    asset: str
    detail: dict[str, Any]


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _history_root(root: Path, account: str) -> Path:
    return root / "state" / "exchange_history" / account


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            text = line.strip()
            if not text:
                continue
            row = json.loads(text)
            if not isinstance(row, dict):
                raise ValueError(f"JSONL row must be object: {path}")
            rows.append(row)
    return rows


def _load_source_rows(history_root: Path, source: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    source_dir = history_root / source
    for path in sorted(source_dir.glob("*.jsonl")):
        rows.extend(_load_jsonl(path))
    return rows


def _decimal(value: Any, *, context: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid decimal: {context} value={value!r}") from exc


def _int(value: Any, *, context: str) -> int:
    try:
        return int(value)
    except Exception as exc:
        raise ValueError(f"invalid integer: {context} value={value!r}") from exc


def _quantize_text(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return format(normalized, "f")
    return format(normalized, "f")


def _row_raw(row: dict[str, Any]) -> dict[str, Any]:
    raw = row.get("raw")
    if not isinstance(raw, dict):
        raise ValueError(f"row.raw must be object: source={row.get('source')}")
    return raw


def _filter_time(rows: list[dict[str, Any]], start_ms: int | None, end_ms: int | None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        event_ms = _int(row.get("event_time_ms"), context="event_time_ms")
        if start_ms is not None and event_ms < start_ms:
            continue
        if end_ms is not None and event_ms > end_ms:
            continue
        out.append(row)
    return out


def _bj_to_ms(text: str) -> int:
    from datetime import datetime, timezone

    value = str(text or "").strip()
    if not value:
        raise ValueError("empty BJ time")
    parsed = datetime.fromisoformat(value.replace(" ", "T") + BJ_TZ_OFFSET)
    return int(parsed.astimezone(timezone.utc).timestamp() * 1000)


def _income_sum_by_asset_between(
    income_rows: list[dict[str, Any]],
    *,
    asset: str,
    start_exclusive_ms: int,
    end_inclusive_ms: int,
) -> Decimal:
    total = Decimal("0")
    for row in income_rows:
        raw = _row_raw(row)
        row_asset = str(raw.get("asset") or row.get("asset") or "").upper().strip()
        if row_asset != asset:
            continue
        event_ms = _int(row.get("event_time_ms"), context="income.event_time_ms")
        if start_exclusive_ms < event_ms <= end_inclusive_ms:
            total += _decimal(raw.get("income"), context=f"income {asset}")
    return total


def _audit_balance_continuity(
    balance_rows: list[dict[str, Any]],
    income_rows: list[dict[str, Any]],
    *,
    tolerance: Decimal,
) -> tuple[dict[str, Any], list[Finding]]:
    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in balance_rows:
        raw = _row_raw(row)
        asset = str(raw.get("asset") or row.get("asset") or "").upper().strip()
        if not asset:
            raise ValueError("balance snapshot asset missing")
        by_asset[asset].append(row)

    findings: list[Finding] = []
    checked_intervals = 0
    for asset, rows in sorted(by_asset.items()):
        ordered = sorted(rows, key=lambda row: _int(row.get("event_time_ms"), context="balance.event_time_ms"))
        if len(ordered) < 2:
            findings.append(
                Finding(
                    kind="insufficient_balance_snapshots",
                    severity="warning",
                    asset=asset,
                    detail={"snapshot_count": len(ordered)},
                )
            )
            continue
        for prev, curr in zip(ordered, ordered[1:]):
            prev_raw = _row_raw(prev)
            curr_raw = _row_raw(curr)
            start_ms = _int(prev.get("event_time_ms"), context="balance.start")
            end_ms = _int(curr.get("event_time_ms"), context="balance.end")
            if end_ms <= start_ms:
                raise ValueError(f"balance snapshots not increasing: asset={asset}")
            start_wallet = _decimal(prev_raw.get("wallet_balance"), context=f"wallet start {asset}")
            end_wallet = _decimal(curr_raw.get("wallet_balance"), context=f"wallet end {asset}")
            income_sum = _income_sum_by_asset_between(
                income_rows,
                asset=asset,
                start_exclusive_ms=start_ms,
                end_inclusive_ms=end_ms,
            )
            expected = start_wallet + income_sum
            diff = end_wallet - expected
            checked_intervals += 1
            if abs(diff) > tolerance:
                findings.append(
                    Finding(
                        kind="balance_continuity_mismatch",
                        severity="error",
                        asset=asset,
                        detail={
                            "start_ms": start_ms,
                            "end_ms": end_ms,
                            "start_wallet": _quantize_text(start_wallet),
                            "end_wallet": _quantize_text(end_wallet),
                            "income_sum": _quantize_text(income_sum),
                            "expected_end_wallet": _quantize_text(expected),
                            "diff": _quantize_text(diff),
                        },
                    )
                )
    summary = {
        "assets": sorted(by_asset.keys()),
        "balance_snapshot_rows": len(balance_rows),
        "balance_intervals_checked": checked_intervals,
    }
    return summary, findings


def _sum_income_by_type_asset(income_rows: list[dict[str, Any]], income_type: str) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    wanted = income_type.upper()
    for row in income_rows:
        raw = _row_raw(row)
        if str(raw.get("income_type") or "").upper().strip() != wanted:
            continue
        asset = str(raw.get("asset") or row.get("asset") or "").upper().strip()
        if not asset:
            raise ValueError(f"income asset missing: type={wanted}")
        totals[asset] += _decimal(raw.get("income"), context=f"income {wanted} {asset}")
    return dict(totals)


def _sum_trades_realized_by_asset(trade_rows: list[dict[str, Any]]) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for row in trade_rows:
        raw = _row_raw(row)
        asset = "USDT"
        totals[asset] += _decimal(raw.get("realized_pnl"), context="trade realized_pnl")
    return dict(totals)


def _sum_trades_commission_by_asset(trade_rows: list[dict[str, Any]]) -> dict[str, Decimal]:
    totals: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for row in trade_rows:
        raw = _row_raw(row)
        asset = str(raw.get("commission_asset") or "").upper().strip()
        if not asset:
            raise ValueError("trade commission_asset missing")
        totals[asset] += _decimal(raw.get("commission"), context=f"trade commission {asset}")
    return dict(totals)


def _compare_totals(
    *,
    kind: str,
    expected_by_asset: dict[str, Decimal],
    actual_by_asset: dict[str, Decimal],
    tolerance: Decimal,
) -> tuple[dict[str, Any], list[Finding]]:
    assets = sorted(set(expected_by_asset) | set(actual_by_asset))
    findings: list[Finding] = []
    rows: dict[str, Any] = {}
    for asset in assets:
        expected = expected_by_asset.get(asset, Decimal("0"))
        actual = actual_by_asset.get(asset, Decimal("0"))
        diff = actual - expected
        rows[asset] = {
            "expected": _quantize_text(expected),
            "actual": _quantize_text(actual),
            "diff": _quantize_text(diff),
        }
        if abs(diff) > tolerance:
            findings.append(
                Finding(
                    kind=f"{kind}_mismatch",
                    severity="error",
                    asset=asset,
                    detail=rows[asset],
                )
            )
    return rows, findings


def audit(history_root: Path, *, start_ms: int | None, end_ms: int | None, tolerance: Decimal) -> dict[str, Any]:
    balance_rows = _filter_time(_load_source_rows(history_root, "balance_snapshots"), start_ms, end_ms)
    income_rows = _filter_time(_load_source_rows(history_root, "income"), start_ms, end_ms)
    trade_rows = _filter_time(_load_source_rows(history_root, "trades"), start_ms, end_ms)

    balance_summary, findings = _audit_balance_continuity(balance_rows, income_rows, tolerance=tolerance)

    income_realized = _sum_income_by_type_asset(income_rows, "REALIZED_PNL")
    trade_realized = _sum_trades_realized_by_asset(trade_rows)
    realized_summary, realized_findings = _compare_totals(
        kind="realized_pnl_cross_check",
        expected_by_asset=income_realized,
        actual_by_asset=trade_realized,
        tolerance=tolerance,
    )
    findings.extend(realized_findings)

    income_commission = _sum_income_by_type_asset(income_rows, "COMMISSION")
    trade_commission = _sum_trades_commission_by_asset(trade_rows)
    expected_commission = {asset: -value for asset, value in income_commission.items()}
    commission_summary, commission_findings = _compare_totals(
        kind="commission_cross_check",
        expected_by_asset=expected_commission,
        actual_by_asset=trade_commission,
        tolerance=tolerance,
    )
    findings.extend(commission_findings)

    error_count = sum(1 for row in findings if row.severity == "error")
    warning_count = sum(1 for row in findings if row.severity == "warning")
    return {
        "ok": error_count == 0,
        "history_root": str(history_root),
        "rows": {
            "balance_snapshots": len(balance_rows),
            "income": len(income_rows),
            "trades": len(trade_rows),
        },
        "balance_continuity": balance_summary,
        "cross_checks": {
            "realized_pnl": realized_summary,
            "commission": commission_summary,
        },
        "finding_counts": {"error": error_count, "warning": warning_count},
        "findings": [asdict(row) for row in findings],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit exchange_history balance continuity and trade/income cross-checks.")
    parser.add_argument("--account", required=True)
    parser.add_argument("--root", default="", help="Repository root; default is inferred from this script")
    parser.add_argument("--history-root", default="", help="Explicit state/exchange_history/{account} path")
    parser.add_argument("--start-bj", default="", help="Optional inclusive BJ time, e.g. 2026-05-21 00:00:00")
    parser.add_argument("--end-bj", default="", help="Optional inclusive BJ time, e.g. 2026-05-21 23:59:59")
    parser.add_argument("--tolerance", default="0.00000001")
    parser.add_argument("--pretty", action="store_true")
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve() if args.root else _repo_root()
    history_root = Path(args.history_root).expanduser().resolve() if args.history_root else _history_root(root, args.account)
    if not history_root.exists():
        raise FileNotFoundError(f"exchange history root missing: {history_root}")
    start_ms = _bj_to_ms(args.start_bj) if args.start_bj else None
    end_ms = _bj_to_ms(args.end_bj) if args.end_bj else None
    tolerance = _decimal(args.tolerance, context="tolerance")
    result = audit(history_root, start_ms=start_ms, end_ms=end_ms, tolerance=tolerance)
    print(json.dumps(result, ensure_ascii=False, indent=2 if args.pretty else None, sort_keys=True))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
