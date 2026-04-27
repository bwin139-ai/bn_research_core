#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Audit consistency between data_hub shared bars cache and local data/klines_1m parquet.

Hub source:
- state/live_audit/shared_market/latest_closed_bar.shared.json
- state/live_audit/shared_market/bars/{SYMBOL}.contract.{limit}.shared.json
- state/live_audit/shared_market/bars/{SYMBOL}.index.{limit}.shared.json

Local source:
- data/klines_1m/{SYMBOL}/{YYYY-MM}.parquet

Only closed bars are loaded from hub snapshots.

Audit semantics:
- overlap consistency uses the shared window between hub cache and local sim bars
- hub-only leading tail after local latest bar is recorded as lag info, not mismatch
"""
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

BJ_TZ = "Asia/Shanghai"
FIELD_GROUPS = {
    "contract": ["open", "high", "low", "close", "quote_asset_volume"],
    "index": ["high_idx", "low_idx", "close_idx"],
}
DEFAULT_CONTRACT_LIMIT = 1441
DEFAULT_INDEX_LIMIT = 180


def fmt_bj(ts_ms: int | None) -> str | None:
    if ts_ms is None:
        return None
    return (
        pd.Timestamp(int(ts_ms), unit="ms", tz="UTC")
        .tz_convert(BJ_TZ)
        .strftime("%Y-%m-%d %H:%M:%S")
    )


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_confirmed_delisted_symbols(path: Path) -> set[str]:
    if not path.exists():
        return set()
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit(f"confirmed_delisted file must be a JSON array: {path}")
    out: set[str] = set()
    for raw in data:
        if not isinstance(raw, dict):
            raise SystemExit(f"confirmed_delisted record must be an object: {path}")
        symbol = normalize_symbol(raw.get("symbol", ""))
        if not symbol:
            raise SystemExit(f"confirmed_delisted record missing symbol: {path}")
        out.add(symbol)
    return out


def atomic_write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    tmp.replace(path)


def normalize_symbol(symbol: str) -> str:
    return str(symbol).upper().strip()


def month_keys_for_range(min_ts: int, max_ts: int) -> list[str]:
    cur = pd.Timestamp(min_ts, unit="ms", tz="UTC").to_period("M")
    end = pd.Timestamp(max_ts, unit="ms", tz="UTC").to_period("M")
    out: list[str] = []
    while cur <= end:
        out.append(str(cur))
        cur += 1
    return out


def load_hub_rows(path: Path, latest_closed_bar_ts: int) -> pd.DataFrame:
    payload = read_json(path)
    rows = payload.get("data") or []
    data: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 5:
            continue
        open_time_ms = int(row[0])
        if open_time_ms > latest_closed_bar_ts:
            continue
        rec = {
            "open_time_ms": open_time_ms,
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
        }
        if len(row) > 7:
            rec["quote_asset_volume"] = float(row[7])
        data.append(rec)
    if not data:
        return pd.DataFrame(columns=["open_time_ms", "open", "high", "low", "close", "quote_asset_volume"])
    df = pd.DataFrame(data)
    df = df.sort_values("open_time_ms").drop_duplicates(subset=["open_time_ms"], keep="last").reset_index(drop=True)
    return df


def load_hub_index_rows(path: Path, latest_closed_bar_ts: int) -> pd.DataFrame:
    payload = read_json(path)
    rows = payload.get("data") or []
    data: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 5:
            continue
        open_time_ms = int(row[0])
        if open_time_ms > latest_closed_bar_ts:
            continue
        data.append(
            {
                "open_time_ms": open_time_ms,
                "high_idx": float(row[2]),
                "low_idx": float(row[3]),
                "close_idx": float(row[4]),
            }
        )
    if not data:
        return pd.DataFrame(columns=["open_time_ms", "high_idx", "low_idx", "close_idx"])
    df = pd.DataFrame(data)
    df = df.sort_values("open_time_ms").drop_duplicates(subset=["open_time_ms"], keep="last").reset_index(drop=True)
    return df


def load_local_rows(data_dir: Path, symbol: str, min_ts: int, max_ts: int) -> tuple[pd.DataFrame, list[str]]:
    symbol_dir = data_dir / symbol
    if not symbol_dir.exists():
        return pd.DataFrame(), []
    frames: list[pd.DataFrame] = []
    missing_months: list[str] = []
    for mk in month_keys_for_range(min_ts, max_ts):
        fpath = symbol_dir / f"{mk}.parquet"
        if not fpath.exists():
            missing_months.append(mk)
            continue
        df = pd.read_parquet(fpath)
        needed = ["open_time_ms", "open", "high", "low", "close", "quote_asset_volume"]
        idx_cols = [c for c in ["high_idx", "low_idx", "close_idx"] if c in df.columns]
        cols = [c for c in needed if c in df.columns] + idx_cols
        frames.append(df[cols].copy())
    if not frames:
        return pd.DataFrame(), missing_months
    out = pd.concat(frames, ignore_index=True)
    out = out[(out["open_time_ms"] >= min_ts) & (out["open_time_ms"] <= max_ts)].copy()
    out = out.sort_values("open_time_ms").drop_duplicates(subset=["open_time_ms"], keep="last").reset_index(drop=True)
    return out, missing_months


def _full_window_present(df: pd.DataFrame, columns: list[str]) -> pd.Series:
    if df.empty:
        return pd.Series([], dtype=bool)
    present = pd.Series(True, index=df.index)
    for col in columns:
        if col not in df.columns:
            return pd.Series(False, index=df.index)
        present &= pd.notna(pd.to_numeric(df[col], errors="coerce"))
    return present


@dataclass
class CompareResult:
    summary: dict[str, Any]
    diffs: list[dict[str, Any]]


def compare_symbol(
    *,
    symbol: str,
    latest_closed_bar_ts: int,
    contract_path: Path,
    index_path: Path | None,
    data_dir: Path,
    tolerance_price: float,
    tolerance_volume: float,
) -> CompareResult:
    hub_contract = load_hub_rows(contract_path, latest_closed_bar_ts)
    if hub_contract.empty:
        return CompareResult(
            summary={
                "symbol": symbol,
                "status": "hub_contract_empty",
                "latest_closed_bar_ts": latest_closed_bar_ts,
                "latest_closed_bar_bj": fmt_bj(latest_closed_bar_ts),
            },
            diffs=[],
        )

    hub = hub_contract.copy()
    if index_path is not None and index_path.exists():
        hub_idx = load_hub_index_rows(index_path, latest_closed_bar_ts)
        if not hub_idx.empty:
            hub = hub.merge(hub_idx, on="open_time_ms", how="left")
    hub_min_ts = int(hub["open_time_ms"].min())
    hub_max_ts = int(hub["open_time_ms"].max())

    local, missing_months = load_local_rows(data_dir, symbol, hub_min_ts, hub_max_ts)
    if local.empty:
        return CompareResult(
            summary={
                "symbol": symbol,
                "status": "local_missing",
                "latest_closed_bar_ts": latest_closed_bar_ts,
                "latest_closed_bar_bj": fmt_bj(latest_closed_bar_ts),
                "hub_bar_count": int(len(hub)),
                "local_bar_count": 0,
                "hub_latest_bar_ts": hub_max_ts,
                "hub_latest_bar_bj": fmt_bj(hub_max_ts),
                "missing_months": missing_months,
            },
            diffs=[],
        )

    local_latest_bar_ts = int(local["open_time_ms"].max())
    compare_max_ts = min(hub_max_ts, local_latest_bar_ts)
    hub_leading_mask = hub["open_time_ms"] > compare_max_ts
    hub_leading = hub.loc[hub_leading_mask, "open_time_ms"].tolist()

    if compare_max_ts < hub_min_ts:
        return CompareResult(
            summary={
                "symbol": symbol,
                "status": "no_overlap",
                "latest_closed_bar_ts": latest_closed_bar_ts,
                "latest_closed_bar_bj": fmt_bj(latest_closed_bar_ts),
                "hub_bar_count": int(len(hub)),
                "local_bar_count": int(len(local)),
                "hub_latest_bar_ts": hub_max_ts,
                "hub_latest_bar_bj": fmt_bj(hub_max_ts),
                "local_latest_bar_ts": local_latest_bar_ts,
                "local_latest_bar_bj": fmt_bj(local_latest_bar_ts),
                "compare_max_ts": compare_max_ts,
                "compare_max_bj": fmt_bj(compare_max_ts),
                "hub_leading_bar_count": int(len(hub_leading)),
                "hub_leading_first_ts": int(hub_leading[0]) if hub_leading else None,
                "hub_leading_first_bj": fmt_bj(int(hub_leading[0])) if hub_leading else None,
                "hub_leading_last_ts": int(hub_leading[-1]) if hub_leading else None,
                "hub_leading_last_bj": fmt_bj(int(hub_leading[-1])) if hub_leading else None,
                "missing_months": missing_months,
            },
            diffs=[],
        )

    hub_overlap = hub.loc[~hub_leading_mask].copy()
    local_overlap = local.loc[local["open_time_ms"] <= compare_max_ts].copy()

    merged = hub_overlap.merge(
        local_overlap,
        on="open_time_ms",
        how="outer",
        suffixes=("_hub", "_local"),
        indicator=True,
    )
    diffs: list[dict[str, Any]] = []

    missing_in_local = merged[merged["_merge"] == "left_only"]["open_time_ms"].tolist()
    missing_in_hub = merged[merged["_merge"] == "right_only"]["open_time_ms"].tolist()

    max_abs_by_field: dict[str, float] = {}
    mismatch_count_by_field: dict[str, int] = {}
    compared_bar_count = 0
    contract_compared_bar_count = 0
    idx_compared_bar_count = 0

    both = merged[merged["_merge"] == "both"].copy()
    if not both.empty:
        contract_compared_bar_count = int(len(both))
        compared_bar_count = contract_compared_bar_count

        for field in FIELD_GROUPS["contract"]:
            hub_col = f"{field}_hub"
            local_col = f"{field}_local"
            if hub_col not in both.columns or local_col not in both.columns:
                continue

            hub_s = pd.to_numeric(both[hub_col], errors="coerce")
            local_s = pd.to_numeric(both[local_col], errors="coerce")
            tol = tolerance_volume if field == "quote_asset_volume" else tolerance_price

            if hub_s.notna().sum() == 0 and local_s.notna().sum() == 0:
                continue

            diff_s = (hub_s - local_s).abs()
            mismatch_mask = (
                (hub_s.isna() ^ local_s.isna())
                | ((hub_s.notna()) & (local_s.notna()) & (diff_s > tol))
            )
            mismatch_rows = both.loc[mismatch_mask, ["open_time_ms", hub_col, local_col]]
            mismatch_count_by_field[field] = int(len(mismatch_rows))
            if diff_s.notna().any():
                max_abs_by_field[field] = float(diff_s.max())

            for _, row in mismatch_rows.iterrows():
                diffs.append(
                    {
                        "symbol": symbol,
                        "open_time_ms": int(row["open_time_ms"]),
                        "open_time_bj": fmt_bj(int(row["open_time_ms"])),
                        "field": field,
                        "hub_value": None if pd.isna(row[hub_col]) else float(row[hub_col]),
                        "local_value": None if pd.isna(row[local_col]) else float(row[local_col]),
                        "abs_diff": None if pd.isna(row[hub_col]) or pd.isna(row[local_col]) else abs(float(row[hub_col]) - float(row[local_col])),
                    }
                )

        hub_idx_present = _full_window_present(both, [f"{field}_hub" for field in FIELD_GROUPS["index"]])
        local_idx_present = _full_window_present(both, [f"{field}_local" for field in FIELD_GROUPS["index"]])
        idx_both = both.loc[hub_idx_present & local_idx_present].copy()
        idx_compared_bar_count = int(len(idx_both))
        compared_bar_count = max(contract_compared_bar_count, idx_compared_bar_count)

        if not idx_both.empty:
            for field in FIELD_GROUPS["index"]:
                hub_col = f"{field}_hub"
                local_col = f"{field}_local"
                if hub_col not in idx_both.columns or local_col not in idx_both.columns:
                    continue

                hub_s = pd.to_numeric(idx_both[hub_col], errors="coerce")
                local_s = pd.to_numeric(idx_both[local_col], errors="coerce")
                diff_s = (hub_s - local_s).abs()
                mismatch_mask = (
                    (hub_s.isna() ^ local_s.isna())
                    | ((hub_s.notna()) & (local_s.notna()) & (diff_s > tolerance_price))
                )
                mismatch_rows = idx_both.loc[mismatch_mask, ["open_time_ms", hub_col, local_col]]
                mismatch_count_by_field[field] = int(len(mismatch_rows))
                if diff_s.notna().any():
                    max_abs_by_field[field] = float(diff_s.max())

                for _, row in mismatch_rows.iterrows():
                    diffs.append(
                        {
                            "symbol": symbol,
                            "open_time_ms": int(row["open_time_ms"]),
                            "open_time_bj": fmt_bj(int(row["open_time_ms"])),
                            "field": field,
                            "hub_value": None if pd.isna(row[hub_col]) else float(row[hub_col]),
                            "local_value": None if pd.isna(row[local_col]) else float(row[local_col]),
                            "abs_diff": None if pd.isna(row[hub_col]) or pd.isna(row[local_col]) else abs(float(row[hub_col]) - float(row[local_col])),
                        }
                    )

    for ts in missing_in_local:
        diffs.append({
            "symbol": symbol,
            "open_time_ms": int(ts),
            "open_time_bj": fmt_bj(int(ts)),
            "field": "_row_presence",
            "hub_value": "present",
            "local_value": "missing",
            "abs_diff": None,
        })
    for ts in missing_in_hub:
        diffs.append({
            "symbol": symbol,
            "open_time_ms": int(ts),
            "open_time_bj": fmt_bj(int(ts)),
            "field": "_row_presence",
            "hub_value": "missing",
            "local_value": "present",
            "abs_diff": None,
        })

    summary = {
        "symbol": symbol,
        "status": "ok" if not diffs else "mismatch",
        "latest_closed_bar_ts": latest_closed_bar_ts,
        "latest_closed_bar_bj": fmt_bj(latest_closed_bar_ts),
        "hub_bar_count": int(len(hub)),
        "local_bar_count": int(len(local)),
        "hub_overlap_bar_count": int(len(hub_overlap)),
        "local_overlap_bar_count": int(len(local_overlap)),
        "compared_bar_count": int(compared_bar_count),
        "contract_compared_bar_count": int(contract_compared_bar_count),
        "idx_compared_bar_count": int(idx_compared_bar_count),
        "hub_latest_bar_ts": hub_max_ts,
        "hub_latest_bar_bj": fmt_bj(hub_max_ts),
        "local_latest_bar_ts": local_latest_bar_ts,
        "local_latest_bar_bj": fmt_bj(local_latest_bar_ts),
        "compare_max_ts": compare_max_ts,
        "compare_max_bj": fmt_bj(compare_max_ts),
        "hub_leading_bar_count": int(len(hub_leading)),
        "hub_leading_first_ts": int(hub_leading[0]) if hub_leading else None,
        "hub_leading_first_bj": fmt_bj(int(hub_leading[0])) if hub_leading else None,
        "hub_leading_last_ts": int(hub_leading[-1]) if hub_leading else None,
        "hub_leading_last_bj": fmt_bj(int(hub_leading[-1])) if hub_leading else None,
        "missing_in_local_count": int(len(missing_in_local)),
        "missing_in_hub_count": int(len(missing_in_hub)),
        "missing_months": missing_months,
        "max_abs_by_field": max_abs_by_field,
        "mismatch_count_by_field": mismatch_count_by_field,
    }
    return CompareResult(summary=summary, diffs=diffs)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Audit data_hub shared bars vs data/klines_1m parquet")
    ap.add_argument("--project-root", default=".", help="repo root")
    ap.add_argument("--symbols", default="", help="comma-separated symbols")
    ap.add_argument("--use-finalized-symbols", action="store_true", help="use current finalized passed_symbols")
    ap.add_argument("--contract-limit", type=int, default=DEFAULT_CONTRACT_LIMIT, help="hub shared contract cache limit")
    ap.add_argument("--index-limit", type=int, default=DEFAULT_INDEX_LIMIT, help="hub shared index cache limit")
    ap.add_argument("--tolerance-price", type=float, default=1e-9)
    ap.add_argument("--tolerance-volume", type=float, default=1e-6)
    ap.add_argument(
        "--confirmed-delisted-path",
        default="state/confirmed_delisted_symbols.json",
        help="confirmed delisted symbols JSON file; these symbols are skipped by default",
    )
    ap.add_argument(
        "--include-confirmed-delisted",
        action="store_true",
        help="include confirmed delisted symbols in the audit set",
    )
    ap.add_argument("--out-dir", default="output/hub_vs_klines_1m_audit")
    ap.add_argument("--run-id", default="")
    return ap.parse_args()


def resolve_symbols(args: argparse.Namespace, project_root: Path) -> list[str]:
    manual = [normalize_symbol(x) for x in args.symbols.split(",") if x.strip()]
    if manual:
        return sorted(dict.fromkeys(manual))
    if args.use_finalized_symbols:
        fpath = project_root / "state/live_audit/market_data_hub/shared/current/finalized_candidate_inputs.json"
        data = read_json(fpath)
        fs = data.get("finalize_summary") or {}
        symbols = [normalize_symbol(x) for x in (fs.get("passed_symbols") or [])]
        if symbols:
            return sorted(dict.fromkeys(symbols))
    raise SystemExit("must provide --symbols or --use-finalized-symbols")


def main() -> None:
    args = parse_args()
    project_root = Path(args.project_root).resolve()
    state_root = project_root / "state/live_audit/shared_market"
    latest_closed = read_json(state_root / "latest_closed_bar.shared.json")
    latest_closed_bar_ts = int(latest_closed["latest_closed_bar_ts"])

    symbols = resolve_symbols(args, project_root)
    if not args.include_confirmed_delisted:
        confirmed_delisted = load_confirmed_delisted_symbols(project_root / args.confirmed_delisted_path)
        symbols = [symbol for symbol in symbols if symbol not in confirmed_delisted]
    run_id = args.run_id or pd.Timestamp.utcnow().strftime("RUN_%Y%m%dT%H%M%SZ")
    out_dir = project_root / args.out_dir / run_id
    data_dir = project_root / "data/klines_1m"
    bars_dir = state_root / "bars"

    summaries: list[dict[str, Any]] = []
    diffs: list[dict[str, Any]] = []

    for symbol in symbols:
        contract_path = bars_dir / f"{symbol}.contract.{int(args.contract_limit)}.shared.json"
        index_path = bars_dir / f"{symbol}.index.{int(args.index_limit)}.shared.json"
        if not contract_path.exists():
            summaries.append({
                "symbol": symbol,
                "status": "hub_contract_snapshot_missing",
                "contract_snapshot_path": str(contract_path),
                "latest_closed_bar_ts": latest_closed_bar_ts,
                "latest_closed_bar_bj": fmt_bj(latest_closed_bar_ts),
            })
            continue
        res = compare_symbol(
            symbol=symbol,
            latest_closed_bar_ts=latest_closed_bar_ts,
            contract_path=contract_path,
            index_path=index_path if index_path.exists() else None,
            data_dir=data_dir,
            tolerance_price=float(args.tolerance_price),
            tolerance_volume=float(args.tolerance_volume),
        )
        summaries.append(res.summary)
        diffs.extend(res.diffs)

    atomic_write_jsonl(out_dir / "summary.jsonl", summaries)
    atomic_write_jsonl(out_dir / "diff.jsonl", diffs)

    total = len(summaries)
    mismatch = sum(1 for row in summaries if row.get("status") == "mismatch")
    ok = sum(1 for row in summaries if row.get("status") == "ok")
    no_overlap = sum(1 for row in summaries if row.get("status") == "no_overlap")
    hub_snapshot_missing = sum(1 for row in summaries if row.get("status") == "hub_contract_snapshot_missing")
    local_missing = sum(1 for row in summaries if row.get("status") == "local_missing")
    hub_leading_symbols = sum(1 for row in summaries if int(row.get("hub_leading_bar_count") or 0) > 0)
    hub_leading_rows = sum(int(row.get("hub_leading_bar_count") or 0) for row in summaries)
    print("=== audit_hub_vs_klines_1m_v1 完成 ===")
    print(f"symbols_total          : {total}")
    print(f"symbols_ok             : {ok}")
    print(f"symbols_mismatch       : {mismatch}")
    print(f"symbols_no_overlap     : {no_overlap}")
    print(f"symbols_local_missing  : {local_missing}")
    print(f"symbols_hub_missing    : {hub_snapshot_missing}")
    print(f"symbols_hub_leading    : {hub_leading_symbols}")
    print(f"hub_leading_rows       : {hub_leading_rows}")
    print(f"diff_rows              : {len(diffs)}")
    print(f"latest_closed_bar_bj   : {fmt_bj(latest_closed_bar_ts)}")
    print(f"out_dir                : {out_dir}")


if __name__ == "__main__":
    main()
