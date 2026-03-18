#!/usr/bin/env python3
import argparse
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd


REQUIRED_JOIN_COLS = ["symbol", "entry_time", "exit_time", "reason"]


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                raise SystemExit(f"JSONL 解析失败: {path} line={lineno}: {e}") from e
            if not isinstance(obj, dict):
                raise SystemExit(f"JSONL 行不是对象: {path} line={lineno}")
            rows.append(obj)
    if not rows:
        raise SystemExit(f"输入 trades jsonl 为空: {path}")
    return rows


def _find_key(obj: Dict[str, Any], key: str) -> Any:
    if key in obj:
        return obj.get(key)
    for nest_key in ["features", "context", "signal", "meta", "audit", "payload", "entry_context"]:
        nested = obj.get(nest_key)
        if isinstance(nested, dict) and key in nested:
            return nested.get(key)
    return None


def _norm_time(val: Any) -> str:
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return ""
    try:
        ts = pd.to_datetime(val, utc=True)
    except Exception:
        return str(val)
    return ts.isoformat()


def _to_float(val: Any) -> Optional[float]:
    if val is None or val == "":
        return None
    try:
        x = float(val)
    except Exception:
        return None
    if math.isnan(x):
        return None
    return x


def _build_trade_geometry_rows(trades: Iterable[Dict[str, Any]]) -> pd.DataFrame:
    rows: List[Dict[str, Any]] = []
    for obj in trades:
        symbol = _find_key(obj, "symbol")
        entry_time = _find_key(obj, "entry_time")
        exit_time = _find_key(obj, "exit_time")
        reason = _find_key(obj, "reason")

        row = {
            "symbol": symbol,
            "entry_time": _norm_time(entry_time),
            "exit_time": _norm_time(exit_time),
            "reason": reason,
            "ab_bars": _to_float(_find_key(obj, "ab_bars")),
            "bc_bars": _to_float(_find_key(obj, "bc_bars")),
            "bc_ab_ratio_trade": _to_float(_find_key(obj, "bc_ab_ratio")),
        }
        if not row["bc_ab_ratio_trade"] and row["ab_bars"] not in (None, 0.0) and row["bc_bars"] is not None:
            row["bc_ab_ratio_trade"] = row["bc_bars"] / row["ab_bars"]
        rows.append(row)
    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("trades jsonl 未生成任何 geometry 行")
    return df


def _prepare_csv(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in REQUIRED_JOIN_COLS:
        if col not in out.columns:
            raise SystemExit(f"输入 csv 缺少必需列: {col}")
    out["entry_time"] = pd.to_datetime(out["entry_time"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    out["exit_time"] = pd.to_datetime(out["exit_time"], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    # Insert colon in timezone offset to align with isoformat
    out["entry_time"] = out["entry_time"].str.replace(r"([+-]\d{2})(\d{2})$", r"\1:\2", regex=True)
    out["exit_time"] = out["exit_time"].str.replace(r"([+-]\d{2})(\d{2})$", r"\1:\2", regex=True)
    return out


def _enrich(df_csv: pd.DataFrame, df_geom: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, int]]:
    merged = df_csv.merge(
        df_geom,
        on=REQUIRED_JOIN_COLS,
        how="left",
        validate="many_to_one",
    )
    matched = int(merged["ab_bars"].notna().sum() + merged["bc_bars"].notna().sum() + merged["bc_ab_ratio_trade"].notna().sum() > 0)
    if matched == 0:
        # count joined rows via indicator fallback
        joined = df_csv.merge(df_geom[REQUIRED_JOIN_COLS].drop_duplicates(), on=REQUIRED_JOIN_COLS, how="left", indicator=True)
        matched_rows = int((joined["_merge"] == "both").sum())
    else:
        matched_rows = int(
            merged[["ab_bars", "bc_bars", "bc_ab_ratio_trade"]].notna().any(axis=1).sum()
        )

    if matched_rows == 0:
        raise SystemExit("未匹配到任何 geometry 记录；请检查 join 键和 trades jsonl 字段。")

    if "bc_ab_ratio" in merged.columns:
        merged["bc_ab_ratio_src"] = merged["bc_ab_ratio"]
        merged["bc_ab_ratio"] = merged["bc_ab_ratio"].where(merged["bc_ab_ratio"].notna(), merged["bc_ab_ratio_trade"])
    else:
        merged["bc_ab_ratio"] = merged["bc_ab_ratio_trade"]

    if "bc_ab_ratio_pct" in merged.columns:
        merged["bc_ab_ratio_pct"] = merged["bc_ab_ratio"] * 100.0

    stats = {
        "matched_rows": matched_rows,
        "ab_bars_filled": int(merged["ab_bars"].notna().sum()),
        "bc_bars_filled": int(merged["bc_bars"].notna().sum()),
        "bc_ab_ratio_filled": int(merged["bc_ab_ratio"].notna().sum()),
    }
    return merged, stats


def main() -> None:
    parser = argparse.ArgumentParser(description="从 sim_trades.jsonl 回填 ab_bars / bc_bars / bc_ab_ratio 到审计 csv。")
    parser.add_argument("--trades-jsonl", required=True, help="sim_trades.<RUNID>.jsonl")
    parser.add_argument("--detail-csv", required=True, help="audit_regime...csv 或等价明细表")
    parser.add_argument("--detail-out-csv", required=True, help="回填后的 detail csv")
    parser.add_argument("--summary-json", required=True, help="回填摘要 json")
    parser.add_argument("--tagged-csv", help="可选：audit_samples...tagged.csv")
    parser.add_argument("--tagged-out-csv", help="可选：回填后的 tagged csv")
    args = parser.parse_args()

    trades_path = Path(args.trades_jsonl)
    detail_csv_path = Path(args.detail_csv)
    detail_out_path = Path(args.detail_out_csv)
    summary_path = Path(args.summary_json)
    tagged_csv_path = Path(args.tagged_csv) if args.tagged_csv else None
    tagged_out_path = Path(args.tagged_out_csv) if args.tagged_out_csv else None

    if bool(tagged_csv_path) != bool(tagged_out_path):
        raise SystemExit("tagged-csv 与 tagged-out-csv 必须同时提供或同时不提供。")

    trades = _load_jsonl(trades_path)
    df_geom = _build_trade_geometry_rows(trades)

    geom_presence = {
        "trade_rows": int(len(df_geom)),
        "ab_bars_present": int(df_geom["ab_bars"].notna().sum()),
        "bc_bars_present": int(df_geom["bc_bars"].notna().sum()),
        "bc_ab_ratio_present": int(df_geom["bc_ab_ratio_trade"].notna().sum()),
    }
    if geom_presence["ab_bars_present"] == 0 or geom_presence["bc_bars_present"] == 0:
        raise SystemExit("trades jsonl 中未发现 ab_bars 或 bc_bars，无法回填几何字段。")

    detail_df = pd.read_csv(detail_csv_path)
    detail_prepared = _prepare_csv(detail_df)
    detail_enriched, detail_stats = _enrich(detail_prepared, df_geom)
    detail_out_path.parent.mkdir(parents=True, exist_ok=True)
    detail_enriched.to_csv(detail_out_path, index=False)

    tagged_stats: Dict[str, int] = {}
    if tagged_csv_path and tagged_out_path:
        tagged_df = pd.read_csv(tagged_csv_path)
        tagged_prepared = _prepare_csv(tagged_df)
        tagged_enriched, tagged_stats = _enrich(tagged_prepared, df_geom)
        tagged_out_path.parent.mkdir(parents=True, exist_ok=True)
        tagged_enriched.to_csv(tagged_out_path, index=False)

    summary = {
        "trades_jsonl": str(trades_path),
        "detail_csv": str(detail_csv_path),
        "detail_out_csv": str(detail_out_path),
        "tagged_csv": str(tagged_csv_path) if tagged_csv_path else None,
        "tagged_out_csv": str(tagged_out_path) if tagged_out_path else None,
        "trade_geometry_presence": geom_presence,
        "detail_fill_stats": detail_stats,
        "tagged_fill_stats": tagged_stats,
    }
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=== enrich_geometry_fields 完成 ===")
    print(f"trades jsonl  : {trades_path}")
    print(f"detail csv    : {detail_csv_path}")
    print(f"detail out csv: {detail_out_path}")
    if tagged_csv_path:
        print(f"tagged csv    : {tagged_csv_path}")
        print(f"tagged out csv: {tagged_out_path}")
    print(f"summary json  : {summary_path}")
    print(f"ab_bars rows  : {detail_stats['ab_bars_filled']}")
    print(f"bc_bars rows  : {detail_stats['bc_bars_filled']}")
    print(f"ratio rows    : {detail_stats['bc_ab_ratio_filled']}")


if __name__ == "__main__":
    main()
