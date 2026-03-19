
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
visual_audit_bucketizer.py
"""
import argparse, csv, json, re, shutil
from collections import defaultdict
from pathlib import Path
from statistics import median

AB_BUCKETS = [
    ("le2", "ab_le_2", lambda x: x <= 2),
    ("2_4", "ab_2_4", lambda x: 2 < x <= 4),
    ("4_6", "ab_4_6", lambda x: 4 < x <= 6),
    ("6_10", "ab_6_10", lambda x: 6 < x <= 10),
    ("gt10", "ab_gt_10", lambda x: x > 10),
]
REB_BUCKETS = [
    ("le015", "reb_le_0.15", lambda x: x <= 0.15),
    ("015_030", "reb_0.15_0.30", lambda x: 0.15 < x <= 0.30),
    ("030_050", "reb_0.30_0.50", lambda x: 0.30 < x <= 0.50),
    ("050_070", "reb_0.50_0.70", lambda x: 0.50 < x <= 0.70),
    ("gt070", "reb_gt_0.70", lambda x: x > 0.70),
]
RE_DIGITS = re.compile(r'(\d{10,16})')

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--png-dir", required=True)
    p.add_argument("--trades-jsonl", required=True)
    p.add_argument("--out-dir", required=True)
    p.add_argument("--run-id", default="")
    p.add_argument("--copy-mode", choices=["copy", "symlink"], default="copy")
    return p.parse_args()

def read_jsonl(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            rows.append(json.loads(line))
    if not rows:
        raise RuntimeError("trades-jsonl 为空。")
    return rows

def require(d, key, ctx):
    if key not in d:
        raise RuntimeError(f"{ctx} 缺少字段 {key}")
    return d[key]

def to_float(v, field, ctx):
    try:
        return float(v)
    except Exception:
        raise RuntimeError(f"{ctx} 字段 {field} 无法转为 float: {v}")

def to_int(v, field, ctx):
    try:
        return int(v)
    except Exception:
        raise RuntimeError(f"{ctx} 字段 {field} 无法转为 int: {v}")

def bucketize(x, buckets, field_name, ctx):
    for code, label, fn in buckets:
        if fn(x):
            return code, label
    raise RuntimeError(f"{ctx} 字段 {field_name} 未命中任何桶: {x}")

def pnl_bucket(pnl):
    if pnl > 0:
        return "profit"
    if pnl < 0:
        return "loss"
    return "flat"

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def extract_times_from_name(name: str):
    return set(RE_DIGITS.findall(name))

def build_trade_rows(trades):
    enriched = []
    index_by_signal = defaultdict(list)
    index_by_entry = defaultdict(list)
    for i, tr in enumerate(trades):
        ctx = tr.get("context")
        if not isinstance(ctx, dict):
            raise RuntimeError(f"第 {i+1} 条 trade 缺少 context")
        symbol = require(tr, "symbol", f"trade[{i}]")
        signal_time = to_int(require(tr, "signal_time", f"trade[{i}]"), "signal_time", f"trade[{i}]")
        entry_time = to_int(require(tr, "entry_time", f"trade[{i}]"), "entry_time", f"trade[{i}]")
        pnl_pct = to_float(require(tr, "pnl_pct", f"trade[{i}]"), "pnl_pct", f"trade[{i}]")
        reason = tr.get("reason", "")

        a_time = to_int(require(ctx, "a_time", f"trade[{i}].context"), "a_time", f"trade[{i}].context")
        b_time = to_int(require(ctx, "b_time", f"trade[{i}].context"), "b_time", f"trade[{i}].context")
        c_time = to_int(require(ctx, "c_time", f"trade[{i}].context"), "c_time", f"trade[{i}].context")
        if not (a_time <= b_time < c_time):
            raise RuntimeError(f"第 {i+1} 条 trade 时间顺序错误: a_time={a_time}, b_time={b_time}, c_time={c_time}")
        if ((b_time-a_time)%60000)!=0 or ((c_time-b_time)%60000)!=0:
            raise RuntimeError(f"第 {i+1} 条 trade 时间差不是整 1m bar: a_time={a_time}, b_time={b_time}, c_time={c_time}")
        ab_bars = (b_time-a_time)//60000
        bc_bars = (c_time-b_time)//60000
        bc_ab_ratio = "" if ab_bars == 0 else (bc_bars/ab_bars)

        rebound_ratio = to_float(require(ctx, "rebound_ratio", f"trade[{i}].context"), "rebound_ratio", f"trade[{i}].context")
        drop_window_chg = to_float(require(ctx, "drop_window_chg", f"trade[{i}].context"), "drop_window_chg", f"trade[{i}].context")
        drop_pct = to_float(require(ctx, "drop_pct", f"trade[{i}].context"), "drop_pct", f"trade[{i}].context")

        ab_code, ab_label = bucketize(ab_bars, AB_BUCKETS, "ab_bars", f"trade[{i}]")
        reb_code, reb_label = bucketize(rebound_ratio, REB_BUCKETS, "rebound_ratio", f"trade[{i}]")
        cluster_id = f"{ab_code}__{reb_code}"
        cluster_name = f"{ab_label}__{reb_label}"

        row = {
            "symbol": symbol,
            "signal_time": signal_time,
            "entry_time": entry_time,
            "pnl_pct": pnl_pct,
            "reason": reason,
            "drop_window_chg": drop_window_chg,
            "drop_pct": drop_pct,
            "rebound_ratio": rebound_ratio,
            "ab_bars": ab_bars,
            "bc_bars": bc_bars,
            "bc_ab_ratio": bc_ab_ratio,
            "cluster_id": cluster_id,
            "cluster_name": cluster_name,
            "pnl_bucket": pnl_bucket(pnl_pct),
            "source_trade_index": i,
        }
        enriched.append(row)
        index_by_signal[(symbol.upper(), str(signal_time))].append(row)
        index_by_entry[(symbol.upper(), str(entry_time))].append(row)
    return enriched, index_by_signal, index_by_entry

def match_pngs(png_dir: Path, index_by_signal, index_by_entry):
    matched = []
    unmatched_png = []
    used_trade_ids = set()
    png_files = sorted([p for p in png_dir.rglob("*") if p.is_file() and p.suffix.lower() in {".png",".jpg",".jpeg",".webp"}])
    if not png_files:
        raise RuntimeError("png-dir 下未发现图片文件。")
    for p in png_files:
        name = p.name
        times = extract_times_from_name(name)
        symbol_hits = re.findall(r'([A-Z0-9]+USDT)', name.upper())
        symbol_hits = list(dict.fromkeys(symbol_hits))
        found_rows = []
        for sym in symbol_hits:
            for t in times:
                found_rows.extend(index_by_signal.get((sym, t), []))
        if not found_rows:
            for sym in symbol_hits:
                for t in times:
                    found_rows.extend(index_by_entry.get((sym, t), []))
        if len(found_rows) == 1:
            row = found_rows[0]
            matched.append((p, row))
            used_trade_ids.add(row["source_trade_index"])
        elif len(found_rows) == 0:
            unmatched_png.append({"png_filename": name, "png_path": str(p), "reason": "no_match"})
        else:
            unmatched_png.append({"png_filename": name, "png_path": str(p), "reason": "multi_match"})
    return matched, unmatched_png, used_trade_ids

def write_csv(path: Path, rows, fieldnames):
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

def main():
    args = parse_args()
    png_dir = Path(args.png_dir)
    trades_jsonl = Path(args.trades_jsonl)
    out_dir = Path(args.out_dir)
    if not png_dir.exists():
        raise RuntimeError(f"png-dir 不存在: {png_dir}")
    if not trades_jsonl.exists():
        raise RuntimeError(f"trades-jsonl 不存在: {trades_jsonl}")

    trades = read_jsonl(trades_jsonl)
    trade_rows, index_by_signal, index_by_entry = build_trade_rows(trades)
    matched, unmatched_png, used_trade_ids = match_pngs(png_dir, index_by_signal, index_by_entry)

    ensure_dir(out_dir)
    reports_dir = out_dir / "reports"
    unmatched_dir = out_dir / "unmatched_png"
    ensure_dir(reports_dir)
    ensure_dir(unmatched_dir)

    cluster_index_rows = []
    cluster_map = defaultdict(list)

    for src, row in matched:
        cluster_path = out_dir / row["cluster_id"] / row["pnl_bucket"]
        ensure_dir(cluster_path)
        dst = cluster_path / src.name
        if args.copy_mode == "copy":
            shutil.copy2(src, dst)
        else:
            if dst.exists() or dst.is_symlink():
                dst.unlink()
            dst.symlink_to(src.resolve())
        rec = dict(row)
        rec["png_filename"] = src.name
        rec["png_src_path"] = str(src)
        rec["bucket_path"] = str(cluster_path)
        cluster_index_rows.append(rec)
        cluster_map[row["cluster_id"]].append(rec)

    for r in unmatched_png:
        src = Path(r["png_path"])
        dst = unmatched_dir / src.name
        if args.copy_mode == "copy":
            shutil.copy2(src, dst)
        else:
            if dst.exists() or dst.is_symlink():
                dst.unlink()
            dst.symlink_to(src.resolve())

    unmatched_trades = [r for r in trade_rows if r["source_trade_index"] not in used_trade_ids]

    cluster_summary_rows = []
    for cluster_id, rows in sorted(cluster_map.items()):
        pnls = [r["pnl_pct"] for r in rows]
        def med(field):
            vals = [r[field] for r in rows if r[field] != "" and r[field] is not None]
            return median(vals) if vals else ""
        cluster_summary_rows.append({
            "cluster_id": cluster_id,
            "cluster_name": rows[0]["cluster_name"],
            "count": len(rows),
            "profit_count": sum(r["pnl_bucket"]=="profit" for r in rows),
            "loss_count": sum(r["pnl_bucket"]=="loss" for r in rows),
            "flat_count": sum(r["pnl_bucket"]=="flat" for r in rows),
            "avg_pnl_pct": (sum(pnls)/len(pnls)) if pnls else "",
            "median_pnl_pct": median(pnls) if pnls else "",
            "ab_bars_median": med("ab_bars"),
            "bc_bars_median": med("bc_bars"),
            "bc_ab_ratio_median": med("bc_ab_ratio"),
            "drop_pct_median": med("drop_pct"),
            "rebound_ratio_median": med("rebound_ratio"),
            "drop_window_chg_median": med("drop_window_chg"),
        })

    write_csv(reports_dir/"cluster_index.csv", cluster_index_rows, [
        "cluster_id","cluster_name","bucket_path","png_filename","png_src_path",
        "symbol","signal_time","entry_time","pnl_pct","reason","pnl_bucket",
        "ab_bars","bc_bars","bc_ab_ratio","drop_pct","rebound_ratio","drop_window_chg",
    ])
    write_csv(reports_dir/"cluster_summary.csv", cluster_summary_rows, [
        "cluster_id","cluster_name","count","profit_count","loss_count","flat_count",
        "avg_pnl_pct","median_pnl_pct","ab_bars_median","bc_bars_median",
        "bc_ab_ratio_median","drop_pct_median","rebound_ratio_median","drop_window_chg_median",
    ])
    write_csv(reports_dir/"unmatched_png.csv", unmatched_png, ["png_filename","png_path","reason"])
    write_csv(reports_dir/"unmatched_trades.csv", unmatched_trades, [
        "symbol","signal_time","entry_time","pnl_pct","reason","pnl_bucket",
        "ab_bars","bc_bars","bc_ab_ratio","drop_pct","rebound_ratio","drop_window_chg","cluster_id","cluster_name","source_trade_index",
    ])
    summary = {
        "run_id": args.run_id,
        "png_dir": str(png_dir),
        "trades_jsonl": str(trades_jsonl),
        "out_dir": str(out_dir),
        "copy_mode": args.copy_mode,
        "png_total": len([p for p in png_dir.rglob("*") if p.is_file() and p.suffix.lower() in {".png",".jpg",".jpeg",".webp"}]),
        "trade_total": len(trade_rows),
        "matched_png": len(matched),
        "unmatched_png": len(unmatched_png),
        "unmatched_trades": len(unmatched_trades),
        "cluster_count": len(cluster_map),
    }
    with (reports_dir/"summary.json").open("w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    print("=== visual_audit_bucketizer 完成 ===")
    print(f"run_id         : {args.run_id}")
    print(f"png dir        : {png_dir}")
    print(f"trades jsonl   : {trades_jsonl}")
    print(f"out dir        : {out_dir}")
    print(f"matched png    : {len(matched)}")
    print(f"unmatched png  : {len(unmatched_png)}")
    print(f"unmatched trade: {len(unmatched_trades)}")
    print(f"clusters       : {len(cluster_map)}")

if __name__ == "__main__":
    main()
