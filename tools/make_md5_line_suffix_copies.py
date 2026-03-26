# make_md5_line_suffix_copies.py
import hashlib
import pathlib
import shutil

"""
python3 tools/make_md5_line_suffix_copies.py
"""
FILES = [
    # "schedule_backtests.py",
    # "klines_1m_store.py",
    # "PROJECT_IRON_LAWS.md",
    # "audit_24hchg_vs_drop_window.py",
    #"visual_audit_bucketizer_v5.py",
    #"audit_basis_buckets_v1.py",
    # "=================================",
    # "tools/ai_feature_extractor.py",
    # "tools/extract_crime_scene.py",
    # "tools/fetch_all.sh",
    # "tools/run_full_backtest.sh",
    # "tools/audit_extreme_mae.py",
    # "tools/analyze_snap_env_buckets.py",
    # "audit_tools/audit_1m_data_quality_v2.py",
    # "core/analysis/postprocess_backtests.py",
    # "core/analysis/sim_equity_curves.py",
    # "=================================",
    # "core/config_loader.py",
    # "core/analysis/analyzer.py",
    # "core/analysis/top1_equity_curve.py",
    # "core/analysis/visualizer.py",
    # "core/engine/broker.py",
    # "core/engine/data_feeder.py",
    # "=================================",
    # "core/live/binance_client.py",
    # "core/live/binance_exec.py",
    # "core/live/live_state.py",
    # "core/live/audit_log.py",
    # "core/live/market_data.py",
    # "core/live/custom_id.py",
    # "=================================",
    # "strategies/run_backtest.py",
    # "strategies/snapback/run_live.py",
    # "strategies/snapback/logic.py",
    # "strategies/snapback/live_config.json",
    # "strategies/snapback/config.json",
    # "=================================",
    "sim_live_audit/audit_signal_snapshot_diff.py",
    #"sim_live_audit/audit_signal_window_diff.py",
]


def md5_hex(p: pathlib.Path) -> str:
    h = hashlib.md5()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def line_count(p: pathlib.Path) -> int:
    b = p.read_bytes()
    return b.count(b"\n") + 1


def copy_with_suffix(p: pathlib.Path, suffix: str) -> pathlib.Path:
    # insert suffix before extension
    new_name = f"{p.stem}_{suffix}{p.suffix}"
    out = p.with_name(new_name)

    # avoid overwrite: add _v2/_v3...
    if out.exists():
        i = 2
        while True:
            cand = p.with_name(f"{p.stem}_{suffix}_v{i}{p.suffix}")
            if not cand.exists():
                out = cand
                break
            i += 1

    shutil.copy2(p, out)
    return out


for fp in FILES:
    p = pathlib.Path(fp)
    bmd5 = md5_hex(p)
    lines = line_count(p)
    suf = f"{bmd5[-4:]}_{lines}"
    out = copy_with_suffix(p, suf)

    print(fp)
    print("  MD5  :", bmd5)
    print("  Lines:", lines)
    print("  Copy :", str(out))
