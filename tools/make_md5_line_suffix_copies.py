# tools_make_md5_line_suffix_copies.py
import hashlib
import pathlib
import shutil

"""
python3 tools/tools_make_md5_line_suffix_copies.py
"""
FILES = [
    "strategies/run_backtest.py",
    # "=================================",
    "tools/ai_feature_extractor.py",
    "tools/run_full_backtest.sh",
    # "tools/fetch_all.sh",
    # "=================================",
    "core/config_loader.py",
    "core/analysis/analyzer.py",
    "core/analysis/top1_equity_curve.py",
    "core/analysis/visualizer.py",
    "core/engine/broker.py",
    "core/engine/data_feeder.py",
    # "=================================",
    "strategies/snapback/config.json",
    "strategies/snapback/logic.py",
    # "=================================",
    "strategies/top1_hunter/config.json",
    "strategies/top1_hunter/logic.py",
    # "=================================",
    # "alpha_config.json",
    # "alpha_main.py",
    # "alpha_sim.py",
    # "alpha_strategy_reversal/filters.py",
    # "alpha_strategy_reversal/scan.py",
    # "viz/alpha_viz_v1.py",
    # "viz/viz_core.py",
    # "gold_arb.py"
    # "工具/signal_audit_lockonce_v4.py",
    # "工具/trades_side_summary.py",
    # "sim_binance_client.py",
    # "klines_1m_store.py",
    # "top1_hunter/top1_hunter_sim.py",
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
