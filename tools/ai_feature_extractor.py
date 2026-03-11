import argparse
import glob
import json
import os
import sys

import pandas as pd
import pyarrow.parquet as pq


TOP1_OPTIONAL_CONTEXT_FIELDS = [
    ("mDD_15m", "mDD_15m(%)", "pct"),
    ("mDD_120m", "mDD_120m(%)", "pct"),
    ("micro_drawdown", "mDD_old(%)", "pct"),
    ("micro_momentum", "mMom(%)", "pct"),
    ("micro_vol_ratio", "VolR", "raw"),
]

SNAPBACK_OPTIONAL_CONTEXT_FIELDS = [
    ("drop_pct", "Drop(%)", "pct"),
    ("vol_ratio", "VolR", "raw"),
    ("trigger_type", "Trigger", "str"),
    ("needle_depth_pct", "NeedleDepth(%)", "pct"),
    ("needle_price", "NeedlePx", "price"),
]

COMMON_OPTIONAL_CONTEXT_FIELDS = [
    ("chg_24h", "24hChg(%)", "pct"),
]


def fmt_value(value, kind):
    if value is None:
        return "N/A"
    if kind == "pct":
        return round(float(value) * 100, 2)
    if kind == "price":
        return round(float(value), 8)
    if kind == "raw":
        return round(float(value), 2)
    return value


def infer_strategy_name(run_id, config_data, trades):
    strategy_name = str(config_data.get("strategy_name", "")).strip().lower()
    if strategy_name:
        return strategy_name

    if "top1" in run_id.lower():
        return "top1"

    if "snapback" in run_id.lower():
        return "snapback"

    first_ctx = trades[0].get("context", {}) if trades else {}
    if any(k in first_ctx for k in ["needle_depth_pct", "needle_price", "trigger_type"]):
        return "snapback"
    if any(k in first_ctx for k in ["mDD_15m", "mDD_120m", "micro_drawdown", "micro_momentum"]):
        return "top1"
    return "unknown"


def build_strategy_fields(strategy_name, trades):
    first_ctx = trades[0].get("context", {}) if trades else {}

    fields = list(COMMON_OPTIONAL_CONTEXT_FIELDS)

    if strategy_name == "snapback":
        fields.extend(SNAPBACK_OPTIONAL_CONTEXT_FIELDS)
    elif strategy_name == "top1":
        fields.extend(TOP1_OPTIONAL_CONTEXT_FIELDS)
    else:
        for candidate in TOP1_OPTIONAL_CONTEXT_FIELDS + SNAPBACK_OPTIONAL_CONTEXT_FIELDS:
            if candidate[0] in first_ctx:
                fields.append(candidate)

    dedup = []
    seen = set()
    for item in fields:
        if item[1] not in seen:
            dedup.append(item)
            seen.add(item[1])
    return dedup


def load_run_config(latest_file, run_id):
    summary_file = os.path.join(
        os.path.dirname(latest_file), f"sim_summary.{run_id}.json"
    )
    config_data = {}
    if os.path.exists(summary_file):
        with open(summary_file, "r", encoding="utf-8") as f:
            summary_data = json.load(f)
            config_data = summary_data.get("run_config", {})
            print("✅ 成功关联并读取到回测全局配置参数。")
    else:
        print(f"⚠️ 警告: 未找到对应的 sim_summary 文件 ({summary_file})")
    return config_data


def calc_mfe_mae(data_dir, sym, entry_time, exit_time, entry_price):
    mfe_pct = 0.0
    mae_pct = 0.0

    sym_dir = os.path.join(data_dir, sym)
    if not os.path.isdir(sym_dir):
        return mfe_pct, mae_pct

    try:
        pq_files = [
            os.path.join(sym_dir, f)
            for f in os.listdir(sym_dir)
            if f.endswith(".parquet")
        ]
        if not pq_files:
            return mfe_pct, mae_pct

        df = pq.read_table(pq_files).to_pandas()
        mask = (df["open_time_ms"] >= entry_time) & (df["open_time_ms"] <= exit_time)
        hold_df = df[mask]
        if hold_df.empty:
            return mfe_pct, mae_pct

        high_max = hold_df["high"].max()
        low_min = hold_df["low"].min()
        mfe_pct = (high_max / entry_price - 1) * 100
        mae_pct = (low_min / entry_price - 1) * 100
    except Exception:
        pass

    return mfe_pct, mae_pct


def main():
    parser = argparse.ArgumentParser(
        description="AI 深度复盘特征提取器 (Top1 / Snapback 通用版)"
    )
    parser.add_argument(
        "--run-id",
        type=str,
        help="指定要提取的回测 RUN_ID；如果不指定则默认寻找最新的",
        default=None,
    )
    args = parser.parse_args()

    print("🔍 正在寻找回测交易记录...")

    if args.run_id:
        search_pattern = os.path.join(
            os.getcwd(), "**", f"sim_trades.{args.run_id}.jsonl"
        )
    else:
        search_pattern = os.path.join(os.getcwd(), "**", "sim_trades.*.jsonl")

    files = glob.glob(search_pattern, recursive=True)
    if not files:
        if args.run_id:
            print(f"❌ 未找到 run-id 为 {args.run_id} 的 sim_trades.jsonl 文件。")
        else:
            print("❌ 未找到 sim_trades.jsonl 文件，请确保回测已成功运行。")
        sys.exit(1)

    latest_file = max(files, key=os.path.getmtime)
    base_name = os.path.basename(latest_file)
    print(f"📄 已锁定文件: {base_name}")

    run_id = base_name.replace("sim_trades.", "").replace(".jsonl", "")
    config_data = load_run_config(latest_file, run_id)

    trades = []
    with open(latest_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                trades.append(json.loads(line))

    if not trades:
        print("⚠️ 交易记录为空。")
        sys.exit(0)

    strategy_name = infer_strategy_name(run_id, config_data, trades)
    strategy_fields = build_strategy_fields(strategy_name, trades)
    print(f"🧭 已识别策略: {strategy_name}")
    print("🧠 正在提取高维特征并计算 MFE/MAE，请稍候...")

    data_dir = os.path.join(os.getcwd(), "data", "klines_1m")
    features_list = []

    for t in trades:
        sym = t["symbol"]
        entry_time = t["entry_time"]
        exit_time = t["exit_time"]
        entry_price = t["entry_price"]
        ctx = t.get("context", {})

        hold_mins = int((exit_time - entry_time) / 60000)
        mfe_pct, mae_pct = calc_mfe_mae(
            data_dir, sym, entry_time, exit_time, entry_price
        )

        row = {
            "Symbol": sym,
            "Hold(m)": hold_mins,
            "Reason": t.get("reason", "UNKNOWN"),
            "PnL(%)": round(t.get("pnl_pct", 0) * 100, 2),
        }

        for ctx_key, out_col, kind in strategy_fields:
            value = ctx.get(ctx_key)
            row[out_col] = fmt_value(value, kind)

        row["MFE(%)"] = round(mfe_pct, 2)
        row["MAE(%)"] = round(mae_pct, 2)
        features_list.append(row)

    df_features = pd.DataFrame(features_list)
    df_features.sort_values(by="PnL(%)", ascending=False, inplace=True)
    md_table = df_features.to_markdown(index=False)

    print("\n" + "=" * 80)
    print("🎯 [AI 深度复盘数据包] 生成完毕！请一键复制以下【全部内容】发给我：")
    print("=" * 80 + "\n")
    print("### 【策略识别】")
    print(strategy_name)
    print("\n### 【回测全局参数】")
    print("```json")
    print(json.dumps(config_data, indent=2, ensure_ascii=False))
    print("```\n")
    print("### 【交易特征快照矩阵】")
    print(md_table)
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
