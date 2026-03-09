import argparse
import glob
import json
import os
import sys

import pandas as pd
import pyarrow.parquet as pq


def main():
    # 引入命令行参数解析，完美解决文件抓混问题
    parser = argparse.ArgumentParser(description="AI 深度复盘特征提取器 (V3 mDD版)")
    parser.add_argument(
        "--run-id",
        type=str,
        help="指定要提取的回测 RUN_ID (例如: Topq_Small_13579)，如果不指定则默认寻找最新的",
        default=None,
    )
    parser.add_argument(
        "--strategy",
        type=str,
        help="策略名称 (top1 或 snapback)，若不传将尝试根据 run-id 自动推测",
        default=None,
    )
    args = parser.parse_args()

    # 自动推断策略类型
    strategy_type = args.strategy
    if not strategy_type:
        if args.run_id and "Snapback" in args.run_id:
            strategy_type = "snapback"
        else:
            strategy_type = "top1"

    print("🔍 正在寻找回测交易记录...")

    if args.run_id:
        # 如果指定了 run_id，就精准狙击那个文件
        search_pattern = os.path.join(
            os.getcwd(), "**", f"sim_trades.{args.run_id}.jsonl"
        )
    else:
        # 否则还是按老规矩找最新的
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

    # 提取 RUN_ID (sim_trades.RUN_ID.jsonl -> RUN_ID)
    run_id = base_name.replace("sim_trades.", "").replace(".jsonl", "")

    # 顺藤摸瓜：读取对应的 summary 文件获取配置
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

    trades = []
    with open(latest_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                trades.append(json.loads(line))

    if not trades:
        print("⚠️ 交易记录为空。")
        sys.exit(0)

    data_dir = os.path.join(os.getcwd(), "data", "klines_1m")
    features_list = []
    print("🧠 正在提取高维特征并计算 MFE/MAE，请稍候...")

    for t in trades:
        sym = t["symbol"]
        entry_time = t["entry_time"]
        exit_time = t["exit_time"]
        entry_price = t["entry_price"]

        ctx = t.get("context", {})

        # 计算持仓时间（分钟）
        hold_mins = int((exit_time - entry_time) / 60000)

        # 计算 MFE / MAE (最大有利/不利波动)
        mfe_pct = 0.0
        mae_pct = 0.0

        sym_dir = os.path.join(data_dir, sym)
        if os.path.isdir(sym_dir):
            try:
                pq_files = [
                    os.path.join(sym_dir, f)
                    for f in os.listdir(sym_dir)
                    if f.endswith(".parquet")
                ]
                if pq_files:
                    df = pq.read_table(pq_files).to_pandas()
                    mask = (df["open_time_ms"] >= entry_time) & (
                        df["open_time_ms"] <= exit_time
                    )
                    hold_df = df[mask]
                    if not hold_df.empty:
                        high_max = hold_df["high"].max()
                        low_min = hold_df["low"].min()
                        mfe_pct = (high_max / entry_price - 1) * 100
                        mae_pct = (low_min / entry_price - 1) * 100
            except Exception:
                pass

        # 🚀 策略特征分流路由
        base_features = {
            "Symbol": sym,
            "Hold(m)": hold_mins,
            "Reason": t.get("reason", "UNKNOWN"),
            "PnL(%)": round(t.get("pnl_pct", 0) * 100, 2),
            "24hChg(%)": round(ctx.get("chg_24h", 0) * 100, 1),
            "MFE(%)": round(mfe_pct, 2),
            "MAE(%)": round(mae_pct, 2),
        }

        if strategy_type == "snapback":
            needle_low = ctx.get("needle_low")
            drop_pct = ctx.get("drop_pct", 0)
            sl_dist = ((needle_low / entry_price) - 1.0) if needle_low else 0.0

            strat_features = {
                "Drop(%)": round(drop_pct * 100, 2),
                "VolR": round(ctx.get("vol_ratio", 0), 1),
                "Trigger": ctx.get("trigger_type", "N/A"),
                "NeedleDist(%)": round(sl_dist * 100, 2),
            }
        else:
            # 兼容老版本 Top1 逻辑
            mdd_15m = ctx.get("mDD_15m")
            mdd_120m = ctx.get("mDD_120m")
            strat_features = {
                "mDD_15m(%)": round(mdd_15m * 100, 2) if mdd_15m is not None else "N/A",
                "mDD_120m(%)": (
                    round(mdd_120m * 100, 2) if mdd_120m is not None else "N/A"
                ),
                "mMom(%)": round(ctx.get("micro_momentum", 0) * 100, 2),
                "VolR": round(ctx.get("micro_vol_ratio", 0), 2),
            }

        # 合并字典
        features_list.append({**base_features, **strat_features})

    df_features = pd.DataFrame(features_list)
    df_features.sort_values(by="PnL(%)", ascending=False, inplace=True)
    md_table = df_features.to_markdown(index=False)

    print("\n" + "=" * 80)
    print("🎯 [AI 深度复盘数据包] 生成完毕！请一键复制以下【全部内容】发给我：")
    print("=" * 80 + "\n")
    print("### 【回测全局参数】")
    print("```json")
    print(json.dumps(config_data, indent=2, ensure_ascii=False))
    print("```\n")
    print("### 【交易特征快照矩阵】")
    print(md_table)
    print("\n" + "=" * 80)


if __name__ == "__main__":
    main()
