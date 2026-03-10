import argparse
import json
import os
from collections import defaultdict

import pandas as pd


def load_symbol_klines(data_dir: str, symbol: str) -> pd.DataFrame:
    """进入币种子目录，读取并拼接所有 K 线文件"""
    symbol_dir = os.path.join(data_dir, symbol)
    if not os.path.isdir(symbol_dir):
        return None

    dfs = []
    # 遍历子目录下的所有文件
    for file in os.listdir(symbol_dir):
        if file.endswith(".parquet") or file.endswith(".csv"):
            path = os.path.join(symbol_dir, file)
            try:
                df = (
                    pd.read_parquet(path)
                    if file.endswith(".parquet")
                    else pd.read_csv(path)
                )
                dfs.append(df)
            except Exception as e:
                print(f"  ⚠️ 读取分片文件失败 {file}: {e}")

    if not dfs:
        return None

    # 拼接所有切片
    combined_df = pd.concat(dfs, ignore_index=True)

    # 兼容索引和列名差异
    if "open_time_ms" not in combined_df.columns:
        if combined_df.index.name == "open_time_ms":
            combined_df = combined_df.reset_index()
        elif "timestamp" in combined_df.columns:
            combined_df = combined_df.rename(columns={"timestamp": "open_time_ms"})
        elif "open_time" in combined_df.columns:
            combined_df = combined_df.rename(columns={"open_time": "open_time_ms"})

    # 确保按时间严格排序，并去重
    if "open_time_ms" in combined_df.columns:
        combined_df = (
            combined_df.sort_values("open_time_ms")
            .drop_duplicates(subset=["open_time_ms"])
            .reset_index(drop=True)
        )

    return combined_df


def main():
    parser = argparse.ArgumentParser(description="提取交易前 N 分钟的 K 线全息案发现场")
    parser.add_argument(
        "--trades", required=True, help="输入的 sim_trades.jsonl 文件路径"
    )
    parser.add_argument("--data-dir", default="data/klines_1m", help="1分钟K线数据目录")
    parser.add_argument(
        "--window", type=int, default=120, help="提取信号前多少分钟的 K 线"
    )
    args = parser.parse_args()

    trades = []
    with open(args.trades, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                trades.append(json.loads(line))

    print(f"🔍 成功加载 {len(trades)} 笔交易流水。")

    trades_by_symbol = defaultdict(list)
    for t in trades:
        trades_by_symbol[t["symbol"]].append(t)

    out_file = args.trades.replace(".jsonl", "_crime_scenes.jsonl")
    processed_count = 0
    missing_data_count = 0

    with open(out_file, "w", encoding="utf-8") as f_out:
        for symbol, symbol_trades in trades_by_symbol.items():
            print(f"  👉 正在解析 {symbol} ({len(symbol_trades)} 笔交易)...")
            df = load_symbol_klines(args.data_dir, symbol)

            if df is None or df.empty:
                print(f"  ⚠️ 警告: 找不到 {symbol} 的 K 线数据，跳过。")
                missing_data_count += len(symbol_trades)
                continue

            for t in symbol_trades:
                signal_time = t.get("signal_time")
                if not signal_time:
                    continue

                # 计算 120 分钟前的时间戳
                start_time = signal_time - (args.window * 60 * 1000)
                mask = (df["open_time_ms"] >= start_time) & (
                    df["open_time_ms"] <= signal_time
                )
                scene_df = df[mask].copy()

                kline_list = []
                for _, row in scene_df.iterrows():
                    kline_list.append(
                        {
                            "t": int(row["open_time_ms"]),
                            "o": float(row["open"]),
                            "h": float(row["high"]),
                            "l": float(row["low"]),
                            "c": float(row["close"]),
                        }
                    )

                t["kline_120m"] = kline_list
                f_out.write(json.dumps(t) + "\n")
                processed_count += 1

    print("=" * 60)
    print("🎯 全息案发现场提取完毕！")
    print(f"✅ 成功提取: {processed_count} 笔")
    if missing_data_count > 0:
        print(f"❌ 缺失数据: {missing_data_count} 笔")
    print(f"📁 输出文件: {out_file}")
    print("=" * 60)


if __name__ == "__main__":
    main()
