import argparse
import json
import logging
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd


class NumpyEncoder(json.JSONEncoder):
    """处理 Numpy/Pandas 数据类型的 JSON 序列化器"""

    def default(self, obj):
        if isinstance(obj, (np.integer, np.floating, np.bool_)):
            return obj.item()
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NumpyEncoder, self).default(obj)


PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# 🧠 策略大脑将根据命令行参数动态导入，实现引擎复用

from core.analysis.analyzer import PerformanceAnalyzer  # noqa: E402
from core.analysis.visualizer import StrategyVisualizerMatplotlib  # noqa: E402
from core.config_loader import StrategyConfig  # noqa: E402
from core.engine.broker import Order, VirtualBroker  # noqa: E402
from core.engine.data_feeder import CrossSectionalFeeder  # noqa: E402


def setup_logging(log_file: str):
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )


def main():
    parser = argparse.ArgumentParser(description="Top1 Hunter 回测引擎")
    parser.add_argument(
        "--start", required=True, help="ISO格式开始时间，如 2025-04-18T00:00:00+00:00"
    )
    parser.add_argument(
        "--end", required=True, help="ISO格式结束时间，如 2026-03-03T00:00:00+00:00"
    )
    parser.add_argument("--config", default="config.json", help="策略配置文件路径")
    parser.add_argument("--out-dir", default="state", help="回测结果输出基础目录")
    parser.add_argument(
        "--run-id", default="default", help="运行实例ID，用于文件命名隔离"
    )
    parser.add_argument(
        "--kline-window", type=int, default=800, help="复盘图表展示的1分钟K线总数量"
    )
    parser.add_argument(
        "--strategy",
        choices=["top1", "snapback"],
        default="top1",
        help="选择要运行的策略大脑 (默认: top1)",
    )
    args = parser.parse_args()

    # 初始化目录和日志
    out_dir = os.path.join(PROJECT_ROOT, args.out_dir)
    os.makedirs(out_dir, exist_ok=True)
    log_file = os.path.join(PROJECT_ROOT, "logs", f"top1_sim.{args.run_id}.log")
    setup_logging(log_file)

    logging.info("=" * 60)
    logging.info(f"🚀 启动 {args.strategy.upper()} 策略仿真引擎 (RUNID: {args.run_id})")
    logging.info("=" * 60)

    # 1. 加载配置
    config_path = os.path.join(os.path.dirname(__file__), args.config)
    try:
        config = StrategyConfig.load(config_path)

        # --- 逻辑健壮性防线：启动前强制核对现场参数 ---
        print("\n" + "=" * 60)
        print(f"🚨 [逻辑校验] 正在从以下路径加载配置: {config_path}")
        print(f"🚨 [内存参数] 实际读入的 Key 列表: {list(config.keys())}")
        print("=" * 60 + "\n")
    except Exception as e:
        logging.error(f"配置加载失败: {e}")
        sys.exit(1)

    # 2. 解析时间
    try:
        start_dt = datetime.fromisoformat(args.start)
        end_dt = datetime.fromisoformat(args.end)
        start_ms = int(start_dt.timestamp() * 1000)
        end_ms = int(end_dt.timestamp() * 1000)
    except ValueError as e:
        logging.error(f"时间格式解析失败，请确保使用严格的 ISO8601 格式: {e}")
        sys.exit(1)

    # 3. 初始化基础设施
    data_dir = os.path.join(PROJECT_ROOT, "data", "klines_1m")
    try:
        feeder = CrossSectionalFeeder(
            config=config,
            data_dir=data_dir,
            start_time_ms=start_ms,
            end_time_ms=end_ms,
            ndays_lowest=config.get("ndays_lowest", 3),
        )
        timestamps = feeder.get_timestamps()
        logging.info(
            f"数据加载完毕，时间范围: {start_dt} 至 {end_dt}，共 {len(timestamps)} 根K线"
        )
    except Exception as e:
        logging.error(f"数据源初始化失败: {e}")
        sys.exit(1)

    broker = VirtualBroker(config=config)

    # 🧠 动态挂载策略大脑
    if args.strategy == "top1":
        from strategies.top1_hunter.logic import Top1HunterStrategy

        strategy = Top1HunterStrategy(config=config)
    elif args.strategy == "snapback":
        from strategies.snapback.logic import WashoutSnapbackStrategy

        strategy = WashoutSnapbackStrategy(config=config)
    else:
        logging.error(f"❌ 不支持的策略类型: {args.strategy}")
        sys.exit(1)

    signals_history = []

    # ==========================================
    # 🚀 [基因改造] 预计算：拆解全表为极速字典
    # ==========================================
    logging.info("⚙️ 正在将多重索引大表预先拆解为 O(1) 寻址字典，请稍候...")
    df_dict = {
        sym: df.reset_index(level="symbol", drop=True).sort_index()
        for sym, df in feeder.df.groupby(level="symbol")
    }
    logging.info(
        f"✅ 缓存字典建立完毕，共收录 {len(df_dict)} 个交易标的，开始极速步进！"
    )
    # ==========================================

    # 4. 时间驱动循环
    logging.info("引擎点火，开始时间步进...")
    for i, ts in enumerate(timestamps):
        cross_section = feeder.get_cross_section(ts)

        # 4.1 驱动撮合引擎 (先处理已有订单的成交/撤销)
        broker.on_kline_close(ts, cross_section)

        # 4.2 获取当前活动标的，传给大脑做环境感知
        active_symbols = set(broker.active_orders.keys()) | set(
            broker.active_positions.keys()
        )

        # 4.3 大脑思考，输出信号快照 (若无信号则返回 None)
        signal = strategy.on_kline_close(
            ts, cross_section, active_symbols, full_df=df_dict
        )

        if signal:
            signals_history.append(signal)
            # 4.4 回测入口作为"桥梁"，根据信号向撮合引擎发单
            order = Order(
                symbol=signal["symbol"],
                limit_price=signal["limit_price"],
                create_time_ms=signal["signal_time"],
                timeout_sec=signal["params"]["timeout_sec"],
                signal_time_ms=signal["signal_time"],
                signal_price=signal["current_price"],
                context=signal.get("context", {}),
            )
            order.tp_price = signal["tp_price"]
            order.sl_price = signal["sl_price"]
            broker.active_orders[signal["symbol"]] = order

    # 5. 盘后结算与落盘
    trade_history = broker.trade_history
    trades_out = os.path.join(out_dir, f"sim_trades.{args.run_id}.jsonl")
    signals_out = os.path.join(out_dir, f"sim_signals.{args.run_id}.jsonl")

    # 信号快照落盘
    with open(signals_out, "w", encoding="utf-8") as f:
        for s in signals_history:
            f.write(json.dumps(s, cls=NumpyEncoder) + "\n")

    # 成交记录落盘 (并附加北京时间)
    with open(trades_out, "w", encoding="utf-8") as f:
        for t in trade_history:
            if "signal_time" in t and t["signal_time"]:
                t["signal_time_bj"] = (
                    pd.to_datetime(t["signal_time"], unit="ms") + pd.Timedelta(hours=8)
                ).strftime("%Y-%m-%d %H:%M")
            t["entry_time_bj"] = (
                pd.to_datetime(t["entry_time"], unit="ms") + pd.Timedelta(hours=8)
            ).strftime("%Y-%m-%d %H:%M")
            t["exit_time_bj"] = (
                pd.to_datetime(t["exit_time"], unit="ms") + pd.Timedelta(hours=8)
            ).strftime("%Y-%m-%d %H:%M")
            f.write(json.dumps(t, cls=NumpyEncoder) + "\n")

    if not trade_history:
        logging.warning("本次回测无交易产生。")
        sys.exit(0)

    logging.info(
        f"生成业绩报告... 共 {len(trade_history)} 笔交易，发出 {len(signals_history)} 次信号"
    )
    analyzer = PerformanceAnalyzer(
        trade_history=trade_history, config=config, feeder_df=feeder.df
    )
    report = analyzer.generate_report()

    summary_out = os.path.join(out_dir, f"sim_summary.{args.run_id}.json")
    with open(summary_out, "w", encoding="utf-8") as f:
        # 核心改进：将本次运行的原始配置 config 完整保留在 summary 开头，确保实验可追溯
        safe_report = {"run_config": config}

        # 合并绩效报告字段 (过滤掉不可序列化的 DataFrame)
        for k, v in report.items():
            if k not in ["trades_df", "benchmark_series"]:
                safe_report[k] = v

        json.dump(safe_report, f, indent=2, ensure_ascii=False, cls=NumpyEncoder)
    # 6. 可视化导出
    viz_dir = os.path.join(out_dir, f"sim_viz_{args.run_id}")
    visualizer = StrategyVisualizerMatplotlib(output_dir=viz_dir)
    for trade in trade_history:
        visualizer.plot_trade_kline_mpl(
            trade=trade, feeder_df=feeder.df, window_mins_1m=args.kline_window
        )

    logging.info("=" * 60)
    logging.info("回测完成！")
    logging.info(f"信号快照: {signals_out}")
    logging.info(f"交易明细: {trades_out}")
    logging.info(f"业绩摘要: {summary_out}")
    logging.info(f"高清复盘图目录: {viz_dir}")
    logging.info("=" * 60)


if __name__ == "__main__":
    main()
