import os

import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd


class StrategyVisualizerMatplotlib:
    def __init__(self, output_dir: str = "output"):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        plt.style.use("seaborn-v0_8-whitegrid")
        plt.ioff()
        self.mc = mpf.make_marketcolors(up="g", down="r", inherit=True)
        self.s = mpf.make_mpf_style(base_mpf_style="charles", marketcolors=self.mc)

    def plot_trade_kline_mpl(
        self, trade: dict, feeder_df: pd.DataFrame, window_mins_1m: int = 800
    ):
        symbol = trade["symbol"]
        entry_time_dt = pd.to_datetime(trade["entry_time"], unit="ms")
        exit_time_dt = pd.to_datetime(trade["exit_time"], unit="ms")

        # 获取信号时间与价格，若无此字段则回退使用入场数据
        signal_time_ms = trade.get("signal_time", trade["entry_time"])
        signal_time_dt = pd.to_datetime(signal_time_ms, unit="ms")
        signal_price = trade.get("signal_price", trade["entry_price"])

        # 1. 动态居中算法：以 [信号时间] 到 [离场时间] 的中点为核心延展 K 线
        center_time_dt = signal_time_dt + (exit_time_dt - signal_time_dt) / 2
        start_time_dt = center_time_dt - pd.Timedelta(minutes=window_mins_1m // 2)
        end_time_dt = center_time_dt + pd.Timedelta(minutes=window_mins_1m // 2)

        try:
            sym_df_1m = feeder_df.copy()
            idx = sym_df_1m.index
            if hasattr(idx, "levels"):
                time_level = -1
                for level_pos in range(idx.nlevels):
                    sample_val = idx.get_level_values(level_pos)[0]
                    try:
                        pd.to_datetime(sample_val, unit="ms")
                        time_level = level_pos
                        break
                    except Exception:
                        continue
                if symbol in idx.get_level_values(0 if time_level != 0 else 1):
                    sym_level = 0 if time_level != 0 else 1
                    sym_df_1m = sym_df_1m.xs(symbol, level=sym_level)
            new_index = pd.to_datetime(sym_df_1m.index, unit="ms", errors="coerce")
            sym_df_1m.index = new_index
            sym_df_1m = sym_df_1m[sym_df_1m.index.notnull()].sort_index()
        except Exception as e:
            raise TypeError(f"[数据结构错误] 无法对齐 feeder_df 索引: {e}")

        if sym_df_1m.empty:
            raise ValueError(f"[数据缺失] {symbol} 数据为空")

        plot_df_1m = sym_df_1m.loc[start_time_dt:end_time_dt].copy()

        # --- 2. 准备整合标题信息 ---
        exit_str_full = exit_time_dt.strftime("%Y-%m-%d %H:%M")
        signal_str_short = signal_time_dt.strftime("%H:%M")
        entry_str_short = entry_time_dt.strftime("%H:%M")
        exit_str_short = exit_time_dt.strftime("%H:%M")

        title_line1 = f"{exit_str_full} | {symbol} | PnL: {trade['pnl_pct']*100:.2f}% | {trade['reason']}"
        title_line2 = (
            f"S: {signal_str_short} @ {signal_price:.6f} | "
            f"E: {entry_str_short} @ {trade['entry_price']:.6f} | "
            f"E: {exit_str_short} @ {trade['exit_price']:.6f}"
        )

        # 提取快照特征
        signal_idx = plot_df_1m.index.get_indexer([signal_time_dt], method="nearest")[0]
        entry_idx = plot_df_1m.index.get_indexer([entry_time_dt], method="nearest")[0]
        exit_idx = plot_df_1m.index.get_indexer([exit_time_dt], method="nearest")[0]
        signal_row = plot_df_1m.iloc[signal_idx]
        ctx = trade.get("context", {})

        chg_val = signal_row.get("chg_24h", 0)
        chg_24h = 0.0 if pd.isna(chg_val) else float(chg_val) * 100
        vol_val = signal_row.get("vol_24h", 0)
        vol_24h_10m = (
            0 if pd.isna(vol_val) else int(float(vol_val) / 1000000)
        )  # 改为除以一百万(M)
        lowest_val = signal_row.get("lowest_ndays", signal_price)
        surge_pct = (
            0.0
            if (pd.isna(lowest_val) or lowest_val == 0)
            else (float(signal_price) / float(lowest_val) - 1) * 100
        )

        m_dd = ctx.get("micro_drawdown", 0.0)
        m_mom = ctx.get("micro_momentum", 0.0)
        m_vr = ctx.get("micro_vol_ratio", 0.0)

        title_line3 = (
            f"Snap: 24hChg {chg_24h:.1f}% | 24hVol {vol_24h_10m}M | Surge {surge_pct:.1f}% | "
            f"mDD {m_dd*100:.1f}% | mMom {m_mom*100:.1f}% | VolR {m_vr:.2f}"
        )

        # --- 3. 绘制主图 (关闭 tight_layout，自己接管排版) ---
        fig, axes = mpf.plot(
            plot_df_1m,
            type="candle",
            style=self.s,
            volume=True,
            returnfig=True,
            show_nontrading=False,
            datetime_format="%m-%d %H:%M",
            figscale=1.5,
            tight_layout=False,  # 必须关闭紧凑布局，否则会覆盖我们的边界调整
        )
        ax = axes[0]
        _ax_vol = axes[2]

        # 优化边界：极致压缩左/下留白，把画板空间最大程度还给图表
        fig.subplots_adjust(top=0.86, bottom=0.05, left=0.02, right=0.92)

        # 放大字号并整体上提，将坐标逼近画板顶部 (0.98~0.90)，彻底远离图表区 (0.86)
        fig.text(
            0.5,
            0.98,
            title_line1,
            fontsize=18,
            fontweight="bold",
            color="g" if trade["pnl_pct"] > 0 else "r",
            ha="center",
        )
        fig.text(
            0.5,
            0.94,
            title_line2,
            fontsize=14,
            color="black",
            ha="center",
            family="monospace",
        )
        fig.text(
            0.5,
            0.90,
            title_line3,
            fontsize=13,
            color="#333333",
            ha="center",
            family="monospace",
        )

        # 4. 绘制标记点
        ax.scatter(
            signal_idx, signal_price, color="purple", s=120, marker="D", zorder=9
        )
        ax.scatter(
            entry_idx, trade["entry_price"], color="blue", s=100, marker="^", zorder=10
        )
        ax.scatter(
            exit_idx, trade["exit_price"], color="magenta", s=100, marker="v", zorder=10
        )

        # 5. 为图表区添加黑色矩形边框隔离
        for ax_target in [ax, _ax_vol]:
            for spine in ax_target.spines.values():
                spine.set_visible(True)
                spine.set_linewidth(1.5)
                spine.set_color("black")

        # 6. 保存图表
        filename = f"{symbol}_{entry_time_dt.strftime('%Y%m%d_%H%M%S')}.png"
        save_path = os.path.join(self.output_dir, filename)
        fig.savefig(save_path, dpi=100)
        plt.close(fig)
