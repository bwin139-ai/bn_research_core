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

        if "quote_asset_volume" not in plot_df_1m.columns:
            raise ValueError(
                '[数据缺失] plot_df_1m 缺少 "quote_asset_volume"，无法绘制成交额副图'
            )

        def _ctx_first(*keys, default=None):
            ctx_local = trade.get("context", {}) or {}
            for key in keys:
                if key in ctx_local and not pd.isna(ctx_local[key]):
                    return ctx_local[key]
            return default

        def _to_dt(value, fallback_dt):
            if value is None or pd.isna(value):
                return fallback_dt
            try:
                return pd.to_datetime(int(value), unit="ms")
            except Exception:
                try:
                    return pd.to_datetime(value)
                except Exception:
                    return fallback_dt

        def _to_float(value, fallback_value):
            if value is None or pd.isna(value):
                return float(fallback_value)
            try:
                return float(value)
            except Exception:
                return float(fallback_value)

        def _fmt_num(value, digits=2, scale=1.0, suffix=""):
            if value is None or pd.isna(value):
                return "NA"
            try:
                return f"{float(value) * scale:.{digits}f}{suffix}"
            except Exception:
                return "NA"

        def _fmt_price(value):
            if value is None or pd.isna(value):
                return "NA"
            try:
                return f"{float(value):.6f}"
            except Exception:
                return "NA"

        def _fmt_time(value_dt):
            return value_dt.strftime("%H:%M")

        exit_reason_map = {
            "TAKE_PROFIT": "TP",
            "STOP_LOSS": "SL",
            "TIME_STOP": "TS",
            "BREAKEVEN_STOP": "BS",
        }
        exit_short = exit_reason_map.get(trade.get("reason"), "OTHER")

        a_time_dt = _to_dt(
            _ctx_first("a_time_ms", "a_ts_ms", "a_time", "a_ts"), signal_time_dt
        )
        b_time_dt = _to_dt(
            _ctx_first("b_time_ms", "b_ts_ms", "b_time", "b_ts"), signal_time_dt
        )
        c_time_dt = _to_dt(
            _ctx_first("c_time_ms", "c_ts_ms", "c_time", "c_ts"), entry_time_dt
        )
        e_time_dt = exit_time_dt

        a_price = _to_float(
            _ctx_first("a_price", "a_px", "point_a_price"), signal_price
        )
        b_price = _to_float(
            _ctx_first("b_price", "b_px", "point_b_price"), signal_price
        )
        c_price = _to_float(
            _ctx_first("c_price", "c_px", "point_c_price"), trade["entry_price"]
        )
        e_price = _to_float(trade.get("exit_price"), trade["exit_price"])

        # --- 2. 准备整合标题信息 ---
        entry_str_full = entry_time_dt.strftime("%Y-%m-%d %H:%M")

        title_line1 = (
            f"{entry_str_full} | {symbol} | PnL: {trade['pnl_pct']*100:.2f}% | {exit_short}"
        )
        title_line2 = (
            f"A: {_fmt_time(a_time_dt)} @ {_fmt_price(a_price)} | "
            f"B: {_fmt_time(b_time_dt)} @ {_fmt_price(b_price)} | "
            f"C: {_fmt_time(c_time_dt)} @ {_fmt_price(c_price)} | "
            f"E: {_fmt_time(e_time_dt)} @ {_fmt_price(e_price)}"
        )

        # 提取快照特征
        signal_idx = plot_df_1m.index.get_indexer([signal_time_dt], method="nearest")[0]
        a_idx = plot_df_1m.index.get_indexer([a_time_dt], method="nearest")[0]
        b_idx = plot_df_1m.index.get_indexer([b_time_dt], method="nearest")[0]
        c_idx = plot_df_1m.index.get_indexer([c_time_dt], method="nearest")[0]
        e_idx = plot_df_1m.index.get_indexer([e_time_dt], method="nearest")[0]
        signal_row = plot_df_1m.iloc[signal_idx]
        ctx = trade.get("context", {}) or {}

        chg_val = signal_row.get("chg_24h", 0)
        chg_24h = 0.0 if pd.isna(chg_val) else float(chg_val) * 100
        vol_val = signal_row.get("vol_24h", 0)
        vol_24h_m = 0 if pd.isna(vol_val) else int(float(vol_val) / 1000000)

        ab_bars = _ctx_first("ab_bars", "ab_bar_count", "ab_bars_count")
        bc_bars = _ctx_first("bc_bars", "bc_bar_count", "bc_bars_count")
        bc_ab_ratio = _ctx_first("bc_ab_ratio", "bc_ab", "bc_over_ab")
        drop_pct = _ctx_first("drop_pct", "drop_ratio", "a_to_b_drop_pct")
        rebound_ratio = _ctx_first("rebound_ratio", "bc_rebound_ratio", "rebound_pct_ratio")
        bindex = _ctx_first("bindex", "b_index", "bindex_score", "b_idx")
        tp_tier = _ctx_first("tp_tier", "selected_tp_tier")
        selected_tp_pct = _ctx_first("selected_tp_pct", "tp_pct", "take_profit_pct")
        vol_r = _ctx_first("micro_vol_ratio", "vol_r", "vol_ratio", "volume_ratio")

        title_line3 = (
            f"Snap: abBars {ab_bars if ab_bars is not None else 'NA'} | "
            f"bcBars {bc_bars if bc_bars is not None else 'NA'} | "
            f"bc/ab {_fmt_num(bc_ab_ratio, digits=2)} | "
            f"Drop {_fmt_num(drop_pct, digits=2, scale=100, suffix='%')} | "
            f"Rebound {_fmt_num(rebound_ratio, digits=2)} | "
            f"BIndex {_fmt_num(bindex, digits=2)}"
        )
        title_line4 = (
            f"Env: 24hChg {chg_24h:.1f}% | 24hVol {vol_24h_m}M | "
            f"VolR {_fmt_num(vol_r, digits=2)} | "
            f"tpTier {tp_tier if tp_tier is not None else 'NA'} | "
            f"selTP {_fmt_num(selected_tp_pct, digits=2, scale=100, suffix='%')}"
        )

        # --- 3. 绘制主图 + 成交额副图 (关闭 tight_layout，自己接管排版) ---
        quote_vol_addplot = mpf.make_addplot(
            plot_df_1m["quote_asset_volume"],
            panel=1,
            type="bar",
            ylabel="Quote Vol",
            color="dimgray",
        )

        fig, axes = mpf.plot(
            plot_df_1m,
            type="candle",
            style=self.s,
            addplot=quote_vol_addplot,
            volume=False,
            panel_ratios=(3, 1),
            returnfig=True,
            show_nontrading=False,
            datetime_format="%m-%d %H:%M",
            figscale=1.5,
            tight_layout=False,  # 必须关闭紧凑布局，否则会覆盖我们的边界调整
        )
        ax = axes[0]
        _ax_vol = axes[2]

        # 预留更高标题区，避免顶部裁切
        fig.subplots_adjust(top=0.72, bottom=0.05, left=0.04, right=0.92)

        fig.text(
            0.5,
            0.985,
            title_line1,
            fontsize=18,
            fontweight="bold",
            color="g" if trade["pnl_pct"] > 0 else "r",
            ha="center",
            va="top",
        )
        fig.text(
            0.5,
            0.952,
            title_line2,
            fontsize=13,
            color="black",
            ha="center",
            va="top",
            family="monospace",
        )
        fig.text(
            0.5,
            0.922,
            title_line3,
            fontsize=11,
            color="#333333",
            ha="center",
            va="top",
            family="monospace",
        )
        fig.text(
            0.5,
            0.892,
            title_line4,
            fontsize=11,
            color="#333333",
            ha="center",
            va="top",
            family="monospace",
        )

        # 4. 绘制 A / B / C / E 字母标记
        ax.annotate(
            "A",
            xy=(a_idx, a_price),
            xytext=(0, 12),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=13,
            fontweight="bold",
            color="darkorange",
            bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="darkorange", alpha=0.9),
            zorder=10,
        )
        ax.annotate(
            "B",
            xy=(b_idx, b_price),
            xytext=(0, -16),
            textcoords="offset points",
            ha="center",
            va="top",
            fontsize=13,
            fontweight="bold",
            color="crimson",
            bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="crimson", alpha=0.9),
            zorder=10,
        )
        ax.annotate(
            "C",
            xy=(c_idx, c_price),
            xytext=(0, 12),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=13,
            fontweight="bold",
            color="blue",
            bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="blue", alpha=0.9),
            zorder=10,
        )
        ax.annotate(
            "E",
            xy=(e_idx, e_price),
            xytext=(0, 12),
            textcoords="offset points",
            ha="center",
            va="bottom",
            fontsize=13,
            fontweight="bold",
            color="purple",
            bbox=dict(boxstyle="round,pad=0.18", fc="white", ec="purple", alpha=0.9),
            zorder=10,
        )

        # 5. 为图表区添加黑色矩形边框隔离
        for ax_target in [ax, _ax_vol]:
            for spine in ax_target.spines.values():
                spine.set_visible(True)
                spine.set_linewidth(1.5)
                spine.set_color("black")

        # 6. 保存图表
        filename = f"SNAP_{entry_time_dt.strftime('%Y%m%d_%H%M')}_{symbol}_{exit_short}.png"
        save_path = os.path.join(self.output_dir, filename)
        fig.savefig(save_path, dpi=100, bbox_inches="tight", pad_inches=0.25)
        plt.close(fig)
