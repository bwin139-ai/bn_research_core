# 目标文件: core/engine/data_feeder.py

import logging
import os
from typing import List, Optional

import pandas as pd
import pyarrow.parquet as pq


class CrossSectionalFeeder:
    def __init__(
        self,
        data_dir: str,
        start_time_ms: int,
        end_time_ms: int,
        ndays_lowest: int = 3,
        symbols: Optional[List[str]] = None,
        config: dict = None,
    ):
        self.data_dir = data_dir
        self.start_time_ms = start_time_ms
        self.end_time_ms = end_time_ms
        self.ndays_lowest = ndays_lowest
        self.symbols = symbols if symbols else self._get_all_symbols()
        self.config = config

        # --- 生产级防御底线：严格读取历史投喂窗口契约 ---
        # 如果 config.json 中未定义此参数，将直接抛出 KeyError，严禁使用隐式默认值
        self.max_history_window_mins = self.config["max_history_window_mins"]

        # 为了能计算期初的 24h 和 N 天滚动指标，实际加载数据需要往前推 N 天
        self.buffer_start_ms = start_time_ms - (ndays_lowest + 1) * 24 * 3600 * 1000

        self.df = self._load_and_align_data()

    def _get_all_symbols(self) -> List[str]:
        return [
            d
            for d in os.listdir(self.data_dir)
            if os.path.isdir(os.path.join(self.data_dir, d))
        ]

    def _load_and_align_data(self) -> pd.DataFrame:
        logging.info(
            f"正在加载 Parquet 数据... (包含预热缓冲期，起点: {pd.to_datetime(self.buffer_start_ms, unit='ms')})"
        )
        all_data = []
        for sym in self.symbols:
            sym_dir = os.path.join(self.data_dir, sym)
            if not os.path.isdir(sym_dir):
                continue

            files = [
                os.path.join(sym_dir, f)
                for f in os.listdir(sym_dir)
                if f.endswith(".parquet")
            ]
            if not files:
                continue

            tbl = pq.read_table(files)
            df_sym = tbl.to_pandas()

            # 过滤时间范围（带 Buffer）
            df_sym = df_sym[
                (df_sym["open_time_ms"] >= self.buffer_start_ms)
                & (df_sym["open_time_ms"] <= self.end_time_ms)
            ].copy()

            if df_sym.empty:
                continue

            df_sym.sort_values("open_time_ms", inplace=True)
            df_sym["symbol"] = sym

            # --- Snapback branch 1 所需：显式保证 idx 列存在 ---
            # 允许 pre-list / 缺少 index 数据的品种存在，此时 *_idx 为 NaN。
            for idx_col in ("high_idx", "low_idx", "close_idx"):
                if idx_col not in df_sym.columns:
                    df_sym[idx_col] = float("nan")

            # --- 核心：向量化预计算特征 ---
            # 假设 K 线是连续的 1m，24 小时 = 1440 根 K 线
            window_24h = 24 * 60
            window_ndays = self.ndays_lowest * 24 * 60

            # 计算 24h 涨幅: (当前 close / 24h前 close) - 1
            df_sym["chg_24h"] = (
                df_sym["close"] / df_sym["close"].shift(window_24h) - 1.0
            )

            # 计算 24h 成交额总和
            df_sym["vol_24h"] = (
                df_sym["quote_asset_volume"]
                .rolling(window=window_24h, min_periods=1)
                .sum()
            )

            # 计算近 N 天最低价
            df_sym["lowest_ndays"] = (
                df_sym["low"].rolling(window=window_ndays, min_periods=1).min()
            )

            # --- Pro级内存优化：向下转型 (Downcasting) ---
            # 动态抓取所有 64 位浮点列，强转为 32 位。内存直降 50%，无损 7 位有效精度
            # 注意：时间戳 open_time_ms 原本就是 int64，不会被此逻辑误伤，确保了毫秒级时间安全
            float_cols = df_sym.select_dtypes(include=["float64"]).columns
            df_sym[float_cols] = df_sym[float_cols].astype("float32")

            all_data.append(df_sym)

        if not all_data:
            raise ValueError("没有找到任何符合时间范围内的数据！")

        full_df = pd.concat(all_data, ignore_index=True)

        # 裁剪掉预热缓冲期的数据，只保留实际回测所需的时间段
        full_df = full_df[full_df["open_time_ms"] >= self.start_time_ms]

        # 建立多重索引 (open_time_ms -> symbol) 方便横截面提取
        full_df.set_index(["open_time_ms", "symbol"], inplace=True)
        full_df.sort_index(inplace=True)

        logging.info("面板数据及滚动特征加载完毕。")
        return full_df

    def get_timestamps(self) -> List[int]:
        """获取所有唯一的时间戳，用于驱动时间轮"""
        return self.df.index.get_level_values("open_time_ms").unique().tolist()

    def get_cross_section(self, timestamp_ms: int) -> pd.DataFrame:
        """获取 T 时刻全市场的 K 线切片"""
        try:
            return self.df.xs(timestamp_ms, level="open_time_ms")
        except KeyError:
            return pd.DataFrame()
