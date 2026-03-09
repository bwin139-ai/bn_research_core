import logging
from typing import Any, Dict

import pandas as pd


class Top1HunterStrategy:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

        # 严格获取配置参数
        self.min_24h_pct = self.config["min_24h_pct_chg"]
        self.min_24h_vol = self.config["min_24h_quote_vol"]
        self.max_surge = self.config["max_surge_from_lowest"]
        self.entry_pullback = self.config["entry_pullback_pct"]
        self.tp_pct = self.config["take_profit_pct"]
        self.sl_pct = self.config["stop_loss_pct"]
        self.timeout_sec = self.config["order_timeout_sec"]
        self.cooldown_ms = self.config["cooldown_hours"] * 3600 * 1000

        # 微观体检参数 (严格映射，不存在则报错阻断)
        self.micro_high_window_mins = self.config["micro_high_window_mins"]
        self.micro_max_drawdown_pct = self.config["micro_max_drawdown_pct"]
        self.micro_momentum_window_mins = self.config["micro_momentum_window_mins"]
        self.micro_min_momentum_pct = self.config["micro_min_momentum_pct"]
        self.micro_vol_short_mins = self.config["micro_vol_short_mins"]
        self.micro_vol_long_mins = self.config["micro_vol_long_mins"]
        self.micro_vol_ratio = self.config["micro_vol_ratio"]

        # V4 新增：极度高潮拦截器 (严禁默认值，缺失即崩溃)
        self.micro_max_momentum_pct = self.config["micro_max_momentum_pct"]
        self.micro_max_vol_ratio = self.config["micro_max_vol_ratio"]

        # 🕵️‍♂️ V3 宏观视觉雷达参数：严格执行 JSON 铁律 (严禁任何默认值)
        # 必须直接读取。如果 config.json 没配，程序必须报错中止！
        self.macro_15m_max_drawdown_pct = self.config["macro_15m_max_drawdown_pct"]
        self.macro_120m_max_drawdown_pct = self.config["macro_120m_max_drawdown_pct"]

        # 策略自带冷却期状态管理
        self.cooldown_until: Dict[str, int] = {}

    def on_kline_close(
        self,
        current_time_ms: int,
        cross_section: pd.DataFrame,
        active_symbols: set,
        full_df: Dict[str, pd.DataFrame] = None,
    ) -> dict:
        """
        纯函数大脑：接收截面数据与环境状态，返回信号快照字典(或None)
        """
        if cross_section.empty:
            return None

        # 1. 过滤掉涨跌幅数据缺失的无效标的 (防异常)
        cs = cross_section.dropna(subset=["chg_24h"]).copy()
        if cs.empty:
            return None

        # 2. 获取全网绝对真正的 Top 1 (无视其他任何附加条件)
        # 完美契合实盘 O(1) 逻辑：只看涨幅榜第一名
        candidates = cs.sort_values(by="chg_24h", ascending=False)
        top1_symbol = candidates.index[0]
        top1_data = candidates.iloc[0]

        # 生成全网真实的 Global Top 10 快照 (记录当时的极端情绪)
        top10_df = candidates.head(10)
        top_ranks = [
            {"symbol": sym, "chg_24h": row["chg_24h"]}
            for sym, row in top10_df.iterrows()
        ]

        # 3. 对这位唯一的 "龙一" 进行极其严苛的单人政审 (一票否决制，不符合直接放弃本轮)

        # 体检 A: 24h 涨幅是否达到硬性底线 (防全网普跌时的矮子里拔将军)
        if top1_data["chg_24h"] < self.min_24h_pct:
            return None

        # 体检 B: 24h 成交额是否达标 (防小盘弱庄、防插针骗线)
        if pd.isna(top1_data["vol_24h"]) or top1_data["vol_24h"] < self.min_24h_vol:
            return None

        # 体检 C: 距离 N 日低点拉升是否过高 (防鱼尾接盘)
        if pd.isna(top1_data["lowest_ndays"]):
            return None  # 缺少历史数据(刚上市的新币)，直接一票否决

        surge = top1_data["close"] / top1_data["lowest_ndays"] - 1
        if surge > self.max_surge:
            return None

        # 体检 D: 是否已经持有或正在挂单
        if top1_symbol in active_symbols:
            return None

        # 体检 E: 是否处于交易冷却期
        cooldown_end = self.cooldown_until.get(top1_symbol, 0)
        if current_time_ms < cooldown_end:
            return None

        # ==========================================
        # ⚠️ 核心架构重构：O(1) 极速历史数据切片
        # ==========================================
        if full_df is None or top1_symbol not in full_df:
            return None

        sym_df = full_df[top1_symbol]

        # searchsorted 底层是 C 语言的二分查找，寻找当前时间对应的绝对行号
        idx = sym_df.index.searchsorted(current_time_ms, side="right")
        if idx == 0:
            return None

        # 我们最多只需要用到过去 max_history_window_mins 的数据
        max_window = self.config.get("max_history_window_mins", 300)
        start_idx = max(0, idx - max_window)

        # 直接通过整数位置 (iloc) 截取，零复制
        history_df = sym_df.iloc[start_idx:idx]

        if len(history_df) == 0:
            return None
        # ==========================================

        # ==========================================
        # 🕵️‍♂️ V3 宏观视觉雷达：连贯回撤深度(mDD)检测
        # ==========================================
        # 必须拥有至少 120 分钟的历史来确认筹码结构，新币直接放弃
        if len(history_df) < 120:
            return None

        recent_120 = history_df.tail(120)
        recent_15 = history_df.tail(15)

        # 探测 15 分钟最大连贯回撤 (mDD_15m) -> 捕捉点火期的抛压
        rolling_max_15 = recent_15["high"].cummax()
        mdd_15 = ((rolling_max_15 - recent_15["low"]) / rolling_max_15).max()
        if mdd_15 > self.macro_15m_max_drawdown_pct:
            return None

        # 探测 120 分钟最大连贯回撤 (mDD_120m) -> 捕捉蓄力期的绞肉机
        rolling_max_120 = recent_120["high"].cummax()
        mdd_120 = ((rolling_max_120 - recent_120["low"]) / rolling_max_120).max()
        if mdd_120 > self.macro_120m_max_drawdown_pct:
            return None
        # ==========================================

        # 体检 F: 局部高点回撤过滤 (现场算 max)
        if len(history_df) >= self.micro_high_window_mins:
            high_val = history_df["high"].tail(self.micro_high_window_mins).max()
        else:
            high_val = top1_data["high"]  # 数据不足时的保底

        drawdown = 0.0
        if pd.notna(high_val) and high_val > 0:
            drawdown = (high_val - top1_data["close"]) / high_val
            if drawdown > self.micro_max_drawdown_pct:
                return None

        # 体检 G: 短期动量要求 (现场抓历史收盘价)
        momentum = 0.0
        if len(history_df) > self.micro_momentum_window_mins:
            price_ago_val = history_df["close"].iloc[
                -self.micro_momentum_window_mins - 1
            ]
            if pd.notna(price_ago_val) and price_ago_val > 0:
                momentum = top1_data["close"] / price_ago_val - 1
                if (
                    momentum < self.micro_min_momentum_pct
                    or momentum > self.micro_max_momentum_pct
                ):
                    return None
        else:
            return None

        # 体检 H: 微观量价配合 (现场算均线)
        vol_ratio = 0.0
        if len(history_df) >= self.micro_vol_long_mins:
            vol_short_val = (
                history_df["quote_asset_volume"].tail(self.micro_vol_short_mins).mean()
            )
            vol_long_val = (
                history_df["quote_asset_volume"].tail(self.micro_vol_long_mins).mean()
            )
            if pd.notna(vol_short_val) and pd.notna(vol_long_val) and vol_long_val > 0:
                vol_ratio = vol_short_val / vol_long_val
                if (
                    vol_ratio < self.micro_vol_ratio
                    or vol_ratio > self.micro_max_vol_ratio
                ):
                    return None
        else:
            return None

        # 4. 全部体检通过，计算价格与止盈止损
        current_price = top1_data["close"]
        limit_price = current_price * (1 - self.entry_pullback)
        tp_price = limit_price * (1 + self.tp_pct)
        sl_price = limit_price * (1 - self.sl_pct)

        # 记录冷却期
        self.cooldown_until[top1_symbol] = current_time_ms + self.cooldown_ms

        # 计算北京时间字符串 (UTC+8)
        time_bj_str = (
            pd.to_datetime(current_time_ms, unit="ms") + pd.Timedelta(hours=8)
        ).strftime("%Y-%m-%d %H:%M")

        # 5. 组装全量信号快照字典
        signal = {
            "signal_time": current_time_ms,
            "signal_time_bj": time_bj_str,
            "symbol": top1_symbol,
            "action": "BUY",
            "current_price": current_price,
            "limit_price": limit_price,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "params": {
                "entry_pullback_pct": self.entry_pullback,
                "take_profit_pct": self.tp_pct,
                "stop_loss_pct": self.sl_pct,
                "timeout_sec": self.timeout_sec,
            },
            "context": {
                "chg_24h": top1_data["chg_24h"],
                "vol_24h": top1_data["vol_24h"],
                "lowest_ndays_price": top1_data["lowest_ndays"],
                "surge_from_lowest": surge,
                "micro_drawdown": drawdown,
                "micro_momentum": momentum,
                "micro_vol_ratio": vol_ratio,
                "mDD_15m": mdd_15,
                "mDD_120m": mdd_120,
            },
            "top_ranks": top_ranks,
        }

        logging.info(
            f"[{time_bj_str} BJ] 猎物锁定: {top1_symbol} | 当前价: {current_price:.4f} | "
            f"挂单价: {limit_price:.4f} | 24h涨幅: {top1_data['chg_24h']*100:.2f}% | "
            f"微观回撤: {drawdown*100:.2f}% | 微观动量: {momentum*100:.2f}% | 量比: {vol_ratio:.2f} | "
            f"mDD_15m: {mdd_15*100:.2f}% | mDD_120m: {mdd_120*100:.2f}%"
        )

        return signal
