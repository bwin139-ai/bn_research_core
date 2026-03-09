import logging
from typing import Any, Dict

import pandas as pd


class WashoutSnapbackStrategy:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

        # 基础过滤
        self.min_24h_vol = self.config["min_24h_quote_vol"]

        # 🩸 案发现场参数
        self.drop_window = self.config["drop_window_mins"]
        self.min_drop_pct = self.config["min_drop_pct"]
        self.vol_climax_window = self.config["vol_climax_window_mins"]
        self.vol_baseline_window = self.config["vol_baseline_window_mins"]
        self.min_vol_ratio = self.config["min_vol_climax_ratio"]

        # 🦅 猎手出击信号参数
        triggers = self.config["trigger_signals"]
        self.enable_pin_bar = triggers["enable_pin_bar"]
        self.min_lower_shadow_ratio = triggers["min_lower_shadow_ratio"]
        self.enable_engulfing = triggers["enable_engulfing"]
        self.engulfing_req_vol = triggers["engulfing_requires_vol"]

        # 游击战交易参数
        self.entry_pullback = self.config["entry_pullback_pct"]
        self.tp_pct = self.config["take_profit_pct"]
        self.timeout_sec = self.config["order_timeout_sec"]
        self.cooldown_ms = self.config["cooldown_hours"] * 3600 * 1000

        self.cooldown_until: Dict[str, int] = {}

    def on_kline_close(
        self,
        current_time_ms: int,
        cross_section: pd.DataFrame,
        active_symbols: set,
        full_df: Dict[str, pd.DataFrame] = None,
    ) -> dict:

        if cross_section.empty or full_df is None:
            return None

        # 1. 过滤垃圾币种，保证流动性底线
        cs = cross_section.dropna(subset=["vol_24h"]).copy()
        cs = cs[cs["vol_24h"] >= self.min_24h_vol]
        if cs.empty:
            return None

        candidates = []

        # 2. 全局扫街：寻找案发现场
        for sym, row in cs.iterrows():
            if sym in active_symbols:
                continue
            if current_time_ms < self.cooldown_until.get(sym, 0):
                continue

            sym_df = full_df.get(sym)
            if sym_df is None:
                continue

            # O(1) 极速二分定位
            idx = sym_df.index.searchsorted(current_time_ms, side="right")
            if idx < self.vol_baseline_window:
                continue

            current_price = row["close"]
            curr_k = sym_df.iloc[idx - 1]
            prev_k = sym_df.iloc[idx - 2]

            # 🚀 提速3倍：直接读取引擎下发的预计算数据，时间复杂度从 O(N) 降为 O(1)
            highest_price = curr_k.get("roll_max_15", 0)
            needle_low = curr_k.get("roll_min_15", 0)
            vol_climax = curr_k.get("roll_vol_5", 0)
            vol_baseline = curr_k.get("roll_vol_120", 0)

            if highest_price == 0 or vol_baseline == 0 or needle_low == 0:
                continue

            drop_pct = (highest_price - current_price) / highest_price
            if drop_pct < self.min_drop_pct:
                continue

            # 🚀 V2 黄金区间防守过滤：太浅容易扫，太深是接飞刀
            sl_dist_pct = (needle_low / current_price) - 1.0
            min_sl_dist = -self.config.get("max_needle_depth_pct", 0.10)
            max_sl_dist = -self.config.get("min_needle_depth_pct", 0.025)
            if not (min_sl_dist <= sl_dist_pct <= max_sl_dist):
                continue

            vol_ratio = vol_climax / vol_baseline
            if vol_ratio < self.min_vol_ratio:
                continue

            # ==========================================
            # 🦅 核心形态审查
            # ==========================================

            trigger_matched = False
            trigger_name = ""

            # 形态 1: 擎天一柱 (Pin Bar)
            if self.enable_pin_bar:
                k_range = curr_k["high"] - curr_k["low"]
                if k_range > 0:
                    lower_shadow = min(curr_k["open"], curr_k["close"]) - curr_k["low"]
                    if (lower_shadow / k_range) >= self.min_lower_shadow_ratio:
                        trigger_matched = True
                        trigger_name = "PinBar"

            # 形态 2: 阳包阴吞噬 (Engulfing)
            if not trigger_matched and self.enable_engulfing:
                prev_is_red = prev_k["close"] < prev_k["open"]
                curr_is_green = curr_k["close"] > curr_k["open"]
                if prev_is_red and curr_is_green:
                    # 现价高于前阴开盘，且低位接住了前阴收盘
                    if (
                        curr_k["close"] > prev_k["open"]
                        and curr_k["open"] <= prev_k["close"]
                    ):
                        if not self.engulfing_req_vol or (
                            curr_k["quote_asset_volume"] > prev_k["quote_asset_volume"]
                        ):
                            trigger_matched = True
                            trigger_name = "Engulfing"

            # 信号确立，推入候选池
            if trigger_matched:
                candidates.append(
                    {
                        "symbol": sym,
                        "current_price": current_price,
                        "drop_pct": drop_pct,
                        "vol_ratio": vol_ratio,
                        "trigger": trigger_name,
                        "chg_24h": row["chg_24h"],
                        "vol_24h": row["vol_24h"],
                        "needle_low": needle_low,
                    }
                )

        if not candidates:
            return None

        # 3. 如果同时有多个币暴跌并发出反转信号，选跌得最惨的那个去救
        candidates.sort(key=lambda x: x["drop_pct"], reverse=True)
        target = candidates[0]

        top1_symbol = target["symbol"]
        current_price = target["current_price"]

        limit_price = current_price * (1 - self.entry_pullback)
        # 🚀 核心修复：止盈止损必须基于真实的当前价格 (current_price) 计算，不能受追高/回踩限价的影响
        tp_price = current_price * (1 + self.tp_pct)
        sl_price = current_price * (1 - self.sl_pct)

        self.cooldown_until[top1_symbol] = current_time_ms + self.cooldown_ms
        time_bj_str = (
            pd.to_datetime(current_time_ms, unit="ms") + pd.Timedelta(hours=8)
        ).strftime("%Y-%m-%d %H:%M")

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
                "stop_loss_pct": (target["needle_low"] / target["current_price"]) - 1.0,
                "timeout_sec": self.timeout_sec,
            },
            "context": {
                "chg_24h": target["chg_24h"],
                "vol_24h": target["vol_24h"],
                "drop_pct": target["drop_pct"],
                "vol_ratio": target["vol_ratio"],
                "trigger_type": target["trigger"],
                "needle_low": target["needle_low"],
            },
        }

        logging.info(
            f"[{time_bj_str} BJ] 🦅 洗盘反抽雷达锁定: {top1_symbol} | 信号: {target['trigger']} | 当前价: {current_price:.4f} | 15m跌幅: {target['drop_pct']*100:.2f}% | 爆量倍数: {target['vol_ratio']:.2f}"
        )

        return signal
