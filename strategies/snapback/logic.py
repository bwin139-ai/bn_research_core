import logging
from typing import Any, Dict

import pandas as pd


class WashoutSnapbackStrategy:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

        # 基础过滤
        self.min_24h_vol = self.config["min_24h_quote_vol"]

        # 第一层：对超跌的观察（价 + 量 共振）
        self.drop_window = self.config["drop_window_mins"]
        self.min_drop_pct = self.config["min_drop_pct"]
        self.max_drop_pct = self.config["max_drop_pct"]
        self.vol_climax_window = self.config["vol_climax_window_mins"]
        self.vol_baseline_window = self.config["vol_baseline_window_mins"]
        self.min_vol_ratio = self.config["min_vol_climax_ratio"]

        # 第二层：对反弹的观察（ABC 结构修复比例）
        self.min_rebound_ratio = self.config["min_rebound_ratio"]
        self.max_rebound_ratio = self.config["max_rebound_ratio"]

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

            # 截取回溯视距 (只需要长均量线的长度即可)
            start_idx = max(0, idx - self.vol_baseline_window - 5)
            history_df = sym_df.iloc[start_idx:idx]
            if len(history_df) < self.vol_baseline_window:
                continue

            current_price = row["close"]

            # ==============================
            # 第一层：对超跌的观察
            # CAB 取数顺序：
            # C = 当前收盘；
            # A = 从 C 往前回看 drop_window 根 bars 的最高点；
            # B = A 与 C 之间的最低点。
            # 虽然命名叫 ABC，但数据获取顺序必须严格按 CAB 理解。
            # ==============================
            recent_drop_df = history_df.tail(self.drop_window)
            recent_high_price = recent_drop_df["high"].max()
            drop_pct = (
                (recent_high_price - current_price) / recent_high_price
                if recent_high_price > 0
                else 0
            )
            if drop_pct < self.min_drop_pct:
                continue
            if drop_pct > self.max_drop_pct:
                continue

            vol_climax = (
                history_df["quote_asset_volume"].tail(self.vol_climax_window).mean()
            )
            vol_baseline = (
                history_df["quote_asset_volume"].tail(self.vol_baseline_window).mean()
            )
            vol_ratio = vol_climax / vol_baseline if vol_baseline > 0 else 0
            if vol_ratio < self.min_vol_ratio:
                continue

            # ==============================
            # 第二层：对反弹的观察
            # 用 ABC 结构修复比例替代 PinBar / Engulfing + 固定针深区间。
            # A = recent_high_price
            # B = recent_low_price (限定在 A 与 C 所处窗口之间)
            # C = current_price
            # rebound_ratio = (C - B) / (A - B)
            # ==============================
            b_contract_ts = recent_drop_df["low"].idxmin()
            b_contract_price = recent_drop_df.loc[b_contract_ts, "low"]
            b_index_price = recent_drop_df.loc[b_contract_ts, "low_idx"]
            if pd.isna(b_index_price):
                continue

            extreme_drop_range = recent_high_price - b_index_price
            if extreme_drop_range <= 0:
                continue
            if current_price <= b_index_price:
                continue

            rebound_ratio = (current_price - b_index_price) / extreme_drop_range
            if rebound_ratio < self.min_rebound_ratio:
                continue
            if rebound_ratio > self.max_rebound_ratio:
                continue

            candidates.append(
                {
                    "symbol": sym,
                    "current_price": current_price,
                    "drop_pct": drop_pct,
                    "vol_ratio": vol_ratio,
                    "recent_high_price": recent_high_price,
                    "b_contract_price": b_contract_price,
                    "b_index_price": b_index_price,
                    "rebound_ratio": rebound_ratio,
                    "chg_24h": row["chg_24h"],
                    "vol_24h": row["vol_24h"],
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
        sl_price = target["b_index_price"]

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
                "max_drop_pct": self.max_drop_pct,
                "min_rebound_ratio": self.min_rebound_ratio,
                "max_rebound_ratio": self.max_rebound_ratio,
                "timeout_sec": self.timeout_sec,
            },
            "context": {
                "chg_24h": target["chg_24h"],
                "vol_24h": target["vol_24h"],
                "drop_pct": target["drop_pct"],
                "vol_ratio": target["vol_ratio"],
                "recent_high_price": target["recent_high_price"],
                "b_contract_price": target["b_contract_price"],
                "b_index_price": target["b_index_price"],
                "rebound_ratio": target["rebound_ratio"],
            },
        }

        logging.info(
            f"[{time_bj_str} BJ] 🦅 洗盘反抽雷达锁定: {top1_symbol} | 当前价: {current_price:.4f} | 15m跌幅: {target['drop_pct']*100:.2f}% | 爆量倍数: {target['vol_ratio']:.2f} | ABC反弹比例: {target['rebound_ratio']*100:.2f}%"
        )

        return signal
