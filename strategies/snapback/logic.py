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

        # 第二层：对反弹的观察（branch 1: ABC 修复 / branch 2: 流动性失真单针）
        self.min_rebound_ratio = self.config["min_rebound_ratio"]
        self.max_rebound_ratio = self.config["max_rebound_ratio"]
        self.min_distortion_wick_ratio = self.config["min_distortion_wick_ratio"]
        self.min_basis_spike_pct = self.config["min_basis_spike_pct"]
        self.max_basis_close_pct = self.config["max_basis_close_pct"]
        self.min_bc_bars = self.config["min_bc_bars"]

        # 游击战交易参数
        self.entry_pullback = self.config["entry_pullback_pct"]
        self.base_tp_pct = self.config["base_take_profit_pct"]
        self.strong_tp_pct = self.config["strong_take_profit_pct"]
        self.strong_tp_min_drop_pct = self.config["strong_tp_min_drop_pct"]
        self.strong_tp_min_rebound_ratio = self.config["strong_tp_min_rebound_ratio"]
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
            time_bj_str = (
                pd.to_datetime(current_time_ms, unit="ms") + pd.Timedelta(hours=8)
            ).strftime("%Y-%m-%d %H:%M")

            # ==============================
            # 第一层：对超跌的观察
            # CAB 取数顺序：
            # C = 当前收盘；
            # A = 从 C 往前回看 drop_window 根 bars 的最高点；
            # B = A 与 C 之间的最低点。
            # 虽然命名叫 ABC，但数据获取顺序必须严格按 CAB 理解。
            # ==============================
            recent_drop_df = history_df.tail(self.drop_window)
            recent_high_ts = recent_drop_df["high"].idxmax()
            recent_high_price = recent_drop_df.loc[recent_high_ts, "high"]
            # 真正的 ABC 语义：
            # A = drop_window 窗口内最高点；
            # C = 当前收盘；
            # B = A 与 C 之间的最低点。
            # 因此 branch1 后续找 B 时，必须只在 [A, C] 区间内取最低点，
            # 不能再在整个 recent_drop_df 窗口上取最低点。
            ac_df = recent_drop_df.loc[recent_high_ts:]
            if ac_df.empty:
                continue

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
            # branch 1 = ABC 结构修复比例（用 B_index 去失真）
            # branch 2 = 流动性失真单针（当前 bar 既是 B 也是 C）
            # 二者为 OR 关系，任一成立即可通过反弹层。
            # ==============================
            trigger_name = None
            b_contract_ts = None
            b_contract_price = None
            b_index_price = None
            rebound_ratio = None
            basis_spike_pct = None
            basis_close_pct = None
            wick_ratio = None

            current_bar = recent_drop_df.iloc[-1]
            current_low = current_bar["low"]
            current_low_idx = current_bar.get("low_idx")
            current_close_idx = current_bar.get("close_idx")
            current_open = current_bar["open"]
            current_high = current_bar["high"]

            # branch 2：流动性失真单针（效率优先，先判断）
            price_range = current_high - current_low
            lower_shadow = min(current_open, current_price) - current_low
            wick_ratio = (lower_shadow / price_range) if price_range > 0 else 0

            if (
                current_low == recent_drop_df["low"].min()
                and pd.notna(current_low_idx)
                and pd.notna(current_close_idx)
                and current_low_idx > 0
                and current_close_idx > 0
            ):
                basis_spike_pct = (current_low_idx - current_low) / current_low_idx
                basis_close_pct = (current_close_idx - current_price) / current_close_idx
                if (
                    wick_ratio >= self.min_distortion_wick_ratio
                    and basis_spike_pct >= self.min_basis_spike_pct
                    and abs(basis_close_pct) <= self.max_basis_close_pct
                    and basis_close_pct < basis_spike_pct
                ):
                    trigger_name = "DISTORTION_PIN"
                    b_contract_ts = current_bar.name
                    b_contract_price = current_low
                    b_index_price = current_low_idx

            # branch 1：ABC 结构修复比例
            if trigger_name is None:
                b_contract_ts = ac_df["low"].idxmin()
                b_contract_price = ac_df.loc[b_contract_ts, "low"]
                b_index_price = ac_df.loc[b_contract_ts, "low_idx"]
                if pd.isna(b_index_price):
                    continue

                extreme_drop_range = recent_high_price - b_index_price
                if extreme_drop_range <= 0:
                    continue
                if current_price <= b_index_price:
                    continue

                bc_bars = len(ac_df) - 1
                if bc_bars < self.min_bc_bars:
                    continue

                rebound_ratio = (current_price - b_index_price) / extreme_drop_range
                if rebound_ratio < self.min_rebound_ratio:
                    continue
                if rebound_ratio > self.max_rebound_ratio:
                    continue
                trigger_name = "ABC_BINDEX"

            selected_tp_pct = self.base_tp_pct
            tp_tier = "BASE"
            if (
                trigger_name == "ABC_BINDEX"
                and drop_pct >= self.strong_tp_min_drop_pct
                and rebound_ratio is not None
                and rebound_ratio >= self.strong_tp_min_rebound_ratio
            ):
                selected_tp_pct = self.strong_tp_pct
                tp_tier = "STRONG"

            candidates.append(
                {
                    "symbol": sym,
                    "current_price": current_price,
                    "drop_pct": drop_pct,
                    "vol_ratio": vol_ratio,
                    "recent_high_price": recent_high_price,
                    "a_time": recent_high_ts,
                    "a_high_price": recent_high_price,
                    "b_time": b_contract_ts,
                    "c_time": current_time_ms,
                    "c_price": current_price,
                    "b_contract_price": b_contract_price,
                    "b_index_price": b_index_price,
                    "rebound_ratio": rebound_ratio,
                    "trigger_name": trigger_name,
                    "selected_tp_pct": selected_tp_pct,
                    "tp_tier": tp_tier,
                    "basis_spike_pct": basis_spike_pct,
                    "basis_close_pct": basis_close_pct,
                    "wick_ratio": wick_ratio,
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
        selected_tp_pct = target["selected_tp_pct"]
        tp_price = current_price * (1 + selected_tp_pct)
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
                "base_take_profit_pct": self.base_tp_pct,
                "strong_take_profit_pct": self.strong_tp_pct,
                "strong_tp_min_drop_pct": self.strong_tp_min_drop_pct,
                "strong_tp_min_rebound_ratio": self.strong_tp_min_rebound_ratio,
                "selected_take_profit_pct": selected_tp_pct,
                "max_drop_pct": self.max_drop_pct,
                "min_rebound_ratio": self.min_rebound_ratio,
                "max_rebound_ratio": self.max_rebound_ratio,
                "min_distortion_wick_ratio": self.min_distortion_wick_ratio,
                "min_basis_spike_pct": self.min_basis_spike_pct,
                "max_basis_close_pct": self.max_basis_close_pct,
                "min_bc_bars": self.min_bc_bars,
                "timeout_sec": self.timeout_sec,
            },
            "context": {
                "chg_24h": target["chg_24h"],
                "vol_24h": target["vol_24h"],
                "drop_pct": target["drop_pct"],
                "vol_ratio": target["vol_ratio"],
                "recent_high_price": target["recent_high_price"],
                "a_time": target["a_time"],
                "a_high_price": target["a_high_price"],
                "b_time": target["b_time"],
                "c_time": target["c_time"],
                "c_price": target["c_price"],
                "b_contract_price": target["b_contract_price"],
                "b_index_price": target["b_index_price"],
                "rebound_ratio": target["rebound_ratio"],
                "trigger_name": target["trigger_name"],
                "selected_tp_pct": target["selected_tp_pct"],
                "tp_tier": target["tp_tier"],
                "basis_spike_pct": target["basis_spike_pct"],
                "basis_close_pct": target["basis_close_pct"],
                "wick_ratio": target["wick_ratio"],
            },
        }

        if target["trigger_name"] == "DISTORTION_PIN":
            logging.info(
                f"[{time_bj_str} BJ] 🦅 洗盘反抽雷达锁定: {top1_symbol} | 当前价: {current_price:.4f} | 15m跌幅: {target['drop_pct']*100:.2f}% | 爆量倍数: {target['vol_ratio']:.2f} | 失真单针: wick={target['wick_ratio']*100:.2f}% | 针尖基差: {target['basis_spike_pct']*100:.2f}% | 收盘基差: {target['basis_close_pct']*100:.2f}% | TP档位: {target['tp_tier']}({target['selected_tp_pct']*100:.2f}%)"
            )
        else:
            logging.info(
                f"[{time_bj_str} BJ] 🦅 洗盘反抽雷达锁定: {top1_symbol} | 当前价: {current_price:.4f} | 15m跌幅: {target['drop_pct']*100:.2f}% | 爆量倍数: {target['vol_ratio']:.2f} | ABC反弹比例: {target['rebound_ratio']*100:.2f}% | TP档位: {target['tp_tier']}({target['selected_tp_pct']*100:.2f}%)"
            )

        return signal
