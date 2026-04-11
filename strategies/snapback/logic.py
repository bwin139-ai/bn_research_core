import logging
from typing import Any, Dict

import pandas as pd


class WashoutSnapbackStrategy:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

        runtime = self.config["runtime"]
        universe = self.config["universe"]
        structure = self.config["structure"]
        s_to_c_window = structure["s_to_c_window"]
        selloff = structure["selloff"]
        rebound = structure["rebound"]
        basis = structure["basis"]
        joint_filters = structure.get("joint_filters") or {}
        exit_policy = self.config["exit_policy"]
        take_profit = exit_policy["take_profit"]
        strong_mode = take_profit["strong_mode"]
        time_stop = exit_policy["time_stop"]
        risk_controls = self.config["risk_controls"]

        # runtime / benchmark
        self.max_history_window_mins = runtime["max_history_window_mins"]

        # 基础过滤
        self.min_24h_vol = universe["24h_quote_volume_min"]
        self.min_24h_chg = universe["24h_chg_pct"]["min"]
        self.max_24h_chg = universe["24h_chg_pct"]["max"]

        # 第一层：对超跌的观察（价 + 量 共振）
        self.drop_window = s_to_c_window["mins"]
        self.min_drop_window_chg = s_to_c_window["chg_pct"]["min"] / 100.0
        self.max_drop_window_chg = s_to_c_window["chg_pct"]["max"] / 100.0
        self.skip_hot_market_quadrant = bool(s_to_c_window["skip_hot_market_quadrant"])
        self.min_ab_bars = selloff["ab_bars"]["min"]
        self.max_ab_bars = selloff["ab_bars"]["max"]
        self.min_drop_pct = selloff["a_to_c_drop_pct"]["min"]
        self.max_drop_pct = selloff["a_to_c_drop_pct"]["max"]
        self.vol_climax_window = selloff["vol_climax"]["recent_window_mins"]
        self.vol_baseline_window = selloff["vol_climax"]["baseline_window_mins"]
        self.min_vol_ratio = selloff["vol_climax"]["ratio_min"]

        # 第二层：对反弹的观察（唯一结构：ABC_BINDEX）
        self.min_rebound_ratio = rebound["ratio"]["min"]
        self.max_rebound_ratio = rebound["ratio"]["max"]
        self.min_bc_bars = rebound["bc_bars_min"]
        self.min_basis_b_pct = basis["b_pct"]["min"]
        self.max_basis_b_pct = basis["b_pct"]["max"]
        self.min_basis_c_pct = basis["c_pct"]["min"]
        self.max_basis_c_pct = basis["c_pct"]["max"]
        self.min_bc_rebound_speed = float(joint_filters.get("min_bc_rebound_speed", -1e9))
        self.min_speed_ratio_bc_over_ab = float(joint_filters.get("min_speed_ratio_bc_over_ab", -1e9))
        self.min_a_to_b_drop_speed = float(joint_filters.get("min_a_to_b_drop_speed", -1e9))
        self.enable_min_bc_rebound_speed = self.min_bc_rebound_speed >= 0
        self.enable_min_speed_ratio_bc_over_ab = self.min_speed_ratio_bc_over_ab >= 0
        self.enable_min_a_to_b_drop_speed = self.min_a_to_b_drop_speed >= 0

        # 游击战交易参数
        self.base_tp_pct = take_profit["base_pct"]
        self.strong_tp_pct = take_profit["strong_pct"]
        self.strong_tp_min_drop_pct = strong_mode["a_to_c_drop_pct_min"]
        self.strong_tp_min_rebound_ratio = strong_mode["rebound_ratio_min"]
        self.cooldown_ms = risk_controls["cooldown_hours"] * 3600 * 1000
        self.max_hold_mins = time_stop["max_hold_mins"]
        self.time_stop_min_profit = time_stop["min_profit_pct"]

        self.cooldown_until: Dict[str, int] = {}

    def audit_symbols_at_kline_close(
        self,
        current_time_ms: int,
        cross_section: pd.DataFrame,
        active_symbols: set,
        full_df: Dict[str, pd.DataFrame] = None,
        target_symbols: set | None = None,
    ) -> Dict[str, Dict[str, Any]]:
        audits: Dict[str, Dict[str, Any]] = {}
        target_set = {str(s).upper().strip() for s in (target_symbols or set()) if str(s).strip()}
        if not target_set:
            return audits
        if cross_section is None or cross_section.empty or full_df is None:
            for sym in target_set:
                audits[sym] = {
                    "stage5_pass": False,
                    "fail_reason": "cross_section_empty_or_full_df_missing",
                }
            return audits

        for sym in target_set:
            record: Dict[str, Any] = {
                "symbol": sym,
                "stage5_pass": False,
                "fail_reason": "",
            }
            row = cross_section.loc[sym] if sym in cross_section.index else None
            if row is not None:
                record["current_price"] = row["close"]
                record["chg_24h"] = row["chg_24h"]
                record["vol_24h"] = row["vol_24h"]
            if row is None:
                record["fail_reason"] = "symbol_not_in_cross_section"
                audits[sym] = record
                continue
            if pd.isna(row["vol_24h"]) or pd.isna(row["chg_24h"]):
                record["fail_reason"] = "cross_section_nan_metrics"
                audits[sym] = record
                continue
            if row["vol_24h"] < self.min_24h_vol:
                record["fail_reason"] = "vol_24h_below_min"
                audits[sym] = record
                continue
            if row["chg_24h"] * 100 < self.min_24h_chg:
                record["fail_reason"] = "chg_24h_below_min"
                audits[sym] = record
                continue
            if row["chg_24h"] * 100 > self.max_24h_chg:
                record["fail_reason"] = "chg_24h_above_max"
                audits[sym] = record
                continue
            if sym in active_symbols:
                record["fail_reason"] = "symbol_in_active_symbols"
                audits[sym] = record
                continue

            cooldown_until = self.cooldown_until.get(sym, 0)
            record["cooldown_until"] = cooldown_until
            if current_time_ms < cooldown_until:
                record["fail_reason"] = "symbol_in_cooldown"
                audits[sym] = record
                continue

            sym_df = full_df.get(sym)
            if sym_df is None:
                record["fail_reason"] = "symbol_df_missing"
                audits[sym] = record
                continue

            idx = sym_df.index.searchsorted(current_time_ms, side="right")
            record["history_right_index"] = int(idx)
            if idx < self.vol_baseline_window:
                record["fail_reason"] = "history_short_before_baseline"
                audits[sym] = record
                continue

            start_idx = max(0, idx - self.vol_baseline_window - 5)
            history_df = sym_df.iloc[start_idx:idx]
            record["history_rows"] = int(len(history_df))
            if len(history_df) < self.vol_baseline_window:
                record["fail_reason"] = "history_short_after_slice"
                audits[sym] = record
                continue

            current_price = row["close"]
            recent_drop_df = history_df.tail(self.drop_window)
            sc_window_df = history_df.tail(self.drop_window + 1)
            if len(sc_window_df) < self.drop_window + 1:
                record["fail_reason"] = "sc_window_short"
                audits[sym] = record
                continue

            s_ts = sc_window_df.index[0]
            s_close = sc_window_df.iloc[0]["close"]
            record["s_time"] = int(s_ts)
            record["s_close"] = s_close
            if pd.isna(s_close) or s_close <= 0:
                record["fail_reason"] = "invalid_s_close"
                audits[sym] = record
                continue

            drop_window_chg = (current_price - s_close) / s_close
            record["drop_window_chg"] = drop_window_chg
            if drop_window_chg < self.min_drop_window_chg:
                record["fail_reason"] = "drop_window_chg_below_min"
                audits[sym] = record
                continue
            if drop_window_chg > self.max_drop_window_chg:
                record["fail_reason"] = "drop_window_chg_above_max"
                audits[sym] = record
                continue
            if self.skip_hot_market_quadrant and row["chg_24h"] > 0 and drop_window_chg > 0:
                record["fail_reason"] = "hot_market_quadrant_skip"
                audits[sym] = record
                continue

            recent_high_ts = recent_drop_df["high"].idxmax()
            recent_high_price = recent_drop_df.loc[recent_high_ts, "high"]
            ac_df = recent_drop_df.loc[recent_high_ts:]
            record["a_time"] = int(recent_high_ts)
            record["a_high_price"] = recent_high_price
            if ac_df.empty:
                record["fail_reason"] = "ac_df_empty"
                audits[sym] = record
                continue

            drop_pct = ((recent_high_price - current_price) / recent_high_price) if recent_high_price > 0 else 0
            record["drop_pct"] = drop_pct
            if drop_pct < self.min_drop_pct:
                record["fail_reason"] = "drop_pct_below_min"
                audits[sym] = record
                continue
            if drop_pct > self.max_drop_pct:
                record["fail_reason"] = "drop_pct_above_max"
                audits[sym] = record
                continue

            vol_climax = history_df["quote_asset_volume"].tail(self.vol_climax_window).mean()
            vol_baseline = history_df["quote_asset_volume"].tail(self.vol_baseline_window).mean()
            vol_ratio = vol_climax / vol_baseline if vol_baseline > 0 else 0
            record["vol_ratio"] = vol_ratio
            if vol_ratio < self.min_vol_ratio:
                record["fail_reason"] = "vol_ratio_below_min"
                audits[sym] = record
                continue

            b_contract_ts = ac_df["low"].idxmin()
            b_contract_price = ac_df.loc[b_contract_ts, "low"]
            b_index_price = ac_df.loc[b_contract_ts, "low_idx"]
            record["b_time"] = int(b_contract_ts)
            record["b_contract_price"] = b_contract_price
            record["b_index_price"] = b_index_price
            if pd.isna(b_index_price) or b_index_price <= 0:
                record["fail_reason"] = "invalid_b_index_price"
                audits[sym] = record
                continue

            basis_b_pct = (b_contract_price - b_index_price) / b_index_price
            record["basis_b_pct"] = basis_b_pct
            if basis_b_pct < self.min_basis_b_pct:
                record["fail_reason"] = "basis_b_pct_below_min"
                audits[sym] = record
                continue
            if basis_b_pct > self.max_basis_b_pct:
                record["fail_reason"] = "basis_b_pct_above_max"
                audits[sym] = record
                continue

            c_index_price = row["close_idx"]
            record["c_index_price"] = c_index_price
            if pd.isna(c_index_price) or c_index_price <= 0:
                record["fail_reason"] = "invalid_c_index_price"
                audits[sym] = record
                continue

            basis_c_pct = (current_price - c_index_price) / c_index_price
            record["basis_c_pct"] = basis_c_pct
            if basis_c_pct < self.min_basis_c_pct:
                record["fail_reason"] = "basis_c_pct_below_min"
                audits[sym] = record
                continue
            if basis_c_pct > self.max_basis_c_pct:
                record["fail_reason"] = "basis_c_pct_above_max"
                audits[sym] = record
                continue

            extreme_drop_range = recent_high_price - b_index_price
            record["extreme_drop_range"] = extreme_drop_range
            if extreme_drop_range <= 0:
                record["fail_reason"] = "extreme_drop_range_non_positive"
                audits[sym] = record
                continue
            if current_price <= b_index_price:
                record["fail_reason"] = "current_price_below_or_equal_b_index"
                audits[sym] = record
                continue

            b_pos = ac_df.index.get_indexer([b_contract_ts])[0]
            record["b_pos"] = int(b_pos)
            if b_pos < 0:
                record["fail_reason"] = "invalid_b_pos"
                audits[sym] = record
                continue

            ab_bars = b_pos
            record["ab_bars"] = int(ab_bars)
            if ab_bars < self.min_ab_bars:
                record["fail_reason"] = "ab_bars_below_min"
                audits[sym] = record
                continue
            if ab_bars > self.max_ab_bars:
                record["fail_reason"] = "ab_bars_above_max"
                audits[sym] = record
                continue

            ab_drop_pct_index = ((recent_high_price - b_index_price) / recent_high_price) if recent_high_price > 0 else None
            record["ab_drop_pct_index"] = ab_drop_pct_index
            a_to_b_drop_speed = (ab_drop_pct_index / ab_bars) if ab_drop_pct_index is not None and ab_bars > 0 else None
            record["a_to_b_drop_speed"] = a_to_b_drop_speed
            if self.enable_min_a_to_b_drop_speed:
                if a_to_b_drop_speed is None or a_to_b_drop_speed < self.min_a_to_b_drop_speed:
                    record["fail_reason"] = "a_to_b_drop_speed_below_min"
                    audits[sym] = record
                    continue

            bc_bars = (len(ac_df) - 1) - b_pos
            record["bc_bars"] = int(bc_bars)
            if bc_bars < self.min_bc_bars:
                record["fail_reason"] = "bc_bars_below_min"
                audits[sym] = record
                continue

            rebound_ratio = (current_price - b_index_price) / extreme_drop_range
            record["rebound_ratio"] = rebound_ratio
            if rebound_ratio < self.min_rebound_ratio:
                record["fail_reason"] = "rebound_ratio_below_min"
                audits[sym] = record
                continue
            if rebound_ratio > self.max_rebound_ratio:
                record["fail_reason"] = "rebound_ratio_above_max"
                audits[sym] = record
                continue

            c_pos_in_ac_index = rebound_ratio
            record["c_pos_in_ac_index"] = c_pos_in_ac_index
            bc_rebound_pct_index = (current_price - b_index_price) / b_index_price
            record["bc_rebound_pct_index"] = bc_rebound_pct_index
            bc_rebound_speed = (bc_rebound_pct_index / bc_bars) if bc_bars > 0 else None
            record["bc_rebound_speed"] = bc_rebound_speed
            if self.enable_min_bc_rebound_speed:
                if bc_rebound_speed is None or bc_rebound_speed < self.min_bc_rebound_speed:
                    record["fail_reason"] = "bc_rebound_speed_below_min"
                    audits[sym] = record
                    continue

            ab_drop_pct_index = ((recent_high_price - b_index_price) / recent_high_price) if recent_high_price > 0 else None
            record["ab_drop_pct_index"] = ab_drop_pct_index
            ab_drop_speed = (ab_drop_pct_index / ab_bars) if ab_drop_pct_index is not None and ab_bars > 0 else None
            record["ab_drop_speed"] = ab_drop_speed
            speed_ratio_bc_over_ab = (bc_rebound_speed / ab_drop_speed) if (bc_rebound_speed is not None and ab_drop_speed not in (None, 0)) else None
            record["speed_ratio_bc_over_ab"] = speed_ratio_bc_over_ab
            if self.enable_min_speed_ratio_bc_over_ab:
                if speed_ratio_bc_over_ab is None or speed_ratio_bc_over_ab < self.min_speed_ratio_bc_over_ab:
                    record["fail_reason"] = "speed_ratio_bc_over_ab_below_min"
                    audits[sym] = record
                    continue

            trigger_name = "ABC_BINDEX"
            selected_tp_pct = self.base_tp_pct
            tp_tier = "BASE"
            if drop_pct >= self.strong_tp_min_drop_pct and rebound_ratio >= self.strong_tp_min_rebound_ratio:
                selected_tp_pct = self.strong_tp_pct
                tp_tier = "STRONG"

            record.update({
                "stage5_pass": True,
                "fail_reason": "",
                "trigger_name": trigger_name,
                "selected_tp_pct": selected_tp_pct,
                "tp_tier": tp_tier,
            })
            audits[sym] = record

        return audits


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
        cs = cross_section.dropna(subset=["vol_24h", "chg_24h"]).copy()
        cs = cs[cs["vol_24h"] >= self.min_24h_vol]
        cs = cs[cs["chg_24h"] * 100 >= self.min_24h_chg]
        cs = cs[cs["chg_24h"] * 100 <= self.max_24h_chg]
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
            sc_window_df = history_df.tail(self.drop_window + 1)
            if len(sc_window_df) < self.drop_window + 1:
                continue

            s_ts = sc_window_df.index[0]
            s_close = sc_window_df.iloc[0]["close"]
            if pd.isna(s_close) or s_close <= 0:
                continue

            # S 点定义：
            # S = C 点向前数 drop_window_mins 根 K 线的那一点；
            # 若 C.open_time=09:30, drop_window_mins=15，
            # 则 S.open_time=09:15。
            drop_window_chg = (current_price - s_close) / s_close
            if drop_window_chg < self.min_drop_window_chg:
                continue
            if drop_window_chg > self.max_drop_window_chg:
                continue

            # 方向 A：排除第一象限
            # 语义：24h 偏热，且 S->C 窗口也偏热。
            # 这类更像“热中更热”的延续，不属于 Snapback 想抓的
            # “冷却后的反弹起点”。
            if self.skip_hot_market_quadrant and row["chg_24h"] > 0 and drop_window_chg > 0:
                continue

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
            # 唯一结构 = ABC_BINDEX（用 B_index 去失真）
            # ==============================
            trigger_name = None
            b_contract_ts = None
            b_contract_price = None
            b_index_price = None
            rebound_ratio = None

            # 唯一结构：ABC_BINDEX
            b_contract_ts = ac_df["low"].idxmin()
            b_contract_price = ac_df.loc[b_contract_ts, "low"]
            b_index_price = ac_df.loc[b_contract_ts, "low_idx"]
            if pd.isna(b_index_price) or b_index_price <= 0:
                continue

            basis_b_pct = (b_contract_price - b_index_price) / b_index_price
            if basis_b_pct < self.min_basis_b_pct:
                continue
            if basis_b_pct > self.max_basis_b_pct:
                continue

            c_index_price = row["close_idx"]
            if pd.isna(c_index_price) or c_index_price <= 0:
                continue

            basis_c_pct = (current_price - c_index_price) / c_index_price
            if basis_c_pct < self.min_basis_c_pct:
                continue
            if basis_c_pct > self.max_basis_c_pct:
                continue

            extreme_drop_range = recent_high_price - b_index_price
            if extreme_drop_range <= 0:
                continue
            if current_price <= b_index_price:
                continue

            # 注意：
            # 1) ab_bars 的业务语义是 A -> B 之间相隔多少根 bars；
            # 2) bc_bars 的业务语义是 B -> C 之间相隔多少根 bars；
            # 3) b_pos 是 B 在 ac_df 中的位置；ac_df 的第 0 根就是 A。
            b_pos = ac_df.index.get_indexer([b_contract_ts])[0]
            if b_pos < 0:
                continue

            ab_bars = b_pos
            if ab_bars < self.min_ab_bars:
                continue
            if ab_bars > self.max_ab_bars:
                continue

            ab_drop_pct_index = ((recent_high_price - b_index_price) / recent_high_price) if recent_high_price > 0 else None
            a_to_b_drop_speed = (ab_drop_pct_index / ab_bars) if ab_drop_pct_index is not None and ab_bars > 0 else None
            if self.enable_min_a_to_b_drop_speed:
                if a_to_b_drop_speed is None or a_to_b_drop_speed < self.min_a_to_b_drop_speed:
                    continue

            bc_bars = (len(ac_df) - 1) - b_pos
            if bc_bars < self.min_bc_bars:
                continue

            rebound_ratio = (current_price - b_index_price) / extreme_drop_range
            if rebound_ratio < self.min_rebound_ratio:
                continue
            if rebound_ratio > self.max_rebound_ratio:
                continue

            c_pos_in_ac_index = rebound_ratio
            bc_rebound_pct_index = (current_price - b_index_price) / b_index_price
            bc_rebound_speed = (bc_rebound_pct_index / bc_bars) if bc_bars > 0 else None
            if self.enable_min_bc_rebound_speed:
                if bc_rebound_speed is None or bc_rebound_speed < self.min_bc_rebound_speed:
                    continue

            ab_drop_pct_index = ((recent_high_price - b_index_price) / recent_high_price) if recent_high_price > 0 else None
            ab_drop_speed = (ab_drop_pct_index / ab_bars) if ab_drop_pct_index is not None and ab_bars > 0 else None
            speed_ratio_bc_over_ab = (bc_rebound_speed / ab_drop_speed) if (bc_rebound_speed is not None and ab_drop_speed not in (None, 0)) else None
            if self.enable_min_speed_ratio_bc_over_ab:
                if speed_ratio_bc_over_ab is None or speed_ratio_bc_over_ab < self.min_speed_ratio_bc_over_ab:
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
                    "drop_window_chg": drop_window_chg,
                    "vol_ratio": vol_ratio,
                    "recent_high_price": recent_high_price,
                    "s_time": s_ts,
                    "s_close": s_close,
                    "a_time": recent_high_ts,
                    "a_high_price": recent_high_price,
                    "ab_bars": ab_bars,
                    "ab_drop_pct_index": ab_drop_pct_index,
                    "a_to_b_drop_speed": a_to_b_drop_speed,
                    "b_time": b_contract_ts,
                    "bc_bars": bc_bars,
                    "c_time": current_time_ms,
                    "c_price": current_price,
                    "b_contract_price": b_contract_price,
                    "b_index_price": b_index_price,
                    "basis_b_pct": basis_b_pct,
                    "c_index_price": c_index_price,
                    "basis_c_pct": basis_c_pct,
                    "rebound_ratio": rebound_ratio,
                    "c_pos_in_ac_index": c_pos_in_ac_index,
                    "bc_rebound_pct_index": bc_rebound_pct_index,
                    "bc_rebound_speed": bc_rebound_speed,
                    "speed_ratio_bc_over_ab": speed_ratio_bc_over_ab,
                    "trigger_name": trigger_name,
                    "selected_tp_pct": selected_tp_pct,
                    "tp_tier": tp_tier,
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

        # 🚀 核心修复：止盈止损必须基于真实的当前价格 (current_price) 计算
        selected_tp_pct = target["selected_tp_pct"]
        tp_price = current_price * (1 + selected_tp_pct)
        sl_price = target["b_index_price"]

        self.cooldown_until[top1_symbol] = current_time_ms + self.cooldown_ms
        signal_time_ms = current_time_ms + 60000
        signal_time_bj_str = (
            pd.to_datetime(signal_time_ms, unit="ms") + pd.Timedelta(hours=8)
        ).strftime("%Y-%m-%d %H:%M")

        signal = {
            "signal_time": signal_time_ms,
            "signal_time_bj": signal_time_bj_str,
            "symbol": top1_symbol,
            "action": "BUY",
            "current_price": current_price,
            "tp_price": tp_price,
            "sl_price": sl_price,
            "params": {
                "base_take_profit_pct": self.base_tp_pct,
                "strong_take_profit_pct": self.strong_tp_pct,
                "strong_tp_min_drop_pct": self.strong_tp_min_drop_pct,
                "strong_tp_min_rebound_ratio": self.strong_tp_min_rebound_ratio,
                "selected_take_profit_pct": selected_tp_pct,
                "max_drop_pct": self.max_drop_pct,
                "min_drop_window_chg": self.min_drop_window_chg,
                "max_drop_window_chg": self.max_drop_window_chg,
                "min_ab_bars": self.min_ab_bars,
                "max_ab_bars": self.max_ab_bars,
                "min_a_to_b_drop_speed": self.min_a_to_b_drop_speed,
                "min_rebound_ratio": self.min_rebound_ratio,
                "max_rebound_ratio": self.max_rebound_ratio,
                "min_bc_bars": self.min_bc_bars,
                "min_basis_b_pct": self.min_basis_b_pct,
                "max_basis_b_pct": self.max_basis_b_pct,
                "min_basis_c_pct": self.min_basis_c_pct,
                "max_basis_c_pct": self.max_basis_c_pct,
                "min_bc_rebound_speed": self.min_bc_rebound_speed,
                "min_speed_ratio_bc_over_ab": self.min_speed_ratio_bc_over_ab,
                "min_24h_chg": self.min_24h_chg,
                "max_24h_chg": self.max_24h_chg,
            },
            "context": {
                "chg_24h": target["chg_24h"],
                "vol_24h": target["vol_24h"],
                "drop_pct": target["drop_pct"],
                "drop_window_chg": target["drop_window_chg"],
                "vol_ratio": target["vol_ratio"],
                "recent_high_price": target["recent_high_price"],
                "s_time": target["s_time"],
                "s_close": target["s_close"],
                "a_time": target["a_time"],
                "a_high_price": target["a_high_price"],
                "ab_bars": target["ab_bars"],
                "ab_drop_pct_index": target["ab_drop_pct_index"],
                "a_to_b_drop_speed": target["a_to_b_drop_speed"],
                "b_time": target["b_time"],
                "bc_bars": target["bc_bars"],
                "c_time": target["c_time"],
                "c_price": target["c_price"],
                "b_contract_price": target["b_contract_price"],
                "b_index_price": target["b_index_price"],
                "basis_b_pct": target["basis_b_pct"],
                "c_index_price": target["c_index_price"],
                "basis_c_pct": target["basis_c_pct"],
                "rebound_ratio": target["rebound_ratio"],
                "c_pos_in_ac_index": target["c_pos_in_ac_index"],
                "bc_rebound_pct_index": target["bc_rebound_pct_index"],
                "bc_rebound_speed": target["bc_rebound_speed"],
                "speed_ratio_bc_over_ab": target["speed_ratio_bc_over_ab"],
                "trigger_name": target["trigger_name"],
                "selected_tp_pct": target["selected_tp_pct"],
                "tp_tier": target["tp_tier"],
            },
        }

        logging.info(
            f"[{signal_time_bj_str} BJ] 🦅 洗盘反抽雷达锁定: {top1_symbol} | 当前价: {current_price:.4f} | 15m跌幅: {target['drop_pct']*100:.2f}% | 爆量倍数: {target['vol_ratio']:.2f} | ABC反弹比例: {target['rebound_ratio']*100:.2f}% | TP档位: {target['tp_tier']}({target['selected_tp_pct']*100:.2f}%)"
        )

        return signal
