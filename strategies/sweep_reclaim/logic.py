import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

from strategies.spring.logic import SpringSABCStrategy


class SweepReclaimStrategy(SpringSABCStrategy):
    """sweep-reclaim sim strategy.

    SWR reuses the Spring strong TopN universe election, then applies its own
    H -> gamma -> B -> C structure. It is LONG-only and contract-bars only.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.strategy_name = str(config["strategy_name"])

        runtime = config["runtime"]
        universe = config["universe"]
        structure = config["structure"]
        exit_policy = config["exit_policy"]
        risk_controls = config["risk_controls"]

        self.bar_interval = str(runtime["bar_interval"])
        self.max_history_window_mins = int(runtime["max_history_window_mins"])

        self.exclude_symbols = {
            str(x).upper().strip() for x in universe["exclude_symbols"] if str(x).strip()
        }
        self.min_24h_chg_pct = float(universe["min_24h_chg_pct"])
        self.min_24h_quote_volume = float(universe["min_24h_quote_volume"])
        self.score_top_n = int(universe["score_top_n"])

        self.support_window_mins = int(structure["support_window_mins"])
        self.hb_drop_min = float(structure["hb_drop"]["min"])
        rebound = structure["rebound"]
        self.bc_rebound_min = float(rebound["bc_rebound_min"])
        self.bc_rebound_max = float(rebound["bc_rebound_max"])
        self.hb_bars_min = int(rebound["hb_bars_min"])
        self.bc_bars_min = int(rebound["bc_bars_min"])
        self.bc_bars_max = int(rebound["bc_bars_max"])
        self.bc_over_hb_bars_max = float(rebound["bc_over_hb_bars_max"])
        self.vol_climax_ratio_min = float(structure["vol_climax"]["ratio_min"])

        self.stop_loss_anchor = str(exit_policy["stop_loss_anchor"])
        self.take_profit_r_multiple = float(exit_policy["take_profit_r_multiple"])
        self.max_hold_mins = int(exit_policy["max_hold_mins"])
        self.time_stop_min_profit_pct = float(exit_policy["time_stop_min_profit_pct"])
        breakeven_guard = exit_policy["breakeven_guard"]
        self.breakeven_guard_enabled = bool(breakeven_guard["enabled"])
        self.breakeven_guard_trigger_r = float(breakeven_guard["trigger_r"])
        self.breakeven_guard_floor_r = float(breakeven_guard["floor_r"])

        self.cooldown_hours = float(risk_controls["cooldown_hours"])
        self.base_order_notional_usdt = float(risk_controls["base_order_notional_usdt"])
        self.full_notional_risk_pct = float(risk_controls["full_notional_risk_pct"])
        self.cooldown_ms = int(round(self.cooldown_hours * 3600_000))
        self.cooldown_until: Dict[str, int] = {}

        self._last_universe_candidates: List[Dict[str, Any]] = []
        self._last_universe_audits: Dict[str, Dict[str, Any]] = {}
        self._last_structure_candidates: List[Dict[str, Any]] = []
        self._last_structure_audits: Dict[str, Dict[str, Any]] = {}
        self._last_signal: Optional[Dict[str, Any]] = None
        self._last_signal_audits: Dict[str, Dict[str, Any]] = {}
        self._prepared_history_cache: Dict[str, Tuple[int, pd.DataFrame]] = {}

    def build_signal_lock_log(self, signal: Dict[str, Any]) -> str:
        context = dict(signal.get("context") or {})
        signal_time_bj = signal.get("signal_time_bj") or self._bj_from_ms(int(signal["signal_time"]))
        return (
            f"[{signal_time_bj} BJ] 📈 SWR雷达锁定: {signal['symbol']}"
            f" | H: {self._bj_short_from_ms(int(context['h_time_ms']))} close={self._price_text(context.get('h_close'))}"
            f" | B: {self._bj_short_from_ms(int(context['b_time_ms']))} low={self._price_text(context.get('b_low'))}"
            f" | C: {self._bj_short_from_ms(int(context['c_time_ms']))} close={self._price_text(context.get('c_close'))}"
            f" | HB跌幅: {self._pct_text(context.get('hb_drop'))}"
            f" | 修复: {self._pct_text(context.get('bc_rebound'))}"
            f" | 速度: {self._safe_float(context.get('bc_over_hb_bars'), 0.0):.2f}"
            f" | 爆量: {self._safe_float(context.get('vol_climax'), 0.0):.2f}"
            f" | 评分: {int(context.get('score', 0))} (#{int(context.get('score_order', 0))})"
        )

    def build_entry_log(self, signal: Dict[str, Any]) -> str:
        signal_time_bj = signal.get("signal_time_bj") or self._bj_from_ms(int(signal["signal_time"]))
        return (
            f"[{signal_time_bj} BJ] 📈 SWR结构信号: {signal['symbol']}"
            f" | 满额金额: {self._safe_float(signal.get('base_order_notional_usdt'), 0.0):.2f}U"
            f" | TP: {self.take_profit_r_multiple:.2f}R"
            f" | 止损: {self._price_text(signal.get('sl_price'))}"
        )

    def _evaluate_structure_for_symbol(
        self,
        symbol: str,
        current_time_ms: int,
        universe_rec: Dict[str, Any],
        *,
        full_df: Any,
    ) -> Dict[str, Any]:
        rec = dict(universe_rec)
        rec["structure_pass"] = False
        rec["h_time_ms"] = None
        rec["gamma_time_ms"] = None
        rec["b_time_ms"] = None
        rec["c_time_ms"] = None
        rec["support_window_mins"] = self.support_window_mins

        sym_df = self._extract_symbol_history(full_df, symbol)
        if sym_df is None:
            rec["fail_reason"] = "missing_symbol_history"
            return rec

        hist = self._prepare_history(symbol, sym_df, current_time_ms)
        if hist is None or hist.empty:
            rec["fail_reason"] = "empty_symbol_history"
            return rec

        required_columns = ("open", "high", "low", "close", "quote_asset_volume")
        missing_columns = [col for col in required_columns if col not in hist.columns]
        if missing_columns:
            rec["fail_reason"] = "missing_required_columns"
            rec["missing_columns"] = missing_columns
            return rec

        support_df = hist.tail(self.support_window_mins).copy()
        if len(support_df) < self.support_window_mins:
            rec["fail_reason"] = "support_window_insufficient_bars"
            rec["support_window_bars"] = int(len(support_df))
            return rec

        numeric_df = support_df.loc[:, required_columns].apply(pd.to_numeric, errors="coerce")
        if numeric_df.isna().any().any():
            rec["fail_reason"] = "support_window_numeric_nan"
            return rec

        close_values = [float(x) for x in numeric_df["close"].tolist()]
        low_values = [float(x) for x in numeric_df["low"].tolist()]
        vol_values = [float(x) for x in numeric_df["quote_asset_volume"].tolist()]
        time_values = [int(x) for x in support_df.index.tolist()]
        c_idx = len(support_df) - 1
        c_close = close_values[c_idx]
        c_time_ms = time_values[c_idx]

        min_low = min(low_values)
        b_candidates = [
            idx for idx, value in enumerate(low_values) if abs(float(value) - float(min_low)) <= 1e-12
        ]
        if not b_candidates:
            rec["fail_reason"] = "b_low_not_found"
            return rec
        b_idx = max(b_candidates)
        bars_bc = c_idx - b_idx
        if bars_bc < self.bc_bars_min:
            rec["fail_reason"] = "bc_bars_below_min"
            rec["bc_bars"] = int(bars_bc)
            rec["c_time_ms"] = c_time_ms
            rec["c_close"] = float(c_close)
            return rec
        if bars_bc > self.bc_bars_max:
            rec["fail_reason"] = "bc_bars_above_max"
            rec["bc_bars"] = int(bars_bc)
            rec["c_time_ms"] = c_time_ms
            rec["c_close"] = float(c_close)
            return rec

        b_low = low_values[b_idx]
        b_close = close_values[b_idx]
        if c_close <= b_close:
            rec["fail_reason"] = "c_close_not_above_b_close"
            rec["b_time_ms"] = time_values[b_idx]
            rec["c_time_ms"] = c_time_ms
            rec["b_close"] = float(b_close)
            rec["c_close"] = float(c_close)
            return rec

        if b_idx <= 0:
            rec["fail_reason"] = "missing_h_left_window"
            rec["b_time_ms"] = time_values[b_idx]
            rec["c_time_ms"] = c_time_ms
            return rec

        left_close_values = close_values[:b_idx]
        h_close = max(left_close_values)
        h_candidates = [
            idx for idx, value in enumerate(left_close_values) if abs(float(value) - float(h_close)) <= 1e-12
        ]
        h_idx = max(h_candidates)
        bars_hb = b_idx - h_idx
        if bars_hb < self.hb_bars_min:
            rec["fail_reason"] = "hb_bars_below_min"
            rec["h_time_ms"] = time_values[h_idx]
            rec["b_time_ms"] = time_values[b_idx]
            rec["bars_hb"] = int(bars_hb)
            return rec
        if h_close <= 0 or h_close <= b_low:
            rec["fail_reason"] = "invalid_h_close_or_b_low"
            rec["h_close"] = float(h_close)
            rec["b_low"] = float(b_low)
            return rec

        hb_drop = (h_close - b_low) / h_close
        if hb_drop < self.hb_drop_min:
            rec["fail_reason"] = "hb_drop_below_min"
            rec["hb_drop"] = float(hb_drop)
            rec["hb_drop_min"] = float(self.hb_drop_min)
            return rec

        bc_rebound = (c_close - b_low) / (h_close - b_low)
        if bc_rebound < self.bc_rebound_min:
            rec["fail_reason"] = "bc_rebound_below_min"
            rec["bc_rebound"] = float(bc_rebound)
            return rec
        if bc_rebound > self.bc_rebound_max:
            rec["fail_reason"] = "bc_rebound_above_max"
            rec["bc_rebound"] = float(bc_rebound)
            return rec

        bc_over_hb_bars = float(bars_bc) / float(bars_hb)
        if bc_over_hb_bars > self.bc_over_hb_bars_max:
            rec["fail_reason"] = "bc_over_hb_bars_above_max"
            rec["bc_over_hb_bars"] = float(bc_over_hb_bars)
            return rec

        gamma_idx = b_idx - bars_bc
        if gamma_idx <= h_idx:
            rec["fail_reason"] = "gamma_not_right_of_h"
            rec["h_time_ms"] = time_values[h_idx]
            rec["gamma_time_ms"] = time_values[gamma_idx] if gamma_idx >= 0 else None
            rec["b_time_ms"] = time_values[b_idx]
            rec["c_time_ms"] = c_time_ms
            return rec

        h_gamma_vol_values = vol_values[h_idx + 1 : gamma_idx + 1]
        gamma_c_vol_values = vol_values[gamma_idx + 1 : c_idx + 1]
        if not h_gamma_vol_values or not gamma_c_vol_values:
            rec["fail_reason"] = "volume_window_empty"
            return rec
        h_gamma_avg_vol = float(sum(h_gamma_vol_values) / float(len(h_gamma_vol_values)))
        gamma_c_avg_vol = float(sum(gamma_c_vol_values) / float(len(gamma_c_vol_values)))
        if h_gamma_avg_vol <= 0:
            rec["fail_reason"] = "h_gamma_avg_volume_nonpositive"
            return rec
        vol_climax = gamma_c_avg_vol / h_gamma_avg_vol
        if vol_climax < self.vol_climax_ratio_min:
            rec["fail_reason"] = "vol_climax_below_min"
            rec["vol_climax"] = float(vol_climax)
            return rec

        rec.update(
            {
                "structure_pass": True,
                "fail_reason": "structure_pass",
                "selected_for_structure": True,
                "support_window_bars": int(len(support_df)),
                "h_time_ms": int(time_values[h_idx]),
                "gamma_time_ms": int(time_values[gamma_idx]),
                "b_time_ms": int(time_values[b_idx]),
                "c_time_ms": int(c_time_ms),
                "h_close": float(h_close),
                "b_close": float(b_close),
                "b_low": float(b_low),
                "c_close": float(c_close),
                "bars_hb": int(bars_hb),
                "bc_bars": int(bars_bc),
                "bc_over_hb_bars": float(bc_over_hb_bars),
                "hb_drop": float(hb_drop),
                "hb_drop_min": float(self.hb_drop_min),
                "bc_rebound": float(bc_rebound),
                "bc_rebound_min": float(self.bc_rebound_min),
                "bc_rebound_max": float(self.bc_rebound_max),
                "h_gamma_avg_vol": float(h_gamma_avg_vol),
                "gamma_c_avg_vol": float(gamma_c_avg_vol),
                "vol_climax": float(vol_climax),
                "vol_climax_ratio_min": float(self.vol_climax_ratio_min),
                "volume_column": "quote_asset_volume",
                "stop_loss_price": float(b_close),
                "swr_selection_mode": "support_window_low_nearest_c",
                "h_selection_mode": "left_of_b_highest_close_nearest_b",
            }
        )
        return rec

    def _build_signal_from_candidates(
        self,
        current_time_ms: int,
        cross_section: pd.DataFrame,
        active_symbols: Set[str],
        structure_candidates: List[Dict[str, Any]],
        structure_audits: Dict[str, Dict[str, Any]],
        *,
        commit_cooldown: bool = True,
    ) -> Optional[Dict[str, Any]]:
        normalized_active = {
            self._norm_symbol(s) for s in (active_symbols or set()) if self._norm_symbol(s)
        }
        ordered = sorted(
            structure_candidates,
            key=lambda x: (int(x.get("score_order", 10**9)), float(x.get("score", 10**9))),
        )

        for candidate in ordered:
            symbol = self._norm_symbol(candidate["symbol"])
            audit_rec = structure_audits.get(symbol)
            cooldown_until = self._cooldown_until_for(symbol)
            if cooldown_until > int(current_time_ms):
                if audit_rec is not None:
                    audit_rec["signal_emit"] = False
                    audit_rec["cooldown_active"] = True
                    audit_rec["cooldown_until"] = cooldown_until
                    audit_rec["cooldown_until_bj"] = self._bj_from_ms(cooldown_until)
                    audit_rec["signal_fail_reason"] = "cooldown_active"
                continue
            if symbol in normalized_active:
                if audit_rec is not None:
                    audit_rec["signal_emit"] = False
                    audit_rec["signal_fail_reason"] = "symbol_in_active_symbols"
                continue

            sl_price = float(candidate["stop_loss_price"])
            signal_time_ms = int(current_time_ms)
            signal = {
                "signal_time": signal_time_ms,
                "signal_time_bj": self._bj_from_ms(signal_time_ms),
                "symbol": symbol,
                "action": "BUY",
                "sl_price": float(sl_price),
                "params": {
                    "take_profit_mode": "risk_reward_r_multiple",
                    "take_profit_r_multiple": float(self.take_profit_r_multiple),
                    "max_hold_mins": self.max_hold_mins,
                    "time_stop_min_profit_pct": self.time_stop_min_profit_pct,
                    "stop_loss_anchor": self.stop_loss_anchor,
                    "breakeven_guard_enabled": self.breakeven_guard_enabled,
                    "breakeven_guard_trigger_r": self.breakeven_guard_trigger_r,
                    "breakeven_guard_floor_r": self.breakeven_guard_floor_r,
                    "cooldown_hours": self.cooldown_hours,
                    "base_order_notional_usdt": self.base_order_notional_usdt,
                    "full_notional_risk_pct": self.full_notional_risk_pct,
                    "support_window_mins": self.support_window_mins,
                    "hb_drop_min": self.hb_drop_min,
                    "bc_rebound_min": self.bc_rebound_min,
                    "bc_rebound_max": self.bc_rebound_max,
                    "bc_over_hb_bars_max": self.bc_over_hb_bars_max,
                    "vol_climax_ratio_min": self.vol_climax_ratio_min,
                },
                "risk_budget_pct": float(self.full_notional_risk_pct),
                "base_order_notional_usdt": float(self.base_order_notional_usdt),
                "context": {
                    "strategy_name": self.strategy_name,
                    "rank_chg_24h": int(candidate.get("rank_chg_24h", 0)),
                    "rank_vol_24h": int(candidate.get("rank_vol_24h", 0)),
                    "score_rank_all": int(candidate.get("score_rank_all", 0)),
                    "selected_score_order": int(candidate.get("selected_score_order", candidate.get("score_order", 0))),
                    "score_order": int(candidate.get("score_order", 0)),
                    "score_top_n": int(candidate.get("score_top_n", self.score_top_n)),
                    "selected_for_structure": bool(candidate.get("selected_for_structure", True)),
                    "universe_hard_gate_pass": bool(candidate.get("universe_hard_gate_pass", True)),
                    "score": int(candidate.get("score", 0)),
                    "chg_24h": float(candidate.get("chg_24h", 0.0)),
                    "vol_24h": float(candidate.get("vol_24h", 0.0)),
                    "support_window_mins": int(self.support_window_mins),
                    "h_time_ms": int(candidate["h_time_ms"]),
                    "gamma_time_ms": int(candidate["gamma_time_ms"]),
                    "b_time_ms": int(candidate["b_time_ms"]),
                    "c_time_ms": int(candidate["c_time_ms"]),
                    "h_close": float(candidate["h_close"]),
                    "b_close": float(candidate["b_close"]),
                    "b_low": float(candidate["b_low"]),
                    "c_close": float(candidate["c_close"]),
                    "bars_hb": int(candidate["bars_hb"]),
                    "bc_bars": int(candidate["bc_bars"]),
                    "bc_over_hb_bars": float(candidate["bc_over_hb_bars"]),
                    "hb_drop": float(candidate["hb_drop"]),
                    "hb_drop_min": float(candidate["hb_drop_min"]),
                    "bc_rebound": float(candidate["bc_rebound"]),
                    "bc_rebound_min": float(candidate["bc_rebound_min"]),
                    "bc_rebound_max": float(candidate["bc_rebound_max"]),
                    "h_gamma_avg_vol": float(candidate["h_gamma_avg_vol"]),
                    "gamma_c_avg_vol": float(candidate["gamma_c_avg_vol"]),
                    "vol_climax": float(candidate["vol_climax"]),
                    "vol_climax_ratio_min": float(candidate["vol_climax_ratio_min"]),
                    "volume_column": "quote_asset_volume",
                    "stop_loss_price": float(sl_price),
                    "base_order_notional_usdt": self.base_order_notional_usdt,
                    "full_notional_risk_pct": self.full_notional_risk_pct,
                    "take_profit_mode": "risk_reward_r_multiple",
                    "take_profit_r_multiple": float(self.take_profit_r_multiple),
                    "breakeven_guard_enabled": self.breakeven_guard_enabled,
                    "breakeven_guard_trigger_r": self.breakeven_guard_trigger_r,
                    "breakeven_guard_floor_r": self.breakeven_guard_floor_r,
                    "swr_selection_mode": candidate.get("swr_selection_mode"),
                    "h_selection_mode": candidate.get("h_selection_mode"),
                },
            }
            cooldown_until_after_signal = signal_time_ms + self.cooldown_ms if self.cooldown_ms > 0 else 0
            if commit_cooldown and cooldown_until_after_signal > 0:
                self.cooldown_until[symbol] = cooldown_until_after_signal
            if audit_rec is not None:
                audit_rec["signal_emit"] = True
                audit_rec["signal_fail_reason"] = None
                audit_rec["signal_time"] = signal_time_ms
                audit_rec["signal_time_bj"] = signal["signal_time_bj"]
                audit_rec["sl_price"] = float(sl_price)
                audit_rec["take_profit_mode"] = "risk_reward_r_multiple"
                audit_rec["take_profit_r_multiple"] = float(self.take_profit_r_multiple)
                audit_rec["base_order_notional_usdt"] = self.base_order_notional_usdt
                audit_rec["full_notional_risk_pct"] = self.full_notional_risk_pct
                audit_rec["cooldown_until_after_signal"] = (
                    cooldown_until_after_signal if cooldown_until_after_signal > 0 else None
                )
                audit_rec["cooldown_until_after_signal_bj"] = (
                    self._bj_from_ms(cooldown_until_after_signal)
                    if cooldown_until_after_signal > 0
                    else None
                )
            return signal

        for candidate in ordered:
            symbol = self._norm_symbol(candidate["symbol"])
            audit_rec = structure_audits.get(symbol)
            if audit_rec is not None and "signal_fail_reason" not in audit_rec:
                audit_rec["signal_emit"] = False
                audit_rec["signal_fail_reason"] = "signal_not_selected"
        return None
