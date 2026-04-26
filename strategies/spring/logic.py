import logging
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd


class SpringSABCStrategy:
    """spring-sabc strategy with universe + structure + signal generation.

    Patch 4 closes the sim-side decision loop:
    - universe election
    - structure detection
    - emit one BUY signal on CB for the best structure-pass candidate
    - stop_loss_price fixed at b_close
    - take_profit / time_stop fields wired from exit_policy

    Live integration is intentionally not implemented yet.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.strategy_name = str(config.get("strategy_name") or "spring-sabc")

        runtime = config["runtime"]
        universe = config["universe"]
        structure = config["structure"]
        exit_policy = config["exit_policy"]
        risk_controls = config.get("risk_controls", {})

        self.bar_interval = str(runtime["bar_interval"])
        self.max_history_window_mins = int(runtime["max_history_window_mins"])

        self.exclude_symbols = {
            str(x).upper().strip() for x in universe["exclude_symbols"] if str(x).strip()
        }
        self.min_24h_chg_pct = float(universe["min_24h_chg_pct"])
        self.min_24h_quote_volume = float(universe["min_24h_quote_volume"])
        self.score_top_n = int(universe["score_top_n"])

        self.pattern_window_mins = int(structure["pattern_window_mins"])
        self.ab_chg_pct_min = float(structure["ab"]["chg_pct_min"])
        self.ab_consecutive_down_bars_min = int(structure["ab"]["consecutive_down_bars_min"])
        self.vol_baseline_window_mins = int(structure["vol_climax"]["baseline_window_mins"])
        self.vol_ratio_min = float(structure["vol_climax"]["ratio_min"])
        self.gamma_ac_vol_ratio_min = float(structure["vol_climax"]["gamma_ac_vol_ratio_min"])
        self.rebound_ratio_min = float(structure["rebound"]["ratio_min"])
        self.bc_over_ab_bars_max = float(structure["rebound"]["bc_over_ab_bars_max"])

        self.stop_loss_anchor = str(exit_policy["stop_loss_anchor"])
        self.take_profit_pct = float(exit_policy["take_profit_pct"])
        self.max_hold_mins = int(exit_policy["max_hold_mins"])
        self.time_stop_min_profit_pct = float(exit_policy["time_stop_min_profit_pct"])
        breakeven_guard = exit_policy["breakeven_guard"]
        self.breakeven_guard_enabled = bool(breakeven_guard["enabled"])
        self.breakeven_guard_trigger_r = float(breakeven_guard["trigger_r"])
        self.breakeven_guard_floor_r = float(breakeven_guard["floor_r"])
        self.cooldown_hours = float(risk_controls["cooldown_hours"])
        self.max_risk_pct = float(risk_controls["max_risk_pct"])
        self.cooldown_ms = int(round(self.cooldown_hours * 3600_000))
        self.cooldown_until: Dict[str, int] = {}

        self._last_universe_candidates: List[Dict[str, Any]] = []
        self._last_universe_audits: Dict[str, Dict[str, Any]] = {}
        self._last_structure_candidates: List[Dict[str, Any]] = []
        self._last_structure_audits: Dict[str, Dict[str, Any]] = {}
        self._last_signal: Optional[Dict[str, Any]] = None
        self._last_signal_audits: Dict[str, Dict[str, Any]] = {}
        self._prepared_history_cache: Dict[str, Tuple[int, pd.DataFrame]] = {}

    @staticmethod
    def _norm_symbol(value: Any) -> str:
        return str(value).upper().strip() if value is not None else ""

    @staticmethod
    def _safe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
        try:
            if value is None:
                return default
            if pd.isna(value):
                return default
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _bj_from_ms(value: int) -> str:
        return (pd.to_datetime(int(value), unit="ms") + pd.Timedelta(hours=8)).strftime("%Y-%m-%d %H:%M")

    @staticmethod
    def _bj_short_from_ms(value: int) -> str:
        return (pd.to_datetime(int(value), unit="ms") + pd.Timedelta(hours=8)).strftime("%m-%d %H:%M")

    def _cooldown_until_for(self, symbol: str) -> int:
        return int(self.cooldown_until.get(self._norm_symbol(symbol), 0) or 0)

    def _is_cooldown_active(self, symbol: str, current_time_ms: int) -> bool:
        return self._cooldown_until_for(symbol) > int(current_time_ms)

    @staticmethod
    def _pct_text(value: Any) -> str:
        safe = SpringSABCStrategy._safe_float(value)
        if safe is None:
            return "NA"
        return f"{safe * 100.0:.2f}%"

    @staticmethod
    def _price_text(value: Any) -> str:
        safe = SpringSABCStrategy._safe_float(value)
        if safe is None:
            return "NA"
        return f"{safe:.6f}"

    def build_signal_lock_log(self, signal: Dict[str, Any]) -> str:
        context = dict(signal.get("context") or {})
        signal_time_bj = signal.get("signal_time_bj") or self._bj_from_ms(int(signal["signal_time"]))
        return (
            f"[{signal_time_bj} BJ] 🌱 Spring雷达锁定: {signal['symbol']} | 当前价: {self._price_text(signal.get('current_price'))}"
            f" | A: {self._bj_short_from_ms(int(context['a_time_ms']))} high={self._price_text(context.get('a_high', context.get('a_close')))}"
            f" | B: {self._bj_short_from_ms(int(context['b_time_ms']))} low={self._price_text(context.get('b_low', context.get('b_close')))}"
            f" | C: {self._bj_short_from_ms(int(context['c_time_ms']))} close={self._price_text(context.get('c_close'))}"
            f" | 24h涨幅: {self._pct_text(context.get('chg_24h'))}"
            f" | 24h成交额: {self._safe_float(context.get('vol_24h'), 0.0):.0f}"
            f" | AB跌幅: {self._pct_text(context.get('ab_chg_pct'))}"
            f" | 爆量倍数: {self._safe_float(context.get('vol_ratio'), 0.0):.2f}"
            f" | AC/γA量比: {self._safe_float(context.get('gamma_ac_vol_ratio'), 0.0):.2f}"
            f" | 反弹比例: {self._pct_text(context.get('rebound_ratio'))}"
            f" | AB/BC: {int(context.get('ab_bars', 0))}/{int(context.get('bc_bars', 0))}"
            f" | 评分: {int(context.get('score', 0))} (#{int(context.get('score_order', 0))})"
        )

    def build_entry_log(self, signal: Dict[str, Any]) -> str:
        signal_time_bj = signal.get("signal_time_bj") or self._bj_from_ms(int(signal["signal_time"]))
        return (
            f"[{signal_time_bj} BJ] 市价开仓成交: {signal['symbol']} 进场多单 @ {self._price_text(signal.get('current_price'))}"
            f" | 止盈: {self._price_text(signal.get('tp_price'))}"
            f" | 止损: {self._price_text(signal.get('sl_price'))}"
        )

    def build_exit_log(self, trade: Dict[str, Any]) -> str:
        context = dict(trade.get("context") or {})
        exit_time_bj = trade.get("exit_time_bj") or self._bj_from_ms(int(trade["exit_time"]))
        entry_time = self._safe_float(trade.get("entry_time"))
        exit_time = self._safe_float(trade.get("exit_time"))
        hold_mins = 0
        if entry_time is not None and exit_time is not None and exit_time >= entry_time:
            hold_mins = int(round((exit_time - entry_time) / 60000.0))
        parts = [
            f"[{exit_time_bj} BJ] 平仓离场: {trade['symbol']} @ {self._price_text(trade.get('exit_price'))}",
            f"原因: {trade.get('reason', 'UNKNOWN')}",
            f"盈亏: {self._pct_text(trade.get('pnl_pct'))}",
            f"持仓: {hold_mins}m",
        ]
        if str(trade.get("reason") or "") == "TIME_STOP":
            parts.append(f"保本阈值: {self._pct_text(self.time_stop_min_profit_pct)}")
        return " | ".join(parts)


    def _empty_audit(self, fail_reason: str) -> Dict[str, Any]:
        return {
            "universe_pass": False,
            "structure_pass": False,
            "signal_emit": False,
            "fail_reason": fail_reason,
            "score_top_n": self.score_top_n,
        }

    def _build_universe_state(
        self,
        cross_section: pd.DataFrame,
        active_symbols: Set[str],
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        audits: Dict[str, Dict[str, Any]] = {}
        normalized_active = {self._norm_symbol(s) for s in (active_symbols or set()) if self._norm_symbol(s)}

        if cross_section is None or cross_section.empty:
            return [], audits
        if "chg_24h" not in cross_section.columns or "vol_24h" not in cross_section.columns:
            return [], audits

        cs = cross_section.copy()
        cs.index = [self._norm_symbol(sym) for sym in cs.index]
        cs["chg_24h_num"] = pd.to_numeric(cs["chg_24h"], errors="coerce")
        cs["vol_24h_num"] = pd.to_numeric(cs["vol_24h"], errors="coerce")

        for symbol, row in cs.iterrows():
            record: Dict[str, Any] = {
                "symbol": symbol,
                "universe_pass": False,
                "structure_pass": False,
                "signal_emit": False,
                "fail_reason": "",
                "chg_24h": self._safe_float(row.get("chg_24h_num")),
                "vol_24h": self._safe_float(row.get("vol_24h_num")),
                "in_active_symbols": symbol in normalized_active,
                "is_excluded_symbol": symbol in self.exclude_symbols,
                "score_top_n": self.score_top_n,
            }
            if record["chg_24h"] is None or record["vol_24h"] is None:
                record["fail_reason"] = "cross_section_nan_metrics"
                audits[symbol] = record
                continue
            if record["is_excluded_symbol"]:
                record["fail_reason"] = "symbol_in_exclude_symbols"
                audits[symbol] = record
                continue
            if record["chg_24h"] * 100.0 < self.min_24h_chg_pct:
                record["fail_reason"] = "chg_24h_below_min"
                audits[symbol] = record
                continue
            if record["vol_24h"] < self.min_24h_quote_volume:
                record["fail_reason"] = "vol_24h_below_min"
                audits[symbol] = record
                continue
            audits[symbol] = record

        eligible_symbols = [sym for sym, rec in audits.items() if not rec["fail_reason"]]
        if not eligible_symbols:
            return [], audits

        eligible_df = cs.loc[eligible_symbols].copy()
        eligible_df["rank_chg_24h"] = eligible_df["chg_24h_num"].rank(method="min", ascending=False)
        eligible_df["rank_vol_24h"] = eligible_df["vol_24h_num"].rank(method="min", ascending=False)
        eligible_df["score"] = eligible_df["rank_chg_24h"] + eligible_df["rank_vol_24h"]
        eligible_df = eligible_df.sort_values(
            by=["score", "chg_24h_num", "vol_24h_num"],
            ascending=[True, False, False],
            kind="stable",
        )

        selected = eligible_df.head(self.score_top_n)
        selected_symbols = set(selected.index.tolist())

        candidates: List[Dict[str, Any]] = []
        for order_idx, (symbol, row) in enumerate(selected.iterrows(), start=1):
            candidate = {
                "symbol": symbol,
                "chg_24h": float(row["chg_24h_num"]),
                "vol_24h": float(row["vol_24h_num"]),
                "rank_chg_24h": int(row["rank_chg_24h"]),
                "rank_vol_24h": int(row["rank_vol_24h"]),
                "score": int(row["score"]),
                "score_order": order_idx,
                "score_top_n": self.score_top_n,
                "in_active_symbols": symbol in normalized_active,
            }
            candidates.append(candidate)

        for symbol, row in eligible_df.iterrows():
            rec = audits[symbol]
            rec["rank_chg_24h"] = int(row["rank_chg_24h"])
            rec["rank_vol_24h"] = int(row["rank_vol_24h"])
            rec["score"] = int(row["score"])
            rec["selected_for_structure"] = symbol in selected_symbols
            if symbol in selected_symbols:
                rec["universe_pass"] = True
                rec["fail_reason"] = "pending_structure_check"
                rec["score_order"] = next(c["score_order"] for c in candidates if c["symbol"] == symbol)
            else:
                rec["fail_reason"] = "score_not_in_top_n"

        return candidates, audits

    @staticmethod
    def _pick_volume_column(df: pd.DataFrame) -> Optional[str]:
        for col in ("quote_asset_volume", "volume"):
            if col in df.columns:
                return col
        return None

    def _extract_symbol_history(self, full_df: Any, symbol: str) -> Optional[pd.DataFrame]:
        if full_df is None:
            return None
        if isinstance(full_df, dict):
            df = full_df.get(symbol)
            return df if isinstance(df, pd.DataFrame) else None
        return None

    def _prepare_history(self, symbol: str, df: pd.DataFrame, current_time_ms: int) -> Optional[pd.DataFrame]:
        if df is None or df.empty:
            return None

        cache_key = self._norm_symbol(symbol)
        source_id = id(df)
        cached = self._prepared_history_cache.get(cache_key)
        if cached is not None and cached[0] == source_id:
            prepared = cached[1]
        else:
            prepared = df.copy()
            try:
                prepared = prepared.sort_index()
            except Exception:
                pass
            try:
                prepared.index = pd.Index([int(x) for x in prepared.index])
            except Exception:
                return None
            self._prepared_history_cache[cache_key] = (source_id, prepared)

        idx = prepared.index
        try:
            end_pos = idx.searchsorted(int(current_time_ms), side="left")
        except Exception:
            hist = prepared[prepared.index < int(current_time_ms)]
        else:
            start_pos = max(0, int(end_pos) - self.max_history_window_mins)
            hist = prepared.iloc[start_pos:int(end_pos)]

        if hist.empty:
            return None
        return hist

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
        rec["a_time_ms"] = None
        rec["b_time_ms"] = None
        rec["c_time_ms"] = None

        sym_df = self._extract_symbol_history(full_df, symbol)
        if sym_df is None:
            rec["fail_reason"] = "missing_symbol_history"
            return rec

        hist = self._prepare_history(symbol, sym_df, current_time_ms)
        if hist is None or hist.empty:
            rec["fail_reason"] = "empty_symbol_history"
            return rec

        if "close" not in hist.columns:
            rec["fail_reason"] = "missing_close_column"
            return rec

        vol_col = self._pick_volume_column(hist)
        if vol_col is None:
            rec["fail_reason"] = "missing_volume_column"
            return rec

        pattern_df = hist.tail(self.pattern_window_mins).copy()
        baseline_df = hist.tail(self.vol_baseline_window_mins).copy()

        if len(pattern_df) < self.ab_consecutive_down_bars_min + 2:
            rec["fail_reason"] = "pattern_window_insufficient_bars"
            rec["pattern_window_bars"] = int(len(pattern_df))
            return rec
        if len(baseline_df) < self.vol_baseline_window_mins:
            rec["fail_reason"] = "baseline_window_insufficient_bars"
            rec["baseline_window_bars"] = int(len(baseline_df))
            return rec

        closes = pd.to_numeric(pattern_df["close"], errors="coerce")
        vols = pd.to_numeric(pattern_df[vol_col], errors="coerce")
        baseline_vols = pd.to_numeric(baseline_df[vol_col], errors="coerce")

        if closes.isna().any():
            rec["fail_reason"] = "pattern_close_nan"
            return rec
        if vols.isna().any() or baseline_vols.isna().any():
            rec["fail_reason"] = "pattern_volume_nan"
            return rec

        baseline_avg_vol = float(baseline_vols.mean())
        if baseline_avg_vol <= 0:
            rec["fail_reason"] = "baseline_avg_volume_nonpositive"
            return rec

        c_idx = len(pattern_df) - 1
        close_values = [float(x) for x in closes.tolist()]
        high_values = (
            [float(x) for x in pd.to_numeric(pattern_df["high"], errors="coerce").tolist()]
            if "high" in pattern_df.columns
            else list(close_values)
        )
        open_values = (
            [float(x) for x in pd.to_numeric(pattern_df["open"], errors="coerce").tolist()]
            if "open" in pattern_df.columns
            else list(close_values)
        )
        low_values = (
            [float(x) for x in pd.to_numeric(pattern_df["low"], errors="coerce").tolist()]
            if "low" in pattern_df.columns
            else list(close_values)
        )
        vol_values = [float(x) for x in vols.tolist()]
        quote_vol_values = (
            [float(x) for x in pd.to_numeric(pattern_df["quote_asset_volume"], errors="coerce").fillna(0.0).tolist()]
            if "quote_asset_volume" in pattern_df.columns
            else []
        )
        base_vol_values = (
            [float(x) for x in pd.to_numeric(pattern_df["volume"], errors="coerce").fillna(0.0).tolist()]
            if "volume" in pattern_df.columns
            else []
        )
        time_values = [int(x) for x in pattern_df.index.tolist()]
        c_close = close_values[c_idx]
        c_time_ms = time_values[c_idx]

        # Spring-SABC constitution:
        # - C is fixed as HBs[0].
        # - Scan B from nearest to farthest.
        # - B initial filter only requires C_close > B_close.
        # - A is the earliest start of B's strict consecutive close-down run.
        # - B_low must be the lowest low in the whole A-B interval.
        down_run = [0] * len(close_values)
        for idx_pos in range(1, len(close_values)):
            if close_values[idx_pos] < close_values[idx_pos - 1]:
                down_run[idx_pos] = down_run[idx_pos - 1] + 1

        vol_prefix = [0.0]
        for value in vol_values:
            vol_prefix.append(vol_prefix[-1] + value)

        best: Optional[Dict[str, Any]] = None
        for b_search_rank_from_c, b_idx in enumerate(range(c_idx - 1, 0, -1), start=1):
            bc_bars = c_idx - b_idx
            if bc_bars <= 0:
                continue
            b_close = close_values[b_idx]
            if b_close <= 0:
                continue
            if c_close <= b_close:
                continue

            ab_down_run_bars = int(down_run[b_idx])
            if ab_down_run_bars <= 0:
                continue
            a_idx = b_idx - ab_down_run_bars
            ab_bars = b_idx - a_idx
            if ab_bars <= 0:
                continue

            ab_required_bars_min = max(
                int(self.ab_consecutive_down_bars_min),
                int((float(bc_bars) / float(self.bc_over_ab_bars_max)) + 0.999999999),
            )
            if ab_bars < ab_required_bars_min:
                continue

            a_close = close_values[a_idx]
            if a_close <= 0 or a_close <= b_close:
                continue

            b_low = low_values[b_idx]
            ab_low_min = min(low_values[a_idx : b_idx + 1])
            b_low_is_ab_low = abs(float(b_low) - float(ab_low_min)) <= 1e-12
            if not b_low_is_ab_low:
                continue

            ab_chg_pct = (a_close - b_close) / a_close
            if ab_chg_pct < self.ab_chg_pct_min:
                continue

            ab_drop_abs = a_close - b_close
            if ab_drop_abs <= 0:
                continue

            rebound_ratio = (c_close - b_close) / ab_drop_abs
            if rebound_ratio < self.rebound_ratio_min:
                continue

            bc_over_ab = float(bc_bars) / float(ab_bars)
            if bc_over_ab > self.bc_over_ab_bars_max:
                continue

            ab_vol_sum = vol_prefix[b_idx + 1] - vol_prefix[a_idx + 1]
            ab_avg_vol = float(ab_vol_sum / float(ab_bars))
            if ab_avg_vol <= 0:
                continue
            vol_ratio = ab_avg_vol / baseline_avg_vol
            if vol_ratio < self.vol_ratio_min:
                continue

            hist_len = len(hist)
            pattern_start_pos = hist_len - len(pattern_df)
            hist_a_pos = pattern_start_pos + a_idx
            hist_c_pos = pattern_start_pos + c_idx
            bars_ac = hist_c_pos - hist_a_pos
            gamma_pos = hist_a_pos - bars_ac
            if bars_ac <= 0 or gamma_pos < 0:
                continue
            hist_vol_values = [float(x) for x in pd.to_numeric(hist[vol_col], errors="coerce").tolist()]
            if any(pd.isna(x) for x in hist_vol_values):
                continue
            hist_time_values = [int(x) for x in hist.index.tolist()]
            vol_gamma_a = float(sum(hist_vol_values[gamma_pos + 1 : hist_a_pos + 1]))
            vol_ac = float(sum(hist_vol_values[hist_a_pos + 1 : hist_c_pos + 1]))
            if vol_gamma_a <= 0:
                continue
            gamma_ac_vol_ratio = vol_ac / vol_gamma_a
            if gamma_ac_vol_ratio < self.gamma_ac_vol_ratio_min:
                continue

            pre_slice_end = a_idx + 1
            pre_start_close = close_values[0] if pre_slice_end > 0 else None
            pre_end_close = close_values[a_idx] if pre_slice_end > 0 else None
            pre_high = max(high_values[:pre_slice_end]) if pre_slice_end > 0 else None
            pre_low = min(low_values[:pre_slice_end]) if pre_slice_end > 0 else None
            pre_chg_pct = (
                (float(pre_end_close) / float(pre_start_close)) - 1.0
                if pre_start_close is not None and pre_end_close is not None and pre_start_close > 0
                else None
            )
            pre_range_pct = (
                (float(pre_high) / float(pre_low)) - 1.0
                if pre_high is not None and pre_low is not None and pre_low > 0
                else None
            )
            pre_high_to_a_close_pct = (
                (float(pre_high) / float(a_close)) - 1.0
                if pre_high is not None and a_close > 0
                else None
            )
            pre_a_close_pos_in_range = (
                (float(a_close) - float(pre_low)) / (float(pre_high) - float(pre_low))
                if pre_high is not None and pre_low is not None and pre_high > pre_low
                else None
            )
            pre_a_high_pos_in_range = (
                (float(high_values[a_idx]) - float(pre_low)) / (float(pre_high) - float(pre_low))
                if pre_high is not None and pre_low is not None and pre_high > pre_low
                else None
            )
            pre_up_bars = sum(
                1 for open_value, close_value in zip(open_values[:pre_slice_end], close_values[:pre_slice_end]) if close_value > open_value
            )
            pre_down_bars = sum(
                1 for open_value, close_value in zip(open_values[:pre_slice_end], close_values[:pre_slice_end]) if close_value < open_value
            )
            pre_flat_bars = max(0, int(pre_slice_end) - int(pre_up_bars) - int(pre_down_bars))
            pre_quote_vol = float(sum(quote_vol_values[:pre_slice_end])) if quote_vol_values else None
            pre_base_vol = float(sum(base_vol_values[:pre_slice_end])) if base_vol_values else None

            best = {
                "a_idx": a_idx,
                "b_idx": b_idx,
                "c_idx": c_idx,
                "s_time_ms": time_values[0],
                "gamma_time_ms": hist_time_values[gamma_pos],
                "a_time_ms": time_values[a_idx],
                "b_time_ms": time_values[b_idx],
                "c_time_ms": c_time_ms,
                "a_close": a_close,
                "a_high": high_values[a_idx],
                "b_close": b_close,
                "b_low": b_low,
                "c_close": c_close,
                "ab_bars": int(ab_bars),
                "bc_bars": int(bc_bars),
                "ab_chg_pct": float(ab_chg_pct),
                "rebound_ratio": float(rebound_ratio),
                "bars_ac": int(bars_ac),
                "bc_over_ab_bars": float(bc_over_ab),
                "ab_avg_vol": float(ab_avg_vol),
                "baseline_avg_vol": float(baseline_avg_vol),
                "vol_ratio": float(vol_ratio),
                "vol_gamma_a": float(vol_gamma_a),
                "vol_ac": float(vol_ac),
                "gamma_ac_vol_ratio": float(gamma_ac_vol_ratio),
                "gamma_ac_vol_ratio_min": float(self.gamma_ac_vol_ratio_min),
                "pre_a_bars": int(pre_slice_end),
                "pre_a_start_close": float(pre_start_close) if pre_start_close is not None else None,
                "pre_a_end_close": float(pre_end_close) if pre_end_close is not None else None,
                "pre_a_high": float(pre_high) if pre_high is not None else None,
                "pre_a_low": float(pre_low) if pre_low is not None else None,
                "pre_a_chg_pct": float(pre_chg_pct) if pre_chg_pct is not None else None,
                "pre_a_range_pct": float(pre_range_pct) if pre_range_pct is not None else None,
                "pre_a_high_to_a_close_pct": float(pre_high_to_a_close_pct) if pre_high_to_a_close_pct is not None else None,
                "pre_a_close_pos_in_range": float(pre_a_close_pos_in_range) if pre_a_close_pos_in_range is not None else None,
                "pre_a_high_pos_in_range": float(pre_a_high_pos_in_range) if pre_a_high_pos_in_range is not None else None,
                "pre_a_quote_vol": float(pre_quote_vol) if pre_quote_vol is not None else None,
                "pre_a_base_vol": float(pre_base_vol) if pre_base_vol is not None else None,
                "pre_a_avg_quote_vol_per_bar": float(pre_quote_vol / float(pre_slice_end)) if pre_quote_vol is not None and pre_slice_end > 0 else None,
                "pre_a_up_bars": int(pre_up_bars),
                "pre_a_down_bars": int(pre_down_bars),
                "pre_a_flat_bars": int(pre_flat_bars),
                "pre_a_up_ratio": float(pre_up_bars / float(pre_slice_end)) if pre_slice_end > 0 else None,
                "pre_a_down_ratio": float(pre_down_bars / float(pre_slice_end)) if pre_slice_end > 0 else None,
                "abc_selection_mode": "nearest_valid_b",
                "b_scan_direction": "from_c_leftward",
                "b_initial_filter": "c_close_gt_b_close",
                "c_close_gt_b_close": True,
                "ab_down_rule": "strict_close_down",
                "ab_down_run_bars": int(ab_down_run_bars),
                "ab_required_bars_min": int(ab_required_bars_min),
                "ab_low_min": float(ab_low_min),
                "b_low_is_ab_low": bool(b_low_is_ab_low),
                "b_search_rank_from_c": int(b_search_rank_from_c),
            }
            break

        if best is None:
            rec["fail_reason"] = "spring_structure_not_found"
            rec["pattern_window_bars"] = int(len(pattern_df))
            rec["baseline_window_bars"] = int(len(baseline_df))
            rec["c_time_ms"] = c_time_ms
            rec["c_close"] = c_close
            rec["abc_selection_mode"] = "nearest_valid_b"
            rec["b_scan_direction"] = "from_c_leftward"
            rec["b_initial_filter"] = "c_close_gt_b_close"
            rec["ab_down_rule"] = "strict_close_down"
            rec["volume_column"] = vol_col
            return rec

        rec.update(best)
        rec["structure_pass"] = True
        rec["fail_reason"] = "structure_pass"
        rec["selected_for_structure"] = True
        rec["pattern_window_bars"] = int(len(pattern_df))
        rec["baseline_window_bars"] = int(len(baseline_df))
        rec["volume_column"] = vol_col
        rec["stop_loss_price"] = float(best["b_close"])
        return rec

    def _build_structure_state(
        self,
        current_time_ms: int,
        universe_candidates: List[Dict[str, Any]],
        universe_audits: Dict[str, Dict[str, Any]],
        *,
        full_df: Any,
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]]]:
        structure_candidates: List[Dict[str, Any]] = []
        structure_audits: Dict[str, Dict[str, Any]] = {sym: dict(rec) for sym, rec in universe_audits.items()}

        for candidate in universe_candidates:
            symbol = candidate["symbol"]
            universe_rec = structure_audits.get(symbol, dict(candidate))
            structure_rec = self._evaluate_structure_for_symbol(
                symbol,
                current_time_ms,
                universe_rec,
                full_df=full_df,
            )
            structure_audits[symbol] = structure_rec
            if structure_rec.get("structure_pass"):
                structure_candidates.append(structure_rec)

        return structure_candidates, structure_audits

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
        normalized_active = {self._norm_symbol(s) for s in (active_symbols or set()) if self._norm_symbol(s)}
        cs = cross_section.copy() if isinstance(cross_section, pd.DataFrame) else pd.DataFrame()
        if not cs.empty:
            cs.index = [self._norm_symbol(sym) for sym in cs.index]

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
            if audit_rec is not None:
                audit_rec["cooldown_active"] = False
                audit_rec["cooldown_until"] = cooldown_until if cooldown_until > 0 else None
                audit_rec["cooldown_until_bj"] = self._bj_from_ms(cooldown_until) if cooldown_until > 0 else None
            if symbol in normalized_active:
                if audit_rec is not None:
                    audit_rec["signal_emit"] = False
                    audit_rec["signal_fail_reason"] = "symbol_in_active_symbols"
                continue
            if symbol not in cs.index:
                if audit_rec is not None:
                    audit_rec["signal_emit"] = False
                    audit_rec["signal_fail_reason"] = "signal_symbol_not_in_cross_section"
                continue
            row = cs.loc[symbol]
            current_price = self._safe_float(row.get("open"), self._safe_float(row.get("close")))
            if current_price is None or current_price <= 0:
                if audit_rec is not None:
                    audit_rec["signal_emit"] = False
                    audit_rec["signal_fail_reason"] = "signal_current_price_invalid"
                continue

            sl_price = float(candidate["stop_loss_price"])
            risk_pct = (float(current_price) - float(sl_price)) / float(current_price)
            if risk_pct <= 0:
                if audit_rec is not None:
                    audit_rec["signal_emit"] = False
                    audit_rec["risk_pct"] = float(risk_pct)
                    audit_rec["max_risk_pct"] = self.max_risk_pct
                    audit_rec["signal_fail_reason"] = "signal_risk_distance_nonpositive"
                continue
            if risk_pct > self.max_risk_pct:
                if audit_rec is not None:
                    audit_rec["signal_emit"] = False
                    audit_rec["risk_pct"] = float(risk_pct)
                    audit_rec["max_risk_pct"] = self.max_risk_pct
                    audit_rec["signal_fail_reason"] = "signal_risk_pct_above_max"
                continue
            if self.take_profit_pct == -1.0:
                risk_distance = float(current_price) - float(sl_price)
                if risk_distance <= 0:
                    if audit_rec is not None:
                        audit_rec["signal_emit"] = False
                        audit_rec["signal_fail_reason"] = "signal_risk_distance_nonpositive"
                    continue
                tp_price = float(current_price) + risk_distance
                take_profit_mode = "risk_reward_1r"
            else:
                tp_price = float(current_price) * (1.0 + self.take_profit_pct)
                take_profit_mode = "fixed_pct"
            signal_time_ms = int(current_time_ms)
            signal = {
                "signal_time": signal_time_ms,
                "signal_time_bj": self._bj_from_ms(signal_time_ms),
                "symbol": symbol,
                "action": "BUY",
                "current_price": float(current_price),
                "tp_price": float(tp_price),
                "sl_price": float(sl_price),
                "params": {
                    "take_profit_pct": self.take_profit_pct,
                    "take_profit_mode": take_profit_mode,
                    "max_hold_mins": self.max_hold_mins,
                    "time_stop_min_profit_pct": self.time_stop_min_profit_pct,
                    "stop_loss_anchor": self.stop_loss_anchor,
                    "breakeven_guard_enabled": self.breakeven_guard_enabled,
                    "breakeven_guard_trigger_r": self.breakeven_guard_trigger_r,
                    "breakeven_guard_floor_r": self.breakeven_guard_floor_r,
                    "cooldown_hours": self.cooldown_hours,
                    "max_risk_pct": self.max_risk_pct,
                    "gamma_ac_vol_ratio_min": self.gamma_ac_vol_ratio_min,
                },
                "context": {
                    "strategy_name": self.strategy_name,
                    "score_order": int(candidate.get("score_order", 0)),
                    "score": int(candidate.get("score", 0)),
                    "chg_24h": float(candidate.get("chg_24h", 0.0)),
                    "vol_24h": float(candidate.get("vol_24h", 0.0)),
                    "s_time_ms": int(candidate.get("s_time_ms", candidate["a_time_ms"])),
                    "a_time_ms": int(candidate["a_time_ms"]),
                    "b_time_ms": int(candidate["b_time_ms"]),
                    "gamma_time_ms": int(candidate["gamma_time_ms"]),
                    "c_time_ms": int(candidate["c_time_ms"]),
                    "a_close": float(candidate["a_close"]),
                    "a_high": float(candidate.get("a_high", candidate["a_close"])),
                    "b_close": float(candidate["b_close"]),
                    "b_low": float(candidate.get("b_low", candidate["b_close"])),
                    "c_close": float(candidate["c_close"]),
                    "ab_bars": int(candidate["ab_bars"]),
                    "bc_bars": int(candidate["bc_bars"]),
                    "ab_chg_pct": float(candidate["ab_chg_pct"]),
                    "rebound_ratio": float(candidate["rebound_ratio"]),
                    "bars_ac": int(candidate["bars_ac"]),
                    "bc_over_ab_bars": float(candidate["bc_over_ab_bars"]),
                    "vol_ratio": float(candidate["vol_ratio"]),
                    "vol_gamma_a": float(candidate["vol_gamma_a"]),
                    "vol_ac": float(candidate["vol_ac"]),
                    "gamma_ac_vol_ratio": float(candidate["gamma_ac_vol_ratio"]),
                    "gamma_ac_vol_ratio_min": float(candidate["gamma_ac_vol_ratio_min"]),
                    "pre_a_bars": int(candidate.get("pre_a_bars", 0)),
                    "pre_a_start_close": candidate.get("pre_a_start_close"),
                    "pre_a_end_close": candidate.get("pre_a_end_close"),
                    "pre_a_high": candidate.get("pre_a_high"),
                    "pre_a_low": candidate.get("pre_a_low"),
                    "pre_a_chg_pct": candidate.get("pre_a_chg_pct"),
                    "pre_a_range_pct": candidate.get("pre_a_range_pct"),
                    "pre_a_high_to_a_close_pct": candidate.get("pre_a_high_to_a_close_pct"),
                    "pre_a_close_pos_in_range": candidate.get("pre_a_close_pos_in_range"),
                    "pre_a_high_pos_in_range": candidate.get("pre_a_high_pos_in_range"),
                    "pre_a_quote_vol": candidate.get("pre_a_quote_vol"),
                    "pre_a_base_vol": candidate.get("pre_a_base_vol"),
                    "pre_a_avg_quote_vol_per_bar": candidate.get("pre_a_avg_quote_vol_per_bar"),
                    "pre_a_up_bars": candidate.get("pre_a_up_bars"),
                    "pre_a_down_bars": candidate.get("pre_a_down_bars"),
                    "pre_a_flat_bars": candidate.get("pre_a_flat_bars"),
                    "pre_a_up_ratio": candidate.get("pre_a_up_ratio"),
                    "pre_a_down_ratio": candidate.get("pre_a_down_ratio"),
                    "pattern_window_bars": int(candidate.get("pattern_window_bars", 0)),
                    "baseline_window_bars": int(candidate.get("baseline_window_bars", 0)),
                    "stop_loss_price": float(sl_price),
                    "risk_pct": float(risk_pct),
                    "max_risk_pct": self.max_risk_pct,
                    "take_profit_mode": take_profit_mode,
                    "breakeven_guard_enabled": self.breakeven_guard_enabled,
                    "breakeven_guard_trigger_r": self.breakeven_guard_trigger_r,
                    "breakeven_guard_floor_r": self.breakeven_guard_floor_r,
                    "abc_selection_mode": str(candidate.get("abc_selection_mode", "nearest_valid_b")),
                    "b_scan_direction": str(candidate.get("b_scan_direction", "from_c_leftward")),
                    "b_initial_filter": str(candidate.get("b_initial_filter", "c_close_gt_b_close")),
                    "c_close_gt_b_close": bool(candidate.get("c_close_gt_b_close", False)),
                    "ab_down_rule": str(candidate.get("ab_down_rule", "strict_close_down")),
                    "ab_down_run_bars": int(candidate.get("ab_down_run_bars", candidate.get("ab_bars", 0))),
                    "ab_required_bars_min": int(candidate.get("ab_required_bars_min", 0)),
                    "ab_low_min": float(candidate.get("ab_low_min", candidate.get("b_low", candidate["b_close"]))),
                    "b_low_is_ab_low": bool(candidate.get("b_low_is_ab_low", False)),
                    "b_search_rank_from_c": int(candidate.get("b_search_rank_from_c", 0)),
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
                audit_rec["current_price"] = float(current_price)
                audit_rec["tp_price"] = float(tp_price)
                audit_rec["sl_price"] = float(sl_price)
                audit_rec["risk_pct"] = float(risk_pct)
                audit_rec["max_risk_pct"] = self.max_risk_pct
                audit_rec["cooldown_active"] = False
                audit_rec["cooldown_until_after_signal"] = cooldown_until_after_signal if cooldown_until_after_signal > 0 else None
                audit_rec["cooldown_until_after_signal_bj"] = self._bj_from_ms(cooldown_until_after_signal) if cooldown_until_after_signal > 0 else None
            return signal

        for candidate in ordered:
            symbol = self._norm_symbol(candidate["symbol"])
            audit_rec = structure_audits.get(symbol)
            if audit_rec is not None and "signal_fail_reason" not in audit_rec:
                audit_rec["signal_emit"] = False
                audit_rec["signal_fail_reason"] = "signal_not_selected"
        return None

    def on_kline_close(
        self,
        current_time_ms: int,
        cross_section,
        active_symbols: Set[str],
        *,
        full_df,
    ) -> Optional[Dict[str, Any]]:
        universe_candidates, universe_audits = self._build_universe_state(cross_section, active_symbols)
        self._last_universe_candidates = universe_candidates
        self._last_universe_audits = universe_audits

        structure_candidates, structure_audits = self._build_structure_state(
            current_time_ms,
            universe_candidates,
            universe_audits,
            full_df=full_df,
        )
        signal = self._build_signal_from_candidates(
            current_time_ms,
            cross_section,
            active_symbols,
            structure_candidates,
            structure_audits,
        )

        self._last_structure_candidates = structure_candidates
        self._last_structure_audits = structure_audits
        self._last_signal = signal
        self._last_signal_audits = structure_audits
        if signal:
            logging.info(self.build_signal_lock_log(signal))
        return signal

    def audit_symbols_at_kline_close(
        self,
        current_time_ms: int,
        cross_section,
        active_symbols: Set[str],
        *,
        full_df,
        target_symbols,
    ) -> Dict[str, Any]:
        target_set = {self._norm_symbol(s) for s in (target_symbols or set()) if self._norm_symbol(s)}
        if not target_set:
            return {}

        universe_candidates, universe_audits = self._build_universe_state(cross_section, active_symbols)
        structure_candidates, structure_audits = self._build_structure_state(
            current_time_ms,
            universe_candidates,
            universe_audits,
            full_df=full_df,
        )
        signal = self._build_signal_from_candidates(
            current_time_ms,
            cross_section,
            active_symbols,
            structure_candidates,
            structure_audits,
            commit_cooldown=False,
        )

        selected_symbols = {c["symbol"] for c in universe_candidates}
        structure_pass_symbols = {c["symbol"] for c in structure_candidates}
        emitted_symbol = self._norm_symbol((signal or {}).get("symbol")) if signal else ""
        result: Dict[str, Dict[str, Any]] = {}
        cross_index_obj = getattr(cross_section, "index", [])
        cross_index = {self._norm_symbol(sym) for sym in list(cross_index_obj)}
        for sym in sorted(target_set):
            if sym in structure_audits:
                result[sym] = structure_audits[sym]
            elif sym not in cross_index:
                result[sym] = {**self._empty_audit("symbol_not_in_cross_section"), "symbol": sym}
            else:
                result[sym] = {**self._empty_audit("universe_state_missing"), "symbol": sym}
            result[sym]["selected_for_structure"] = sym in selected_symbols
            result[sym]["structure_pass"] = sym in structure_pass_symbols
            result[sym]["signal_emit"] = sym == emitted_symbol
        return result
