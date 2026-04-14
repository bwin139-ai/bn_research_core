from typing import Any, Dict, List, Optional, Set

import pandas as pd


class SpringSABCStrategy:
    """spring-sabc strategy skeleton with universe election only.

    Patch 2 implements only the universe layer:
    - exclude_symbols
    - min_24h_chg_pct
    - min_24h_quote_volume
    - score = rank(chg_24h) + rank(vol_24h)
    - select score_top_n for later structure checks

    Structure detection / entry / exit are intentionally not implemented yet.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.strategy_name = str(config.get("strategy_name") or "spring-sabc")

        runtime = config["runtime"]
        universe = config["universe"]
        structure = config["structure"]
        exit_policy = config["exit_policy"]

        self.bar_interval = str(runtime["bar_interval"])
        self.max_history_window_mins = int(runtime["max_history_window_mins"])

        self.exclude_symbols = {
            str(x).upper().strip() for x in universe["exclude_symbols"] if str(x).strip()
        }
        self.min_24h_chg_pct = float(universe["min_24h_chg_pct"])
        self.min_24h_quote_volume = float(universe["min_24h_quote_volume"])
        self.score_top_n = int(universe["score_top_n"])

        self.pattern_window_mins = int(structure["pattern_window_mins"])
        self.stop_loss_anchor = str(exit_policy["stop_loss_anchor"])

        self._last_universe_candidates: List[Dict[str, Any]] = []
        self._last_universe_audits: Dict[str, Dict[str, Any]] = {}

    @staticmethod
    def _norm_symbol(value: Any) -> str:
        return str(value).upper().strip() if value is not None else ""

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            if pd.isna(value):
                return None
            return float(value)
        except Exception:
            return None

    def _empty_audit(self, fail_reason: str) -> Dict[str, Any]:
        return {
            "universe_pass": False,
            "fail_reason": fail_reason,
            "score_top_n": self.score_top_n,
        }

    def _build_universe_state(
        self,
        cross_section: pd.DataFrame,
        active_symbols: Set[str],
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
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

        eligible_symbols = [
            sym for sym, rec in audits.items()
            if not rec["fail_reason"]
        ]
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

    def on_kline_close(
        self,
        current_time_ms: int,
        cross_section,
        active_symbols: Set[str],
        *,
        full_df,
    ) -> Optional[Dict[str, Any]]:
        candidates, audits = self._build_universe_state(cross_section, active_symbols)
        self._last_universe_candidates = candidates
        self._last_universe_audits = audits
        return None

    def audit_symbols_at_kline_close(
        self,
        current_time_ms: int,
        cross_section,
        active_symbols: Set[str],
        *,
        full_df,
        target_symbols,
    ) -> Dict[str, Any]:
        target_set = {
            self._norm_symbol(s) for s in (target_symbols or set()) if self._norm_symbol(s)
        }
        if not target_set:
            return {}

        candidates, audits = self._build_universe_state(cross_section, active_symbols)
        selected_symbols = {c["symbol"] for c in candidates}
        result: Dict[str, Dict[str, Any]] = {}
        cross_index_obj = getattr(cross_section, "index", [])
        cross_index = {
            self._norm_symbol(sym) for sym in list(cross_index_obj)
        }
        for sym in sorted(target_set):
            if sym in audits:
                result[sym] = audits[sym]
            elif sym not in cross_index:
                result[sym] = {**self._empty_audit("symbol_not_in_cross_section"), "symbol": sym}
            else:
                result[sym] = {**self._empty_audit("universe_state_missing"), "symbol": sym}
            result[sym]["selected_for_structure"] = sym in selected_symbols
        return result
