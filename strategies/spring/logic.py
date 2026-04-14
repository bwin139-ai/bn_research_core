from typing import Any, Dict, Optional, Set


class SpringSABCStrategy:
    """ARCH_ONLY skeleton for spring-sabc.

    Patch 1 only registers the strategy into the project skeleton.
    Business logic will be implemented in later LOGIC_ONLY patches.
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.strategy_name = str(config.get("strategy_name") or "spring-sabc")

    def on_kline_close(
        self,
        current_time_ms: int,
        cross_section,
        active_symbols: Set[str],
        *,
        full_df,
    ) -> Optional[Dict[str, Any]]:
        raise NotImplementedError(
            "spring-sabc ARCH_ONLY skeleton loaded successfully; strategy logic will be added in later LOGIC_ONLY patches."
        )

    def audit_symbols_at_kline_close(
        self,
        current_time_ms: int,
        cross_section,
        active_symbols: Set[str],
        *,
        full_df,
        target_symbols,
    ) -> Dict[str, Any]:
        return {}
