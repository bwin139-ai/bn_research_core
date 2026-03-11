# 目标文件: core/config_loader.py

import json
import os
from typing import Any, Dict, List


class StrategyConfig:
    # 仅保留真正适用于所有策略的最小身份字段
    COMMON_REQUIRED_KEYS = [
        "strategy_name",
    ]

    # Top1 暂沿用旧版必填集合，避免本轮 Snapback 清账顺手破坏 Top1 路由
    TOP1_REQUIRED_KEYS = [
        "strategy_name",
        "scan_interval_sec",
        "min_24h_pct_chg",
        "min_24h_quote_vol",
        "ndays_lowest",
        "max_surge_from_lowest",
        "entry_pullback_pct",
        "take_profit_pct",
        "min_needle_depth_pct",
        "max_needle_depth_pct",
        "order_timeout_sec",
        "cooldown_hours",
        "benchmark_index",
    ]

    # Snapback Phase 1：仅要求当前链路真实依赖的字段，不再为 Top1 字段买单
    SNAPBACK_REQUIRED_KEYS = [
        "strategy_name",
        "min_24h_quote_vol",
        "drop_window_mins",
        "min_drop_pct",
        "max_drop_pct",
        "vol_climax_window_mins",
        "vol_baseline_window_mins",
        "min_vol_climax_ratio",
        "min_rebound_ratio",
        "max_rebound_ratio",
        "entry_pullback_pct",
        "take_profit_pct",
        "order_timeout_sec",
        "cooldown_hours",
        "max_history_window_mins",
        "benchmark_index",
        "max_hold_mins",
        "time_stop_min_profit",
        "defense_trigger_pct",
        "defense_lock_pct",
    ]

    @staticmethod
    def _require_keys(raw_data: Dict[str, Any], keys: List[str]) -> None:
        for key in keys:
            if key not in raw_data:
                raise KeyError(f"【铁律违背】配置文件缺少必要参数: '{key}'")

    @staticmethod
    def load(config_path: str) -> Dict[str, Any]:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件缺失: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        StrategyConfig._require_keys(raw_data, StrategyConfig.COMMON_REQUIRED_KEYS)

        strategy_name = raw_data["strategy_name"]
        if strategy_name == "top1":
            StrategyConfig._require_keys(raw_data, StrategyConfig.TOP1_REQUIRED_KEYS)
        elif strategy_name == "snapback":
            StrategyConfig._require_keys(raw_data, StrategyConfig.SNAPBACK_REQUIRED_KEYS)
        else:
            raise KeyError(f"【铁律违背】未知 strategy_name: '{strategy_name}'")

        return raw_data
