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
        "min_24h_chg",
        "max_24h_chg",
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

    # Snapback ARCH_ONLY：配置已改为分层 schema，必填校验改为嵌套路径校验
    SNAPBACK_REQUIRED_PATHS = [
        ("strategy_name",),
        ("runtime", "max_history_window_mins"),
        ("universe", "24h_quote_volume_min"),
        ("universe", "24h_chg_pct", "min"),
        ("universe", "24h_chg_pct", "max"),
        ("structure", "s_to_c_window", "mins"),
        ("structure", "s_to_c_window", "chg_pct", "min"),
        ("structure", "s_to_c_window", "chg_pct", "max"),
        ("structure", "selloff", "ab_bars", "min"),
        ("structure", "selloff", "ab_bars", "max"),
        ("structure", "selloff", "a_to_c_drop_pct", "min"),
        ("structure", "selloff", "a_to_c_drop_pct", "max"),
        ("structure", "selloff", "vol_climax", "recent_window_mins"),
        ("structure", "selloff", "vol_climax", "baseline_window_mins"),
        ("structure", "selloff", "vol_climax", "ratio_min"),
        ("structure", "rebound", "ratio", "min"),
        ("structure", "rebound", "ratio", "max"),
        ("structure", "rebound", "bc_bars_min"),
        ("structure", "basis", "b_pct", "max"),
        ("exit_policy", "take_profit", "base_pct"),
        ("exit_policy", "take_profit", "strong_pct"),
        ("exit_policy", "take_profit", "strong_mode", "a_to_c_drop_pct_min"),
        ("exit_policy", "take_profit", "strong_mode", "rebound_ratio_min"),
        ("exit_policy", "time_stop", "max_hold_mins"),
        ("exit_policy", "time_stop", "min_profit_pct"),
        ("risk_controls", "cooldown_hours"),
        ("benchmark", "index_weights"),
    ]

    @staticmethod
    def _require_keys(raw_data: Dict[str, Any], keys: List[str]) -> None:
        for key in keys:
            if key not in raw_data:
                raise KeyError(f"【铁律违背】配置文件缺少必要参数: '{key}'")

    @staticmethod
    def _require_paths(raw_data: Dict[str, Any], paths: List[tuple]) -> None:
        for path in paths:
            cur = raw_data
            walked = []
            for part in path:
                walked.append(part)
                if not isinstance(cur, dict) or part not in cur:
                    raise KeyError(
                        f"【铁律违背】配置文件缺少必要参数路径: '{'.'.join(walked)}'"
                    )
                cur = cur[part]

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
            StrategyConfig._require_paths(raw_data, StrategyConfig.SNAPBACK_REQUIRED_PATHS)
        else:
            raise KeyError(f"【铁律违背】未知 strategy_name: '{strategy_name}'")

        return raw_data
