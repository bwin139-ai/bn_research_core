# 目标文件: core/config_loader.py

import json
import os
from typing import Any, Dict


class StrategyConfig:
    # 严格定义必须存在的字段，没有任何默认值（捍卫铁律2）
    REQUIRED_KEYS = [
        "strategy_name",
        "scan_interval_sec",
        "min_24h_pct_chg",
        "min_24h_quote_vol",
        "ndays_lowest",
        "max_surge_from_lowest",
        "entry_pullback_pct",
        "take_profit_pct",
        "stop_loss_pct",
        "order_timeout_sec",
        "cooldown_hours",
        "benchmark_index",
    ]

    @staticmethod
    def load(config_path: str) -> Dict[str, Any]:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件缺失: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        for key in StrategyConfig.REQUIRED_KEYS:
            if key not in raw_data:
                raise KeyError(f"【铁律违背】配置文件缺少必要参数: '{key}'")

        return raw_data
