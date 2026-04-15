# 目标文件: core/config_loader.py

import json
import os
from typing import Any, Dict, List


class StrategyConfig:
    # 仅保留真正适用于所有策略的最小身份字段
    COMMON_REQUIRED_KEYS = [
        "strategy_name",
    ]

    # Snapback：配置已改为分层 schema，必填校验改为嵌套路径校验
    SNAPBACK_REQUIRED_PATHS = [
        ("strategy_name",),
        ("runtime", "max_history_window_mins"),
        ("universe", "24h_quote_volume_min"),
        ("universe", "24h_chg_pct", "min"),
        ("universe", "24h_chg_pct", "max"),
        ("universe", "market_total_24h_vol_min"),
        ("structure", "a_high_source"),
        ("structure", "s_to_c_window", "mins"),
        ("structure", "s_to_c_window", "chg_pct", "min"),
        ("structure", "s_to_c_window", "chg_pct", "max"),
        ("structure", "s_to_c_window", "skip_hot_market_quadrant"),
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
        ("structure", "basis", "b_pct", "min"),
        ("structure", "basis", "b_pct", "max"),
        ("structure", "basis", "c_pct", "min"),
        ("structure", "basis", "c_pct", "max"),
        ("structure", "election_rule"),
        ("structure", "joint_filters", "min_bc_rebound_speed"),
        ("structure", "joint_filters", "min_speed_ratio_bc_over_ab"),
        ("structure", "joint_filters", "min_a_to_b_drop_speed"),
        ("structure", "joint_filters", "enable_messy_one_leg_filter"),
        ("structure", "joint_filters", "messy_one_leg_block_depth_bands"),
        ("structure", "joint_filters", "enable_clean_one_leg_sharp_top_filter"),
        ("exit_policy", "take_profit", "base_pct"),
        ("exit_policy", "take_profit", "strong_pct"),
        ("exit_policy", "take_profit", "strong_mode", "a_to_c_drop_pct_min"),
        ("exit_policy", "take_profit", "strong_mode", "rebound_ratio_min"),
        ("exit_policy", "time_stop", "max_hold_mins"),
        ("exit_policy", "time_stop", "min_profit_pct"),
        ("risk_controls", "cooldown_hours"),
        ("benchmark", "index_weights"),
    ]

    SPRING_REQUIRED_PATHS = [
        ("strategy_name",),
        ("runtime", "bar_interval"),
        ("runtime", "max_history_window_mins"),
        ("universe", "exclude_symbols"),
        ("universe", "min_24h_chg_pct"),
        ("universe", "min_24h_quote_volume"),
        ("universe", "score_top_n"),
        ("structure", "pattern_window_mins"),
        ("structure", "ab", "chg_pct_min"),
        ("structure", "ab", "consecutive_down_bars_min"),
        ("structure", "vol_climax", "baseline_window_mins"),
        ("structure", "vol_climax", "ratio_min"),
        ("structure", "rebound", "ratio_min"),
        ("structure", "rebound", "bc_over_ab_bars_max"),
        ("exit_policy", "stop_loss_anchor"),
        ("exit_policy", "take_profit_pct"),
        ("exit_policy", "max_hold_mins"),
        ("exit_policy", "time_stop_min_profit_pct"),
        ("risk_controls", "cooldown_hours"),
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
    def _validate_snapback(raw_data: Dict[str, Any]) -> None:
        StrategyConfig._require_paths(raw_data, StrategyConfig.SNAPBACK_REQUIRED_PATHS)
        a_high_source = raw_data["structure"]["a_high_source"]
        if a_high_source not in ("contract", "idx"):
            raise ValueError('【铁律违背】structure.a_high_source 只允许 "contract" 或 "idx"')
        market_total_24h_vol_min = raw_data["universe"].get("market_total_24h_vol_min")
        if not isinstance(market_total_24h_vol_min, (int, float)):
            raise ValueError('【铁律违背】universe.market_total_24h_vol_min 必须是 number')
        if float(market_total_24h_vol_min) < 0:
            raise ValueError('【铁律违背】universe.market_total_24h_vol_min 必须 >= 0')
        election_rule = str(raw_data["structure"].get("election_rule") or "").strip()
        allowed_election_rules = {
            "drop_pct_top1",
            "vol_ratio_top1",
            "drop_pct_plus_vol_ratio_top1",
            "drop_pct_plus_vol_ratio_plus_24h_vol_top1",
        }
        if election_rule not in allowed_election_rules:
            raise ValueError(
                "【铁律违背】structure.election_rule 只允许 " + str(sorted(allowed_election_rules))
            )
        joint_filters = raw_data["structure"]["joint_filters"]
        if not isinstance(joint_filters.get("enable_messy_one_leg_filter"), bool):
            raise ValueError('【铁律违背】structure.joint_filters.enable_messy_one_leg_filter 必须是 bool')
        if not isinstance(joint_filters.get("enable_clean_one_leg_sharp_top_filter"), bool):
            raise ValueError('【铁律违背】structure.joint_filters.enable_clean_one_leg_sharp_top_filter 必须是 bool')
        depth_bands = joint_filters.get("messy_one_leg_block_depth_bands")
        if not isinstance(depth_bands, list) or not depth_bands:
            raise ValueError('【铁律违背】structure.joint_filters.messy_one_leg_block_depth_bands 必须是非空 list')
        allowed_depth_bands = {"shallow", "mid", "deep", "extreme"}
        invalid_depth_bands = [str(x) for x in depth_bands if str(x) not in allowed_depth_bands]
        if invalid_depth_bands:
            raise ValueError(
                "【铁律违背】structure.joint_filters.messy_one_leg_block_depth_bands 只允许 "
                + str(sorted(allowed_depth_bands))
            )

    @staticmethod
    def _validate_spring(raw_data: Dict[str, Any]) -> None:
        StrategyConfig._require_paths(raw_data, StrategyConfig.SPRING_REQUIRED_PATHS)
        if raw_data["runtime"]["bar_interval"] != "1m":
            raise ValueError('【铁律违背】runtime.bar_interval 目前只允许 "1m"')
        exclude_symbols = raw_data["universe"]["exclude_symbols"]
        if not isinstance(exclude_symbols, list):
            raise ValueError('【铁律违背】universe.exclude_symbols 必须是 list')
        if any(not isinstance(x, str) or not x.strip() for x in exclude_symbols):
            raise ValueError('【铁律违背】universe.exclude_symbols 只允许非空字符串 symbol')
        score_top_n = raw_data["universe"]["score_top_n"]
        if not isinstance(score_top_n, int) or score_top_n <= 0:
            raise ValueError('【铁律违背】universe.score_top_n 必须是正整数')
        stop_loss_anchor = str(raw_data["exit_policy"]["stop_loss_anchor"])
        if stop_loss_anchor != "b_close":
            raise ValueError('【铁律违背】exit_policy.stop_loss_anchor 目前只允许 "b_close"')
        pattern_window_mins = raw_data["structure"]["pattern_window_mins"]
        baseline_window_mins = raw_data["structure"]["vol_climax"]["baseline_window_mins"]
        max_history_window_mins = raw_data["runtime"]["max_history_window_mins"]
        if not isinstance(pattern_window_mins, int) or pattern_window_mins <= 0:
            raise ValueError('【铁律违背】structure.pattern_window_mins 必须是正整数')
        if not isinstance(baseline_window_mins, int) or baseline_window_mins <= 0:
            raise ValueError('【铁律违背】structure.vol_climax.baseline_window_mins 必须是正整数')
        if not isinstance(max_history_window_mins, int) or max_history_window_mins <= 0:
            raise ValueError('【铁律违背】runtime.max_history_window_mins 必须是正整数')
        if max_history_window_mins < max(pattern_window_mins, baseline_window_mins):
            raise ValueError(
                '【铁律违背】runtime.max_history_window_mins 必须 >= max(structure.pattern_window_mins, structure.vol_climax.baseline_window_mins)'
            )
        take_profit_pct = raw_data["exit_policy"]["take_profit_pct"]
        max_hold_mins_cfg = raw_data["exit_policy"]["max_hold_mins"]
        time_stop_min_profit_pct = raw_data["exit_policy"]["time_stop_min_profit_pct"]
        if not isinstance(take_profit_pct, (int, float)):
            raise ValueError('【铁律违背】exit_policy.take_profit_pct 必须是 number')
        take_profit_pct_value = float(take_profit_pct)
        if not (take_profit_pct_value == -1.0 or take_profit_pct_value > 0):
            raise ValueError('【铁律违背】exit_policy.take_profit_pct 只允许 > 0，或 -1 表示止盈距离=止损距离')
        if not isinstance(max_hold_mins_cfg, int) or max_hold_mins_cfg < 0:
            raise ValueError('【铁律违背】exit_policy.max_hold_mins 必须是非负整数')
        if not isinstance(time_stop_min_profit_pct, (int, float)):
            raise ValueError('【铁律违背】exit_policy.time_stop_min_profit_pct 必须是 number')
        consecutive_down_bars_min = raw_data["structure"]["ab"]["consecutive_down_bars_min"]
        if not isinstance(consecutive_down_bars_min, int) or consecutive_down_bars_min <= 0:
            raise ValueError('【铁律违背】structure.ab.consecutive_down_bars_min 必须是正整数')
        cooldown_hours = raw_data["risk_controls"]["cooldown_hours"]
        if not isinstance(cooldown_hours, (int, float)):
            raise ValueError('【铁律违背】risk_controls.cooldown_hours 必须是 number')
        if float(cooldown_hours) < 0:
            raise ValueError('【铁律违背】risk_controls.cooldown_hours 必须 >= 0')

    @staticmethod
    def load(config_path: str) -> Dict[str, Any]:
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"配置文件缺失: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        StrategyConfig._require_keys(raw_data, StrategyConfig.COMMON_REQUIRED_KEYS)

        strategy_name = raw_data["strategy_name"]
        if strategy_name == "snapback":
            StrategyConfig._validate_snapback(raw_data)
        elif strategy_name == "spring-sabc":
            StrategyConfig._validate_spring(raw_data)
        else:
            raise KeyError(f"【铁律违背】未知 strategy_name: '{strategy_name}'")

        return raw_data
