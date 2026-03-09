# 目标文件: core/analysis/analyzer.py

from typing import Any, Dict, List

import pandas as pd


class PerformanceAnalyzer:
    def __init__(
        self,
        trade_history: List[Dict],
        config: Dict[str, Any],
        feeder_df: pd.DataFrame,
        initial_capital: float = 100.0,  # 强制修改为默认 100 U 初始资金
        fee_rate: float = 0.001,  # 新增: 默认双边总手续费千分之一 (0.1%)
    ):
        self.trade_history = trade_history
        self.config = config
        self.feeder_df = feeder_df
        self.initial_capital = initial_capital
        self.fee_rate = fee_rate

    def generate_report(self) -> Dict:
        if not self.trade_history:
            return {"error": "没有产生任何交易记录"}

        # 1. 转换流水为 DataFrame
        trades_df = pd.DataFrame(self.trade_history)
        trades_df["entry_time_dt"] = pd.to_datetime(trades_df["entry_time"], unit="ms")
        trades_df["exit_time_dt"] = pd.to_datetime(trades_df["exit_time"], unit="ms")

        # 严格固定金额（单利）+ 扣除双边手续费模式
        trades_df["net_pnl_pct"] = trades_df["pnl_pct"] - self.fee_rate
        trades_df["equity_change"] = trades_df["net_pnl_pct"] * self.initial_capital
        trades_df["cumulative_equity"] = (
            self.initial_capital + trades_df["equity_change"].cumsum()
        )

        # 2. 计算核心指标
        total_trades = len(trades_df)
        winning_trades_df = trades_df[trades_df["equity_change"] > 0]
        losing_trades_df = trades_df[trades_df["equity_change"] <= 0]

        win_count = len(winning_trades_df)
        loss_count = len(losing_trades_df)
        win_rate = win_count / total_trades if total_trades > 0 else 0.0

        avg_profit = winning_trades_df["equity_change"].mean() if win_count > 0 else 0.0
        avg_loss = losing_trades_df["equity_change"].mean() if loss_count > 0 else 0.0

        # 最大回撤 (Max Drawdown)
        trades_df["high_water_mark"] = trades_df["cumulative_equity"].cummax()
        trades_df["drawdown"] = (
            trades_df["cumulative_equity"] - trades_df["high_water_mark"]
        ) / trades_df["high_water_mark"]
        max_drawdown = trades_df["drawdown"].min()

        # 3. 计算基准指数 (Benchmark Index)
        benchmark_series = self._calculate_benchmark()

        return {
            "summary": {
                "total_trades": total_trades,
                "win_rate": round(win_rate, 4),
                "winning_trades": win_count,
                "losing_trades": loss_count,
                "avg_profit_amount": round(avg_profit, 2),
                "avg_loss_amount": round(avg_loss, 2),
                "max_drawdown": round(max_drawdown, 4),
                "final_equity": round(trades_df["cumulative_equity"].iloc[-1], 2),
                "total_return_pct": round(
                    (trades_df["cumulative_equity"].iloc[-1] / self.initial_capital)
                    - 1.0,
                    4,
                ),
            },
            "trades_df": trades_df,
            "benchmark_series": benchmark_series,
        }

    def _calculate_benchmark(self) -> pd.Series:
        weights = self.config.get("benchmark_index", {})
        if not weights:
            return pd.Series(dtype=float)

        timestamps = (
            self.feeder_df.index.get_level_values("open_time_ms").unique().sort_values()
        )
        benchmark_values = pd.Series(0.0, index=timestamps)

        for symbol, weight in weights.items():
            try:
                sym_data = self.feeder_df.xs(symbol, level="symbol")["close"]
                initial_price = sym_data.iloc[0]
                normalized_price = sym_data / initial_price
                benchmark_values += normalized_price * weight
            except KeyError:
                continue

        return benchmark_values * self.initial_capital
