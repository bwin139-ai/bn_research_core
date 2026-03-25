import logging
from typing import Dict, List

import pandas as pd


class Order:
    def __init__(
        self,
        symbol: str,
        create_time_ms: int,
        signal_time_ms: int = None,
        signal_price: float = None,
        context: dict = None,
    ):
        self.symbol = symbol
        self.create_time_ms = create_time_ms
        self.status = "OPEN"
        self.signal_time_ms = (
            signal_time_ms if signal_time_ms is not None else create_time_ms
        )
        self.signal_price = signal_price if signal_price is not None else 0.0
        self.tp_price = 0.0
        self.sl_price = 0.0
        self.context = context if context is not None else {}


class Position:
    def __init__(
        self,
        symbol: str,
        entry_price: float,
        entry_time_ms: int,
        tp_price: float,
        sl_price: float,
        signal_time_ms: int = 0,
        signal_price: float = 0.0,
        context: dict = None,
    ):
        self.symbol = symbol
        self.entry_price = entry_price
        self.entry_time_ms = entry_time_ms
        self.tp_price = tp_price
        self.sl_price = sl_price
        self.signal_time_ms = signal_time_ms
        self.signal_price = signal_price
        self.context = context if context is not None else {}


class VirtualBroker:
    def __init__(self, config: dict = None):
        self.active_orders: Dict[str, Order] = {}
        self.active_positions: Dict[str, Position] = {}
        self.trade_history: List[dict] = []
        self.cooldown_until: Dict[str, int] = {}

        self.config = config if config is not None else {}
        self.max_hold_mins = self.config["exit_policy"]["time_stop"]["max_hold_mins"]
        self.time_stop_min_profit = self.config["exit_policy"]["time_stop"]["min_profit_pct"]

    def on_kline_close(self, current_time_ms: int, cross_section: pd.DataFrame):
        # 1. 先处理上一根 bar 收盘后产生的入场意图：
        #    在当前这根 bar 的 open 以“市价近似”成交。
        filled_symbols = []
        for sym, order in list(self.active_orders.items()):
            if sym not in cross_section.index:
                continue

            row = cross_section.loc[sym]
            exec_price = row["open"]
            self._fill_order(sym, order, current_time_ms, exec_price)
            filled_symbols.append(sym)

        for sym in filled_symbols:
            if sym in self.active_orders:
                del self.active_orders[sym]

        # 2. 再检查持仓（包括刚刚在当前 bar open 成交的新仓），
        #    因此同一根 bar 内允许发生 entry -> TP/SL。
        closed_symbols = []
        for sym, pos in list(self.active_positions.items()):
            if sym not in cross_section.index:
                continue

            row = cross_section.loc[sym]
            high, low, close = row["high"], row["low"], row["close"]

            # ⏱️ 模块二：时间熔断 (Time Stop)
            held_mins = int((current_time_ms - pos.entry_time_ms) / 60000)
            if held_mins >= self.max_hold_mins:
                current_profit_pct = (close / pos.entry_price) - 1.0
                if current_profit_pct < self.time_stop_min_profit:
                    self._close_position(sym, close, current_time_ms, "TIME_STOP")
                    closed_symbols.append(sym)
                    continue

            # 原有 TP/SL 逻辑（保持 STOP_LOSS 优先于 TAKE_PROFIT）
            if low <= pos.sl_price:
                self._close_position(sym, pos.sl_price, current_time_ms, "STOP_LOSS")
                closed_symbols.append(sym)
            elif high >= pos.tp_price:
                self._close_position(sym, pos.tp_price, current_time_ms, "TAKE_PROFIT")
                closed_symbols.append(sym)

        for sym in closed_symbols:
            if sym in self.active_positions:
                del self.active_positions[sym]

    def _fill_order(
        self, symbol: str, order: Order, time_ms: int, exec_price: float = None
    ):
        if exec_price is None:
            exec_price = order.signal_price

        pos = Position(
            symbol=symbol,
            entry_price=exec_price,
            entry_time_ms=time_ms,
            tp_price=order.tp_price,
            sl_price=order.sl_price,
            signal_time_ms=order.signal_time_ms,
            signal_price=order.signal_price,
            context=order.context,
        )
        self.active_positions[symbol] = pos
        time_str = pd.to_datetime(time_ms, unit="ms").strftime("%Y-%m-%d %H:%M")
        logging.info(
            f"[{time_str}] 市价开仓成交: {symbol} 进场多单 @ {exec_price:.4f} | "
            f"止盈: {order.tp_price:.4f} | 止损: {order.sl_price:.4f}"
        )

    def _close_position(self, symbol: str, price: float, time_ms: int, reason: str):
        pos = self.active_positions[symbol]
        pct_pnl = (price / pos.entry_price) - 1.0

        self.trade_history.append(
            {
                "symbol": symbol,
                "signal_time": pos.signal_time_ms,
                "signal_price": pos.signal_price,
                "entry_time": pos.entry_time_ms,
                "exit_time": time_ms,
                "entry_price": pos.entry_price,
                "exit_price": price,
                "pnl_pct": pct_pnl,
                "reason": reason,
                "context": pos.context,
            }
        )
        time_str = pd.to_datetime(time_ms, unit="ms").strftime("%Y-%m-%d %H:%M")
        logging.info(
            f"[{time_str}] 平仓离场: {symbol} @ {price:.4f} | 原因: {reason} | 盈亏: {pct_pnl*100:.2f}%"
        )
