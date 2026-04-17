import logging
from typing import Dict, List, Optional

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
        self.breakeven_guard_enabled = False
        self.breakeven_guard_armed = False
        self.breakeven_guard_trigger_r = 0.0
        self.breakeven_guard_floor_r = 0.0
        self.breakeven_trigger_price = 0.0
        self.breakeven_sl_price = 0.0
        self.breakeven_armed_time_ms: Optional[int] = None


class VirtualBroker:
    def __init__(self, config: dict = None):
        self.active_orders: Dict[str, Order] = {}
        self.active_positions: Dict[str, Position] = {}
        self.trade_history: List[dict] = []
        self.cooldown_until: Dict[str, int] = {}

        self.config = config if config is not None else {}
        strategy_name = str(self.config.get("strategy_name") or "").strip()

        self.breakeven_guard_enabled = False
        self.breakeven_guard_trigger_r = 0.0
        self.breakeven_guard_floor_r = 0.0

        if strategy_name == "spring-sabc":
            exit_policy = self.config["exit_policy"]
            self.max_hold_mins = int(exit_policy["max_hold_mins"])
            self.time_stop_min_profit = float(exit_policy["time_stop_min_profit_pct"])
            breakeven_guard = exit_policy["breakeven_guard"]
            self.breakeven_guard_enabled = bool(breakeven_guard["enabled"])
            self.breakeven_guard_trigger_r = float(breakeven_guard["trigger_r"])
            self.breakeven_guard_floor_r = float(breakeven_guard["floor_r"])
        else:
            time_stop_cfg = self.config["exit_policy"]["time_stop"]
            self.max_hold_mins = int(time_stop_cfg["max_hold_mins"])
            self.time_stop_min_profit = float(time_stop_cfg["min_profit_pct"])

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
            exit_bar_tp_sl_both_hit = bool(
                low <= pos.sl_price and high >= pos.tp_price
            )

            # 持仓检查优先级按 Spring-SABC 语义基线执行：
            # 1) 原始 STOP_LOSS
            # 2) TAKE_PROFIT
            # 3) BREAKEVEN_GUARD（armed 后下一根 bar 才允许触发）
            # 4) TIME_STOP
            #
            # live 侧 SL/TP 是 entry 后已在交易所存在的条件单，实时触发；
            # BREAKEVEN_GUARD 只能在 on_kline_close(...) 观察最近闭合 bar 后，于 CB 执行撤旧 SL / 挂新 SL。
            # 因此 sim 禁止同一根 bar 内先 armed 再 BREAKEVEN_GUARD exit。
            if low <= pos.sl_price:
                self._close_position(
                    sym,
                    pos.sl_price,
                    current_time_ms,
                    "STOP_LOSS",
                    exit_bar_tp_sl_both_hit=exit_bar_tp_sl_both_hit,
                )
                closed_symbols.append(sym)
                continue
            if high >= pos.tp_price:
                self._close_position(
                    sym,
                    pos.tp_price,
                    current_time_ms,
                    "TAKE_PROFIT",
                    exit_bar_tp_sl_both_hit=exit_bar_tp_sl_both_hit,
                )
                closed_symbols.append(sym)
                continue

            breakeven_active_from_prior_bar = (
                pos.breakeven_guard_armed
                and pos.breakeven_armed_time_ms is not None
                and int(current_time_ms) > int(pos.breakeven_armed_time_ms)
            )
            if breakeven_active_from_prior_bar and low <= pos.breakeven_sl_price:
                self._close_position(
                    sym,
                    pos.breakeven_sl_price,
                    current_time_ms,
                    "BREAKEVEN_GUARD",
                    exit_bar_tp_sl_both_hit=exit_bar_tp_sl_both_hit,
                )
                closed_symbols.append(sym)
                continue

            if pos.breakeven_guard_enabled and not pos.breakeven_guard_armed:
                if high >= pos.breakeven_trigger_price:
                    pos.breakeven_guard_armed = True
                    pos.breakeven_armed_time_ms = current_time_ms

            # ⏱️ 模块二：时间熔断 (Time Stop)
            held_mins = int((current_time_ms - pos.entry_time_ms) / 60000)
            if held_mins >= self.max_hold_mins:
                current_profit_pct = (close / pos.entry_price) - 1.0
                if current_profit_pct < self.time_stop_min_profit:
                    self._close_position(
                        sym,
                        close,
                        current_time_ms,
                        "TIME_STOP",
                        exit_bar_tp_sl_both_hit=exit_bar_tp_sl_both_hit,
                    )
                    closed_symbols.append(sym)
                    continue

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

        risk_distance = float(exec_price) - float(order.sl_price)
        if self.breakeven_guard_enabled and risk_distance > 0:
            pos.breakeven_guard_enabled = True
            pos.breakeven_guard_trigger_r = self.breakeven_guard_trigger_r
            pos.breakeven_guard_floor_r = self.breakeven_guard_floor_r
            pos.breakeven_trigger_price = float(exec_price) + risk_distance * self.breakeven_guard_trigger_r
            pos.breakeven_sl_price = float(exec_price) + risk_distance * self.breakeven_guard_floor_r

        self.active_positions[symbol] = pos
        time_str = pd.to_datetime(time_ms, unit="ms").strftime("%Y-%m-%d %H:%M")
        logging.info(
            f"[{time_str}] 市价开仓成交: {symbol} 进场多单 @ {exec_price:.4f} | "
            f"止盈: {order.tp_price:.4f} | 止损: {order.sl_price:.4f}"
        )

    def _close_position(
        self,
        symbol: str,
        price: float,
        time_ms: int,
        reason: str,
        exit_bar_tp_sl_both_hit: bool = False,
    ):
        pos = self.active_positions[symbol]
        pct_pnl = (price / pos.entry_price) - 1.0

        breakeven_armed_time_bj = None
        if pos.breakeven_armed_time_ms is not None:
            breakeven_armed_time_bj = pd.to_datetime(pos.breakeven_armed_time_ms, unit="ms").strftime("%Y-%m-%d %H:%M")

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
                "exit_bar_tp_sl_both_hit": bool(exit_bar_tp_sl_both_hit),
                "breakeven_guard_enabled": bool(pos.breakeven_guard_enabled),
                "breakeven_guard_armed": bool(pos.breakeven_guard_armed),
                "breakeven_guard_trigger_r": float(pos.breakeven_guard_trigger_r),
                "breakeven_guard_floor_r": float(pos.breakeven_guard_floor_r),
                "breakeven_trigger_price": float(pos.breakeven_trigger_price),
                "breakeven_sl_price": float(pos.breakeven_sl_price),
                "breakeven_armed_time": pos.breakeven_armed_time_ms,
                "breakeven_armed_time_bj": breakeven_armed_time_bj,
                "context": pos.context,
            }
        )
        time_str = pd.to_datetime(time_ms, unit="ms").strftime("%Y-%m-%d %H:%M")
        logging.info(
            f"[{time_str}] 平仓离场: {symbol} @ {price:.4f} | 原因: {reason} | 盈亏: {pct_pnl*100:.2f}%"
        )
