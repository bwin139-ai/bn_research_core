from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from core.live.live_state import load_cooldown_map


def _symbol_set(values: Any) -> set[str]:
    return {str(value).upper().strip() for value in (values or []) if str(value).strip()}


def _cooldown_map(values: Mapping[str, Any] | None, *, current_time_ms: int) -> dict[str, int]:
    result: dict[str, int] = {}
    for raw_symbol, raw_until in dict(values or {}).items():
        symbol = str(raw_symbol).upper().strip()
        if not symbol:
            continue
        try:
            cooldown_until = int(raw_until)
        except (TypeError, ValueError):
            continue
        if cooldown_until <= int(current_time_ms):
            continue
        result[symbol] = cooldown_until
    return result


@dataclass(frozen=True)
class LiveSignalGate:
    strategy_name: str
    account: str
    strategy_concurrency_scope: str
    configured_active_symbols: set[str]
    pending_symbols: set[str]
    open_symbols: set[str]
    cooldown_map: dict[str, int]

    @property
    def cooldown_symbols(self) -> set[str]:
        return set(self.cooldown_map)

    @property
    def active_symbols_for_strategy(self) -> set[str]:
        return set(self.configured_active_symbols) | set(self.pending_symbols) | set(self.open_symbols)

    @property
    def blocked_symbols(self) -> set[str]:
        return set(self.active_symbols_for_strategy) | set(self.cooldown_symbols)

    @property
    def live_state_active_symbols(self) -> set[str]:
        return set(self.pending_symbols) | set(self.open_symbols)

    @property
    def blocks_new_signals(self) -> bool:
        return self.strategy_concurrency_scope == "account" and bool(self.live_state_active_symbols)

    @property
    def signal_blockers(self) -> list[str]:
        if not self.blocks_new_signals:
            return []
        blockers: list[str] = []
        if self.pending_symbols:
            blockers.append("strategy_account_pending_entry_order")
        if self.open_symbols:
            blockers.append("strategy_account_open_trade")
        return blockers

    def to_projection(self) -> dict[str, Any]:
        return {
            "strategy_name": self.strategy_name,
            "account": self.account,
            "strategy_concurrency_scope": self.strategy_concurrency_scope,
            "configured_active_symbols": sorted(self.configured_active_symbols),
            "pending_symbols": sorted(self.pending_symbols),
            "open_symbols": sorted(self.open_symbols),
            "cooldown_symbols": sorted(self.cooldown_symbols),
            "active_symbols_for_strategy": sorted(self.active_symbols_for_strategy),
            "blocked_symbols": sorted(self.blocked_symbols),
            "blocks_new_signals": bool(self.blocks_new_signals),
            "signal_blockers": list(self.signal_blockers),
        }


def build_live_signal_gate(
    *,
    account: str,
    strategy_name: str,
    current_time_ms: int,
    configured_active_symbols: set[str] | list[str] | tuple[str, ...] | None,
    strategy_concurrency_scope: str,
    account_local_precheck: Mapping[str, Any] | None = None,
    cooldown_map: Mapping[str, Any] | None = None,
) -> LiveSignalGate:
    scope = str(strategy_concurrency_scope).strip()
    if scope not in {"symbol", "account"}:
        raise ValueError("strategy_concurrency_scope must be symbol or account")

    pending_symbols: set[str] = set()
    open_symbols: set[str] = set()
    if isinstance(account_local_precheck, Mapping):
        pending_symbols = _symbol_set(account_local_precheck.get("pending_symbols"))
        open_symbols = _symbol_set(account_local_precheck.get("open_symbols"))

    if cooldown_map is None:
        cooldown_map = load_cooldown_map(
            account,
            now_ts=int(current_time_ms),
            strategy_name=strategy_name,
        )

    return LiveSignalGate(
        strategy_name=str(strategy_name),
        account=str(account),
        strategy_concurrency_scope=scope,
        configured_active_symbols=_symbol_set(configured_active_symbols),
        pending_symbols=pending_symbols,
        open_symbols=open_symbols,
        cooldown_map=_cooldown_map(cooldown_map, current_time_ms=int(current_time_ms)),
    )
