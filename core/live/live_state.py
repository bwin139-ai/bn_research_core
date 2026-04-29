from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any

from core.runtime_state import load_runtime_json, save_runtime_json, _normalize_for_json

_BJ = ZoneInfo("Asia/Shanghai")


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _fmt_bj(dt: datetime) -> str:
    return dt.astimezone(_BJ).strftime("%Y-%m-%d %H:%M:%S")


def _safe_copy(value: Any) -> Any:
    return deepcopy(_normalize_for_json(value))


def _default_symbol_state() -> dict[str, Any]:
    return {
        "last_processed_bar_ts": None,
        "last_processed_bar_bj": None,
        "last_signal_side": None,
        "last_signal_bar_ts": None,
        "last_signal_bar_bj": None,
        "last_signal_c_bar_ts": None,
        "last_signal_c_bar_bj": None,
        "last_signal_time_ts": None,
        "last_signal_time_bj": None,
        "last_signal_digest": None,
        "last_signal_snapshot": None,
        "cooldown_until_ts": None,
        "cooldown_until_bj": None,
        "pending_entry_order": None,
        "open_trade": None,
        "last_position_reconcile_bj": None,
        "last_order_reconcile_bj": None,
        "last_error_code": None,
        "last_error_message": None,
        "last_error_bj": None,
    }


def _default_state(account: str, strategy_name: str) -> dict[str, Any]:
    now = _now_utc()
    return {
        "account": str(account),
        "strategy_name": str(strategy_name),
        "runner_started_at_bj": _fmt_bj(now),
        "runner_pid": None,
        "last_loop_bj": None,
        "state_version": 1,
        "symbols": {},
    }


def _strategy_file_key(strategy_name: str) -> str:
    key = str(strategy_name).strip().lower().replace("-", "_")
    if not key:
        raise ValueError("strategy_name must not be empty")
    if not all(ch.isalnum() or ch == "_" for ch in key):
        raise ValueError(f"strategy_name contains unsupported path chars: {strategy_name!r}")
    return key


def _filename(account: str, strategy_name: str = "snapback") -> str:
    account_key = str(account).strip()
    if not account_key:
        raise ValueError("account must not be empty")
    return f"live/{_strategy_file_key(strategy_name)}_{account_key}.state.json"


def load_live_state(account: str, *, strategy_name: str = "snapback") -> dict[str, Any]:
    data = load_runtime_json(_filename(account, strategy_name), default=None)
    if not isinstance(data, dict):
        data = _default_state(account, strategy_name)
    data.setdefault("account", str(account))
    data.setdefault("strategy_name", str(strategy_name))
    data.setdefault("runner_started_at_bj", _fmt_bj(_now_utc()))
    data.setdefault("runner_pid", None)
    data.setdefault("last_loop_bj", None)
    data.setdefault("state_version", 1)
    data.setdefault("symbols", {})
    if not isinstance(data["symbols"], dict):
        data["symbols"] = {}
    return data


def save_live_state(account: str, state: dict[str, Any], *, strategy_name: str = "snapback") -> None:
    state = dict(state)
    state.setdefault("account", str(account))
    state.setdefault("strategy_name", str(strategy_name))
    save_runtime_json(_filename(account, strategy_name), _normalize_for_json(state), indent=2)


def load_symbol_state(account: str, symbol: str, *, strategy_name: str = "snapback") -> dict[str, Any]:
    state = load_live_state(account, strategy_name=strategy_name)
    symbols = state.setdefault("symbols", {})
    symbol_key = str(symbol).upper().strip()
    if symbol_key not in symbols or not isinstance(symbols[symbol_key], dict):
        symbols[symbol_key] = _default_symbol_state()
        save_live_state(account, state, strategy_name=strategy_name)
    base = _default_symbol_state()
    base.update(symbols[symbol_key])
    return deepcopy(base)


def save_symbol_state(account: str, symbol: str, symbol_state: dict[str, Any], *, strategy_name: str = "snapback") -> None:
    state = load_live_state(account, strategy_name=strategy_name)
    symbols = state.setdefault("symbols", {})
    symbol_key = str(symbol).upper().strip()
    base = _default_symbol_state()
    base.update(_safe_copy(symbol_state or {}))
    symbols[symbol_key] = base
    save_live_state(account, state, strategy_name=strategy_name)


def mark_loop_heartbeat(account: str, *, runner_pid: int | None = None, strategy_name: str = "snapback") -> dict[str, Any]:
    state = load_live_state(account, strategy_name=strategy_name)
    state["last_loop_bj"] = _fmt_bj(_now_utc())
    if runner_pid is not None:
        state["runner_pid"] = int(runner_pid)
    save_live_state(account, state, strategy_name=strategy_name)
    return state


def mark_last_processed_bar(account: str, symbol: str, *, bar_ts: int | None, bar_bj: str | None, strategy_name: str = "snapback") -> dict[str, Any]:
    symbol_state = load_symbol_state(account, symbol, strategy_name=strategy_name)
    symbol_state["last_processed_bar_ts"] = bar_ts
    symbol_state["last_processed_bar_bj"] = bar_bj
    save_symbol_state(account, symbol, symbol_state, strategy_name=strategy_name)
    return symbol_state


def set_cooldown(account: str, symbol: str, *, cooldown_until_ts: int | None, cooldown_until_bj: str | None, strategy_name: str = "snapback") -> dict[str, Any]:
    symbol_state = load_symbol_state(account, symbol, strategy_name=strategy_name)
    symbol_state["cooldown_until_ts"] = cooldown_until_ts
    symbol_state["cooldown_until_bj"] = cooldown_until_bj
    save_symbol_state(account, symbol, symbol_state, strategy_name=strategy_name)
    return symbol_state


def load_cooldown_map(account: str, *, now_ts: int | None = None, strategy_name: str = "snapback") -> dict[str, int]:
    state = load_live_state(account, strategy_name=strategy_name)
    symbols = state.get("symbols") or {}
    result: dict[str, int] = {}
    now_ts_i = int(now_ts) if now_ts is not None else None
    for symbol, payload in symbols.items():
        if not isinstance(payload, dict):
            continue
        cooldown_until_ts = payload.get("cooldown_until_ts")
        if cooldown_until_ts in (None, ""):
            continue
        try:
            cooldown_until_i = int(cooldown_until_ts)
        except (TypeError, ValueError):
            continue
        if now_ts_i is not None and cooldown_until_i <= now_ts_i:
            continue
        symbol_key = str(symbol).upper().strip()
        if symbol_key:
            result[symbol_key] = cooldown_until_i
    return result


def sync_cooldown_map(account: str, cooldown_map: dict[str, int] | None, *, now_ts: int | None = None, strategy_name: str = "snapback") -> dict[str, Any]:
    state = load_live_state(account, strategy_name=strategy_name)
    symbols = state.setdefault("symbols", {})
    active_map: dict[str, int] = {}
    now_ts_i = int(now_ts) if now_ts is not None else None
    for symbol, cooldown_until_ts in (cooldown_map or {}).items():
        symbol_key = str(symbol).upper().strip()
        if not symbol_key:
            continue
        try:
            cooldown_until_i = int(cooldown_until_ts)
        except (TypeError, ValueError):
            continue
        if now_ts_i is not None and cooldown_until_i <= now_ts_i:
            continue
        active_map[symbol_key] = cooldown_until_i

    for symbol_key, cooldown_until_i in active_map.items():
        payload = _default_symbol_state()
        if isinstance(symbols.get(symbol_key), dict):
            payload.update(symbols[symbol_key])
        payload["cooldown_until_ts"] = cooldown_until_i
        payload["cooldown_until_bj"] = _fmt_bj(datetime.fromtimestamp(cooldown_until_i / 1000.0, tz=timezone.utc))
        symbols[symbol_key] = payload

    for symbol_key, payload in list(symbols.items()):
        if not isinstance(payload, dict):
            continue
        if symbol_key in active_map:
            continue
        if payload.get("cooldown_until_ts") in (None, ""):
            continue
        merged = _default_symbol_state()
        merged.update(payload)
        merged["cooldown_until_ts"] = None
        merged["cooldown_until_bj"] = None
        symbols[symbol_key] = merged

    save_live_state(account, state, strategy_name=strategy_name)
    return state


def set_pending_entry_order(account: str, symbol: str, order: dict[str, Any] | None, *, strategy_name: str = "snapback") -> dict[str, Any]:
    symbol_state = load_symbol_state(account, symbol, strategy_name=strategy_name)
    symbol_state["pending_entry_order"] = _safe_copy(order) if isinstance(order, dict) else None
    save_symbol_state(account, symbol, symbol_state, strategy_name=strategy_name)
    return symbol_state


def set_open_trade(account: str, symbol: str, trade: dict[str, Any] | None, *, strategy_name: str = "snapback") -> dict[str, Any]:
    symbol_state = load_symbol_state(account, symbol, strategy_name=strategy_name)
    symbol_state["open_trade"] = _safe_copy(trade) if isinstance(trade, dict) else None
    save_symbol_state(account, symbol, symbol_state, strategy_name=strategy_name)
    return symbol_state


def mark_signal(
    account: str,
    symbol: str,
    *,
    signal_side: str | None,
    signal_time_ts: int | None,
    signal_time_bj: str | None,
    c_bar_ts: int | None,
    c_bar_bj: str | None,
    signal_digest: str | None,
    signal_snapshot: dict[str, Any] | None,
    strategy_name: str = "snapback",
) -> dict[str, Any]:
    symbol_state = load_symbol_state(account, symbol, strategy_name=strategy_name)
    symbol_state["last_signal_side"] = signal_side
    symbol_state["last_signal_bar_ts"] = signal_time_ts
    symbol_state["last_signal_bar_bj"] = signal_time_bj
    symbol_state["last_signal_c_bar_ts"] = c_bar_ts
    symbol_state["last_signal_c_bar_bj"] = c_bar_bj
    symbol_state["last_signal_time_ts"] = signal_time_ts
    symbol_state["last_signal_time_bj"] = signal_time_bj
    symbol_state["last_signal_digest"] = signal_digest
    symbol_state["last_signal_snapshot"] = _safe_copy(signal_snapshot) if isinstance(signal_snapshot, dict) else _normalize_for_json(signal_snapshot)
    save_symbol_state(account, symbol, symbol_state, strategy_name=strategy_name)
    return symbol_state


def mark_position_reconcile(account: str, symbol: str, *, reconcile_bj: str | None, strategy_name: str = "snapback") -> dict[str, Any]:
    symbol_state = load_symbol_state(account, symbol, strategy_name=strategy_name)
    symbol_state["last_position_reconcile_bj"] = reconcile_bj
    save_symbol_state(account, symbol, symbol_state, strategy_name=strategy_name)
    return symbol_state


def mark_order_reconcile(account: str, symbol: str, *, reconcile_bj: str | None, strategy_name: str = "snapback") -> dict[str, Any]:
    symbol_state = load_symbol_state(account, symbol, strategy_name=strategy_name)
    symbol_state["last_order_reconcile_bj"] = reconcile_bj
    save_symbol_state(account, symbol, symbol_state, strategy_name=strategy_name)
    return symbol_state


def mark_error(account: str, symbol: str, *, error_code: str | None, error_message: str | None, error_bj: str | None, strategy_name: str = "snapback") -> dict[str, Any]:
    symbol_state = load_symbol_state(account, symbol, strategy_name=strategy_name)
    symbol_state["last_error_code"] = error_code
    symbol_state["last_error_message"] = error_message
    symbol_state["last_error_bj"] = error_bj
    save_symbol_state(account, symbol, symbol_state, strategy_name=strategy_name)
    return symbol_state
