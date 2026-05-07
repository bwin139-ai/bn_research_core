from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Mapping


class LiveExecutionIntentError(ValueError):
    """Raised when a strategy signal cannot enter live execution."""


@dataclass(frozen=True)
class ValidatedLiveExecutionIntent:
    """LONG-only order lifecycle contract between strategy logic and live execution."""

    strategy_name: str
    strategy_code: str
    account: str
    symbol: str
    side: str
    signal_time: int
    signal_time_bj: str
    sl_price: float
    base_order_notional_usdt: float
    full_notional_risk_pct: float
    take_profit_mode: str
    take_profit_pct: float
    max_hold_mins: int
    time_stop_min_profit_pct: float
    signal_snapshot: dict[str, Any]
    c_bar_ts: int | None = None
    c_bar_bj: str | None = None
    take_profit_r_multiple: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _fail(field: str, reason: str) -> None:
    raise LiveExecutionIntentError(f"invalid live execution intent field {field}: {reason}")


def _require(payload: Mapping[str, Any], field: str) -> Any:
    if field not in payload:
        _fail(field, "missing")
    value = payload[field]
    if value is None:
        _fail(field, "null")
    return value


def _non_empty_str(payload: Mapping[str, Any], field: str) -> str:
    value = str(_require(payload, field)).strip()
    if not value:
        _fail(field, "empty")
    return value


def _positive_int(payload: Mapping[str, Any], field: str) -> int:
    value = _require(payload, field)
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        _fail(field, f"not int: {value!r}")
    if parsed <= 0:
        _fail(field, f"must be > 0, got {parsed}")
    return parsed


def _optional_positive_int(payload: Mapping[str, Any], field: str) -> int | None:
    value = payload.get(field)
    if value in (None, ""):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        _fail(field, f"not int: {value!r}")
    if parsed <= 0:
        _fail(field, f"must be > 0, got {parsed}")
    return parsed


def _positive_float(payload: Mapping[str, Any], field: str) -> float:
    value = _require(payload, field)
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        _fail(field, f"not float: {value!r}")
    if parsed <= 0:
        _fail(field, f"must be > 0, got {parsed}")
    return parsed


def _non_negative_float(payload: Mapping[str, Any], field: str) -> float:
    value = _require(payload, field)
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        _fail(field, f"not float: {value!r}")
    if parsed < 0:
        _fail(field, f"must be >= 0, got {parsed}")
    return parsed


def _dict_field(payload: Mapping[str, Any], field: str) -> dict[str, Any]:
    value = _require(payload, field)
    if not isinstance(value, dict):
        _fail(field, f"must be dict, got {type(value).__name__}")
    return dict(value)


def validate_live_execution_intent(payload: Mapping[str, Any]) -> ValidatedLiveExecutionIntent:
    """Validate a strategy-owned signal as a public LONG-only live execution intent."""

    strategy_name = _non_empty_str(payload, "strategy_name")
    strategy_code = _non_empty_str(payload, "strategy_code").upper()
    account = _non_empty_str(payload, "account")
    symbol = _non_empty_str(payload, "symbol").upper()
    side = _non_empty_str(payload, "side").upper()
    if side != "LONG":
        _fail("side", f"only LONG is supported, got {side!r}")

    signal_time = _positive_int(payload, "signal_time")
    signal_time_bj = _non_empty_str(payload, "signal_time_bj")
    sl_price = _positive_float(payload, "sl_price")
    base_order_notional_usdt = _positive_float(payload, "base_order_notional_usdt")
    full_notional_risk_pct = _positive_float(payload, "full_notional_risk_pct")
    take_profit_mode = _non_empty_str(payload, "take_profit_mode")
    take_profit_pct = float(_require(payload, "take_profit_pct"))
    if take_profit_mode not in {"risk_reward_1r", "risk_reward_r_multiple", "fixed_pct"}:
        _fail("take_profit_mode", f"unsupported mode: {take_profit_mode!r}")
    if take_profit_mode == "fixed_pct" and take_profit_pct <= 0:
        _fail("take_profit_pct", f"fixed_pct mode requires > 0, got {take_profit_pct}")
    take_profit_r_multiple = None
    if take_profit_mode == "risk_reward_1r":
        take_profit_r_multiple = 1.0
    elif take_profit_mode == "risk_reward_r_multiple":
        take_profit_r_multiple = _positive_float(payload, "take_profit_r_multiple")
    max_hold_mins = _positive_int(payload, "max_hold_mins")
    time_stop_min_profit_pct = _non_negative_float(payload, "time_stop_min_profit_pct")
    signal_snapshot = _dict_field(payload, "signal_snapshot")
    c_bar_ts = _optional_positive_int(payload, "c_bar_ts")
    c_bar_bj_raw = payload.get("c_bar_bj")
    c_bar_bj = str(c_bar_bj_raw).strip() if c_bar_bj_raw not in (None, "") else None

    return ValidatedLiveExecutionIntent(
        strategy_name=strategy_name,
        strategy_code=strategy_code,
        account=account,
        symbol=symbol,
        side=side,
        signal_time=signal_time,
        signal_time_bj=signal_time_bj,
        sl_price=sl_price,
        base_order_notional_usdt=base_order_notional_usdt,
        full_notional_risk_pct=full_notional_risk_pct,
        take_profit_mode=take_profit_mode,
        take_profit_pct=take_profit_pct,
        max_hold_mins=max_hold_mins,
        time_stop_min_profit_pct=time_stop_min_profit_pct,
        signal_snapshot=signal_snapshot,
        c_bar_ts=c_bar_ts,
        c_bar_bj=c_bar_bj,
        take_profit_r_multiple=take_profit_r_multiple,
    )
