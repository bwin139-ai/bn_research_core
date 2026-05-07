from __future__ import annotations

from typing import Any, Mapping

from core.live.execution_intent import ValidatedLiveExecutionIntent, validate_live_execution_intent


SWR_LIVE_STRATEGY_CODE = "SWR"


def _require_signal_field(signal: Mapping[str, Any], field: str) -> Any:
    if field not in signal:
        raise ValueError(f"sweep-reclaim signal missing required field: {field}")
    value = signal[field]
    if value is None:
        raise ValueError(f"sweep-reclaim signal field is null: {field}")
    return value


def _require_signal_params(signal: Mapping[str, Any]) -> dict[str, Any]:
    params = _require_signal_field(signal, "params")
    if not isinstance(params, dict):
        raise ValueError(f"sweep-reclaim signal params must be dict, got {type(params).__name__}")
    return dict(params)


def _require_param(params: Mapping[str, Any], field: str) -> Any:
    if field not in params:
        raise ValueError(f"sweep-reclaim signal params missing required field: {field}")
    value = params[field]
    if value is None:
        raise ValueError(f"sweep-reclaim signal params field is null: {field}")
    return value


def _extract_c_bar_ts(signal: Mapping[str, Any]) -> int:
    context = signal.get("context")
    if not isinstance(context, dict):
        raise ValueError("sweep-reclaim signal context must be dict")
    value = context.get("c_time_ms")
    if value in (None, ""):
        raise ValueError("sweep-reclaim signal context missing c_time_ms")
    return int(value)


def build_sweep_reclaim_live_execution_intent(
    signal: Mapping[str, Any],
    *,
    account: str,
) -> ValidatedLiveExecutionIntent:
    """Convert a Sweep-Reclaim signal into the public live execution boundary."""

    action = str(_require_signal_field(signal, "action")).upper().strip()
    if action != "BUY":
        raise ValueError(f"sweep-reclaim live only supports BUY/LONG signal action, got {action!r}")

    params = _require_signal_params(signal)
    payload = {
        "strategy_name": "sweep-reclaim",
        "strategy_code": SWR_LIVE_STRATEGY_CODE,
        "account": account,
        "symbol": _require_signal_field(signal, "symbol"),
        "side": "LONG",
        "signal_time": _require_signal_field(signal, "signal_time"),
        "signal_time_bj": _require_signal_field(signal, "signal_time_bj"),
        "sl_price": _require_signal_field(signal, "sl_price"),
        "base_order_notional_usdt": _require_param(params, "base_order_notional_usdt"),
        "full_notional_risk_pct": _require_param(params, "full_notional_risk_pct"),
        "take_profit_mode": _require_param(params, "take_profit_mode"),
        "take_profit_pct": -1.0,
        "take_profit_r_multiple": _require_param(params, "take_profit_r_multiple"),
        "max_hold_mins": _require_param(params, "max_hold_mins"),
        "time_stop_min_profit_pct": _require_param(params, "time_stop_min_profit_pct"),
        "signal_snapshot": dict(signal),
        "c_bar_ts": _extract_c_bar_ts(signal),
    }
    return validate_live_execution_intent(payload)
