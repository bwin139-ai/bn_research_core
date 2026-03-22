from __future__ import annotations

import math
import time
import uuid
from decimal import Decimal, ROUND_DOWN
from typing import Any

from core.live.binance_client import get_client

MARGIN_TYPE = "CROSSED"
POSITION_MODE = "HEDGE"
ENTRY_ORDER_TYPE = "MARKET"
TAKE_PROFIT_ORDER_TYPE = "LIMIT"
STOP_LOSS_ORDER_TYPE = "STOP_MARKET"


def _ok(data: Any = None, **extra: Any) -> dict[str, Any]:
    payload = {"ok": True, "reason": "", "data": data}
    payload.update(extra)
    return payload


def _err(reason: str, **extra: Any) -> dict[str, Any]:
    payload = {"ok": False, "reason": str(reason), "data": None}
    payload.update(extra)
    return payload


def _call_with_retry(fn, *, retry_max: int = 0, retry_delay_secs: float = 1.0):
    last_err: Exception | None = None
    attempts = max(0, int(retry_max)) + 1
    for attempt in range(1, attempts + 1):
        try:
            result = fn()
            return _ok(result, attempts=attempt)
        except Exception as e:  # pragma: no cover - network/api path
            last_err = e
            if attempt >= attempts:
                break
            time.sleep(max(0.0, float(retry_delay_secs)))
    return _err(str(last_err or "unknown error"), attempts=attempts)


def _decimal_places_from_step(value: str | float | None) -> int:
    if value is None:
        return 0
    dec = Decimal(str(value)).normalize()
    exp = dec.as_tuple().exponent
    return max(0, -exp)


def _floor_to_step(value: float, step: float | None) -> float:
    if step is None or step <= 0:
        return float(value)
    dec_value = Decimal(str(value))
    dec_step = Decimal(str(step))
    floored = (dec_value / dec_step).to_integral_value(rounding=ROUND_DOWN) * dec_step
    return float(floored)


def _round_to_tick(value: float, tick: float | None) -> float:
    if tick is None or tick <= 0:
        return float(value)
    dec_value = Decimal(str(value))
    dec_tick = Decimal(str(tick))
    rounded = (dec_value / dec_tick).to_integral_value(rounding=ROUND_DOWN) * dec_tick
    return float(rounded)


def _normalize_position_side(position_side: str) -> str:
    side = str(position_side or "").upper().strip()
    if side not in {"LONG", "SHORT"}:
        raise ValueError(f"非法 position_side: {position_side}")
    return side


def _exit_side_for_position(position_side: str) -> str:
    pos = _normalize_position_side(position_side)
    return "SELL" if pos == "LONG" else "BUY"


def _gen_client_order_id(role: str, symbol: str) -> str:
    role_clean = (role or "X").upper()[:3]
    sym_clean = (symbol or "").upper().replace("USDT", "U")[:6]
    return f"SB{role_clean}{sym_clean}{uuid.uuid4().hex[:18]}"[:36]


def _extract_filters(raw_symbol: dict[str, Any]) -> dict[str, Any]:
    filters = {f.get("filterType"): f for f in raw_symbol.get("filters", []) if isinstance(f, dict)}
    price_filter = filters.get("PRICE_FILTER", {})
    lot_size = filters.get("LOT_SIZE", {})
    min_notional = filters.get("MIN_NOTIONAL", {})
    notional = filters.get("NOTIONAL", {})
    tick_size = price_filter.get("tickSize")
    step_size = lot_size.get("stepSize")
    return {
        "symbol": raw_symbol.get("symbol"),
        "price_precision": int(raw_symbol.get("pricePrecision", 0)),
        "quantity_precision": int(raw_symbol.get("quantityPrecision", 0)),
        "tick_size": float(tick_size) if tick_size not in (None, "") else None,
        "step_size": float(step_size) if step_size not in (None, "") else None,
        "min_qty": float(lot_size.get("minQty", 0.0) or 0.0),
        "min_notional": float(
            notional.get("notional")
            or min_notional.get("notional")
            or min_notional.get("minNotional")
            or 0.0
        ),
        "raw": raw_symbol,
    }


def get_account_status(account: str) -> dict[str, Any]:
    client = get_client(account)
    res = _call_with_retry(client.futures_account)
    if not res["ok"]:
        return res
    raw = res["data"]
    return _ok(
        {
            "margin_usdt": float(raw.get("totalMarginBalance", 0.0)),
            "wallet_usdt": float(raw.get("totalWalletBalance", 0.0)),
            "available_usdt": float(raw.get("availableBalance", 0.0)),
            "unrealized_usdt": float(raw.get("totalUnrealizedProfit", 0.0)),
            "raw": raw,
        }
    )


def get_symbol_filters(account: str, symbol: str) -> dict[str, Any]:
    client = get_client(account)
    res = _call_with_retry(client.futures_exchange_info)
    if not res["ok"]:
        return res
    su = (symbol or "").upper().strip()
    target = next((s for s in res["data"].get("symbols", []) if s.get("symbol") == su), None)
    if not target:
        return _err(f"找不到交易对: {symbol}")
    return _ok(_extract_filters(target))


def get_last_price(account: str, symbol: str) -> dict[str, Any]:
    client = get_client(account)
    su = (symbol or "").upper().strip()
    res = _call_with_retry(lambda: client.futures_symbol_ticker(symbol=su))
    if not res["ok"]:
        return res
    raw = res["data"]
    return _ok({"symbol": su, "price": float(raw["price"]), "raw": raw})


def get_open_orders(account: str, symbol: str | None = None) -> dict[str, Any]:
    client = get_client(account)
    su = (symbol or "").upper().strip()
    res = _call_with_retry(lambda: client.futures_get_open_orders(symbol=su) if su else client.futures_get_open_orders())
    if not res["ok"]:
        return res
    rows = []
    for o in res["data"]:
        rows.append(
            {
                "symbol": o.get("symbol"),
                "order_id": o.get("orderId"),
                "client_order_id": o.get("clientOrderId"),
                "side": o.get("side"),
                "position_side": o.get("positionSide"),
                "type": o.get("type"),
                "status": o.get("status"),
                "price": float(o.get("price", 0.0) or 0.0),
                "orig_qty": float(o.get("origQty", 0.0) or 0.0),
                "executed_qty": float(o.get("executedQty", 0.0) or 0.0),
                "stop_price": float(o.get("stopPrice", 0.0) or 0.0),
                "reduce_only": bool(o.get("reduceOnly", False)),
                "raw": o,
            }
        )
    return _ok(rows)


def get_positions(account: str, symbol: str | None = None) -> dict[str, Any]:
    client = get_client(account)
    res = _call_with_retry(client.futures_position_information)
    if not res["ok"]:
        return res
    su = (symbol or "").upper().strip()
    rows = []
    for p in res["data"]:
        if su and p.get("symbol") != su:
            continue
        amt = float(p.get("positionAmt", 0.0) or 0.0)
        pos_side = str(p.get("positionSide", "")).upper()
        if pos_side not in {"LONG", "SHORT"}:
            continue
        if math.isclose(amt, 0.0, abs_tol=1e-12):
            continue
        rows.append(
            {
                "symbol": p.get("symbol"),
                "position_side": pos_side,
                "qty": abs(amt),
                "signed_qty": amt,
                "entry_price": float(p.get("entryPrice", 0.0) or 0.0),
                "unrealized_usdt": float(p.get("unRealizedProfit", 0.0) or 0.0),
                "mark_price": float(p.get("markPrice", 0.0) or 0.0),
                "liquidation_price": float(p.get("liquidationPrice", 0.0) or 0.0),
                "raw": p,
            }
        )
    return _ok(rows)


def get_position(account: str, symbol: str, position_side: str) -> dict[str, Any]:
    su = (symbol or "").upper().strip()
    want_side = _normalize_position_side(position_side)
    res = get_positions(account, su)
    if not res["ok"]:
        return res
    row = next((x for x in res["data"] if x["position_side"] == want_side), None)
    return _ok(row)


def ensure_hedge_mode(account: str) -> dict[str, Any]:
    client = get_client(account)
    get_res = _call_with_retry(client.futures_get_position_mode)
    if not get_res["ok"]:
        return get_res
    raw = get_res["data"]
    dual_side = bool(raw.get("dualSidePosition", False))
    if dual_side:
        return _ok({"position_side_mode": POSITION_MODE, "changed": False, "raw": raw})
    set_res = _call_with_retry(lambda: client.futures_change_position_mode(dualSidePosition="true"))
    if not set_res["ok"]:
        return set_res
    return _ok({"position_side_mode": POSITION_MODE, "changed": True, "raw": set_res["data"]})


def ensure_cross_margin(account: str, symbol: str) -> dict[str, Any]:
    client = get_client(account)
    su = (symbol or "").upper().strip()
    res = _call_with_retry(lambda: client.futures_change_margin_type(symbol=su, marginType=MARGIN_TYPE))
    if res["ok"]:
        return _ok({"symbol": su, "margin_type": MARGIN_TYPE, "changed": True, "raw": res["data"]})
    reason = str(res["reason"])
    if "No need to change margin type" in reason or "code=-4046" in reason:
        return _ok({"symbol": su, "margin_type": MARGIN_TYPE, "changed": False, "raw": None})
    return res


def ensure_leverage(account: str, symbol: str, leverage: int) -> dict[str, Any]:
    client = get_client(account)
    su = (symbol or "").upper().strip()
    lev = int(leverage)
    res = _call_with_retry(lambda: client.futures_change_leverage(symbol=su, leverage=lev))
    if not res["ok"]:
        return res
    return _ok({"symbol": su, "leverage": int(res["data"].get("leverage", lev)), "raw": res["data"]})


def _normalize_quantity(account: str, symbol: str, quantity: float) -> dict[str, Any]:
    filters_res = get_symbol_filters(account, symbol)
    if not filters_res["ok"]:
        return filters_res
    f = filters_res["data"]
    qty = _floor_to_step(float(quantity), f["step_size"])
    if qty <= 0:
        return _err(f"quantity 归整后 <= 0: {quantity}")
    if f["min_qty"] and qty < f["min_qty"]:
        return _err(f"quantity 小于 min_qty: {qty} < {f['min_qty']}")
    return _ok({"qty": qty, "filters": f})


def _normalize_price(price: float, tick_size: float | None) -> float:
    return _round_to_tick(float(price), tick_size)


def place_entry_order(
    account: str,
    symbol: str,
    position_side: str,
    quantity: float,
    *,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
    client_order_id: str | None = None,
) -> dict[str, Any]:
    client = get_client(account)
    su = (symbol or "").upper().strip()
    pos = _normalize_position_side(position_side)
    qty_res = _normalize_quantity(account, su, quantity)
    if not qty_res["ok"]:
        return qty_res
    side = "BUY" if pos == "LONG" else "SELL"
    cid = client_order_id or _gen_client_order_id("ENT", su)
    payload = {
        "symbol": su,
        "side": side,
        "positionSide": pos,
        "type": ENTRY_ORDER_TYPE,
        "quantity": qty_res["data"]["qty"],
        "newClientOrderId": cid,
    }
    res = _call_with_retry(
        lambda: client.futures_create_order(**payload),
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not res["ok"]:
        return _err(res["reason"], payload=payload, attempts=res.get("attempts"))
    raw = res["data"]
    return _ok(
        {
            "symbol": su,
            "order_role": "ENTRY",
            "order_type": ENTRY_ORDER_TYPE,
            "side": side,
            "position_side": pos,
            "qty": qty_res["data"]["qty"],
            "client_order_id": raw.get("clientOrderId", cid),
            "exchange_order_id": raw.get("orderId"),
            "status": raw.get("status"),
            "avg_price": float(raw.get("avgPrice", 0.0) or 0.0),
            "executed_qty": float(raw.get("executedQty", 0.0) or 0.0),
            "payload": payload,
            "raw": raw,
        },
        attempts=res.get("attempts"),
    )


def place_tp_order(
    account: str,
    symbol: str,
    position_side: str,
    quantity: float,
    limit_price: float,
    *,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
    client_order_id: str | None = None,
) -> dict[str, Any]:
    client = get_client(account)
    su = (symbol or "").upper().strip()
    pos = _normalize_position_side(position_side)
    qty_res = _normalize_quantity(account, su, quantity)
    if not qty_res["ok"]:
        return qty_res
    f = qty_res["data"]["filters"]
    px = _normalize_price(limit_price, f["tick_size"])
    if px <= 0:
        return _err(f"limit_price 非法: {limit_price}")
    cid = client_order_id or _gen_client_order_id("TP", su)
    payload = {
        "symbol": su,
        "side": _exit_side_for_position(pos),
        "positionSide": pos,
        "type": TAKE_PROFIT_ORDER_TYPE,
        "timeInForce": "GTC",
        "quantity": qty_res["data"]["qty"],
        "price": px,
        "newClientOrderId": cid,
        "workingType": "CONTRACT_PRICE",
    }
    res = _call_with_retry(
        lambda: client.futures_create_order(**payload),
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not res["ok"]:
        return _err(res["reason"], payload=payload, attempts=res.get("attempts"))
    raw = res["data"]
    return _ok(
        {
            "symbol": su,
            "order_role": "TP",
            "order_type": TAKE_PROFIT_ORDER_TYPE,
            "side": payload["side"],
            "position_side": pos,
            "qty": qty_res["data"]["qty"],
            "price": px,
            "client_order_id": raw.get("clientOrderId", cid),
            "exchange_order_id": raw.get("orderId"),
            "status": raw.get("status"),
            "payload": payload,
            "raw": raw,
        },
        attempts=res.get("attempts"),
    )


def place_sl_order(
    account: str,
    symbol: str,
    position_side: str,
    stop_price: float,
    *,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
    client_order_id: str | None = None,
) -> dict[str, Any]:
    client = get_client(account)
    su = (symbol or "").upper().strip()
    pos = _normalize_position_side(position_side)
    filters_res = get_symbol_filters(account, su)
    if not filters_res["ok"]:
        return filters_res
    px = _normalize_price(stop_price, filters_res["data"]["tick_size"])
    if px <= 0:
        return _err(f"stop_price 非法: {stop_price}")
    cid = client_order_id or _gen_client_order_id("SL", su)
    payload = {
        "symbol": su,
        "side": _exit_side_for_position(pos),
        "positionSide": pos,
        "type": STOP_LOSS_ORDER_TYPE,
        "stopPrice": px,
        "closePosition": "true",
        "workingType": "CONTRACT_PRICE",
        "newClientOrderId": cid,
    }
    res = _call_with_retry(
        lambda: client.futures_create_order(**payload),
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not res["ok"]:
        return _err(res["reason"], payload=payload, attempts=res.get("attempts"))
    raw = res["data"]
    return _ok(
        {
            "symbol": su,
            "order_role": "SL",
            "order_type": STOP_LOSS_ORDER_TYPE,
            "side": payload["side"],
            "position_side": pos,
            "stop_price": px,
            "client_order_id": raw.get("clientOrderId", cid),
            "exchange_order_id": raw.get("orderId"),
            "status": raw.get("status"),
            "payload": payload,
            "raw": raw,
        },
        attempts=res.get("attempts"),
    )


def cancel_order(
    account: str,
    symbol: str,
    *,
    exchange_order_id: int | None = None,
    client_order_id: str | None = None,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
) -> dict[str, Any]:
    if exchange_order_id is None and not client_order_id:
        return _err("撤单必须提供 exchange_order_id 或 client_order_id")
    client = get_client(account)
    su = (symbol or "").upper().strip()
    payload = {"symbol": su}
    if exchange_order_id is not None:
        payload["orderId"] = int(exchange_order_id)
    if client_order_id:
        payload["origClientOrderId"] = client_order_id
    res = _call_with_retry(
        lambda: client.futures_cancel_order(**payload),
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not res["ok"]:
        return _err(res["reason"], payload=payload, attempts=res.get("attempts"))
    raw = res["data"]
    return _ok(
        {
            "symbol": su,
            "exchange_order_id": raw.get("orderId"),
            "client_order_id": raw.get("clientOrderId"),
            "status": raw.get("status"),
            "payload": payload,
            "raw": raw,
        },
        attempts=res.get("attempts"),
    )


def cancel_all_orders(
    account: str,
    symbol: str,
    *,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
) -> dict[str, Any]:
    open_res = get_open_orders(account, symbol)
    if not open_res["ok"]:
        return open_res
    rows = []
    for order in open_res["data"]:
        cancel_res = cancel_order(
            account,
            symbol,
            exchange_order_id=order["order_id"],
            retry_max=retry_max,
            retry_delay_secs=retry_delay_secs,
        )
        rows.append(cancel_res)
    return _ok(rows)
