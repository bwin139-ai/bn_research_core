from __future__ import annotations

import logging
import math
import time
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_DOWN
from typing import Any

from core.live.binance_rest_gateway import (
    REQUEST_PRIORITY_CRITICAL,
    REQUEST_PRIORITY_HIGH,
    call_client_method,
    call_futures_signed,
)
from core.message_bridge import send_to_bot

MARGIN_TYPE = "CROSSED"
POSITION_MODE = "HEDGE"
ENTRY_ORDER_TYPE = "MARKET"
TAKE_PROFIT_ORDER_TYPE = "LIMIT"
STOP_LOSS_ORDER_TYPE = "STOP_MARKET"
TIME_STOP_ORDER_TYPE = "MARKET"
BJ = timezone(timedelta(hours=8))


def _ok(data: Any = None, **extra: Any) -> dict[str, Any]:
    payload = {"ok": True, "reason": "", "data": data}
    payload.update(extra)
    return payload


def _err(reason: str, **extra: Any) -> dict[str, Any]:
    payload = {"ok": False, "reason": str(reason), "data": None}
    payload.update(extra)
    return payload


def _preview_reason(reason: Any, limit: int = 180) -> str:
    text = str(reason or "").replace("\r\n", " | ").replace("\r", " | ").replace("\n", " | ").strip()
    if len(text) <= limit:
        return text
    return text[:limit] + "…"


def _fmt_event_hms(event_time_ms: Any) -> str:
    try:
        ts_ms = int(event_time_ms)
    except Exception:
        return "UNKNOWN"
    if ts_ms <= 0:
        return "UNKNOWN"
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%H:%M:%S")


def _extract_order_event_time_ms(*payloads: Any) -> int | None:
    for payload in payloads:
        if not isinstance(payload, dict):
            continue
        for key in ("update_time_ms", "time_ms", "updateTime", "time", "createTime", "transactTime"):
            value = payload.get(key)
            try:
                ts_ms = int(value)
            except Exception:
                continue
            if ts_ms > 0:
                return ts_ms
    return None


def _strategy_code_from_client_order_id(client_order_id: str | None) -> str:
    cid = str(client_order_id or "").upper()
    if "_CAL_" in cid:
        return "CAL"
    if "_TVR_" in cid:
        return "TVR"
    if "_HSH_" in cid:
        return "HSH"
    if "_SPR_" in cid:
        return "SPR"
    if "_SWR_" in cid:
        return "SWR"
    if "_SNP_" in cid:
        return "SNP"
    return "BN"


def _strategy_icon(strategy_code: str) -> str:
    if strategy_code == "SNP":
        return "🦅"
    if strategy_code == "SPR":
        return "🌱"
    if strategy_code == "SWR":
        return "📈"
    if strategy_code == "TVR":
        return "🏛"
    if strategy_code == "CAL":
        return "⚓"
    if strategy_code == "HSH":
        return "HSH"
    return "BN"


def _strategy_header_tag(strategy_code: str) -> str:
    icon = _strategy_icon(strategy_code)
    if icon == strategy_code:
        return strategy_code
    return f"{icon} {strategy_code}"


def _trade_event_status_icon(status: str | None) -> str:
    status_text = str(status or "").lower().strip()
    if status_text == "ok":
        return "✅"
    if status_text.startswith("fail") or status_text in {"error", "rejected", "reject"}:
        return "❌"
    return "🔵"


def _format_trade_event_message(
    action: str,
    status: str,
    *,
    account: str,
    symbol: str,
    qty: float | None,
    price: float | None,
    stop_price: float | None,
    client_order_id: str | None,
    exchange_order_id: int | None,
    order_status: str | None,
    reason: str | None,
    attempts: int | None,
    is_algo_order: bool | None,
    event_time_ms: int | None,
) -> str:
    strategy_code = _strategy_code_from_client_order_id(client_order_id)
    status_text = str(status or "").lower().strip()
    status_icon = _trade_event_status_icon(status_text)
    lines = [
        f"{status_icon} [{_fmt_event_hms(event_time_ms)} {_strategy_header_tag(strategy_code)}] {account}",
        f"{str(action or '').upper()} {status_text}【BN_EXEC】",
        f"symbol={symbol}",
    ]
    value_parts: list[str] = []
    if qty is not None:
        value_parts.append(f"qty={qty}")
    if price is not None:
        value_parts.append(f"price={price}")
    if value_parts:
        lines.append(" | ".join(value_parts))
    if stop_price is not None:
        lines.append(f"stop={stop_price}")
    if client_order_id:
        lines.append(f"cid={client_order_id}")
    if exchange_order_id is not None:
        lines.append(f"oid={exchange_order_id}")
    tail_parts: list[str] = []
    if order_status:
        tail_parts.append(f"status={order_status}")
    if attempts is not None:
        tail_parts.append(f"attempts={attempts}")
    if is_algo_order is not None:
        tail_parts.append(f"algo={bool(is_algo_order)}")
    if tail_parts:
        lines.append(" | ".join(tail_parts))
    if reason:
        lines.append(f"reason={_preview_reason(reason)}")
    return "\n".join(lines)


def _emit_trade_event(
    action: str,
    status: str,
    *,
    account: str,
    symbol: str,
    position_side: str | None = None,
    side: str | None = None,
    qty: float | None = None,
    price: float | None = None,
    stop_price: float | None = None,
    client_order_id: str | None = None,
    exchange_order_id: int | None = None,
    order_status: str | None = None,
    reason: str | None = None,
    attempts: int | None = None,
    is_algo_order: bool | None = None,
    event_time_ms: int | None = None,
    notify_label: str = "snapback",
) -> None:
    queue_label = str(notify_label or "").strip()
    if not queue_label:
        raise ValueError("notify_label must not be empty")
    legacy_parts: list[str] = [
        f"[BN_EXEC] {str(action or '').upper()} {str(status or '').lower()}",
        f"account={account}",
        f"symbol={symbol}",
    ]
    if position_side:
        legacy_parts.append(f"pos={position_side}")
    if side:
        legacy_parts.append(f"side={side}")
    if qty is not None:
        legacy_parts.append(f"qty={qty}")
    if price is not None:
        legacy_parts.append(f"price={price}")
    if stop_price is not None:
        legacy_parts.append(f"stop={stop_price}")
    if client_order_id:
        legacy_parts.append(f"cid={client_order_id}")
    if exchange_order_id is not None:
        legacy_parts.append(f"oid={exchange_order_id}")
    if order_status:
        legacy_parts.append(f"status={order_status}")
    if attempts is not None:
        legacy_parts.append(f"attempts={attempts}")
    if is_algo_order is not None:
        legacy_parts.append(f"algo={bool(is_algo_order)}")
    if reason:
        legacy_parts.append(f"reason={_preview_reason(reason)}")
    log_msg = " | ".join(legacy_parts)
    bot_msg = _format_trade_event_message(
        action,
        status,
        account=account,
        symbol=symbol,
        qty=qty,
        price=price,
        stop_price=stop_price,
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
        order_status=order_status,
        reason=reason,
        attempts=attempts,
        is_algo_order=is_algo_order,
        event_time_ms=event_time_ms,
    )
    if str(status or "").lower() == "ok":
        logging.info(log_msg)
    else:
        logging.error(log_msg)
    send_to_bot(bot_msg, label=queue_label)


def _call_with_retry(
    fn,
    *,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
    before_attempt=None,
    on_success=None,
):
    last_err: Exception | None = None
    attempts = max(0, int(retry_max)) + 1
    for attempt in range(1, attempts + 1):
        try:
            if callable(before_attempt):
                before_attempt()
            result = fn()
            if callable(on_success):
                on_success(result)
            return _ok(result, attempts=attempt)
        except Exception as e:  # pragma: no cover - network/api path
            last_err = e
            if attempt >= attempts:
                break
            time.sleep(max(0.0, float(retry_delay_secs)))
    return _err(str(last_err or "unknown error"), attempts=attempts)


def _call_gateway_client_with_retry(
    account: str,
    source: str,
    method_name: str,
    *,
    priority: str = REQUEST_PRIORITY_HIGH,
    payload: dict[str, Any] | None = None,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
):
    return _call_with_retry(
        lambda: call_client_method(
            account,
            source=source,
            method_name=method_name,
            priority=priority,
            **dict(payload or {}),
        ),
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )


def _normalize_algo_order_row(raw: dict[str, Any]) -> dict[str, Any]:
    algo_status = str(raw.get("algoStatus") or raw.get("status") or "").upper()
    trigger_price = raw.get("triggerPrice", raw.get("stopPrice", 0.0))
    price = raw.get("price", 0.0)
    quantity = raw.get("quantity", 0.0)
    actual_price = raw.get("actualPrice", 0.0)
    return {
        "symbol": raw.get("symbol"),
        "order_id": raw.get("algoId"),
        "client_order_id": raw.get("clientAlgoId"),
        "side": raw.get("side"),
        "position_side": raw.get("positionSide"),
        "type": raw.get("orderType") or raw.get("type"),
        "status": algo_status,
        "price": float(price or 0.0),
        "avg_price": float(actual_price or 0.0),
        "orig_qty": float(quantity or 0.0),
        "executed_qty": 0.0,
        "cum_quote": 0.0,
        "stop_price": float(trigger_price or 0.0),
        "reduce_only": bool(raw.get("reduceOnly", False)),
        "close_position": bool(raw.get("closePosition", False)),
        "algo_id": raw.get("algoId"),
        "client_algo_id": raw.get("clientAlgoId"),
        "algo_status": algo_status,
        "actual_order_id": raw.get("actualOrderId"),
        "actual_price": float(actual_price or 0.0),
        "is_algo_order": True,
        "raw": raw,
    }


def _is_order_not_found_reason(reason: str | None) -> bool:
    text = str(reason or "")
    needles = [
        "code=-2013",
        "Order does not exist",
        "Unknown order sent",
        "algo order does not exist",
        "order not exist",
    ]
    return any(needle in text for needle in needles)


def _get_open_algo_orders(account: str, symbol: str | None = None) -> list[dict[str, Any]]:
    payload: dict[str, Any] = {}
    su = (symbol or "").upper().strip()
    if su:
        payload["symbol"] = su
    data = call_futures_signed(
        account,
        source='binance_exec.algo:/fapi/v1/openAlgoOrders',
        method="GET",
        path="/fapi/v1/openAlgoOrders",
        params=payload,
        priority=REQUEST_PRIORITY_HIGH,
    )
    if not isinstance(data, list):
        raise RuntimeError(f"unexpected openAlgoOrders payload: {data}")
    return data


def _query_algo_order(account: str, *, exchange_order_id: int | None = None, client_order_id: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if exchange_order_id is not None:
        payload["algoId"] = int(exchange_order_id)
    if client_order_id:
        payload["clientAlgoId"] = client_order_id
    if not payload:
        raise ValueError("查询 algo 订单必须提供 algoId 或 clientAlgoId")
    data = call_futures_signed(
        account,
        source='binance_exec.algo:/fapi/v1/algoOrder',
        method="GET",
        path="/fapi/v1/algoOrder",
        params=payload,
        priority=REQUEST_PRIORITY_HIGH,
    )
    if not isinstance(data, dict):
        raise RuntimeError(f"unexpected algoOrder payload: {data}")
    return data


def _cancel_algo_order(account: str, *, exchange_order_id: int | None = None, client_order_id: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    if exchange_order_id is not None:
        payload["algoId"] = int(exchange_order_id)
    if client_order_id:
        payload["clientAlgoId"] = client_order_id
    if not payload:
        raise ValueError("撤销 algo 订单必须提供 algoId 或 clientAlgoId")
    data = call_futures_signed(
        account,
        source='binance_exec.algo:/fapi/v1/algoOrder',
        method="DELETE",
        path="/fapi/v1/algoOrder",
        params=payload,
        priority=REQUEST_PRIORITY_CRITICAL,
    )
    if not isinstance(data, dict):
        raise RuntimeError(f"unexpected cancel algo payload: {data}")
    return data


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


def _normalize_order_row(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": raw.get("symbol"),
        "order_id": raw.get("orderId"),
        "client_order_id": raw.get("clientOrderId"),
        "side": raw.get("side"),
        "position_side": raw.get("positionSide"),
        "type": raw.get("type"),
        "status": raw.get("status"),
        "price": float(raw.get("price", 0.0) or 0.0),
        "avg_price": float(raw.get("avgPrice", 0.0) or 0.0),
        "orig_qty": float(raw.get("origQty", 0.0) or 0.0),
        "executed_qty": float(raw.get("executedQty", 0.0) or 0.0),
        "cum_quote": float(raw.get("cumQuote", 0.0) or 0.0),
        "stop_price": float(raw.get("stopPrice", 0.0) or 0.0),
        "reduce_only": bool(raw.get("reduceOnly", False)),
        "close_position": bool(raw.get("closePosition", False)),
        "time_ms": raw.get("time"),
        "update_time_ms": raw.get("updateTime"),
        "working_type": raw.get("workingType"),
        "orig_type": raw.get("origType"),
        "price_protect": raw.get("priceProtect"),
        "raw": raw,
    }


def _normalize_trade_row(raw: dict[str, Any]) -> dict[str, Any]:
    buyer = raw.get("buyer")
    side = raw.get("side")
    if not side:
        if buyer is True:
            side = "BUY"
        elif buyer is False:
            side = "SELL"
    return {
        "symbol": raw.get("symbol"),
        "trade_id": raw.get("id"),
        "order_id": raw.get("orderId"),
        "side": side,
        "position_side": raw.get("positionSide"),
        "price": float(raw.get("price", 0.0) or 0.0),
        "qty": float(raw.get("qty", 0.0) or 0.0),
        "quote_qty": float(raw.get("quoteQty", 0.0) or 0.0),
        "commission": float(raw.get("commission", 0.0) or 0.0),
        "commission_asset": raw.get("commissionAsset"),
        "realized_pnl": float(raw.get("realizedPnl", 0.0) or 0.0),
        "maker": bool(raw.get("maker", False)),
        "buyer": raw.get("buyer"),
        "time_ms": raw.get("time"),
        "raw": raw,
    }


def _normalize_income_row(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "symbol": raw.get("symbol"),
        "income_type": raw.get("incomeType"),
        "income": float(raw.get("income", 0.0) or 0.0),
        "asset": raw.get("asset"),
        "info": raw.get("info"),
        "time_ms": raw.get("time"),
        "tran_id": raw.get("tranId"),
        "trade_id": raw.get("tradeId"),
        "raw": raw,
    }


def get_account_status(account: str) -> dict[str, Any]:
    res = _call_gateway_client_with_retry(account, 'binance_exec.futures_account', 'futures_account')
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
    res = _call_gateway_client_with_retry(account, 'binance_exec.futures_exchange_info', 'futures_exchange_info')
    if not res["ok"]:
        return res
    su = (symbol or "").upper().strip()
    target = next((s for s in res["data"].get("symbols", []) if s.get("symbol") == su), None)
    if not target:
        return _err(f"找不到交易对: {symbol}")
    return _ok(_extract_filters(target))


def get_last_price(account: str, symbol: str) -> dict[str, Any]:
    su = (symbol or "").upper().strip()
    res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_symbol_ticker',
        'futures_symbol_ticker',
        payload={'symbol': su},
    )
    if not res["ok"]:
        return res
    raw = res["data"]
    return _ok({"symbol": su, "price": float(raw["price"]), "raw": raw})


def get_order_book_top(account: str, symbol: str) -> dict[str, Any]:
    su = (symbol or "").upper().strip()
    res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_order_book',
        'futures_order_book',
        payload={'symbol': su, 'limit': 5},
        priority=REQUEST_PRIORITY_HIGH,
    )
    if not res["ok"]:
        return res
    raw = res["data"]
    bids = raw.get("bids") or []
    asks = raw.get("asks") or []
    if not bids or not asks:
        return _err(f"empty order book: {su}", raw=raw)
    best_bid = float(bids[0][0])
    best_bid_qty = float(bids[0][1])
    best_ask = float(asks[0][0])
    best_ask_qty = float(asks[0][1])
    if best_bid <= 0 or best_ask <= 0:
        return _err(f"invalid order book top: {su}", raw=raw)
    return _ok(
        {
            "symbol": su,
            "best_bid": best_bid,
            "best_bid_qty": best_bid_qty,
            "best_ask": best_ask,
            "best_ask_qty": best_ask_qty,
            "raw": raw,
        }
    )


def get_open_orders(account: str, symbol: str | None = None) -> dict[str, Any]:
    su = (symbol or "").upper().strip()
    payload = {'symbol': su} if su else {}
    base_res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_get_open_orders',
        'futures_get_open_orders',
        payload=payload,
    )
    if not base_res["ok"]:
        return base_res
    algo_res = _call_with_retry(lambda: _get_open_algo_orders(account, su if su else None))
    if not algo_res["ok"]:
        return algo_res
    rows = [_normalize_order_row(o) for o in base_res["data"]]
    rows.extend(_normalize_algo_order_row(o) for o in algo_res["data"])
    return _ok(rows)


def get_order(
    account: str,
    symbol: str,
    *,
    exchange_order_id: int | None = None,
    client_order_id: str | None = None,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
) -> dict[str, Any]:
    if exchange_order_id is None and not client_order_id:
        return _err("查询订单必须提供 exchange_order_id 或 client_order_id")
    su = (symbol or "").upper().strip()
    payload: dict[str, Any] = {"symbol": su}
    if exchange_order_id is not None:
        payload["orderId"] = int(exchange_order_id)
    if client_order_id:
        payload["origClientOrderId"] = client_order_id
    res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_get_order',
        'futures_get_order',
        payload=payload,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if res["ok"]:
        return _ok(_normalize_order_row(res["data"]), attempts=res.get("attempts"))
    if not _is_order_not_found_reason(res["reason"]):
        return _err(res["reason"], payload=payload, attempts=res.get("attempts"))
    algo_res = _call_with_retry(
        lambda: _query_algo_order(
            account,
            exchange_order_id=exchange_order_id,
            client_order_id=client_order_id,
        ),
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not algo_res["ok"]:
        return _err(algo_res["reason"], payload=payload, attempts=algo_res.get("attempts"))
    return _ok(_normalize_algo_order_row(algo_res["data"]), attempts=algo_res.get("attempts"))


def _normalize_position_snapshot_row(raw: dict[str, Any]) -> dict[str, Any]:
    amt = float(raw.get("positionAmt", 0.0) or 0.0)
    pos_side = str(raw.get("positionSide", "")).upper()
    return {
        "symbol": raw.get("symbol"),
        "position_side": pos_side,
        "qty": abs(amt),
        "signed_qty": amt,
        "entry_price": float(raw.get("entryPrice", 0.0) or 0.0),
        "unrealized_usdt": float(raw.get("unRealizedProfit", 0.0) or 0.0),
        "mark_price": float(raw.get("markPrice", 0.0) or 0.0),
        "liquidation_price": float(raw.get("liquidationPrice", 0.0) or 0.0),
        "margin_type": raw.get("marginType"),
        "isolated_wallet": float(raw.get("isolatedWallet", 0.0) or 0.0),
        "raw": raw,
    }


def get_positions(account: str, symbol: str | None = None) -> dict[str, Any]:
    res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_position_information',
        'futures_position_information',
    )
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


def get_position_mode(account: str) -> dict[str, Any]:
    get_res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_get_position_mode',
        'futures_get_position_mode',
    )
    if not get_res["ok"]:
        return get_res
    raw = get_res["data"]
    dual_side = bool(raw.get("dualSidePosition", False))
    mode = POSITION_MODE if dual_side else "ONE_WAY"
    return _ok({"position_side_mode": mode, "dual_side": dual_side, "raw": raw})


def ensure_hedge_mode(account: str) -> dict[str, Any]:
    mode_res = get_position_mode(account)
    if not mode_res["ok"]:
        return mode_res
    if bool(mode_res["data"].get("dual_side")):
        return _ok({"position_side_mode": POSITION_MODE, "changed": False, "raw": mode_res["data"].get("raw")})
    set_res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_change_position_mode',
        'futures_change_position_mode',
        priority=REQUEST_PRIORITY_CRITICAL,
        payload={'dualSidePosition': "true"},
    )
    if not set_res["ok"]:
        return set_res
    return _ok({"position_side_mode": POSITION_MODE, "changed": True, "raw": set_res["data"]})


def ensure_cross_margin(account: str, symbol: str) -> dict[str, Any]:
    su = (symbol or "").upper().strip()
    res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_change_margin_type',
        'futures_change_margin_type',
        priority=REQUEST_PRIORITY_CRITICAL,
        payload={'symbol': su, 'marginType': MARGIN_TYPE},
    )
    if res["ok"]:
        return _ok({"symbol": su, "margin_type": MARGIN_TYPE, "changed": True, "raw": res["data"]})
    reason = str(res["reason"])
    if "No need to change margin type" in reason or "code=-4046" in reason:
        return _ok({"symbol": su, "margin_type": MARGIN_TYPE, "changed": False, "raw": None})
    return res


def _max_initial_leverage(account: str, symbol: str) -> dict[str, Any]:
    su = (symbol or "").upper().strip()
    res = _call_with_retry(
        lambda: call_futures_signed(
            account,
            source='binance_exec.futures_leverage_bracket',
            method='GET',
            path='/fapi/v1/leverageBracket',
            params={'symbol': su},
            priority=REQUEST_PRIORITY_CRITICAL,
        ),
        retry_max=0,
        retry_delay_secs=1.0,
    )
    if not res.get("ok"):
        return res
    rows = res.get("data")
    if not isinstance(rows, list) or not rows:
        return _err(f"unexpected leverageBracket payload: {rows!r}")
    symbol_row = None
    for row in rows:
        if isinstance(row, dict) and str(row.get("symbol") or "").upper() == su:
            symbol_row = row
            break
    if symbol_row is None and len(rows) == 1 and isinstance(rows[0], dict):
        symbol_row = rows[0]
    if not isinstance(symbol_row, dict):
        return _err(f"leverageBracket symbol row missing: symbol={su}")
    brackets = symbol_row.get("brackets")
    if not isinstance(brackets, list) or not brackets:
        return _err(f"leverageBracket brackets missing: symbol={su}")
    max_leverage = 0
    for bracket in brackets:
        if not isinstance(bracket, dict):
            continue
        try:
            value = int(bracket.get("initialLeverage") or 0)
        except Exception:
            value = 0
        if value > max_leverage:
            max_leverage = value
    if max_leverage <= 0:
        return _err(f"leverageBracket max leverage invalid: symbol={su}")
    return _ok({"symbol": su, "max_initial_leverage": max_leverage, "raw": symbol_row})


def ensure_leverage(account: str, symbol: str, leverage: int, *, allow_max_downgrade: bool = False) -> dict[str, Any]:
    su = (symbol or "").upper().strip()
    lev = int(leverage)
    res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_change_leverage',
        'futures_change_leverage',
        priority=REQUEST_PRIORITY_CRITICAL,
        payload={'symbol': su, 'leverage': lev},
    )
    if not res["ok"] and bool(allow_max_downgrade):
        bracket_res = _max_initial_leverage(account, su)
        if bracket_res.get("ok"):
            max_lev = int((bracket_res.get("data") or {}).get("max_initial_leverage") or 0)
            if 0 < max_lev < lev:
                downgrade_res = _call_gateway_client_with_retry(
                    account,
                    'binance_exec.futures_change_leverage',
                    'futures_change_leverage',
                    priority=REQUEST_PRIORITY_CRITICAL,
                    payload={'symbol': su, 'leverage': max_lev},
                )
                if downgrade_res.get("ok"):
                    data = dict(downgrade_res.get("data") or {})
                    return _ok({
                        "symbol": su,
                        "leverage": int(data.get("leverage", max_lev)),
                        "requested_leverage": lev,
                        "effective_leverage": int(data.get("leverage", max_lev)),
                        "leverage_downgraded": True,
                        "downgrade_reason": "exchange_max_initial_leverage",
                        "initial_error": res,
                        "leverage_bracket": bracket_res.get("data"),
                        "raw": data,
                    })
                downgraded = dict(downgrade_res)
                downgraded["initial_error"] = res
                downgraded["leverage_bracket"] = bracket_res
                return downgraded
        enriched = dict(res)
        enriched["leverage_bracket"] = bracket_res
        return enriched
    if not res["ok"]:
        return res
    return _ok({"symbol": su, "leverage": int(res["data"].get("leverage", lev)), "raw": res["data"]})


def resolve_order_fill_price(order_row: dict[str, Any] | None, *, fallback_price: float | None = None) -> dict[str, Any]:
    row = order_row or {}
    avg_price = float(row.get("avg_price", 0.0) or 0.0)
    if avg_price > 0:
        return _ok({"fill_price": avg_price, "price_source": "avg_price"})

    executed_qty = float(row.get("executed_qty", 0.0) or 0.0)
    cum_quote = float(row.get("cum_quote", 0.0) or 0.0)
    if executed_qty > 0 and cum_quote > 0:
        return _ok({"fill_price": (cum_quote / executed_qty), "price_source": "cum_quote_div_executed_qty"})

    if fallback_price is not None:
        fallback = float(fallback_price or 0.0)
        if fallback > 0:
            return _ok({"fill_price": fallback, "price_source": "fallback_price"})

    return _err("entry fill price unavailable")


def _entry_fill_fields_complete(order_row: dict[str, Any] | None) -> bool:
    row = order_row or {}
    avg_price = float(row.get("avg_price", 0.0) or 0.0)
    if avg_price > 0:
        return True
    executed_qty = float(row.get("executed_qty", 0.0) or 0.0)
    cum_quote = float(row.get("cum_quote", 0.0) or 0.0)
    return executed_qty > 0 and cum_quote > 0


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
    notify_label: str = "snapback",
) -> dict[str, Any]:
    su = (symbol or "").upper().strip()
    pos = _normalize_position_side(position_side)
    qty_res = _normalize_quantity(account, su, quantity)
    if not qty_res["ok"]:
        _emit_trade_event(
            "ENTRY",
            "fail",
            account=account,
            symbol=su,
            position_side=pos,
            client_order_id=client_order_id,
            reason=qty_res["reason"],
            attempts=qty_res.get("attempts"),
            notify_label=notify_label,
        )
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
    res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_create_order',
        'futures_create_order',
        priority=REQUEST_PRIORITY_CRITICAL,
        payload=payload,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not res["ok"]:
        _emit_trade_event(
            "ENTRY",
            "fail",
            account=account,
            symbol=su,
            position_side=pos,
            side=side,
            qty=qty_res["data"]["qty"],
            client_order_id=cid,
            reason=res["reason"],
            attempts=res.get("attempts"),
            notify_label=notify_label,
        )
        return _err(res["reason"], payload=payload, attempts=res.get("attempts"))
    raw = res["data"]
    client_order_id_value = raw.get("clientOrderId", cid)
    exchange_order_id_value = raw.get("orderId")
    normalized_entry = _normalize_order_row(raw)

    fill_query_res: dict[str, Any] = {
        "ok": True,
        "reason": "",
        "data": None,
        "skipped": True,
    }
    if not _entry_fill_fields_complete(normalized_entry):
        fill_query_res = get_order(
            account,
            su,
            exchange_order_id=exchange_order_id_value,
            client_order_id=client_order_id_value,
            retry_max=max(int(retry_max), 1),
            retry_delay_secs=retry_delay_secs,
        )
        if fill_query_res.get("ok") and fill_query_res.get("data"):
            queried_entry = fill_query_res["data"]
            normalized_entry["avg_price"] = float(queried_entry.get("avg_price", 0.0) or 0.0)
            normalized_entry["executed_qty"] = float(queried_entry.get("executed_qty", 0.0) or 0.0)
            normalized_entry["cum_quote"] = float(queried_entry.get("cum_quote", 0.0) or 0.0)
            normalized_entry["status"] = queried_entry.get("status") or normalized_entry.get("status")

    _emit_trade_event(
        "ENTRY",
        "ok",
        account=account,
        symbol=su,
        position_side=pos,
        side=side,
        qty=qty_res["data"]["qty"],
        price=float(normalized_entry.get("avg_price", 0.0) or 0.0) or None,
        client_order_id=client_order_id_value,
        exchange_order_id=exchange_order_id_value,
        order_status=normalized_entry.get("status"),
        attempts=res.get("attempts"),
        event_time_ms=_extract_order_event_time_ms(normalized_entry, raw),
        notify_label=notify_label,
    )
    return _ok(
        {
            "symbol": su,
            "order_role": "ENTRY",
            "order_type": ENTRY_ORDER_TYPE,
            "side": side,
            "position_side": pos,
            "qty": qty_res["data"]["qty"],
            "client_order_id": client_order_id_value,
            "exchange_order_id": exchange_order_id_value,
            "status": normalized_entry.get("status"),
            "avg_price": float(normalized_entry.get("avg_price", 0.0) or 0.0),
            "executed_qty": float(normalized_entry.get("executed_qty", 0.0) or 0.0),
            "cum_quote": float(normalized_entry.get("cum_quote", 0.0) or 0.0),
            "fill_query_ok": bool(fill_query_res.get("ok", False)),
            "fill_query_reason": fill_query_res.get("reason"),
            "fill_query_attempts": fill_query_res.get("attempts"),
            "fill_query_skipped": bool(fill_query_res.get("skipped", False)),
            "payload": payload,
            "raw": raw,
            "fill_query_snapshot": fill_query_res if not fill_query_res.get("skipped") else None,
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
    notify_label: str = "snapback",
) -> dict[str, Any]:
    su = (symbol or "").upper().strip()
    pos = _normalize_position_side(position_side)
    qty_res = _normalize_quantity(account, su, quantity)
    if not qty_res["ok"]:
        _emit_trade_event(
            "TP",
            "fail",
            account=account,
            symbol=su,
            position_side=pos,
            client_order_id=client_order_id,
            reason=qty_res["reason"],
            attempts=qty_res.get("attempts"),
            notify_label=notify_label,
        )
        return qty_res
    f = qty_res["data"]["filters"]
    px = _normalize_price(limit_price, f["tick_size"])
    if px <= 0:
        reason = f"limit_price 非法: {limit_price}"
        _emit_trade_event(
            "TP",
            "fail",
            account=account,
            symbol=su,
            position_side=pos,
            side=_exit_side_for_position(pos),
            qty=qty_res["data"]["qty"],
            client_order_id=client_order_id,
            reason=reason,
            notify_label=notify_label,
        )
        return _err(reason)
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
    res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_create_order',
        'futures_create_order',
        priority=REQUEST_PRIORITY_CRITICAL,
        payload=payload,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not res["ok"]:
        _emit_trade_event(
            "TP",
            "fail",
            account=account,
            symbol=su,
            position_side=pos,
            side=payload["side"],
            qty=qty_res["data"]["qty"],
            price=px,
            client_order_id=cid,
            reason=res["reason"],
            attempts=res.get("attempts"),
            notify_label=notify_label,
        )
        return _err(res["reason"], payload=payload, attempts=res.get("attempts"))
    raw = res["data"]
    _emit_trade_event(
        "TP",
        "ok",
        account=account,
        symbol=su,
        position_side=pos,
        side=payload["side"],
        qty=qty_res["data"]["qty"],
        price=px,
        client_order_id=raw.get("clientOrderId", cid),
        exchange_order_id=raw.get("orderId"),
        order_status=raw.get("status"),
        attempts=res.get("attempts"),
        event_time_ms=_extract_order_event_time_ms(raw),
        notify_label=notify_label,
    )
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


def place_limit_order(
    account: str,
    symbol: str,
    position_side: str,
    side: str,
    quantity: float,
    limit_price: float | None,
    *,
    order_role: str,
    time_in_force: str,
    price_match: str | None = None,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
    client_order_id: str | None = None,
    notify_label: str = "snapback",
    notify_on_error: bool = True,
    notify_on_success: bool = True,
    notify_order_statuses: set[str] | None = None,
) -> dict[str, Any]:
    su = (symbol or "").upper().strip()
    pos = _normalize_position_side(position_side)
    side_value = str(side or "").upper().strip()
    if side_value not in {"BUY", "SELL"}:
        return _err(f"unsupported LIMIT side: {side}")
    role = str(order_role or "LIMIT").upper().strip() or "LIMIT"
    tif = str(time_in_force or "").upper().strip()
    if not tif:
        return _err("time_in_force is required")
    qty_res = _normalize_quantity(account, su, quantity)
    if not qty_res["ok"]:
        if notify_on_error:
            _emit_trade_event(
                role,
                "fail",
                account=account,
                symbol=su,
                position_side=pos,
                side=side_value,
                client_order_id=client_order_id,
                reason=qty_res["reason"],
                attempts=qty_res.get("attempts"),
                notify_label=notify_label,
            )
        return qty_res
    price_match_value = str(price_match or "").upper().strip()
    if price_match_value and price_match_value not in {"QUEUE", "QUEUE_5"}:
        return _err(f"unsupported price_match: {price_match}")
    filters = qty_res["data"]["filters"]
    px = None
    if not price_match_value:
        px = _normalize_price(float(limit_price or 0.0), filters["tick_size"])
    if not price_match_value and (px is None or px <= 0):
        reason = f"limit_price 非法: {limit_price}"
        if notify_on_error:
            _emit_trade_event(
                role,
                "fail",
                account=account,
                symbol=su,
                position_side=pos,
                side=side_value,
                qty=qty_res["data"]["qty"],
                client_order_id=client_order_id,
                reason=reason,
                notify_label=notify_label,
            )
        return _err(reason)
    cid = client_order_id or _gen_client_order_id(role[:3], su)
    payload = {
        "symbol": su,
        "side": side_value,
        "positionSide": pos,
        "type": TAKE_PROFIT_ORDER_TYPE,
        "timeInForce": tif,
        "quantity": qty_res["data"]["qty"],
        "newClientOrderId": cid,
    }
    if price_match_value:
        payload["priceMatch"] = price_match_value
    else:
        payload["price"] = px
    res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_create_order',
        'futures_create_order',
        priority=REQUEST_PRIORITY_CRITICAL,
        payload=payload,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not res["ok"]:
        if notify_on_error:
            _emit_trade_event(
                role,
                "fail",
                account=account,
                symbol=su,
                position_side=pos,
                side=side_value,
                qty=qty_res["data"]["qty"],
                price=px,
                client_order_id=cid,
                reason=res["reason"],
                attempts=res.get("attempts"),
                notify_label=notify_label,
            )
        return _err(res["reason"], payload=payload, attempts=res.get("attempts"))
    raw = res["data"]
    normalized = _normalize_order_row(raw)
    normalized_status = str(normalized.get("status") or "").upper()
    allowed_notify_statuses = (
        {str(x).upper() for x in notify_order_statuses}
        if notify_order_statuses is not None
        else None
    )
    if notify_on_success and (allowed_notify_statuses is None or normalized_status in allowed_notify_statuses):
        _emit_trade_event(
            role,
            "ok",
            account=account,
            symbol=su,
            position_side=pos,
            side=side_value,
            qty=qty_res["data"]["qty"],
            price=px,
            client_order_id=raw.get("clientOrderId", cid),
            exchange_order_id=raw.get("orderId"),
            order_status=raw.get("status"),
            attempts=res.get("attempts"),
            event_time_ms=_extract_order_event_time_ms(normalized, raw),
            notify_label=notify_label,
        )
    normalized.update({
        "order_role": role,
        "order_type": TAKE_PROFIT_ORDER_TYPE,
        "time_in_force": tif,
        "price_match": price_match_value or None,
        "payload": payload,
    })
    return _ok(normalized, attempts=res.get("attempts"))


def place_sl_order(
    account: str,
    symbol: str,
    position_side: str,
    stop_price: float,
    *,
    quantity: float | None = None,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
    client_order_id: str | None = None,
    notify_label: str = "snapback",
) -> dict[str, Any]:
    su = (symbol or "").upper().strip()
    pos = _normalize_position_side(position_side)
    filters_res = get_symbol_filters(account, su)
    if not filters_res["ok"]:
        _emit_trade_event(
            "SL",
            "fail",
            account=account,
            symbol=su,
            position_side=pos,
            stop_price=stop_price,
            client_order_id=client_order_id,
            reason=filters_res["reason"],
            attempts=filters_res.get("attempts"),
            is_algo_order=True,
            notify_label=notify_label,
        )
        return filters_res
    px = _normalize_price(stop_price, filters_res["data"]["tick_size"])
    if px <= 0:
        reason = f"stop_price 非法: {stop_price}"
        _emit_trade_event(
            "SL",
            "fail",
            account=account,
            symbol=su,
            position_side=pos,
            stop_price=stop_price,
            client_order_id=client_order_id,
            reason=reason,
            is_algo_order=True,
            notify_label=notify_label,
        )
        return _err(reason)
    qty_value: float | None = None
    if quantity is not None:
        qty_res = _normalize_quantity(account, su, quantity)
        if not qty_res["ok"]:
            _emit_trade_event(
                "SL",
                "fail",
                account=account,
                symbol=su,
                position_side=pos,
                stop_price=px,
                client_order_id=client_order_id,
                reason=qty_res["reason"],
                attempts=qty_res.get("attempts"),
                is_algo_order=True,
                notify_label=notify_label,
            )
            return qty_res
        qty_value = float(qty_res["data"]["qty"])
    cid = client_order_id or _gen_client_order_id("SL", su)
    payload = {
        "algoType": "CONDITIONAL",
        "symbol": su,
        "side": _exit_side_for_position(pos),
        "positionSide": pos,
        "type": STOP_LOSS_ORDER_TYPE,
        "triggerPrice": px,
        "workingType": "CONTRACT_PRICE",
        "clientAlgoId": cid,
    }
    if qty_value is None:
        payload["closePosition"] = "true"
    else:
        payload["quantity"] = qty_value
    res = _call_with_retry(
        lambda: call_futures_signed(
            account,
            source='binance_exec.algo:/fapi/v1/algoOrder',
            method="POST",
            path="/fapi/v1/algoOrder",
            params=payload,
            priority=REQUEST_PRIORITY_CRITICAL,
        ),
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not res["ok"]:
        _emit_trade_event(
            "SL",
            "fail",
            account=account,
            symbol=su,
            position_side=pos,
            side=payload["side"],
            qty=qty_value,
            stop_price=px,
            client_order_id=cid,
            reason=res["reason"],
            attempts=res.get("attempts"),
            is_algo_order=True,
            notify_label=notify_label,
        )
        return _err(res["reason"], payload=payload, attempts=res.get("attempts"))
    raw = res["data"]
    _emit_trade_event(
        "SL",
        "ok",
        account=account,
        symbol=su,
        position_side=pos,
        side=payload["side"],
        qty=qty_value,
        stop_price=px,
        client_order_id=raw.get("clientAlgoId", cid),
        exchange_order_id=raw.get("algoId"),
        order_status=raw.get("algoStatus"),
        attempts=res.get("attempts"),
        is_algo_order=True,
        event_time_ms=_extract_order_event_time_ms(raw),
        notify_label=notify_label,
    )
    return _ok(
        {
            "symbol": su,
            "order_role": "SL",
            "order_type": STOP_LOSS_ORDER_TYPE,
            "side": payload["side"],
            "position_side": pos,
            "qty": qty_value,
            "stop_price": px,
            "client_order_id": raw.get("clientAlgoId", cid),
            "exchange_order_id": raw.get("algoId"),
            "status": raw.get("algoStatus"),
            "payload": payload,
            "raw": raw,
            "is_algo_order": True,
        },
        attempts=res.get("attempts"),
    )
def place_time_stop_order(
    account: str,
    symbol: str,
    position_side: str,
    quantity: float,
    *,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
    client_order_id: str | None = None,
    order_role: str = "TIME_STOP",
    notify_label: str = "snapback",
) -> dict[str, Any]:
    su = (symbol or "").upper().strip()
    pos = _normalize_position_side(position_side)
    role = str(order_role or "TIME_STOP").upper().strip()
    if not role:
        role = "TIME_STOP"
    qty_res = _normalize_quantity(account, su, quantity)
    if not qty_res["ok"]:
        _emit_trade_event(
            role,
            "fail",
            account=account,
            symbol=su,
            position_side=pos,
            client_order_id=client_order_id,
            reason=qty_res["reason"],
            attempts=qty_res.get("attempts"),
            notify_label=notify_label,
        )
        return qty_res
    cid = client_order_id or _gen_client_order_id("TS", su)
    payload = {
        "symbol": su,
        "side": _exit_side_for_position(pos),
        "positionSide": pos,
        "type": TIME_STOP_ORDER_TYPE,
        "quantity": qty_res["data"]["qty"],
        "newClientOrderId": cid,
    }
    res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_create_order',
        'futures_create_order',
        priority=REQUEST_PRIORITY_CRITICAL,
        payload=payload,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not res["ok"]:
        _emit_trade_event(
            role,
            "fail",
            account=account,
            symbol=su,
            position_side=pos,
            side=payload["side"],
            qty=qty_res["data"]["qty"],
            client_order_id=cid,
            reason=res["reason"],
            attempts=res.get("attempts"),
            notify_label=notify_label,
        )
        return _err(res["reason"], payload=payload, attempts=res.get("attempts"))
    raw = res["data"]
    _emit_trade_event(
        role,
        "ok",
        account=account,
        symbol=su,
        position_side=pos,
        side=payload["side"],
        qty=qty_res["data"]["qty"],
        price=float(raw.get("avgPrice", 0.0) or 0.0) or None,
        client_order_id=raw.get("clientOrderId", cid),
        exchange_order_id=raw.get("orderId"),
        order_status=raw.get("status"),
        attempts=res.get("attempts"),
        event_time_ms=_extract_order_event_time_ms(raw),
        notify_label=notify_label,
    )
    return _ok(
        {
            "symbol": su,
            "order_role": role,
            "order_type": TIME_STOP_ORDER_TYPE,
            "side": payload["side"],
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
def get_all_orders(
    account: str,
    symbol: str,
    *,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    limit: int = 1000,
    priority: str = REQUEST_PRIORITY_HIGH,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
) -> dict[str, Any]:
    su = (symbol or "").upper().strip()
    payload: dict[str, Any] = {
        "symbol": su,
        "limit": int(limit),
    }
    if start_time_ms is not None:
        payload["startTime"] = int(start_time_ms)
    if end_time_ms is not None:
        payload["endTime"] = int(end_time_ms)
    res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_get_all_orders',
        'futures_get_all_orders',
        payload=payload,
        priority=priority,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not res["ok"]:
        return _err(res["reason"], payload=payload, attempts=res.get("attempts"))
    rows = [_normalize_order_row(o) for o in (res["data"] or [])]
    return _ok(rows, payload=payload, attempts=res.get("attempts"))


def get_account_trades(
    account: str,
    symbol: str,
    *,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    limit: int = 1000,
    priority: str = REQUEST_PRIORITY_HIGH,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
) -> dict[str, Any]:
    su = (symbol or "").upper().strip()
    payload: dict[str, Any] = {
        "symbol": su,
        "limit": int(limit),
    }
    if start_time_ms is not None:
        payload["startTime"] = int(start_time_ms)
    if end_time_ms is not None:
        payload["endTime"] = int(end_time_ms)
    res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_account_trades',
        'futures_account_trades',
        payload=payload,
        priority=priority,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not res["ok"]:
        return _err(res["reason"], payload=payload, attempts=res.get("attempts"))
    rows = [_normalize_trade_row(o) for o in (res["data"] or [])]
    return _ok(rows, payload=payload, attempts=res.get("attempts"))

def get_income_history(
    account: str,
    symbol: str | None = None,
    *,
    income_type: str | None = None,
    start_time_ms: int | None = None,
    end_time_ms: int | None = None,
    limit: int = 1000,
    priority: str = REQUEST_PRIORITY_HIGH,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "limit": int(limit),
    }
    su = (symbol or "").upper().strip()
    if su:
        payload["symbol"] = su
    income_type_value = str(income_type or "").strip()
    if income_type_value:
        payload["incomeType"] = income_type_value
    if start_time_ms is not None:
        payload["startTime"] = int(start_time_ms)
    if end_time_ms is not None:
        payload["endTime"] = int(end_time_ms)
    res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_income_history',
        'futures_income_history',
        payload=payload,
        priority=priority,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not res["ok"]:
        return _err(res["reason"], payload=payload, attempts=res.get("attempts"))
    rows = [_normalize_income_row(o) for o in (res["data"] or [])]
    return _ok(rows, payload=payload, attempts=res.get("attempts"))


def get_position_snapshots(
    account: str,
    symbol: str | None = None,
    *,
    include_zero: bool = True,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
) -> dict[str, Any]:
    res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_position_information',
        'futures_position_information',
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not res["ok"]:
        return res
    su = (symbol or "").upper().strip()
    rows = []
    for p in res["data"] or []:
        if su and p.get("symbol") != su:
            continue
        pos_side = str(p.get("positionSide", "")).upper()
        if pos_side not in {"LONG", "SHORT"}:
            continue
        amt = float(p.get("positionAmt", 0.0) or 0.0)
        if (not include_zero) and math.isclose(amt, 0.0, abs_tol=1e-12):
            continue
        rows.append(_normalize_position_snapshot_row(p))
    return _ok(rows)




def cancel_order(
    account: str,
    symbol: str,
    *,
    exchange_order_id: int | None = None,
    client_order_id: str | None = None,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
    notify_label: str = "snapback",
) -> dict[str, Any]:
    su = (symbol or "").upper().strip()
    if exchange_order_id is None and not client_order_id:
        reason = "撤单必须提供 exchange_order_id 或 client_order_id"
        _emit_trade_event(
            "CANCEL",
            "fail",
            account=account,
            symbol=su,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            reason=reason,
            notify_label=notify_label,
        )
        return _err(reason)
    payload = {"symbol": su}
    if exchange_order_id is not None:
        payload["orderId"] = int(exchange_order_id)
    if client_order_id:
        payload["origClientOrderId"] = client_order_id
    res = _call_gateway_client_with_retry(
        account,
        'binance_exec.futures_cancel_order',
        'futures_cancel_order',
        priority=REQUEST_PRIORITY_CRITICAL,
        payload=payload,
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if res["ok"]:
        raw = res["data"]
        _emit_trade_event(
            "CANCEL",
            "ok",
            account=account,
            symbol=su,
            client_order_id=raw.get("clientOrderId"),
            exchange_order_id=raw.get("orderId"),
            order_status=raw.get("status"),
            attempts=res.get("attempts"),
            is_algo_order=False,
            event_time_ms=_extract_order_event_time_ms(raw),
            notify_label=notify_label,
        )
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
    if not _is_order_not_found_reason(res["reason"]):
        _emit_trade_event(
            "CANCEL",
            "fail",
            account=account,
            symbol=su,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            reason=res["reason"],
            attempts=res.get("attempts"),
            notify_label=notify_label,
        )
        return _err(res["reason"], payload=payload, attempts=res.get("attempts"))
    algo_res = _call_with_retry(
        lambda: _cancel_algo_order(
            account,
            exchange_order_id=exchange_order_id,
            client_order_id=client_order_id,
        ),
        retry_max=retry_max,
        retry_delay_secs=retry_delay_secs,
    )
    if not algo_res["ok"]:
        _emit_trade_event(
            "CANCEL",
            "fail",
            account=account,
            symbol=su,
            client_order_id=client_order_id,
            exchange_order_id=exchange_order_id,
            reason=algo_res["reason"],
            attempts=algo_res.get("attempts"),
            is_algo_order=True,
            notify_label=notify_label,
        )
        return _err(algo_res["reason"], payload=payload, attempts=algo_res.get("attempts"))
    raw = algo_res["data"]
    _emit_trade_event(
        "CANCEL",
        "ok",
        account=account,
        symbol=su,
        client_order_id=raw.get("clientAlgoId"),
        exchange_order_id=raw.get("algoId"),
        order_status=raw.get("msg") or raw.get("code"),
        attempts=algo_res.get("attempts"),
        is_algo_order=True,
        event_time_ms=_extract_order_event_time_ms(raw),
        notify_label=notify_label,
    )
    return _ok(
        {
            "symbol": su,
            "exchange_order_id": raw.get("algoId"),
            "client_order_id": raw.get("clientAlgoId"),
            "status": raw.get("msg") or raw.get("code"),
            "payload": payload,
            "raw": raw,
            "is_algo_order": True,
        },
        attempts=algo_res.get("attempts"),
    )
def cancel_all_orders(
    account: str,
    symbol: str,
    *,
    retry_max: int = 0,
    retry_delay_secs: float = 1.0,
    notify_label: str = "snapback",
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
            notify_label=notify_label,
        )
        rows.append(cancel_res)
    return _ok(rows)
