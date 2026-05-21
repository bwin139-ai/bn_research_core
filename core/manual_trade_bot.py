from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Coroutine

from telegram import BotCommand, BotCommandScopeChat, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

from core.live.binance_exec import (
    cancel_all_orders,
    cancel_order,
    ensure_cross_margin,
    ensure_leverage,
    get_account_status,
    get_last_price,
    get_order,
    get_order_book_top,
    get_open_orders,
    get_position_mode,
    get_positions,
    place_entry_order,
    place_limit_order,
    place_sl_order,
    place_tp_order,
    place_time_stop_order,
)
from core.live.custom_id import build_client_order_id, make_order_root
from core.runtime_state import load_json_file, save_json_file_atomic, state_path

BJ = timezone(timedelta(hours=8))
LONG = "LONG"
NOTIFY_LABEL = "manual"

OPEN_SELECT_SYMBOL = 101
OPEN_SELECT_TYPE = 102
OPEN_INPUT_PRICE = 103
OPEN_INPUT_NOTIONAL = 104
CLOSE_SELECT_SYMBOL = 201
CLOSE_SELECT_TYPE = 202
CLOSE_INPUT_PRICE = 203
CLOSE_INPUT_QTY = 204
STOP_SELECT_SYMBOL = 301
STOP_INPUT_PRICE = 302
EDIT_SYMBOLS_INPUT = 401

PO_WATCH_TIMEOUT_SECS = 60
PO_WATCH_POLL_SECS = 2
PO_ENTRY_SUBMIT_MAX_ATTEMPTS = 3
_ACTIVE_PO_WATCHERS: set[tuple[str, str]] = set()

def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _permissions_path() -> Path:
    return _repo_root() / "permissions.json"


def _symbols_path() -> Path:
    return state_path("manual_trade_symbols.json")


def _manual_events_path() -> Path:
    day = datetime.now(tz=BJ).strftime("%Y-%m-%d")
    return state_path("manual_trade", "orders", f"{day}.jsonl")


def _manual_events_dir() -> Path:
    return state_path("manual_trade", "orders")


def _bot_token() -> str:
    token = os.getenv("TG_BOT_TOKEN", "").strip()
    if token:
        return token
    secrets_path = _repo_root() / "secrets.json"
    if secrets_path.exists():
        data = load_json_file(secrets_path, default={})
        token = str((data or {}).get("telegram_bot_token") or "").strip()
        if token:
            return token
    raise RuntimeError("TG_BOT_TOKEN or secrets.json.telegram_bot_token is required")


def _load_permissions() -> dict[str, Any]:
    path = _permissions_path()
    if not path.exists():
        raise FileNotFoundError(f"permissions.json missing: {path}")
    data = load_json_file(path, default={})
    admins = data.get("admins")
    if not isinstance(admins, list) or not all(str(x).strip() for x in admins):
        raise ValueError("permissions.json admins must be a non-empty list")
    return data


def _admin_ids() -> set[str]:
    return {str(x).strip() for x in _load_permissions()["admins"]}


def _is_admin(update: Update) -> bool:
    user = update.effective_user
    return bool(user and str(user.id) in _admin_ids())


def _admin_required(
    fn: Callable[[Update, ContextTypes.DEFAULT_TYPE], Coroutine[Any, Any, Any]]
) -> Callable[[Update, ContextTypes.DEFAULT_TYPE], Coroutine[Any, Any, Any]]:
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE) -> Any:
        if _is_admin(update):
            try:
                return await fn(update, context)
            except ValueError as exc:
                if update.callback_query:
                    await update.callback_query.answer(str(exc), show_alert=True)
                elif update.message:
                    await update.message.reply_text(str(exc))
                return ConversationHandler.END
        if update.callback_query:
            await update.callback_query.answer("unauthorized", show_alert=True)
            return ConversationHandler.END
        if update.message:
            await update.message.reply_text("unauthorized")
        return ConversationHandler.END

    return wrapper


def _secrets_dir() -> Path:
    raw = os.getenv("BN_SECRETS_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return _repo_root()


def _discover_accounts() -> list[str]:
    root = _secrets_dir()
    if not root.exists():
        raise FileNotFoundError(f"BN secrets dir missing: {root}")
    accounts = []
    for path in root.glob("secrets_*.json"):
        account = path.name[len("secrets_") : -len(".json")]
        if account:
            accounts.append(account)
    return sorted(set(accounts))


def _selected_account(context: ContextTypes.DEFAULT_TYPE) -> str:
    account = str(context.user_data.get("current_account") or "").strip()
    if not account:
        raise ValueError("请先使用 /set_current_account 选择账户")
    return account


def _load_symbol_rows() -> list[dict[str, Any]]:
    path = _symbols_path()
    if not path.exists():
        return []
    data = load_json_file(path, default=[])
    if not isinstance(data, list):
        raise ValueError(f"manual_trade_symbols.json must be a list: {path}")
    rows = []
    for item in data:
        if not isinstance(item, dict):
            raise ValueError("manual_trade_symbols.json item must be object")
        symbol = str(item.get("symbol") or "").upper().strip()
        leverage = int(item.get("leverage"))
        if not symbol.endswith("USDT") or leverage <= 0:
            raise ValueError(f"invalid manual symbol row: {item}")
        rows.append({"symbol": symbol, "leverage": leverage})
    rows.sort(key=lambda x: x["symbol"])
    return rows


def _save_symbol_rows(rows: list[dict[str, Any]]) -> None:
    clean: dict[str, dict[str, Any]] = {}
    for row in rows:
        symbol = str(row.get("symbol") or "").upper().strip()
        leverage = int(row.get("leverage"))
        if not symbol.endswith("USDT"):
            raise ValueError(f"symbol must end with USDT: {symbol}")
        if leverage <= 0:
            raise ValueError(f"leverage must be positive: {symbol}")
        clean[symbol] = {"symbol": symbol, "leverage": leverage}
    save_json_file_atomic(_symbols_path(), sorted(clean.values(), key=lambda x: x["symbol"]))


def _symbol_row(symbol: str) -> dict[str, Any]:
    su = str(symbol or "").upper().strip()
    for row in _load_symbol_rows():
        if row["symbol"] == su:
            return row
    raise ValueError(f"symbol not configured for manual trading: {su}")


def _fmt_usdt(value: Any) -> str:
    try:
        return f"{float(value):.2f}"
    except Exception:
        return str(value)


def _fmt_float(value: Any, digits: int = 6) -> str:
    try:
        return f"{float(value):.{digits}f}".rstrip("0").rstrip(".")
    except Exception:
        return str(value)


def _fmt_intish(value: Any) -> str:
    try:
        number = float(value)
    except Exception:
        return str(value)
    if abs(number - round(number)) < 1e-9:
        return str(int(round(number)))
    return f"{number:.1f}"


def _order_qty(order: dict[str, Any]) -> float:
    for key in ("orig_qty", "qty", "executed_qty"):
        try:
            value = float(order.get(key, 0.0) or 0.0)
        except Exception:
            continue
        if value > 0:
            return value
    return 0.0


def _order_display_price(order: dict[str, Any]) -> float:
    for key in ("price", "stop_price", "avg_price"):
        try:
            value = float(order.get(key, 0.0) or 0.0)
        except Exception:
            continue
        if value > 0:
            return value
    return 0.0


def _order_icon(order: dict[str, Any]) -> str:
    order_type = str(order.get("type") or order.get("orig_type") or "").upper()
    if order_type == "STOP_MARKET":
        return "🚦"
    if order_type == "TAKE_PROFIT_MARKET":
        return "⏳"
    return ""


def _tp_ratio(entry_price: float, orders: list[dict[str, Any]]) -> str:
    tp_orders = [
        o
        for o in orders
        if str(o.get("side") or "").upper() == "SELL" and _order_display_price(o) > entry_price
    ]
    weighted_qty = sum(_order_qty(o) for o in tp_orders)
    if weighted_qty <= 0 or entry_price <= 0:
        return "⚪"
    weighted_price = sum(_order_display_price(o) * _order_qty(o) for o in tp_orders) / weighted_qty
    ratio = ((weighted_price - entry_price) / entry_price) * 100.0
    if abs(ratio) < 0.05:
        return "🟡"
    return f"{ratio:.1f}🎯"


def _bj_minute(ts_ms: Any) -> str:
    try:
        value = int(ts_ms)
    except Exception:
        return "UNKNOWN"
    if value <= 0:
        return "UNKNOWN"
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%m-%d %H:%M")


def _bj_second(ts_ms: Any) -> str:
    try:
        value = int(ts_ms)
    except Exception:
        return "UNKNOWN"
    if value <= 0:
        return "UNKNOWN"
    return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc).astimezone(BJ).strftime("%Y-%m-%d %H:%M:%S")


def _chunk_lines(lines: list[str], limit: int = 3600) -> list[str]:
    chunks: list[str] = []
    buf: list[str] = []
    size = 0
    for line in lines:
        extra = len(line) + 1
        if buf and size + extra > limit:
            chunks.append("\n".join(buf))
            buf = []
            size = 0
        buf.append(line)
        size += extra
    if buf:
        chunks.append("\n".join(buf))
    return chunks


async def _send_lines(update: Update, lines: list[str]) -> None:
    target = update.message or (update.callback_query.message if update.callback_query else None)
    if not target:
        return
    for chunk in _chunk_lines(lines):
        await target.reply_text(chunk)


def _long_positions(account: str) -> list[dict[str, Any]]:
    res = get_positions(account)
    if not res["ok"]:
        raise RuntimeError(res["reason"])
    return [p for p in res["data"] if p.get("position_side") == LONG]


def _long_orders(account: str) -> list[dict[str, Any]]:
    res = get_open_orders(account)
    if not res["ok"]:
        raise RuntimeError(res["reason"])
    return [o for o in res["data"] if str(o.get("position_side") or "").upper() == LONG]


def _order_root(leg: str) -> str:
    return build_client_order_id(strat="MAN", leg=leg, root=make_order_root())


def _client_order_id(leg: str, root: str) -> str:
    return build_client_order_id(strat="MAN", leg=leg, root=root)


def _append_manual_event(event: str, **payload: Any) -> None:
    record = {
        "event": event,
        "ts_utc_ms": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
        "ts_bj": datetime.now(tz=BJ).isoformat(),
    }
    record.update(payload)
    path = _manual_events_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False, default=str) + "\n")
        f.flush()
        os.fsync(f.fileno())


def _recent_manual_event_paths(limit: int = 3) -> list[Path]:
    root = _manual_events_dir()
    if not root.exists():
        return []
    return sorted(root.glob("*.jsonl"))[-limit:]


def _manual_po_pending_entries() -> list[dict[str, Any]]:
    pending: dict[str, dict[str, Any]] = {}
    for path in _recent_manual_event_paths():
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                event = str(record.get("event") or "")
                account = str(record.get("account") or "")
                symbol = str(record.get("symbol") or "")
                result = record.get("result") if isinstance(record.get("result"), dict) else {}
                data = result.get("data") if isinstance(result.get("data"), dict) else {}
                client_order_id = str(
                    record.get("entry_client_order_id")
                    or data.get("client_order_id")
                    or data.get("clientOrderId")
                    or ""
                )
                key = f"{account}|{symbol}|{client_order_id}"
                if not account or not symbol or not client_order_id:
                    continue
                if event == "manual_trade_po_entry_submit" and record.get("ok") is True:
                    pending[key] = {
                        "account": account,
                        "symbol": symbol,
                        "client_order_id": client_order_id,
                        "ts_bj": record.get("ts_bj"),
                    }
                elif event == "manual_trade_po_watcher_done":
                    pending.pop(key, None)
    return list(pending.values())


def _fail_fast_if_pending_manual_po_entries() -> None:
    pending = _manual_po_pending_entries()
    if not pending:
        return
    summary = ", ".join(
        f"{x['account']} {x['symbol']} {x['client_order_id']} ts={x.get('ts_bj')}"
        for x in pending[:5]
    )
    raise RuntimeError(
        "manual PO watcher state has unfinished entries; check Binance open orders manually before restarting bot: "
        + summary
    )


def _parse_price_match(value: str) -> str | None:
    text = str(value or "").upper().strip()
    if text == "Q":
        return "QUEUE"
    if text == "Q5":
        return "QUEUE_5"
    return None


def _trade_usage() -> str:
    return (
        "Usage:\n"
        "/trade open ACCOUNT SYMBOL LEVERAGE M|PO NOTIONAL SL PRICE TP PRICE\n"
        "/trade close ACCOUNT[ | ACCOUNT...] SYMBOL M|PO [PCT%]\n"
        "/trade close ACCOUNT[ | ACCOUNT...] SYMBOL L PRICE [PCT%]\n"
        "/trade sl ACCOUNT[ | ACCOUNT...] SYMBOL PRICE [PCT%]\n"
        "/trade cancel ACCOUNT[ | ACCOUNT...] SYMBOL\n"
        "Example:\n"
        "/trade open mybwin139 BTCUSDT 10x M 100 SL 80888.8 TP 81999.9\n"
        "/trade open mybwin139 BTCUSDT 10x PO 100 SL 80888.8 TP 81999.9\n"
        "/trade close bwin182 | chen912 | junjie2026 CLUSDT M\n"
        "/trade close bwin182 | chen912 | junjie2026 CLUSDT PO 50%\n"
        "/trade close bwin182 | chen912 | junjie2026 CLUSDT L 101.36 30%\n"
        "/trade sl bwin182 | chen912 | junjie2026 CLUSDT 98.37 50%\n"
        "/trade cancel bwin182 | chen912 | junjie2026 CLUSDT"
    )


def _parse_trade_accounts(tokens: list[str]) -> list[str]:
    raw = " ".join(str(x).strip() for x in tokens if str(x).strip())
    accounts = [x.strip() for x in raw.split("|") if x.strip()]
    if not accounts:
        raise ValueError("missing account")
    known = set(_discover_accounts())
    unknown = [x for x in accounts if x not in known]
    if unknown:
        raise ValueError(f"unknown account(s): {', '.join(unknown)}")
    if len(accounts) != len(set(accounts)):
        raise ValueError("duplicate account in command")
    return accounts


def _parse_percent_suffix(tokens: list[str], *, field_name: str) -> tuple[list[str], float]:
    remaining = [str(x).strip() for x in tokens if str(x).strip()]
    ratio = 1.0
    if remaining and remaining[-1].endswith("%"):
        percent_text = remaining.pop()[:-1].strip()
        percent = float(percent_text)
        if percent <= 0 or percent > 100:
            raise ValueError(f"{field_name} percent must be > 0 and <= 100")
        ratio = percent / 100.0
    return remaining, ratio


def _parse_trade_open_args(args: list[str]) -> dict[str, Any]:
    if len(args) != 10 or str(args[0]).lower() != "open":
        raise ValueError(_trade_usage())
    accounts = _parse_trade_accounts([str(args[1]).strip()])
    if len(accounts) != 1:
        raise ValueError("open command requires exactly one account")
    account = accounts[0]
    symbol = str(args[2]).upper().strip()
    if not symbol.endswith("USDT"):
        raise ValueError(f"symbol must end with USDT: {symbol}")
    leverage_text = str(args[3]).lower().strip()
    if not leverage_text.endswith("x"):
        raise ValueError("leverage must look like 10x")
    leverage = int(leverage_text[:-1])
    if leverage <= 0:
        raise ValueError("leverage must be positive")
    mode = str(args[4]).upper().strip()
    if mode not in {"M", "PO"}:
        raise ValueError("mode must be M or PO")
    notional = float(args[5])
    if notional <= 0:
        raise ValueError("notional must be positive")
    if str(args[6]).upper().strip() != "SL":
        raise ValueError("missing SL field")
    sl_price = float(args[7])
    if sl_price < 0:
        raise ValueError("SL price must be >= 0")
    if str(args[8]).upper().strip() != "TP":
        raise ValueError("missing TP field")
    tp_price = float(args[9])
    if tp_price < 0:
        raise ValueError("TP price must be >= 0")
    return {
        "account": account,
        "symbol": symbol,
        "leverage": leverage,
        "mode": mode,
        "notional": notional,
        "sl_price": sl_price,
        "tp_price": tp_price,
    }


def _parse_trade_close_args(args: list[str]) -> dict[str, Any]:
    if len(args) < 4 or str(args[0]).lower() != "close":
        raise ValueError(_trade_usage())
    tokens, close_ratio = _parse_percent_suffix([str(x) for x in args[1:]], field_name="close")
    if len(tokens) < 3:
        raise ValueError(_trade_usage())
    limit_price: float | None = None
    tail_mode = str(tokens[-1]).upper().strip()
    if tail_mode in {"M", "PO"}:
        mode = tail_mode
        symbol = str(tokens[-2]).upper().strip()
        account_tokens = tokens[:-2]
    else:
        if len(tokens) < 4:
            raise ValueError(_trade_usage())
        mode = str(tokens[-2]).upper().strip()
        if mode != "L":
            raise ValueError("close mode must be M, PO, or L")
        limit_price = float(tokens[-1])
        if limit_price <= 0:
            raise ValueError("limit close price must be positive")
        symbol = str(tokens[-3]).upper().strip()
        account_tokens = tokens[:-3]
    if not symbol.endswith("USDT"):
        raise ValueError(f"symbol must end with USDT: {symbol}")
    accounts = _parse_trade_accounts(account_tokens)
    return {
        "accounts": accounts,
        "symbol": symbol,
        "mode": mode,
        "limit_price": limit_price,
        "close_ratio": close_ratio,
    }


def _parse_trade_sl_args(args: list[str]) -> dict[str, Any]:
    if len(args) < 4 or str(args[0]).lower() != "sl":
        raise ValueError(_trade_usage())
    tokens, sl_ratio = _parse_percent_suffix([str(x) for x in args[1:]], field_name="sl")
    if len(tokens) < 3:
        raise ValueError(_trade_usage())
    stop_price = float(tokens[-1])
    if stop_price <= 0:
        raise ValueError("SL price must be positive")
    symbol = str(tokens[-2]).upper().strip()
    if not symbol.endswith("USDT"):
        raise ValueError(f"symbol must end with USDT: {symbol}")
    accounts = _parse_trade_accounts(tokens[:-2])
    return {
        "accounts": accounts,
        "symbol": symbol,
        "stop_price": stop_price,
        "sl_ratio": sl_ratio,
    }


def _parse_trade_cancel_args(args: list[str]) -> dict[str, Any]:
    if len(args) < 3 or str(args[0]).lower() != "cancel":
        raise ValueError(_trade_usage())
    symbol = str(args[-1]).upper().strip()
    if not symbol.endswith("USDT"):
        raise ValueError(f"symbol must end with USDT: {symbol}")
    accounts = _parse_trade_accounts([str(x) for x in args[1:-1]])
    return {
        "accounts": accounts,
        "symbol": symbol,
    }


def _entry_qty_from_notional(account: str, symbol: str, notional: float, price: float) -> float:
    if price <= 0:
        raise ValueError("entry price must be positive")
    return float(notional) / float(price)


def _validate_long_protection_prices(reference_price: float, sl_price: float, tp_price: float) -> None:
    if sl_price > 0 and sl_price >= reference_price:
        raise ValueError(f"LONG SL must be below entry reference price: sl={sl_price} ref={reference_price}")
    if tp_price > 0 and tp_price <= reference_price:
        raise ValueError(f"LONG TP must be above entry reference price: tp={tp_price} ref={reference_price}")


def _is_po_maker_reject(reason: str) -> bool:
    text = str(reason or "")
    return (
        "code=-5022" in text
        or "could not be executed as maker" in text
        or "Post Only order will be rejected" in text
    )


async def _protect_manual_entry(
    bot: Any,
    chat_id: int,
    *,
    account: str,
    symbol: str,
    quantity: float,
    sl_price: float,
    tp_price: float,
    root: str,
) -> list[str]:
    messages: list[str] = []
    if quantity <= 0:
        raise ValueError("executed quantity must be positive before SL/TP")
    if sl_price > 0:
        sl_res = place_sl_order(
            account,
            symbol,
            LONG,
            sl_price,
            client_order_id=_client_order_id("SL", root),
            notify_label=NOTIFY_LABEL,
        )
        _append_manual_event("manual_trade_sl_submit", account=account, symbol=symbol, ok=sl_res["ok"], result=sl_res)
        if sl_res["ok"]:
            messages.append(f"SL ok stop={_fmt_float(sl_res['data'].get('stop_price'))}")
        else:
            messages.append(f"SL failed reason={sl_res['reason']}")
    else:
        messages.append("SL skipped")
    if tp_price > 0:
        tp_res = place_tp_order(
            account,
            symbol,
            LONG,
            quantity,
            tp_price,
            client_order_id=_client_order_id("TP", root),
            notify_label=NOTIFY_LABEL,
        )
        _append_manual_event("manual_trade_tp_submit", account=account, symbol=symbol, ok=tp_res["ok"], result=tp_res)
        if tp_res["ok"]:
            messages.append(f"TP ok price={_fmt_float(tp_res['data'].get('price'))}")
        else:
            messages.append(f"TP failed reason={tp_res['reason']}")
    else:
        messages.append("TP skipped")
    await bot.send_message(chat_id=chat_id, text="\n".join([f"Manual protection {account} {symbol}", *messages]))
    return messages


async def _watch_po_entry(
    bot: Any,
    chat_id: int,
    *,
    account: str,
    symbol: str,
    entry_order_id: int | None,
    entry_client_order_id: str,
    root: str,
    sl_price: float,
    tp_price: float,
    timeout_secs: int = PO_WATCH_TIMEOUT_SECS,
) -> None:
    key = (account, symbol)
    async def finish(outcome: str, **payload: Any) -> None:
        _append_manual_event(
            "manual_trade_po_watcher_done",
            account=account,
            symbol=symbol,
            entry_client_order_id=entry_client_order_id,
            outcome=outcome,
            **payload,
        )

    try:
        deadline = asyncio.get_running_loop().time() + float(timeout_secs)
        last_order: dict[str, Any] | None = None
        while asyncio.get_running_loop().time() < deadline:
            await asyncio.sleep(PO_WATCH_POLL_SECS)
            order_res = get_order(
                account,
                symbol,
                exchange_order_id=entry_order_id,
                client_order_id=entry_client_order_id,
            )
            _append_manual_event("manual_trade_po_poll", account=account, symbol=symbol, ok=order_res["ok"], result=order_res)
            if not order_res["ok"]:
                continue
            last_order = order_res["data"]
            status = str(last_order.get("status") or "").upper()
            executed_qty = float(last_order.get("executed_qty", 0.0) or 0.0)
            if status == "FILLED" and executed_qty > 0:
                await _protect_manual_entry(
                    bot,
                    chat_id,
                    account=account,
                    symbol=symbol,
                    quantity=executed_qty,
                    sl_price=sl_price,
                    tp_price=tp_price,
                    root=root,
                )
                await bot.send_message(chat_id=chat_id, text=f"PO entry filled {account} {symbol} qty={_fmt_float(executed_qty)}")
                await finish("filled", status=status, executed_qty=executed_qty)
                return
            if status in {"CANCELED", "EXPIRED", "REJECTED"}:
                if executed_qty > 0:
                    await _protect_manual_entry(
                        bot,
                        chat_id,
                        account=account,
                        symbol=symbol,
                        quantity=executed_qty,
                        sl_price=sl_price,
                        tp_price=tp_price,
                        root=root,
                    )
                    await bot.send_message(chat_id=chat_id, text=f"PO entry terminal with fill {account} {symbol} status={status} qty={_fmt_float(executed_qty)}")
                    await finish("terminal_with_fill", status=status, executed_qty=executed_qty)
                else:
                    await bot.send_message(chat_id=chat_id, text=f"PO entry terminal no fill {account} {symbol} status={status}")
                    await finish("terminal_no_fill", status=status, executed_qty=executed_qty)
                return

        executed_qty = float((last_order or {}).get("executed_qty", 0.0) or 0.0)
        status = str((last_order or {}).get("status") or "").upper()
        if status in {"NEW", "PARTIALLY_FILLED", ""}:
            cancel_res = cancel_order(
                account,
                symbol,
                exchange_order_id=entry_order_id,
                client_order_id=entry_client_order_id,
                notify_label=NOTIFY_LABEL,
            )
            _append_manual_event("manual_trade_po_timeout_cancel", account=account, symbol=symbol, ok=cancel_res["ok"], result=cancel_res)
            final_res = get_order(
                account,
                symbol,
                exchange_order_id=entry_order_id,
                client_order_id=entry_client_order_id,
            )
            if final_res["ok"]:
                executed_qty = max(executed_qty, float(final_res["data"].get("executed_qty", 0.0) or 0.0))
        if executed_qty > 0:
            await _protect_manual_entry(
                bot,
                chat_id,
                account=account,
                symbol=symbol,
                quantity=executed_qty,
                sl_price=sl_price,
                tp_price=tp_price,
                root=root,
            )
            await bot.send_message(chat_id=chat_id, text=f"PO timeout, protected partial fill {account} {symbol} qty={_fmt_float(executed_qty)}")
            await finish("timeout_partial_fill", status=status, executed_qty=executed_qty)
        else:
            await bot.send_message(chat_id=chat_id, text=f"PO timeout canceled no fill {account} {symbol}")
            await finish("timeout_no_fill", status=status, executed_qty=executed_qty)
    except Exception as exc:
        logging.error("[manual_po_watcher] failed: %s", exc, exc_info=True)
        _append_manual_event(
            "manual_trade_po_watcher_error",
            account=account,
            symbol=symbol,
            entry_client_order_id=entry_client_order_id,
            reason=str(exc),
        )
        await bot.send_message(chat_id=chat_id, text=f"PO watcher error {account} {symbol}: {exc}")
    finally:
        _ACTIVE_PO_WATCHERS.discard(key)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    tb = "".join(traceback.format_exception(None, context.error, context.error.__traceback__))
    logging.error("[manual_bot] unhandled error: %s\n%s", context.error, tb)


async def post_init(application: Application) -> None:
    await application.bot.set_chat_menu_button()
    await application.bot.set_my_commands(
        [
            BotCommand("start", "Start"),
            BotCommand("help", "Help"),
        ]
    )


@_admin_required
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat_id = update.effective_chat.id
    commands = [
        BotCommand("set_current_account", "Select"),
        BotCommand("open", "Open"),
        BotCommand("close", "Close"),
        BotCommand("status", "All Accounts"),
        BotCommand("account_detail", "Account Detail"),
        BotCommand("pending_orders", "Pending Orders"),
        BotCommand("view_history", "History"),
        BotCommand("trade", "Command Trade"),
        BotCommand("stop_market", "Stop Market"),
        BotCommand("edit_symbols", "Edit Symbols"),
    ]
    await context.bot.set_my_commands(commands=commands, scope=BotCommandScopeChat(chat_id=chat_id))
    await update.message.reply_text("menu updated")


@_admin_required
async def set_current_account(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    accounts = _discover_accounts()
    if not accounts:
        await update.message.reply_text("no secrets_*.json accounts found")
        return
    buttons = [[InlineKeyboardButton(acc, callback_data=f"acct:{acc}")] for acc in accounts]
    await update.message.reply_text("Select account", reply_markup=InlineKeyboardMarkup(buttons))


@_admin_required
async def select_account(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    account = query.data.split(":", 1)[1]
    if account not in _discover_accounts():
        await query.edit_message_text(f"account not found: {account}")
        return
    context.user_data["current_account"] = account
    await query.edit_message_text(f"current account: {account}")


@_admin_required
async def account_status(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    accounts = _discover_accounts()
    lines = ["📊 活跃账户列表"]
    account_rows: list[tuple[str, str, str, str, str]] = []
    for account in accounts:
        res = get_account_status(account)
        if not res["ok"]:
            account_rows.append((account, "查询异常", "", "", ""))
            continue
        data = res["data"]
        account_rows.append(
            (
                account,
                _fmt_intish(data["wallet_usdt"]),
                _fmt_intish(data["margin_usdt"]),
                _fmt_intish(data["available_usdt"]),
                _fmt_intish(data["unrealized_usdt"]),
            )
        )
    for i in range(0, len(account_rows), 2):
        left = account_rows[i]
        right = account_rows[i + 1] if i + 1 < len(account_rows) else None
        if right:
            lines.append(
                f"🔹 {left[0]:<12} 🔹 {right[0]:<12}\n"
                f"W: {left[1]:>10}    W: {right[1]:>10}\n"
                f"B: {left[2]:>10}    B: {right[2]:>10}\n"
                f"A: {left[3]:>10}    A: {right[3]:>10}\n"
                f"U: {left[4]:>10}    U: {right[4]:>10}"
            )
        else:
            lines.append(
                f"🔹 {left[0]:<12}\n"
                f"W: {left[1]:>10}\n"
                f"B: {left[2]:>10}\n"
                f"A: {left[3]:>10}\n"
                f"U: {left[4]:>10}"
            )
    await _send_lines(update, lines)


@_admin_required
async def account_detail(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    account = _selected_account(context)
    status = get_account_status(account)
    if not status["ok"]:
        await update.message.reply_text(status["reason"])
        return
    lines: list[str] = [""]
    data = status["data"]
    lines.append(
        f"🔷{account}  M: {_fmt_intish(data['margin_usdt'])}  "
        f"uPnL: {_fmt_intish(data['unrealized_usdt'])}"
    )
    positions = _long_positions(account)
    orders = _long_orders(account)
    if positions:
        net_amount = sum(int(float(p["qty"]) * float(p["entry_price"])) for p in positions)
        lines.append(f"      W: {_fmt_intish(data['wallet_usdt'])}      净持仓: {_fmt_intish(net_amount)}")
        lines.append("")
        for p in sorted(positions, key=lambda x: float(x["qty"]) * float(x["entry_price"]), reverse=True):
            amount = int(float(p["qty"]) * float(p["entry_price"]))
            symbol_orders = [o for o in orders if o.get("symbol") == p["symbol"]]
            lines.append(
                f"🟢{p['symbol']}    uPnL: {_fmt_intish(p['unrealized_usdt'])}\n"
                f"     {_fmt_float(p['qty'])} | {_fmt_float(p['entry_price'])} "
                f"| {amount} | {_tp_ratio(float(p['entry_price']), symbol_orders)}"
            )
    else:
        lines.append("ℹ️ 无持仓信息")
    keyboard = [
        [
            InlineKeyboardButton("📄 Pending", callback_data="detail_pending"),
            InlineKeyboardButton("📜 History", callback_data="detail_history"),
        ]
    ]
    await update.message.reply_text("\n".join(lines), reply_markup=InlineKeyboardMarkup(keyboard))


@_admin_required
async def pending_orders(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    account = _selected_account(context)
    query = update.callback_query
    if query:
        await query.answer()
    orders = _long_orders(account)
    if not orders:
        text = "📄 当前挂单\n\n✅ 当前无挂单"
        if query:
            await query.edit_message_text(text)
        else:
            await update.message.reply_text(text)
        return
    lines = [f"📄 当前挂单 {account}"]
    buttons = []
    order_groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for order in sorted(orders, key=lambda x: (str(x.get("symbol")), str(x.get("side")), _order_display_price(x)), reverse=False):
        key = (str(order.get("symbol")), str(order.get("side")))
        order_groups.setdefault(key, []).append(order)
    for (symbol, side), group in order_groups.items():
        buttons.append([InlineKeyboardButton(f"📌 {symbol} {side} 撤单 ↓↓↓", callback_data=f"cancel_group:{symbol}")])
        row_buttons = []
        for order in sorted(group, key=_order_display_price, reverse=True):
            oid = order.get("order_id")
            price = _order_display_price(order)
            qty = _order_qty(order)
            lines.append(
                f"{symbol} {side} {order.get('type')} "
                f"{_order_icon(order)}{_fmt_float(price)}({_fmt_float(qty)}) oid={oid}"
            )
            if oid is not None:
                row_buttons.append(
                    InlineKeyboardButton(
                        f"{_order_icon(order)}{_fmt_float(price)}({_fmt_float(qty)})",
                        callback_data=f"cancel:{symbol}:{oid}",
                    )
                )
            if len(row_buttons) == 2:
                buttons.append(row_buttons)
                row_buttons = []
        if row_buttons:
            buttons.append(row_buttons)
    text = "\n".join(lines[:60])
    markup = InlineKeyboardMarkup(buttons[:40])
    if query:
        await query.edit_message_text(text, reply_markup=markup)
    else:
        await update.message.reply_text(text, reply_markup=markup)


@_admin_required
async def detail_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await send_history(update, context)
    return ConversationHandler.END


@_admin_required
async def confirm_cancel_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, symbol = query.data.split(":", 1)
    await query.edit_message_text(
        f"Confirm cancel all LONG orders for {symbol}?",
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Confirm", callback_data=f"cancel_group_ok:{symbol}")],
                [InlineKeyboardButton("Abort", callback_data="abort")],
            ]
        ),
    )


@_admin_required
async def do_cancel_group(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    account = _selected_account(context)
    _, symbol = query.data.split(":", 1)
    res = cancel_all_orders(account, symbol, notify_label=NOTIFY_LABEL)
    if not res["ok"]:
        await query.edit_message_text(f"cancel orders failed: {res['reason']}")
        return
    await query.edit_message_text(f"cancel orders submitted: {symbol}")


@_admin_required
async def confirm_cancel_order(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    _, symbol, oid = query.data.split(":", 2)
    await query.edit_message_text(
        f"Confirm cancel {symbol} {oid}?",
        reply_markup=InlineKeyboardMarkup(
            [
                [InlineKeyboardButton("Confirm", callback_data=f"cancel_ok:{symbol}:{oid}")],
                [InlineKeyboardButton("Abort", callback_data="abort")],
            ]
        ),
    )


@_admin_required
async def do_cancel_order(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    account = _selected_account(context)
    _, symbol, oid = query.data.split(":", 2)
    res = cancel_order(account, symbol, exchange_order_id=int(oid), notify_label=NOTIFY_LABEL)
    if not res["ok"]:
        await query.edit_message_text(f"cancel failed: {res['reason']}")
        return
    await query.edit_message_text(f"cancel submitted: {symbol} {oid}")


@_admin_required
async def abort(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("aborted")
    return ConversationHandler.END


@_admin_required
async def open_position(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    _selected_account(context)
    rows = _load_symbol_rows()
    if not rows:
        await update.message.reply_text("manual_trade_symbols.json is empty. Use /edit_symbols first.")
        return ConversationHandler.END
    buttons = [
        [InlineKeyboardButton(f"{row['symbol']} {row['leverage']}x", callback_data=f"open_symbol:{row['symbol']}")]
        for row in rows
    ]
    buttons.append([InlineKeyboardButton("Abort", callback_data="abort")])
    await update.message.reply_text("Select LONG symbol", reply_markup=InlineKeyboardMarkup(buttons))
    return OPEN_SELECT_SYMBOL


@_admin_required
async def open_symbol_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    symbol = query.data.split(":", 1)[1]
    row = _symbol_row(symbol)
    context.user_data["open_symbol"] = row["symbol"]
    context.user_data["open_leverage"] = row["leverage"]
    buttons = [
        [InlineKeyboardButton("Market LONG", callback_data="open_type:MARKET")],
        [InlineKeyboardButton("Limit LONG", callback_data="open_type:LIMIT")],
        [InlineKeyboardButton("Abort", callback_data="abort")],
    ]
    await query.edit_message_text(f"{row['symbol']} {row['leverage']}x", reply_markup=InlineKeyboardMarkup(buttons))
    return OPEN_SELECT_TYPE


@_admin_required
async def open_type_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    order_type = query.data.split(":", 1)[1]
    context.user_data["open_type"] = order_type
    if order_type == "LIMIT":
        await query.edit_message_text("Input limit price (Q=QUEUE, Q5=QUEUE_5)")
        return OPEN_INPUT_PRICE
    await query.edit_message_text("Input notional USDT")
    return OPEN_INPUT_NOTIONAL


@_admin_required
async def open_input_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw_price = update.message.text.strip()
    price_match = _parse_price_match(raw_price)
    if price_match:
        context.user_data["open_price"] = None
        context.user_data["open_price_match"] = price_match
    else:
        context.user_data["open_price"] = float(raw_price)
        context.user_data.pop("open_price_match", None)
    await update.message.reply_text("Input notional USDT")
    return OPEN_INPUT_NOTIONAL


def _prepare_symbol(account: str, symbol: str, leverage: int) -> None:
    mode_res = get_position_mode(account)
    if not mode_res["ok"]:
        raise RuntimeError(mode_res["reason"])
    if not bool(mode_res["data"].get("dual_side")):
        mode = mode_res["data"].get("position_side_mode")
        raise RuntimeError(f"account position mode must be HEDGE before manual LONG trade: mode={mode}")
    for res in (
        ensure_cross_margin(account, symbol),
        ensure_leverage(account, symbol, leverage),
    ):
        if not res["ok"]:
            raise RuntimeError(res["reason"])


@_admin_required
async def open_input_notional(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    account = _selected_account(context)
    symbol = str(context.user_data["open_symbol"])
    leverage = int(context.user_data["open_leverage"])
    order_type = str(context.user_data["open_type"])
    price_match = context.user_data.get("open_price_match")
    notional = float(update.message.text.strip())
    if notional <= 0:
        await update.message.reply_text("notional must be positive")
        return OPEN_INPUT_NOTIONAL
    price = float(context.user_data.get("open_price") or 0.0)
    if order_type == "MARKET" or price_match:
        price_res = get_last_price(account, symbol)
        if not price_res["ok"]:
            await update.message.reply_text(price_res["reason"])
            return ConversationHandler.END
        price = float(price_res["data"]["price"])
    quantity = notional / price
    try:
        _prepare_symbol(account, symbol, leverage)
        if order_type == "MARKET":
            res = place_entry_order(
                account,
                symbol,
                LONG,
                quantity,
                client_order_id=_order_root("ENT"),
                notify_label=NOTIFY_LABEL,
            )
        else:
            res = place_limit_order(
                account,
                symbol,
                LONG,
                "BUY",
                quantity,
                price,
                order_role="MANUAL_ENTRY",
                time_in_force="GTC",
                price_match=str(price_match) if price_match else None,
                client_order_id=_order_root("ENT"),
                notify_label=NOTIFY_LABEL,
            )
    except Exception as exc:
        await update.message.reply_text(f"open failed: {exc}")
        return ConversationHandler.END
    if not res["ok"]:
        await update.message.reply_text(f"open failed: {res['reason']}")
        return ConversationHandler.END
    data = res["data"]
    await update.message.reply_text(
        f"LONG open submitted {symbol}\nqty={_fmt_float(data.get('qty') or data.get('orig_qty'))}\n"
        f"price={data.get('price_match') or _fmt_float(data.get('price') or data.get('avg_price') or price)}\n"
        f"oid={data.get('exchange_order_id') or data.get('order_id')}\ncid={data.get('client_order_id')}"
    )
    return ConversationHandler.END


@_admin_required
async def close_position(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    account = _selected_account(context)
    positions = _long_positions(account)
    if not positions:
        await update.message.reply_text(f"{account}: no LONG positions")
        return ConversationHandler.END
    buttons = [
        [InlineKeyboardButton(f"{p['symbol']} qty={_fmt_float(p['qty'])}", callback_data=f"close_symbol:{p['symbol']}")]
        for p in positions
    ]
    buttons.append([InlineKeyboardButton("Abort", callback_data="abort")])
    await update.message.reply_text("Select LONG position", reply_markup=InlineKeyboardMarkup(buttons))
    return CLOSE_SELECT_SYMBOL


@_admin_required
async def close_symbol_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    symbol = query.data.split(":", 1)[1]
    context.user_data["close_symbol"] = symbol
    buttons = [
        [InlineKeyboardButton("Market close all", callback_data="close_type:MARKET")],
        [InlineKeyboardButton("Limit close", callback_data="close_type:LIMIT")],
        [InlineKeyboardButton("Cancel all orders", callback_data="close_type:CANCEL_ORDERS")],
        [InlineKeyboardButton("Abort", callback_data="abort")],
    ]
    await query.edit_message_text(f"Close {symbol}", reply_markup=InlineKeyboardMarkup(buttons))
    return CLOSE_SELECT_TYPE


@_admin_required
async def close_type_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    account = _selected_account(context)
    symbol = str(context.user_data["close_symbol"])
    close_type = query.data.split(":", 1)[1]
    if close_type == "CANCEL_ORDERS":
        res = cancel_all_orders(account, symbol, notify_label=NOTIFY_LABEL)
        if not res["ok"]:
            await query.edit_message_text(f"cancel orders failed: {res['reason']}")
            return ConversationHandler.END
        await query.edit_message_text(f"cancel orders submitted: {symbol}")
        return ConversationHandler.END
    if close_type == "MARKET":
        pos_res = get_positions(account, symbol)
        if not pos_res["ok"]:
            await query.edit_message_text(pos_res["reason"])
            return ConversationHandler.END
        pos = next((p for p in pos_res["data"] if p.get("position_side") == LONG), None)
        if not pos:
            await query.edit_message_text(f"no LONG position: {symbol}")
            return ConversationHandler.END
        res = place_time_stop_order(
            account,
            symbol,
            LONG,
            float(pos["qty"]),
            order_role="MANUAL_CLOSE",
            client_order_id=_order_root("CLS"),
            notify_label=NOTIFY_LABEL,
        )
        if not res["ok"]:
            await query.edit_message_text(f"market close failed: {res['reason']}")
            return ConversationHandler.END
        await query.edit_message_text(f"market close submitted: {symbol}")
        return ConversationHandler.END
    await query.edit_message_text("Input limit close price (Q=QUEUE, Q5=QUEUE_5)")
    return CLOSE_INPUT_PRICE


@_admin_required
async def close_input_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    raw_price = update.message.text.strip()
    price_match = _parse_price_match(raw_price)
    if price_match:
        context.user_data["close_price"] = None
        context.user_data["close_price_match"] = price_match
    else:
        context.user_data["close_price"] = float(raw_price)
        context.user_data.pop("close_price_match", None)
    await update.message.reply_text("Input close qty, or ALL")
    return CLOSE_INPUT_QTY


@_admin_required
async def close_input_qty(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    account = _selected_account(context)
    symbol = str(context.user_data["close_symbol"])
    price_match = context.user_data.get("close_price_match")
    price = None if price_match else float(context.user_data["close_price"])
    pos_res = get_positions(account, symbol)
    if not pos_res["ok"]:
        await update.message.reply_text(pos_res["reason"])
        return ConversationHandler.END
    pos = next((p for p in pos_res["data"] if p.get("position_side") == LONG), None)
    if not pos:
        await update.message.reply_text(f"no LONG position: {symbol}")
        return ConversationHandler.END
    raw_qty = update.message.text.strip().upper()
    qty = float(pos["qty"]) if raw_qty == "ALL" else float(raw_qty)
    if qty <= 0 or qty > float(pos["qty"]):
        await update.message.reply_text("invalid qty")
        return CLOSE_INPUT_QTY
    res = place_limit_order(
        account,
        symbol,
        LONG,
        "SELL",
        qty,
        price,
        order_role="MANUAL_CLOSE",
        time_in_force="GTC",
        price_match=str(price_match) if price_match else None,
        client_order_id=_order_root("CLS"),
        notify_label=NOTIFY_LABEL,
    )
    if not res["ok"]:
        await update.message.reply_text(f"limit close failed: {res['reason']}")
        return ConversationHandler.END
    data = res["data"]
    await update.message.reply_text(
        f"limit close submitted {symbol}\nqty={_fmt_float(data.get('qty') or data.get('orig_qty'))}\n"
        f"price={data.get('price_match') or _fmt_float(data.get('price'))}\n"
        f"oid={data.get('exchange_order_id') or data.get('order_id')}\ncid={data.get('client_order_id')}"
    )
    return ConversationHandler.END


@_admin_required
async def stop_market(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    account = _selected_account(context)
    positions = _long_positions(account)
    if not positions:
        await update.message.reply_text(f"{account}: no LONG positions")
        return ConversationHandler.END
    buttons = [
        [InlineKeyboardButton(f"{p['symbol']} qty={_fmt_float(p['qty'])}", callback_data=f"stop_symbol:{p['symbol']}")]
        for p in positions
    ]
    buttons.append([InlineKeyboardButton("Abort", callback_data="abort")])
    await update.message.reply_text("Select LONG position", reply_markup=InlineKeyboardMarkup(buttons))
    return STOP_SELECT_SYMBOL


@_admin_required
async def stop_symbol_selected(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    context.user_data["stop_symbol"] = query.data.split(":", 1)[1]
    await query.edit_message_text("Input stop trigger price")
    return STOP_INPUT_PRICE


@_admin_required
async def stop_input_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    account = _selected_account(context)
    symbol = str(context.user_data["stop_symbol"])
    stop_price = float(update.message.text.strip())
    res = place_sl_order(
        account,
        symbol,
        LONG,
        stop_price,
        client_order_id=_order_root("STP"),
        notify_label=NOTIFY_LABEL,
    )
    if not res["ok"]:
        await update.message.reply_text(f"stop failed: {res['reason']}")
        return ConversationHandler.END
    data = res["data"]
    await update.message.reply_text(
        f"stop market submitted {symbol}\ntrigger={_fmt_float(data.get('stop_price'))}\n"
        f"oid={data.get('exchange_order_id')}\ncid={data.get('client_order_id')}"
    )
    return ConversationHandler.END


@_admin_required
async def edit_symbols(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    rows = _load_symbol_rows()
    lines = ["Manual Symbols"]
    lines.extend(f"{row['symbol']} {row['leverage']}x" for row in rows)
    lines.append("")
    lines.append("Send: ADD BTCUSDT 20, DEL BTCUSDT, LIST, or DONE")
    await _send_lines(update, lines)
    return EDIT_SYMBOLS_INPUT


@_admin_required
async def edit_symbols_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text.strip().upper()
    rows = _load_symbol_rows()
    if text == "DONE":
        await update.message.reply_text("done")
        return ConversationHandler.END
    if text == "LIST":
        lines = ["Manual Symbols"]
        lines.extend(f"{row['symbol']} {row['leverage']}x" for row in rows)
        await _send_lines(update, lines)
        return EDIT_SYMBOLS_INPUT
    parts = text.split()
    try:
        if len(parts) == 3 and parts[0] == "ADD":
            rows.append({"symbol": parts[1], "leverage": int(parts[2])})
            _save_symbol_rows(rows)
            await update.message.reply_text(f"added {parts[1]}")
            return EDIT_SYMBOLS_INPUT
        if len(parts) == 2 and parts[0] == "DEL":
            keep = [row for row in rows if row["symbol"] != parts[1]]
            if len(keep) == len(rows):
                await update.message.reply_text(f"not found {parts[1]}")
                return EDIT_SYMBOLS_INPUT
            _save_symbol_rows(keep)
            await update.message.reply_text(f"deleted {parts[1]}")
            return EDIT_SYMBOLS_INPUT
    except Exception as exc:
        await update.message.reply_text(f"edit failed: {exc}")
        return EDIT_SYMBOLS_INPUT
    await update.message.reply_text("invalid input")
    return EDIT_SYMBOLS_INPUT


def _history_days(start: datetime, end: datetime) -> list[str]:
    start_day = start.astimezone(BJ).date()
    end_day = end.astimezone(BJ).date()
    days: list[str] = []
    cur = start_day
    while cur <= end_day:
        days.append(cur.isoformat())
        cur = cur + timedelta(days=1)
    return days


def _exchange_history_root(account: str) -> Path:
    account_key = str(account or "").strip()
    if not account_key:
        raise ValueError("account is required")
    return state_path("exchange_history", account_key, ".keep").parent


def _int_ms(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _read_exchange_history_records(account: str, source: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
    if start_ms > end_ms:
        return []
    start = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc)
    end = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc)
    source_dir = _exchange_history_root(account) / source
    records: list[dict[str, Any]] = []
    for day in _history_days(start, end):
        path = source_dir / f"{day}.jsonl"
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                try:
                    record = json.loads(line)
                except Exception:
                    continue
                if not isinstance(record, dict):
                    continue
                if str(record.get("account") or "") != account:
                    continue
                event_ms = _int_ms(record.get("event_time_ms"))
                if start_ms <= event_ms <= end_ms:
                    records.append(record)
    return records


def _exchange_history_rows(account: str, source: str, start_ms: int, end_ms: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in _read_exchange_history_records(account, source, start_ms, end_ms):
        raw = record.get("raw")
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        row["_history_event_time_ms"] = record.get("event_time_ms")
        row["_history_sync_time_ms"] = record.get("sync_time_ms")
        rows.append(row)
    return rows


def _latest_exchange_history_sync_ms(account: str) -> int:
    latest = 0
    root = _exchange_history_root(account)
    sync_state_path = root / "sync_state.json"
    if sync_state_path.exists():
        try:
            state = load_json_file(sync_state_path, default={})
        except Exception:
            state = {}
        sources = state.get("sources", {}) if isinstance(state, dict) else {}
        if isinstance(sources, dict):
            for source_map in sources.values():
                if not isinstance(source_map, dict):
                    continue
                for row in source_map.values():
                    if isinstance(row, dict) and row.get("ok"):
                        latest = max(latest, _int_ms(row.get("last_sync_time_ms")))
    return latest


def _latest_loaded_rows_sync_ms(*rowsets: list[dict[str, Any]]) -> int:
    latest = 0
    for rows in rowsets:
        for row in rows:
            latest = max(latest, _int_ms(row.get("_history_sync_time_ms")))
    return latest


def _history_display_ms(row: dict[str, Any], *keys: str) -> int:
    for key in keys:
        value = _int_ms(row.get(key))
        if value > 0:
            return value
    return _int_ms(row.get("_history_event_time_ms"))


def _position_base_asset(symbol: str) -> str:
    if symbol.endswith("USDT"):
        return symbol[:-4]
    return symbol


def _fmt_history_duration(open_ms: Any, close_ms: Any) -> str:
    start = _int_ms(open_ms)
    end = _int_ms(close_ms)
    if start <= 0 or end <= 0:
        return "UNKNOWN"
    minutes = max(0, (end - start) // 60000)
    if minutes < 1:
        return "<1分"
    days, rem = divmod(minutes, 24 * 60)
    hours, mins = divmod(rem, 60)
    parts: list[str] = []
    if days:
        parts.append(f"{days}天")
    if hours:
        parts.append(f"{hours}时")
    if mins or not parts:
        parts.append(f"{mins}分")
    return "".join(parts)


def _fmt_history_float(value: Any, digits: int = 6) -> str:
    if value is None or value == "":
        return "UNKNOWN"
    return _fmt_float(value, digits=digits)


def _fmt_history_usdt(value: Any) -> str:
    if value is None or value == "":
        return "UNKNOWN"
    return _fmt_usdt(value)


def _sum_income(account: str, *, days: int, income_type: str) -> float:
    end_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    start_ms = end_ms - int(days) * 24 * 60 * 60 * 1000
    rows = _exchange_history_rows(account, "income", start_ms, end_ms)
    total = sum(
        float(row.get("income", 0.0) or 0.0)
        for row in rows
        if str(row.get("income_type") or "").upper() == income_type.upper()
    )
    return round(total, 4)


def _load_exchange_history_rows_or_missing(account: str, source: str, start_ms: int, end_ms: int) -> tuple[list[dict[str, Any]], bool]:
    path = _exchange_history_root(account) / source
    if not path.exists():
        return [], True
    return _exchange_history_rows(account, source, start_ms, end_ms), False


def _manual_order_type(order: dict[str, Any]) -> str | None:
    side = str(order.get("side") or "").upper()
    position_side = str(order.get("position_side") or "").upper()
    if position_side != LONG:
        return None
    if side == "BUY":
        return "开多"
    if side == "SELL":
        return "平多"
    return None


@_admin_required
async def send_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    account = _selected_account(context)
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    start_ms = now_ms - 48 * 60 * 60 * 1000
    order_rows, orders_missing = _load_exchange_history_rows_or_missing(account, "orders", start_ms, now_ms)
    trade_rows, trades_missing = _load_exchange_history_rows_or_missing(account, "trades", start_ms, now_ms)
    transfer_rows, transfers_missing = _load_exchange_history_rows_or_missing(account, "transfers", start_ms, now_ms)
    position_rows, positions_missing = _load_exchange_history_rows_or_missing(account, "positions", start_ms, now_ms)

    filled_orders = [
        row
        for row in order_rows
        if str(row.get("status") or "").upper() in {"FILLED", "PARTIALLY_FILLED"}
        and _manual_order_type(row)
    ]
    filled_orders.sort(key=lambda x: _history_display_ms(x, "time_ms", "update_time_ms"))
    trade_pnl_by_order: dict[str, float] = {}
    for row in trade_rows:
        oid = str(row.get("order_id") or "")
        trade_pnl_by_order[oid] = trade_pnl_by_order.get(oid, 0.0) + float(row.get("realized_pnl", 0.0) or 0.0)

    long_positions = [
        row
        for row in position_rows
        if str(row.get("position_side") or "").upper() == LONG
        and str(row.get("status") or "").upper() in {"CLOSED", "INCOMPLETE"}
    ]
    long_positions.sort(key=lambda x: _history_display_ms(x, "close_time_ms", "last_trade_time_ms"), reverse=True)
    transfer_rows.sort(key=lambda x: _history_display_ms(x, "time_ms", "time"), reverse=True)
    deposit_total = sum(float(row.get("income", 0.0) or 0.0) for row in transfer_rows if float(row.get("income", 0.0) or 0.0) > 0)
    withdraw_total = abs(sum(float(row.get("income", 0.0) or 0.0) for row in transfer_rows if float(row.get("income", 0.0) or 0.0) < 0))

    latest_sync_ms = max(
        _latest_exchange_history_sync_ms(account),
        _latest_loaded_rows_sync_ms(order_rows, trade_rows, transfer_rows, position_rows),
    )
    sync_text = _bj_second(latest_sync_ms) if latest_sync_ms > 0 else "未发现本地同步数据"
    lines = [
        f"📜 {account} 最近48小时历史记录",
        f"数据同步时间: {sync_text}",
        "说明: 以下为本地 exchange_history 账本数据，常驻同步存在分钟级延迟。",
        "",
        "🔸 历史委托:",
    ]
    if filled_orders:
        for order in filled_orders[-60:]:
            order_type = _manual_order_type(order)
            oid = str(order.get("order_id") or "")
            qty = float(order.get("executed_qty", 0.0) or order.get("orig_qty", 0.0) or 0.0)
            price = float(order.get("avg_price", 0.0) or order.get("price", 0.0) or 0.0)
            pnl = trade_pnl_by_order.get(oid, 0.0)
            pnl_text = f"  已实现{_fmt_intish(pnl)}" if abs(pnl) > 1e-9 else ""
            lines.append(
                f"{_bj_minute(order.get('time_ms') or order.get('update_time_ms'))}{order_type} {order.get('symbol')}\n"
                f"                 量{_fmt_float(qty)}  价{_fmt_float(price)}{pnl_text}"
            )
    else:
        lines.append("本地账本无 LONG 成交委托" if not orders_missing else "本地账本缺少 orders 落盘")

    lines.extend(["", "📌 仓位历史:"])
    if long_positions:
        for position in long_positions[:60]:
            symbol = str(position.get("symbol") or "").upper()
            status = str(position.get("status") or "").upper()
            status_text = "完全平仓" if status == "CLOSED" else f"异常: {position.get('incomplete_reason') or status}"
            lines.append(
                f"{symbol} | {status_text} | 盈亏 {_fmt_history_usdt(position.get('realized_pnl'))}\n"
                f"  开仓价: {_fmt_history_float(position.get('entry_price'))}  平仓价: {_fmt_history_float(position.get('average_close_price'))}\n"
                f"  开仓时间: {_bj_minute(position.get('open_time_ms'))}\n"
                f"  平仓时间: {_bj_minute(position.get('close_time_ms'))}\n"
                f"  持仓时间: {_fmt_history_duration(position.get('open_time_ms'), position.get('close_time_ms'))}\n"
                f"  最高O: {_fmt_history_float(position.get('max_open_qty'))}  已平仓量: {_fmt_history_float(position.get('closed_qty'))}"
            )
    else:
        lines.append("本地账本无仓位历史" if not positions_missing else "本地账本缺少 positions 落盘")

    lines.extend(["", "🔹 转账流水:"])
    if transfer_rows:
        for row in transfer_rows[:60]:
            lines.append(
                f"{_bj_minute(row.get('time_ms') or row.get('time'))} | 转账 | "
                f"{row.get('asset') or ''} | {_fmt_float(row.get('income'))}"
            )
    else:
        lines.append("本地账本无转账记录" if not transfers_missing else "本地账本缺少 transfers 落盘")

    fee_24h = _sum_income(account, days=1, income_type="FUNDING_FEE")
    fee_3d = _sum_income(account, days=3, income_type="FUNDING_FEE")
    fee_7d = _sum_income(account, days=7, income_type="FUNDING_FEE")
    fee_30d = _sum_income(account, days=30, income_type="FUNDING_FEE")
    lines.extend(
        [
            "",
            "🔖 汇总统计:",
            f"资金费(24h): {fee_24h if fee_24h is not None else '查询失败'}",
            f"资金费(3d): {fee_3d if fee_3d is not None else '查询失败'}",
            f"资金费(7d): {fee_7d if fee_7d is not None else '查询失败'}",
            f"资金费(30d): {fee_30d if fee_30d is not None else '查询失败'}",
            f"",
            f"净入金(入金-出金): {round(deposit_total - withdraw_total, 4)}",
        ]
    )
    missing_sources = [
        source
        for source, missing in (
            ("orders", orders_missing),
            ("trades", trades_missing),
            ("positions", positions_missing),
            ("transfers", transfers_missing),
        )
        if missing
    ]
    if missing_sources:
        lines.extend(["", f"⚠️ 缺少本地落盘 source: {', '.join(missing_sources)}"])
    await _send_lines(update, lines)
    return ConversationHandler.END


async def _run_market_trade_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    account: str,
    symbol: str,
    leverage: int,
    notional: float,
    sl_price: float,
    tp_price: float,
) -> None:
    price_res = get_last_price(account, symbol)
    if not price_res["ok"]:
        await update.message.reply_text(f"price query failed: {price_res['reason']}")
        return
    entry_price = float(price_res["data"]["price"])
    try:
        _validate_long_protection_prices(entry_price, sl_price, tp_price)
    except ValueError as exc:
        await update.message.reply_text(str(exc))
        return
    quantity = _entry_qty_from_notional(account, symbol, notional, entry_price)
    root = make_order_root()
    try:
        _prepare_symbol(account, symbol, leverage)
    except Exception as exc:
        await update.message.reply_text(f"prepare symbol failed: {exc}")
        return
    entry_res = place_entry_order(
        account,
        symbol,
        LONG,
        quantity,
        client_order_id=_client_order_id("ENT", root),
        notify_label=NOTIFY_LABEL,
    )
    _append_manual_event("manual_trade_market_entry", account=account, symbol=symbol, ok=entry_res["ok"], result=entry_res)
    if not entry_res["ok"]:
        await update.message.reply_text(f"M entry failed: {entry_res['reason']}")
        return
    entry = entry_res["data"]
    executed_qty = float(entry.get("executed_qty", 0.0) or entry.get("qty", 0.0) or 0.0)
    await update.message.reply_text(
        f"M entry submitted\n"
        f"account={account}\n"
        f"symbol={symbol}\n"
        f"qty={_fmt_float(executed_qty)}\n"
        f"avg={_fmt_float(entry.get('avg_price') or entry_price)}\n"
        f"cid={entry.get('client_order_id')}"
    )
    if executed_qty <= 0:
        await update.message.reply_text("M entry has no executed quantity; SL/TP skipped")
        return
    await _protect_manual_entry(
        context.bot,
        update.effective_chat.id,
        account=account,
        symbol=symbol,
        quantity=executed_qty,
        sl_price=sl_price,
        tp_price=tp_price,
        root=root,
    )


async def _run_post_only_trade_command(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    *,
    account: str,
    symbol: str,
    leverage: int,
    notional: float,
    sl_price: float,
    tp_price: float,
) -> None:
    key = (account, symbol)
    if key in _ACTIVE_PO_WATCHERS:
        await update.message.reply_text(f"PO watcher already active: {account} {symbol}")
        return
    root = make_order_root()
    try:
        _prepare_symbol(account, symbol, leverage)
    except Exception as exc:
        await update.message.reply_text(f"prepare symbol failed: {exc}")
        return

    best_bid = 0.0
    entry_res: dict[str, Any] | None = None
    for attempt in range(1, PO_ENTRY_SUBMIT_MAX_ATTEMPTS + 1):
        book_res = get_order_book_top(account, symbol)
        if not book_res["ok"]:
            await update.message.reply_text(f"order book query failed: {book_res['reason']}")
            return
        best_bid = float(book_res["data"]["best_bid"])
        try:
            _validate_long_protection_prices(best_bid, sl_price, tp_price)
        except ValueError as exc:
            await update.message.reply_text(str(exc))
            return
        quantity = _entry_qty_from_notional(account, symbol, notional, best_bid)
        leg = "POE" if attempt == 1 else f"PO{attempt}"
        entry_res = place_limit_order(
            account,
            symbol,
            LONG,
            "BUY",
            quantity,
            best_bid,
            order_role="MANUAL_PO_ENTRY",
            time_in_force="GTX",
            client_order_id=_client_order_id(leg, root),
            notify_label=NOTIFY_LABEL,
        )
        _append_manual_event(
            "manual_trade_po_entry_submit",
            account=account,
            symbol=symbol,
            ok=entry_res["ok"],
            attempt=attempt,
            max_attempts=PO_ENTRY_SUBMIT_MAX_ATTEMPTS,
            price=best_bid,
            result=entry_res,
        )
        if entry_res["ok"]:
            break
        if _is_po_maker_reject(entry_res["reason"]):
            if attempt < PO_ENTRY_SUBMIT_MAX_ATTEMPTS:
                continue
            break
        await update.message.reply_text(f"PO entry failed: {entry_res['reason']}")
        return

    if not entry_res or not entry_res["ok"]:
        reason = entry_res["reason"] if entry_res else "unknown PO entry submit failure"
        await update.message.reply_text(f"PO entry failed after {PO_ENTRY_SUBMIT_MAX_ATTEMPTS} attempts: {reason}")
        return
    entry = entry_res["data"]
    entry_order_id = entry.get("order_id") or entry.get("exchange_order_id")
    entry_client_order_id = str(entry.get("client_order_id") or "")
    if not entry_client_order_id:
        await update.message.reply_text("PO entry missing client_order_id; watcher not started")
        return
    _ACTIVE_PO_WATCHERS.add(key)
    context.application.create_task(
        _watch_po_entry(
            context.bot,
            update.effective_chat.id,
            account=account,
            symbol=symbol,
            entry_order_id=int(entry_order_id) if entry_order_id is not None else None,
            entry_client_order_id=entry_client_order_id,
            root=root,
            sl_price=sl_price,
            tp_price=tp_price,
            timeout_secs=PO_WATCH_TIMEOUT_SECS,
        )
    )
    await update.message.reply_text(
        f"PO entry submitted\n"
        f"account={account}\n"
        f"symbol={symbol}\n"
        f"price={_fmt_float(best_bid)}\n"
        f"wait={PO_WATCH_TIMEOUT_SECS}s\n"
        f"cid={entry_client_order_id}"
    )


def _long_position_qty(account: str, symbol: str, close_ratio: float = 1.0) -> float:
    if close_ratio <= 0 or close_ratio > 1:
        raise ValueError(f"close_ratio must be > 0 and <= 1: {close_ratio}")
    pos_res = get_positions(account, symbol)
    if not pos_res["ok"]:
        raise RuntimeError(pos_res["reason"])
    pos = next((p for p in pos_res["data"] if p.get("position_side") == LONG), None)
    if not pos:
        raise ValueError(f"no LONG position: {symbol}")
    qty = float(pos["qty"])
    if qty <= 0:
        raise ValueError(f"invalid LONG position qty: {symbol} qty={qty}")
    return qty if close_ratio == 1.0 else qty * close_ratio


async def _run_market_close_command(
    update: Update,
    *,
    accounts: list[str],
    symbol: str,
    close_ratio: float,
) -> None:
    lines = [f"M close {symbol}"]
    for account in accounts:
        try:
            qty = _long_position_qty(account, symbol, close_ratio)
            root = make_order_root()
            res = place_time_stop_order(
                account,
                symbol,
                LONG,
                qty,
                order_role="MANUAL_CLOSE",
                client_order_id=_client_order_id("CLS", root),
                notify_label=NOTIFY_LABEL,
            )
            _append_manual_event("manual_trade_market_close", account=account, symbol=symbol, ok=res["ok"], result=res)
            if not res["ok"]:
                lines.append(f"{account}: failed reason={res['reason']}")
                continue
            data = res["data"]
            lines.append(
                f"{account}: submitted qty={_fmt_float(data.get('qty') or qty)} "
                f"avg={_fmt_float(data.get('avg_price'))} cid={data.get('client_order_id')}"
            )
        except Exception as exc:
            _append_manual_event("manual_trade_market_close", account=account, symbol=symbol, ok=False, reason=str(exc))
            lines.append(f"{account}: failed reason={exc}")
    await update.message.reply_text("\n".join(lines))


async def _run_post_only_close_command(
    update: Update,
    *,
    accounts: list[str],
    symbol: str,
    close_ratio: float,
) -> None:
    lines = [f"PO close {symbol}"]
    for account in accounts:
        try:
            qty = _long_position_qty(account, symbol, close_ratio)
            root = make_order_root()
            entry_res: dict[str, Any] | None = None
            best_ask = 0.0
            for attempt in range(1, PO_ENTRY_SUBMIT_MAX_ATTEMPTS + 1):
                book_res = get_order_book_top(account, symbol)
                if not book_res["ok"]:
                    raise RuntimeError(f"order book query failed: {book_res['reason']}")
                best_ask = float(book_res["data"]["best_ask"])
                leg = "CPO" if attempt == 1 else f"CP{attempt}"
                entry_res = place_limit_order(
                    account,
                    symbol,
                    LONG,
                    "SELL",
                    qty,
                    best_ask,
                    order_role="MANUAL_CLOSE",
                    time_in_force="GTX",
                    client_order_id=_client_order_id(leg, root),
                    notify_label=NOTIFY_LABEL,
                )
                _append_manual_event(
                    "manual_trade_po_close_submit",
                    account=account,
                    symbol=symbol,
                    ok=entry_res["ok"],
                    attempt=attempt,
                    max_attempts=PO_ENTRY_SUBMIT_MAX_ATTEMPTS,
                    price=best_ask,
                    result=entry_res,
                )
                if entry_res["ok"]:
                    break
                if _is_po_maker_reject(entry_res["reason"]):
                    if attempt < PO_ENTRY_SUBMIT_MAX_ATTEMPTS:
                        continue
                    break
                lines.append(f"{account}: failed reason={entry_res['reason']}")
                entry_res = None
                break
            if not entry_res or not entry_res["ok"]:
                reason = entry_res["reason"] if entry_res else "unknown PO close submit failure"
                lines.append(f"{account}: failed after {PO_ENTRY_SUBMIT_MAX_ATTEMPTS} attempts reason={reason}")
                continue
            data = entry_res["data"]
            lines.append(
                f"{account}: submitted qty={_fmt_float(data.get('qty') or data.get('orig_qty') or qty)} "
                f"price={_fmt_float(data.get('price') or best_ask)} cid={data.get('client_order_id')}"
            )
        except Exception as exc:
            _append_manual_event("manual_trade_po_close_submit", account=account, symbol=symbol, ok=False, reason=str(exc))
            lines.append(f"{account}: failed reason={exc}")
    await update.message.reply_text("\n".join(lines))


async def _run_limit_close_command(
    update: Update,
    *,
    accounts: list[str],
    symbol: str,
    limit_price: float,
    close_ratio: float,
) -> None:
    lines = [f"L close {symbol} price={_fmt_float(limit_price)}"]
    for account in accounts:
        try:
            qty = _long_position_qty(account, symbol, close_ratio)
            root = make_order_root()
            res = place_limit_order(
                account,
                symbol,
                LONG,
                "SELL",
                qty,
                limit_price,
                order_role="MANUAL_CLOSE",
                time_in_force="GTC",
                client_order_id=_client_order_id("CLS", root),
                notify_label=NOTIFY_LABEL,
            )
            _append_manual_event(
                "manual_trade_limit_close",
                account=account,
                symbol=symbol,
                ok=res["ok"],
                price=limit_price,
                close_ratio=close_ratio,
                result=res,
            )
            if not res["ok"]:
                lines.append(f"{account}: failed reason={res['reason']}")
                continue
            data = res["data"]
            lines.append(
                f"{account}: submitted qty={_fmt_float(data.get('qty') or data.get('orig_qty') or qty)} "
                f"price={_fmt_float(data.get('price') or limit_price)} cid={data.get('client_order_id')}"
            )
        except Exception as exc:
            _append_manual_event(
                "manual_trade_limit_close",
                account=account,
                symbol=symbol,
                ok=False,
                price=limit_price,
                close_ratio=close_ratio,
                reason=str(exc),
            )
            lines.append(f"{account}: failed reason={exc}")
    await update.message.reply_text("\n".join(lines))


async def _run_sl_command(
    update: Update,
    *,
    accounts: list[str],
    symbol: str,
    stop_price: float,
    sl_ratio: float,
) -> None:
    lines = [f"SL {symbol} stop={_fmt_float(stop_price)}"]
    for account in accounts:
        try:
            quantity = None if sl_ratio == 1.0 else _long_position_qty(account, symbol, sl_ratio)
            root = make_order_root()
            res = place_sl_order(
                account,
                symbol,
                LONG,
                stop_price,
                quantity=quantity,
                client_order_id=_client_order_id("SL", root),
                notify_label=NOTIFY_LABEL,
            )
            _append_manual_event(
                "manual_trade_sl_command",
                account=account,
                symbol=symbol,
                ok=res["ok"],
                stop_price=stop_price,
                sl_ratio=sl_ratio,
                quantity=quantity,
                result=res,
            )
            if not res["ok"]:
                lines.append(f"{account}: failed reason={res['reason']}")
                continue
            data = res["data"]
            qty_text = "ALL" if quantity is None else _fmt_float(data.get("qty") or quantity)
            lines.append(
                f"{account}: submitted qty={qty_text} "
                f"stop={_fmt_float(data.get('stop_price') or stop_price)} cid={data.get('client_order_id')}"
            )
        except Exception as exc:
            _append_manual_event(
                "manual_trade_sl_command",
                account=account,
                symbol=symbol,
                ok=False,
                stop_price=stop_price,
                sl_ratio=sl_ratio,
                reason=str(exc),
            )
            lines.append(f"{account}: failed reason={exc}")
    await update.message.reply_text("\n".join(lines))


async def _run_cancel_command(
    update: Update,
    *,
    accounts: list[str],
    symbol: str,
) -> None:
    lines = [f"Cancel open orders {symbol}"]
    for account in accounts:
        res = cancel_all_orders(account, symbol, notify_label=NOTIFY_LABEL)
        _append_manual_event("manual_trade_cancel_all", account=account, symbol=symbol, ok=res["ok"], result=res)
        if not res["ok"]:
            lines.append(f"{account}: failed reason={res['reason']}")
            continue
        rows = list(res.get("data") or [])
        failed = [row for row in rows if not row.get("ok")]
        if failed:
            reasons = "; ".join(str(row.get("reason") or "unknown") for row in failed[:3])
            lines.append(f"{account}: partial failed cancelled={len(rows) - len(failed)} failed={len(failed)} reason={reasons}")
            continue
        lines.append(f"{account}: cancelled={len(rows)}")
    await update.message.reply_text("\n".join(lines))


@_admin_required
async def trade_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    args = list(context.args or [])
    if not args:
        raise ValueError(_trade_usage())
    action = str(args[0]).lower()
    if action == "open":
        spec = _parse_trade_open_args(args)
        mode = spec["mode"]
        if mode == "M":
            await _run_market_trade_command(update, context, **{k: v for k, v in spec.items() if k != "mode"})
            return
        if mode == "PO":
            await _run_post_only_trade_command(update, context, **{k: v for k, v in spec.items() if k != "mode"})
            return
    if action == "close":
        spec = _parse_trade_close_args(args)
        mode = spec["mode"]
        if mode == "M":
            await _run_market_close_command(
                update,
                accounts=spec["accounts"],
                symbol=spec["symbol"],
                close_ratio=spec["close_ratio"],
            )
            return
        if mode == "PO":
            await _run_post_only_close_command(
                update,
                accounts=spec["accounts"],
                symbol=spec["symbol"],
                close_ratio=spec["close_ratio"],
            )
            return
        if mode == "L":
            await _run_limit_close_command(
                update,
                accounts=spec["accounts"],
                symbol=spec["symbol"],
                limit_price=spec["limit_price"],
                close_ratio=spec["close_ratio"],
            )
            return
    if action == "sl":
        spec = _parse_trade_sl_args(args)
        await _run_sl_command(
            update,
            accounts=spec["accounts"],
            symbol=spec["symbol"],
            stop_price=spec["stop_price"],
            sl_ratio=spec["sl_ratio"],
        )
        return
    if action == "cancel":
        spec = _parse_trade_cancel_args(args)
        await _run_cancel_command(
            update,
            accounts=spec["accounts"],
            symbol=spec["symbol"],
        )
        return
    raise ValueError("unsupported trade command")


async def cancel_conv(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    if update.message:
        await update.message.reply_text("cancelled")
    elif update.callback_query:
        await update.callback_query.answer()
        await update.callback_query.edit_message_text("cancelled")
    return ConversationHandler.END


def run_bot() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)-24s | %(message)s",
        datefmt="%m-%d %H:%M:%S",
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("apscheduler").setLevel(logging.WARNING)

    token = _bot_token()
    application = Application.builder().token(token).post_init(post_init).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", start))
    application.add_handler(CommandHandler("set_current_account", set_current_account))
    application.add_handler(CommandHandler("status", account_status))
    application.add_handler(CommandHandler("account_detail", account_detail))
    application.add_handler(CommandHandler("pending_orders", pending_orders))
    application.add_handler(CommandHandler("view_history", send_history))
    application.add_handler(CommandHandler("trade", trade_command))
    application.add_handler(CallbackQueryHandler(select_account, pattern=r"^acct:"))
    application.add_handler(CallbackQueryHandler(pending_orders, pattern=r"^detail_pending$"))
    application.add_handler(CallbackQueryHandler(detail_history, pattern=r"^detail_history$"))
    application.add_handler(CallbackQueryHandler(confirm_cancel_group, pattern=r"^cancel_group:"))
    application.add_handler(CallbackQueryHandler(do_cancel_group, pattern=r"^cancel_group_ok:"))
    application.add_handler(CallbackQueryHandler(confirm_cancel_order, pattern=r"^cancel:"))
    application.add_handler(CallbackQueryHandler(do_cancel_order, pattern=r"^cancel_ok:"))
    application.add_handler(CallbackQueryHandler(abort, pattern=r"^abort$"))

    application.add_handler(
        ConversationHandler(
            entry_points=[CommandHandler("open", open_position)],
            states={
                OPEN_SELECT_SYMBOL: [CallbackQueryHandler(open_symbol_selected, pattern=r"^open_symbol:")],
                OPEN_SELECT_TYPE: [CallbackQueryHandler(open_type_selected, pattern=r"^open_type:")],
                OPEN_INPUT_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, open_input_price)],
                OPEN_INPUT_NOTIONAL: [MessageHandler(filters.TEXT & ~filters.COMMAND, open_input_notional)],
            },
            fallbacks=[CommandHandler("cancel", cancel_conv), CallbackQueryHandler(cancel_conv, pattern=r"^abort$")],
            allow_reentry=True,
            per_user=True,
        )
    )
    application.add_handler(
        ConversationHandler(
            entry_points=[CommandHandler("close", close_position)],
            states={
                CLOSE_SELECT_SYMBOL: [CallbackQueryHandler(close_symbol_selected, pattern=r"^close_symbol:")],
                CLOSE_SELECT_TYPE: [CallbackQueryHandler(close_type_selected, pattern=r"^close_type:")],
                CLOSE_INPUT_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, close_input_price)],
                CLOSE_INPUT_QTY: [MessageHandler(filters.TEXT & ~filters.COMMAND, close_input_qty)],
            },
            fallbacks=[CommandHandler("cancel", cancel_conv), CallbackQueryHandler(cancel_conv, pattern=r"^abort$")],
            allow_reentry=True,
            per_user=True,
        )
    )
    application.add_handler(
        ConversationHandler(
            entry_points=[CommandHandler("stop_market", stop_market)],
            states={
                STOP_SELECT_SYMBOL: [CallbackQueryHandler(stop_symbol_selected, pattern=r"^stop_symbol:")],
                STOP_INPUT_PRICE: [MessageHandler(filters.TEXT & ~filters.COMMAND, stop_input_price)],
            },
            fallbacks=[CommandHandler("cancel", cancel_conv), CallbackQueryHandler(cancel_conv, pattern=r"^abort$")],
            allow_reentry=True,
            per_user=True,
        )
    )
    application.add_handler(
        ConversationHandler(
            entry_points=[CommandHandler("edit_symbols", edit_symbols)],
            states={EDIT_SYMBOLS_INPUT: [MessageHandler(filters.TEXT & ~filters.COMMAND, edit_symbols_input)]},
            fallbacks=[CommandHandler("cancel", cancel_conv)],
            allow_reentry=True,
            per_user=True,
        )
    )
    application.add_error_handler(error_handler)
    application.run_polling()


def main() -> int:
    try:
        _load_permissions()
        _fail_fast_if_pending_manual_po_entries()
        run_bot()
        return 0
    except Exception as exc:
        logging.error("manual bot failed: %s", exc, exc_info=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())
