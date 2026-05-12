from __future__ import annotations

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
    ensure_hedge_mode,
    ensure_leverage,
    get_account_status,
    get_account_trades,
    get_all_orders,
    get_income_history,
    get_last_price,
    get_open_orders,
    get_positions,
    place_entry_order,
    place_limit_order,
    place_sl_order,
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

def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _permissions_path() -> Path:
    return _repo_root() / "permissions.json"


def _symbols_path() -> Path:
    return state_path("manual_trade_symbols.json")


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
        await query.edit_message_text("Input limit price")
        return OPEN_INPUT_PRICE
    await query.edit_message_text("Input notional USDT")
    return OPEN_INPUT_NOTIONAL


@_admin_required
async def open_input_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["open_price"] = float(update.message.text.strip())
    await update.message.reply_text("Input notional USDT")
    return OPEN_INPUT_NOTIONAL


def _prepare_symbol(account: str, symbol: str, leverage: int) -> None:
    for res in (
        ensure_hedge_mode(account),
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
    notional = float(update.message.text.strip())
    if notional <= 0:
        await update.message.reply_text("notional must be positive")
        return OPEN_INPUT_NOTIONAL
    price = float(context.user_data.get("open_price") or 0.0)
    if order_type == "MARKET":
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
        f"LONG open submitted {symbol}\nqty={_fmt_float(data.get('qty'))}\n"
        f"price={_fmt_float(data.get('price') or data.get('avg_price') or price)}\n"
        f"oid={data.get('exchange_order_id')}\ncid={data.get('client_order_id')}"
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
    await query.edit_message_text("Input limit close price")
    return CLOSE_INPUT_PRICE


@_admin_required
async def close_input_price(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    context.user_data["close_price"] = float(update.message.text.strip())
    await update.message.reply_text("Input close qty, or ALL")
    return CLOSE_INPUT_QTY


@_admin_required
async def close_input_qty(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    account = _selected_account(context)
    symbol = str(context.user_data["close_symbol"])
    price = float(context.user_data["close_price"])
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
        client_order_id=_order_root("CLS"),
        notify_label=NOTIFY_LABEL,
    )
    if not res["ok"]:
        await update.message.reply_text(f"limit close failed: {res['reason']}")
        return ConversationHandler.END
    data = res["data"]
    await update.message.reply_text(
        f"limit close submitted {symbol}\nqty={_fmt_float(data.get('qty'))}\n"
        f"price={_fmt_float(data.get('price'))}\noid={data.get('order_id')}\ncid={data.get('client_order_id')}"
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


def _history_symbols(account: str) -> list[str]:
    symbols = {row["symbol"] for row in _load_symbol_rows()}
    try:
        symbols.update(str(p.get("symbol") or "").upper() for p in _long_positions(account))
    except Exception:
        pass
    try:
        symbols.update(str(o.get("symbol") or "").upper() for o in _long_orders(account))
    except Exception:
        pass
    return sorted(s for s in symbols if s.endswith("USDT"))


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


def _sum_income(account: str, *, days: int, income_type: str) -> float | None:
    end_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    start_ms = end_ms - int(days) * 24 * 60 * 60 * 1000
    res = get_income_history(
        account,
        income_type=income_type,
        start_time_ms=start_ms,
        end_time_ms=end_ms,
        limit=1000,
    )
    if not res["ok"]:
        return None
    return round(sum(float(row.get("income", 0.0) or 0.0) for row in res["data"]), 4)


@_admin_required
async def send_history(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    account = _selected_account(context)
    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    start_ms = now_ms - 24 * 60 * 60 * 1000
    order_rows: list[dict[str, Any]] = []
    trade_rows: list[dict[str, Any]] = []
    errors: list[str] = []
    for symbol in _history_symbols(account):
        orders = get_all_orders(account, symbol, start_time_ms=start_ms, end_time_ms=now_ms, limit=100)
        if orders["ok"]:
            order_rows.extend(orders["data"])
        else:
            errors.append(f"{symbol} orders: {orders['reason']}")
        trades = get_account_trades(account, symbol, start_time_ms=start_ms, end_time_ms=now_ms, limit=100)
        if trades["ok"]:
            trade_rows.extend(trades["data"])
        else:
            errors.append(f"{symbol} trades: {trades['reason']}")

    filled_orders = [
        row
        for row in order_rows
        if str(row.get("status") or "").upper() in {"FILLED", "PARTIALLY_FILLED"}
        and _manual_order_type(row)
    ]
    filled_orders.sort(key=lambda x: int(x.get("time_ms") or x.get("update_time_ms") or 0))
    transfer = get_income_history(
        account,
        income_type="TRANSFER",
        start_time_ms=start_ms,
        end_time_ms=now_ms,
        limit=1000,
    )
    transfer_rows = transfer["data"] if transfer["ok"] else []
    deposit_total = sum(float(row.get("income", 0.0) or 0.0) for row in transfer_rows if float(row.get("income", 0.0) or 0.0) > 0)
    withdraw_total = abs(sum(float(row.get("income", 0.0) or 0.0) for row in transfer_rows if float(row.get("income", 0.0) or 0.0) < 0))
    trade_pnl_by_order: dict[Any, float] = {}
    for row in trade_rows:
        oid = row.get("order_id")
        trade_pnl_by_order[oid] = trade_pnl_by_order.get(oid, 0.0) + float(row.get("realized_pnl", 0.0) or 0.0)

    lines = [f"📜 {account} 最近24小时历史记录", "", "🔸 历史委托:"]
    if filled_orders:
        for order in filled_orders[-60:]:
            order_type = _manual_order_type(order)
            oid = order.get("order_id")
            qty = float(order.get("executed_qty", 0.0) or order.get("orig_qty", 0.0) or 0.0)
            price = float(order.get("avg_price", 0.0) or order.get("price", 0.0) or 0.0)
            pnl = trade_pnl_by_order.get(oid, 0.0)
            pnl_text = f"  已实现{_fmt_intish(pnl)}" if abs(pnl) > 1e-9 else ""
            lines.append(
                f"{_bj_minute(order.get('time_ms') or order.get('update_time_ms'))}{order_type} {order.get('symbol')}\n"
                f"                 量{_fmt_float(qty)}  价{_fmt_float(price)}{pnl_text}"
            )
    else:
        lines.append("无 LONG 成交委托")

    lines.extend(["", "🔹 转账流水:"])
    if transfer_rows:
        for row in transfer_rows:
            lines.append(
                f"{_bj_minute(row.get('time_ms') or row.get('time'))} | 转账 | "
                f"{row.get('asset') or ''} | {_fmt_float(row.get('income'))}"
            )
    else:
        lines.append("无转账记录" if transfer["ok"] else f"转账查询失败: {transfer['reason']}")

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
    if errors:
        lines.extend(["", "⚠️ 部分查询失败:"])
        lines.extend(errors[:10])
    await _send_lines(update, lines)
    return ConversationHandler.END


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
        run_bot()
        return 0
    except Exception as exc:
        logging.error("manual bot failed: %s", exc, exc_info=True)
        return 2


if __name__ == "__main__":
    sys.exit(main())
