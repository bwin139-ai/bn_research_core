from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from core.live.binance_exec import (
    get_account_trades,
    get_all_orders,
    get_income_history,
    get_open_orders,
    get_positions,
)
from core.runtime_state import load_json_file, save_json_file_atomic, state_path

BJ = timezone(timedelta(hours=8))
SOURCE_ORDERS = "orders"
SOURCE_TRADES = "trades"
SOURCE_INCOME = "income"
SOURCE_TRANSFERS = "transfers"
ACCOUNT_KEY = "_account"
DAY_MS = 24 * 60 * 60 * 1000
ORDER_TRADE_QUERY_WINDOW_MS = 6 * DAY_MS
INCOME_QUERY_WINDOW_MS = DAY_MS
QUERY_LIMIT = 1000

TRADE_LIFECYCLE_NEEDLES = (
    "entry_submitted",
    "entry_fill_observed",
    "sl_submitted",
    "tp_submitted",
    "time_stop_submitted",
    "time_stop_triggered",
    "position_closed_detected",
    "state_cleared_after_exit",
)


def _now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)


def _bj_day(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).date().isoformat()


def _bj_time(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc).astimezone(BJ).isoformat()


def _history_root(account: str) -> Path:
    return state_path("exchange_history", account, ".keep").parent


def _source_path(account: str, source: str, day: str) -> Path:
    return state_path("exchange_history", account, source, f"{day}.jsonl")


def _sync_state_path(account: str) -> Path:
    return state_path("exchange_history", account, "sync_state.json")


def _symbols_path(account: str) -> Path:
    return state_path("exchange_history", account, "symbols.json")


def _manual_symbols_path() -> Path:
    return state_path("manual_trade_symbols.json")


def _manual_events_dir() -> Path:
    return state_path("manual_trade", "orders")


def _live_audit_dir() -> Path:
    return state_path("live_audit")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _secrets_dir() -> Path:
    raw = os.getenv("BN_SECRETS_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return _repo_root()


def _account_secrets_path(account: str) -> Path:
    account_key = str(account or "").strip()
    if not account_key:
        raise ValueError("account must not be empty")
    return _secrets_dir() / f"secrets_{account_key}.json"


def _load_account_secrets(account: str) -> dict[str, Any]:
    path = _account_secrets_path(account)
    if not path.exists():
        raise FileNotFoundError(f"account secrets missing: {path}")
    data = load_json_file(path, default={})
    if not isinstance(data, dict):
        raise ValueError(f"account secrets must be object: {path}")
    return data


def _exchange_history_floor_ms(account: str) -> int | None:
    path = _account_secrets_path(account)
    data = _load_account_secrets(account)
    raw = data.get("exchange_history_start_time")
    if raw is None or str(raw).strip() == "":
        return None
    if not isinstance(raw, str):
        raise ValueError(f"exchange_history_start_time must be timezone ISO string: {path}")
    text = raw.strip()
    try:
        parsed = datetime.fromisoformat(text)
    except Exception as exc:
        raise ValueError(f"invalid exchange_history_start_time: {path} value={text!r}") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"exchange_history_start_time must include timezone: {path} value={text!r}")
    return int(parsed.astimezone(timezone.utc).timestamp() * 1000)


def _symbols_from_file(path_text: str) -> set[str]:
    path = Path(path_text).expanduser()
    if not path.exists():
        raise FileNotFoundError(f"symbol file missing: {path}")
    text = path.read_text(encoding="utf-8")
    symbols: list[Any]
    if path.suffix.lower() == ".json":
        data = json.loads(text)
        if not isinstance(data, list):
            raise ValueError(f"symbol file JSON must be a list: {path}")
        symbols = data
    else:
        symbols = [line.strip() for line in text.splitlines() if line.strip()]
    cleaned = {str(s or "").upper().strip() for s in symbols}
    bad = sorted(s for s in cleaned if not s.endswith("USDT"))
    if bad:
        raise ValueError(f"symbol file contains non-USDT symbols: {path} bad={bad[:10]}")
    if not cleaned:
        raise ValueError(f"symbol file must contain at least one symbol: {path}")
    return cleaned


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _append_jsonl_unique(path: Path, records: list[dict[str, Any]]) -> int:
    if not records:
        return 0
    existing = {str(row.get("dedupe_key") or "") for row in _load_jsonl(path)}
    existing.discard("")
    fresh = [row for row in records if str(row.get("dedupe_key") or "") not in existing]
    if not fresh:
        return 0
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        for row in fresh:
            f.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    return len(fresh)


def _event_time_ms(source: str, row: dict[str, Any]) -> int:
    keys_by_source = {
        SOURCE_ORDERS: ("update_time_ms", "time_ms"),
        SOURCE_TRADES: ("time_ms",),
        SOURCE_INCOME: ("time_ms",),
        SOURCE_TRANSFERS: ("time_ms",),
    }
    for key in keys_by_source.get(source, ("time_ms", "update_time_ms")):
        try:
            value = int(row.get(key) or 0)
        except Exception:
            value = 0
        if value > 0:
            return value
    return _now_ms()


def _dedupe_key(account: str, source: str, row: dict[str, Any]) -> str:
    symbol = str(row.get("symbol") or "").upper().strip()
    if source == SOURCE_ORDERS:
        return f"{account}|{source}|{symbol}|{row.get('order_id')}"
    if source == SOURCE_TRADES:
        return f"{account}|{source}|{symbol}|{row.get('trade_id')}|{row.get('order_id')}"
    if source in {SOURCE_INCOME, SOURCE_TRANSFERS}:
        return (
            f"{account}|{source}|{symbol}|{row.get('income_type')}|{row.get('tran_id')}|"
            f"{row.get('trade_id')}|{row.get('time_ms')}|{row.get('income')}"
        )
    raise ValueError(f"unsupported source: {source}")


def _history_record(account: str, source: str, row: dict[str, Any], sync_ms: int) -> dict[str, Any]:
    event_ms = _event_time_ms(source, row)
    symbol = str(row.get("symbol") or "").upper().strip()
    return {
        "source": source,
        "account": account,
        "symbol": symbol,
        "event_time_ms": event_ms,
        "event_day_bj": _bj_day(event_ms),
        "sync_time_ms": sync_ms,
        "sync_time_bj": _bj_time(sync_ms),
        "dedupe_key": _dedupe_key(account, source, row),
        "raw": row,
    }


def _write_history_rows(account: str, source: str, rows: list[dict[str, Any]], sync_ms: int) -> int:
    records_by_day: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        record = _history_record(account, source, row, sync_ms)
        records_by_day.setdefault(record["event_day_bj"], []).append(record)
    written = 0
    for day, records in records_by_day.items():
        written += _append_jsonl_unique(_source_path(account, source, day), records)
    return written


def _load_sync_state(account: str) -> dict[str, Any]:
    data = load_json_file(_sync_state_path(account), default={})
    if not isinstance(data, dict):
        raise ValueError(f"sync_state must be object: {_sync_state_path(account)}")
    data.setdefault("sources", {})
    return data


def _state_has_source_progress(state: dict[str, Any]) -> bool:
    sources = state.get("sources", {})
    if not isinstance(sources, dict):
        return False
    for source_map in sources.values():
        if not isinstance(source_map, dict):
            continue
        for row in source_map.values():
            if not isinstance(row, dict):
                continue
            try:
                last_end = int(row.get("last_end_ms") or 0)
            except Exception:
                last_end = 0
            if row.get("ok") and last_end > 0:
                return True
    return False


def _source_state_key(source: str, symbol: str | None) -> str:
    if source in {SOURCE_INCOME, SOURCE_TRANSFERS}:
        return ACCOUNT_KEY
    value = str(symbol or "").upper().strip()
    if not value:
        raise ValueError(f"{source} sync requires symbol")
    return value


def _source_start_ms(
    state: dict[str, Any],
    source: str,
    symbol: str | None,
    default_start_ms: int,
    overlap_ms: int,
) -> int:
    source_map = state.get("sources", {}).get(source, {})
    if not isinstance(source_map, dict):
        return default_start_ms
    row = source_map.get(_source_state_key(source, symbol), {})
    if not isinstance(row, dict):
        return default_start_ms
    if row.get("ok") is False:
        return default_start_ms
    try:
        last_end = int(row.get("last_end_ms") or 0)
    except Exception:
        last_end = 0
    if last_end <= 0:
        return default_start_ms
    return max(default_start_ms, last_end - overlap_ms)


def _update_source_state(
    state: dict[str, Any],
    source: str,
    symbol: str | None,
    *,
    start_ms: int,
    end_ms: int,
    rows_seen: int,
    rows_written: int,
    ok: bool,
    reason: str = "",
) -> None:
    sources = state.setdefault("sources", {})
    source_map = sources.setdefault(source, {})
    key = _source_state_key(source, symbol)
    source_map[key] = {
        "last_start_ms": int(start_ms),
        "last_end_ms": int(end_ms),
        "last_sync_time_ms": _now_ms(),
        "last_sync_time_bj": _bj_time(_now_ms()),
        "rows_seen": int(rows_seen),
        "rows_written": int(rows_written),
        "ok": bool(ok),
        "reason": str(reason or ""),
    }


def _iter_windows(start_ms: int, end_ms: int, window_ms: int) -> list[tuple[int, int]]:
    if start_ms > end_ms:
        return []
    if window_ms <= 0:
        raise ValueError("window_ms must be positive")
    windows: list[tuple[int, int]] = []
    cur = int(start_ms)
    final = int(end_ms)
    while cur <= final:
        win_end = min(final, cur + int(window_ms) - 1)
        windows.append((cur, win_end))
        cur = win_end + 1
    return windows


def _days_for_window(start_ms: int, end_ms: int) -> list[str]:
    start_day = datetime.fromtimestamp(start_ms / 1000.0, tz=timezone.utc).astimezone(BJ).date()
    end_day = datetime.fromtimestamp(end_ms / 1000.0, tz=timezone.utc).astimezone(BJ).date()
    days: list[str] = []
    cur = start_day
    while cur <= end_day:
        days.append(cur.isoformat())
        cur = cur + timedelta(days=1)
    return days


def _symbols_from_manual_config() -> set[str]:
    data = load_json_file(_manual_symbols_path(), default=[])
    if not isinstance(data, list):
        return set()
    return {str(row.get("symbol") or "").upper().strip() for row in data if isinstance(row, dict)}


def _symbols_from_existing_index(account: str) -> set[str]:
    data = load_json_file(_symbols_path(account), default=[])
    if not isinstance(data, list):
        return set()
    return {str(x or "").upper().strip() for x in data}


def _symbols_from_jsonl(path: Path, *, account: str | None = None, trade_events_only: bool = False) -> set[str]:
    symbols: set[str] = set()
    for row in _load_jsonl(path):
        if account and str(row.get("account") or "") != account:
            continue
        if trade_events_only:
            event = str(row.get("event") or "").lower().strip()
            if not any(needle in event for needle in TRADE_LIFECYCLE_NEEDLES):
                continue
        symbol = str(row.get("symbol") or "").upper().strip()
        if symbol.endswith("USDT"):
            symbols.add(symbol)
    return symbols


def _symbols_from_local_events(account: str, start_ms: int, end_ms: int) -> set[str]:
    symbols: set[str] = set()
    for day in _days_for_window(start_ms, end_ms):
        symbols.update(_symbols_from_jsonl(_manual_events_dir() / f"{day}.jsonl", account=account))
        for path in _live_audit_dir().glob(f"*_{account}.{day}.jsonl"):
            symbols.update(_symbols_from_jsonl(path, account=account, trade_events_only=True))
    return symbols


def _symbols_from_existing_history(account: str) -> set[str]:
    symbols: set[str] = set()
    root = _history_root(account)
    for source in (SOURCE_ORDERS, SOURCE_TRADES):
        for path in (root / source).glob("*.jsonl"):
            for row in _load_jsonl(path):
                symbol = str(row.get("symbol") or row.get("raw", {}).get("symbol") or "").upper().strip()
                if symbol.endswith("USDT"):
                    symbols.add(symbol)
    return symbols


def _symbols_from_exchange_snapshot(account: str) -> tuple[set[str], list[str]]:
    symbols: set[str] = set()
    errors: list[str] = []
    pos_res = get_positions(account)
    if pos_res["ok"]:
        symbols.update(str(row.get("symbol") or "").upper().strip() for row in pos_res["data"] if float(row.get("qty", 0.0) or 0.0) != 0.0)
    else:
        errors.append(f"positions: {pos_res['reason']}")
    ord_res = get_open_orders(account)
    if ord_res["ok"]:
        symbols.update(str(row.get("symbol") or "").upper().strip() for row in ord_res["data"])
    else:
        errors.append(f"open_orders: {ord_res['reason']}")
    return {s for s in symbols if s.endswith("USDT")}, errors


def discover_symbols(
    account: str,
    *,
    start_ms: int,
    end_ms: int,
    explicit_symbols: list[str] | None = None,
    include_exchange_snapshot: bool = True,
) -> dict[str, Any]:
    symbols = {str(s or "").upper().strip() for s in (explicit_symbols or [])}
    symbols.update(_symbols_from_manual_config())
    symbols.update(_symbols_from_existing_index(account))
    symbols.update(_symbols_from_local_events(account, start_ms, end_ms))
    symbols.update(_symbols_from_existing_history(account))
    errors: list[str] = []
    if include_exchange_snapshot:
        exchange_symbols, exchange_errors = _symbols_from_exchange_snapshot(account)
        symbols.update(exchange_symbols)
        errors.extend(exchange_errors)
    cleaned = sorted(s for s in symbols if s.endswith("USDT"))
    save_json_file_atomic(_symbols_path(account), cleaned, indent=2)
    return {"symbols": cleaned, "errors": errors}


def _sync_orders(account: str, symbol: str, start_ms: int, end_ms: int, sync_ms: int) -> dict[str, Any]:
    rows_seen = 0
    rows_written = 0
    cursor_end_ms = int(start_ms)
    windows = _iter_windows(start_ms, end_ms, ORDER_TRADE_QUERY_WINDOW_MS)
    for window_start, window_end in windows:
        res = get_all_orders(account, symbol, start_time_ms=window_start, end_time_ms=window_end, limit=QUERY_LIMIT)
        if not res["ok"]:
            return {
                "ok": False,
                "reason": res["reason"],
                "rows_seen": rows_seen,
                "rows_written": rows_written,
                "cursor_end_ms": cursor_end_ms,
                "windows": len(windows),
                "failed_window": {"start_ms": window_start, "end_ms": window_end},
            }
        rows = list(res["data"] or [])
        if len(rows) >= QUERY_LIMIT:
            return {
                "ok": False,
                "reason": f"orders window hit limit={QUERY_LIMIT}; split window smaller before trusting completeness",
                "rows_seen": rows_seen,
                "rows_written": rows_written,
                "cursor_end_ms": cursor_end_ms,
                "windows": len(windows),
                "failed_window": {"start_ms": window_start, "end_ms": window_end},
            }
        rows_seen += len(rows)
        rows_written += _write_history_rows(account, SOURCE_ORDERS, rows, sync_ms)
        cursor_end_ms = window_end
    return {"ok": True, "reason": "", "rows_seen": rows_seen, "rows_written": rows_written, "cursor_end_ms": int(end_ms), "windows": len(windows)}


def _sync_trades(account: str, symbol: str, start_ms: int, end_ms: int, sync_ms: int) -> dict[str, Any]:
    rows_seen = 0
    rows_written = 0
    cursor_end_ms = int(start_ms)
    windows = _iter_windows(start_ms, end_ms, ORDER_TRADE_QUERY_WINDOW_MS)
    for window_start, window_end in windows:
        res = get_account_trades(account, symbol, start_time_ms=window_start, end_time_ms=window_end, limit=QUERY_LIMIT)
        if not res["ok"]:
            return {
                "ok": False,
                "reason": res["reason"],
                "rows_seen": rows_seen,
                "rows_written": rows_written,
                "cursor_end_ms": cursor_end_ms,
                "windows": len(windows),
                "failed_window": {"start_ms": window_start, "end_ms": window_end},
            }
        rows = list(res["data"] or [])
        if len(rows) >= QUERY_LIMIT:
            return {
                "ok": False,
                "reason": f"trades window hit limit={QUERY_LIMIT}; split window smaller before trusting completeness",
                "rows_seen": rows_seen,
                "rows_written": rows_written,
                "cursor_end_ms": cursor_end_ms,
                "windows": len(windows),
                "failed_window": {"start_ms": window_start, "end_ms": window_end},
            }
        rows_seen += len(rows)
        rows_written += _write_history_rows(account, SOURCE_TRADES, rows, sync_ms)
        cursor_end_ms = window_end
    return {"ok": True, "reason": "", "rows_seen": rows_seen, "rows_written": rows_written, "cursor_end_ms": int(end_ms), "windows": len(windows)}


def _sync_income(account: str, start_ms: int, end_ms: int, sync_ms: int) -> dict[str, Any]:
    income_seen = 0
    income_written = 0
    transfers_seen = 0
    transfers_written = 0
    cursor_end_ms = int(start_ms)
    windows = _iter_windows(start_ms, end_ms, INCOME_QUERY_WINDOW_MS)
    for window_start, window_end in windows:
        res = get_income_history(account, start_time_ms=window_start, end_time_ms=window_end, limit=QUERY_LIMIT)
        if not res["ok"]:
            return {
                "ok": False,
                "reason": res["reason"],
                "income_seen": income_seen,
                "income_written": income_written,
                "transfers_seen": transfers_seen,
                "transfers_written": transfers_written,
                "cursor_end_ms": cursor_end_ms,
                "windows": len(windows),
                "failed_window": {"start_ms": window_start, "end_ms": window_end},
            }
        income_rows = list(res["data"] or [])
        if len(income_rows) >= QUERY_LIMIT:
            return {
                "ok": False,
                "reason": f"income window hit limit={QUERY_LIMIT}; split window smaller before trusting completeness",
                "income_seen": income_seen,
                "income_written": income_written,
                "transfers_seen": transfers_seen,
                "transfers_written": transfers_written,
                "cursor_end_ms": cursor_end_ms,
                "windows": len(windows),
                "failed_window": {"start_ms": window_start, "end_ms": window_end},
            }
        transfer_rows = [row for row in income_rows if str(row.get("income_type") or "").upper() == "TRANSFER"]
        income_seen += len(income_rows)
        transfers_seen += len(transfer_rows)
        income_written += _write_history_rows(account, SOURCE_INCOME, income_rows, sync_ms)
        transfers_written += _write_history_rows(account, SOURCE_TRANSFERS, transfer_rows, sync_ms)
        cursor_end_ms = window_end
    return {
        "ok": True,
        "reason": "",
        "income_seen": income_seen,
        "income_written": income_written,
        "transfers_seen": transfers_seen,
        "transfers_written": transfers_written,
        "cursor_end_ms": int(end_ms),
        "windows": len(windows),
    }


def sync_account_history(
    account: str,
    *,
    symbols: list[str] | None = None,
    symbol_files: list[str] | None = None,
    lookback_hours: int = 24,
    overlap_minutes: int = 10,
    include_exchange_snapshot: bool = True,
    end_ms: int | None = None,
    bootstrap: bool = False,
) -> dict[str, Any]:
    account_key = str(account or "").strip()
    if not account_key:
        raise ValueError("account must not be empty")
    sync_ms = _now_ms()
    final_end_ms = int(end_ms or sync_ms)
    requested_start_ms = final_end_ms - int(lookback_hours) * 60 * 60 * 1000
    floor_ms = _exchange_history_floor_ms(account_key)
    overlap_ms = int(overlap_minutes) * 60 * 1000
    state = _load_sync_state(account_key)
    if bootstrap and floor_ms is None:
        raise ValueError("bootstrap requires exchange_history_start_time in account secrets")
    has_progress = _state_has_source_progress(state)
    if bootstrap:
        default_start_ms = int(floor_ms)
        state_for_start = {"sources": {}}
        sync_mode = "bootstrap"
    elif floor_ms is not None and not has_progress:
        default_start_ms = int(floor_ms)
        state_for_start = state
        sync_mode = "initial_from_exchange_history_start_time"
    else:
        default_start_ms = max(requested_start_ms, floor_ms) if floor_ms is not None else requested_start_ms
        state_for_start = state
        sync_mode = "incremental"
    explicit_symbols = list(symbols or [])
    for symbol_file in symbol_files or []:
        explicit_symbols.extend(sorted(_symbols_from_file(symbol_file)))
    discovery = discover_symbols(
        account_key,
        start_ms=default_start_ms,
        end_ms=final_end_ms,
        explicit_symbols=explicit_symbols,
        include_exchange_snapshot=include_exchange_snapshot,
    )
    sync_symbols = list(discovery["symbols"])
    results: dict[str, Any] = {
        "ok": True,
        "account": account_key,
        "start_ms": default_start_ms,
        "requested_start_ms": requested_start_ms,
        "exchange_history_start_ms": floor_ms,
        "sync_mode": sync_mode,
        "bootstrap": bool(bootstrap),
        "end_ms": final_end_ms,
        "symbols": sync_symbols,
        "discovery_errors": list(discovery.get("errors") or []),
        "sources": {SOURCE_ORDERS: {}, SOURCE_TRADES: {}, SOURCE_INCOME: {}, SOURCE_TRANSFERS: {}},
        "errors": [],
    }

    for symbol in sync_symbols:
        order_start = _source_start_ms(state_for_start, SOURCE_ORDERS, symbol, default_start_ms, overlap_ms)
        order_res = _sync_orders(account_key, symbol, order_start, final_end_ms, sync_ms)
        _update_source_state(
            state,
            SOURCE_ORDERS,
            symbol,
            start_ms=order_start,
            end_ms=int(order_res.get("cursor_end_ms", order_start)),
            rows_seen=order_res["rows_seen"],
            rows_written=order_res["rows_written"],
            ok=order_res["ok"],
            reason=order_res["reason"],
        )
        results["sources"][SOURCE_ORDERS][symbol] = order_res
        if not order_res["ok"]:
            results["ok"] = False
            results["errors"].append(f"{SOURCE_ORDERS} {symbol}: {order_res['reason']}")

        trade_start = _source_start_ms(state_for_start, SOURCE_TRADES, symbol, default_start_ms, overlap_ms)
        trade_res = _sync_trades(account_key, symbol, trade_start, final_end_ms, sync_ms)
        _update_source_state(
            state,
            SOURCE_TRADES,
            symbol,
            start_ms=trade_start,
            end_ms=int(trade_res.get("cursor_end_ms", trade_start)),
            rows_seen=trade_res["rows_seen"],
            rows_written=trade_res["rows_written"],
            ok=trade_res["ok"],
            reason=trade_res["reason"],
        )
        results["sources"][SOURCE_TRADES][symbol] = trade_res
        if not trade_res["ok"]:
            results["ok"] = False
            results["errors"].append(f"{SOURCE_TRADES} {symbol}: {trade_res['reason']}")

    income_start = _source_start_ms(state_for_start, SOURCE_INCOME, None, default_start_ms, overlap_ms)
    income_res = _sync_income(account_key, income_start, final_end_ms, sync_ms)
    _update_source_state(
        state,
        SOURCE_INCOME,
        None,
        start_ms=income_start,
        end_ms=int(income_res.get("cursor_end_ms", income_start)),
        rows_seen=income_res["income_seen"],
        rows_written=income_res["income_written"],
        ok=income_res["ok"],
        reason=income_res["reason"],
    )
    _update_source_state(
        state,
        SOURCE_TRANSFERS,
        None,
        start_ms=income_start,
        end_ms=int(income_res.get("cursor_end_ms", income_start)),
        rows_seen=income_res["transfers_seen"],
        rows_written=income_res["transfers_written"],
        ok=income_res["ok"],
        reason=income_res["reason"],
    )
    results["sources"][SOURCE_INCOME][ACCOUNT_KEY] = {
        "ok": income_res["ok"],
        "reason": income_res["reason"],
        "rows_seen": income_res["income_seen"],
        "rows_written": income_res["income_written"],
        "cursor_end_ms": income_res.get("cursor_end_ms"),
        "windows": income_res.get("windows"),
    }
    results["sources"][SOURCE_TRANSFERS][ACCOUNT_KEY] = {
        "ok": income_res["ok"],
        "reason": income_res["reason"],
        "rows_seen": income_res["transfers_seen"],
        "rows_written": income_res["transfers_written"],
        "cursor_end_ms": income_res.get("cursor_end_ms"),
        "windows": income_res.get("windows"),
    }
    if not income_res["ok"]:
        results["ok"] = False
        results["errors"].append(f"{SOURCE_INCOME}: {income_res['reason']}")

    save_json_file_atomic(_sync_state_path(account_key), state, indent=2)
    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Binance account exchange history into local state.")
    parser.add_argument("--account", required=True)
    parser.add_argument("--symbol", action="append", default=[], help="Optional USDT symbol. Can be repeated.")
    parser.add_argument("--symbol-file", action="append", default=[], help="Optional JSON list or newline text file of USDT symbols. Can be repeated.")
    parser.add_argument("--lookback-hours", type=int, default=24)
    parser.add_argument("--overlap-minutes", type=int, default=10)
    parser.add_argument("--no-exchange-snapshot", action="store_true")
    parser.add_argument("--bootstrap", action="store_true", help="Backfill from exchange_history_start_time and ignore prior per-source cursors.")
    parser.add_argument("--loop", action="store_true", help="Run continuously instead of one sync pass.")
    parser.add_argument("--interval-secs", type=int, default=300, help="Loop sleep interval in seconds.")
    parser.add_argument("--max-iterations", type=int, default=0, help="Loop iteration limit. 0 means unlimited.")
    return parser.parse_args()


def _run_once(args: argparse.Namespace) -> dict[str, Any]:
    return sync_account_history(
        args.account,
        symbols=args.symbol,
        symbol_files=args.symbol_file,
        lookback_hours=args.lookback_hours,
        overlap_minutes=args.overlap_minutes,
        include_exchange_snapshot=not args.no_exchange_snapshot,
        bootstrap=args.bootstrap,
    )


def _print_result(result: dict[str, Any]) -> None:
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)


def main() -> int:
    args = _parse_args()
    if args.interval_secs <= 0:
        raise ValueError("--interval-secs must be positive")
    if args.max_iterations < 0:
        raise ValueError("--max-iterations must be >= 0")
    if args.loop and args.bootstrap:
        raise ValueError("--bootstrap must be run as a bounded one-shot sync, not with --loop")

    if not args.loop:
        result = _run_once(args)
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0 if result["ok"] else 1

    iterations = 0
    last_ok = True
    while True:
        iterations += 1
        try:
            result = _run_once(args)
        except Exception as exc:
            result = {
                "ok": False,
                "account": args.account,
                "sync_time_ms": _now_ms(),
                "sync_time_bj": _bj_time(_now_ms()),
                "reason": str(exc),
            }
        last_ok = bool(result.get("ok"))
        _print_result(result)
        if args.max_iterations and iterations >= args.max_iterations:
            break
        time.sleep(int(args.interval_secs))
    return 0 if last_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
