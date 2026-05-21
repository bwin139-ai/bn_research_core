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
    get_account_status,
    get_all_orders,
    get_income_history,
)
from core.live.binance_rest_gateway import REQUEST_PRIORITY_LOW
from core.runtime_state import load_json_file, save_json_file_atomic, state_path

BJ = timezone(timedelta(hours=8))
SOURCE_ORDERS = "orders"
SOURCE_TRADES = "trades"
SOURCE_INCOME = "income"
SOURCE_TRANSFERS = "transfers"
SOURCE_BALANCE_SNAPSHOTS = "balance_snapshots"
ACCOUNT_KEY = "_account"
DAY_MS = 24 * 60 * 60 * 1000
ORDER_TRADE_QUERY_WINDOW_MS = 6 * DAY_MS
INCOME_QUERY_WINDOW_MS = DAY_MS
QUERY_LIMIT = 1000
DEFAULT_REQUEST_SLEEP_SECS = 0.3


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


def _clean_usdt_symbols(symbols: list[str] | set[str]) -> set[str]:
    cleaned = {str(s or "").upper().strip() for s in symbols}
    cleaned.discard("")
    bad = sorted(s for s in cleaned if not s.endswith("USDT"))
    if bad:
        raise ValueError(f"symbols must be USDT symbols: bad={bad[:10]}")
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
        SOURCE_BALANCE_SNAPSHOTS: ("time_ms",),
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
    if source == SOURCE_BALANCE_SNAPSHOTS:
        asset = str(row.get("asset") or "").upper().strip()
        return f"{account}|{source}|{asset}|{row.get('time_ms')}"
    raise ValueError(f"unsupported source: {source}")


def _history_record(account: str, source: str, row: dict[str, Any], sync_ms: int) -> dict[str, Any]:
    event_ms = _event_time_ms(source, row)
    symbol = str(row.get("symbol") or "").upper().strip()
    asset = str(row.get("asset") or "").upper().strip()
    return {
        "source": source,
        "account": account,
        "symbol": symbol,
        "asset": asset,
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


def _sleep_between_requests(request_sleep_secs: float) -> None:
    seconds = float(request_sleep_secs)
    if seconds > 0:
        time.sleep(seconds)


def _symbols_from_existing_index(account: str) -> set[str]:
    data = load_json_file(_symbols_path(account), default=[])
    if not isinstance(data, list):
        return set()
    return {str(x or "").upper().strip() for x in data}


def _income_symbols(rows: list[dict[str, Any]]) -> set[str]:
    symbols = {str(row.get("symbol") or "").upper().strip() for row in rows}
    return {symbol for symbol in symbols if symbol.endswith("USDT")}


def _merge_symbol_index(account: str, symbols: set[str]) -> list[str]:
    merged = _symbols_from_existing_index(account)
    merged.update(symbols)
    cleaned = sorted(symbol for symbol in merged if symbol.endswith("USDT"))
    save_json_file_atomic(_symbols_path(account), cleaned, indent=2)
    return cleaned


def _sync_orders(account: str, symbol: str, start_ms: int, end_ms: int, sync_ms: int, request_sleep_secs: float) -> dict[str, Any]:
    rows_seen = 0
    rows_written = 0
    cursor_end_ms = int(start_ms)
    windows = _iter_windows(start_ms, end_ms, ORDER_TRADE_QUERY_WINDOW_MS)
    for window_start, window_end in windows:
        res = get_all_orders(
            account,
            symbol,
            start_time_ms=window_start,
            end_time_ms=window_end,
            limit=QUERY_LIMIT,
            priority=REQUEST_PRIORITY_LOW,
        )
        _sleep_between_requests(request_sleep_secs)
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


def _sync_trades(account: str, symbol: str, start_ms: int, end_ms: int, sync_ms: int, request_sleep_secs: float) -> dict[str, Any]:
    rows_seen = 0
    rows_written = 0
    cursor_end_ms = int(start_ms)
    windows = _iter_windows(start_ms, end_ms, ORDER_TRADE_QUERY_WINDOW_MS)
    for window_start, window_end in windows:
        res = get_account_trades(
            account,
            symbol,
            start_time_ms=window_start,
            end_time_ms=window_end,
            limit=QUERY_LIMIT,
            priority=REQUEST_PRIORITY_LOW,
        )
        _sleep_between_requests(request_sleep_secs)
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


def _sync_income(account: str, start_ms: int, end_ms: int, sync_ms: int, request_sleep_secs: float) -> dict[str, Any]:
    income_seen = 0
    income_written = 0
    transfers_seen = 0
    transfers_written = 0
    active_symbols: set[str] = set()
    cursor_end_ms = int(start_ms)
    windows = _iter_windows(start_ms, end_ms, INCOME_QUERY_WINDOW_MS)
    for window_start, window_end in windows:
        res = get_income_history(
            account,
            start_time_ms=window_start,
            end_time_ms=window_end,
            limit=QUERY_LIMIT,
            priority=REQUEST_PRIORITY_LOW,
        )
        _sleep_between_requests(request_sleep_secs)
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
        active_symbols.update(_income_symbols(income_rows))
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
        "active_symbols": sorted(active_symbols),
        "cursor_end_ms": int(end_ms),
        "windows": len(windows),
    }


def _required_float(raw: dict[str, Any], key: str, *, context: str) -> float:
    if key not in raw:
        raise ValueError(f"missing {key}: {context}")
    try:
        return float(raw.get(key))
    except Exception as exc:
        raise ValueError(f"invalid {key}: {context} value={raw.get(key)!r}") from exc


def _balance_snapshot_rows(account_status: dict[str, Any], sync_ms: int) -> list[dict[str, Any]]:
    raw = account_status.get("raw")
    if not isinstance(raw, dict):
        raise ValueError("account status raw must be object")
    assets = raw.get("assets")
    if not isinstance(assets, list):
        raise ValueError("account status raw.assets must be list")
    rows: list[dict[str, Any]] = []
    for idx, asset_row in enumerate(assets):
        if not isinstance(asset_row, dict):
            raise ValueError(f"account status asset row must be object: idx={idx}")
        asset = str(asset_row.get("asset") or "").upper().strip()
        if not asset:
            raise ValueError(f"account status asset missing asset: idx={idx}")
        rows.append(
            {
                "asset": asset,
                "time_ms": int(sync_ms),
                "wallet_balance": _required_float(asset_row, "walletBalance", context=asset),
                "available_balance": _required_float(asset_row, "availableBalance", context=asset),
                "margin_balance": _required_float(asset_row, "marginBalance", context=asset),
                "unrealized_profit": _required_float(asset_row, "unrealizedProfit", context=asset),
                "raw": asset_row,
            }
        )
    return rows


def _sync_balance_snapshots(account: str, sync_ms: int, request_sleep_secs: float) -> dict[str, Any]:
    res = get_account_status(account)
    _sleep_between_requests(request_sleep_secs)
    if not res["ok"]:
        return {"ok": False, "reason": res["reason"], "rows_seen": 0, "rows_written": 0}
    try:
        rows = _balance_snapshot_rows(res["data"], sync_ms)
    except Exception as exc:
        return {"ok": False, "reason": str(exc), "rows_seen": 0, "rows_written": 0}
    rows_written = _write_history_rows(account, SOURCE_BALANCE_SNAPSHOTS, rows, sync_ms)
    return {
        "ok": True,
        "reason": "",
        "rows_seen": len(rows),
        "rows_written": rows_written,
        "assets": sorted(row["asset"] for row in rows),
    }


def sync_account_history(
    account: str,
    *,
    symbols: list[str] | None = None,
    symbol_files: list[str] | None = None,
    lookback_hours: int = 24,
    overlap_minutes: int = 10,
    end_ms: int | None = None,
    bootstrap: bool = False,
    request_sleep_secs: float = DEFAULT_REQUEST_SLEEP_SECS,
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
    explicit_symbol_set = _clean_usdt_symbols(explicit_symbols)
    results: dict[str, Any] = {
        "ok": True,
        "account": account_key,
        "start_ms": default_start_ms,
        "requested_start_ms": requested_start_ms,
        "exchange_history_start_ms": floor_ms,
        "sync_mode": sync_mode,
        "bootstrap": bool(bootstrap),
        "request_priority": REQUEST_PRIORITY_LOW,
        "request_sleep_secs": float(request_sleep_secs),
        "end_ms": final_end_ms,
        "active_sync_symbols": [],
        "explicit_symbols": sorted(explicit_symbol_set),
        "historical_symbols": [],
        "symbols": [],
        "discovery_errors": [],
        "sources": {
            SOURCE_ORDERS: {},
            SOURCE_TRADES: {},
            SOURCE_INCOME: {},
            SOURCE_TRANSFERS: {},
            SOURCE_BALANCE_SNAPSHOTS: {},
        },
        "errors": [],
    }

    balance_res = _sync_balance_snapshots(account_key, sync_ms, request_sleep_secs)
    results["sources"][SOURCE_BALANCE_SNAPSHOTS][ACCOUNT_KEY] = balance_res
    if not balance_res["ok"]:
        results["ok"] = False
        results["errors"].append(f"{SOURCE_BALANCE_SNAPSHOTS}: {balance_res['reason']}")
        return results

    income_start = _source_start_ms(state_for_start, SOURCE_INCOME, None, default_start_ms, overlap_ms)
    income_res = _sync_income(account_key, income_start, final_end_ms, sync_ms, request_sleep_secs)
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
        "active_symbols": income_res.get("active_symbols", []),
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
        results["historical_symbols"] = _merge_symbol_index(account_key, set())
        results["symbols"] = []
        save_json_file_atomic(_sync_state_path(account_key), state, indent=2)
        return results

    active_symbol_set = _clean_usdt_symbols(set(income_res.get("active_symbols") or []))
    sync_symbols = sorted(active_symbol_set | explicit_symbol_set)
    results["active_sync_symbols"] = sync_symbols
    results["symbols"] = sync_symbols
    results["historical_symbols"] = _merge_symbol_index(account_key, active_symbol_set | explicit_symbol_set)

    for symbol in sync_symbols:
        order_start = _source_start_ms(state_for_start, SOURCE_ORDERS, symbol, default_start_ms, overlap_ms)
        order_res = _sync_orders(account_key, symbol, order_start, final_end_ms, sync_ms, request_sleep_secs)
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
        trade_res = _sync_trades(account_key, symbol, trade_start, final_end_ms, sync_ms, request_sleep_secs)
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

    save_json_file_atomic(_sync_state_path(account_key), state, indent=2)
    return results


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync Binance account exchange history into local state.")
    parser.add_argument("--account", action="append", required=True, help="Account key. Can be repeated; accounts run serially.")
    parser.add_argument("--symbol", action="append", default=[], help="Optional USDT symbol. Can be repeated.")
    parser.add_argument("--symbol-file", action="append", default=[], help="Optional JSON list or newline text file of USDT symbols. Can be repeated.")
    parser.add_argument("--lookback-hours", type=int, default=24)
    parser.add_argument("--overlap-minutes", type=int, default=10)
    parser.add_argument("--request-sleep-secs", type=float, default=DEFAULT_REQUEST_SLEEP_SECS, help="Sleep after each Binance history request.")
    parser.add_argument("--account-sleep-secs", type=float, default=30.0, help="Sleep between accounts when multiple --account values are provided.")
    parser.add_argument("--bootstrap", action="store_true", help="Backfill from exchange_history_start_time and ignore prior per-source cursors.")
    parser.add_argument("--loop", action="store_true", help="Run continuously instead of one sync pass.")
    parser.add_argument("--interval-secs", type=int, default=300, help="Loop sleep interval in seconds.")
    parser.add_argument("--max-iterations", type=int, default=0, help="Loop iteration limit. 0 means unlimited.")
    return parser.parse_args()


def _run_account_once(args: argparse.Namespace, account: str) -> dict[str, Any]:
    return sync_account_history(
        account,
        symbols=args.symbol,
        symbol_files=args.symbol_file,
        lookback_hours=args.lookback_hours,
        overlap_minutes=args.overlap_minutes,
        bootstrap=args.bootstrap,
        request_sleep_secs=args.request_sleep_secs,
    )


def _run_once(args: argparse.Namespace) -> dict[str, Any]:
    accounts = [str(account or "").strip() for account in args.account]
    if any(not account for account in accounts):
        raise ValueError("--account must not be empty")
    if len(accounts) == 1:
        return _run_account_once(args, accounts[0])
    results: list[dict[str, Any]] = []
    for idx, account in enumerate(accounts):
        results.append(_run_account_once(args, account))
        if idx < len(accounts) - 1 and args.account_sleep_secs > 0:
            time.sleep(float(args.account_sleep_secs))
    return {
        "ok": all(bool(row.get("ok")) for row in results),
        "mode": "multi_account_serial",
        "accounts": accounts,
        "account_sleep_secs": float(args.account_sleep_secs),
        "results": results,
    }


def _print_result(result: dict[str, Any]) -> None:
    print(json.dumps(result, ensure_ascii=False, sort_keys=True), flush=True)


def main() -> int:
    args = _parse_args()
    if args.interval_secs <= 0:
        raise ValueError("--interval-secs must be positive")
    if args.max_iterations < 0:
        raise ValueError("--max-iterations must be >= 0")
    if args.request_sleep_secs < 0:
        raise ValueError("--request-sleep-secs must be >= 0")
    if args.account_sleep_secs < 0:
        raise ValueError("--account-sleep-secs must be >= 0")
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
