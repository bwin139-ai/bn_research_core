#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import logging
import math
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set

import requests

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from core.live.rate_limit_guard import record_binance_rest_ban
from core.live.binance_rest_gateway import (
    REQUEST_PRIORITY_LOW,
    REQUEST_PRIORITY_NORMAL,
    request_futures_public,
)

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except Exception as e:
    raise SystemExit(
        "Missing dependency: pyarrow. Install with: pip install -U pyarrow"
    ) from e


BASE_URL = "https://fapi.binance.com"  # USDⓈ-M Futures
INTERVAL = "1m"
INTERVAL_MS = 60_000
PRICE_SOURCE_CONTRACT = "contract"
PRICE_SOURCE_INDEX = "index"
HTTP_429_GLOBAL_COOLDOWN_SECS = 180.0
HTTP_429_GLOBAL_COOLDOWN_WINDOW_SECS = 180.0
_HTTP_429_SEEN_AT: List[float] = []


class BinanceRestIpBan(RuntimeError):
    pass


class BinanceRestNonRetryableHttpError(RuntimeError):
    pass


def _record_http_429_seen(now_epoch: float | None = None) -> int:
    if now_epoch is None:
        now_epoch = time.time()
    cutoff = float(now_epoch) - HTTP_429_GLOBAL_COOLDOWN_WINDOW_SECS
    recent = [ts for ts in _HTTP_429_SEEN_AT if ts >= cutoff]
    _HTTP_429_SEEN_AT[:] = recent
    _HTTP_429_SEEN_AT.append(float(now_epoch))
    return len(_HTTP_429_SEEN_AT)


def _response_error_body_text(resp: requests.Response) -> str:
    try:
        data = resp.json()
    except Exception:
        data = None
    if isinstance(data, dict):
        code = data.get("code")
        msg = data.get("msg")
        parts = []
        if code is not None:
            parts.append(f"code={code}")
        if msg is not None:
            parts.append(f"msg={msg}")
        if parts:
            return " | ".join(parts)
        try:
            return json.dumps(data, ensure_ascii=False, sort_keys=True)
        except Exception:
            pass
    text = (resp.text or "").strip()
    return text[:500]


def _is_static_index_price_400(url: str, body_text: str) -> bool:
    if "indexPriceKlines" not in str(url):
        return False
    text = str(body_text or "").lower()
    static_markers = [
        "invalid symbol",
        "invalid pair",
        "symbol is invalid",
        "pair is invalid",
        "not support",
        "not supported",
        "index price",
        "no index",
        "premarket",
        "pre-market",
        "pre market",
    ]
    return any(marker in text for marker in static_markers)


def _is_present_number(v: Any) -> bool:
    if v is None:
        return False
    if isinstance(v, float) and math.isnan(v):
        return False
    return True


# ----------------------------
# helpers
# ----------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso() -> str:
    return utc_now().isoformat()


def parse_utc_date(s: str) -> datetime:
    dt = datetime.strptime(s, "%Y-%m-%d")
    return dt.replace(tzinfo=timezone.utc)


def stable_sync_end_ms(now_ms: Optional[int] = None, stable_lag_minutes: int = 3) -> int:
    if now_ms is None:
        now_ms = int(utc_now().timestamp() * 1000)
    return floor_to_minute_ms(now_ms) - int(stable_lag_minutes) * INTERVAL_MS


def date_range_to_ms(start_date: Optional[str], end_date: Optional[str], days: int) -> tuple[int, int]:
    if start_date and end_date:
        start_dt = parse_utc_date(start_date)
        end_dt = parse_utc_date(end_date) + timedelta(days=1)
        return floor_to_minute_ms(int(start_dt.timestamp() * 1000)), floor_to_minute_ms(int(end_dt.timestamp() * 1000))
    end_ms = stable_sync_end_ms()
    start_ms = floor_to_minute_ms(end_ms - int(days) * 24 * 60 * 60 * 1000)
    return start_ms, end_ms


def floor_to_minute_ms(ts_ms: int) -> int:
    return ts_ms - (ts_ms % INTERVAL_MS)


def month_key_from_ms(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return f"{dt.year:04d}-{dt.month:02d}"


def ensure_dir(p: str) -> None:
    os.makedirs(p, exist_ok=True)


def atomic_write_bytes(path: str, data: bytes) -> None:
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


def load_json(path: str, default):
    if not os.path.exists(path):
        return default
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: str, obj) -> None:
    ensure_dir(os.path.dirname(path) or ".")
    atomic_write_bytes(
        path, json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")
    )


def load_symbol_lines(path: str) -> List[str]:
    if not path or not os.path.exists(path):
        return []
    out: List[str] = []
    seen: Set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            s = raw.strip().upper()
            if not s or s.startswith("#"):
                continue
            if s not in seen:
                seen.add(s)
                out.append(s)
    return out


def save_symbol_lines(path: str, symbols: List[str]) -> None:
    ensure_dir(os.path.dirname(path) or ".")
    uniq_sorted = sorted({str(x).strip().upper() for x in symbols if str(x).strip()})
    payload = "\n".join(uniq_sorted)
    if payload:
        payload += "\n"
    atomic_write_bytes(path, payload.encode("utf-8"))


def load_confirmed_delisted_records(path: str) -> List[Dict[str, Any]]:
    if not path or not os.path.exists(path):
        return []
    data = load_json(path, default=[])
    if not isinstance(data, list):
        raise SystemExit(f"confirmed_delisted file must be a JSON array: {path}")

    out: List[Dict[str, Any]] = []
    seen: Set[str] = set()
    for raw in data:
        if not isinstance(raw, dict):
            raise SystemExit(f"confirmed_delisted record must be an object: {path}")
        symbol = str(raw.get("symbol", "")).strip().upper()
        if not symbol:
            raise SystemExit(f"confirmed_delisted record missing symbol: {path}")
        if symbol in seen:
            raise SystemExit(f"duplicate confirmed_delisted symbol: {symbol}")
        seen.add(symbol)
        out.append(
            {
                "symbol": symbol,
                "last_open_time_ms": int(raw["last_open_time_ms"]) if raw.get("last_open_time_ms") is not None else None,
                "last_open_time_bj": raw.get("last_open_time_bj"),
                "reason": raw.get("reason", ""),
                "confirmed_utc": raw.get("confirmed_utc"),
                "updated_utc": raw.get("updated_utc"),
            }
        )
    return out


def save_confirmed_delisted_records(path: str, records: List[Dict[str, Any]]) -> None:
    ensure_dir(os.path.dirname(path) or ".")
    normalized = sorted(records, key=lambda x: str(x["symbol"]).upper())
    save_json(path, normalized)


def list_local_symbol_dirs(data_dir: str) -> List[str]:
    if not os.path.isdir(data_dir):
        return []
    out: List[str] = []
    for name in sorted(os.listdir(data_dir)):
        path = os.path.join(data_dir, name)
        if not os.path.isdir(path):
            continue
        try:
            has_parquet = any(fn.endswith(".parquet") for fn in os.listdir(path))
        except Exception:
            has_parquet = False
        if has_parquet:
            out.append(name.upper())
    return out


def resolve_last_open_time_ms(data_dir: str, state: Dict, symbol: str) -> Optional[int]:
    per = (state.get("per_symbol") or {}).get(symbol) or {}
    if per.get("last_open_time_ms") is not None:
        return int(per["last_open_time_ms"])
    return infer_last_open_from_local(data_dir, symbol)


def build_delisted_record(
    *,
    symbol: str,
    data_dir: str,
    state: Dict,
    reason: str,
    confirmed_utc: Optional[str],
) -> Dict[str, Any]:
    last_open_time_ms = resolve_last_open_time_ms(data_dir, state, symbol)
    now_utc = utc_iso()
    return {
        "symbol": symbol,
        "last_open_time_ms": last_open_time_ms,
        "last_open_time_bj": (
            datetime.fromtimestamp(last_open_time_ms / 1000, tz=timezone.utc)
            .astimezone(timezone(timedelta(hours=8)))
            .strftime("%Y-%m-%d %H:%M:%S")
            if last_open_time_ms is not None
            else None
        ),
        "reason": reason,
        "confirmed_utc": confirmed_utc or now_utc,
        "updated_utc": now_utc,
    }


def refresh_confirmed_delisted(
    live_symbols: List[str],
    data_dir: str,
    state: Dict,
    confirmed_delisted_path: str,
    force_include_path: str,
    delisted_status_path: str,
    stale_hours: int,
) -> Dict:
    """
    Hard rules:
    1) --symbols has highest priority (handled in main).
    2) symbols_force_include.txt has higher priority than confirmed_delisted JSON.
    3) confirmed delisted only affects symbol selection / audit scope; it must not move local parquet data.
    """
    live_set = {s.upper() for s in live_symbols}
    local_dirs = list_local_symbol_dirs(data_dir)
    require_usdt_symbols(local_dirs, f"local data_dir {data_dir}")
    local_set = set(local_dirs)

    force_include = load_symbol_lines(force_include_path)
    force_include = require_usdt_symbols(force_include, force_include_path)
    force_set = set(force_include)

    existing_confirmed_raw = load_confirmed_delisted_records(confirmed_delisted_path)
    require_usdt_symbols(
        [str(row["symbol"]) for row in existing_confirmed_raw],
        confirmed_delisted_path,
    )
    existing_confirmed_raw_set = {str(row["symbol"]).upper() for row in existing_confirmed_raw}
    existing_confirmed_by_symbol = {
        str(row["symbol"]).upper(): row for row in existing_confirmed_raw
    }

    conflict_force_vs_confirmed = sorted(existing_confirmed_raw_set & force_set)
    stale_threshold_ms = int(stale_hours) * 60 * 60 * 1000
    latest_sync_end_ms = stable_sync_end_ms()

    auto_suspected_delisted: List[Dict[str, Any]] = []
    auto_confirmed_symbols: Set[str] = set()
    for sym in sorted((local_set - live_set) - force_set):
        last_open_time_ms = resolve_last_open_time_ms(data_dir, state, sym)
        lag_ms = None if last_open_time_ms is None else max(0, latest_sync_end_ms - last_open_time_ms)
        record = {
            "symbol": sym,
            "last_open_time_ms": last_open_time_ms,
            "last_open_time_bj": (
                datetime.fromtimestamp(last_open_time_ms / 1000, tz=timezone.utc)
                .astimezone(timezone(timedelta(hours=8)))
                .strftime("%Y-%m-%d %H:%M:%S")
                if last_open_time_ms is not None
                else None
            ),
            "stale_hours": None if lag_ms is None else round(lag_ms / 3600000, 3),
        }
        auto_suspected_delisted.append(record)
        if lag_ms is not None and lag_ms > stale_threshold_ms:
            auto_confirmed_symbols.add(sym)

    effective_confirmed_symbols = sorted(
        (existing_confirmed_raw_set | auto_confirmed_symbols) - force_set - live_set
    )
    effective_confirmed_records = [
        build_delisted_record(
            symbol=sym,
            data_dir=data_dir,
            state=state,
            reason="not_in_live_symbols_and_stale_over_threshold",
            confirmed_utc=(existing_confirmed_by_symbol.get(sym) or {}).get("confirmed_utc"),
        )
        for sym in effective_confirmed_symbols
    ]

    save_confirmed_delisted_records(confirmed_delisted_path, effective_confirmed_records)

    status = {
        "updated_utc": utc_iso(),
        "data_dir": data_dir,
        "confirmed_delisted_path": confirmed_delisted_path,
        "force_include_path": force_include_path,
        "latest_sync_end_ms": latest_sync_end_ms,
        "latest_sync_end_bj": datetime.fromtimestamp(latest_sync_end_ms / 1000, tz=timezone.utc)
        .astimezone(timezone(timedelta(hours=8)))
        .strftime("%Y-%m-%d %H:%M:%S"),
        "stale_hours_threshold": int(stale_hours),
        "live_symbols_count": len(live_symbols),
        "local_symbol_dirs_count": len(local_dirs),
        "force_include_count": len(force_include),
        "force_include_symbols": force_include,
        "confirmed_delisted_count_raw": len(existing_confirmed_raw),
        "confirmed_delisted_raw": existing_confirmed_raw,
        "confirmed_delisted_count_effective": len(effective_confirmed_records),
        "confirmed_delisted": effective_confirmed_records,
        "conflict_force_vs_confirmed_count": len(conflict_force_vs_confirmed),
        "conflict_force_vs_confirmed": conflict_force_vs_confirmed,
        "auto_suspected_delisted_count": len(auto_suspected_delisted),
        "auto_suspected_delisted": auto_suspected_delisted,
        "auto_confirmed_delisted_count": len(auto_confirmed_symbols),
        "auto_confirmed_delisted_symbols": sorted(auto_confirmed_symbols),
    }
    save_json(delisted_status_path, status)
    return status


# ----------------------------
# state
# ----------------------------
@dataclass
class StorePaths:
    data_dir: str
    state_path: str


def load_state(state_path: str, price_source: str) -> Dict:
    st = load_json(state_path, default={})
    st.setdefault("version", 1)
    st.setdefault("base_url", BASE_URL)
    st.setdefault("interval", INTERVAL)
    st.setdefault("price_source", price_source)
    st.setdefault(
        "per_symbol", {}
    )  # symbol -> {"last_open_time_ms": int, "updated_utc": str}
    return st


def update_symbol_state(st: Dict, symbol: str, last_open_time_ms: int) -> None:
    st["per_symbol"][symbol] = {
        "last_open_time_ms": int(last_open_time_ms),
        "updated_utc": utc_iso(),
    }
    st["updated_utc"] = utc_iso()


# ----------------------------
# binance REST
# ----------------------------
def _gateway_endpoint_from_url(url: str) -> str:
    prefix = f"{BASE_URL}/fapi/v1/"
    url_key = str(url or "").strip()
    if not url_key.startswith(prefix):
        raise ValueError(f"unsupported Binance futures URL: {url}")
    endpoint = url_key[len(prefix):].strip()
    if not endpoint:
        raise ValueError(f"empty Binance futures endpoint: {url}")
    return endpoint


def http_get_json(
    session: requests.Session,
    url: str,
    params: Dict,
    timeout: int = 20,
    max_retry: int = 8,
    priority: str = REQUEST_PRIORITY_LOW,
    source: str | None = None,
):
    backoff = 1.0
    endpoint = _gateway_endpoint_from_url(url)
    source_key = str(source or f"klines_1m_store.{endpoint}").strip()
    for attempt in range(1, max_retry + 1):
        try:
            r = request_futures_public(
                source=source_key,
                endpoint=endpoint,
                params=params,
                priority=priority,
                timeout=float(timeout),
                session=session,
            )
            if r.status_code == 418:
                retry_after = r.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else 600.0
                ban_until_utc_ms = int(time.time() * 1000 + sleep_s * 1000)
                record_binance_rest_ban(
                    ban_until_utc_ms=ban_until_utc_ms,
                    source="klines_1m_store",
                    status_code=r.status_code,
                    reason="binance_rest_ip_ban",
                    url=url,
                    params=params,
                )
                logging.error(
                    "rate limited (%s). stop task and record ban until %s. url=%s params=%s",
                    r.status_code,
                    datetime.fromtimestamp(ban_until_utc_ms / 1000.0, tz=timezone.utc).isoformat(),
                    url,
                    params,
                )
                raise BinanceRestIpBan(f"binance REST IP banned until {ban_until_utc_ms}")
            if r.status_code == 429:
                recent_429_count = _record_http_429_seen()
                retry_after = r.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else backoff
                if recent_429_count >= 2:
                    sleep_s = max(sleep_s, HTTP_429_GLOBAL_COOLDOWN_SECS)
                logging.warning(
                    "rate limited (%s). recent_429_count=%s sleep %.1fs. url=%s params=%s",
                    r.status_code,
                    recent_429_count,
                    sleep_s,
                    url,
                    params,
                )
                time.sleep(sleep_s)
                backoff = min(backoff * 2, 60.0)
                continue
            if 400 <= r.status_code < 500:
                body_text = _response_error_body_text(r)
                logging.warning(
                    "client error (%s). url=%s params=%s body=%s",
                    r.status_code,
                    url,
                    params,
                    body_text,
                )
                if r.status_code == 400 and _is_static_index_price_400(url, body_text):
                    raise BinanceRestNonRetryableHttpError(
                        f"non-retryable client error status={r.status_code} body={body_text}"
                    )
                if attempt == max_retry:
                    r.raise_for_status()
                time.sleep(10.0)
                continue
            if 500 <= r.status_code < 600:
                logging.warning(
                    "server error (%s). sleep %.1fs. url=%s",
                    r.status_code,
                    backoff,
                    url,
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, 60.0)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            if isinstance(e, BinanceRestIpBan):
                raise
            if isinstance(e, BinanceRestNonRetryableHttpError):
                raise
            if attempt == max_retry:
                raise
            logging.warning(
                "http error attempt=%s/%s: %s. sleep %.1fs",
                attempt,
                max_retry,
                e,
                backoff,
            )
            time.sleep(backoff)
            backoff = min(backoff * 2, 60.0)


def fetch_exchange_info(session: requests.Session) -> Dict:
    url = f"{BASE_URL}/fapi/v1/exchangeInfo"
    return http_get_json(
        session,
        url,
        params={},
        priority=REQUEST_PRIORITY_NORMAL,
        source="klines_1m_store.exchangeInfo",
    )


def require_usdt_symbols(symbols: List[str], source: str) -> List[str]:
    normalized = [str(s).strip().upper() for s in symbols if str(s).strip()]
    invalid = sorted({s for s in normalized if not s.endswith("USDT")})
    if invalid:
        raise SystemExit(
            f"{source} contains non-USDT symbols: {','.join(invalid)}"
        )
    return normalized


def list_usdt_perpetual_symbols(session: requests.Session) -> List[str]:
    info = fetch_exchange_info(session)
    out = []
    for s in info.get("symbols", []):
        # 只取交易中 + 永续
        if s.get("status") != "TRADING":
            continue
        if s.get("contractType") != "PERPETUAL":
            continue

        sym = str(s.get("symbol", "")).strip().upper()
        quote = str(s.get("quoteAsset", "")).strip().upper()

        if quote != "USDT" or not sym.endswith("USDT"):
            continue

        out.append(sym)

    out.sort()
    return out


def fetch_klines(
    session: requests.Session,
    symbol: str,
    start_ms: int,
    end_ms: Optional[int],
    limit: int,
    price_source: str,
) -> List[List]:
    if price_source == PRICE_SOURCE_CONTRACT:
        url = f"{BASE_URL}/fapi/v1/klines"
        params = {
            "symbol": symbol,
            "interval": INTERVAL,
            "startTime": int(start_ms),
            "limit": int(limit),
        }
    elif price_source == PRICE_SOURCE_INDEX:
        url = f"{BASE_URL}/fapi/v1/indexPriceKlines"
        params = {
            "pair": symbol,
            "interval": INTERVAL,
            "startTime": int(start_ms),
            "limit": int(limit),
        }
    else:
        raise ValueError(f"unsupported price_source: {price_source}")

    if end_ms is not None:
        params["endTime"] = int(end_ms)
    return http_get_json(
        session,
        url,
        params=params,
        priority=REQUEST_PRIORITY_LOW,
        source=f"klines_1m_store.{price_source}.{url.rsplit('/', 1)[-1]}",
    )


# ----------------------------
# parquet IO (month shards)
# ----------------------------
SCHEMA_CONTRACT = pa.schema(
    [
        ("open_time_ms", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("quote_asset_volume", pa.float64()),
        ("high_idx", pa.float64()),
        ("low_idx", pa.float64()),
        ("close_idx", pa.float64()),
    ]
)

SCHEMA_INDEX = pa.schema(
    [
        ("open_time_ms", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
    ]
)


def rows_to_table(rows: List[List], price_source: str) -> pa.Table:
    # Binance kline array (contract):
    # [ open_time, open, high, low, close, volume, close_time,
    #   quote_asset_volume, number_of_trades, taker_buy_base, taker_buy_quote, ignore ]
    # Binance index price kline array keeps the same OHLC layout, but the non-price
    # fields are ignored by this store.
    open_time = [int(r[0]) for r in rows]
    open_ = [float(r[1]) for r in rows]
    high = [float(r[2]) for r in rows]
    low = [float(r[3]) for r in rows]
    close = [float(r[4]) for r in rows]

    if price_source == PRICE_SOURCE_INDEX:
        return pa.Table.from_arrays(
            [
                pa.array(open_time, type=pa.int64()),
                pa.array(open_, type=pa.float64()),
                pa.array(high, type=pa.float64()),
                pa.array(low, type=pa.float64()),
                pa.array(close, type=pa.float64()),
            ],
            schema=SCHEMA_INDEX,
        )

    quote_vol = [float(r[7]) if len(r) > 7 else 0.0 for r in rows]
    high_idx = [float(r[8]) if len(r) > 8 and r[8] is not None else None for r in rows]
    low_idx = [float(r[9]) if len(r) > 9 and r[9] is not None else None for r in rows]
    close_idx = [float(r[10]) if len(r) > 10 and r[10] is not None else None for r in rows]
    return pa.Table.from_arrays(
        [
            pa.array(open_time, type=pa.int64()),
            pa.array(open_, type=pa.float64()),
            pa.array(high, type=pa.float64()),
            pa.array(low, type=pa.float64()),
            pa.array(close, type=pa.float64()),
            pa.array(quote_vol, type=pa.float64()),
            pa.array(high_idx, type=pa.float64()),
            pa.array(low_idx, type=pa.float64()),
            pa.array(close_idx, type=pa.float64()),
        ],
        schema=SCHEMA_CONTRACT,
    )


def month_file(data_dir: str, symbol: str, month_key: str) -> str:
    return os.path.join(data_dir, symbol, f"{month_key}.parquet")


def merge_write_month(
    data_dir: str, symbol: str, month_key: str, new_rows: List[List], price_source: str
) -> int:
    """
    Merge by open_time_ms (dedup), sort asc, write shard.
    Strict schema: contract main table keeps 6 core columns plus 3 optional idx columns; index source uses a 5-column OHLC schema.
    """
    ensure_dir(os.path.join(data_dir, symbol))
    fpath = month_file(data_dir, symbol, month_key)

    if price_source == PRICE_SOURCE_CONTRACT:
        # Fresh contract klines must not pollute idx columns with non-index fields.
        merged: Dict[int, List] = {
            int(r[0]): [
                int(r[0]),
                float(r[1]),
                float(r[2]),
                float(r[3]),
                float(r[4]),
                0.0,
                0,
                float(r[7]) if len(r) > 7 else 0.0,
                None,
                None,
                None,
            ]
            for r in new_rows
        }
    else:
        # Keep a Binance-like sparse row shape so rows_to_table can consume both
        # fresh API rows and locally reconstructed rows with the same field indices.
        merged = {int(r[0]): r for r in new_rows}

    if os.path.exists(fpath):
        if price_source == PRICE_SOURCE_INDEX:
            tbl = pq.read_table(
                fpath,
                columns=[
                    "open_time_ms",
                    "open",
                    "high",
                    "low",
                    "close",
                ],
            )

            ot = tbl.column("open_time_ms").to_pylist()
            o = tbl.column("open").to_pylist()
            h = tbl.column("high").to_pylist()
            low_ = tbl.column("low").to_pylist()
            c = tbl.column("close").to_pylist()

            for i in range(len(ot)):
                k = int(ot[i])
                if k in merged:
                    continue
                merged[k] = [
                    k,
                    o[i],
                    h[i],
                    low_[i],
                    c[i],
                ]
        else:
            contract_cols = [
                "open_time_ms",
                "open",
                "high",
                "low",
                "close",
                "quote_asset_volume",
            ]
            idx_cols = ["high_idx", "low_idx", "close_idx"]
            available_cols = contract_cols + [c for c in idx_cols if c in pq.read_schema(fpath).names]
            tbl = pq.read_table(fpath, columns=available_cols)

            ot = tbl.column("open_time_ms").to_pylist()
            o = tbl.column("open").to_pylist()
            h = tbl.column("high").to_pylist()
            low_ = tbl.column("low").to_pylist()
            c = tbl.column("close").to_pylist()
            qv = tbl.column("quote_asset_volume").to_pylist()
            hi_idx = tbl.column("high_idx").to_pylist() if "high_idx" in tbl.column_names else [None] * len(ot)
            lo_idx = tbl.column("low_idx").to_pylist() if "low_idx" in tbl.column_names else [None] * len(ot)
            cl_idx = tbl.column("close_idx").to_pylist() if "close_idx" in tbl.column_names else [None] * len(ot)

            for i in range(len(ot)):
                k = int(ot[i])
                if k in merged:
                    continue
                merged[k] = [
                    k,
                    o[i],
                    h[i],
                    low_[i],
                    c[i],
                    0.0,
                    0,
                    qv[i],
                    hi_idx[i],
                    lo_idx[i],
                    cl_idx[i],
                ]

    keys = sorted(merged.keys())
    tbl_out = rows_to_table([merged[k] for k in keys], price_source)

    tmp = fpath + ".tmp"
    pq.write_table(tbl_out, tmp, compression="zstd")
    os.replace(tmp, fpath)
    return tbl_out.num_rows



def infer_last_open_from_local(data_dir: str, symbol: str) -> Optional[int]:
    sym_dir = os.path.join(data_dir, symbol)
    if not os.path.isdir(sym_dir):
        return None

    parquet_files = sorted(fn for fn in os.listdir(sym_dir) if fn.endswith(".parquet"))
    if not parquet_files:
        return None

    fp = os.path.join(sym_dir, parquet_files[-1])
    tbl = pq.read_table(fp, columns=["open_time_ms"])
    col = tbl.column("open_time_ms")
    if len(col) <= 0:
        return None

    return int(max(col.to_pylist()))


def infer_last_idx_open_from_local(data_dir: str, symbol: str) -> Optional[int]:
    sym_dir = os.path.join(data_dir, symbol)
    if not os.path.isdir(sym_dir):
        return None

    parquet_files = sorted(fn for fn in os.listdir(sym_dir) if fn.endswith(".parquet"))
    if not parquet_files:
        return None

    for fn in reversed(parquet_files):
        fp = os.path.join(sym_dir, fn)
        schema_names = pq.read_schema(fp).names
        idx_cols = ["high_idx", "low_idx", "close_idx"]
        if any(col not in schema_names for col in idx_cols):
            continue

        tbl = pq.read_table(fp, columns=["open_time_ms"] + idx_cols)
        ot = tbl.column("open_time_ms").to_pylist()
        hi = tbl.column("high_idx").to_pylist()
        lo = tbl.column("low_idx").to_pylist()
        cl = tbl.column("close_idx").to_pylist()

        for i in range(len(ot) - 1, -1, -1):
            if (
                _is_present_number(hi[i])
                and _is_present_number(lo[i])
                and _is_present_number(cl[i])
            ):
                return int(ot[i])
    return None


def infer_auto_augment_idx_window(
    data_dir: str,
    symbol: str,
    limit: int,
) -> Optional[tuple[int, int, int, Optional[int]]]:
    contract_last_open = infer_last_open_from_local(data_dir, symbol)
    if contract_last_open is None:
        return None

    end_ms = min(contract_last_open, stable_sync_end_ms())
    if end_ms <= 0:
        return None

    idx_last_open = infer_last_idx_open_from_local(data_dir, symbol)
    if idx_last_open is not None:
        start_ms = idx_last_open + INTERVAL_MS
    else:
        # Bootstrap only the recent tail when no local idx cursor exists.
        tail_span_ms = max(1, int(limit)) * INTERVAL_MS
        start_ms = max(0, end_ms - tail_span_ms + INTERVAL_MS)

    if start_ms > end_ms:
        return None
    return start_ms, end_ms, contract_last_open, idx_last_open

# ----------------------------
# backfill / sync
# ----------------------------
def backfill_symbol(
    session: requests.Session,
    symbol: str,
    days: int,
    data_dir: str,
    state: Dict,
    limit: int,
    sleep_ms: int,
    price_source: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> None:
    start_ms, end_ms = date_range_to_ms(start_date, end_date, days)

    buckets: Dict[str, List[List]] = {}
    cur = start_ms
    last_open = None

    while True:
        rows = fetch_klines(session, symbol, start_ms=cur, end_ms=end_ms, limit=limit, price_source=price_source)
        if not rows:
            break

        for r in rows:
            mk = month_key_from_ms(int(r[0]))
            buckets.setdefault(mk, []).append(r)

        last_open = int(rows[-1][0])
        cur = last_open + INTERVAL_MS

        if cur > end_ms:
            break

        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

    if not buckets:
        logging.info("[backfill] %s: no data returned", symbol)
        return

    total_written = 0
    for mk in sorted(buckets.keys()):
        n = merge_write_month(data_dir, symbol, mk, buckets[mk], price_source)
        total_written += n

    if last_open is not None:
        update_symbol_state(state, symbol, last_open)

    logging.info(
        "[backfill] %s: months=%s last_open=%s", symbol, len(buckets), last_open
    )


def sync_symbol(
    session: requests.Session,
    symbol: str,
    data_dir: str,
    state: Dict,
    limit: int,
    sleep_ms: int,
    price_source: str,
) -> None:
    per = state.get("per_symbol", {}).get(symbol)
    if not per or "last_open_time_ms" not in per:
        logging.info("[sync] %s: no state; backfill first", symbol)
        return

    last = int(per["last_open_time_ms"])
    cur = last + INTERVAL_MS
    end_ms = stable_sync_end_ms()

    if cur > end_ms:
        return  # up to date

    buckets: Dict[str, List[List]] = {}
    last_open = None

    while True:
        rows = fetch_klines(session, symbol, start_ms=cur, end_ms=end_ms, limit=limit, price_source=price_source)
        # Apply sleep after EVERY HTTP request. In sync mode, most symbols finish in a single request;
        # the old logic slept only between pages, causing burst requests across symbols
        # and triggering 429 / -1003 rate limits.
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

        if not rows:
            break

        for r in rows:
            mk = month_key_from_ms(int(r[0]))
            buckets.setdefault(mk, []).append(r)

        last_open = int(rows[-1][0])
        cur = last_open + INTERVAL_MS

        if cur > end_ms:
            break
    if not buckets:
        return

    for mk in sorted(buckets.keys()):
        merge_write_month(data_dir, symbol, mk, buckets[mk], price_source)

    if last_open is not None:
        update_symbol_state(state, symbol, last_open)

    logging.info("[sync] %s: updated to %s", symbol, last_open)


def merge_contract_and_index_month(
    data_dir: str,
    symbol: str,
    month_key: str,
    idx_rows: List[List],
) -> int:
    fpath = month_file(data_dir, symbol, month_key)
    if not os.path.exists(fpath):
        return 0

    schema_names = pq.read_schema(fpath).names
    base_cols = ["open_time_ms", "open", "high", "low", "close", "quote_asset_volume"]
    idx_cols = [c for c in ["high_idx", "low_idx", "close_idx"] if c in schema_names]
    tbl = pq.read_table(fpath, columns=base_cols + idx_cols)

    ot = tbl.column("open_time_ms").to_pylist()
    o = tbl.column("open").to_pylist()
    h = tbl.column("high").to_pylist()
    low_ = tbl.column("low").to_pylist()
    c = tbl.column("close").to_pylist()
    qv = tbl.column("quote_asset_volume").to_pylist()
    hi_idx_old = tbl.column("high_idx").to_pylist() if "high_idx" in tbl.column_names else [None] * len(ot)
    lo_idx_old = tbl.column("low_idx").to_pylist() if "low_idx" in tbl.column_names else [None] * len(ot)
    cl_idx_old = tbl.column("close_idx").to_pylist() if "close_idx" in tbl.column_names else [None] * len(ot)

    idx_map = {int(r[0]): (float(r[2]), float(r[3]), float(r[4])) for r in idx_rows}
    merged_rows = []
    for i in range(len(ot)):
        k = int(ot[i])
        hi_idx, lo_idx, cl_idx = idx_map.get(k, (hi_idx_old[i], lo_idx_old[i], cl_idx_old[i]))
        merged_rows.append([
            k, o[i], h[i], low_[i], c[i], 0.0, 0, qv[i], hi_idx, lo_idx, cl_idx
        ])

    tbl_out = rows_to_table(merged_rows, PRICE_SOURCE_CONTRACT)
    tmp = fpath + ".tmp"
    pq.write_table(tbl_out, tmp, compression="zstd")
    os.replace(tmp, fpath)
    return tbl_out.num_rows


def augment_idx_symbol(
    session: requests.Session,
    symbol: str,
    data_dir: str,
    limit: int,
    sleep_ms: int,
    start_date: Optional[str],
    end_date: Optional[str],
    days: int,
    days_explicit: bool,
) -> None:
    if start_date or end_date or days_explicit:
        start_ms, end_ms = date_range_to_ms(start_date, end_date, days)
        logging.info(
            "[augment-idx] %s: explicit_window start=%s end=%s",
            symbol,
            start_ms,
            end_ms,
        )
    else:
        auto_window = infer_auto_augment_idx_window(data_dir, symbol, limit)
        if auto_window is None:
            logging.info("[augment-idx] %s: no local contract tail to augment", symbol)
            return
        start_ms, end_ms, contract_last_open, idx_last_open = auto_window
        logging.info(
            "[augment-idx] %s: auto_window start=%s end=%s contract_last=%s idx_last=%s",
            symbol,
            start_ms,
            end_ms,
            contract_last_open,
            idx_last_open,
        )

    if start_ms > end_ms:
        logging.info("[augment-idx] %s: up to date", symbol)
        return

    buckets: Dict[str, List[List]] = {}
    cur = start_ms
    while True:
        rows = fetch_klines(session, symbol, start_ms=cur, end_ms=end_ms, limit=limit, price_source=PRICE_SOURCE_INDEX)
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)
        if not rows:
            break
        for r in rows:
            mk = month_key_from_ms(int(r[0]))
            buckets.setdefault(mk, []).append(r)
        last_open = int(rows[-1][0])
        cur = last_open + INTERVAL_MS
        if cur > end_ms - INTERVAL_MS:
            break

    touched = 0
    for mk in sorted(buckets.keys()):
        if merge_contract_and_index_month(data_dir, symbol, mk, buckets[mk]) > 0:
            touched += 1
    logging.info("[augment-idx] %s: months=%s", symbol, touched)


def main():
    ap = argparse.ArgumentParser(
        description="USD-M futures 1m contract/index klines -> parquet store (month shards) + incremental sync"
    )
    ap.add_argument(
        "--days", type=int, default=180, help="fallback window in days when --start-date/--end-date are not provided (default: 180)"
    )
    ap.add_argument("--start-date", default="", help="UTC start date YYYY-MM-DD")
    ap.add_argument("--end-date", default="", help="UTC end date YYYY-MM-DD (inclusive)")
    ap.add_argument(
        "--limit", type=int, default=1000, help="klines limit per request (<=1500)"
    )
    ap.add_argument(
        "--sleep-ms",
        type=int,
        default=200,
        help="sleep between requests per symbol (ms)",
    )
    ap.add_argument(
        "--price-source",
        required=False,
        choices=[PRICE_SOURCE_CONTRACT, PRICE_SOURCE_INDEX],
        help="explicit price source: contract=futures trade price klines, index=index price klines",
    )
    ap.add_argument("--data-dir", default="", help="output data dir (required to be explicit via --price-source defaults or manual override)")
    ap.add_argument(
        "--state-path", default="", help="state json path (required to be explicit via --price-source defaults or manual override)"
    )
    ap.add_argument(
        "--confirmed-delisted-path",
        default="state/confirmed_delisted_symbols.json",
        help="confirmed delisted symbols JSON file with last kline time metadata",
    )
    ap.add_argument(
        "--force-include-path",
        default="state/symbols_force_include.txt",
        help="force-include symbols text file (one symbol per line); higher priority than confirmed delisted",
    )
    ap.add_argument(
        "--delisted-data-dir",
        default="data/klines_1m_delisted",
        help="unused legacy option; confirmed delisted symbols are no longer moved on disk",
    )
    ap.add_argument(
        "--delisted-status-path",
        default="state/delisted_status.json",
        help="status json path for auto-confirmed delisted detection",
    )
    ap.add_argument(
        "--confirmed-delisted-stale-hours",
        type=int,
        default=24,
        help="auto-confirm a symbol as delisted when it is absent from live symbols and its last local kline is older than this many hours",
    )
    ap.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Backfill: do NOT skip symbols that already have local parquet shards",
    )

    ap.add_argument(
        "--symbols",
        default="",
        help="optional: comma-separated symbols for testing (e.g. BTCUSDT,ETHUSDT)",
    )
    ap.add_argument(
        "cmd",
        choices=["backfill", "sync", "augment-idx"],
        help="backfill=init store; sync=incremental update",
    )
    args = ap.parse_args()
    args.days_explicit = any(
        raw == "--days" or str(raw).startswith("--days=") for raw in sys.argv[1:]
    )

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )

    if args.cmd == "augment-idx":
        resolved_data_dir = args.data_dir or "data/klines_1m"
        resolved_state_path = args.state_path or "state/klines_1m_state.json"
    elif not args.price_source:
        raise SystemExit("--price-source is required for backfill and sync")
    elif args.price_source == PRICE_SOURCE_CONTRACT:
        resolved_data_dir = args.data_dir or "data/klines_1m"
        resolved_state_path = args.state_path or "state/klines_1m_state.json"
    elif args.price_source == PRICE_SOURCE_INDEX:
        resolved_data_dir = args.data_dir or "data/index_klines_1m"
        resolved_state_path = args.state_path or "state/index_klines_1m_state.json"
    else:
        raise SystemExit(f"unsupported --price-source: {args.price_source}")

    args.data_dir = resolved_data_dir
    args.state_path = resolved_state_path

    ensure_dir(args.data_dir)
    ensure_dir(os.path.dirname(args.state_path) or ".")

    state = load_state(args.state_path, args.price_source)

    with requests.Session() as sess:
        live_symbols = list_usdt_perpetual_symbols(sess)
        force_include_symbols = load_symbol_lines(args.force_include_path)
        force_include_symbols = require_usdt_symbols(
            force_include_symbols,
            args.force_include_path,
        )
        force_include_set = set(force_include_symbols)
        confirmed_delisted_symbols = [
            str(row["symbol"]).upper()
            for row in load_confirmed_delisted_records(args.confirmed_delisted_path)
        ]
        confirmed_delisted_symbols = require_usdt_symbols(
            confirmed_delisted_symbols,
            args.confirmed_delisted_path,
        )
        effective_confirmed_delisted = sorted(set(confirmed_delisted_symbols) - force_include_set)

        if args.symbols.strip():
            symbols = require_usdt_symbols(
                [x.strip().upper() for x in args.symbols.split(",") if x.strip()],
                "--symbols",
            )
        else:
            symbols = sorted((set(live_symbols) | force_include_set) - set(effective_confirmed_delisted))

        if args.cmd == "augment-idx" or args.price_source == PRICE_SOURCE_CONTRACT:
            try:
                delisted_status = refresh_confirmed_delisted(
                    live_symbols=live_symbols,
                    data_dir=args.data_dir,
                    state=state,
                    confirmed_delisted_path=args.confirmed_delisted_path,
                    force_include_path=args.force_include_path,
                    delisted_status_path=args.delisted_status_path,
                    stale_hours=int(args.confirmed_delisted_stale_hours),
                )
                force_include_symbols = delisted_status["force_include_symbols"]
                force_include_symbols = require_usdt_symbols(
                    force_include_symbols,
                    args.force_include_path,
                )
                force_include_set = set(force_include_symbols)
                effective_confirmed_delisted = [
                    str(row["symbol"]).upper()
                    for row in delisted_status["confirmed_delisted"]
                ]
                effective_confirmed_delisted = require_usdt_symbols(
                    effective_confirmed_delisted,
                    args.confirmed_delisted_path,
                )
                if not args.symbols.strip():
                    symbols = sorted((set(live_symbols) | force_include_set) - set(effective_confirmed_delisted))
                logging.info(
                    "[delisted] live=%s local=%s force=%s confirmed_raw=%s confirmed_effective=%s auto_suspected=%s auto_confirmed=%s path=%s force_path=%s stale_hours=%s",
                    delisted_status["live_symbols_count"],
                    delisted_status["local_symbol_dirs_count"],
                    delisted_status["force_include_count"],
                    delisted_status["confirmed_delisted_count_raw"],
                    delisted_status["confirmed_delisted_count_effective"],
                    delisted_status["auto_suspected_delisted_count"],
                    delisted_status["auto_confirmed_delisted_count"],
                    args.confirmed_delisted_path,
                    args.force_include_path,
                    args.confirmed_delisted_stale_hours,
                )
                if delisted_status["conflict_force_vs_confirmed"]:
                    logging.warning(
                        "[delisted] conflict_force_overrides_confirmed=%s",
                        ",".join(delisted_status["conflict_force_vs_confirmed"]),
                    )
                if delisted_status["auto_suspected_delisted"]:
                    logging.info(
                        "[delisted] auto_suspected=%s",
                        ",".join(row["symbol"] for row in delisted_status["auto_suspected_delisted"]),
                    )
            except Exception as e:
                logging.error("[delisted] refresh failed: %s", e)

        logging.info(
            "[start] cmd=%s price_source=%s symbols=%s interval=%s days=%s start=%s end=%s data_dir=%s",
            args.cmd,
            args.price_source,
            len(symbols),
            INTERVAL,
            args.days,
            args.start_date or "",
            args.end_date or "",
            args.data_dir,
        )

        if args.cmd == "backfill":
            for i, sym in enumerate(symbols, 1):
                # ---------------------- Patch-KLINES-SKIP-EXISTING ----------------------
                # Default behavior: when re-running backfill, skip symbols that already have
                # local parquet shards. This prevents restarting from the beginning after a reboot.
                if not getattr(args, "no_skip_existing", False):
                    try:
                        sym_dir = os.path.join(args.data_dir, sym)
                        if os.path.isdir(sym_dir):
                            parquet_files = sorted(
                                [
                                    fn
                                    for fn in os.listdir(sym_dir)
                                    if fn.endswith(".parquet")
                                ]
                            )
                            if parquet_files:
                                # Best-effort: if state is missing, infer last_open from local shards so sync can work later.
                                try:
                                    last_open = (
                                        state.get("per_symbol", {})
                                        .get(sym, {})
                                        .get("last_open_time_ms", 0)
                                    )
                                    if ((not last_open) or last_open <= 0) and (
                                        "pq" in globals()
                                    ):
                                        fp = os.path.join(sym_dir, parquet_files[-1])
                                        tbl = pq.read_table(
                                            fp, columns=["open_time_ms"]
                                        )
                                        col = tbl.column("open_time_ms")
                                        if len(col) > 0:
                                            last_open = int(max(col.to_pylist()))
                                            state.setdefault(
                                                "per_symbol", {}
                                            ).setdefault(sym, {})[
                                                "last_open_time_ms"
                                            ] = last_open
                                            save_json(args.state_path, state)
                                except Exception:
                                    pass
                                logging.info(
                                    "[backfill] %s: skip (local shards exist)", sym
                                )
                                continue
                    except Exception:
                        pass

                try:
                    backfill_symbol(
                        sess,
                        sym,
                        args.days,
                        args.data_dir,
                        state,
                        args.limit,
                        args.sleep_ms,
                        args.price_source,
                        args.start_date or None,
                        args.end_date or None,
                    )
                    save_json(args.state_path, state)
                except Exception as e:
                    if isinstance(e, BinanceRestIpBan):
                        raise
                    logging.error("[backfill] %s failed: %s", sym, e)
        elif args.cmd == "augment-idx":
            # 将 index 价格按日期段补写进单目录 contract 主表（9 字段 schema）
            for i, sym in enumerate(symbols, 1):
                try:
                    augment_idx_symbol(
                        sess,
                        sym,
                        args.data_dir,
                        args.limit,
                        args.sleep_ms,
                        args.start_date or None,
                        args.end_date or None,
                        args.days,
                        args.days_explicit,
                    )
                except Exception as e:
                    if isinstance(e, BinanceRestIpBan):
                        raise
                    logging.error("[augment-idx] %s failed: %s", sym, e)
        else:
            # sync：优先依赖 state；若 state 缺失但本地已有 shards，则先从本地恢复 last_open_time_ms。
            # 只有既没有 state、也没有本地 shards 时，才执行 full backfill。
            for i, sym in enumerate(symbols, 1):
                try:
                    per = state.get("per_symbol", {}).get(sym)
                    if not per or "last_open_time_ms" not in per:
                        try:
                            last_open = infer_last_open_from_local(args.data_dir, sym)
                        except Exception as e:
                            logging.warning(
                                "[sync] %s: infer local last_open failed: %s", sym, e
                            )
                            last_open = None

                        if last_open is not None:
                            update_symbol_state(state, sym, last_open)
                            logging.info(
                                "[sync] %s: recovered state from local shards last_open=%s",
                                sym,
                                last_open,
                            )
                            sync_symbol(
                                sess,
                                sym,
                                args.data_dir,
                                state,
                                args.limit,
                                args.sleep_ms,
                                args.price_source,
                            )
                        else:
                            logging.info("[sync] %s: no state/local shards; backfill first", sym)
                            backfill_symbol(
                                sess,
                                sym,
                                args.days,
                                args.data_dir,
                                state,
                                args.limit,
                                args.sleep_ms,
                                args.price_source,
                                args.start_date or None,
                                args.end_date or None,
                            )
                    else:
                        sync_symbol(
                            sess, sym, args.data_dir, state, args.limit, args.sleep_ms, args.price_source
                        )
                    save_json(args.state_path, state)
                except Exception as e:
                    if isinstance(e, BinanceRestIpBan):
                        raise
                    logging.error("[sync] %s failed: %s", sym, e)

    logging.info("[done] price_source=%s state=%s", args.price_source, args.state_path)


if __name__ == "__main__":
    main()
