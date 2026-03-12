#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import requests

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


def date_range_to_ms(start_date: Optional[str], end_date: Optional[str], days: int) -> tuple[int, int]:
    if start_date and end_date:
        start_dt = parse_utc_date(start_date)
        end_dt = parse_utc_date(end_date) + timedelta(days=1)
        return floor_to_minute_ms(int(start_dt.timestamp() * 1000)), floor_to_minute_ms(int(end_dt.timestamp() * 1000))
    now_ms = int(utc_now().timestamp() * 1000)
    start_ms = floor_to_minute_ms(now_ms - int(days) * 24 * 60 * 60 * 1000)
    return start_ms, now_ms


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
def http_get_json(
    session: requests.Session,
    url: str,
    params: Dict,
    timeout: int = 20,
    max_retry: int = 8,
):
    backoff = 1.0
    for attempt in range(1, max_retry + 1):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 418):
                retry_after = r.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else backoff
                logging.warning(
                    "rate limited (%s). sleep %.1fs. url=%s params=%s",
                    r.status_code,
                    sleep_s,
                    url,
                    params,
                )
                time.sleep(sleep_s)
                backoff = min(backoff * 2, 60.0)
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
    return http_get_json(session, url, params={})


def list_symbols_excluding_usdc(session: requests.Session) -> List[str]:
    info = fetch_exchange_info(session)
    out = []
    for s in info.get("symbols", []):
        # 只取交易中 + 永续
        if s.get("status") != "TRADING":
            continue
        if s.get("contractType") != "PERPETUAL":
            continue

        sym = s.get("symbol", "")
        quote = s.get("quoteAsset", "")

        # 排除 *USDC
        if quote == "USDC" or sym.endswith("USDC"):
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
    return http_get_json(session, url, params=params)


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

    # Keep a Binance-like sparse row shape so rows_to_table can consume both
    # fresh API rows and locally reconstructed rows with the same field indices.
    merged: Dict[int, List] = {int(r[0]): r for r in new_rows}

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
    start_ms, now_ms = date_range_to_ms(start_date, end_date, days)

    buckets: Dict[str, List[List]] = {}
    cur = start_ms
    last_open = None

    while True:
        rows = fetch_klines(session, symbol, start_ms=cur, end_ms=now_ms, limit=limit, price_source=price_source)
        if not rows:
            break

        for r in rows:
            mk = month_key_from_ms(int(r[0]))
            buckets.setdefault(mk, []).append(r)

        last_open = int(rows[-1][0])
        cur = last_open + INTERVAL_MS

        if cur > now_ms - INTERVAL_MS:
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
    now_ms = int(utc_now().timestamp() * 1000)

    if cur > now_ms - INTERVAL_MS:
        return  # up to date

    buckets: Dict[str, List[List]] = {}
    last_open = None

    while True:
        rows = fetch_klines(session, symbol, start_ms=cur, end_ms=now_ms, limit=limit, price_source=price_source)
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

        if cur > now_ms - INTERVAL_MS:
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
) -> None:
    start_ms, end_ms = date_range_to_ms(start_date, end_date, days)
    buckets: Dict[str, List[List]] = {}
    cur = start_ms
    while True:
        rows = fetch_klines(session, symbol, start_ms=cur, end_ms=end_ms, limit=limit, price_source=PRICE_SOURCE_INDEX)
        if not rows:
            break
        for r in rows:
            mk = month_key_from_ms(int(r[0]))
            buckets.setdefault(mk, []).append(r)
        last_open = int(rows[-1][0])
        cur = last_open + INTERVAL_MS
        if cur > end_ms - INTERVAL_MS:
            break
        if sleep_ms > 0:
            time.sleep(sleep_ms / 1000.0)

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
        if args.symbols.strip():
            symbols = [x.strip().upper() for x in args.symbols.split(",") if x.strip()]
        else:
            symbols = list_symbols_excluding_usdc(sess)

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
                    )
                except Exception as e:
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
                    logging.error("[sync] %s failed: %s", sym, e)

    logging.info("[done] price_source=%s state=%s", args.price_source, args.state_path)


if __name__ == "__main__":
    main()
