#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
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


# ----------------------------
# helpers
# ----------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso() -> str:
    return utc_now().isoformat()


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


def load_state(state_path: str) -> Dict:
    st = load_json(state_path, default={})
    st.setdefault("version", 1)
    st.setdefault("base_url", BASE_URL)
    st.setdefault("interval", INTERVAL)
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
) -> List[List]:
    url = f"{BASE_URL}/fapi/v1/klines"
    params = {
        "symbol": symbol,
        "interval": INTERVAL,
        "startTime": int(start_ms),
        "limit": int(limit),
    }
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    return http_get_json(session, url, params=params)


# ----------------------------
# parquet IO (month shards)
# ----------------------------
SCHEMA = pa.schema(
    [
        ("open_time_ms", pa.int64()),
        ("open", pa.float64()),
        ("high", pa.float64()),
        ("low", pa.float64()),
        ("close", pa.float64()),
        ("quote_asset_volume", pa.float64()),
    ]
)


def rows_to_table(rows: List[List]) -> pa.Table:
    # Binance kline array:
    # [ open_time, open, high, low, close, volume, close_time,
    #   quote_asset_volume, number_of_trades, taker_buy_base, taker_buy_quote, ignore ]
    # Store contract is intentionally narrowed to 6 columns.
    open_time = [int(r[0]) for r in rows]
    open_ = [float(r[1]) for r in rows]
    high = [float(r[2]) for r in rows]
    low = [float(r[3]) for r in rows]
    close = [float(r[4]) for r in rows]
    quote_vol = [float(r[7]) if len(r) > 7 else 0.0 for r in rows]

    return pa.Table.from_arrays(
        [
            pa.array(open_time, type=pa.int64()),
            pa.array(open_, type=pa.float64()),
            pa.array(high, type=pa.float64()),
            pa.array(low, type=pa.float64()),
            pa.array(close, type=pa.float64()),
            pa.array(quote_vol, type=pa.float64()),
        ],
        schema=SCHEMA,
    )


def month_file(data_dir: str, symbol: str, month_key: str) -> str:
    return os.path.join(data_dir, symbol, f"{month_key}.parquet")


def merge_write_month(
    data_dir: str, symbol: str, month_key: str, new_rows: List[List]
) -> int:
    """
    Merge by open_time_ms (dedup), sort asc, write shard.
    Strict schema: assumes existing parquet (if any) uses current 6-column SCHEMA.
    """
    ensure_dir(os.path.join(data_dir, symbol))
    fpath = month_file(data_dir, symbol, month_key)

    # Keep a Binance-like sparse row shape so rows_to_table can consume both
    # fresh API rows and locally reconstructed rows with the same field indices.
    merged: Dict[int, List] = {int(r[0]): r for r in new_rows}

    if os.path.exists(fpath):
        tbl = pq.read_table(
            fpath,
            columns=[
                "open_time_ms",
                "open",
                "high",
                "low",
                "close",
                "quote_asset_volume",
            ],
        )

        ot = tbl.column("open_time_ms").to_pylist()
        o = tbl.column("open").to_pylist()
        h = tbl.column("high").to_pylist()
        low_ = tbl.column("low").to_pylist()
        c = tbl.column("close").to_pylist()
        qv = tbl.column("quote_asset_volume").to_pylist()

        for i in range(len(ot)):
            k = int(ot[i])
            if k in merged:
                continue
            # Rebuild the minimal index positions consumed by rows_to_table:
            # 0=open_time_ms, 1=open, 2=high, 3=low, 4=close, 7=quote_asset_volume
            merged[k] = [
                k,
                o[i],
                h[i],
                low_[i],
                c[i],
                0.0,
                0,
                qv[i],
            ]

    keys = sorted(merged.keys())
    tbl_out = rows_to_table([merged[k] for k in keys])

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
) -> None:
    now_ms = int(utc_now().timestamp() * 1000)
    start_ms = floor_to_minute_ms(now_ms - int(days) * 24 * 60 * 60 * 1000)

    buckets: Dict[str, List[List]] = {}
    cur = start_ms
    last_open = None

    while True:
        rows = fetch_klines(session, symbol, start_ms=cur, end_ms=now_ms, limit=limit)
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
        n = merge_write_month(data_dir, symbol, mk, buckets[mk])
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
        rows = fetch_klines(session, symbol, start_ms=cur, end_ms=now_ms, limit=limit)
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
        merge_write_month(data_dir, symbol, mk, buckets[mk])

    if last_open is not None:
        update_symbol_state(state, symbol, last_open)

    logging.info("[sync] %s: updated to %s", symbol, last_open)


def main():
    ap = argparse.ArgumentParser(
        description="USD-M futures 1m klines -> parquet store (month shards) + incremental sync"
    )
    ap.add_argument(
        "--days", type=int, default=180, help="backfill days (default: 180)"
    )
    ap.add_argument(
        "--limit", type=int, default=1000, help="klines limit per request (<=1500)"
    )
    ap.add_argument(
        "--sleep-ms",
        type=int,
        default=200,
        help="sleep between requests per symbol (ms)",
    )
    ap.add_argument("--data-dir", default="data/klines_1m", help="output data dir")
    ap.add_argument(
        "--state-path", default="state/klines_1m_state.json", help="state json path"
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
        choices=["backfill", "sync"],
        help="backfill=init store; sync=incremental update",
    )
    args = ap.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )

    ensure_dir(args.data_dir)
    ensure_dir(os.path.dirname(args.state_path) or ".")

    state = load_state(args.state_path)

    with requests.Session() as sess:
        if args.symbols.strip():
            symbols = [x.strip().upper() for x in args.symbols.split(",") if x.strip()]
        else:
            symbols = list_symbols_excluding_usdc(sess)

        logging.info(
            "[start] cmd=%s symbols=%s interval=%s days=%s",
            args.cmd,
            len(symbols),
            INTERVAL,
            args.days,
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
                    )
                    save_json(args.state_path, state)
                except Exception as e:
                    logging.error("[backfill] %s failed: %s", sym, e)
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
                            )
                    else:
                        sync_symbol(
                            sess, sym, args.data_dir, state, args.limit, args.sleep_ms
                        )
                    save_json(args.state_path, state)
                except Exception as e:
                    logging.error("[sync] %s failed: %s", sym, e)

    logging.info("[done] state=%s", args.state_path)


if __name__ == "__main__":
    main()
