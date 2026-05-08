from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from binance.client import Client

from core.live.binance_rest_gateway import (
    REQUEST_PRIORITY_NORMAL,
    call_futures_public,
)

_CLIENTS: dict[str, Client] = {}


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _secrets_dir() -> Path:
    raw = os.getenv("BN_SECRETS_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return _repo_root()


def _secrets_path(account: str) -> Path:
    account_clean = (account or "").strip()
    if not account_clean:
        raise ValueError("account 不能为空")
    return _secrets_dir() / f"secrets_{account_clean}.json"


def load_account_secrets(account: str) -> dict[str, Any]:
    path = _secrets_path(account)
    if not path.exists():
        raise FileNotFoundError(f"账户密钥文件缺失: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    api_key = str(data.get("api_key") or data.get("API_KEY") or "").strip()
    api_secret = str(data.get("api_secret") or data.get("API_SECRET") or "").strip()
    if not api_key or not api_secret:
        raise KeyError(f"账户密钥缺少 api_key/api_secret: {path}")

    return {
        "api_key": api_key,
        "api_secret": api_secret,
        "raw": data,
        "path": str(path),
    }


def get_client(account: str, *, use_cache: bool = True) -> Client:
    account_clean = (account or "").strip()
    if not account_clean:
        raise ValueError("account 不能为空")

    if use_cache and account_clean in _CLIENTS:
        return _CLIENTS[account_clean]

    secrets = load_account_secrets(account_clean)
    client = Client(secrets["api_key"], secrets["api_secret"])
    _CLIENTS[account_clean] = client
    return client


def reset_client(account: str | None = None) -> None:
    if account is None:
        _CLIENTS.clear()
        return
    _CLIENTS.pop((account or "").strip(), None)


def ping(account: str) -> dict[str, Any]:
    try:
        client = get_client(account)
        client.ping()
        return {"ok": True, "reason": ""}
    except Exception as e:
        return {"ok": False, "reason": str(e)}


def get_index_price_klines(
    account: str,
    symbol: str,
    *,
    interval: str = "1m",
    limit: int = 500,
    start_time: int | None = None,
    end_time: int | None = None,
) -> list[list[Any]]:
    pair = (symbol or "").upper().strip()
    if not pair:
        raise ValueError("symbol 不能为空")
    params: dict[str, Any] = {
        "pair": pair,
        "interval": str(interval),
        "limit": int(limit),
    }
    if start_time is not None:
        params["startTime"] = int(start_time)
    if end_time is not None:
        params["endTime"] = int(end_time)

    rows = call_futures_public(
        account,
        source='binance_client.index_price_klines',
        endpoint='indexPriceKlines',
        params=params,
        priority=REQUEST_PRIORITY_NORMAL,
    )
    if not isinstance(rows, list):
        raise RuntimeError(f"indexPriceKlines 返回异常: {rows}")
    return rows
