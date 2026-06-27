from __future__ import annotations

import os
import re
import time
from typing import Any

import requests


DEFAULT_API_BASE_URL = "https://api.telegram.org"


def mask_secret(text: str, secret: str) -> str:
    if secret:
        return text.replace(secret, "***masked***")
    return text


def parse_proxy_urls(raw: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for item in re.split(r"[,\s]+", raw.strip()):
        value = item.strip()
        if not value or value in seen:
            continue
        seen.add(value)
        urls.append(value)
    return urls


def telegram_proxy_urls(
    *,
    primary_env: str = "TG_PROXY_URLS",
    fallback_env: str = "TG_PROXY_URL",
) -> list[str]:
    values: list[str] = []
    primary = os.getenv(primary_env, "").strip()
    if primary:
        values.extend(parse_proxy_urls(primary))
    fallback = os.getenv(fallback_env, "").strip()
    if fallback:
        values.extend(parse_proxy_urls(fallback))
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def telegram_proxies(proxy_url: str | None) -> dict[str, str] | None:
    if not proxy_url:
        return None
    return {"http": proxy_url, "https": proxy_url}


def telegram_getme_health(
    *,
    token: str,
    proxy_url: str | None,
    api_base_url: str = DEFAULT_API_BASE_URL,
    timeout_secs: int = 10,
) -> dict[str, Any]:
    api_base = api_base_url.rstrip("/")
    url = f"{api_base}/bot{token}/getMe"
    session = requests.Session()
    session.trust_env = False
    started = time.monotonic()
    try:
        resp = session.get(url, timeout=timeout_secs, proxies=telegram_proxies(proxy_url))
    except Exception as exc:
        return {
            "ok": False,
            "reason": "telegram_request_exception",
            "proxy_url": proxy_url,
            "elapsed_ms": int((time.monotonic() - started) * 1000),
            "error": mask_secret(str(exc), token)[:240],
        }

    elapsed_ms = int((time.monotonic() - started) * 1000)
    body_preview = resp.text[:240]
    try:
        payload = resp.json()
    except Exception:
        payload = None
    api_ok = isinstance(payload, dict) and bool(payload.get("ok"))
    return {
        "ok": resp.status_code == 200 and api_ok,
        "reason": "telegram_api_ok" if resp.status_code == 200 and api_ok else "telegram_api_bad_response",
        "proxy_url": proxy_url,
        "elapsed_ms": elapsed_ms,
        "http_status": resp.status_code,
        "api_ok": api_ok,
        "body_preview": body_preview,
    }


def select_telegram_proxy_url(
    *,
    token: str,
    proxy_urls: list[str] | None = None,
    api_base_url: str = DEFAULT_API_BASE_URL,
    timeout_secs: int = 8,
) -> tuple[str | None, list[dict[str, Any]]]:
    urls = list(proxy_urls) if proxy_urls is not None else telegram_proxy_urls()
    if not urls:
        return None, []
    results: list[dict[str, Any]] = []
    for url in urls:
        result = telegram_getme_health(
            token=token,
            proxy_url=url,
            api_base_url=api_base_url,
            timeout_secs=timeout_secs,
        )
        results.append(result)
        if bool(result["ok"]):
            return url, results
    return urls[0], results
