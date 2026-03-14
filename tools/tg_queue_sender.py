#!/usr/bin/env python3
from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import requests
from filelock import FileLock

# New-lib runtime state helpers only; do not depend on old utils.py
from core.runtime_state import get_state_dir, resolve_state_path, load_json_file

LOG = logging.getLogger("tg_queue_sender")
POLL_SEC = 2.0
SEND_TIMEOUT_SEC = 15
RETRY_SEC = 1.0
PERMISSIONS_FILE = "permissions.json"


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(message)s",
    )


def load_permissions() -> tuple[list[str], dict[str, list[str]]]:
    admins: list[str] = []
    viewers_by_account: dict[str, list[str]] = {}

    perm_path = Path(PERMISSIONS_FILE)
    if perm_path.exists():
        try:
            data = load_json_file(perm_path)
            if isinstance(data, dict):
                raw_admins = data.get("admins", [])
                if isinstance(raw_admins, list):
                    admins = [str(x) for x in raw_admins if str(x).strip()]

                raw_viewers = data.get("viewers", {})
                if isinstance(raw_viewers, dict):
                    for viewer_id, info in raw_viewers.items():
                        if not isinstance(info, dict):
                            continue
                        accounts = info.get("accounts", [])
                        if not isinstance(accounts, list):
                            continue
                        vid = str(viewer_id).strip()
                        if not vid:
                            continue
                        for acct in accounts:
                            key = str(acct).strip()
                            if not key:
                                continue
                            viewers_by_account.setdefault(key, []).append(vid)
        except Exception as e:
            LOG.warning("[PERM] failed to load permissions.json: %s", e)

    env_chat_id = str(os.getenv("TG_CHAT_ID", "")).strip()
    if not admins and env_chat_id:
        admins = [env_chat_id]

    LOG.info("[PERM] loaded admins=%s viewer_accounts=%s", len(admins), len(viewers_by_account))
    return admins, viewers_by_account


def tg_api_url(token: str) -> str:
    return f"https://api.telegram.org/bot{token}/sendMessage"


def send_tg(token: str, chat_id: str, text: str) -> bool:
    payload = {"chat_id": chat_id, "text": text}
    url = tg_api_url(token)
    for attempt in (1, 2):
        try:
            resp = requests.post(url, json=payload, timeout=SEND_TIMEOUT_SEC)
            if resp.status_code == 200:
                LOG.info("[SENDER] -> %s ok len=%s", chat_id, len(text))
                return True
            LOG.warning("[SENDER] -> %s failed status=%s body=%s", chat_id, resp.status_code, resp.text[:300])
        except Exception as e:
            LOG.warning("[SENDER] -> %s exception attempt=%s err=%s", chat_id, attempt, e)
        time.sleep(RETRY_SEC)
    return False


def parse_queue_name(queue_path: Path) -> str:
    name = queue_path.name
    prefix = "tg_messages_"
    suffix = ".queue"
    if name.startswith(prefix) and name.endswith(suffix):
        return name[len(prefix):-len(suffix)]
    return "global"


def route_recipients(label: str, content: str, admins: list[str], viewers_by_account: dict[str, list[str]]) -> list[str]:
    # Old business semantics preserved:
    # - non-[PUSH] -> admins only
    # - [PUSH] + label==global -> admins only
    # - [PUSH] + label!=global -> viewers authorized for that account label
    # - [CFLOW] -> admins only
    recipients: list[str] = []
    if content.startswith("[PUSH]"):
        if label == "global":
            recipients = list(admins)
        else:
            recipients = list(viewers_by_account.get(label, []))
    else:
        recipients = list(admins)
    # Deduplicate but preserve order
    seen: set[str] = set()
    out: list[str] = []
    for r in recipients:
        if r and r not in seen:
            seen.add(r)
            out.append(r)
    LOG.info("[ROUTE] label=%s recipients=%s prefix=%s", label, len(out), content[:20])
    return out


def process_queue_file(queue_path: Path, token: str, admins: list[str], viewers_by_account: dict[str, list[str]]) -> None:
    lock = FileLock(str(queue_path) + ".lock")
    ts = int(time.time())
    processing_path = queue_path.with_suffix(queue_path.suffix + f".processing.{ts}")

    with lock:
        if not queue_path.exists() or queue_path.stat().st_size == 0:
            return
        queue_path.rename(processing_path)

    label = parse_queue_name(queue_path)
    try:
        with processing_path.open("r", encoding="utf-8") as f:
            for line_no, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    item: Any = json.loads(line)
                    content = str(item.get("content", "")).strip()
                except Exception as e:
                    LOG.warning("[QUEUE] invalid json file=%s line=%s err=%s", processing_path.name, line_no, e)
                    continue
                if not content:
                    continue
                recipients = route_recipients(label, content, admins, viewers_by_account)
                for chat_id in recipients:
                    send_tg(token, chat_id, content)
    finally:
        try:
            processing_path.unlink(missing_ok=True)
        except Exception as e:
            LOG.warning("[QUEUE] cleanup failed %s err=%s", processing_path, e)


def iter_queue_files(state_dir: Path) -> list[Path]:
    return sorted(state_dir.glob("tg_messages_*.queue"))


def main() -> int:
    setup_logging()
    token = str(os.getenv("TG_BOT_TOKEN", "")).strip()
    if not token:
        LOG.error("TG_BOT_TOKEN is required")
        return 2

    state_dir = get_state_dir()
    if not state_dir.exists():
        state_dir.mkdir(parents=True, exist_ok=True)

    admins, viewers_by_account = load_permissions()
    LOG.info("[BOOT] state_dir=%s", state_dir)

    while True:
        processed_any = False
        for queue_path in iter_queue_files(state_dir):
            processed_any = True
            process_queue_file(queue_path, token, admins, viewers_by_account)
        if not processed_any:
            time.sleep(POLL_SEC)
        else:
            time.sleep(0.2)


if __name__ == "__main__":
    raise SystemExit(main())
