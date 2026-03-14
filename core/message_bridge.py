from __future__ import annotations

import json
import logging
import os
import time
from pathlib import Path

from filelock import FileLock

from core.runtime_state import get_state_dir

QUEUE_DIR = get_state_dir()


def _preview_text(content: str, limit: int = 60) -> str:
    try:
        text = str(content).replace('\r\n', '\n').replace('\r', '\n').replace('\n', '换行')
        data = text.encode('utf-8', errors='ignore')
        if len(data) <= limit:
            return data.decode('utf-8', errors='ignore')
        return data[:limit].decode('utf-8', errors='ignore') + '…'
    except Exception:
        return '<unprintable>'


def send_to_bot(content: str, label: str = 'global') -> bool:
    """Append a scheduler/strategy notification to tg_messages_<label>.queue as JSONL."""
    try:
        queue_label = (label or 'global').strip().lower()
        qpath: Path = QUEUE_DIR / f'tg_messages_{queue_label}.queue'
        record = {'content': str(content), 'ts': int(time.time())}

        lock = FileLock(str(qpath) + '.lock')
        with lock:
            with qpath.open('a', encoding='utf-8') as f:
                f.write(json.dumps(record, ensure_ascii=False) + '\n')
                f.flush()
                os.fsync(f.fileno())

        logging.info('[PUSH] queued label=%s msg="%s"', queue_label, _preview_text(content))
        return True
    except Exception as e:
        logging.error('[PUSH] send_to_bot failed: %s', e, exc_info=True)
        return False
