from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable


def _json_default(value: Any) -> Any:
    if hasattr(value, 'item'):
        return value.item()
    raise TypeError(f'Object of type {type(value).__name__} is not JSON serializable')


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def append_jsonl_unique(
    out_path: Path,
    index_path: Path,
    records: list[dict[str, Any]],
    *,
    key_fn: Callable[[dict[str, Any]], str],
) -> dict[str, Any]:
    ensure_parent(out_path)
    ensure_parent(index_path)

    seen: set[str] = set()
    if index_path.exists():
        with index_path.open('r', encoding='utf-8') as f:
            for line in f:
                key = line.strip()
                if key:
                    seen.add(key)

    appended = 0
    skipped = 0
    written_keys: list[str] = []

    with out_path.open('a', encoding='utf-8') as out_f, index_path.open('a', encoding='utf-8') as idx_f:
        for record in records:
            key = str(key_fn(record)).strip()
            if not key:
                raise ValueError('dedupe key must not be empty')
            if key in seen:
                skipped += 1
                continue
            out_f.write(json.dumps(record, ensure_ascii=False, default=_json_default) + '\n')
            idx_f.write(key + '\n')
            seen.add(key)
            written_keys.append(key)
            appended += 1

    return {
        'appended': appended,
        'skipped': skipped,
        'written_keys': written_keys,
        'out_path': str(out_path),
        'index_path': str(index_path),
    }
