from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any


def _now_ms() -> int:
    return int(time.time() * 1000)


def load_checkpoint(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            'version': 1,
            'updated_ms': None,
            'datasets': {},
        }
    return json.loads(path.read_text(encoding='utf-8'))


def save_checkpoint(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    normalized = dict(payload)
    normalized['updated_ms'] = _now_ms()
    path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def get_symbol_watermark(payload: dict[str, Any], dataset: str, symbol: str) -> int | None:
    datasets = payload.get('datasets') or {}
    ds = datasets.get(str(dataset)) or {}
    sym = (ds.get('symbols') or {}).get(str(symbol).upper().strip()) or {}
    value = sym.get('last_event_ms')
    if value is None:
        return None
    return int(value)


def update_symbol_watermark(
    payload: dict[str, Any],
    dataset: str,
    symbol: str,
    *,
    last_event_ms: int | None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    out = dict(payload)
    datasets = dict(out.get('datasets') or {})
    ds = dict(datasets.get(str(dataset)) or {})
    symbols = dict(ds.get('symbols') or {})
    symbol_key = str(symbol).upper().strip()
    item = dict(symbols.get(symbol_key) or {})
    item['last_event_ms'] = int(last_event_ms) if last_event_ms is not None else None
    if extra:
        item.update(extra)
    symbols[symbol_key] = item
    ds['symbols'] = symbols
    datasets[str(dataset)] = ds
    out['datasets'] = datasets
    return out
