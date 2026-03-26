from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional


def get_state_dir() -> Path:
    """Runtime state directory (kept outside git)."""
    path = Path(os.getenv("BN_STATE_DIR", "state"))
    path.mkdir(parents=True, exist_ok=True)
    return path


def state_path(*parts: str) -> Path:
    """Build a path under the runtime state directory and ensure parent exists."""
    path = get_state_dir().joinpath(*parts)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def resolve_state_path(filename: str) -> Path:
    """Resolve a single runtime-state filename to an absolute path under state/."""
    return state_path(filename)


def _json_default(obj: Any) -> Any:
    """
    Convert common numpy/pandas/datetime scalars into plain JSON-serializable types.
    Keep this helper dependency-light by importing optional libraries lazily.
    """
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj

    # numpy scalar / array
    try:
        import numpy as np  # type: ignore
        if isinstance(obj, np.generic):
            return obj.item()
        if isinstance(obj, np.ndarray):
            return obj.tolist()
    except Exception:
        pass

    # pandas scalar / timestamp
    try:
        import pandas as pd  # type: ignore
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if obj is pd.NA:
            return None
    except Exception:
        pass

    # stdlib date/datetime/path
    if hasattr(obj, "isoformat") and callable(getattr(obj, "isoformat", None)):
        try:
            return obj.isoformat()
        except Exception:
            pass
    if isinstance(obj, Path):
        return str(obj)

    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _normalize_for_json(data: Any) -> Any:
    """Recursively convert nested structures into plain JSON-safe Python values."""
    if data is None or isinstance(data, (str, int, float, bool)):
        return data
    if isinstance(data, dict):
        normalized: dict[str, Any] = {}
        for k, v in data.items():
            normalized[str(_normalize_for_json(k))] = _normalize_for_json(v)
        return normalized
    if isinstance(data, (list, tuple, set)):
        return [_normalize_for_json(v) for v in data]
    return _json_default(data)


def load_json_file(path: Path | str, default: Any = None) -> Any:
    p = Path(path)
    if not p.exists():
        return default
    with p.open('r', encoding='utf-8') as f:
        return json.load(f)


def save_json_file_atomic(path: Path | str, data: Any, indent: int = 2) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(p.suffix + '.tmp')
    normalized = _normalize_for_json(data)
    with tmp.open('w', encoding='utf-8') as f:
        json.dump(normalized, f, ensure_ascii=False, indent=indent)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, p)


def load_runtime_json(filename: str, default: Any = None, legacy_path: Optional[str] = None) -> Any:
    new_path = state_path(filename)
    if new_path.exists():
        return load_json_file(new_path, default=default)

    old_path = Path(legacy_path or filename)
    if old_path.exists():
        data = load_json_file(old_path, default=default)
        save_json_file_atomic(new_path, data)
        return data

    return default


def save_runtime_json(filename: str, data: Any, indent: int = 2) -> None:
    save_json_file_atomic(state_path(filename), data, indent=indent)
