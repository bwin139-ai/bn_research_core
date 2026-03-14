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
    with tmp.open('w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)
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
