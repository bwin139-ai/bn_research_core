#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path


GATEWAY_PATH = "core/live/binance_rest_gateway.py"
CLIENT_PATH = "core/live/binance_client.py"
AUDIT_PATH = "audit_tools/maintenance/audit_binance_rest_gateway_coverage.py"

ALLOWED_BINANCE_HOST_REFERENCES = {
    AUDIT_PATH,
    GATEWAY_PATH,
    "strategies/klines_1m_store.py",
}

ALLOWED_BINANCE_CLIENT_ACCESS = {
    AUDIT_PATH,
    GATEWAY_PATH,
    CLIENT_PATH,
}

SKIP_DIRS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    "__pycache__",
    "data",
    "logs",
    "output",
    "state",
}


@dataclass(frozen=True)
class Finding:
    kind: str
    path: str
    line: int
    text: str
    reason: str


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _tracked_python_files(root: Path) -> list[Path]:
    try:
        proc = subprocess.run(
            ["git", "ls-files", "*.py"],
            cwd=str(root),
            check=True,
            text=True,
            capture_output=True,
        )
        return [root / line.strip() for line in proc.stdout.splitlines() if line.strip()]
    except Exception:
        out: list[Path] = []
        for path in root.rglob("*.py"):
            rel_parts = path.relative_to(root).parts
            if any(part in SKIP_DIRS for part in rel_parts):
                continue
            out.append(path)
        return sorted(out)


def _strip_comment(line: str) -> str:
    return line.split("#", 1)[0]


def audit_file(root: Path, path: Path) -> list[Finding]:
    rel = path.relative_to(root).as_posix()
    text = path.read_text(encoding="utf-8")
    lines = text.splitlines()
    findings: list[Finding] = []

    file_has_binance_host = any(
        marker in text
        for marker in (
            "api.binance.com",
            "fapi.binance.com",
            "dapi.binance.com",
        )
    )

    for idx, raw_line in enumerate(lines, start=1):
        line = _strip_comment(raw_line)
        compact = line.strip()
        if not compact:
            continue

        has_binance_host = any(
            marker in line
            for marker in (
                "api.binance.com",
                "fapi.binance.com",
                "dapi.binance.com",
            )
        )
        if has_binance_host and rel not in ALLOWED_BINANCE_HOST_REFERENCES:
            findings.append(
                Finding(
                    kind="binance_host_reference",
                    path=rel,
                    line=idx,
                    text=compact,
                    reason="Binance REST host references must live behind Binance REST Gateway.",
                )
            )

        direct_http = any(
            token in line
            for token in (
                "requests.get(",
                "requests.post(",
                "requests.request(",
                "session.get(",
                ".get(url",
            )
        )
        if direct_http and file_has_binance_host and rel not in {AUDIT_PATH, GATEWAY_PATH}:
            findings.append(
                Finding(
                    kind="direct_binance_http_call",
                    path=rel,
                    line=idx,
                    text=compact,
                    reason="Direct Binance HTTP calls must use request_futures_public/call_futures_public/call_futures_signed.",
                )
            )

        imports_get_client = "from core.live.binance_client import" in line and (
            "get_client" in line or "load_account_secrets" in line
        )
        calls_get_client = bool(re.search(r"\bget_client\s*\(", line))
        calls_load_secrets = bool(re.search(r"\bload_account_secrets\s*\(", line))
        defines_allowed_client_fn = rel == CLIENT_PATH and (
            re.search(r"\bdef\s+get_client\s*\(", line)
            or re.search(r"\bdef\s+load_account_secrets\s*\(", line)
        )
        if (
            (imports_get_client or calls_get_client or calls_load_secrets)
            and rel not in ALLOWED_BINANCE_CLIENT_ACCESS
            and not defines_allowed_client_fn
        ):
            findings.append(
                Finding(
                    kind="direct_binance_client_access",
                    path=rel,
                    line=idx,
                    text=compact,
                    reason="Code outside Gateway/client should not directly obtain Binance clients or secrets.",
                )
            )

        if "_request_futures_api" in line and rel not in {AUDIT_PATH, GATEWAY_PATH}:
            findings.append(
                Finding(
                    kind="raw_python_binance_request",
                    path=rel,
                    line=idx,
                    text=compact,
                    reason="Raw python-binance futures requests must be centralized in Binance REST Gateway.",
                )
            )

        direct_client_method = bool(re.search(r"\bclient\.futures_[a-zA-Z0-9_]+\s*\(", line))
        if direct_client_method and rel not in ALLOWED_BINANCE_CLIENT_ACCESS:
            findings.append(
                Finding(
                    kind="direct_python_binance_futures_call",
                    path=rel,
                    line=idx,
                    text=compact,
                    reason="Direct python-binance futures methods must go through call_client_method.",
                )
            )

    return findings


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Audit that Binance REST consumers are routed through core/live/binance_rest_gateway.py."
    )
    parser.add_argument("--root", default="", help="Repository root; default is inferred from this script")
    parser.add_argument("--json", action="store_true", help="Emit findings as JSON")
    args = parser.parse_args()

    root = Path(args.root).expanduser().resolve() if args.root else _repo_root()
    findings: list[Finding] = []
    for path in _tracked_python_files(root):
        if path.exists():
            findings.extend(audit_file(root, path))

    if args.json:
        print(json.dumps([asdict(row) for row in findings], ensure_ascii=False, indent=2))
    else:
        print("=== Binance REST Gateway coverage audit ===")
        print(f"root     : {root}")
        print(f"findings : {len(findings)}")
        for row in findings:
            print(f"[{row.kind}] {row.path}:{row.line} | {row.reason}")
            print(f"  {row.text}")

    if findings:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
