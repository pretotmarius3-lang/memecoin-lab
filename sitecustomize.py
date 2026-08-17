"""Project-local Python startup hook.

Automatically loads ~/memecoin_lab/.env before any Memecoin Lab script imports
its environment variables. This keeps API secrets local and avoids exporting
keys separately in every terminal.

The .env file itself must remain untracked / gitignored.
"""
from __future__ import annotations

import os
from pathlib import Path

ROOT = Path.home() / "memecoin_lab"
ENV_FILE = ROOT / ".env"


def _load_env(path: Path) -> None:
    if not path.is_file():
        return
    try:
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[7:].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
                value = value[1:-1]
            # Project .env is authoritative. This intentionally replaces stale
            # shell exports left over from earlier terminals.
            os.environ[key] = value
    except OSError:
        # Individual scripts will still emit their normal missing-key error.
        pass


_load_env(ENV_FILE)
