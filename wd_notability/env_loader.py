from __future__ import annotations

import os
from pathlib import Path


def _parse_env_value(raw_value: str) -> str:
    value = raw_value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]
    return value


def load_env_file(path: Path, *, override: bool = False) -> bool:
    if not path.is_file():
        return False

    loaded = False
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            continue

        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue

        value = _parse_env_value(raw_value)
        if override or key not in os.environ:
            os.environ[key] = value
        loaded = True

    return loaded


def load_default_env() -> None:
    project_root = Path(__file__).resolve().parent.parent
    candidates = [Path.cwd() / ".env", project_root / ".env"]

    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        load_env_file(candidate, override=False)
