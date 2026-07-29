from __future__ import annotations

import configparser
import os
from pathlib import Path


TOOLFORGE_DEFAULTS_FILE = Path.home() / "replica.my.cnf"
DEFAULT_TOOLFORGE_DATABASE = "wd_notability"
DEFAULT_CACHE_ROOT = Path("/tmp/wd-notability")


def toolforge_defaults_file() -> Path:
    return TOOLFORGE_DEFAULTS_FILE


def toolforge_defaults_file_exists() -> bool:
    return toolforge_defaults_file().exists()


def _env_flag(name: str) -> bool:
    raw_value = os.getenv(name)
    if raw_value is None:
        return False
    return raw_value.strip().lower() in {"1", "true", "yes", "on"}


def toolforge_mode_enabled() -> bool:
    return _env_flag("WD_NOTABILITY_TOOLFORGE")


def toolforge_cache_root() -> Path:
    return DEFAULT_CACHE_ROOT


def toolforge_database_name(
    defaults_file: str | Path | None = None,
    *,
    default_database: str = DEFAULT_TOOLFORGE_DATABASE,
) -> str:
    path = Path(defaults_file) if defaults_file is not None else toolforge_defaults_file()
    if not path.exists():
        return default_database

    config = configparser.ConfigParser(interpolation=None)
    config.read(path)
    client = config["client"] if "client" in config else {}
    user = str(client.get("user", "")).strip()
    return user or default_database
