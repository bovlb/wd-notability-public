from __future__ import annotations

import configparser
from pathlib import Path


TOOLFORGE_DEFAULTS_FILE = Path.home() / "replica.my.cnf"
DEFAULT_TOOLFORGE_DATABASE = "wd_notability"


def toolforge_defaults_file() -> Path:
    return TOOLFORGE_DEFAULTS_FILE


def toolforge_defaults_file_exists() -> bool:
    return toolforge_defaults_file().exists()


def toolforge_cache_root() -> Path:
    if toolforge_defaults_file_exists():
        return Path("/tmp/wd-notability")
    return Path.home() / "localdbs" / "wd-notability"


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
