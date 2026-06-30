from __future__ import annotations

import configparser
from pathlib import Path
from typing import Any


def connect_replica(
    pymysql_module: Any,
    *,
    defaults_file: Path,
    host: str,
    port: int,
    database: str,
    autocommit: bool = True,
):
    if not defaults_file.exists():
        raise RuntimeError(f"Toolforge replica credential file is missing: {defaults_file}")

    config = configparser.ConfigParser(interpolation=None)
    config.read(defaults_file)
    client = config["client"] if "client" in config else {}
    return pymysql_module.connect(
        user=client.get("user"),
        password=client.get("password"),
        host=host,
        port=port,
        database=database,
        charset="utf8mb4",
        autocommit=autocommit,
    )
