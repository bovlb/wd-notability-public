from __future__ import annotations

from typing import Any

from wd_notability.db_env import credentials_from_env, require_env_value


def connect_replica(
    pymysql_module: Any,
    *,
    host: str,
    port: int,
    database: str,
    autocommit: bool = True,
):
    credentials = credentials_from_env(
        "REPLICADB_USER",
        "REPLICADB_PASSWORD",
    )
    host = require_env_value("REPLICADB_HOST")
    port = int(require_env_value("REPLICADB_PORT"))
    return pymysql_module.connect(
        user=credentials.user,
        password=credentials.password,
        host=host,
        port=port,
        database=database,
        charset="utf8mb4",
        autocommit=autocommit,
    )
