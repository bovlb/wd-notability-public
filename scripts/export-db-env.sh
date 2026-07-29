#!/bin/sh
set -eu

defaults_file=${1:-${REPLICADB_DEFAULTS_FILE:-$HOME/replica.my.cnf}}
toolsdb_host=${TOOLSDB_HOST:?TOOLSDB_HOST is required}
toolsdb_port=${TOOLSDB_PORT:?TOOLSDB_PORT is required}
script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
python_bin=${PYTHON:-"$script_dir/../.venv/bin/python"}
if [ ! -x "$python_bin" ]; then
  python_bin=${PYTHON:-python3}
fi

"$python_bin" - "$defaults_file" "$toolsdb_host" "$toolsdb_port" <<'PY'
from __future__ import annotations

import configparser
import os
import shlex
import sys
from pathlib import Path

defaults_file = Path(sys.argv[1])
toolsdb_host = sys.argv[2]
toolsdb_port = sys.argv[3]
toolsdb_database_override = os.getenv("TOOLSDB_DATABASE", "").strip() or None
toolsdb_user_override = os.getenv("TOOLSDB_USER", "").strip() or None
toolsdb_password_override = os.getenv("TOOLSDB_PASSWORD", "").strip() or None

config = configparser.ConfigParser(interpolation=None)
if defaults_file.exists():
    config.read(defaults_file)
client = config["client"] if "client" in config else {}
user = str(client.get("user", "")).strip()
password = str(client.get("password", "")).strip()
if not user or not password:
    raise SystemExit(f"Unable to read client credentials from {defaults_file}")

toolsdb_database = toolsdb_database_override or user
toolsdb_user = toolsdb_user_override or user
toolsdb_password = toolsdb_password_override or password

for key, value in (
    ("REPLICADB_USER", user),
    ("REPLICADB_PASSWORD", password),
    ("TOOLSDB_HOST", toolsdb_host),
    ("TOOLSDB_PORT", toolsdb_port),
    ("TOOLSDB_DATABASE", toolsdb_database),
    ("TOOLSDB_USER", toolsdb_user),
    ("TOOLSDB_PASSWORD", toolsdb_password),
):
    print(f"export {key}={shlex.quote(value)}")
PY
