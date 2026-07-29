#!/bin/sh
set -eu

BIN_DIR=$(realpath "$(dirname "$0")")
ENV_FILE=$BIN_DIR/../.env
if [ -f "$ENV_FILE" ]; then
  export $(grep -v '^#' "$ENV_FILE" | xargs)
fi

image=${WD_NOTABILITY_MARIADB_IMAGE:-mariadb:11.4}
container_name=${WD_NOTABILITY_MARIADB_CONTAINER:-wd-notability-mariadb}
data_dir=${WD_NOTABILITY_MARIADB_DATA_DIR:-${XDG_DATA_HOME:-$HOME/.local/share}/wd-notability/mariadb}
port=${WD_NOTABILITY_MARIADB_PORT:-3307}
root_password=${WD_NOTABILITY_MARIADB_ROOT_PASSWORD:-root}
database=${TOOLSDB_DATABASE:?}
user=${TOOLSDB_USER:?}
password=${TOOLSDB_PASSWORD:?}

mkdir -p "$data_dir"

exec docker run --rm \
  --name "$container_name" \
  -p "${port}:3306" \
  -v "${data_dir}:/var/lib/mysql" \
  -e MARIADB_ROOT_PASSWORD="$root_password" \
  -e MARIADB_DATABASE="$database" \
  -e MARIADB_USER="$user" \
  -e MARIADB_PASSWORD="$password" \
  "$image"
