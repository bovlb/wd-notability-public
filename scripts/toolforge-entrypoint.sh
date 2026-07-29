#!/bin/sh
set -eu

export LD_LIBRARY_PATH=/layers/heroku_python/python/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
exec /layers/heroku_python/venv/bin/python3 "$@"
