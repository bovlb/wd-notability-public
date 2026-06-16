#!/usr/bin/env bash
set -euo pipefail

REPO_DIR=${WD_NOTABILITY_REPO_DIR:-"$HOME/wd-notability"}
VENV_DIR=${WD_NOTABILITY_VENV_DIR:-"$HOME/venv/wd-notability"}
TOOLFORGE_IMAGE=${WD_NOTABILITY_TOOLFORGE_IMAGE:-python3.11}
LOOKUP_BACKEND=${WD_NOTABILITY_LOOKUP_BACKEND:-mariadb}
LOOKUP_DATABASE=${WD_NOTABILITY_LOOKUP_DATABASE:-wd_notability}
LOOKUP_HOST=${WD_NOTABILITY_LOOKUP_HOST:-tools.db.svc.wikimedia.cloud}
CACHE_BACKEND=${WD_NOTABILITY_CACHE_BACKEND:-mariadb}
CACHE_DATABASE=${WD_NOTABILITY_CACHE_DATABASE:-wd_notability}
CACHE_HOST=${WD_NOTABILITY_CACHE_HOST:-tools.db.svc.wikimedia.cloud}
CACHE_DEFAULTS_FILE=${WD_NOTABILITY_CACHE_DEFAULTS_FILE:-"$HOME/replica.my.cnf"}

run_job() {
  local job_name="$1"
  local command="$2"
  toolforge jobs run "$job_name" \
    --image "$TOOLFORGE_IMAGE" \
    --command "$command"
}

common_env="PYTHONPATH=\"$REPO_DIR\" WD_NOTABILITY_CACHE_BACKEND=\"$CACHE_BACKEND\" WD_NOTABILITY_CACHE_DATABASE=\"$CACHE_DATABASE\" WD_NOTABILITY_CACHE_HOST=\"$CACHE_HOST\" WD_NOTABILITY_CACHE_DEFAULTS_FILE=\"$CACHE_DEFAULTS_FILE\" WD_NOTABILITY_LOOKUP_BACKEND=\"$LOOKUP_BACKEND\" WD_NOTABILITY_LOOKUP_DATABASE=\"$LOOKUP_DATABASE\" WD_NOTABILITY_LOOKUP_HOST=\"$LOOKUP_HOST\""

run_job wd-notability-entitydata "cd \"$REPO_DIR\" && $common_env \"$VENV_DIR/bin/python\" main.py worker --workers \"\${WD_NOTABILITY_ENTITYDATA_WORKERS:-1}\" --poll-seconds \"\${WD_NOTABILITY_ENTITYDATA_POLL_SECONDS:-5.0}\""
run_job wd-notability-inlinks "cd \"$REPO_DIR\" && $common_env \"$VENV_DIR/bin/python\" main.py inlinks-worker --batch-size \"\${WD_NOTABILITY_INLINKS_BATCH_SIZE:-100}\" --run-interval-seconds \"\${WD_NOTABILITY_INLINKS_RUN_INTERVAL_SECONDS:-600}\""
run_job wd-notability-recent-changes "cd \"$REPO_DIR\" && $common_env \"$VENV_DIR/bin/python\" main.py recent-changes-worker --poll-seconds \"\${WD_NOTABILITY_RECENT_CHANGES_POLL_SECONDS:-60}\" --rewind-seconds \"\${WD_NOTABILITY_RECENT_CHANGES_REWIND_SECONDS:-300}\""
run_job wd-notability-cache-sync "cd \"$REPO_DIR\" && $common_env \"$VENV_DIR/bin/python\" main.py cache-sync-worker --batch-size \"\${WD_NOTABILITY_CACHE_SYNC_BATCH_SIZE:-100}\" --run-interval-seconds \"\${WD_NOTABILITY_CACHE_SYNC_RUN_INTERVAL_SECONDS:-60}\""
