#!/usr/bin/env bash
set -euo pipefail

jobs=(
  "${WD_NOTABILITY_INIT_JOB_NAME:-wd-notability-bootstrap}"
  wd-notability-entitydata
  wd-notability-inlinks
  wd-notability-recent-changes
  wd-notability-cache-sync
)

for job_name in "${jobs[@]}"; do
  if toolforge jobs delete "$job_name" >/dev/null 2>&1; then
    printf 'Stopped job: %s\n' "$job_name"
  else
    printf 'Job not running or already removed: %s\n' "$job_name"
  fi
done
