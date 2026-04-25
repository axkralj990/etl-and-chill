#!/usr/bin/env sh
set -eu

mkdir -p /app/data /app/logs

if [ -f /etc/timezone ]; then
  TZ_VALUE="$(cat /etc/timezone)"
  export TZ="${TZ:-$TZ_VALUE}"
fi

# Persist relevant runtime environment so cron jobs can load it.
env | grep -E '^(NOTION_|OURA_|DUCKDB_PATH|TIMEZONE|WEEK_START|DATA_DIR|LEGACY_PATH|PIPELINE_CONFIG_PATH|OPENAI_|ENABLE_OPENAI_TAGS|TZ)=' > /etc/life-cron-env || true

cron

exec streamlit run /app/src/life/dashboard/app.py \
  --server.headless=true \
  --global.developmentMode=false \
  --server.fileWatcherType=none
