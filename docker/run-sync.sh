#!/usr/bin/env sh
set -eu

cd /app

if [ -f /etc/life-cron-env ]; then
  set -a
  # shellcheck disable=SC1091
  . /etc/life-cron-env
  set +a
fi

if [ ! -f /app/.env ] && [ -f /app/.env.example ]; then
  cp /app/.env.example /app/.env
fi

life sync-incremental
