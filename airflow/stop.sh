#!/usr/bin/env bash
# Stops the local Airflow instance started by start.sh.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

PID_FILE="home/standalone.pid"

if [ ! -f "$PID_FILE" ] || ! kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Not running."
  rm -f "$PID_FILE"
  exit 0
fi

PID="$(cat "$PID_FILE")"
kill "$PID"
rm -f "$PID_FILE"
echo "Stopped (pid $PID)."
