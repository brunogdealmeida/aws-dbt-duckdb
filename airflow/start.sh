#!/usr/bin/env bash
# Starts a persistent local Airflow instance for this project (webserver/UI +
# scheduler + triggerer, all via `airflow standalone`), backgrounded with a
# PID file. Safe to re-run: it's a no-op if already running.
#
#   ./start.sh          # start (prints the URL + where to find the password)
#   tail -f home/standalone.log   # watch startup / DAG run logs
#   ./stop.sh            # stop
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

export AIRFLOW_HOME="$(pwd)/home"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/dags"

# Without this, boto3/urllib3 calls macOS's SCDynamicStoreCopyProxiesWithOptions
# (system proxy auto-detection) before every HTTP request. LocalExecutor task
# workers are forked processes, and that CoreFoundation call isn't fork-safe on
# macOS — it hangs indefinitely (spinning on signal handling, 100% CPU, no
# network activity) instead of erroring. Confirmed by stack-sampling a stuck
# `dbt_build` task with `sample <pid>`. Setting NO_PROXY skips the lookup.
export NO_PROXY="*"
export no_proxy="*"
# Belt-and-suspenders for the same fork-safety class of issue: makes the
# Objective-C runtime tolerate being touched after fork() instead of
# aborting/hanging. Standard macOS workaround for this bug class (also
# needed by e.g. matplotlib/TensorFlow under multiprocessing on macOS).
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export AIRFLOW__CORE__MP_START_METHOD=spawn

PID_FILE="$AIRFLOW_HOME/standalone.pid"
LOG_FILE="$AIRFLOW_HOME/standalone.log"

if [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "Already running (pid $(cat "$PID_FILE")). UI: http://localhost:8080"
  exit 0
fi

source .venv/bin/activate

nohup airflow standalone > "$LOG_FILE" 2>&1 &
echo $! > "$PID_FILE"

echo "Starting Airflow (pid $(cat "$PID_FILE"))..."
echo "Waiting for the UI to come up on http://localhost:8080 ..."
for _ in $(seq 1 60); do
  if curl -s -o /dev/null http://localhost:8080/ 2>/dev/null; then
    echo "UI is up: http://localhost:8080"
    echo "User: admin"
    PW_FILE="$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated"
    if [ -f "$PW_FILE" ]; then
      python3 -c "import json; print('Password:', json.load(open('$PW_FILE'))['admin'])"
    else
      echo "(password file not created yet — check $LOG_FILE)"
    fi
    exit 0
  fi
  sleep 2
done

echo "Still starting after 2 minutes — check $LOG_FILE"
