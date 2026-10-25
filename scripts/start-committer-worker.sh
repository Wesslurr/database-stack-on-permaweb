#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export DB_DIR="${DB_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"

export NODE_BIN="${NODE_BIN:-node}"

if [ "${HYDB_COMMITTER_ENABLED:-true}" = "false" ]; then
  echo "[committer-worker] HYDB_COMMITTER_ENABLED=false. Worker disabled."
  exit 0
fi

if ! command -v "$NODE_BIN" >/dev/null 2>&1; then
  echo "[committer-worker] ERROR: Node.js runtime ($NODE_BIN) not found in PATH. Committer worker cannot start." >&2
  exit 1
fi

COMMS_SCRIPT="$DB_DIR/ao_process/committer_daemon.mjs"
if [ ! -f "$COMMS_SCRIPT" ]; then
  echo "[committer-worker] ERROR: Committer daemon script not found at $COMMS_SCRIPT" >&2
  exit 1
fi

CHILD_PID=""
terminate() {
  echo "[committer-worker] Received termination signal. Forwarding to daemon..."
  if [ -n "$CHILD_PID" ] && kill -0 "$CHILD_PID" 2>/dev/null; then
    kill -TERM "$CHILD_PID" 2>/dev/null || true
    wait "$CHILD_PID" 2>/dev/null || true
  fi
  exit 0
}
trap terminate SIGTERM SIGINT

MAX_RETRIES="${HYDB_COMMITTER_MAX_RETRIES:-5}"
RETRY_COUNT=0
BASE_BACKOFF_SEC=2

echo "[committer-worker] Starting AO Committer Supervisor..."

while true; do
  echo "[committer-worker] Spawning $NODE_BIN $COMMS_SCRIPT..."
  "$NODE_BIN" "$COMMS_SCRIPT" &
  CHILD_PID=$!

  wait "$CHILD_PID" && EXIT_CODE=0 || EXIT_CODE=$?
  CHILD_PID=""

  if [ "$EXIT_CODE" -eq 0 ]; then
    echo "[committer-worker] Committer exited cleanly (code 0). Exiting supervisor."
    exit 0
  fi

  # Exit code 78: EX_CONFIG / fatal fail-closed (state divergence, hash mismatch, lock held, process id missing in prod)
  if [ "$EXIT_CODE" -eq 78 ]; then
    echo "[committer-worker] FATAL: Committer exited with code 78 (fail-closed condition detected: divergence, lock conflict, or invalid configuration)." >&2
    echo "[committer-worker] Halting supervisor without restart to prevent crash loops and state corruption." >&2
    exit 78
  fi

  RETRY_COUNT=$((RETRY_COUNT + 1))
  if [ "$RETRY_COUNT" -gt "$MAX_RETRIES" ]; then
    echo "[committer-worker] ERROR: Max retries ($MAX_RETRIES) reached after consecutive transient failures. Supervisor exiting with error." >&2
    exit "$EXIT_CODE"
  fi

  BACKOFF_SEC=$((BASE_BACKOFF_SEC * RETRY_COUNT))
  echo "[committer-worker] Committer crashed with exit code $EXIT_CODE (transient attempt $RETRY_COUNT/$MAX_RETRIES). Retrying in ${BACKOFF_SEC}s..."
  sleep "$BACKOFF_SEC"
done
