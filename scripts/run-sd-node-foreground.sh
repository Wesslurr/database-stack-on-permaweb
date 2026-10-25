#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export DB_DIR="${DB_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
NODE_SD_RUN_DIR="${NODE_SD_RUN_DIR:-$DB_DIR/data/node_sd}"
SU_PORT="${SU_PORT:-8735}"

echo "[sd-foreground] Starting HyperBEAM SD Node and Native Services..."

cleanup() {
  echo "[sd-foreground] Caught termination signal. Gracefully stopping SD node stack..."
  "$SCRIPT_DIR/stop-sd-node.sh" || true
  exit 0
}
trap cleanup SIGTERM SIGINT

# 1. Start SD node and colocated committer (if enabled)
"$SCRIPT_DIR/start-sd-node.sh"

echo "[sd-foreground] SD Node initialization complete. Entering active supervisor monitoring loop..."

while true; do
  # A. Check HyperBEAM Health
  if ! curl -sf --max-time 2 -o /dev/null "http://127.0.0.1:$SU_PORT/~meta@1.0/info"; then
    echo "[sd-foreground] FATAL: HyperBEAM SD node is no longer responding on port :$SU_PORT!" >&2
    "$SCRIPT_DIR/stop-sd-node.sh" || true
    exit 1
  fi

  # B. Check Colocated Committer Worker (if this node is the designated committer owner)
  if [ "${HYDB_COMMITTER_ENABLED:-false}" = "true" ]; then
    PID_FILE="$NODE_SD_RUN_DIR/committer.pid"
    if [ ! -f "$PID_FILE" ]; then
      echo "[sd-foreground] FATAL: HYDB_COMMITTER_ENABLED=true but committer PID file ($PID_FILE) is missing!" >&2
      "$SCRIPT_DIR/stop-sd-node.sh" || true
      exit 1
    fi

    CPID="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [ -z "$CPID" ] || ! kill -0 "$CPID" 2>/dev/null; then
      echo "[sd-foreground] FATAL: Committer worker process (PID ${CPID:-unknown}) has terminated!" >&2

      # Check if committer died due to fatal fail-closed condition (exit code 78)
      STATUS_FILE="${HYDB_COMMITTER_CURSOR_PATH:-$DB_DIR/data/committer/committer_cursor.json}"
      STATUS_DIR="$(dirname "$STATUS_FILE")"
      HEARTBEAT_JSON="$STATUS_DIR/committer_status.json"

      IS_FAIL_CLOSED=false
      if [ -f "$HEARTBEAT_JSON" ]; then
        if grep -q '"health": "failed_closed"' "$HEARTBEAT_JSON" 2>/dev/null || \
           grep -q '"status": "halted"' "$HEARTBEAT_JSON" 2>/dev/null; then
          IS_FAIL_CLOSED=true
        fi
      fi

      "$SCRIPT_DIR/stop-sd-node.sh" || true

      if [ "$IS_FAIL_CLOSED" = "true" ]; then
        echo "[sd-foreground] Committer exited in fatal fail-closed state (code 78). Halting container without crash loop." >&2
        exit 78
      else
        echo "[sd-foreground] Committer exited unexpectedly. Exiting container with error code 1." >&2
        exit 1
      fi
    fi
  fi

  sleep 2
done
