#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export DB_DIR="${DB_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
NODE_SD_RUN_DIR="${NODE_SD_RUN_DIR:-$DB_DIR/data/${HYDB_NODE_ID:-node_sd}}"
SU_PORT="${SU_PORT:-8735}"

# Clean up any stale halted markers from previous runs
mkdir -p "$NODE_SD_RUN_DIR"
rm -f "$NODE_SD_RUN_DIR/CONTAINER_HALTED" "$NODE_SD_RUN_DIR/halted.json"

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

  # B. Check Colocated In-BEAM Committer Worker (if this node is the designated committer owner)
  if [ "${HYDB_COMMITTER_ENABLED:-false}" = "true" ]; then
    STATUS_FILE="${HYDB_COMMITTER_CURSOR_PATH:-$DB_DIR/data/committer/committer_cursor.json}"
    STATUS_DIR="$(dirname "$STATUS_FILE")"
    HEARTBEAT_JSON="$STATUS_DIR/committer_status.json"

    # 1. Check for fatal fail-closed condition (exit code 78 / halted)
    IS_FAIL_CLOSED=false
    if [ -f "$HEARTBEAT_JSON" ]; then
      if grep -q '"health": "failed_closed"' "$HEARTBEAT_JSON" 2>/dev/null || \
         grep -q '"status": "halted"' "$HEARTBEAT_JSON" 2>/dev/null; then
        IS_FAIL_CLOSED=true
      fi
    fi

    if [ "$IS_FAIL_CLOSED" = "true" ]; then
      echo "[sd-foreground] FATAL: Committer exited in fatal fail-closed state (code 78)." >&2
      echo "[sd-foreground] Halting container execution to prevent crash loop. Container status is now halted/unhealthy." >&2
      "$SCRIPT_DIR/stop-sd-node.sh" || true
      echo "fail_closed_fatal" > "$NODE_SD_RUN_DIR/CONTAINER_HALTED"
      echo "{\"status\":\"halted\",\"reason\":\"fatal_fail_closed_exit_78\",\"timestamp\":\"$(date -u +"%Y-%m-%dT%H:%M:%SZ")\"}" > "$NODE_SD_RUN_DIR/halted.json"

      while true; do
        sleep 3600 &
        wait $!
      done
    fi

    # 2. Check if in-BEAM committer GenServer is still alive and responsive via status query
    SD_STATUS="$(curl -sf --max-time 2 -H 'Accept: application/json' "http://127.0.0.1:$SU_PORT/~sd@1.0/status" 2>/dev/null || echo "{}")"
    if echo "$SD_STATUS" | grep -q '"committer"' && echo "$SD_STATUS" | grep -q '"status":"disabled"'; then
      echo "[sd-foreground] FATAL: In-BEAM committer GenServer died or is disabled while HYDB_COMMITTER_ENABLED=true!" >&2
      "$SCRIPT_DIR/stop-sd-node.sh" || true
      exit 1
    fi
  fi

  sleep 2
done
