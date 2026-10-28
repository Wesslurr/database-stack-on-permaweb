#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export DB_DIR="${DB_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"

check_node() {
  local NODE_LABEL="$1"
  local PORT="$2"
  local DATA_DIR="$3"
  local DEVICE_ENDPOINT="${4:-}"

  local PID="DEAD"
  if [ -f "$DATA_DIR/node.pid" ]; then
    PID="$(cat "$DATA_DIR/node.pid" 2>/dev/null || echo "UNKNOWN")"
  fi

  local HTTP_STATUS="DOWN"
  local TIME_TOTAL="0"
  if curl -sf --max-time 5 -H 'Accept: application/json' -o /dev/null "http://127.0.0.1:$PORT/~meta@1.0/info"; then
    HTTP_STATUS="HEALTHY (HTTP 200)"
  fi

  local DEVICE_STATUS="N/A"
  if [ -n "$DEVICE_ENDPOINT" ]; then
    local DEV_RESP
    DEV_RESP="$(curl -s --max-time 2 -H 'Accept: application/json' "http://127.0.0.1:$PORT/$DEVICE_ENDPOINT" 2>/dev/null || echo "{}")"
    if echo "$DEV_RESP" | grep -q 'pong'; then
      DEVICE_STATUS="ACTIVE (pong)"
    elif echo "$DEV_RESP" | grep -q 'error'; then
      DEVICE_STATUS="ERROR"
    else
      DEVICE_STATUS="RESPONSE_RECEIVED"
    fi
  fi

  local ROCKS_SIZE="0 KB"
  if [ -d "$DATA_DIR/cache-rocksdb" ]; then
    ROCKS_SIZE="$(du -sh "$DATA_DIR/cache-rocksdb" 2>/dev/null | awk '{print $1}' || echo "0 KB")"
  fi

  printf "| %-12s | %-5s | %-7s | %-18s | %-16s | %-10s |\n" \
    "$NODE_LABEL" "$PORT" "$PID" "$HTTP_STATUS" "$DEVICE_STATUS" "$ROCKS_SIZE"
}

echo "+--------------+-------+---------+--------------------+------------------+------------+"
echo "| Node         | Port  | PID     | Health (~meta)     | Device Status    | Store Size |"
echo "+--------------+-------+---------+--------------------+------------------+------------+"
check_node "hb-genesis" 8733 "$DB_DIR/data/genesis" ""
check_node "hb-hot" 8734 "$DB_DIR/data/node" "~hymessage@1.0/ping"
check_node "hb-sd-1" 8735 "$DB_DIR/data/node_sd_1" "~sd@1.0/ping"
check_node "hb-sd-2" 8745 "$DB_DIR/data/node_sd_2" "~sd@1.0/ping"
check_node "hb-sd-3" 8755 "$DB_DIR/data/node_sd_3" "~sd@1.0/ping"
echo "+--------------+-------+---------+--------------------+------------------+------------+"
