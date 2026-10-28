#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export DB_DIR="${DB_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"

stop_single_node() {
  local NODE_NAME="$1"
  local PORT="$2"
  local DATA_DIR="$DB_DIR/data/$NODE_NAME"
  local TMUX_NAME="hb_${NODE_NAME}"

  echo "  [$NODE_NAME] Stopping..."
  tmux kill-session -t "$TMUX_NAME" 2>/dev/null || true

  if [ -f "$DATA_DIR/node.pid" ]; then
    local PID="$(cat "$DATA_DIR/node.pid" 2>/dev/null || true)"
    if [ -n "$PID" ] && kill -0 "$PID" 2>/dev/null; then
      kill -TERM "$PID" 2>/dev/null || true
    fi
    rm -f "$DATA_DIR/node.pid"
  fi

  # Fallback: kill by port if still listening
  fuser -k "${PORT}/tcp" 2>/dev/null || true
  rm -f "$DATA_DIR/cache-rocksdb/LOCK"
  echo "  [$NODE_NAME] Stopped."
}

# Optional node selection: `stop-cluster.sh [genesis|node|node_sd_1|node_sd_2|node_sd_3 ...]`
# With no arguments, all five nodes are stopped.
SELECTED_NODES="${*:-genesis node node_sd_1 node_sd_2 node_sd_3}"
want() { [[ " $SELECTED_NODES " == *" $1 "* ]]; }

echo "Stopping HyperBEAM Local Cluster (nodes: $SELECTED_NODES)..."
want genesis   && stop_single_node "genesis" 8733
want node      && stop_single_node "node" 8734
want node_sd_1 && stop_single_node "node_sd_1" 8735
want node_sd_2 && stop_single_node "node_sd_2" 8745
want node_sd_3 && stop_single_node "node_sd_3" 8755

echo "Selected cluster nodes stopped cleanly."
