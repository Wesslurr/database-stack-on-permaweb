#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export DB_DIR="${DB_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
source "$SCRIPT_DIR/env.sh"

export ERL_LIBS="$HB_REL/lib/sd_runtime-1.0.0/ebin:$HB_REL/lib/hymessage_runtime-1.0.0/ebin:$HB_REL/lib/gleam_stdlib-1.0.0/ebin:$HB_REL/lib${ERL_LIBS:+:$ERL_LIBS}"

mkdir -p "$DB_DIR/data/seed_db"

start_single_node() {
  local NODE_NAME="$1"
  local PORT="$2"
  local FLAT_CFG="$3"
  local STORE_CFG="$4"
  local WALLET_PATH="$5"
  local EXTRA_CFGS="${6:-}"
  local DATA_DIR="$DB_DIR/data/$NODE_NAME"
  local LOG_DIR="$DATA_DIR/logs"
  local TMUX_NAME="hb_${NODE_NAME}"

  mkdir -p "$LOG_DIR"
  mkdir -p "$DATA_DIR/cache-mainnet"

  # Seed cache-rocksdb if empty or absent
  if [ ! -d "$DATA_DIR/cache-rocksdb" ] || [ -z "$(ls -A "$DATA_DIR/cache-rocksdb" 2>/dev/null)" ]; then
    echo "  [$NODE_NAME] Initializing RocksDB store from data/seed_db..."
    rm -rf "$DATA_DIR/cache-rocksdb"
    cp -r "$DB_DIR/data/seed_db" "$DATA_DIR/cache-rocksdb"
    rm -f "$DATA_DIR/cache-rocksdb/LOCK"
  else
    rm -f "$DATA_DIR/cache-rocksdb/LOCK"
  fi

  # Check if already running on this port
  if curl -sf --max-time 4 -H "Accept: application/json" -o /dev/null "http://127.0.0.1:$PORT/~meta@1.0/info"; then
    echo "  [$NODE_NAME] Already alive on :$PORT"
    return 0
  fi

  # Kill any stale tmux session for this node
  tmux kill-session -t "$TMUX_NAME" 2>/dev/null || true

  # Set node-specific configs
  export VMARGS_PATH="$DB_DIR/config/vm_${NODE_NAME}.args"
  export RUNNER_LOG_DIR="$LOG_DIR"
  export HB_ERL_SNAME="$TMUX_NAME"
  export HB_KEY="$WALLET_PATH"
  export WALLET_PATH="$WALLET_PATH"
  export HYDB_NODE_ID="$NODE_NAME"
  export HYDB_WAL_PATH="$DATA_DIR/wal.log"
  export HYDB_CURSOR_PATH="$DATA_DIR/cursor.json"
  export AO_GATEWAY_URL="http://127.0.0.1:8733"
  export HYDB_CLUSTER_MEMBERS="${HYDB_CLUSTER_MEMBERS:-node_sd_1,node_sd_2,node_sd_3}"
  export HYDB_CLUSTER_SIZE="${HYDB_CLUSTER_SIZE:-3}"
  export HOT_TCP_PORT="8736"

  local ALL_CFGS="$FLAT_CFG,$STORE_CFG"
  if [ -n "$EXTRA_CFGS" ]; then
    ALL_CFGS="$ALL_CFGS,$EXTRA_CFGS"
  fi
  export HB_CONFIG="$ALL_CFGS"

  # Launch HyperBEAM instance inside persistent detached tmux session
  tmux new-session -d -s "$TMUX_NAME" "bash -c 'cd \"$DB_DIR\" && export VMARGS_PATH=\"$VMARGS_PATH\" RUNNER_LOG_DIR=\"$RUNNER_LOG_DIR\" HB_ERL_SNAME=\"$HB_ERL_SNAME\" HB_KEY=\"$HB_KEY\" HB_CONFIG=\"$HB_CONFIG\" ERL_LIBS=\"$ERL_LIBS\" CODE_LOADING_MODE=\"interactive\" WALLET_PATH=\"$WALLET_PATH\" HYDB_NODE_ID=\"$HYDB_NODE_ID\" HYDB_WAL_PATH=\"$HYDB_WAL_PATH\" HYDB_CURSOR_PATH=\"$HYDB_CURSOR_PATH\" HYDB_COMMITTER_ENABLED=\"${HYDB_COMMITTER_ENABLED:-false}\" AO_GATEWAY_URL=\"$AO_GATEWAY_URL\" HYDB_CLUSTER_SECRET=\"$HYDB_CLUSTER_SECRET\" HYDB_ADMIN_TOKEN=\"$HYDB_ADMIN_TOKEN\" HYDB_PROFILE=\"$HYDB_PROFILE\" HYDB_CLUSTER_MEMBERS=\"$HYDB_CLUSTER_MEMBERS\" HYDB_CLUSTER_SIZE=\"$HYDB_CLUSTER_SIZE\" HOT_TCP_PORT=\"$HOT_TCP_PORT\" DB_DIR=\"$DB_DIR\" && \"$HB_REL/bin/hb\" foreground 2>&1 | tee \"$LOG_DIR/stdout.log\"'"

  # Await node readiness
  for i in $(seq 1 40); do
    if curl -sf --max-time 4 -H "Accept: application/json" -o /dev/null "http://127.0.0.1:$PORT/~meta@1.0/info"; then
      local NPID
      NPID="$(tmux list-panes -t "$TMUX_NAME" -F "#{pane_pid}" 2>/dev/null || echo "tmux")"
      echo $NPID > "$DATA_DIR/node.pid"
      echo "  [$NODE_NAME] UP on :$PORT (tmux: $TMUX_NAME, PID: $NPID)"
      return 0
    fi
    sleep 0.5
  done

  echo "  [$NODE_NAME] FAILED to start within 15s. Last log:"
  tail -n 25 "$LOG_DIR/stdout.log" 2>/dev/null || true
  return 1
}

# Optional node selection: `start-cluster.sh [genesis|node|node_sd_1|node_sd_2|node_sd_3 ...]`
# With no arguments, all five nodes are started.
SELECTED_NODES="${*:-genesis node node_sd_1 node_sd_2 node_sd_3}"
want() { [[ " $SELECTED_NODES " == *" $1 "* ]]; }

echo "=========================================================="
echo "Starting HyperBEAM 5-Node Local Cluster (nodes: $SELECTED_NODES)"
echo "Unified Runtime: $HB_REL"
echo "=========================================================="

if want genesis; then
  echo "1. Starting hb-genesis (:8733)..."
  start_single_node "genesis" 8733 "$DB_DIR/config/node_genesis.flat" "$DB_DIR/config/store_genesis.json" "$DB_DIR/secrets/wallet_genesis.json"
fi

if want node; then
  echo "2. Starting hb-hot (:8734, TCP :8736)..."
  start_single_node "node" 8734 "$DB_DIR/config/node_hot.flat" "$DB_DIR/config/store_hot.json" "$DB_DIR/secrets/wallet.json" "$DB_DIR/config/hymessage.json"
fi

if want node_sd_1; then
  echo "3. Starting hb-sd-1 (:8735, Committer Owner)..."
  export HYDB_COMMITTER_ENABLED=true
  start_single_node "node_sd_1" 8735 "$DB_DIR/config/node_sd_1.flat" "$DB_DIR/config/store_sd_1.json" "$DB_DIR/secrets/wallet_su.json" "$DB_DIR/config/sd.json"
fi

if want node_sd_2; then
  echo "4. Starting hb-sd-2 (:8745)..."
  export HYDB_COMMITTER_ENABLED=false
  start_single_node "node_sd_2" 8745 "$DB_DIR/config/node_sd_2.flat" "$DB_DIR/config/store_sd_2.json" "$DB_DIR/secrets/wallet_sd_2.json" "$DB_DIR/config/sd.json"
fi

if want node_sd_3; then
  echo "5. Starting hb-sd-3 (:8755)..."
  export HYDB_COMMITTER_ENABLED=false
  start_single_node "node_sd_3" 8755 "$DB_DIR/config/node_sd_3.flat" "$DB_DIR/config/store_sd_3.json" "$DB_DIR/secrets/wallet_sd_3.json" "$DB_DIR/config/sd.json"
fi

echo "=========================================================="
echo "Cluster startup complete! Verifying health across all nodes:"
"$SCRIPT_DIR/status-cluster.sh"
