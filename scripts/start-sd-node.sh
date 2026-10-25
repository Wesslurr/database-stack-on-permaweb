#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export DB_DIR="${DB_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
export HB_SD_REL="$DB_DIR/vendor/HyperBEAM_sd/_build/genesis_wasm+rocksdb/rel/hb_sd"
export NODE_SD_RUN_DIR="$DB_DIR/data/node_sd"

if [ -d "$HOME/erlang/27.0/bin" ]; then
  export PATH="$HOME/erlang/27.0/bin:$PATH"
elif [ -d "/home/weslur/erlang/27.0/bin" ]; then
  export PATH="/home/weslur/erlang/27.0/bin:$PATH"
fi
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

export ERLANG_ROCKSDB_OPTS="-DCMAKE_POLICY_VERSION_MINIMUM=3.5"
export CXXFLAGS="-include cstdint"
export CFLAGS="-include stdint.h -D_DEFAULT_SOURCE"
if [ -f "$DB_DIR/config/sd.json" ]; then
  export HB_CONFIG="$DB_DIR/config/node_sd.flat,$DB_DIR/config/node-rocksdb.json,$DB_DIR/config/sd.json"
else
  export HB_CONFIG="$DB_DIR/config/node_sd.flat,$DB_DIR/config/node-rocksdb.json"
fi
export HB_KEY="${WALLET_SU_PATH:-$DB_DIR/secrets/wallet_su.json}"
export HB_ERL_SNAME="hb_sd"
export ERL_LIBS="$HB_SD_REL/lib/sd_runtime-1.0.0/ebin:$HB_SD_REL/lib/gleam_stdlib-1.0.0/ebin:$HB_SD_REL/lib${ERL_LIBS:+:$ERL_LIBS}"
export CODE_LOADING_MODE="interactive"

mkdir -p "$NODE_SD_RUN_DIR"
cd "$NODE_SD_RUN_DIR"

if [ ! -f "$DB_DIR/secrets/wallet_su.json" ]; then
  echo "[sd-node] Generating fresh SU wallet..."
  (cd "$DB_DIR" && node "$DB_DIR/scripts/gen-wallet-su.mjs")
fi

SU_PORT="${SU_PORT:-8735}"

start_committer_if_enabled() {
  if [ "${HYDB_COMMITTER_ENABLED:-false}" = "true" ]; then
    NODE_BIN="${NODE_BIN:-node}"
    if ! command -v "$NODE_BIN" >/dev/null 2>&1; then
      echo "[sd-node] FATAL: HYDB_COMMITTER_ENABLED=true but '$NODE_BIN' is not found in PATH." >&2
      echo "[sd-node] Committer owner node cannot start without Node.js runtime. Failing fast." >&2
      exit 1
    fi
    if [ ! -f "$DB_DIR/ao_process/committer_daemon.mjs" ]; then
      echo "[sd-node] FATAL: HYDB_COMMITTER_ENABLED=true but $DB_DIR/ao_process/committer_daemon.mjs is missing." >&2
      exit 1
    fi
    if [ ! -d "$DB_DIR/ao_process/node_modules" ] && [ ! -d "$DB_DIR/node_modules" ]; then
      echo "[sd-node] FATAL: HYDB_COMMITTER_ENABLED=true but AO dependencies (node_modules) are not installed." >&2
      exit 1
    fi

    # Check if already running
    if [ -f "$NODE_SD_RUN_DIR/committer.pid" ]; then
      EXISTING_PID="$(cat "$NODE_SD_RUN_DIR/committer.pid" 2>/dev/null || true)"
      if [ -n "$EXISTING_PID" ] && kill -0 "$EXISTING_PID" 2>/dev/null; then
        echo "[sd-node] Committer worker already running (PID $EXISTING_PID)"
        return 0
      fi
    fi

    echo "[sd-node] Starting colocated AO Committer Worker..."
    NODE_BIN="$NODE_BIN" "$DB_DIR/scripts/start-committer-worker.sh" > "$NODE_SD_RUN_DIR/committer.log" 2>&1 &
    COMMITTER_PID=$!
    echo "$COMMITTER_PID" > "$NODE_SD_RUN_DIR/committer.pid"
    echo "[sd-node] Committer worker started with PID $COMMITTER_PID"
  fi
}

if [ "${1:-}" = "--check-committer" ]; then
  start_committer_if_enabled
  exit 0
fi

if curl -sf --max-time 2 -o /dev/null "http://127.0.0.1:$SU_PORT/~meta@1.0/info"; then
  echo "[sd-node] already running on :$SU_PORT"
  start_committer_if_enabled
  exit 0
fi

sed -e "s|secrets/wallet_su.json|$DB_DIR/secrets/wallet_su.json|g" \
    -e "s|port: 8735|port: $SU_PORT|g" \
    "$DB_DIR/config/node_sd.flat" > "$HB_SD_REL/config.flat"
"$HB_SD_REL/bin/hb" daemon

for i in $(seq 1 45); do
  if curl -sf --max-time 2 -o /dev/null "http://127.0.0.1:$SU_PORT/~meta@1.0/info"; then
    echo "[sd-node] UP on :$SU_PORT"
    start_committer_if_enabled
    exit 0
  fi
  sleep 1
done

echo "[sd-node] FAILED to come up; last log:"
tail -n 40 "$HB_SD_REL/log/erlang.log.1" 2>/dev/null \
  || tail -n 40 "$NODE_SD_RUN_DIR/logs/erlang.log.1" 2>/dev/null \
  || true
exit 1
