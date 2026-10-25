#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_DIR="${DB_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
HB_SD_REL="$DB_DIR/vendor/HyperBEAM_sd/_build/genesis_wasm+rocksdb/rel/hb_sd"

NODE_SD_RUN_DIR="${NODE_SD_RUN_DIR:-$DB_DIR/data/node_sd}"
if [ -f "$NODE_SD_RUN_DIR/committer.pid" ]; then
  CPID="$(cat "$NODE_SD_RUN_DIR/committer.pid" 2>/dev/null || true)"
  if [ -n "$CPID" ] && kill -0 "$CPID" 2>/dev/null; then
    echo "[sd-node] Stopping committer worker (PID $CPID)..."
    kill "$CPID" 2>/dev/null || true
  fi
  rm -f "$NODE_SD_RUN_DIR/committer.pid"
fi
pkill -f "start-committer-worker.sh" 2>/dev/null || true
pkill -f "ao_process/committer_daemon.mjs" 2>/dev/null || true

if [ -f "$HB_SD_REL/bin/hb" ]; then
  "$HB_SD_REL/bin/hb" stop 2>/dev/null || true
fi
pkill -9 -f "hb_sd" 2>/dev/null || true
sleep 1
for i in $(seq 1 10); do
  if ! curl -sf --max-time 1 -o /dev/null http://127.0.0.1:8735/~meta@1.0/info; then
    break
  fi
  sleep 0.5
done
echo "[sd-node] stopped."
