#!/usr/bin/env bash
set -euo pipefail
DB_DIR="/home/weslur/database"
HB_SD_REL="$DB_DIR/vendor/HyperBEAM_sd/_build/genesis_wasm+rocksdb/rel/hb_sd"

if [ -f "$HB_SD_REL/bin/hb" ]; then
  "$HB_SD_REL/bin/hb" stop 2>/dev/null || true
fi
pkill -f "rel/hb_sd" 2>/dev/null || true
echo "[sd-node] stopped."
