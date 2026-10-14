#!/usr/bin/env bash
set -euo pipefail
DB_DIR="/home/weslur/database"
HB_SU_REL="$DB_DIR/vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb_su"

"$HB_SU_REL/bin/hb" stop 2>/dev/null || true
pkill -f "rel/hb_su" 2>/dev/null || true
echo "[su-node] stopped."
