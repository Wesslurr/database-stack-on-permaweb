#!/usr/bin/env bash
set -euo pipefail
DB_DIR="/home/weslur/database"
HB_SU_REL="$DB_DIR/vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb_su"
ERLC="/home/weslur/erlang/27.0/bin/erlc"

mkdir -p "$HB_SU_REL/lib/su_runtime-1.0.0/ebin"
"$ERLC" -o "$HB_SU_REL/lib/su_runtime-1.0.0/ebin" "$DB_DIR/pool_su/src/su_runtime.erl"
echo "[pool_su] su_runtime.beam built to $HB_SU_REL/lib/su_runtime-1.0.0/ebin"
