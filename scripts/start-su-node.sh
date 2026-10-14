#!/usr/bin/env bash
set -euo pipefail

export DB_DIR="/home/weslur/database"
export HB_SU_REL="$DB_DIR/vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb_su"
export NODE_SU_RUN_DIR="$DB_DIR/data/node_su"
export PATH="/home/weslur/erlang/27.0/bin:/home/weslur/.local/bin:/home/weslur/.nvm/versions/node/v22.23.2/bin:/home/weslur/.cargo/bin:$PATH"
export ERLANG_ROCKSDB_OPTS="-DCMAKE_POLICY_VERSION_MINIMUM=3.5"
export CXXFLAGS="-include cstdint"
export CFLAGS="-include stdint.h -D_DEFAULT_SOURCE"
export HB_CONFIG="$DB_DIR/config/node_su.flat,$DB_DIR/config/node-rocksdb.json"
export HB_KEY="$DB_DIR/secrets/wallet_su.json"
export HB_ERL_SNAME="hb_su"
export ERL_LIBS="$HB_SU_REL/lib/su_runtime-1.0.0/ebin:$HB_SU_REL/lib/hymessage_runtime-1.0.0/ebin:$HB_SU_REL/lib/gleam_stdlib-1.0.0/ebin:$HB_SU_REL/lib${ERL_LIBS:+:$ERL_LIBS}"
export CODE_LOADING_MODE="interactive"

mkdir -p "$NODE_SU_RUN_DIR"
cd "$NODE_SU_RUN_DIR"

if curl -sf --max-time 2 -o /dev/null http://127.0.0.1:8735/~meta@1.0/info; then
  echo "[su-node] already running on :8735"
  "$HB_SU_REL/bin/hb" eval "su_runtime:ensure_started()." >/dev/null 2>&1 || true
  exit 0
fi

cp "$DB_DIR/config/node_su.flat" "$HB_SU_REL/config.flat"
"$HB_SU_REL/bin/hb" daemon

for i in $(seq 1 45); do
  if curl -sf --max-time 2 -o /dev/null http://127.0.0.1:8735/~meta@1.0/info; then
    echo "[su-node] UP on :8735"
    "$HB_SU_REL/bin/hb" eval "su_runtime:ensure_started()." >/dev/null 2>&1 || true
    exit 0
  fi
  sleep 1
done

echo "[su-node] FAILED to come up; last log:"
tail -n 40 "$HB_SU_REL/log/erlang.log.1" 2>/dev/null \
  || tail -n 40 "$NODE_SU_RUN_DIR/logs/erlang.log.1" 2>/dev/null \
  || true
exit 1
