#!/usr/bin/env bash
# Sumber dari script lain: source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export DB_DIR="${DB_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
export HB_SRC="${HB_SRC:-$DB_DIR/vendor/HyperBEAM}"
export HB_REL="${HB_REL:-$HB_SRC/_build/genesis_wasm+rocksdb/rel/hb}"
export NODE_RUN_DIR="${NODE_RUN_DIR:-$DB_DIR/data/node}"

# Add standard Erlang / Rust / Node paths if present
if [ -d "$HOME/erlang/27.0/bin" ]; then
  export PATH="$HOME/erlang/27.0/bin:$PATH"
elif [ -d "/home/weslur/erlang/27.0/bin" ]; then
  export PATH="/home/weslur/erlang/27.0/bin:$PATH"
fi
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

export ERLANG_ROCKSDB_OPTS="-DCMAKE_POLICY_VERSION_MINIMUM=3.5"
export CXXFLAGS="-include cstdint"
export CFLAGS="-include stdint.h -D_DEFAULT_SOURCE"
export HB_CONFIG="$DB_DIR/config/node.flat,$DB_DIR/config/node-rocksdb.json,$DB_DIR/config/hymessage.json"
export HB_KEY="${WALLET_PATH:-$DB_DIR/secrets/wallet.json}"
# Runtime deps Gleam (gleam_stdlib) untuk device hymessage — ditambah ke code
# path tanpa menyentuh source HyperBEAM (bukan fork, murni dependency deploy).
export ERL_LIBS="$HB_REL/lib/hymessage_runtime-1.0.0/ebin:$HB_REL/lib/gleam_stdlib-1.0.0/ebin:$HB_REL/lib${ERL_LIBS:+:$ERL_LIBS}"
export CODE_LOADING_MODE="interactive"
export HYDB_CLUSTER_SECRET="${HYDB_CLUSTER_SECRET:-cluster_secret_for_local_mesh_test_only_32_bytes_long}"
export HYDB_ADMIN_TOKEN="${HYDB_ADMIN_TOKEN:-admin_token_for_local_mesh_test_only_32_bytes_long}"
export HYDB_PROFILE="${HYDB_PROFILE:-test}"
export HYDB_CLUSTER_MEMBERS="${HYDB_CLUSTER_MEMBERS:-node_sd_1,node_sd_2,node_sd_3}"
export HYDB_CLUSTER_SIZE="${HYDB_CLUSTER_SIZE:-3}"
