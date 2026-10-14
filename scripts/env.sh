#!/usr/bin/env bash
# Sumber dari script lain: source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
export DB_DIR="/home/weslur/database"
export HB_SRC="$DB_DIR/vendor/HyperBEAM"
export HB_REL="$HB_SRC/_build/genesis_wasm+rocksdb/rel/hb"
export NODE_RUN_DIR="$DB_DIR/data/node"
export PATH="/home/weslur/erlang/27.0/bin:/home/weslur/.local/bin:/home/weslur/.nvm/versions/node/v22.23.2/bin:/home/weslur/.cargo/bin:$PATH"
export ERLANG_ROCKSDB_OPTS="-DCMAKE_POLICY_VERSION_MINIMUM=3.5"
export CXXFLAGS="-include cstdint"
export CFLAGS="-include stdint.h -D_DEFAULT_SOURCE"
export HB_CONFIG="$DB_DIR/config/node.flat,$DB_DIR/config/node-rocksdb.json,$DB_DIR/config/hymessage.json"
export HB_KEY="$DB_DIR/secrets/wallet.json"
# Runtime deps Gleam (gleam_stdlib) untuk device hymessage — ditambah ke code
# path tanpa menyentuh source HyperBEAM (bukan fork, murni dependency deploy).
export ERL_LIBS="$HB_REL/lib/hymessage_runtime-1.0.0/ebin:$HB_REL/lib/gleam_stdlib-1.0.0/ebin:$HB_REL/lib${ERL_LIBS:+:$ERL_LIBS}"
export CODE_LOADING_MODE="interactive"
