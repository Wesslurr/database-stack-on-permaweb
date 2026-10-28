#!/usr/bin/env bash
EXPR=$(cat "$1")
/home/weslur/database/vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb/bin/hb eval "$EXPR"
