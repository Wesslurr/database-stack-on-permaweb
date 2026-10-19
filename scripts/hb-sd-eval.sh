#!/usr/bin/env bash
set -euo pipefail
export HB_ERL_SNAME="hb_sd"
/home/weslur/database/vendor/HyperBEAM_sd/_build/genesis_wasm+rocksdb/rel/hb_sd/bin/hb eval "$@"
