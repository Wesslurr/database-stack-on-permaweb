#!/usr/bin/env bash
set -euo pipefail
export HB_ERL_SNAME="hb_su"
/home/weslur/database/vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb_su/bin/hb eval "$@"
