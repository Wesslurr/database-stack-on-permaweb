#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_DIR="${DB_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
export HB_ERL_SNAME="hb_sd"
"$DB_DIR/vendor/HyperBEAM_sd/_build/genesis_wasm+rocksdb/rel/hb_sd/bin/hb" eval "$@"
