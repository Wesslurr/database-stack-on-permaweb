#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/env.sh"

ERLC="${ERLC:-$(command -v erlc || echo "$HOME/erlang/27.0/bin/erlc")}"
HB_SD_REL="$DB_DIR/vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb"
SDD="$DB_DIR/sd"

echo "Compiling sd_ao_client.erl, sd_committer.erl, sd_runtime.erl..."
mkdir -p "$HB_SD_REL/lib/sd_runtime-1.0.0/ebin"
"$ERLC" -I "$SDD/src" -o "$HB_SD_REL/lib/sd_runtime-1.0.0/ebin" "$SDD/src/sd_ao_client.erl" "$SDD/src/sd_committer.erl" "$SDD/src/sd_runtime.erl"
echo "Compiled successfully into $HB_SD_REL/lib/sd_runtime-1.0.0/ebin"
