#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_DIR="${DB_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
HB_SU_REL="$DB_DIR/vendor/HyperBEAM/_build/genesis_wasm+rocksdb/rel/hb_su"

# 1. Clean cache and logs
rm -rf "$HB_SU_REL/cache-rocksdb" "$HB_SU_REL/log" "$HB_SU_REL/logs"
mkdir -p "$HB_SU_REL/cache-rocksdb" "$HB_SU_REL/log" "$HB_SU_REL/logs"

# 2. Sync config
cp "$DB_DIR/config/node_su.flat" "$HB_SU_REL/config.flat"

# 3. Ensure vm.args has -sname hb_su
printf -- '-sname "hb_su"\n' > "$HB_SU_REL/releases/0.0.1/vm.args"

echo "[setup-hb-su] Configured hb_su release successfully."
