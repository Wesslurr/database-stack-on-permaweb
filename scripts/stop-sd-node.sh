#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_DIR="${DB_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
HB_SD_REL="$DB_DIR/vendor/HyperBEAM_sd/_build/genesis_wasm+rocksdb/rel/hb_sd"

if [ -f "$HB_SD_REL/bin/hb" ]; then
  "$HB_SD_REL/bin/hb" stop 2>/dev/null || true
fi
pkill -9 -f "hb_sd" 2>/dev/null || true
sleep 1
for i in $(seq 1 10); do
  if ! curl -sf --max-time 1 -o /dev/null http://127.0.0.1:8735/~meta@1.0/info; then
    break
  fi
  sleep 0.5
done
echo "[sd-node] stopped."
