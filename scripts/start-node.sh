#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
mkdir -p "$NODE_RUN_DIR"
cd "$NODE_RUN_DIR"

if [ ! -f "$DB_DIR/secrets/wallet.json" ]; then
  echo "[node] Generating fresh wallet..."
  (cd "$DB_DIR" && node "$DB_DIR/scripts/gen-wallet.mjs")
fi

HOT_PORT="${HOT_PORT:-8734}"

if curl -sf --max-time 2 -o /dev/null "http://127.0.0.1:$HOT_PORT/~meta@1.0/info"; then
  echo "[node] already running on :$HOT_PORT"
  exit 0
fi
# relx selalu cd ke release root dan load config.flat dari sana bila HB_CONFIG
# tidak terbaca - sinkronkan dulu biar tidak baca config basi.
sed -e "s|secrets/wallet.json|$DB_DIR/secrets/wallet.json|g" \
    -e "s|port: 8734|port: $HOT_PORT|g" \
    "$DB_DIR/config/node.flat" > "$HB_REL/config.flat"
"$HB_REL/bin/hb" daemon
for i in $(seq 1 45); do
  if curl -sf --max-time 2 -o /dev/null "http://127.0.0.1:$HOT_PORT/~meta@1.0/info"; then
    echo "[node] UP on :$HOT_PORT"
    exit 0
  fi
  sleep 1
done
echo "[node] FAILED to come up; last log:"
tail -n 40 "$NODE_RUN_DIR/logs/erlang.log.1" 2>/dev/null \
  || tail -n 40 "$HB_REL/log/erlang.log.1" 2>/dev/null \
  || tail -n 40 "$HB_REL/logs/erlang.log.1" 2>/dev/null \
  || true
exit 1
