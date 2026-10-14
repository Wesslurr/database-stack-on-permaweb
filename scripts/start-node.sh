#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
mkdir -p "$NODE_RUN_DIR"
cd "$NODE_RUN_DIR"
if curl -sf --max-time 2 -o /dev/null http://127.0.0.1:8734/~meta@1.0/info; then
  echo "[node] already running on :8734"
  exit 0
fi
# relx selalu cd ke release root dan load config.flat dari sana bila HB_CONFIG
# tidak terbaca - sinkronkan dulu biar tidak baca config basi.
cp "$DB_DIR/config/node.flat" "$HB_REL/config.flat"
"$HB_REL/bin/hb" daemon
for i in $(seq 1 45); do
  if curl -sf --max-time 2 -o /dev/null http://127.0.0.1:8734/~meta@1.0/info; then
    echo "[node] UP on :8734"
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
