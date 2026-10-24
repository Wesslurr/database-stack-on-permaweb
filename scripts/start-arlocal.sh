#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
cd "$DB_DIR"
mkdir -p "$DB_DIR/data"
if curl -sf --max-time 2 -o /dev/null http://127.0.0.1:1984/info; then
  echo "[arlocal] already running on :1984"
  exit 0
fi
pkill -f "start-arlocal.mjs" 2>/dev/null || true
nohup node scripts/start-arlocal.mjs < /dev/null > "$DB_DIR/data/arlocal.log" 2>&1 &
disown || true
for i in $(seq 1 20); do
  if curl -sf --max-time 2 -o /dev/null http://127.0.0.1:1984/; then
    echo "[arlocal] UP on :1984"
    exit 0
  fi
  sleep 1
done
echo "[arlocal] FAILED; last log:"
tail -20 "$DB_DIR/data/arlocal.log" 2>/dev/null || true
exit 1
