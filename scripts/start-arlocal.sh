#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
cd "$DB_DIR"
if curl -sf --max-time 2 -o /dev/null http://127.0.0.1:1984/; then
  echo "[arlocal] already running on :1984"
  exit 0
fi
tmux kill-session -t db-arlocal 2>/dev/null || true
tmux new-session -d -s db-arlocal "cd $DB_DIR && node scripts/start-arlocal.mjs"
for i in $(seq 1 20); do
  if curl -sf --max-time 2 -o /dev/null http://127.0.0.1:1984/; then
    echo "[arlocal] UP on :1984"
    exit 0
  fi
  sleep 1
done
echo "[arlocal] FAILED; tmux pane:"
tmux capture-pane -t db-arlocal -p 2>/dev/null | tail -20 || true
exit 1
