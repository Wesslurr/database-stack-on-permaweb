#!/usr/bin/env bash
set -euo pipefail
pkill -f "start-arlocal.mjs" 2>/dev/null || true
tmux kill-session -t db-arlocal 2>/dev/null || true
echo "[arlocal] stopped."
