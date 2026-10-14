#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
"$HB_REL/bin/hb" stop || true
sleep 1
if curl -sf --max-time 2 -o /dev/null http://127.0.0.1:8734/~meta@1.0/info; then
  echo "[node] still responding after stop; check manually"
  exit 1
fi
echo "[node] stopped"
