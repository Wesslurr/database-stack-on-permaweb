#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
mkdir -p "$DB_DIR/vendor"
if [ -d "$HB_SRC/.git" ]; then
  echo "[01] already cloned: $HB_SRC"
  exit 0
fi
git clone /home/weslur/HyperBEAM "$HB_SRC"
git -C "$HB_SRC" submodule update --init --recursive
echo "[01] cloned:"
git -C "$HB_SRC" log --oneline -1
git -C "$HB_SRC" status -sb
