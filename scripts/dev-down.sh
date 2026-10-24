#!/usr/bin/env bash
set -euo pipefail
DIR="$(dirname "${BASH_SOURCE[0]}")"
"$DIR/stop-node.sh" 2>/dev/null || true
"$DIR/stop-sd-node.sh" 2>/dev/null || true
"$DIR/stop-arlocal.sh" 2>/dev/null || true
echo "[dev-down] all services stopped."
