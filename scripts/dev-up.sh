#!/usr/bin/env bash
set -euo pipefail
DIR="$(dirname "${BASH_SOURCE[0]}")"
"$DIR/start-node.sh"
"$DIR/start-arlocal.sh"
echo "[dev-up] node :8734 + arlocal :1984 ready"
