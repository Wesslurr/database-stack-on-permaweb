#!/usr/bin/env bash
set -euo pipefail
DIR="$(dirname "${BASH_SOURCE[0]}")"
"$DIR/start-node.sh"
"$DIR/start-su-node.sh"
"$DIR/start-arlocal.sh"
echo "[dev-up] hot :8734 + su :8735 + arlocal :1984 ready"
