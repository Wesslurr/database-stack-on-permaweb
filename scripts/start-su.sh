#!/usr/bin/env bash
set -euo pipefail
DB_DIR="/home/weslur/database"

# Start HyperBEAM SU Node
"$DB_DIR/scripts/start-su-node.sh"

echo "[SU Node] Native HyperBEAM SU Node active."
curl -s -H 'Accept: application/json' http://127.0.0.1:8735/~scheduler@1.0/status || echo "Waiting for SU..."
