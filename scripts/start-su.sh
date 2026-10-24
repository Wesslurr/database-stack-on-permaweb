#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_DIR="${DB_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"

# Start HyperBEAM SU Node
"$DB_DIR/scripts/start-su-node.sh"

echo "[SU Node] Native HyperBEAM SU Node active."
curl -s -H 'Accept: application/json' http://127.0.0.1:8735/~scheduler@1.0/status || echo "Waiting for SU..."
