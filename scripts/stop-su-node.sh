#!/usr/bin/env bash
# Backward-compatibility wrapper -> redirects to stop-sd-node.sh
set -euo pipefail
DIR="$(dirname "${BASH_SOURCE[0]}")"
exec "$DIR/stop-sd-node.sh" "$@"
