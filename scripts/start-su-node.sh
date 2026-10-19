#!/usr/bin/env bash
# Backward-compatibility wrapper -> redirects to start-sd-node.sh
set -euo pipefail
DIR="$(dirname "${BASH_SOURCE[0]}")"
exec "$DIR/start-sd-node.sh" "$@"
