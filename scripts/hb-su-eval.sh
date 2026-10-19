#!/usr/bin/env bash
# Backward-compatibility wrapper -> redirects to hb-sd-eval.sh
set -euo pipefail
DIR="$(dirname "${BASH_SOURCE[0]}")"
exec "$DIR/hb-sd-eval.sh" "$@"
