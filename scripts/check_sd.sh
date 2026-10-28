#!/usr/bin/env bash
# Syntax-check sd/*.erl into a temp dir without touching the live release ebin.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/env.sh"
ERLC="${ERLC:-$(command -v erlc || echo "$HOME/erlang/27.0/bin/erlc")}"
OUT="$(mktemp -d)"
"$ERLC" -I "$DB_DIR/sd/src" -o "$OUT" "$DB_DIR/sd/src/sd_ao_client.erl" "$DB_DIR/sd/src/sd_committer.erl" "$DB_DIR/sd/src/sd_runtime.erl"
rm -rf "$OUT"
echo "COMPILE_OK"
