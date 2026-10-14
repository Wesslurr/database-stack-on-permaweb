#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/env.sh"
"$DB_DIR/scripts/install-rust.sh"
source "$HOME/.cargo/env" 2>/dev/null || true
export PATH="$HOME/.cargo/bin:$PATH"
export MAKEFLAGS="-j2"
cd "$HB_SRC"
# relx overlay meng-copy config.flat ke release; file itu gitignored di repo
# sumber jadi tidak ikut ter-clone — sediakan dari config project.
cp "$DB_DIR/config/node.flat" "$HB_SRC/config.flat"
rebar3 as genesis_wasm,rocksdb release
test -x "$HB_REL/bin/hb"
echo "[02] release ready: $HB_REL/bin/hb"
