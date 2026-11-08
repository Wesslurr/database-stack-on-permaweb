#!/usr/bin/env bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
[ -d "$HOME/erlang/27.0/bin" ] && export PATH="$HOME/erlang/27.0/bin:$PATH"
[ -d "$HOME/.local/bin" ] && export PATH="$HOME/.local/bin:$PATH"
[ -d "$HOME/.cargo/bin" ] && export PATH="$HOME/.cargo/bin:$PATH"
export ERL_FLAGS="+S 4:4 +A 4 +MBas aobf"
cd "$PROJECT_ROOT/HyperBEAM"
HB_PORT="8734" HB_CONFIG="$PROJECT_ROOT/engine/config/primary.json" rebar3 shell --sname hb-primary
