#!/usr/bin/env bash
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HB_SRC="$PROJECT_ROOT/HyperBEAM"
[ -d "$HOME/erlang/27.0/bin" ] && export PATH="$HOME/erlang/27.0/bin:$PATH"
[ -d "$HOME/.local/bin" ] && export PATH="$HOME/.local/bin:$PATH"
[ -d "$HOME/.cargo/bin" ] && export PATH="$HOME/.cargo/bin:$PATH"

start_node() {
    local SNAME="$1"
    local PORT="$2"
    local CFG="$3"
    echo "Starting $SNAME on port $PORT..."
    tmux kill-session -t "$SNAME" 2>/dev/null || true
    sleep 1
    tmux new-session -d -s "$SNAME" "bash -lc 'export ERL_FLAGS=\"+S 4:4 +A 4 +MBas aobf\" && cd $HB_SRC && HB_PORT=\"$PORT\" HB_CONFIG=\"$CFG\" rebar3 shell --sname $SNAME'"
}

start_node hb-genesis 8733 "$PROJECT_ROOT/engine/config/genesis.json"
sleep 2
start_node hb-primary 8734 "$PROJECT_ROOT/engine/config/primary.json"
sleep 2
start_node hb-sd-1 8735 "$PROJECT_ROOT/engine/config/sd1.json"
sleep 2
start_node hb-sd-2 8736 "$PROJECT_ROOT/engine/config/sd2.json"
sleep 2
echo "All nodes launched."
