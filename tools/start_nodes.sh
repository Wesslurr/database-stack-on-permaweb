#!/usr/bin/env bash
set -e
export PATH="/home/weslur/.local/bin:/home/weslur/.cargo/bin:$PATH"
HB_SRC="/home/weslur/database/HyperBEAM"

start_node() {
    local SNAME="$1"
    local PORT="$2"
    local CFG="$3"
    echo "Starting $SNAME on port $PORT..."
    tmux kill-session -t "$SNAME" 2>/dev/null || true
    sleep 1
    tmux new-session -d -s "$SNAME" "bash -lc 'export ERL_FLAGS=\"+S 4:4 +A 4 +MBas aobf\" && cd $HB_SRC && HB_PORT=\"$PORT\" HB_CONFIG=\"$CFG\" rebar3 shell --sname $SNAME'"
}

start_node hb-genesis 8733 "/home/weslur/database/engine/config/genesis.json"
sleep 2
start_node hb-primary 8734 "/home/weslur/database/engine/config/primary.json"
sleep 2
start_node hb-sd-1 8735 "/home/weslur/database/engine/config/sd1.json"
sleep 2
start_node hb-sd-2 8736 "/home/weslur/database/engine/config/sd2.json"
sleep 2
echo "All nodes launched."
