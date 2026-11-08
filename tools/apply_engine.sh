#!/usr/bin/env bash
# Rebuild node configs from the Lua sources and restart the HyperBEAM workers.
# ArLocal is left untouched. Use after editing engine/processes/*.lua.
set -uo pipefail

PROJECT_ROOT="/home/weslur/database"
HB_SRC="$PROJECT_ROOT/HyperBEAM"
export PATH="/home/weslur/erlang/27.0/bin:/home/weslur/.local/bin:/home/weslur/.cargo/bin:$PATH"

python3 "$PROJECT_ROOT/tools/build_configs.py" >/dev/null || exit 1

# Ensure HyperBEAM is compiled once to avoid shared _build races among workers
(cd "$HB_SRC" && rebar3 compile >/dev/null 2>&1 || true)

# HyperBEAM persists the node-process name registry (local-name@1.0) in the
# node store. A changed Lua module therefore needs a clean registry, otherwise
# the node keeps resolving the old process ID. FRESH=1 wipes the node stores.
if [ "${FRESH:-0}" = "1" ]; then
    for NODE in genesis primary sd1 sd2 arbit; do
        rm -rf "$PROJECT_ROOT/engine/data/$NODE/lmdb"
        mkdir -p "$PROJECT_ROOT/engine/data/$NODE/lmdb"
    done
    echo "  [FRESH] Node LMDB stores wiped (wallets preserved)."
fi

NODES=(
    "hb-genesis:8733:$PROJECT_ROOT/engine/config/genesis.json"
    "hb-primary:8734:$PROJECT_ROOT/engine/config/primary.json"
    "hb-sd-1:8735:$PROJECT_ROOT/engine/config/sd1.json"
    "hb-sd-2:8736:$PROJECT_ROOT/engine/config/sd2.json"
    "hb-arbit:8737:$PROJECT_ROOT/engine/config/arbit.json"
)

for NODE_SPEC in "${NODES[@]}"; do
    IFS=":" read -r SESSION_NAME PORT CONFIG_PATH <<< "$NODE_SPEC"
    tmux kill-session -t "$SESSION_NAME" 2>/dev/null || true
done

# Wait for the BEAM VMs to actually exit before touching their stores. Wiping
# an LMDB directory while the old node is still shutting down can leave a lock
# behind and make the replacement node crash on boot.
for NODE_SPEC in "${NODES[@]}"; do
    IFS=":" read -r SESSION_NAME PORT CONFIG_PATH <<< "$NODE_SPEC"
    for _ in {1..40}; do
        if ! pgrep -f -- "sname[ =]${SESSION_NAME}( |\$)" >/dev/null 2>&1; then break; fi
        sleep 0.5
    done
done
sleep 1

for NODE_SPEC in "${NODES[@]}"; do
    IFS=":" read -r SESSION_NAME PORT CONFIG_PATH <<< "$NODE_SPEC"
    CMD="export PATH=\"/home/weslur/erlang/27.0/bin:/home/weslur/.local/bin:/home/weslur/.cargo/bin:\$PATH\" && export ERL_FLAGS=\"+S 4:4 +A 4 +MBas aobf\" && cd \"$HB_SRC\" && HB_PORT=\"$PORT\" HB_CONFIG=\"$CONFIG_PATH\" rebar3 shell --sname $SESSION_NAME"
    tmux new-session -d -s "$SESSION_NAME" "bash -c '$CMD'"
    sleep 2
done

FAILED=0
for NODE_SPEC in "${NODES[@]}"; do
    IFS=":" read -r SESSION_NAME PORT CONFIG_PATH <<< "$NODE_SPEC"
    OK=false
    for _ in {1..40}; do
        ADDR=$(curl -s -f --max-time 1 "http://127.0.0.1:$PORT/~meta@1.0/info/address" 2>/dev/null || echo "")
        if [ -n "$ADDR" ] && [ "$ADDR" != "null" ]; then OK=true; break; fi
        sleep 1
    done
    if [ "$OK" = true ]; then
        echo "  [OK] $SESSION_NAME :$PORT"
    else
        echo "  [FAILED] $SESSION_NAME :$PORT"
        FAILED=1
    fi
done
exit $FAILED
