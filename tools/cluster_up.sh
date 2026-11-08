#!/usr/bin/env bash
set -euo pipefail

# Project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HB_SRC="$PROJECT_ROOT/HyperBEAM"
[ -d "$HOME/erlang/27.0/bin" ] && export PATH="$HOME/erlang/27.0/bin:$PATH"
[ -d "$HOME/.local/bin" ] && export PATH="$HOME/.local/bin:$PATH"
[ -d "$HOME/.cargo/bin" ] && export PATH="$HOME/.cargo/bin:$PATH"

echo "=== [1/4] Ensuring clean state before launch ==="
"$PROJECT_ROOT/tools/cluster_down.sh" >/dev/null 2>&1 || true
sleep 1

echo ""
echo "=== [2/4] Starting ArLocal (Arweave L1) in Docker ==="
if ! docker image inspect ao-localnet-arlocal:latest >/dev/null 2>&1; then
    echo "  [Auto-Fetch] Image ao-localnet-arlocal:latest not found locally."
    echo "  Pulling official prebuilt textury/arlocal:latest from Docker Hub..."
    docker pull textury/arlocal:latest
    docker tag textury/arlocal:latest ao-localnet-arlocal:latest
    echo "  [OK] Image ao-localnet-arlocal:latest ready."
fi
docker run -d --rm --name arlocal -p 1984:1984 ao-localnet-arlocal:latest >/dev/null
echo "  [OK] ArLocal container started on port 1984."

# Wait for ArLocal to be ready
echo "  Waiting for ArLocal on :1984..."
for i in {1..30}; do
    if curl -s -f "http://127.0.0.1:1984/info" >/dev/null 2>&1; then
        echo "  [READY] ArLocal is responding at http://127.0.0.1:1984/info"
        break
    fi
    sleep 1
    if [ "$i" -eq 30 ]; then
        echo "  [ERROR] ArLocal failed to start within timeout."
        exit 1
    fi
done

echo ""
# Automatically initialize HyperBEAM git submodule if absent
if [ ! -f "$HB_SRC/rebar.config" ]; then
    echo ""
    echo "=== [Setup] Initializing HyperBEAM git submodule automatically ==="
    git -C "$PROJECT_ROOT" submodule update --init --recursive
    echo "  [OK] HyperBEAM submodule initialized."
fi

echo ""
echo "=== [2.5/4] Precompiling HyperBEAM once (avoids shared _build races) ==="
(
    cd "$HB_SRC"
    rebar3 compile
)
echo "  [OK] HyperBEAM compiled."

echo ""
echo "=== [3/4] Starting 5 Independent HyperBEAM Worker Nodes in Tmux ==="

NODES=(
    "hb-genesis:8733:$PROJECT_ROOT/engine/config/genesis.json"
    "hb-primary:8734:$PROJECT_ROOT/engine/config/primary.json"
    "hb-sd-1:8735:$PROJECT_ROOT/engine/config/sd1.json"
    "hb-sd-2:8736:$PROJECT_ROOT/engine/config/sd2.json"
    "hb-arbit:8737:$PROJECT_ROOT/engine/config/arbit.json"
)

for NODE_SPEC in "${NODES[@]}"; do
    IFS=":" read -r SESSION_NAME PORT CONFIG_PATH <<< "$NODE_SPEC"
    
    # Start tmux session running HyperBEAM node
    CMD="export ERL_FLAGS=\"+S 4:4 +A 4 +MBas aobf\" && cd \"$HB_SRC\" && HB_PORT=\"$PORT\" HB_CONFIG=\"$CONFIG_PATH\" rebar3 shell --sname $SESSION_NAME"
    tmux new-session -d -s "$SESSION_NAME" "bash -c '$CMD'"
    echo "  [STARTED] Node '$SESSION_NAME' on port $PORT (config: $(basename "$CONFIG_PATH"))"
    # Brief pause to let node initialize its VM
    sleep 3
done

echo ""
echo "=== [4/4] Waiting for HyperBEAM Nodes to Initialize ==="

ALL_READY=true
for NODE_SPEC in "${NODES[@]}"; do
    IFS=":" read -r SESSION_NAME PORT CONFIG_PATH <<< "$NODE_SPEC"
    printf "  Checking %-12s (port %s)... " "$SESSION_NAME" "$PORT"
    
    NODE_OK=false
    for attempt in {1..40}; do
        ADDR=$(curl -s -f --max-time 1 "http://127.0.0.1:$PORT/~meta@1.0/info/address" 2>/dev/null || echo "")
        if [ -n "$ADDR" ] && [ "$ADDR" != "null" ]; then
            echo "[OK] Address: $ADDR"
            NODE_OK=true
            break
        fi
        sleep 1
    done

    if [ "$NODE_OK" = false ]; then
        echo "[FAILED]"
        ALL_READY=false
    fi
done

echo ""
if [ "$ALL_READY" = true ]; then
    echo "=========================================================="
    echo "  SUCCESS: All 6 cluster nodes (1 ArLocal + 5 HyperBEAM)   "
    echo "           are live, independent, and healthy!            "
    echo "=========================================================="
else
    echo "=========================================================="
    echo "  WARNING: One or more HyperBEAM nodes did not respond.   "
    echo "  Check logs via: tmux attach -t <session_name>           "
    echo "=========================================================="
    exit 1
fi
