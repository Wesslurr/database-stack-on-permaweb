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
echo "=== [2/4] Starting ArLocal (Arweave L1) ==="
if docker info >/dev/null 2>&1; then
    if ! docker image inspect ao-localnet-arlocal:latest >/dev/null 2>&1; then
        echo "  [Auto-Fetch] Image ao-localnet-arlocal:latest not found locally."
        echo "  Pulling official prebuilt textury/arlocal:latest from Docker Hub..."
        docker pull textury/arlocal:latest || true
        docker tag textury/arlocal:latest ao-localnet-arlocal:latest 2>/dev/null || true
        echo "  [OK] Image ao-localnet-arlocal:latest ready."
    fi
    docker rm -f arlocal >/dev/null 2>&1 || true
    docker run -d --rm --name arlocal -p 1984:1984 ao-localnet-arlocal:latest >/dev/null
    echo "  [OK] ArLocal container started on port 1984."
elif command -v npx >/dev/null 2>&1; then
    echo "  [Fallback] Docker daemon inactive, launching ArLocal via npx..."
    fuser -k 1984/tcp >/dev/null 2>&1 || true
    nohup npx -y arlocal 1984 > /tmp/arlocal.log 2>&1 &
    echo "  [OK] ArLocal process spawned via npx."
fi

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
# In Erlang/OTP 26, maybe expressions (EEP-49) require enabling the feature in erl_opts
if ! grep -q "feature, maybe_expr" "$HB_SRC/rebar.config" 2>/dev/null; then
    sed -i 's/{erl_opts, \[/{erl_opts, [\n    {feature, maybe_expr, enable},/' "$HB_SRC/rebar.config"
fi
if ! grep -q "add, cowlib" "$HB_SRC/rebar.config" 2>/dev/null; then
    sed -i 's/{override, gun, \[{deps, \[cowlib\]}\]}/{override, gun, [{deps, [cowlib]}]},\n    {add, cowlib, [{erl_opts, [{feature, maybe_expr, enable}]}]},\n    {add, gun, [{erl_opts, [{feature, maybe_expr, enable}]}]}/' "$HB_SRC/rebar.config"
fi

for F in "$HB_SRC/_build/default/lib/cowlib/src/cow_capsule.erl" "$HB_SRC/_build/default/lib/gun/src/gun.erl"; do
    if [ -f "$F" ] && ! grep -q "feature(maybe_expr" "$F"; then
        sed -i '1s/^/-feature(maybe_expr, enable).\n/' "$F"
    fi
done
rm -f "$HB_SRC/_build/default/lib/cowlib/ebin/cow_capsule.beam" "$HB_SRC/_build/default/lib/gun/ebin/gun.beam"

# Remove any legacy preloaded custom device before clean HyperBEAM compile
rm -f "$HB_SRC/src/preloaded/arweave/dev_l1_checkpoint.erl"
find "$HB_SRC/_build" -name "dev_l1_checkpoint.beam" -delete 2>/dev/null || true

(
    cd "$HB_SRC"
    rebar3 compile
    rebar3 device package --device-src ../engine/devices/l1_checkpoint/src --output-dir ../engine/devices/l1_checkpoint/_build/device-packages
    rebar3 device verify --device-src ../engine/devices/l1_checkpoint/src --output-dir ../engine/devices/l1_checkpoint/_build/device-packages
)
echo "  [OK] HyperBEAM compiled & external Forge device verified."

echo ""
echo "=== [2.7/4] Compiling Rust WASM Engines (Primary & Shared Data) ==="
(
    cd "$PROJECT_ROOT/engine/processes/primary"
    cargo build --target wasm32-unknown-unknown --release
)
(
    cd "$PROJECT_ROOT/engine/processes/shared_data"
    cargo build --target wasm32-unknown-unknown --release
)
echo "  [OK] Rust WASM engines compiled."

echo ""
echo "=== [2.8/4] Generating node configurations for current environment ==="
python3 "$PROJECT_ROOT/tools/build_configs.py"
mkdir -p "$PROJECT_ROOT/logs"

# Ensure epmd is running for distributed node naming
epmd -daemon >/dev/null 2>&1 || true

echo ""
echo "=== [3/4] Starting 5 Independent HyperBEAM Worker Nodes in Tmux ==="

# If Genesis wallet does not exist yet (e.g. fresh clone or --wipe-wallets), boot
# hb-genesis first so its wallet is created and build_configs.py can populate
# arbit's trusted-device-signers before hb-arbit starts.
if [ ! -f "$PROJECT_ROOT/engine/data/genesis/wallet.json" ]; then
    tmux new-session -d -s "hb-genesis" -c "$HB_SRC"
    tmux pipe-pane -t "hb-genesis" -o "cat >> \"$PROJECT_ROOT/logs/hb-genesis.log\""
    tmux send-keys -t "hb-genesis" "export ERL_FLAGS=\"+S 4:4 +A 4 +MBas aobf\" && HB_PORT=\"8733\" HB_CONFIG=\"$PROJECT_ROOT/engine/config/genesis.json\" rebar3 shell --sname hb-genesis" C-m
    echo "  [BOOTSTRAP] Waiting for hb-genesis wallet creation..."
    for attempt in {1..40}; do
        ADDR=$(curl -s -f --max-time 1 "http://127.0.0.1:8733/~meta@1.0/info/address" 2>/dev/null || echo "")
        if [ -n "$ADDR" ] && [ "$ADDR" != "null" ]; then
            echo "  [BOOTSTRAP] Genesis address ready: $ADDR"
            break
        fi
        sleep 1
    done
    python3 "$PROJECT_ROOT/tools/build_configs.py"
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
    if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
        continue
    fi
    # Start tmux session running HyperBEAM node with interactive PTY and log persistence
    tmux new-session -d -s "$SESSION_NAME" -c "$HB_SRC"
    tmux pipe-pane -t "$SESSION_NAME" -o "cat >> \"$PROJECT_ROOT/logs/${SESSION_NAME}.log\""
    tmux send-keys -t "$SESSION_NAME" "export ERL_FLAGS=\"+S 4:4 +A 4 +MBas aobf\" && HB_PORT=\"$PORT\" HB_CONFIG=\"$CONFIG_PATH\" rebar3 shell --sname $SESSION_NAME" C-m
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
        if [ -f "$PROJECT_ROOT/logs/${SESSION_NAME}.log" ]; then
            echo "    --- Last 5 lines of $SESSION_NAME log: ---"
            tail -n 5 "$PROJECT_ROOT/logs/${SESSION_NAME}.log" | sed 's/^/    /'
            echo "    ------------------------------------------"
        fi
    fi
done

echo ""
if [ "$ALL_READY" = true ]; then
    echo "=== [4.5/4] Deploying External L1 Checkpoint Device via HyperBEAM Forge ==="
    python3 "$PROJECT_ROOT/tools/deploy_forge_device.py" --skip-verify
    echo ""
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
