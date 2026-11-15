#!/usr/bin/env bash
set -euo pipefail

# Project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
HB_SRC="$PROJECT_ROOT/HyperBEAM"
if [ -x /usr/bin/erl ]; then
    _clean_path=""
    IFS=':' read -ra _path_parts <<< "$PATH"
    for _p in "${_path_parts[@]}"; do
        case "$_p" in
            "$HOME"/erlang/*/bin) ;;
            *) [ -n "$_p" ] && _clean_path="${_clean_path:+$_clean_path:}$_p" ;;
        esac
    done
    export PATH="$_clean_path"
    unset _clean_path _path_parts _p
fi
[ -f "$HOME/.cargo/env" ] && . "$HOME/.cargo/env"
for _extra in "/usr/local/cargo/bin" "$HOME/.cargo/bin" "$HOME/.local/bin"; do
    case ":$PATH:" in
        *":$_extra:"*) ;;
        *) [ -d "$_extra" ] && export PATH="$_extra:$PATH" ;;
    esac
done
export CFLAGS="${CFLAGS:-} -Wno-error=incompatible-pointer-types"

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
echo "=== [2.5/4] Precompiling HyperBEAM & Verifying Forge Devices ==="
(
    cd "$HB_SRC"
    rebar3 compile
    for DEV_NAME in l1_checkpoint hs_wasm_output hs_push; do
        DEV_SRC="../engine/devices/${DEV_NAME}/src"
        DEV_OUT="../engine/devices/${DEV_NAME}/_build/device-packages"
        if [ -d "$DEV_SRC" ]; then
            rebar3 device package --device-src "$DEV_SRC" --output-dir "$DEV_OUT"
            rebar3 device verify --device-src "$DEV_SRC" --output-dir "$DEV_OUT"
        fi
    done
)
echo "  [OK] HyperBEAM compiled & external Forge devices verified."

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
    tmux new-session -d -s "hb-genesis" -c "$HB_SRC" "bash --noprofile --norc"
    tmux pipe-pane -t "hb-genesis" -o "cat >> \"$PROJECT_ROOT/logs/hb-genesis.log\""
    tmux send-keys -t "hb-genesis" "export PATH=\"$PATH\" ERL_FLAGS=\"+S 4:4 +A 4 +MBas aobf\" && HB_PRINT=\"error,http_error,cron_error,hook_error,warning,compute_short\" HB_PORT=\"8733\" HB_CONFIG=\"$PROJECT_ROOT/engine/config/genesis.json\" erl -sname hb-genesis -config config/app.config -pa _build/default/lib/*/ebin -eval '{ok, _} = application:ensure_all_started(hb).'" C-m
    echo "  [BOOTSTRAP] Waiting for hb-genesis wallet creation..."
    for attempt in {1..40}; do
        ADDR=$(curl -s -f --max-time 1 "http://127.0.0.1:8733/~meta@1.0/info/address" 2>/dev/null || echo "")
        if [ -n "$ADDR" ] && [ "$ADDR" != "null" ] && [ -f "$PROJECT_ROOT/engine/data/genesis/wallet.json" ]; then
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
    # Start tmux session running precompiled HyperBEAM node (avoids shared _build races)
    tmux new-session -d -s "$SESSION_NAME" -c "$HB_SRC" "bash --noprofile --norc"
    tmux pipe-pane -t "$SESSION_NAME" -o "cat >> \"$PROJECT_ROOT/logs/${SESSION_NAME}.log\""
    tmux send-keys -t "$SESSION_NAME" "export PATH=\"$PATH\" ERL_FLAGS=\"+S 4:4 +A 4 +MBas aobf\" && HB_PRINT=\"error,http_error,cron_error,hook_error,warning,compute_short\" HB_PORT=\"$PORT\" HB_CONFIG=\"$CONFIG_PATH\" erl -sname $SESSION_NAME -config config/app.config -pa _build/default/lib/*/ebin -eval '{ok, _} = application:ensure_all_started(hb).'" C-m
    echo "  [STARTED] Node '$SESSION_NAME' on port $PORT (config: $(basename "$CONFIG_PATH"))"
    # Brief pause to let node initialize its VM
    sleep 1
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
