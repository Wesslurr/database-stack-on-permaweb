#!/usr/bin/env bash
# Rebuild Rust WASM engines (primary, shared_data), node configs (genesis, arbit Lua),
# and restart the HyperBEAM workers. ArLocal is left untouched.
set -uo pipefail

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

python3 "$PROJECT_ROOT/tools/build_configs.py" >/dev/null || exit 1

(
    cd "$PROJECT_ROOT/engine/processes/primary"
    cargo build --target wasm32-unknown-unknown --release
) || exit 1
(
    cd "$PROJECT_ROOT/engine/processes/shared_data"
    cargo build --target wasm32-unknown-unknown --release
) || exit 1

# Ensure HyperBEAM & external Forge devices are compiled cleanly before worker spawn
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
) || exit 1

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

python3 "$PROJECT_ROOT/tools/build_configs.py"
epmd -daemon >/dev/null 2>&1 || true

for NODE_SPEC in "${NODES[@]}"; do
    IFS=":" read -r SESSION_NAME PORT CONFIG_PATH <<< "$NODE_SPEC"
    CMD="export PATH=\"$PATH\" ERL_FLAGS=\"+S 4:4 +sbwt none +sbwtdcpu none +sbwtdio none +A 8 +MBas aobf\" && cd \"$HB_SRC\" && HB_PRINT=\"error,http_error,cron_error,hook_error,warning,compute_short\" HB_PORT=\"$PORT\" HB_CONFIG=\"$CONFIG_PATH\" erl -sname $SESSION_NAME -config config/app.config -pa _build/default/lib/*/ebin -eval '{ok, _} = application:ensure_all_started(hb).'"
    printf -v ESCAPED_CMD '%q' "$CMD"
    tmux new-session -d -s "$SESSION_NAME" "bash --noprofile --norc -c $ESCAPED_CMD"
    sleep 1
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
if [ "$FAILED" -eq 0 ]; then
    python3 "$PROJECT_ROOT/tools/deploy_forge_device.py" --skip-verify || FAILED=1
fi
exit $FAILED
