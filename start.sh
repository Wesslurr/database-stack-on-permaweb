#!/usr/bin/env bash
# ==============================================================================
# One-Click All-in-One Launcher for HyperStack
# Automatically verifies dependencies, launches cluster, and serves game UI.
# ==============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"
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

echo "=========================================================="
echo "      🚀 HyperStack Real-Time Engine Auto-Launcher        "
echo "=========================================================="

# 1. Dependency check
if ! command -v apt-get &>/dev/null && [ ! -f /usr/bin/apt-get ]; then
    echo "=========================================================="
    echo "⚠️  Codespace sedang berjalan dalam RECOVERY MODE"
    echo "    (Container build sebelumnya gagal pada commit lama)."
    echo ""
    echo "👉  Cara keluar dari Recovery Mode & restore Ubuntu penuh:"
    echo "    1. Tekan Ctrl+Shift+P (atau F1)"
    echo "    2. Ketik dan pilih: 'Codespaces: Rebuild Container'"
    echo "=========================================================="
    exit 1
fi

OTP_VER=$(erl -noshell -eval 'io:format("~s", [erlang:system_info(otp_release)]), halt().' 2>/dev/null || echo "0")
if [ "$OTP_VER" = "0" ] || [ "$OTP_VER" -lt 26 ] || ! command -v rebar3 &>/dev/null; then
    echo "=== [1/3] Erlang OTP release ($OTP_VER) is older than 26. Upgrading to modern Erlang... ==="
    bash .devcontainer/setup.sh
    # Clean previous cowlib compilation artifact
    rm -rf HyperBEAM/_build/default/lib/cowlib
else
    echo "=== [1/3] Erlang OTP $OTP_VER & Rebar3 runtime verified. ==="
fi

# 1.5. Ensure required Erlang OTP modules (os_mon, runtime_tools) and Rust wasm32 target
if ! erl -noshell -eval 'case application:load(os_mon) of ok -> halt(0); {error, {already_loaded, _}} -> halt(0); _ -> halt(1) end.' 2>/dev/null; then
    echo "=== [1.5/3] Installing missing Erlang OTP modules (erlang-os-mon, erlang-runtime-tools) ==="
    sudo apt-get update && sudo apt-get install -y erlang-os-mon erlang-runtime-tools
fi
if command -v rustup &>/dev/null && ! rustup target list --installed 2>/dev/null | grep -q "^wasm32-unknown-unknown$"; then
    echo "=== [1.6/3] Installing Rust wasm32-unknown-unknown target ==="
    rustup target add wasm32-unknown-unknown
fi

# 2. Launch cluster (compiles HyperBEAM, Forge devices, Rust WASM, and generates configs in order)
echo "=== [2/3] Launching Multi-Node HyperBEAM Cluster ==="
export CFLAGS="${CFLAGS:-} -Wno-error=incompatible-pointer-types"

if [ ! -f "HyperBEAM/rebar.config" ]; then
    echo "=== [Setup] Initializing HyperBEAM git submodule ==="
    git submodule update --init --recursive
fi
if [ ! -f "engine/processes/primary/src/lib.rs" ] || [ ! -f "engine/processes/shared_data/src/lib.rs" ]; then
    echo "❌ ERROR: Rust WASM process sources missing in engine/processes/{primary,shared_data}"
    exit 1
fi

./tools/cluster_up.sh

# 3. Serve game client
echo ""
echo "=========================================================="
echo "  🎮 Interactive Client & Backstage Inspector Ready!     "
echo "  👉 Local / Codespace Port: 8080                         "
echo "=========================================================="
echo ""
fuser -k 8080/tcp >/dev/null 2>&1 || true
pkill -f "tools/game_client/serve.py" >/dev/null 2>&1 || true
sleep 0.5
python3 tools/game_client/serve.py
