#!/usr/bin/env bash
# ==============================================================================
# One-Click All-in-One Launcher for HyperStack
# Automatically verifies dependencies, launches cluster, and serves game UI.
# ==============================================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

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

# 1.5. Ensure required Erlang OTP modules (os_mon, runtime_tools)
if ! erl -noshell -eval 'case application:load(os_mon) of ok -> halt(0); {error, {already_loaded, _}} -> halt(0); _ -> halt(1) end.' 2>/dev/null; then
    echo "=== [1.5/3] Installing missing Erlang OTP modules (erlang-os-mon, erlang-runtime-tools) ==="
    sudo apt-get update && sudo apt-get install -y erlang-os-mon erlang-runtime-tools
fi

# 2. Launch cluster
echo "=== [2/3] Launching Multi-Node HyperBEAM Cluster ==="
if [ -f "HyperBEAM/rebar.config" ] && ! grep -q "feature, maybe_expr" "HyperBEAM/rebar.config" 2>/dev/null; then
    sed -i 's/{erl_opts, \[/{erl_opts, [\n    {feature, maybe_expr, enable},/' "HyperBEAM/rebar.config"
fi
for F in HyperBEAM/_build/default/lib/cowlib/src/cow_capsule.erl HyperBEAM/_build/default/lib/gun/src/gun.erl; do
    if [ -f "$F" ] && ! grep -q "feature(maybe_expr" "$F"; then
        sed -i '1s/^/-feature(maybe_expr, enable).\n/' "$F"
    fi
done
rm -f HyperBEAM/_build/default/lib/cowlib/ebin/cow_capsule.beam HyperBEAM/_build/default/lib/gun/ebin/gun.beam
python3 tools/build_configs.py
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
