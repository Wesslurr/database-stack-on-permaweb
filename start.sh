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
OTP_VER=$(erl -noshell -eval 'io:format("~s", [erlang:system_info(otp_release)]), halt().' 2>/dev/null || echo "0")
if [ "$OTP_VER" = "0" ] || [ "$OTP_VER" -lt 26 ] || ! command -v rebar3 &>/dev/null; then
    echo "=== [1/3] Erlang OTP release ($OTP_VER) is older than 26. Upgrading to modern Erlang... ==="
    bash .devcontainer/setup.sh
    # Clean previous cowlib compilation artifact
    rm -rf HyperBEAM/_build/default/lib/cowlib
else
    echo "=== [1/3] Erlang OTP $OTP_VER & Rebar3 runtime verified. ==="
fi

# 2. Launch cluster
echo "=== [2/3] Launching Multi-Node HyperBEAM Cluster ==="
for F in HyperBEAM/_build/default/lib/cowlib/src/cow_capsule.erl HyperBEAM/_build/default/lib/gun/src/gun.erl; do
    if [ -f "$F" ] && ! grep -q "feature(maybe_expr" "$F"; then
        sed -i '1s/^/-feature(maybe_expr, enable).\n/' "$F"
    fi
done
rm -f HyperBEAM/_build/default/lib/cowlib/ebin/cow_capsule.beam HyperBEAM/_build/default/lib/gun/ebin/gun.beam
./tools/cluster_up.sh

# 3. Serve game client
echo ""
echo "=========================================================="
echo "  🎮 Interactive Client & Backstage Inspector Ready!     "
echo "  👉 Local / Codespace Port: 8080                         "
echo "=========================================================="
echo ""
python3 tools/game_client/serve.py
