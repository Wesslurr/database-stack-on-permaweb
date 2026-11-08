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
if ! command -v erl &>/dev/null || ! command -v rebar3 &>/dev/null; then
    echo "=== [1/3] Dependencies missing. Running automatic setup... ==="
    bash .devcontainer/setup.sh
else
    echo "=== [1/3] Erlang & Rebar3 runtime verified. ==="
fi

# 2. Launch cluster
echo "=== [2/3] Launching Multi-Node HyperBEAM Cluster ==="
./tools/cluster_up.sh

# 3. Serve game client
echo ""
echo "=========================================================="
echo "  🎮 Interactive Client & Backstage Inspector Ready!     "
echo "  👉 Local / Codespace Port: 8080                         "
echo "=========================================================="
echo ""
python3 tools/game_client/serve.py
