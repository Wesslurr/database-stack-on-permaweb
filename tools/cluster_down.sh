#!/usr/bin/env bash
set -euo pipefail

echo "=== Stopping HyperBEAM Cluster ==="

TMUX_SESSIONS=("hb-genesis" "hb-primary" "hb-sd-1" "hb-sd-2" "hb-arbit" "hb-client" "hb-ui" "hb-arlocal")

for SESSION in "${TMUX_SESSIONS[@]}"; do
    if tmux has-session -t "$SESSION" 2>/dev/null; then
        echo "  Stopping tmux session: $SESSION"
        tmux kill-session -t "$SESSION" 2>/dev/null || true
    else
        echo "  Tmux session not running: $SESSION"
    fi
done

echo ""
echo "=== Stopping Game Client UI (8080) ==="
fuser -k 8080/tcp >/dev/null 2>&1 || true
pkill -f "tools/game_client/serve.py" >/dev/null 2>&1 || true

echo ""
echo "=== Stopping ArLocal ==="
ARLOCAL_CID=""
if command -v docker >/dev/null 2>&1 && docker info >/dev/null 2>&1; then
    ARLOCAL_CID="$(docker ps -q -f name=^arlocal$ 2>/dev/null || true)"
fi
if [ -n "$ARLOCAL_CID" ]; then
    echo "  Stopping container 'arlocal'..."
    docker stop arlocal >/dev/null 2>&1 || true
    echo "  [OK] ArLocal container stopped."
else
    echo "  ArLocal container is not running."
fi
pkill -f "arlocal 1984" >/dev/null 2>&1 || true
fuser -k 1984/tcp >/dev/null 2>&1 || true

echo ""
echo "=== Cluster shutdown complete. ==="
