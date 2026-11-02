#!/usr/bin/env bash
set -euo pipefail

echo "=== Stopping HyperBEAM Cluster ==="

TMUX_SESSIONS=("hb-genesis" "hb-primary" "hb-sd-1" "hb-sd-2" "hb-arbit")

for SESSION in "${TMUX_SESSIONS[@]}"; do
    if tmux has-session -t "$SESSION" 2>/dev/null; then
        echo "  Stopping tmux session: $SESSION"
        tmux kill-session -t "$SESSION" 2>/dev/null || true
    else
        echo "  Tmux session not running: $SESSION"
    fi
done

echo ""
echo "=== Stopping ArLocal (Docker) ==="
if [ "$(docker ps -q -f name=^arlocal$)" ]; then
    echo "  Stopping container 'arlocal'..."
    docker stop arlocal >/dev/null 2>&1 || true
    echo "  [OK] ArLocal stopped."
else
    echo "  ArLocal container is not running."
fi

echo ""
echo "=== Cluster shutdown complete. ==="
