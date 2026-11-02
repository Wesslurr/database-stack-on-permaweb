#!/usr/bin/env bash
set -euo pipefail

echo "=========================================================="
echo "          HyperBEAM + Arweave Cluster Status              "
echo "=========================================================="

echo "--- 1. Arweave L1 (ArLocal) ---"
if [ "$(docker ps -q -f name=^arlocal$)" ]; then
    INFO=$(curl -s --max-time 1 "http://127.0.0.1:1984/info" 2>/dev/null || echo "")
    if [ -n "$INFO" ]; then
        HEIGHT=$(echo "$INFO" | python3 -c 'import sys, json; print(json.load(sys.stdin).get("height", "unknown"))' 2>/dev/null || echo "unknown")
        BLOCKS=$(echo "$INFO" | python3 -c 'import sys, json; print(json.load(sys.stdin).get("blocks", "unknown"))' 2>/dev/null || echo "unknown")
        NETWORK=$(echo "$INFO" | python3 -c 'import sys, json; print(json.load(sys.stdin).get("network", "unknown"))' 2>/dev/null || echo "unknown")
        echo "  Status  : [RUNNING]"
        echo "  Port    : 1984"
        echo "  Network : $NETWORK"
        echo "  Height  : $HEIGHT (blocks: $BLOCKS)"
    else
        echo "  Status : [CONTAINER UP, BUT NOT RESPONDING ON :1984]"
    fi
else
    echo "  Status : [STOPPED]"
fi

echo ""
echo "--- 2. HyperBEAM Worker Nodes ---"
NODES=(
    "hb-genesis:8733:Genesis Node"
    "hb-primary:8734:Primary Node (Shared State)"
    "hb-sd-1:8735:Shared Data 1"
    "hb-sd-2:8736:Shared Data 2"
    "hb-arbit:8737:Arbitration Node"
)

printf "%-12s | %-5s | %-9s | %-43s | %s\n" "SESSION" "PORT" "STATUS" "ARWEAVE ADDRESS" "ROLE"
echo "-------------+-------+-----------+---------------------------------------------+---------------------------"

for NODE_SPEC in "${NODES[@]}"; do
    IFS=":" read -r SESSION PORT ROLE <<< "$NODE_SPEC"
    
    TMUX_UP=false
    if tmux has-session -t "$SESSION" 2>/dev/null; then
        TMUX_UP=true
    fi
    
    ADDR=$(curl -s --max-time 1 "http://127.0.0.1:$PORT/~meta@1.0/info/address" 2>/dev/null || echo "")
    ADDR=$(echo "$ADDR" | tr -d '"\r\n ')
    
    STATUS="DOWN"
    if [ "$TMUX_UP" = true ] && [ -n "$ADDR" ] && [ "$ADDR" != "null" ]; then
        STATUS="ONLINE"
    elif [ "$TMUX_UP" = true ]; then
        STATUS="STARTING"
        ADDR="-"
    else
        STATUS="OFFLINE"
        ADDR="-"
    fi
    
    printf "%-12s | %-5s | %-9s | %-43s | %s\n" "$SESSION" "$PORT" "[$STATUS]" "$ADDR" "$ROLE"
done

echo "=========================================================="
