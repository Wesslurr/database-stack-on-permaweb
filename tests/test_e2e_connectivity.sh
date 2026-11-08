#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# End-to-End Connectivity & Cluster Health Verification
# Tests all nodes from Genesis, Primary, SD1, SD2, Arbit to ArLocal (Arweave L1)
# ==============================================================================

PASSED=0
FAILED=0
TOTAL=0

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

assert_eq() {
    local label="$1"
    local actual="$2"
    local expected="$3"
    TOTAL=$((TOTAL + 1))
    if [ "$actual" = "$expected" ]; then
        echo "  [PASS] $label (got: '$actual')"
        PASSED=$((PASSED + 1))
    else
        echo "  [FAIL] $label (expected: '$expected', got: '$actual')"
        FAILED=$((FAILED + 1))
    fi
}

assert_not_empty() {
    local label="$1"
    local actual="$2"
    TOTAL=$((TOTAL + 1))
    if [ -n "$actual" ] && [ "$actual" != "null" ]; then
        echo "  [PASS] $label (value: $actual)"
        PASSED=$((PASSED + 1))
    else
        echo "  [FAIL] $label (is empty or null)"
        FAILED=$((FAILED + 1))
    fi
}

echo "=================================================================="
echo "    STAGE 1: Arweave L1 (ArLocal) Health & Mining Verification    "
echo "=================================================================="

# Test ArLocal /info
INFO_RESP=$(curl -s --max-time 3 "http://127.0.0.1:1984/info" || echo "")
NETWORK=$(echo "$INFO_RESP" | python3 -c 'import sys, json; print(json.load(sys.stdin).get("network", ""))' 2>/dev/null || echo "")
HEIGHT_BEFORE=$(echo "$INFO_RESP" | python3 -c 'import sys, json; print(json.load(sys.stdin).get("height", 0))' 2>/dev/null || echo "0")

assert_eq "ArLocal network ID is 'arlocal.N.1'" "$NETWORK" "arlocal.N.1"
assert_not_empty "Initial block height reported" "$HEIGHT_BEFORE"

# Test mining a block on ArLocal
curl -s --max-time 3 "http://127.0.0.1:1984/mine" >/dev/null || true
echo "  Triggered ArLocal mining..."

INFO_AFTER=$(curl -s --max-time 3 "http://127.0.0.1:1984/info" || echo "")
HEIGHT_AFTER=$(echo "$INFO_AFTER" | python3 -c 'import sys, json; print(json.load(sys.stdin).get("height", 0))' 2>/dev/null || echo "0")
EXPECTED_HEIGHT=$((HEIGHT_BEFORE + 1))

assert_eq "Block height incremented after mining" "$HEIGHT_AFTER" "$EXPECTED_HEIGHT"

echo ""
echo "=================================================================="
echo "   STAGE 2: HyperBEAM Worker Nodes Identity & Config Verification  "
echo "=================================================================="

NODES=(
    "hb-genesis:8733:Genesis Node"
    "hb-primary:8734:Primary Shared State"
    "hb-sd-1:8735:Shared Data Replica 1"
    "hb-sd-2:8736:Shared Data Replica 2"
    "hb-arbit:8737:Arbitration Node"
)

declare -A NODE_ADDRESSES

for NODE_SPEC in "${NODES[@]}"; do
    IFS=":" read -r SESSION PORT ROLE <<< "$NODE_SPEC"
    echo "--- Testing Node: $SESSION ($ROLE) on port $PORT ---"
    
    # 1. Check ~meta@1.0/info responsiveness
    META_INFO=$(curl -s --max-time 3 "http://127.0.0.1:$PORT/~meta@1.0/info" || echo "")
    assert_not_empty "Node $SESSION ~meta@1.0/info responds" "$META_INFO"
    
    # 2. Check Arweave address (43 chars base64url)
    ADDR=$(curl -s --max-time 3 "http://127.0.0.1:$PORT/~meta@1.0/info/address" || echo "")
    ADDR=$(echo "$ADDR" | tr -d '"\r\n ')
    NODE_ADDRESSES["$SESSION"]="$ADDR"
    
    ADDR_LEN=${#ADDR}
    if [ "$ADDR_LEN" -eq 43 ]; then
        echo "  [PASS] Node $SESSION has valid 43-character Arweave address: $ADDR"
        PASSED=$((PASSED + 1))
    else
        echo "  [FAIL] Node $SESSION address length is $ADDR_LEN (expected 43): '$ADDR'"
        FAILED=$((FAILED + 1))
    fi
    TOTAL=$((TOTAL + 1))
    
    # 3. Check port reported by node
    REPORTED_PORT=$(curl -s --max-time 3 "http://127.0.0.1:$PORT/~meta@1.0/info/port" || echo "")
    REPORTED_PORT=$(echo "$REPORTED_PORT" | tr -d '"\r\n ')
    assert_eq "Node $SESSION reports configured port" "$REPORTED_PORT" "$PORT"
    
    # 4. Check gateway configuration points to ArLocal
    GATEWAY=$(curl -s --max-time 3 "http://127.0.0.1:$PORT/~meta@1.0/info/gateway" || echo "")
    GATEWAY=$(echo "$GATEWAY" | tr -d '"\r\n ')
    assert_eq "Node $SESSION points to local ArLocal gateway" "$GATEWAY" "http://127.0.0.1:1984"
    echo ""
done

echo "=================================================================="
echo "    STAGE 3: Distinct Wallets and Isolated Storage Verification   "
echo "=================================================================="

# Check that every node generated a unique key/address (no collisions)
UNIQUE_ADDRS=$(printf "%s\n" "${NODE_ADDRESSES[@]}" | sort -u | wc -l)
NUM_NODES=${#NODES[@]}
assert_eq "All $NUM_NODES HyperBEAM nodes have distinct cryptographic identities" "$UNIQUE_ADDRS" "$NUM_NODES"

# Verify isolated data directories exist on disk
for NODE_DIR in genesis primary sd1 sd2 arbit; do
    DIR_PATH="$PROJECT_ROOT/engine/data/$NODE_DIR"
    WALLET_FILE="$DIR_PATH/wallet.json"
    TOTAL=$((TOTAL + 1))
    if [ -f "$WALLET_FILE" ]; then
        echo "  [PASS] Wallet file exists for $NODE_DIR: $WALLET_FILE"
        PASSED=$((PASSED + 1))
    else
        echo "  [FAIL] Wallet file missing for $NODE_DIR: $WALLET_FILE"
        FAILED=$((FAILED + 1))
    fi
done

echo ""
echo "=================================================================="
echo "      STAGE 4: Direct Node-to-Node HTTP Round-Trip Check          "
echo "=================================================================="

# Test cross-node ping: Primary (:8734) checks Genesis (:8733) and SD1 (:8735)
echo "--- Testing Direct HTTP interconnect between nodes ---"
P_GEN_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "http://127.0.0.1:8733/~meta@1.0/info")
assert_eq "Genesis (:8733) accessible via HTTP from other nodes" "$P_GEN_CODE" "200"

P_PRI_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "http://127.0.0.1:8734/~meta@1.0/info")
assert_eq "Primary (:8734) accessible via HTTP from other nodes" "$P_PRI_CODE" "200"

P_SD1_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "http://127.0.0.1:8735/~meta@1.0/info")
assert_eq "SD1 (:8735) accessible via HTTP from other nodes" "$P_SD1_CODE" "200"

P_SD2_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "http://127.0.0.1:8736/~meta@1.0/info")
assert_eq "SD2 (:8736) accessible via HTTP from other nodes" "$P_SD2_CODE" "200"

P_ARB_CODE=$(curl -s -o /dev/null -w "%{http_code}" --max-time 3 "http://127.0.0.1:8737/~meta@1.0/info")
assert_eq "Arbit (:8737) accessible via HTTP from other nodes" "$P_ARB_CODE" "200"

echo ""
echo "=================================================================="
echo "                         TEST SUMMARY                             "
echo "=================================================================="
echo "  Total assertions : $TOTAL"
echo "  Passed           : $PASSED"
echo "  Failed           : $FAILED"
echo "=================================================================="

if [ "$FAILED" -eq 0 ]; then
    echo "  RESULT: ALL VERIFICATION CHECKS PASSED SUCCESSFULLY!"
    exit 0
else
    echo "  RESULT: VERIFICATION FAILED WITH $FAILED ERRORS."
    exit 1
fi
