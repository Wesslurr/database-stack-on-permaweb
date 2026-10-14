#!/usr/bin/env bash
set -euo pipefail
cd /home/weslur/database

export AO_URL="http://127.0.0.1:8734"
export GATEWAY_URL="http://127.0.0.1:1984"

echo "=== Testing AOS CLI with local HyperBEAM + ArLocal ==="
npx aos --mainnet http://127.0.0.1:8734 --gateway-url http://127.0.0.1:1984 --wallet secrets/wallet.json test-process --run "return 'AOS IS RUNNING'" || true
