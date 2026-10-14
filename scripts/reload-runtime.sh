#!/usr/bin/env bash
set -euo pipefail
source /home/weslur/database/scripts/env.sh
export PATH="/home/weslur/.local/bin:/home/weslur/erlang/27.0/bin:$PATH"

echo "=== Compiling hymessage_runtime.erl ==="
erlc -o "$HB_REL/lib/hymessage_runtime-1.0.0/ebin" /home/weslur/database/hymessage/src/hymessage_runtime.erl

echo "=== Restarting Node ==="
"$DB_DIR/scripts/stop-node.sh" || true
sleep 2
"$DB_DIR/scripts/start-node.sh"

echo "=== Liveness Check ==="
for i in 1 2 3 4 5; do
  if curl -sf -H 'Accept: application/json' http://127.0.0.1:8734/~hymessage@1.0/ping >/dev/null; then
    echo "Node is UP and hymessage@1.0 is healthy!"
    exit 0
  fi
  sleep 1
done
echo "Node failed to respond."
exit 1
