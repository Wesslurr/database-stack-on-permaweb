#!/usr/bin/env bash
# Mutation test: prove the suite is not decorative.
#
# This deliberately breaks the ACK verification in the Primary, rebuilds the
# cluster (FRESH), and requires `tests/test_failure_modes.py --yes` to go RED.
# The original file is always restored and the cluster rebuilt afterwards.
#
#   ./tests/mutation_ack.sh
#
# Exit codes: 0 = mutation detected (suite red, as required)
#             1 = mutation NOT detected (suite stayed green) -> the test is useless
#             2 = refused (dirty working tree) or setup failure
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FILE="$ROOT/engine/processes/primary/primary_process.lua"
BACKUP="$(mktemp /tmp/opencode/primary_process.XXXXXX.lua)"

if ! git -C "$ROOT" diff --quiet -- "$FILE"; then
    echo "[refuse] $FILE has uncommitted changes; commit or stash them first"
    exit 2
fi

cp "$FILE" "$BACKUP"

restore() {
    cp "$BACKUP" "$FILE"
    rm -f "$BACKUP"
    echo "[restore] original Primary restored; rebuilding cluster"
    ( cd "$ROOT" && FRESH=1 ./tools/apply_engine.sh >/dev/null 2>&1 ) || true
}
trap restore EXIT

python3 - "$FILE" <<'PY'
import sys

path = sys.argv[1]
src = open(path).read()
old = "    if claimed_root ~= p.root or claimed_tick ~= p.tick_to then"
new = "    if false and (claimed_root ~= p.root or claimed_tick ~= p.tick_to) then"
if src.count(old) != 1:
    sys.exit("mutation anchor not found exactly once")
open(path, "w").write(src.replace(old, new))
print("[mutate] ACK root/tick verification disabled")
PY

echo "[setup] rebuilding cluster with the broken invariant"
if ! ( cd "$ROOT" && FRESH=1 ./tools/apply_engine.sh >/dev/null 2>&1 ); then
    echo "[refuse] cluster did not come up with the mutation"
    exit 2
fi

echo "[run] tests/test_failure_modes.py --yes (expecting RED)"
( cd "$ROOT" && python3 tests/test_failure_modes.py --yes )
status=$?

if [ "$status" -eq 0 ]; then
    echo "[FAIL] mutation NOT detected: the suite stayed green with a broken ACK check"
    exit 1
fi

echo "[PASS] mutation detected: the suite went red as required"
exit 0
