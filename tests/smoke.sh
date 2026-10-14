#!/usr/bin/env bash
set -uo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/../scripts/env.sh"
fail=0
code=$(curl -s --max-time 5 -o /dev/null -w '%{http_code}' http://127.0.0.1:8734/~meta@1.0/info || echo 000)
if [ "$code" = "200" ]; then echo "OK   node /info"; else echo "FAIL node /info ($code)"; fail=1; fi
if "$HB_REL/bin/hb" ping >/dev/null 2>&1; then echo "OK   hb ping"; else echo "FAIL hb ping"; fail=1; fi
code=$(curl -s --max-time 5 -o /dev/null -w '%{http_code}' http://127.0.0.1:1984/ || echo 000)
if [ "$code" = "200" ]; then echo "OK   arlocal /"; else echo "FAIL arlocal / ($code)"; fail=1; fi
exit $fail
