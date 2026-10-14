#!/usr/bin/env bash
set -euo pipefail
source /home/weslur/database/scripts/env.sh
"$HB_REL/bin/hb" eval "$@"
