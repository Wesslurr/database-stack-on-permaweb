#!/usr/bin/env bash
# Launch Tahap 18 in a detached tmux session so it survives host/agent restarts.
LOG=/home/weslur/database/data/tahap18.log
tmux kill-session -t t18 2>/dev/null || true
rm -f "$LOG"
tmux new-session -d -s t18 "node /home/weslur/database/tests/test_tahap18_chaos_and_boundaries.mjs > $LOG 2>&1; echo EXIT=\$? >> $LOG"
echo "started t18 -> $LOG"
