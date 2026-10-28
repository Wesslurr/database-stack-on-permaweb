#!/usr/bin/env bash
# Launch Tahap 17 in a detached tmux session so it survives host/agent restarts.
LOG=/home/weslur/database/data/tahap17.log
tmux kill-session -t t17 2>/dev/null || true
rm -f "$LOG"
tmux new-session -d -s t17 "bash /home/weslur/database/scripts/run_tahap17.sh > $LOG 2>&1; echo EXIT=\$? >> $LOG"
echo "started t17 -> $LOG"
