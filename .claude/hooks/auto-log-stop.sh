#!/bin/bash
# Hook: Stop
# Fires at the end of every Claude turn. If the log hasn't been updated in
# the last 5 minutes, appends a turn-end breadcrumb so there's always a
# timestamp trail — works for main agent and subagents alike.

LOG_DIR="$HOME/.claude/projects/C--Users-awron-projects-Nexdata/memory/logs"
TODAY=$(date +%Y-%m-%d)
LOG_FILE="$LOG_DIR/$TODAY.md"

# Nothing to do if no log file today
if [ ! -f "$LOG_FILE" ]; then
  exit 0
fi

# Only append if log hasn't been touched in the last 5 minutes
LAST_MOD=$(stat -c %Y "$LOG_FILE" 2>/dev/null)
if [ -z "$LAST_MOD" ]; then
  LAST_MOD=$(stat -f %m "$LOG_FILE" 2>/dev/null)
fi
NOW=$(date +%s)

if [ -n "$LAST_MOD" ] && [ $((NOW - LAST_MOD)) -gt 300 ]; then
  echo "<!-- turn-end $(date +%H:%M) -->" >> "$LOG_FILE"
fi

exit 0
