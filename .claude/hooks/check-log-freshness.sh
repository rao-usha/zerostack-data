#!/bin/bash
# Hook: UserPromptSubmit
# Checks if today's session log exists and has been updated recently.
# Injects a reminder if stale or missing.

LOG_DIR="$HOME/.claude/projects/C--Users-awron-projects-Nexdata/memory/logs"
TODAY=$(date +%Y-%m-%d)
LOG_FILE="$LOG_DIR/$TODAY.md"

# No log file for today at all
if [ ! -f "$LOG_FILE" ]; then
  cat <<EOJSON
{"systemMessage": "SESSION LOG MISSING: No log exists for today ($TODAY). You MUST create memory/logs/$TODAY.md and read the last 2 days of logs before doing any work. This is mandatory per CLAUDE.md."}
EOJSON
  exit 0
fi

# Check if file was modified in last 45 minutes
if command -v stat &>/dev/null; then
  LAST_MOD=$(stat -c %Y "$LOG_FILE" 2>/dev/null)
  if [ -z "$LAST_MOD" ]; then
    # macOS stat fallback
    LAST_MOD=$(stat -f %m "$LOG_FILE" 2>/dev/null)
  fi
  NOW=$(date +%s)
  if [ -n "$LAST_MOD" ] && [ -n "$NOW" ]; then
    DIFF=$((NOW - LAST_MOD))
    if [ $DIFF -gt 2700 ]; then
      cat <<EOJSON
{"systemMessage": "LOG STALE: Session log has not been updated in over 45 minutes. If you have completed ANY work since the last log entry, you MUST log it now to memory/logs/$TODAY.md before continuing."}
EOJSON
    fi
  fi
fi

exit 0
