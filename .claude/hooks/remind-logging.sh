#!/bin/bash
# Hook: PostToolUse (Edit|Write)
# AUTO-APPENDS the modified file path to today's log as a breadcrumb.
# Works for main agent AND subagents — no reliance on Claude choosing to log.

LOG_DIR="$HOME/.claude/projects/C--Users-awron-projects-Nexdata/memory/logs"
TODAY=$(date +%Y-%m-%d)
LOG_FILE="$LOG_DIR/$TODAY.md"

# Parse the file path from stdin (matches check-spec-exists.sh pattern)
INPUT=$(cat)
FILE_PATH=$(echo "$INPUT" | python -c "
import json, sys
try:
    d = json.load(sys.stdin)
    fp = d.get('tool_input', {}).get('file_path', '') or d.get('tool_input', {}).get('filePath', '')
    print(fp.strip())
except:
    print('')
" 2>/dev/null)

if [ -z "$FILE_PATH" ]; then
  FILE_PATH="unknown"
fi

# Don't create a feedback loop by logging edits to the log file itself
if [[ "$FILE_PATH" == *"memory/logs"* ]] || [[ "$FILE_PATH" == *"memory/feedback"* ]]; then
  exit 0
fi

# Auto-append a breadcrumb comment to the log (only if log exists)
if [ -f "$LOG_FILE" ]; then
  echo "<!-- $(date +%H:%M) modified: $FILE_PATH -->" >> "$LOG_FILE"
fi

cat <<EOJSON
{
  "hookSpecificOutput": {
    "hookEventName": "PostToolUse",
    "additionalContext": "Modified ${FILE_PATH} at $(date +%H:%M). Add a meaningful log entry to memory/logs/${TODAY}.md — breadcrumb auto-written, but fill in what/why."
  }
}
EOJSON

exit 0
