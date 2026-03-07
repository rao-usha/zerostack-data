#!/bin/bash
# Hook: PostToolUse (Edit|Write)
# After code changes, reminds Claude to log the work.

cat <<EOJSON
{
  "hookSpecificOutput": {
    "hookEventName": "PostToolUse",
    "additionalContext": "You just modified a file. Remember: you MUST log this change to today's session log (memory/logs/$(date +%Y-%m-%d).md) after completing this task. Do not wait until session end."
  }
}
EOJSON

exit 0
