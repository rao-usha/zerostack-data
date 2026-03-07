#!/bin/bash
# Hook: PreToolUse (Edit|Write)
# Reminds Claude to have a plan and task checklist before writing code.
# Does NOT block — injects additionalContext as a gentle nudge.

# Read hook input from stdin
INPUT=$(cat)

# Extract file_path using python (jq not available on this system)
FILE_PATH=$(echo "$INPUT" | python -c "
import json, sys
try:
    d = json.load(sys.stdin)
    print(d.get('tool_input', {}).get('file_path', '') or d.get('tool_input', {}).get('filePath', ''))
except:
    print('')
" 2>/dev/null)

# Normalize path separators
NORM_PATH=$(echo "$FILE_PATH" | tr '\\' '/')

# Skip non-source files (config, docs, memory, hooks, skills, markdown)
if echo "$NORM_PATH" | grep -qiE '(memory/|\.claude/|docs/plans/|\.gitignore|settings|CLAUDE\.md|MEMORY\.md|\.md$)'; then
  exit 0
fi

# Skip if file_path is empty (Write tool for new files sometimes)
if [ -z "$NORM_PATH" ]; then
  exit 0
fi

cat <<EOJSON
{
  "hookSpecificOutput": {
    "hookEventName": "PreToolUse",
    "additionalContext": "WORKFLOW CHECK: Before writing code, confirm you have: (1) A plan or clear approach, (2) A TaskCreate checklist tracking your steps, (3) Today's session log started. If any of these are missing, create them first."
  }
}
EOJSON

exit 0
