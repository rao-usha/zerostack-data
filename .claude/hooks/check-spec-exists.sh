#!/bin/bash
# Hook: PreToolUse (Edit|Write)
# BLOCKS source code edits (app/*.py) when no active spec exists.
# Escape hatches: BYPASS_TRIVIAL in .active_spec, test files, non-.py, docs/memory/.claude

# Read hook input from stdin
INPUT=$(cat)

# Extract file_path
FILE_PATH=$(echo "$INPUT" | python -c "
import json, sys
try:
    d = json.load(sys.stdin)
    print(d.get('tool_input', {}).get('file_path', '') or d.get('tool_input', {}).get('filePath', ''))
except:
    print('')
" 2>/dev/null)

# If we can't determine the file path, allow (no false blocks)
if [ -z "$FILE_PATH" ]; then
  exit 0
fi

# Normalize path separators
NORM_PATH=$(echo "$FILE_PATH" | tr '\\' '/')

# --- ALWAYS ALLOW these file types/locations ---

# Non-.py files (markdown, json, html, css, sh, etc.)
if ! echo "$NORM_PATH" | grep -qE '\.py$'; then
  exit 0
fi

# Test files — always editable without spec
if echo "$NORM_PATH" | grep -qE '(^|/)tests?/|test_.*\.py$|conftest\.py$'; then
  exit 0
fi

# Docs, memory, hooks, skills, config, plans
if echo "$NORM_PATH" | grep -qiE '(memory/|\.claude/|docs/|\.gitignore|settings|CLAUDE\.md|MEMORY\.md)'; then
  exit 0
fi

# --- CHECK: is this an app source file? ---
# Only enforce spec requirement for app/ source code
if ! echo "$NORM_PATH" | grep -qE '(^|/)app/'; then
  exit 0
fi

# --- CHECK: does an active spec exist? ---
SPEC_FILE="docs/specs/.active_spec"

if [ ! -f "$SPEC_FILE" ]; then
  # No active spec file at all — BLOCK
  cat <<EOJSON
{
  "hookSpecificOutput": {
    "hookEventName": "PreToolUse",
    "permissionDecision": "deny",
    "reason": "BLOCKED: No active spec found. You MUST create a spec before editing source code. Run /spec <task_type> <feature_name> to create one. For trivial single-line fixes, write BYPASS_TRIVIAL to docs/specs/.active_spec"
  }
}
EOJSON
  exit 0
fi

# Read the active spec content
ACTIVE_SPEC=$(cat "$SPEC_FILE" 2>/dev/null | tr -d '[:space:]')

# Empty file — BLOCK
if [ -z "$ACTIVE_SPEC" ]; then
  cat <<EOJSON
{
  "hookSpecificOutput": {
    "hookEventName": "PreToolUse",
    "permissionDecision": "deny",
    "reason": "BLOCKED: Active spec file is empty. Run /spec <task_type> <feature_name> to create a spec, or write BYPASS_TRIVIAL for trivial fixes."
  }
}
EOJSON
  exit 0
fi

# BYPASS_TRIVIAL escape hatch — allow
if [ "$ACTIVE_SPEC" = "BYPASS_TRIVIAL" ]; then
  exit 0
fi

# Has a valid spec name — allow edits
# Optionally verify the spec file exists
SPEC_DOC="docs/specs/${ACTIVE_SPEC}.md"
if [ ! -f "$SPEC_DOC" ]; then
  cat <<EOJSON
{
  "hookSpecificOutput": {
    "hookEventName": "PreToolUse",
    "additionalContext": "WARNING: Active spec points to '${ACTIVE_SPEC}' but docs/specs/${ACTIVE_SPEC}.md does not exist. The spec may have been moved or deleted. Edits are allowed but consider running /spec to recreate it."
  }
}
EOJSON
  exit 0
fi

# Valid spec exists — allow with context
cat <<EOJSON
{
  "hookSpecificOutput": {
    "hookEventName": "PreToolUse",
    "additionalContext": "Active spec: ${ACTIVE_SPEC}. Ensure your edit aligns with the spec's acceptance criteria and test cases."
  }
}
EOJSON
exit 0
