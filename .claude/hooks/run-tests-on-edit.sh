#!/bin/bash
# Hook: PostToolUse (Edit|Write)
# Runs matched tests after source file edits. Non-blocking (additionalContext only).
# Test discovery: (1) spec-declared test file, (2) naming convention, (3) grep for imports

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

# If no file path, skip
if [ -z "$FILE_PATH" ]; then
  exit 0
fi

# Normalize path
NORM_PATH=$(echo "$FILE_PATH" | tr '\\' '/')

# Only run tests for .py source files in app/
if ! echo "$NORM_PATH" | grep -qE '\.py$'; then
  exit 0
fi
if ! echo "$NORM_PATH" | grep -qE '(^|/)app/'; then
  # Also run if editing a test file itself
  if ! echo "$NORM_PATH" | grep -qE 'test_.*\.py$'; then
    exit 0
  fi
fi

# Extract module name from path (e.g., app/services/labor_arbitrage.py -> labor_arbitrage)
MODULE_NAME=$(basename "$NORM_PATH" .py)

TEST_FILE=""

# Strategy 1: Check active spec for declared test file
SPEC_FILE="docs/specs/.active_spec"
if [ -f "$SPEC_FILE" ]; then
  ACTIVE_SPEC=$(cat "$SPEC_FILE" 2>/dev/null | tr -d '[:space:]')
  if [ -n "$ACTIVE_SPEC" ] && [ "$ACTIVE_SPEC" != "BYPASS_TRIVIAL" ]; then
    SPEC_DOC="docs/specs/${ACTIVE_SPEC}.md"
    if [ -f "$SPEC_DOC" ]; then
      # Extract test file from spec's "Test file:" field
      SPEC_TEST=$(grep -oP 'Test file:\s*\K\S+' "$SPEC_DOC" 2>/dev/null | head -1)
      if [ -n "$SPEC_TEST" ] && [ -f "$SPEC_TEST" ]; then
        TEST_FILE="$SPEC_TEST"
      fi
    fi
  fi
fi

# Strategy 2: Naming convention (app/x/y/module.py -> tests/test_module.py)
if [ -z "$TEST_FILE" ]; then
  CONV_TEST="tests/test_${MODULE_NAME}.py"
  if [ -f "$CONV_TEST" ]; then
    TEST_FILE="$CONV_TEST"
  fi
fi

# Strategy 3: If editing a test file directly, use that file
if [ -z "$TEST_FILE" ] && echo "$NORM_PATH" | grep -qE 'test_.*\.py$'; then
  if [ -f "$FILE_PATH" ]; then
    TEST_FILE="$FILE_PATH"
  fi
fi

# If no test file found, just remind
if [ -z "$TEST_FILE" ]; then
  cat <<EOJSON
{
  "hookSpecificOutput": {
    "hookEventName": "PostToolUse",
    "additionalContext": "Source file edited: ${MODULE_NAME}.py. No matching test file found (checked: tests/test_${MODULE_NAME}.py). Consider creating tests."
  }
}
EOJSON
  exit 0
fi

# Run tests and capture result (with timeout)
NORM_TEST=$(echo "$TEST_FILE" | tr '\\' '/')
TEST_OUTPUT=$(timeout 55 python -m pytest "$NORM_TEST" -v --tb=short 2>&1 | tail -20)
TEST_EXIT=$?

if [ $TEST_EXIT -eq 0 ]; then
  RESULT_MSG="Tests PASSED for ${NORM_TEST}"
else
  RESULT_MSG="Tests FAILED for ${NORM_TEST}. Fix before marking task complete.\n\nOutput (last 20 lines):\n${TEST_OUTPUT}"
fi

cat <<EOJSON
{
  "hookSpecificOutput": {
    "hookEventName": "PostToolUse",
    "additionalContext": "${RESULT_MSG}"
  }
}
EOJSON
exit 0
