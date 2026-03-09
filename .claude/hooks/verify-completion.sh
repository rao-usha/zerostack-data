#!/bin/bash
# Hook: Notification (SubToolCompleted)
# BLOCKS task completion if spec tests are failing.
# Only blocks when: active spec exists AND spec has a test file AND tests fail.
# If no spec or no tests: allows completion (no false blocks).

# Check for active spec
SPEC_FILE="docs/specs/.active_spec"

if [ ! -f "$SPEC_FILE" ]; then
  # No spec system active — allow completion
  exit 0
fi

ACTIVE_SPEC=$(cat "$SPEC_FILE" 2>/dev/null | tr -d '[:space:]')

# No active spec or bypass — allow
if [ -z "$ACTIVE_SPEC" ] || [ "$ACTIVE_SPEC" = "BYPASS_TRIVIAL" ]; then
  exit 0
fi

# Find the spec document
SPEC_DOC="docs/specs/${ACTIVE_SPEC}.md"
if [ ! -f "$SPEC_DOC" ]; then
  # Spec file missing — allow (don't block on missing files)
  exit 0
fi

# Extract test file from spec
TEST_FILE=$(grep -oP 'Test file:\s*\K\S+' "$SPEC_DOC" 2>/dev/null | head -1)

if [ -z "$TEST_FILE" ] || [ ! -f "$TEST_FILE" ]; then
  # No test file declared or doesn't exist — allow
  exit 0
fi

# Run the spec's tests
TEST_OUTPUT=$(timeout 55 python -m pytest "$TEST_FILE" -v --tb=short 2>&1 | tail -25)
TEST_EXIT=$?

if [ $TEST_EXIT -eq 0 ]; then
  # Tests pass — allow completion
  cat <<EOJSON
{
  "hookSpecificOutput": {
    "hookEventName": "Notification",
    "additionalContext": "Spec tests PASSED (${TEST_FILE}). Task completion allowed."
  }
}
EOJSON
  exit 0
else
  # Tests fail — BLOCK completion
  cat <<EOJSON
{
  "hookSpecificOutput": {
    "hookEventName": "Notification",
    "additionalContext": "BLOCKED: Spec tests FAILING (${TEST_FILE}). Fix failing tests before marking this task complete.\n\n${TEST_OUTPUT}"
  }
}
EOJSON
  exit 2
fi
