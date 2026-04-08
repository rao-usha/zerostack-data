#!/bin/bash
# plan-feedback-tracker.sh — UserPromptSubmit hook
#
# Detects when the user gives feedback post-plan-completion and reminds
# Claude to create a versioned sub-plan document capturing the correction.
#
# Triggers on keywords: "feedback", "fix", "wrong", "broken", "should have",
# "didn't work", "not right", "change this", "correction", "bug", "issue with"

# Read the user's message from stdin
INPUT=$(cat)
# UserPromptSubmit sends the raw message text, not JSON — try both
MSG=$(echo "$INPUT" | jq -r '.user_message // .message // .content // empty' 2>/dev/null)
if [ -z "$MSG" ]; then
  # Might be raw text, not JSON
  MSG="$INPUT"
fi

if [ -z "$MSG" ]; then
  exit 0
fi

# Convert to lowercase for matching
LOWER=$(echo "$MSG" | tr '[:upper:]' '[:lower:]')

# Check for feedback/correction keywords (single words + short phrases)
FEEDBACK=0
for kw in "feedback" "wrong" "broken" "correction" "bug" "missed" "redo" "fix" "issue" "not right" "should have" "didn't work" "/feedback"; do
  if echo "$LOWER" | grep -qF "$kw"; then
    FEEDBACK=1
    break
  fi
done

if [ "$FEEDBACK" = "1" ]; then
  # Check if there's a recently completed plan
  LATEST_PLAN=$(ls -t docs/plans/PLAN_*.md 2>/dev/null | head -1)

  if [ -n "$LATEST_PLAN" ]; then
    PLAN_NAME=$(basename "$LATEST_PLAN" .md)

    # Find next revision number
    EXISTING_REVS=$(ls docs/plans/${PLAN_NAME}_rev_*.md 2>/dev/null | wc -l)
    NEXT_REV=$(printf "%02d" $((EXISTING_REVS + 1)))

    cat <<EOF
{
  "hookSpecificOutput": {
    "hookEventName": "UserPromptSubmit",
    "additionalContext": "POST-PLAN FEEDBACK DETECTED. MANDATORY: Before implementing the fix, you MUST:\n1. Create docs/plans/${PLAN_NAME}_rev_${NEXT_REV}.md with sections: ## Revision ${NEXT_REV}, ## What Was Wrong, ## What Was Fixed, ## Lessons Learned\n2. Append a '## Revisions' section to ${LATEST_PLAN} referencing the revision doc\n3. Append the correction to memory/feedback/corrections.md\nDo ALL THREE before writing any code fixes."
  }
}
EOF
  fi
fi
