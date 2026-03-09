---
name: feedback
description: Capture user corrections and update rubrics for future tasks. Use when the user points out something that was missed or done wrong.
allowed-tools:
  - Read
  - Write
  - Edit
  - Glob
  - Grep
argument-hint: "<correction or feedback text>"
---

You have been asked to capture a user correction or feedback item. This feeds back into the quality system so future specs and tasks avoid the same mistakes.

## Arguments

`$ARGUMENTS` contains the correction text. If empty, ask the user what they want to correct.

## Step 1: Determine Context

Figure out:
- **What was the correction?** (the user's feedback)
- **What task type does it apply to?** (collector, api_endpoint, bug_fix, report, service, model, or general)
- **Is there an active spec?** Read `docs/specs/.active_spec` to find the current spec
- **What was the specific mistake?** (missed check, wrong pattern, security issue, etc.)

## Step 2: Log the Correction

Append to `C:\Users\awron\.claude\projects\C--Users-awron-projects-Nexdata\memory\feedback\corrections.md`:

```markdown
## YYYY-MM-DD — [Brief title]

**Correction:** [What the user said]
**Task type:** [collector | api_endpoint | bug_fix | report | service | model | general]
**Active spec:** [spec name or "none"]
**Root cause:** [Why this was missed — e.g., "rubric didn't check for X"]
**Action:** [What was updated — e.g., "Added item to collector rubric"]

---
```

## Step 3: Update the Relevant Rubric

Read the rubric file for the task type from:
`C:\Users\awron\.claude\projects\C--Users-awron-projects-Nexdata\memory\rubrics/<task_type>.md`

Add a new checklist item that would have caught this mistake. Place it in the most logical position in the rubric.

If the correction is general (applies to all types), update ALL rubric files.

## Step 4: Update Active Spec (if applicable)

If there's an active spec, read it and append to its **Feedback History** section:

```markdown
- **YYYY-MM-DD:** [Correction text] → [Action taken]
```

## Step 5: Confirm to User

Tell the user:
1. Correction logged to `corrections.md`
2. Which rubric(s) were updated and what was added
3. Future `/spec` invocations will include the new check

## Key Rules

- NEVER skip logging to corrections.md — this is the audit trail
- ALWAYS update the rubric — corrections without rubric updates will repeat
- Keep rubric items actionable and checkable (start with a verb)
- If the correction contradicts an existing rubric item, UPDATE the existing item (don't create duplicates)
- If unsure which task type applies, ask the user
