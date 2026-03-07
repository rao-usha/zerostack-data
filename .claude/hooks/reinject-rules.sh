#!/bin/bash
# Hook: SessionStart (compact|startup)
# Re-injects mandatory workflow rules after context compaction or session start.
# This is critical because compaction can lose behavioral instructions.

cat <<'EOF'
MANDATORY WORKFLOW RULES (re-injected by hook):

1. ALWAYS PLAN FIRST: Use EnterPlanMode + write docs/plans/PLAN_XXX.md for any multi-file task. Wait for user approval.

2. ALWAYS CREATE TASK CHECKLIST: Use TaskCreate for every task with 2+ steps. Mark in_progress before starting, completed after finishing.

3. ALWAYS LOG EVERY COMPLETED TASK: Write to memory/logs/YYYY-MM-DD.md after every code change, collection, scoring run, commit, or investigation. Not just at session end — after EVERY task.

4. SELF-CHECK: If you've done work but haven't logged in 2-3 messages, you missed a log entry. Write it now.

5. Read today's session log and last 2 days of logs at session start for context continuity.
EOF

exit 0
