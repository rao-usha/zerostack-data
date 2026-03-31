#!/bin/bash
# Hook: UserPromptSubmit
# AUTO-WRITES today's log if missing or stale (>45 min).
# Agents and subagents both benefit — no reliance on Claude choosing to log.

LOG_DIR="$HOME/.claude/projects/C--Users-awron-projects-Nexdata/memory/logs"
TODAY=$(date +%Y-%m-%d)
LOG_FILE="$LOG_DIR/$TODAY.md"

mkdir -p "$LOG_DIR"

# Auto-create today's log if it doesn't exist
if [ ! -f "$LOG_FILE" ]; then
  cat > "$LOG_FILE" <<LOGENTRY
# Session Log — $TODAY

## $(date +%H:%M) — [Session Start — auto-created by hook]

**What:** Session started
**Decisions:** None yet
**Files changed:** None yet
**Next steps:** Read last 2 days of logs for context continuity
**Blockers:** None

---
LOGENTRY
  cat <<EOJSON
{"systemMessage": "SESSION LOG CREATED: Auto-created memory/logs/$TODAY.md. MANDATORY: Read the last 2 days of logs for context, then update this entry as you complete tasks."}
EOJSON
  exit 0
fi

# Check staleness — auto-append checkpoint if >45 minutes since last update
LAST_MOD=$(stat -c %Y "$LOG_FILE" 2>/dev/null)
if [ -z "$LAST_MOD" ]; then
  LAST_MOD=$(stat -f %m "$LOG_FILE" 2>/dev/null)
fi
NOW=$(date +%s)

if [ -n "$LAST_MOD" ] && [ -n "$NOW" ]; then
  DIFF=$((NOW - LAST_MOD))
  if [ "$DIFF" -gt 2700 ]; then
    # Auto-append a 45-min checkpoint so the file is always current
    cat >> "$LOG_FILE" <<CHECKPOINT

## $(date +%H:%M) — [Auto-checkpoint: 45-min interval]

**What:** Auto-logged by hook — no entry in 45+ min
**Decisions:** *(Claude should replace this with real work summary)*
**Files changed:** *(fill in)*
**Next steps:** *(fill in)*
**Blockers:** None

---
CHECKPOINT
    touch "$LOG_FILE"
    cat <<EOJSON
{"systemMessage": "LOG CHECKPOINT WRITTEN: 45+ minutes without a log update. Auto-appended a checkpoint to memory/logs/$TODAY.md — please fill it in with what you've been working on before continuing."}
EOJSON
  fi
fi

exit 0
