---
name: session-log
description: Log a session checkpoint to the daily work log. Summarizes what was done, decisions made, and next steps. Use anytime to capture progress. MUST be called after every completed task.
allowed-tools:
  - Read
  - Write
  - Edit
  - Glob
  - Bash
argument-hint: "[optional summary note]"
---

Write a checkpoint entry to today's daily work log in the memory directory.

## THIS IS MANDATORY

You MUST call this after every completed task — not just at session end. If you changed code, ran a collection, generated a report, fixed a bug, or made any progress at all, LOG IT. Do not wait to batch entries. Log immediately after each task completes.

## Log file location

```
C:\Users\awron\.claude\projects\C--Users-awron-projects-Nexdata\memory\logs\YYYY-MM-DD.md
```

Use today's date for the filename.

## Behavior

1. **Get the current time** for the entry timestamp:
   ```bash
   date +%H:%M
   ```

2. **Read the existing log file** for today (if it exists) to avoid duplicating entries.

3. **If the file doesn't exist yet**, create it with this header:
   ```markdown
   # Nexdata Work Log — YYYY-MM-DD

   ---
   ```

4. **Append a new entry** at the bottom of the file with this format:
   ```markdown
   ## HH:MM — [Brief title of what was done]

   **What:** [1-3 bullet points describing the work completed]

   **Decisions:** [Key decisions or choices made, if any]

   **Files changed:** [List of files modified/created, if any]

   **Next steps:** [What should happen next or what's left to do]

   **Blockers:** [Any blockers or issues encountered, or "None"]

   ---
   ```

5. **If `$ARGUMENTS` is provided**, use it as context for the summary. Otherwise, summarize the conversation so far.

6. **Also update the log index** at `memory/logs/INDEX.md` — append today's date if not already listed.

## Guidelines

- Keep entries concise but informative — another Claude session should be able to pick up from these notes
- Include specific file paths, endpoint URLs, error messages — concrete details over vague summaries
- If a task is partially done, explicitly note where to resume
- Don't repeat information already logged today — scan existing entries first
- Focus on **what changed** and **why**, not play-by-play of commands run
- Include data results (row counts, score ranges, etc.) when relevant — these are hard to reconstruct later

## What MUST be logged (non-exhaustive)

- Code changes (files created, modified, deleted)
- Bug fixes (what was broken, what fixed it)
- Data collection runs (what was collected, how many rows)
- Report generation (which report, what changed)
- Scoring/ML runs (results, score distributions)
- Commits and pushes
- Infrastructure changes (Docker, config, etc.)
- Research findings and architectural decisions
- Failed attempts and why they failed

## Example entry

```markdown
## 14:32 — Added FCC broadband collector to site intel

**What:**
- Created `app/sources/site_intel/telecom/fcc_broadband.py` with Socrata API integration
- Added unique index on `(provider_id, census_block)` to prevent duplicates
- Registered collector in `telecom/__init__.py`

**Decisions:** Used Socrata API instead of bulk CSV download (faster, supports pagination)

**Files changed:** `fcc_broadband.py`, `telecom/__init__.py`, `models_site_intel.py`

**Next steps:** Test with `POST /api/v1/site-intel/collect/fcc_broadband`, verify dedup works

**Blockers:** None

---
```
