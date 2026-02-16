---
name: logs
description: Search and filter docker logs for a specific keyword, error, or pattern. Use when the user wants to find specific log entries, debug errors, or trace a specific operation.
allowed-tools:
  - Bash
argument-hint: "[keyword or pattern]"
---

Search Nexdata API container logs for a specific keyword or pattern.

## Behavior

1. **Determine what to search for:**
   - Use `$ARGUMENTS` as the search pattern
   - If no argument, ask the user what they want to find

2. **Search the logs:**
   ```bash
   docker logs nexdata-api-1 2>&1 | grep -i "PATTERN" | tail -30
   ```

3. **Smart filtering based on common patterns:**

   If the keyword looks like an **error search** (`error`, `fail`, `traceback`, `exception`):
   ```bash
   docker logs nexdata-api-1 2>&1 | grep -i "ERROR\|CRITICAL\|Traceback\|Exception" | tail -30
   ```
   Also grab context around tracebacks:
   ```bash
   docker logs nexdata-api-1 2>&1 | grep -B 2 -A 5 "Traceback" | tail -40
   ```

   If the keyword is a **company/firm name**:
   ```bash
   docker logs nexdata-api-1 2>&1 | grep -i "COMPANY_NAME" | tail -30
   ```

   If the keyword is a **job type** (`pe`, `people`, `ingest`, `site_intel`):
   - Filter for relevant log prefixes and summarize

   If the keyword is a **time range** (`today`, `last hour`):
   - Filter by timestamp pattern

4. **Present results clearly:**
   - Show matching log lines with timestamps
   - Highlight errors in context
   - If there are many results, summarize the count and show the most recent
   - Offer to dig deeper if the user needs more context

5. **Count matches** to give context:
   ```bash
   docker logs nexdata-api-1 2>&1 | grep -ic "PATTERN"
   ```
   Report: "Found N matches, showing last 30"

## Important
- Container name: `nexdata-api-1`
- Logs can be very large — always use `tail` to limit output
- Use `-i` for case-insensitive search by default
- For regex patterns, use `grep -E`
- If the user wants to follow logs in real-time, suggest using `/monitor` instead
