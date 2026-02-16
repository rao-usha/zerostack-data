---
name: monitor
description: Monitor running jobs by tailing docker logs. Use when the user wants to watch progress of PE collection, people collection, ingestion, or any background job.
allowed-tools:
  - Bash
argument-hint: "[job-type or keyword]"
---

Monitor active jobs running in the Nexdata API container by tailing docker logs.

## Behavior

1. First check the latest logs to identify what's currently running:
   ```bash
   docker logs nexdata-api-1 --tail 30 2>&1
   ```

2. Identify the active job type (PE collection, people collection, ingestion, etc.) from log patterns:
   - PE collection: look for "PE collection", "Collected.*items from", "PE Persister"
   - People collection: look for "people", "PageFinder", "WebsiteAgent", "org_chart"
   - Ingestion: look for "ingest", "IngestionJob", dataset names
   - If `$ARGUMENTS` is provided, filter for that keyword/job type

3. Poll every 60-120 seconds, reporting:
   - Which entities/firms are being processed
   - Items collected so far
   - Any errors or retries happening
   - LLM cost accumulating (from LLMCostTracker lines)

4. When you see completion markers ("Persister done", "collection complete", "Job.*success"), report the final summary as a table with: entities processed, items found, persisted, updated, failed, duration, and LLM cost.

5. If no active jobs are found, report that the API is idle and show when the last job completed.

## Key log patterns

```
# PE collection
"Starting PE collection: N entities"
"Collected N items from (news_api|press_release|sec_13f|pitchbook) for FIRM"
"PE collection complete: N successful, N failed in Xs"
"PE Persister done: persisted=N, updated=N, skipped=N, failed=N"

# People collection
"Starting people collection for COMPANY"
"PageFinder found N URLs"
"Extracted N people from"
"People collection complete"

# General ingestion
"IngestionJob .* status=running"
"IngestionJob .* status=success"
"IngestionJob .* status=failed"

# Errors
"ERROR"
"retry \d+/\d+"
"Timeout fetching"
"Server error"
```

## Important
- Container name is `nexdata-api-1`
- Don't overwhelm output — summarize progress concisely between polls
- If the user says to stop monitoring, stop immediately
- Report errors/warnings but don't panic about retries — they're normal
