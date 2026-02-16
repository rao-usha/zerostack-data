---
name: restart
description: Restart the Nexdata API container after code changes, wait for it to be ready, then verify health. Use when the user has made code changes and needs to restart.
allowed-tools:
  - Bash
argument-hint: "[--rebuild]"
---

Restart the Nexdata API service and verify it comes up healthy.

## Behavior

1. **Check for active jobs first:**
   ```bash
   docker logs nexdata-api-1 --tail 20 2>&1
   ```
   If there's an active collection or ingestion running, **warn the user** and ask for confirmation before restarting. Don't restart without approval if jobs are in progress.

2. **Restart the API:**

   If `$ARGUMENTS` contains `--rebuild` or `build`:
   ```bash
   docker-compose down && docker-compose build --no-cache api && docker-compose up -d
   ```
   Otherwise (default — fast restart for code changes):
   ```bash
   docker-compose restart api
   ```

3. **Wait for startup** — the app takes 20-30 seconds to initialize due to many imports. Poll until ready:
   ```bash
   # Wait then check
   sleep 25
   curl -s -o /dev/null -w "%{http_code}" http://localhost:8001/docs
   ```
   If not 200, wait another 10 seconds and retry (up to 3 attempts).

4. **Verify health:**
   - Confirm API returns 200
   - Check last few log lines for startup errors
   - Confirm scheduler is running (apscheduler lines in logs)

5. **Report result:**
   - Success: "API restarted and healthy (took Xs)"
   - Failure: Show the error logs and suggest next steps (usually `--rebuild` or check for import errors)

## Important
- Volume-mounted `./app` means code changes are live, but the Python process needs restart to pick up new imports
- A full rebuild (`--rebuild`) is needed when dependencies change or Docker cache is stale
- Container name: `nexdata-api-1`
- API is healthy when `http://localhost:8001/docs` returns 200
