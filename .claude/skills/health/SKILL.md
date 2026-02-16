---
name: health
description: Quick health check of all Nexdata services. Use when the user wants to verify the API is up, check for stuck jobs, or see overall system status.
allowed-tools:
  - Bash
---

Perform a comprehensive health check of the Nexdata stack and report status.

## Checks to perform (run in parallel where possible)

1. **Container status:**
   ```bash
   docker-compose ps 2>&1
   ```
   Report which containers are running/stopped/unhealthy.

2. **API responsiveness:**
   ```bash
   curl -s -o /dev/null -w "%{http_code} %{time_total}s" http://localhost:8001/docs
   ```
   Report HTTP status and response time.

3. **Database connectivity:**
   ```bash
   docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "SELECT 1" 2>&1
   ```

4. **Job status overview:**
   ```bash
   docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "
     SELECT status, COUNT(*) FROM ingestion_jobs GROUP BY status ORDER BY status;
   "
   ```

5. **Stuck/failed jobs (last 24h):**
   ```bash
   docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "
     SELECT id, source, status, created_at
     FROM ingestion_jobs
     WHERE status IN ('RUNNING', 'FAILED')
       AND created_at > NOW() - INTERVAL '24 hours'
     ORDER BY created_at DESC
     LIMIT 10;
   "
   ```

6. **LLM spend (from recent logs):**
   ```bash
   docker logs nexdata-api-1 2>&1 | grep "session_total=" | tail -1
   ```

7. **Recent errors (last 50 log lines):**
   ```bash
   docker logs nexdata-api-1 --tail 200 2>&1 | grep -i "ERROR\|CRITICAL\|Traceback" | tail -10
   ```

8. **Scheduler status (are background jobs ticking):**
   ```bash
   docker logs nexdata-api-1 --tail 50 2>&1 | grep "apscheduler" | tail -5
   ```

## Output format

Present a clean status dashboard:

```
Service        | Status | Details
---------------|--------|------------------
API            |   OK   | 200 in 0.12s
PostgreSQL     |   OK   | Connected
Scheduler      |   OK   | Last tick 2m ago
Jobs (24h)     |  WARN  | 2 failed, 0 stuck
LLM Spend      |  INFO  | $1.46 session
Recent Errors  |   OK   | None
```

Use OK / WARN / ERROR status indicators. Only flag items that need attention.
