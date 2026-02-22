---
name: test-endpoint
description: Quick-test any Nexdata API endpoint with formatted JSON output. Handles GET/POST routing, common parameters, and error display. Use when testing or exploring endpoints.
allowed-tools:
  - Bash
argument-hint: "<method> <path> [params]"
---

Test a Nexdata API endpoint and display formatted results.

## Behavior

1. **Parse `$ARGUMENTS`** to extract method, path, and parameters.

   Formats accepted:
   - `GET /fred/categories` — simple GET
   - `POST /fred/ingest category=gdp` — POST with params
   - `/search?q=Sequoia` — GET implied for query paths
   - `fred categories` — shorthand, resolve to GET /fred/categories
   - `jobs 123` — shorthand for GET /jobs/123
   - `pe firms` — shorthand for GET /pe/firms/

2. **Build the request:**

   Base URL: `http://localhost:8001/api/v1`

   For GET requests with query params:
   ```bash
   curl -s "http://localhost:8001/api/v1/PATH?param=value"
   ```

   For POST requests with JSON body:
   ```bash
   curl -s -X POST "http://localhost:8001/api/v1/PATH" \
     -H "Content-Type: application/json" \
     -d '{"param": "value"}'
   ```

3. **Format the output** using Python for readability:
   ```bash
   curl -s [-X POST] "URL" [-H "Content-Type: application/json" -d 'BODY'] | python -c "
   import sys,json
   try:
       d = json.load(sys.stdin)
       # If it's a list, show count and first few items
       if isinstance(d, list):
           print(f'Results: {len(d)} items')
           for item in d[:5]:
               print(json.dumps(item, indent=2, default=str)[:500])
               print('---')
           if len(d) > 5:
               print(f'... and {len(d)-5} more')
       else:
           print(json.dumps(d, indent=2, default=str)[:2000])
   except json.JSONDecodeError:
       text = sys.stdin.read() if hasattr(sys.stdin, 'read') else ''
       print(f'Non-JSON response: {text[:500]}')
   "
   ```

4. **Also show HTTP status:**
   ```bash
   curl -s -o /dev/null -w "%{http_code}" "URL"
   ```

5. **For large responses** (tables, lists), truncate and summarize:
   - Lists: show count + first 5 items
   - Tables data: show column count + row count + first 3 rows
   - Large objects: show top-level keys and sizes

## Common shorthand mappings

| Shorthand | Resolves to |
|-----------|-------------|
| `health` | `GET /jobs/monitoring/health` |
| `dashboard` | `GET /jobs/monitoring/dashboard` |
| `tables` | `GET /export/tables` |
| `preview <table>` | `GET /export/tables/<table>/preview?limit=10` |
| `jobs` | `GET /jobs?limit=10` |
| `job <id>` | `GET /jobs/<id>` |
| `search <query>` | `GET /search?q=<query>` |
| `people` | `GET /people?page_size=10` |
| `pe firms` | `GET /pe/firms/?limit=10` |
| `pe deals` | `GET /pe/deals/?limit=10` |
| `family-offices` | `GET /family-offices/?limit=10` |
| `schedules` | `GET /schedules` |
| `alerts` | `GET /jobs/monitoring/alerts` |
| `formats` | `GET /export/formats` |
| `settings` | `GET /settings/api-keys` |

## Error handling

- If 404: suggest the correct endpoint path (check openapi.json)
- If 422: show the validation error details (FastAPI returns field-level errors)
- If 500: show the error message and suggest checking `/logs`
- If connection refused: suggest `/restart`

## Important
- Always use `http://localhost:8001/api/v1` as the base
- POST endpoints need `Content-Type: application/json`
- Large JSON responses (>100KB) should be piped through a summary instead of raw display
- The `/export/tables` response is ~100KB — always summarize it
