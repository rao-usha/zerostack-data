---
name: export
description: Quick export any database table to CSV, JSON, or Parquet. Creates an export job, waits for completion, and provides the download. Use when the user wants to extract data.
allowed-tools:
  - Bash
argument-hint: "<table_name> [format] [options]"
---

Export a database table to a file (CSV, JSON, or Parquet).

## Behavior

1. **Parse `$ARGUMENTS`:**
   - First arg: table name (required)
   - Second arg: format — `csv` (default), `json`, or `parquet`
   - Options: `--limit N`, `--compress`, `--columns col1,col2,col3`

2. **Verify the table exists:**
   ```bash
   curl -s "http://localhost:8001/api/v1/export/tables" | python -c "
   import sys,json
   TABLE = 'TABLE_NAME'
   tables = json.load(sys.stdin)
   match = [t for t in tables if t['table_name'] == TABLE]
   if not match:
       # Try fuzzy match
       fuzzy = [t for t in tables if TABLE in t['table_name']]
       if fuzzy:
           print(f'Table \"{TABLE}\" not found. Did you mean:')
           for t in fuzzy[:5]:
               print(f'  {t[\"table_name\"]} ({t[\"row_count\"]:,} rows)')
       else:
           print(f'Table \"{TABLE}\" not found.')
       sys.exit(1)
   t = match[0]
   print(f'Found: {t[\"table_name\"]} — {t[\"row_count\"]:,} rows, {len(t[\"columns\"])} columns')
   "
   ```

3. **Preview before export** (show first 3 rows):
   ```bash
   curl -s "http://localhost:8001/api/v1/export/tables/TABLE_NAME/preview?limit=3" | python -c "
   import sys,json; d=json.load(sys.stdin)
   cols = d['columns'][:6]
   print('Preview (first 3 rows, first 6 columns):')
   print(' | '.join(f'{c:18s}' for c in cols))
   for row in d['rows']:
       print(' | '.join(f'{str(row.get(c,\"\"))[:18]:18s}' for c in cols))
   "
   ```

4. **Create the export job:**
   ```bash
   curl -s -X POST "http://localhost:8001/api/v1/export/jobs" \
     -H "Content-Type: application/json" \
     -d '{
       "table_name": "TABLE_NAME",
       "format": "csv",
       "row_limit": null,
       "compress": false,
       "columns": null
     }' | python -c "
   import sys,json; d=json.load(sys.stdin)
   print(f'Export job created: #{d[\"id\"]} — Status: {d[\"status\"]}')
   "
   ```

5. **Wait for completion** (poll every 2 seconds, up to 60 seconds):
   ```bash
   JOB_ID=<id from step 4>
   for i in $(seq 1 30); do
     sleep 2
     STATUS=$(curl -s "http://localhost:8001/api/v1/export/jobs/$JOB_ID" | python -c "
       import sys,json; d=json.load(sys.stdin); print(d['status'])")
     if [ "$STATUS" = "completed" ]; then
       echo "Export completed!"
       break
     elif [ "$STATUS" = "failed" ]; then
       echo "Export failed!"
       curl -s "http://localhost:8001/api/v1/export/jobs/$JOB_ID" | python -m json.tool
       break
     fi
   done
   ```

6. **Show result and download URL:**
   ```bash
   curl -s "http://localhost:8001/api/v1/export/jobs/$JOB_ID" | python -c "
   import sys,json; d=json.load(sys.stdin)
   print(f'File: {d.get(\"file_name\",\"?\")}')
   print(f'Size: {d.get(\"file_size_bytes\",0):,} bytes')
   print(f'Rows: {d.get(\"row_count\",\"?\"):,}')
   print(f'Download: http://localhost:8001/api/v1/export/jobs/{d[\"id\"]}/download')
   "
   ```

7. **Optionally download** to the local machine:
   ```bash
   curl -s -o "./exports/FILE_NAME" "http://localhost:8001/api/v1/export/jobs/$JOB_ID/download"
   echo "Saved to ./exports/FILE_NAME"
   ```

## If no table specified

Show top 20 tables with data:
```bash
curl -s http://localhost:8001/api/v1/export/tables | python -c "
import sys,json
tables = json.load(sys.stdin)
with_data = sorted([t for t in tables if t['row_count'] > 0], key=lambda x: -x['row_count'])
print(f'Tables with data ({len(with_data)} of {len(tables)}):')
print(f'{\"Table\":42s} {\"Records\":>10s} {\"Columns\":>8s}')
print('-' * 62)
for t in with_data[:20]:
    print(f'{t[\"table_name\"]:42s} {t[\"row_count\"]:>10,} {len(t[\"columns\"]):>8d}')
print(f'... use: /export <table_name> [csv|json|parquet]')
"
```

## Supported formats

| Format | Extension | Notes |
|--------|-----------|-------|
| `csv` | .csv | Default. Headers included. Supports gzip compression. |
| `json` | .json | Array of objects. Supports gzip compression. |
| `parquet` | .parquet | Columnar format. Built-in compression. Best for large datasets. |

## Important
- Exports run as background jobs and write to `/app/exports/` inside the container
- Files expire after a configurable period — download promptly
- For very large tables (>1M rows), consider using `--limit` or Parquet format
- Use `POST /export/cleanup` to clean up old export files
- The download URL is: `http://localhost:8001/api/v1/export/jobs/{id}/download`
