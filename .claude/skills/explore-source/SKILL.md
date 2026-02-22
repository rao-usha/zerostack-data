---
name: explore-source
description: Deep-dive into any data source — show its tables, record counts, sample data, available endpoints, and API key status. Use when the user wants to understand what data they have for a source.
allowed-tools:
  - Bash
argument-hint: "<source-name>"
---

Explore a data source end-to-end: tables, records, schema, sample data, endpoints, and key status.

## Behavior

1. **Identify the source** from `$ARGUMENTS`. Match against known source prefixes:
   - Macro: `fred`, `bea`, `bls`, `treasury`, `eia`, `data_commons`, `prediction_market`, `international`
   - Trade: `us_trade`, `cftc_cot`, `irs_soi`, `census`
   - Financial: `sec`, `fdic`, `form_adv`, `form_d`, `fcc`
   - Energy/Ag: `eia`, `usda`, `noaa`, `fema`, `fbi_crime`
   - Real Estate: `realestate`, `cms`
   - Alt Data: `yelp`, `kaggle`, `m5`, `foot_traffic`, `bts`, `uspto`
   - PE: `pe_firms`, `pe_deals`, `pe_people`, `pe_companies`, `pe_funds`
   - People: `people`, `company_people`, `industrial_companies`, `org_chart`
   - Family Office: `family_offices`, `fo`
   - Site Intel: `site_intel`, `power`, `telecom`, `transport`, `labor`, `risk`, `incentives`, `logistics`, `water`

2. **Find matching tables** in the database:
   ```bash
   curl -s http://localhost:8001/api/v1/export/tables | python -c "
   import sys,json
   PREFIX = 'SOURCE_PREFIX'
   tables = json.load(sys.stdin)
   matched = [t for t in tables if t['table_name'].startswith(PREFIX)]
   if not matched:
       matched = [t for t in tables if PREFIX in t['table_name']]
   total = sum(t['row_count'] for t in matched)
   print(f'Tables: {len(matched)} | Total records: {total:,}')
   print()
   for t in sorted(matched, key=lambda x: -x['row_count']):
       print(f\"  {t['table_name']:40s} {t['row_count']:>10,} rows  ({len(t['columns'])} cols)\")
   "
   ```

3. **Show schema** for the largest table:
   ```bash
   curl -s "http://localhost:8001/api/v1/export/tables/TABLE_NAME/preview?limit=1" | python -c "
   import sys,json; d=json.load(sys.stdin)
   print('Schema:')
   for col, typ in d.get('column_types',{}).items():
       print(f'  {col:30s} {typ}')
   "
   ```

4. **Show sample data** (first 5 rows of the largest table):
   ```bash
   curl -s "http://localhost:8001/api/v1/export/tables/TABLE_NAME/preview?limit=5" | python -c "
   import sys,json; d=json.load(sys.stdin)
   cols = d['columns'][:8]  # First 8 columns max
   print(' | '.join(f'{c:20s}' for c in cols))
   print('-' * (21 * len(cols)))
   for row in d['rows']:
       vals = [str(row.get(c,''))[:20] for c in cols]
       print(' | '.join(f'{v:20s}' for v in vals))
   "
   ```

5. **Check API key status:**
   ```bash
   curl -s http://localhost:8001/api/v1/settings/api-keys | python -c "
   import sys,json
   for k in json.load(sys.stdin):
       src = k.get('source','').lower()
       if 'SOURCE' in src:
           status = 'Configured' if k.get('is_set') else 'MISSING'
           print(f\"API Key ({k['source']}): {status}\")
   "
   ```

6. **Check last ingestion job:**
   ```bash
   curl -s "http://localhost:8001/api/v1/jobs?source=SOURCE&limit=3" | python -c "
   import sys,json
   jobs = json.load(sys.stdin)
   if not jobs: print('No ingestion history')
   for j in jobs:
       print(f\"  Job #{j['id']}: {j['status']} | {j.get('rows_collected','?')} rows | {j.get('created_at','?')}\")
   "
   ```

7. **List available endpoints** for this source by checking Swagger:
   ```bash
   curl -s http://localhost:8001/openapi.json | python -c "
   import sys,json
   spec = json.load(sys.stdin)
   paths = spec.get('paths',{})
   SOURCE = 'source_path_fragment'
   for path, methods in sorted(paths.items()):
       if SOURCE in path.lower():
           for method in methods:
               if method in ('get','post','put','delete','patch'):
                   summary = methods[method].get('summary','')
                   print(f'  {method.upper():6s} {path:50s} {summary}')
   "
   ```

8. **Present a clean report:**
   ```
   === FRED (Federal Reserve Economic Data) ===

   API Key: Configured (FRED_API_KEY)
   Tables: 2 | Total Records: 20,431

     fred_series                              847 rows  (12 cols)
     fred_series_observations              19,584 rows  (8 cols)

   Schema (fred_series_observations):
     id                    INTEGER
     series_id             VARCHAR
     date                  DATE
     value                 FLOAT
     ...

   Sample Data:
     id   | series_id | date       | value
     -----|-----------|------------|--------
     1    | GDP       | 2024-01-01 | 28269.5
     ...

   Last Ingestion:
     Job #42: success | 19,584 rows | 2026-02-14 18:30

   Endpoints:
     POST   /fred/ingest                  Ingest FRED category
     POST   /fred/ingest/batch            Batch ingest
     GET    /fred/categories              List categories
     GET    /fred/series/{category}       Series for category
   ```

## If no source specified

Show a summary of all sources with data:
```bash
curl -s http://localhost:8001/api/v1/export/tables | python -c "
import sys,json
from collections import defaultdict
tables = json.load(sys.stdin)
prefixes = defaultdict(lambda: {'count':0, 'rows':0})
for t in tables:
    prefix = t['table_name'].split('_')[0]
    prefixes[prefix]['count'] += 1
    prefixes[prefix]['rows'] += t['row_count']
print(f'{\"Source\":20s} {\"Tables\":>8s} {\"Records\":>12s}')
print('-' * 42)
for p, v in sorted(prefixes.items(), key=lambda x: -x[1]['rows']):
    if v['rows'] > 0:
        print(f'{p:20s} {v[\"count\"]:>8d} {v[\"rows\"]:>12,}')
"
```

## Important
- The `/export/tables` endpoint returns all 331 tables with row counts — use it as the source of truth
- The `/export/tables/{name}/preview` endpoint supports pagination (limit, offset, sort, order)
- Some sources share tables (e.g., people + company_people used by both People and PE)
- Site intel data is in domain-specific tables (power_plant, broadband_coverage, etc.) not prefixed with `site_intel_`
