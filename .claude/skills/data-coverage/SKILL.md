---
name: data-coverage
description: Show a comprehensive data coverage report — which sources have data, which are empty, freshness, record counts, and gaps. CLI version of the Sources dashboard.
allowed-tools:
  - Bash
argument-hint: "[category or 'full']"
---

Generate a data coverage report showing what data exists in the Nexdata database.

## Behavior

1. **Fetch all table data:**
   ```bash
   curl -s http://localhost:8001/api/v1/export/tables -o /tmp/nexdata_tables.json
   ```

2. **Generate the coverage report:**
   ```bash
   python -c "
   import json

   tables = json.load(open('/tmp/nexdata_tables.json'))
   total_tables = len(tables)
   with_data = [t for t in tables if t['row_count'] > 0]
   total_records = sum(t['row_count'] for t in tables)

   print('=' * 65)
   print('NEXDATA DATA COVERAGE REPORT')
   print('=' * 65)
   print(f'Total Tables:     {total_tables}')
   print(f'Tables with Data: {len(with_data)} ({len(with_data)*100//total_tables}%)')
   print(f'Empty Tables:     {total_tables - len(with_data)}')
   print(f'Total Records:    {total_records:,}')
   print()

   # Group by prefix
   from collections import defaultdict
   groups = defaultdict(lambda: {'tables': [], 'rows': 0})

   # Known category mappings
   CATEGORIES = {
       'Macro Economic': ['fred', 'bea', 'bls', 'treasury', 'data_commons', 'prediction_market'],
       'Trade & Commerce': ['us_trade', 'cftc_cot', 'irs_soi', 'census', 'acs5'],
       'Financial & Regulatory': ['sec', 'fdic', 'form_adv', 'form_d', 'fcc', 'corporate_registry'],
       'Energy & Agriculture': ['eia', 'usda', 'noaa', 'fema', 'fbi_crime'],
       'Real Estate & Health': ['realestate', 'cms', 'hud'],
       'Alt Data': ['yelp', 'kaggle', 'm5', 'foot_traffic', 'bts', 'uspto', 'web_traffic', 'app_ranking', 'glassdoor'],
       'PE Intelligence': ['pe_'],
       'People & Org Charts': ['people', 'company_people', 'industrial_companies', 'org_chart'],
       'Family Office & LP': ['family_office', 'lp_commitment'],
       'Site Intel': ['power_plant', 'substation', 'utility', 'broadband', 'peering', 'data_center',
                      'submarine_cable', 'intermodal', 'rail', 'port', 'airport', 'heavy_haul',
                      'labor_', 'wage', 'employment', 'commute', 'flood', 'nri_', 'seismic',
                      'climate', 'environmental', 'wetland', 'opportunity_zone', 'foreign_trade_zone',
                      'incentive', 'industrial_site', 'container_rate', 'truck_rate', 'carrier',
                      'warehouse', 'freight', 'three_pl', 'water_', 'gas_pipeline', 'gas_storage'],
       'Jobs & System': ['ingestion_job', 'dataset_registry', 'export_job', 'webhook', 'schedule',
                         'data_quality', 'lineage', 'chain', 'rate_limit', 'template', 'audit']
   }

   categorized = set()
   cat_stats = {}

   for cat_name, prefixes in CATEGORIES.items():
       cat_tables = []
       for t in tables:
           for prefix in prefixes:
               if t['table_name'].startswith(prefix) or prefix in t['table_name']:
                   if t['table_name'] not in categorized:
                       cat_tables.append(t)
                       categorized.add(t['table_name'])
                   break
       cat_rows = sum(t['row_count'] for t in cat_tables)
       cat_with_data = sum(1 for t in cat_tables if t['row_count'] > 0)
       cat_stats[cat_name] = {
           'tables': len(cat_tables),
           'with_data': cat_with_data,
           'rows': cat_rows,
           'table_list': cat_tables
       }

   # Summary by category
   print(f'{\"Category\":25s} {\"Tables\":>8s} {\"With Data\":>10s} {\"Records\":>12s}')
   print('-' * 58)
   for cat, stats in cat_stats.items():
       pct = f'{stats[\"with_data\"]}/{stats[\"tables\"]}' if stats['tables'] > 0 else '0/0'
       rows = f'{stats[\"rows\"]:,}' if stats['rows'] > 0 else '-'
       print(f'{cat:25s} {stats[\"tables\"]:>8d} {pct:>10s} {rows:>12s}')

   # Uncategorized
   uncat = [t for t in tables if t['table_name'] not in categorized]
   if uncat:
       uncat_rows = sum(t['row_count'] for t in uncat)
       uncat_data = sum(1 for t in uncat if t['row_count'] > 0)
       print(f'{\"Other/Uncategorized\":25s} {len(uncat):>8d} {uncat_data}/{len(uncat):>9s} {uncat_rows:>12,}')

   print()
   print('TOP 15 TABLES BY RECORD COUNT')
   print('-' * 58)
   for t in sorted(with_data, key=lambda x: -x['row_count'])[:15]:
       print(f'  {t[\"table_name\"]:40s} {t[\"row_count\"]:>10,} rows')

   print()
   print('EMPTY CATEGORIES (no data yet)')
   print('-' * 58)
   for cat, stats in cat_stats.items():
       if stats['rows'] == 0 and stats['tables'] > 0:
           print(f'  {cat}: {stats[\"tables\"]} tables defined but empty')
   "
   ```

3. **If `$ARGUMENTS` specifies a category**, show detailed table-level breakdown for that category.

4. **If `$ARGUMENTS` is "full"**, also show:
   - Job history summary per source
   - API key status for all sources
   - Schedule status

5. **Clean up:**
   ```bash
   rm -f /tmp/nexdata_tables.json
   ```

## Output format

```
=================================================================
NEXDATA DATA COVERAGE REPORT
=================================================================
Total Tables:     331
Tables with Data: 199 (60%)
Empty Tables:     132
Total Records:    9,247,831

Category                   Tables  With Data      Records
----------------------------------------------------------
Macro Economic                 22       15      1,245,678
Trade & Commerce               18       12        912,345
...

TOP 15 TABLES BY RECORD COUNT
----------------------------------------------------------
  m5_prices                                  6,841,121 rows
  us_trade_exports_state                       546,095 rows
  ...

EMPTY CATEGORIES (no data yet)
----------------------------------------------------------
  Alt Data: 12 tables defined but empty
```

## Important
- The `/export/tables` endpoint is the single source of truth for table existence and row counts
- This report reflects actual DB state, not what the health system reports
- Table prefixes don't always match source names (e.g., site intel tables have domain-specific prefixes)
