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

   # Domain classification aligned with app/core/domains.py
   # Order matters — first match wins (more specific domains checked first)
   DOMAINS = [
       ('PE Intelligence', [
           'pe_', 'deals', 'deal_', 'exit_readiness_',
           'acquisition_target_', 'diligence_', 'hunt_job',
       ]),
       ('People & Org Charts', [
           'people', 'company_people', 'org_chart_',
           'leadership_', 'industrial_companies',
       ]),
       ('Family Office & LP', [
           'family_office', 'lp_', 'investor_', 'co_invest', 'portfolio_',
       ]),
       ('Site Intelligence', [
           'airport', 'air_cargo_', 'broadband_', 'carrier_safety',
           'cell_tower', 'climate_', 'cold_storage', 'commute_',
           'container_freight', 'data_center_', 'educational_attainment',
           'environmental_', 'ev_charging', 'fault_line', 'fema_',
           'fbi_crime', 'flood_', 'freight_', 'grain_elevator',
           'heavy_haul', 'incentive_', 'industrial_site',
           'interconnection_', 'intermodal_', 'internet_exchange',
           'labor_market', 'lng_terminal', 'motor_carrier',
           'national_risk_index', 'natural_gas_', 'network_latency',
           'noaa_', 'occupational_wage', 'opportunity_zone',
           'pipeline_', 'port', 'power_plant', 'public_water_',
           'rail_', 'renewable_', 'seismic_', 'site_intel_',
           'site_score', 'solar_farm', 'submarine_cable', 'substation',
           'three_pl_', 'trucking_', 'utility_', 'warehouse_',
           'water_', 'wetland', 'wind_farm', 'zoning_',
       ]),
       ('Macro Economic', [
           'fred_', 'bea_', 'bls_', 'treasury_', 'intl_', 'data_commons_',
       ]),
       ('Trade & Commerce', [
           'us_trade_', 'cftc_cot_', 'irs_soi_', 'acs5_',
           'bts_', 'dunl_', 'trade_gateway_', 'census_variable_',
           'foreign_trade_zone',
       ]),
       ('Financial & Regulatory', [
           'sec_', 'fdic_', 'fcc_', 'form_adv', 'form_d',
       ]),
       ('Energy & Agriculture', [
           'usda_', 'electricity_', 'eia_',
       ]),
       ('Real Estate', [
           'realestate_', 'hud_',
       ]),
       ('Healthcare', [
           'cms_', 'fda_', 'medspa_', 'zip_medspa_',
       ]),
       ('Alternative Data', [
           'm5_', 'job_posting', 'app_store_', 'glassdoor_',
           'github_', 'prediction_market', 'foot_traffic_',
           'company_web_', 'company_app_', 'company_ats_',
           'company_health_', 'company_score', 'company_enrichment',
           'competitive_', 'news_', 'hiring_velocity', 'market_',
       ]),
       ('Platform', []),  # catch-all
   ]

   categorized = set()
   cat_stats = {}

   for cat_name, prefixes in DOMAINS:
       cat_tables = []
       for t in tables:
           name = t['table_name']
           if name in categorized:
               continue
           if not prefixes:
               continue  # Platform catch-all handled below
           for prefix in prefixes:
               if name.startswith(prefix):
                   cat_tables.append(t)
                   categorized.add(name)
                   break
       cat_rows = sum(t['row_count'] for t in cat_tables)
       cat_with_data = sum(1 for t in cat_tables if t['row_count'] > 0)
       cat_stats[cat_name] = {
           'tables': len(cat_tables),
           'with_data': cat_with_data,
           'rows': cat_rows,
           'table_list': cat_tables
       }

   # Platform catch-all
   uncat = [t for t in tables if t['table_name'] not in categorized]
   uncat_rows = sum(t['row_count'] for t in uncat)
   uncat_data = sum(1 for t in uncat if t['row_count'] > 0)
   cat_stats['Platform'] = {
       'tables': len(uncat),
       'with_data': uncat_data,
       'rows': uncat_rows,
       'table_list': uncat
   }

   # Summary by category
   print(f'{\"Category\":25s} {\"Tables\":>8s} {\"With Data\":>10s} {\"Records\":>12s}')
   print('-' * 58)
   for cat_name, _ in DOMAINS:
       stats = cat_stats.get(cat_name, {'tables': 0, 'with_data': 0, 'rows': 0})
       if stats['tables'] == 0:
           continue
       pct = f'{stats[\"with_data\"]}/{stats[\"tables\"]}' if stats['tables'] > 0 else '0/0'
       rows = f'{stats[\"rows\"]:,}' if stats['rows'] > 0 else '-'
       print(f'{cat_name:25s} {stats[\"tables\"]:>8d} {pct:>10s} {rows:>12s}')

   print()
   print('TOP 15 TABLES BY RECORD COUNT')
   print('-' * 58)
   for t in sorted(with_data, key=lambda x: -x['row_count'])[:15]:
       print(f'  {t[\"table_name\"]:40s} {t[\"row_count\"]:>10,} rows')

   print()
   print('EMPTY DOMAINS (no data yet)')
   print('-' * 58)
   for cat_name, _ in DOMAINS:
       stats = cat_stats.get(cat_name, {'tables': 0, 'rows': 0})
       if stats['rows'] == 0 and stats['tables'] > 0:
           print(f'  {cat_name}: {stats[\"tables\"]} tables defined but empty')
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
Total Tables:     351
Tables with Data: 199 (56%)
Empty Tables:     152
Total Records:    9,247,831

Category                   Tables  With Data      Records
----------------------------------------------------------
PE Intelligence                22       15      1,245,678
People & Org Charts            12        8        912,345
Family Office & LP             18        2          5,432
Site Intelligence              55       20      1,876,543
Macro Economic                 30       10        264,450
Trade & Commerce               15        5        250,413
...

TOP 15 TABLES BY RECORD COUNT
----------------------------------------------------------
  m5_prices                                  6,841,121 rows
  us_trade_exports_state                       546,095 rows
  ...

EMPTY DOMAINS (no data yet)
----------------------------------------------------------
  Real Estate: 4 tables defined but empty
```

## Domain Classification

Categories are aligned with `app/core/domains.py` — the canonical domain
classification used by all reports and dashboards. See the `domains` skill
for full documentation.

## Important
- The `/export/tables` endpoint is the single source of truth for table existence and row counts
- This report reflects actual DB state, not what the health system reports
- Table prefixes don't always match source names (e.g., site intel tables have domain-specific prefixes)
