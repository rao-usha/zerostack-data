---
name: ingest
description: Trigger data ingestion for any of the 25+ data sources. Handles API key validation, correct endpoint routing, and job monitoring. Use when the user wants to pull data from a specific source.
allowed-tools:
  - Bash
argument-hint: "<source> [options]"
---

Trigger a data ingestion job for the specified source and monitor its progress.

## Behavior

1. **Parse `$ARGUMENTS`** to identify the source and any options (year, state, dataset, etc.).

2. **Check API key status** before attempting ingestion:
   ```bash
   curl -s http://localhost:8001/api/v1/settings/api-keys | python -c "
   import sys,json
   keys = json.load(sys.stdin)
   for k in keys:
       if k.get('source','').lower() == 'SOURCE_NAME'.lower():
           print(f\"Key status: {'configured' if k.get('is_set') else 'MISSING'}\")"
   ```
   If the source requires an API key and it's missing, warn the user and show how to set it:
   ```
   PUT /api/v1/settings/api-keys  {"source": "eia", "key": "your-key-here"}
   ```

3. **Route to the correct endpoint.** Source-to-endpoint mapping:

   | Source | Endpoint | Required Key | Common Options |
   |--------|----------|-------------|----------------|
   | `fred` | `POST /fred/ingest` | Optional | `category`, `series_ids` |
   | `bea` | `POST /bea/nipa/ingest` | BEA_API_KEY | `table_name`, `frequency`, `year` |
   | `bls` | `POST /bls/{dataset}/ingest` | Optional | `dataset` (ces/cps/jolts/cpi/ppi/oes), `start_year`, `end_year` |
   | `treasury` | `POST /treasury/all/ingest` | None | `start_date`, `end_date` |
   | `eia` | `POST /eia/petroleum/ingest` | EIA_API_KEY | `subcategory`, `frequency` |
   | `census` | `POST /census/state` | CENSUS_API_KEY | `survey`, `year`, `table_id` |
   | `sec` | `POST /sec/ingest/company` | None | `cik`, `filing_types` |
   | `fdic` | `POST /fdic/all/ingest` | None | `year`, `state` |
   | `us_trade` | `POST /us-trade/summary/ingest` | None | `year`, `month` |
   | `cftc_cot` | `POST /cftc-cot/ingest` | None | `year`, `report_type` |
   | `irs_soi` | `POST /irs-soi/all/ingest` | None | `year` |
   | `fema` | `POST /fema/disasters/ingest` | None | `state`, `year` |
   | `fbi_crime` | `POST /fbi-crime/ingest/all` | DATA_GOV_API | `datasets` |
   | `fcc` | `POST /fcc-broadband/all-states/ingest` | None | — |
   | `usda` | `POST /usda/all-major-crops/ingest` | USDA_API_KEY | `year` |
   | `noaa` | `POST /noaa/ingest` | NOAA_API_TOKEN | `dataset_key`, `location_id`, `start_date`, `end_date` |
   | `realestate` | `POST /realestate/fhfa/ingest` | None | `geography_type` |
   | `cms` | `POST /cms/ingest/medicare-utilization` | None | `year`, `state` |
   | `yelp` | `POST /yelp/businesses/ingest` | YELP_API_KEY | `location`, `term` |
   | `kaggle` | `POST /kaggle/m5/ingest` | KAGGLE creds | `force_download` |
   | `bts` | `POST /bts/border-crossing/ingest` | None | `start_date`, `end_date` |
   | `uspto` | `POST /uspto/ingest/assignee` | USPTO key | `assignee_name` |
   | `data_commons` | `POST /data-commons/us-states/ingest` | Optional | `variables[]` |
   | `prediction_markets` | `POST /prediction-markets/monitor/all` | None | `limit_per_platform` |
   | `international` | `POST /international/worldbank/wdi/ingest` | None | `indicators[]`, `countries[]` |

4. **Execute the ingestion:**
   ```bash
   curl -s -X POST "http://localhost:8001/api/v1/ENDPOINT" \
     -H "Content-Type: application/json" \
     -d '{"param": "value"}' | python -m json.tool
   ```

5. **Track the job** — extract `job_id` from response and poll:
   ```bash
   curl -s "http://localhost:8001/api/v1/jobs/JOB_ID" | python -c "
   import sys,json; d=json.load(sys.stdin)
   print(f\"Status: {d['status']} | Source: {d['source']} | Rows: {d.get('rows_collected','?')}\")"
   ```

6. **Report result:**
   - Job ID and status
   - Rows collected (if available)
   - Duration
   - Any errors

## If no source specified

Show available sources grouped by API key requirement:
- **No key needed:** treasury, sec, fdic, us_trade, cftc_cot, irs_soi, fema, fcc, realestate, cms, bts, prediction_markets, international
- **Key configured:** fred, bea, bls, eia, census, fbi_crime, usda, data_commons, kaggle
- **Key missing:** noaa, yelp, uspto

## Important
- All POST ingestion endpoints return a `job_id` — always show this to the user
- Jobs run in the background via BackgroundTasks
- Use `/jobs/{job_id}` to check status, NOT the source-specific status endpoints
- Some sources have an "all" endpoint (treasury/all, fdic/all, bls/all) — prefer these for first-time ingestion
