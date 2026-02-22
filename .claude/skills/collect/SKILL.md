---
name: collect
description: Trigger PE, People, Family Office, LP, or Site Intel collection pipelines. These are multi-step agentic pipelines that scrape, extract, and persist data. Use when the user wants to run a collection.
allowed-tools:
  - Bash
argument-hint: "<pipeline> [target]"
---

Trigger one of the agentic collection pipelines and monitor progress.

## Behavior

1. **Parse `$ARGUMENTS`** to identify the pipeline and target.

2. **Route to the correct pipeline:**

### People Collection
```
/collect people <company_id>           → Deep collect (all phases)
/collect people test <company_id>      → Test with diagnostics
/collect people batch                  → Batch collection
/collect people recursive <company_id> → Recursive corporate structure
```

| Target | Endpoint | Method | Body |
|--------|----------|--------|------|
| Deep collect | `/people-jobs/deep-collect/{company_id}` | POST | `{"run_sec": true, "run_website": true, "run_news": true, "build_org_chart": true}` |
| Test | `/people-jobs/test/{company_id}?sources=website` | POST | — |
| Batch | `/collection-jobs/batch` | POST | `{"sources": ["website"], "max_companies": 10}` |
| Recursive | `/people-jobs/recursive-collect/{company_id}` | POST | `{"discover_structure": true}` |
| Process queue | `/people-jobs/process` | POST | `{"max_jobs": 5}` |

### PE Collection
```
/collect pe                     → Collect all PE data
/collect pe firms               → Collect PE firm data
/collect pe <firm_id>           → Collect specific firm
```

| Target | Endpoint | Method | Body |
|--------|----------|--------|------|
| All | `/pe/collection/collect` | POST | `{"entity_type": "firm", "sources": ["sec_13f", "news_api", "press_release"]}` |
| Specific firm | `/pe/collection/collect` | POST | `{"entity_type": "firm", "firm_id": ID, "sources": ["sec_13f", "news_api"]}` |
| Sources list | `/pe/collection/sources` | GET | — |

### Family Office Collection
```
/collect fo                     → Seed + collect all FOs
/collect fo <fo_name>           → Collect specific FO
/collect fo contacts            → Extract contacts from websites
```

| Target | Endpoint | Method | Body |
|--------|----------|--------|------|
| Seed | `/fo-collection/seed-fos` | POST | — |
| Job | `/fo-collection/jobs` | POST | `{"sources": ["sec_form_adv", "website"], "max_concurrent_fos": 5}` |
| Single FO | `/fo-collection/collect/{fo_name}` | POST | `{"sources": ["website", "sec_form_adv"]}` |
| Contacts | `/family-offices/contacts/extract-from-websites` | POST | `{"max_concurrency": 3}` |
| Coverage | `/fo-collection/coverage` | GET | — |

### LP Collection
```
/collect lp                     → Collect all LP data
/collect lp <lp_id>             → Collect specific LP
/collect lp stale               → Refresh stale data
```

| Target | Endpoint | Method | Body |
|--------|----------|--------|------|
| Job | `/lp-collection/jobs` | POST | `{"sources": ["sec_13f", "annual_report"], "mode": "incremental"}` |
| Single LP | `/lp-collection/collect/{lp_id}` | POST | `{"sources": ["sec_13f"]}` |
| Stale | `/lp-collection/collect/stale` | POST | `{"max_age_days": 30, "limit": 20}` |
| Status | `/lp-collection/status` | GET | — |

### Site Intelligence
```
/collect site-intel              → Full collection (all domains)
/collect site-intel power        → Power domain only
/collect site-intel logistics    → Logistics domain only
```

| Target | Endpoint | Method | Body |
|--------|----------|--------|------|
| All domains | `/site-intel/sites/collect` | POST | `{}` |
| With deps | `/site-intel/sites/collect-with-deps` | POST | `{"domains": ["power"]}` |
| Status | `/site-intel/sites/collect/status` | GET | — |
| Watermarks | `/site-intel/sites/watermarks` | GET | — |

3. **Execute the collection:**
   ```bash
   curl -s -X POST "http://localhost:8001/api/v1/ENDPOINT" \
     -H "Content-Type: application/json" \
     -d 'BODY' | python -m json.tool
   ```

4. **Monitor progress** — tail the logs for collection activity:
   ```bash
   docker logs nexdata-api-1 --tail 5 -f 2>&1 | head -50
   ```
   Look for progress indicators: "Collected N items", "Extracted N people", etc.

5. **Report result** with record counts from the response or from `/db-status`.

## If no pipeline specified

Show available pipelines:
```
Available collection pipelines:
  people     - Executive/leadership data (website scraping + SEC + news)
  pe         - PE/VC firm intelligence (13F, news, press releases)
  fo         - Family office data (Form ADV, websites)
  lp         - LP/institutional investor data (13F, annual reports)
  site-intel - Site intelligence (power, telecom, transport, labor, risk, incentives, logistics, water)
```

## Important
- People deep-collect takes ~3-5 min per company (SEC + website + news + org chart)
- PE collection uses LLM extraction — watch for cost accumulation
- Site intel collection can take 30+ min for all domains
- Always check for running jobs first: `curl -s http://localhost:8001/api/v1/jobs?status=running`
