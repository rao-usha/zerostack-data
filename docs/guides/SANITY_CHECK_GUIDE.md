# Sanity Check Guide — PLAN_046 / 047 / 048 / 049

**For:** Verifying the economic intelligence, DQ governance, PE data products, and Sources redesign features after deployment.

**Date:** 2026-03-30

---

## Pre-flight

```bash
# Restart API (required after new router registration)
docker-compose restart api

# Wait ~25s then confirm startup
docker-compose logs api --tail 20

# Health check — should return 200
curl -s http://localhost:8001/health | python -m json.tool
```

Expected health response includes `"status": "healthy"` and lists registered routes count.

---

## Step 1 — Confirm New Routers Are Registered

```bash
# All three new route prefixes should appear
curl -s http://localhost:8001/openapi.json | python -m json.tool | grep '"tags"' | sort -u

# Or check Swagger UI: http://localhost:8001/docs
# Look for these tag groups:
#   Economic Snapshot
#   Economic Data Quality
#   PE Macro Intelligence
```

---

## Step 2 — PLAN_046: Economic Snapshot (Before Data Bootstrap)

These endpoints must return graceful "not_ingested" responses when tables are empty — not 500 errors.

```bash
BASE=http://localhost:8001/api/v1

# Check ingest status — shows which tables are populated
curl -s $BASE/econ-snapshot/ingest-status | python -m json.tool

# Expected when tables empty:
# {
#   "status": "not_ingested",
#   "tables": { "fred_interest_rates": false, "bls_jolts": false, ... },
#   "ingest_hints": [ "POST /api/v1/fred/ingest {\"category\": \"interest_rates\"}", ... ]
# }

# Macro endpoint — graceful fallback when FRED not ingested
curl -s $BASE/econ-snapshot/macro | python -m json.tool
# Expected: {"status": "not_ingested", "missing": [...], "data": null}

# Labor endpoint — graceful fallback
curl -s $BASE/econ-snapshot/labor | python -m json.tool

# Regional endpoint — graceful fallback
curl -s $BASE/econ-snapshot/regional | python -m json.tool

# PE signals — graceful fallback
curl -s $BASE/econ-snapshot/pe-signals | python -m json.tool
```

**Pass criteria:** All return `{"status": "not_ingested", ...}` with ingest hints. Zero 500 errors.

---

## Step 3 — Data Bootstrap

Run these in order. Each takes 30s–5min depending on API response time.

```bash
BASE=http://localhost:8001/api/v1

# --- FRED (most critical) ---
curl -s -X POST "$BASE/fred/ingest" \
  -H "Content-Type: application/json" \
  -d '{"category": "interest_rates"}' | python -m json.tool
# Returns job_id. Monitor: GET /api/v1/jobs/{job_id}

curl -s -X POST "$BASE/fred/ingest" \
  -H "Content-Type: application/json" \
  -d '{"category": "economic_indicators"}' | python -m json.tool

curl -s -X POST "$BASE/fred/ingest" \
  -H "Content-Type: application/json" \
  -d '{"category": "housing_market"}' | python -m json.tool

curl -s -X POST "$BASE/fred/ingest" \
  -H "Content-Type: application/json" \
  -d '{"category": "consumer_sentiment"}' | python -m json.tool

# --- BLS ---
curl -s -X POST "$BASE/bls/ingest/jolts" | python -m json.tool
curl -s -X POST "$BASE/bls/ingest/ces"   | python -m json.tool
curl -s -X POST "$BASE/bls/ingest/cpi"   | python -m json.tool

# --- BEA Regional (requires BEA_API_KEY in .env) ---
curl -s -X POST "$BASE/bea/regional/ingest" \
  -H "Content-Type: application/json" \
  -d '{"table_name": "SAGDP2N", "geo_fips": "STATE", "year": "2019,2020,2021,2022,2023"}' | python -m json.tool

curl -s -X POST "$BASE/bea/regional/ingest" \
  -H "Content-Type: application/json" \
  -d '{"table_name": "SAINC1",  "geo_fips": "STATE", "year": "2019,2020,2021,2022,2023"}' | python -m json.tool

curl -s -X POST "$BASE/bea/regional/ingest" \
  -H "Content-Type: application/json" \
  -d '{"table_name": "SAINC51", "geo_fips": "STATE", "year": "2019,2020,2021,2022,2023"}' | python -m json.tool

# --- Census ACS5 state demographics ---
curl -s -X POST "$BASE/census/state" \
  -H "Content-Type: application/json" \
  -d '{"survey": "acs5", "year": 2023, "table_id": "B01003"}' | python -m json.tool

curl -s -X POST "$BASE/census/state" \
  -H "Content-Type: application/json" \
  -d '{"survey": "acs5", "year": 2023, "table_id": "B19013"}' | python -m json.tool
```

**Monitor jobs:**
```bash
# Check job status (replace JOB_ID)
curl -s $BASE/jobs/JOB_ID | python -m json.tool

# Or watch all recent jobs
curl -s "$BASE/jobs?limit=10" | python -m json.tool | grep -E '"status"|"source"'
```

---

## Step 4 — PLAN_046: Economic Snapshot (After Data Bootstrap)

```bash
BASE=http://localhost:8001/api/v1

# Ingest status — should now show tables as populated
curl -s $BASE/econ-snapshot/ingest-status | python -m json.tool
# Expected: {"status": "ok", "tables": {"fred_interest_rates": true, ...}}

# Macro KPIs
curl -s $BASE/econ-snapshot/macro | python -m json.tool
# Expected:
# {
#   "status": "ok",
#   "kpis": {
#     "fed_funds_rate": {"value": X.XX, "prev_12m": X.XX, "delta": X.XX, "signal": "..."},
#     "ten_year_yield": {...},
#     "yield_spread_10_2": {"value": X.XX, "inverted": true/false, "inverted_months": N},
#     "unemployment_rate": {...},
#     "cpi_yoy_pct": {...}
#   },
#   "history": {"DFF": [...36 entries...], "DGS10": [...], ...}
# }

# Labor market
curl -s $BASE/econ-snapshot/labor | python -m json.tool
# Expected: jolts + sector_employment_12m array with 10+ sectors
# Each sector: {sector, series_id, current, prev_12m, delta, delta_pct}

# Regional (requires BEA + Census bootstrap)
curl -s $BASE/econ-snapshot/regional | python -m json.tool
# Expected: states array with 30-51 entries
# Each: {fips, name, gdp_growth_yoy_pct, personal_income_growth_pct, signal}

# PE signals (synthesized from above)
curl -s $BASE/econ-snapshot/pe-signals | python -m json.tool
# Expected:
# {
#   "deal_environment": {"score": 0-100, "signal": "green/yellow/red", "drivers": [...]},
#   "sector_momentum": [{sector, momentum_score, signal}, ...],
#   "geographic_opportunity": [{state, opportunity_score, signal}, ...]
# }
```

**Pass criteria:**
- `status: "ok"` on all 5 endpoints
- Macro: at least DFF and UNRATE populated
- Labor: at least JOLTS openings present
- Regional: if BEA not yet ingested, `status: "partial"` is acceptable
- PE signals: deal_environment.score between 0-100

---

## Step 5 — PLAN_047: BLS LAUS State Unemployment

```bash
BASE=http://localhost:8001/api/v1

# Ingest LAUS (51 state unemployment rate series)
curl -s -X POST "$BASE/bls/ingest/laus" | python -m json.tool
# Returns job_id — takes ~2-5 min (51 series from BLS API)

# After job completes, check latest rates
curl -s "$BASE/bls/laus/latest" | python -m json.tool
# Expected: array of 51 states with:
# [{"state": "Alabama", "fips": "01", "unemployment_rate": X.X, "period": "2026-MM"}, ...]

# Spot check: find California
curl -s "$BASE/bls/laus/latest" | python -m json.tool | grep -A3 '"California"'
```

**Pass criteria:** 51 entries, `unemployment_rate` values in 2–12% range, period within last 60 days.

---

## Step 6 — PLAN_047: Economic DQ Governance

```bash
BASE=http://localhost:8001/api/v1

# DQ health across all economic sources
curl -s $BASE/econ-dq/health | python -m json.tool
# Expected: {"sources": {"fred_interest_rates": {"score": N, ...}, ...}}
# If no DQ runs yet: {"sources": {}, "message": "No DQ reports yet — trigger with POST /econ-dq/run/{table}"}

# Trigger DQ manually on a specific table
curl -s -X POST "$BASE/econ-dq/run/fred_interest_rates" | python -m json.tool
# Expected: {"status": "triggered", "table": "fred_interest_rates"}

# Re-check health after manual trigger
curl -s $BASE/econ-dq/health | python -m json.tool

# Freshness vs. release calendar
curl -s $BASE/econ-dq/freshness | python -m json.tool
# Expected: per-series freshness status with expected_lag_days vs actual_age_days
# {
#   "series": [
#     {"table": "fred_interest_rates", "series_type": "fred_daily",
#      "expected_lag_days": 1.0, "actual_age_days": 0.5, "status": "fresh"},
#     ...
#   ]
# }

# Cross-source coherence (FRED UNRATE vs BLS)
curl -s $BASE/econ-dq/coherence | python -m json.tool
# Expected: comparison table showing deviation per period
# Note: only runs when both fred_economic_indicators AND bls_ces_employment are ingested

# SLA compliance
curl -s $BASE/econ-dq/sla | python -m json.tool

# Revision log (empty until a second ingest detects changes)
curl -s $BASE/econ-dq/revisions | python -m json.tool
# Expected: {"revisions": [], "total": 0} on first run
```

**Pass criteria:**
- DQ health endpoint returns 200 (empty is OK before first run)
- Manual trigger returns 200
- Freshness shows at least 1 series with `status: "fresh"` after bootstrap
- Coherence check runs without error when both FRED + BLS are populated

---

## Step 7 — PLAN_048: PE Macro Data Products

```bash
BASE=http://localhost:8001/api/v1

# Deal environment scores (all 9 sectors)
curl -s $BASE/pe/macro/deal-scores | python -m json.tool
# Expected: array of 9 sectors, each with:
# {
#   "sector": "industrials",
#   "deal_score": 0-100,
#   "grade": "A/B/C/D",
#   "signal": "green/yellow/red",
#   "drivers": [{factor, score, reading}, ...],
#   "recommendation": "...",
#   "updated_at": "..."
# }

# Single sector detail
curl -s $BASE/pe/macro/deal-scores/industrials | python -m json.tool
curl -s $BASE/pe/macro/deal-scores/technology | python -m json.tool
curl -s $BASE/pe/macro/deal-scores/healthcare | python -m json.tool

# 404 on unknown sector
curl -s $BASE/pe/macro/deal-scores/invalid_sector
# Expected: HTTP 404

# LBO entry scorer
curl -s -X POST "$BASE/pe/macro/lbo-score" \
  -H "Content-Type: application/json" \
  -d '{
    "sector": "industrials",
    "entry_ev_ebitda": 8.5,
    "leverage_debt_ebitda": 4.0,
    "hold_years": 5
  }' | python -m json.tool
# Expected:
# {
#   "sector": "industrials",
#   "irr_estimate_pct": ~10-25,
#   "benchmark_irr_pct": ~18-22,
#   "entry_score": 0-100,
#   "grade": "A/B/C/D",
#   "verdict": "...",
#   "sensitivity": {"+100bps_rates": {...}, "exit_7x_vs_8x": {...}, "ebitda_miss_20pct": {...}}
# }

# LBO score with different sectors
curl -s -X POST "$BASE/pe/macro/lbo-score" \
  -H "Content-Type: application/json" \
  -d '{"sector": "technology", "entry_ev_ebitda": 12.0, "leverage_debt_ebitda": 3.5, "hold_years": 5}' \
  | python -m json.tool

# Weekly digest (stub — returns placeholder until first digest is generated)
curl -s $BASE/pe/macro/digest/latest | python -m json.tool

# Manually trigger digest generation
curl -s -X POST $BASE/pe/macro/digest/generate | python -m json.tool
```

**Pass criteria:**
- 9 sectors returned from `/deal-scores`
- All scores between 0–100, grades A/B/C/D
- LBO score: `irr_estimate_pct` is a plausible number (5–40%), sensitivity block present
- No 500 errors even when FRED data is sparse (scorer uses safe defaults)

---

## Step 8 — PLAN_049: Sources Health Summary

```bash
BASE=http://localhost:8001/api/v1

# Health summary — powers the Sources page Health View
curl -s $BASE/sources/health-summary | python -m json.tool
# Expected:
# {
#   "banner": {
#     "active_count": N,
#     "stale_count": N,
#     "never_run_count": N,
#     "failed_count": N,
#     "open_anomaly_count": N,
#     "overall_status": "healthy/warning/critical"
#   },
#   "domains": [
#     {
#       "key": "macro_economic",
#       "label": "Macro Economic",
#       "sources": [
#         {
#           "key": "fred",
#           "display_name": "FRED",
#           "status": "idle/running",
#           "is_stale": false,
#           "total_rows": N,
#           "row_trend": "stable/growing/shrinking",
#           "last_run_at": "...",
#           "age_hours": N,
#           "quality_score": N,
#           "quality_breakdown": {"completeness": N, "freshness": N, "validity": N, "consistency": N},
#           "open_anomalies": N,
#           "tables": [...]
#         }
#       ]
#     }
#   ],
#   "governance": {
#     "open_anomalies": [...],
#     "top_recommendations": [...]
#   }
# }

# Verify 5 domain lanes are present
curl -s $BASE/sources/health-summary | python -m json.tool | grep '"key"' | head -20
# Should see: macro_economic, pe_intelligence, people_orgs, site_intelligence, regulatory
```

**Pass criteria:**
- Returns 200 with `banner`, `domains`, `governance` structure
- All 5 domain lanes present
- `overall_status` is one of: `healthy`, `warning`, `critical`
- No 500 errors (endpoint is tolerant of missing DQ data)

---

## Step 9 — Frontend Verification

Open each page in a browser. Check DevTools Network tab for any red 4xx/5xx.

### Economic Intelligence Page

```
http://localhost:8001/frontend/economic-intelligence.html
```

1. **Ingest status bar** — green if all tables populated, amber/red callout with ingest commands if not
2. **Section 1 (Macro)**: 5 KPI tiles show values (not "–")
3. **Key Signals box**: 3 auto-generated bullets appear below KPI tiles
4. **Chart A (Rate Timeline)**: Line chart with DFF, DGS10, DGS2 across 36 months
5. **Chart B (Yield Spread)**: Area chart, red shading if spread is negative
6. **PE Insight callout**: Auto-text based on live KPIs (yield curve / rate environment)
7. **Section 2 (Labor)**: Horizontal bar chart with 10 sectors, JOLTS 3-line chart
8. **Section 3 (Regional)**: State table, sortable, color-coded signals
9. **Section 5 (PE Synthesis)**: Deal Environment gauge, sector momentum bars, geo opportunity

### Sources Tab (index.html)

```
http://localhost:8001
```

Navigate to Sources tab:

1. **Status Banner** — top strip shows counts (active / stale / never run / anomaly), colored border
2. **Mode toggle** — "Health" and "Explore" buttons in top-right; Health is selected by default
3. **Domain swim lanes** — 5 lanes visible without clicking; each source row shows:
   - Status dot (●/○)
   - Row count
   - Last run time (relative)
   - Quality score (or "—" if DQ hasn't run)
   - "Run" and "Schedule" buttons
4. **Click mode toggle to "Explore"** — existing accordion view appears, swim lanes hide
5. **Governance panel** — below swim lanes in Health mode:
   - SLA timeline (Gantt-style bars per source)
   - Quality scorecards (4-dim breakdown per source)
   - Open anomalies list
   - Recommendations list

---

## Step 10 — End-to-End PE Demo Flow

Simulates the "10-minute PE pitch":

```bash
BASE=http://localhost:8001/api/v1

# 1. Show macro environment is neutral-to-negative
curl -s $BASE/econ-snapshot/pe-signals | python -m json.tool | grep -A5 '"deal_environment"'

# 2. Pull deal scores — find the best-scoring sector
curl -s $BASE/pe/macro/deal-scores | python -m json.tool | grep -E '"sector"|"deal_score"|"grade"'

# 3. Score a specific deal (Industrials, typical mid-market entry)
curl -s -X POST "$BASE/pe/macro/lbo-score" \
  -H "Content-Type: application/json" \
  -d '{"sector": "industrials", "entry_ev_ebitda": 7.5, "leverage_debt_ebitda": 3.5, "hold_years": 5}' \
  | python -m json.tool

# 4. Check regional opportunities
curl -s $BASE/econ-snapshot/pe-signals | python -m json.tool | grep -A20 '"geographic_opportunity"'
```

Expected narrative: rates elevated → deal score 40-65 for most sectors → best entry in healthcare/industrial services → Sun Belt states showing growth signal.

---

## Common Failure Modes

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `ImportError` on startup | New router not imported in main.py | Add import + include_router to main.py |
| 404 on `/econ-snapshot/*` | Router not registered | Check main.py include_router calls |
| `{"status": "not_ingested"}` on all endpoints | Bootstrap not run | Run Step 3 data bootstrap |
| Empty sector_employment in `/labor` | BLS CES not ingested | `POST /bls/ingest/ces` |
| `"states": []` in `/regional` | BEA not ingested | Run BEA regional bootstrap commands |
| Deal score = 50 for all sectors | FRED tables empty → scorer using defaults | Bootstrap FRED interest_rates + economic_indicators |
| Quality scores show `null` in swim lanes | DQ not yet run post-ingest | `POST /econ-dq/run/fred_interest_rates` etc. |
| Sources page shows "Health" tab blank | `health-summary` 500 error | Check docker logs; usually a missing import |
| LBO score returns 500 | Missing FRED data for FFR | Bootstrap FRED interest_rates first |
| LAUS returns 0 states | BLS API key missing or rate-limited | Check `BLS_API_KEY` in .env; retry after 60s |
| Frontend charts blank | API returning `status: "partial"` | Not a bug — need to bootstrap remaining sources |
| `econ_data_revisions` table missing | Model not picked up by create_all | `docker-compose restart api` — tables auto-create on startup |

---

## Router Registration Checklist

After deployment, verify these 3 routers are in `app/main.py`:

```python
# Economic Intelligence (PLAN_046)
from app.api.v1 import econ_snapshot
app.include_router(econ_snapshot.router, prefix="/api/v1", dependencies=_auth)

# Economic DQ Governance (PLAN_047)
from app.api.v1 import econ_dq
app.include_router(econ_dq.router, prefix="/api/v1", dependencies=_auth)

# PE Macro Data Products (PLAN_048)
from app.api.v1 import pe_macro
app.include_router(pe_macro.router, prefix="/api/v1", dependencies=_auth)
```

Confirm with: `curl -s http://localhost:8001/openapi.json | grep -c "econ-snapshot"` → should return `> 0`.
