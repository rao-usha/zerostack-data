# PLAN_050 — Post-Launch Fixes (PLAN_046–049)

**Status:** Draft — awaiting approval
**Date:** 2026-03-31
**Scope:** 3 code bugs + 2 data bootstraps
**Follows:** PLAN_046–049 (economic intelligence, econ DQ, PE data products, Sources redesign)

---

## What We're Fixing

Four issues found during live sanity-check testing, one data gap, one bootstrap task:

| # | Issue | Root Cause | Severity |
|---|-------|-----------|----------|
| 1 | Sources health-summary `rows=0` on all cards | `pg_stat_user_tables.n_live_tup=0` — PostgreSQL stats not updated after bulk insert without ANALYZE | Medium — cosmetic but misleading |
| 2 | DOMAIN_MAP has 10 keys not in SOURCE_REGISTRY → empty lane cards; 35 sources in "Other" | Agent invented keys that don't match actual registry entries | Medium — health view doesn't show the right sources |
| 3 | Deal scores: 3/9 sectors missing BLS sector labor factor | CES3000000001 (Industrials), CES4200000001 (Consumer), CES1021000001 (Energy) not ingested | Low — scores still work, just 3-factor not 4-factor for these sectors |
| 4 | Bootstrap: Census B19013 + BLS LAUS not yet ingested | Operational gap | Low — regional section partially empty |

---

## Fix 1 — Health-Summary Row Counts

**File:** `app/api/v1/sources.py`

**Root cause:** `_table_stats()` queries `pg_stat_user_tables.n_live_tup` which is an autovacuum estimate. After bulk INSERT, `n_live_tup=0` until PostgreSQL runs ANALYZE. This is a known PostgreSQL behavior — bulk-loaded tables won't reflect accurate live tuples until the next autovacuum or explicit `ANALYZE`.

**Fix:** In `get_health_summary()`, the `_health_job_info()` call already returns `rows_inserted` from the last successful ingestion job. Use this as the authoritative row count when `total_rows=0`:

```python
# Current (broken — always 0):
total_rows = tb.get("total_rows", 0)

# Fixed — prefer job-reported rows, fall back to pg_stat:
total_rows = tb.get("total_rows") or rows_ins or 0
```

`rows_ins` is already fetched at line 517 of the current code — zero additional DB queries needed. This uses the `rows_inserted` value the ingestor writes when it completes, which is always accurate.

**Also fix:** `_table_stats()` currently skips all sources where `src.table_prefix` is falsy (line 178: `if not src.table_prefix: continue`). All 47 sources in SOURCE_REGISTRY have `table_prefix=None`. The pg_stat query still runs but returns an empty dict for every source. To get the per-source table list (used for hover tooltip), change the matching to use the source key as a prefix pattern:

```python
# Replace the per-source matching loop in _table_stats():
for r in rows:
    # Match tables by source key prefix (e.g., key="fred" matches fred_*)
    for src in SOURCE_REGISTRY.values():
        if r["relname"].startswith(src.key + "_") or r["relname"] == src.key:
            stats.setdefault(src.key, {"table_count": 0, "total_rows": 0, "tables": []})
            stats[src.key]["table_count"] += 1
            stats[src.key]["total_rows"] += int(r["n_live_tup"] or 0)
            stats[src.key]["tables"].append({"name": r["relname"], "estimated_rows": int(r["n_live_tup"] or 0)})
```

This gives table names even when n_live_tup=0, and the row count fallback in the card builder handles the rest.

---

## Fix 2 — DOMAIN_MAP Correction

**File:** `app/api/v1/sources.py`

**Root cause:** The agent that built `health-summary` invented domain lane keys (`census_batch`, `census_geo`, `site_intel_logistics`, `site_intel_labor`, `pe_collection`, `people_jobs`, `lp_collection`, `fo_collection`, `sec_form_d`, `afdc`) that don't exist in SOURCE_REGISTRY. Real keys: `census`, `form_d`, etc. Result: those 10 lane slots silently produce no cards, and 35 unmapped sources pile into an "Other" bucket.

**Fix:** Replace `DOMAIN_MAP` with the actual SOURCE_REGISTRY keys:

```python
DOMAIN_MAP: Dict[str, List[str]] = {
    "macro_economic": [
        "fred", "bls", "bea", "census", "international_econ",
    ],
    "pe_intelligence": [
        "sec", "form_d", "form_adv",
        "job_postings",       # hiring signals → deal sourcing
        "opencorporates",     # company registry
    ],
    "people_orgs": [
        "glassdoor",          # executive/employee intelligence
        "github",             # tech org intelligence
        "nppes",              # healthcare provider registry
        "app_rankings",       # consumer app performance
    ],
    "site_intelligence": [
        "eia", "noaa", "osha", "epa_echo", "fema",
        "location_diligence", "realestate", "foot_traffic",
    ],
    "regulatory": [
        "fdic", "fbi_crime", "treasury", "courtlistener",
        "sam_gov", "irs_soi", "usda", "fcc_broadband",
        "cms", "us_trade",
    ],
}
```

Sources not in any lane (kaggle, cftc_cot, dunl, medspa_discovery, etc.) remain in "Other" — but this is now ~10 edge cases rather than 35 mainstream sources.

---

## Fix 3 — Missing BLS Sector Series

**File:** `app/api/v1/bls.py` + `app/sources/bls/metadata.py`

**Root cause:** Three CES sector series not included in the current `bls/ingest/ces` ingest set:
- `CES3000000001` — Manufacturing (Industrials sector)
- `CES4200000001` — Retail Trade (Consumer sector)
- `CES1021000001` — Mining & Logging (Energy sector proxy)

These sectors currently fall back to no-data for the sector labor factor, reducing their score on 4 factors instead of 5.

**Fix:** Add these 3 series to the CES series list in `bls/metadata.py`, then re-trigger `POST /bls/ingest/ces` to pick them up. No code changes to `deal_environment_scorer.py` needed — it already has the correct series IDs; the data just wasn't being fetched.

```python
# In bls/metadata.py — add to CES_SERIES dict:
"CES3000000001": "Manufacturing employment (seasonally adjusted)",
"CES4200000001": "Retail Trade employment (seasonally adjusted)",
"CES1021000001": "Mining and Logging employment (seasonally adjusted)",
```

After re-ingest, Industrials, Consumer, and Energy sectors will have 5-factor scoring.

---

## Fix 4 — Data Bootstrap

Operational — no code changes. Run these after code fixes are deployed:

```bash
# BLS LAUS — state unemployment rates (51 series)
POST /api/v1/bls/ingest/laus

# Census B19013 — Median Household Income (for regional section)
POST /api/v1/census/state  {"survey": "acs5", "year": 2023, "table_id": "B19013"}

# BLS CES re-ingest (picks up 3 new sector series from Fix 3)
POST /api/v1/bls/ingest/ces
```

---

## What We Observed Was Correct (Not Bugs)

- **Deal scores all grade A (83–93)**: FFR dropped from 5.33% → 3.64%, yield curve non-inverted (+0.58pp), CPI benign at 2.7%. These are legitimately accommodative macro conditions. All-A scores are mathematically correct for the current environment. Differentiation exists (83 vs 93 = 10pp spread across sectors).

- **FRED UNRATE vs BLS coherence = 0 divergences**: Correct — both report the same BLS unemployment data.

- **LBO `inputs` nested under `"inputs"` key**: Intentional API design (inputs vs macro_inputs vs outputs). Not a bug; sanity check guide updated to reflect this.

- **EconDQ score=88 on `fred_interest_rates` with freshness=70**: Correct — data is 20 days stale vs the 1-day expected lag for daily FRED series. The staleness flag is working as designed.

- **`econ_data_revisions` table empty**: Expected on first run — revisions only appear when same-period values change between ingests.

---

## Files

| File | Action | Scope |
|------|--------|-------|
| `app/api/v1/sources.py` | MODIFY | Fix 1 (row counts) + Fix 2 (DOMAIN_MAP) |
| `app/sources/bls/metadata.py` | MODIFY | Fix 3 (add 3 CES sector series) |

No new files. No model changes. No main.py changes.

---

## Implementation Phases

| Phase | What | File |
|-------|------|------|
| 1 | Fix DOMAIN_MAP with actual SOURCE_REGISTRY keys | `sources.py` |
| 2 | Fix `_table_stats()` prefix matching + row count fallback | `sources.py` |
| 3 | Add 3 CES series to BLS metadata | `bls/metadata.py` |
| 4 | Restart + verify health-summary shows correct domains + rows | curl test |
| 5 | Run data bootstrap (LAUS + Census B19013 + CES re-ingest) | API calls |
| 6 | Verify deal scores now show 5-factor scoring for all 9 sectors | curl test |

---

## Acceptance Criteria

```bash
# Health-summary: rows should now reflect last ingestion counts
curl http://localhost:8001/api/v1/sources/health-summary | jq '.domains[] | {domain: .label, sources: [.sources[] | {key: .key, rows: .total_rows}]}'
# Expected: fred rows>0, bls rows>0

# Health-summary: no "Other" domain with 30+ sources
curl http://localhost:8001/api/v1/sources/health-summary | jq '.domains | length'
# Expected: ≤6 (5 named + maybe small "Other")

# Deal scores: industrials sector now shows sector_labor factor with data
curl http://localhost:8001/api/v1/pe/macro/deal-scores/industrials | jq '.factors[] | select(.factor=="Sector labor momentum")'
# Expected: reading shows actual CES employment delta, not "no data"

# LAUS: 51 states after bootstrap
curl http://localhost:8001/api/v1/bls/laus/latest | jq 'length'
# Expected: 51
```
