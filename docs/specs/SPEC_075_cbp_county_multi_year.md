# SPEC 075 — County CBP multi-year (renumber from PLAN_067 SPEC_073)

**Status:** Draft
**Task type:** collector (Census CBP API) + atlas layer + cascade
**Date:** 2026-05-25
**Plan:** PLAN_067 §3 (was titled "SPEC_073 multi-year CBP" — renumbered to 075 because SPEC_073 was burned on the SPEC_073 smoke harness)
**Test file:** `tests/test_spec_075_cbp_county.py`
**Builds on:** SPEC_070 county_acs.py infrastructure (similar Census-API single-call ingest pattern); SPEC_066c cascade endpoint pattern (per-year choropleth scrubber).

## Goal

Bring county-grain CBP (Census County Business Patterns) onto the
Atlas with multiple years, so:
- The existing state-grain `econ_cbp_establishments_state` layer gets
  a real county-grain sibling (`econ_cbp_establishments_county`).
- The PLAN_070 §3 time-cascade scrubber pattern extends from FEMA to
  a second domain (industry density over time) — proving the
  pattern's generality.

PLAN_067 §3 asked for "multi-year CBP, full county coverage." Pre-flight
findings show what's missing:

| Table | Year | Grain | Rows |
|---|---|---|---|
| `census_cbp` | 2021 only | mixed (county rows present but sparse) | 7,160 |
| `census_business_patterns` | 2022 only | state-grain only | 63,369 |

So actual county-grain × multi-year is **not** on the cloud. This spec
ingests it.

## Scope decision

**MVP (this spec):**
- Ingest **5 years (2018-2022)** of CBP at **county summary level**
  (NAICS=00, i.e. "total for all sectors") into a new
  grain-explicit table `census_cbp_county_yearly`. ~15K rows total.
- New layer `econ_cbp_establishments_county` reads latest year (2022).
- New cascade endpoint `/atlas/layer/econ_cbp_establishments_county/cascade`
  returning per-year per-county counts — enables the existing FEMA
  scrubber UI pattern to extend to this layer.

**Stretch (deferred):**
- Per-NAICS-2 slicing (~20 sectors × 5 years × 3000 counties = 300K
  rows). Schema supports it (naics_code column), so this is a future
  ingest-more-rows operation; no schema change.

**Out of scope:**
- The frontend scrubber generalization (FEMA scrubber is currently
  hardcoded). Generalizing it is a small SPEC_066e follow-on; the
  cascade endpoint exists either way.
- The legacy `census_cbp` + `census_business_patterns` tables stay
  untouched — same grain-explicit-namespace discipline as SPEC_070's
  `acs5_county_*` table.

## Acceptance Criteria

- [ ] New table `census_cbp_county_yearly` exists with schema:
      `(year int, geo_id text, naics_code text,
       establishments int, employees int, annual_payroll_thousands bigint,
       ingested_at timestamp, PRIMARY KEY (year, geo_id, naics_code))`.
- [ ] ≥3,000 counties × ≥5 years × naics='00' = ≥15,000 rows ingested.
- [ ] `geo_id` is 5-digit county FIPS for every row.
- [ ] New ingester `app/sources/census/county_cbp.py::ingest_county_cbp(db, years)`.
- [ ] One-shot runner `scripts/ingest_cbp_county.py --years 2018,2019,2020,2021,2022`.
- [ ] New layer `econ_cbp_establishments_county` in
      `app/services/atlas/layers.py` — county grain, vintage "2018-2022".
- [ ] New endpoint
      `GET /atlas/layer/econ_cbp_establishments_county/cascade`
      returning `{years, values_by_year}` (same shape as the FEMA
      cascade so the frontend scrubber can be generalized later).
- [ ] `GET /atlas/layer/econ_cbp_establishments_county` returns ≥3,000
      values for the latest year.
- [ ] Smoke harness adds an L-category scenario for the new layer.
- [ ] No regression — all existing tests + 38 smoke scenarios still pass.

## Test Cases

| ID | What | Where |
|---|---|---|
| T1 | `census_cbp_county_yearly` has ≥15,000 rows post-ingest | pytest |
| T2 | every geo_id is 5-digit county FIPS | pytest |
| T3 | years 2018-2022 each have ≥2,500 county rows | pytest |
| T4 | `/atlas/layer/econ_cbp_establishments_county` returns ≥3,000 values | pytest |
| T5 | `/atlas/layer/.../cascade` returns 5 years × per-year county dicts | pytest |
| T6 | establishment counts are realistic (sum across counties = millions) | pytest |
| T7 | smoke harness scenario for the new layer | smoke_atlas.py |

## Design Notes

### Census CBP API call
```
GET https://api.census.gov/data/{year}/cbp
    ?get=NAME,ESTAB,EMP,PAYANN
    &for=county:*
    &NAICS2017=00
    &key=KEY
```
Returns one row per US county (~3,143). One API call per year.

### Table schema
```sql
CREATE TABLE IF NOT EXISTS census_cbp_county_yearly (
    year       INTEGER NOT NULL,
    geo_id     TEXT NOT NULL,                       -- 5-digit county FIPS
    naics_code TEXT NOT NULL DEFAULT '00',          -- '00' = total
    establishments         INTEGER,
    employees              INTEGER,
    annual_payroll_thousands BIGINT,
    ingested_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (year, geo_id, naics_code)
);
CREATE INDEX IF NOT EXISTS idx_census_cbp_county_yearly_year
    ON census_cbp_county_yearly(year);
```

### Layer builder
```python
def _build_cbp_county(db) -> LayerResult:
    rows = _safe_query(db, """
        WITH latest AS (SELECT MAX(year) AS y FROM census_cbp_county_yearly)
        SELECT geo_id, establishments AS value
        FROM census_cbp_county_yearly, latest
        WHERE year = latest.y AND naics_code = '00' AND establishments > 0
    """)
    ...
```

### Cascade endpoint
```python
@router.get("/layer/econ_cbp_establishments_county/cascade")
def cbp_cascade(db = Depends(get_db)):
    return series_mod.fetch_cbp_cascade(db)
```
Same shape as `fetch_fema_cascade` — `{years, values_by_year, meta}`.

### Why a new dedicated table
Same reasoning as SPEC_070 (`acs5_county_*`) and SPEC_071
(`usaspending_county_*`): the existing `census_cbp` table has mixed
NAICS levels at one year only, and is read by other product paths
(e.g. NAICS sector pages); modifying its grain or schema breaks
that contract. New table = additive, zero risk.

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_075_cbp_county_multi_year.md` | Create | This file |
| `docs/specs/.active_spec` | Modify | → `SPEC_075_cbp_county_multi_year` |
| `tests/test_spec_075_cbp_county.py` | Create | T1-T6 |
| `app/sources/census/county_cbp.py` | Create | Focused per-year ingest path |
| `scripts/ingest_cbp_county.py` | Create | One-shot runner |
| `app/services/atlas/layers.py` | Modify | Add `econ_cbp_establishments_county` builder + layer entry |
| `app/services/atlas/series.py` | Modify | Add `fetch_cbp_cascade` |
| `app/api/v1/atlas.py` | Modify | Add `/atlas/layer/.../cascade` endpoint |
| `scripts/smoke_atlas.py` | Modify | Add scenario |

## Verification (manual)

1. `python scripts/ingest_cbp_county.py` → ≥15,000 rows, 5 years × ~3,000 counties.
2. `docker-compose restart api`.
3. `curl /atlas/layer/econ_cbp_establishments_county | jq '.values | length'` ≥3,000.
4. `curl /atlas/layer/econ_cbp_establishments_county/cascade | jq '.years'` returns `[2018, 2019, 2020, 2021, 2022]`.
5. `python scripts/smoke_atlas.py` all pass.

## Feedback History

_No corrections yet._
