# SPEC 070 — ACS county-grain median household income

**Status:** Draft
**Task type:** collector (new focused ingest path) + atlas layer wire-up
**Date:** 2026-05-23
**Plan:** PLAN_067 (Atlas Data Coverage Expansion — Phase 3a)
**Test file:** `tests/test_spec_070_acs_county_wealth.py`
**Builds on:** existing `app/sources/census/` ingestor; SPEC_065 Atlas layer registry; SPEC_064 EXCLUDED_BY_DESIGN list

## Goal

Add a real **county-grain ACS B19013 median household income** layer to
the Atlas, replacing the deferred ZCTA-keyed table that PLAN_066 §4
flagged as "not county-joinable". Hit the Census ACS API directly at
`for=county:*&in=state:*` — that returns ~3,143 rows in one round-trip
and gives us true county data without ZCTA→county crosswalk fuzziness.

Re-rates the PLAN_066 §5.1 demographics layer from 🟡 to ✅; removes the
`acs5_2023_b19013_as_county` line from `layers.EXCLUDED_BY_DESIGN`.

## Pre-flight findings (logged 2026-05-23)

- `acs5_2023_b19013`: 33,772 rows, **all ZCTA-grain** (`ZCTA5 NNNNN` names,
  state_fips empty, no county info). Currently unused — Atlas explicitly
  excludes per `EXCLUDED_BY_DESIGN["acs5_2023_b19013_as_county"]`.
- `acs5_2022_b19013`: empty (0 rows).
- No county-grain ACS table exists in the cloud schema.
- Census ingestor (`app/sources/census/ingest.py`) already supports
  `geo_level="county"` via `ingest_acs_table(...)` — but
  `generate_table_name()` does **not** include geo_level, so calling it
  with B19013 + county would write into the same `acs5_2023_b19013`
  table and either collide or mix grains. **Decision:** add a small
  focused ingest path that writes to a separate, grain-explicit table
  (`acs5_county_2023_b19013`) — zero risk of disturbing the existing
  state/ZCTA ingest contract.

## Acceptance Criteria

- [ ] New table `acs5_county_2023_b19013` exists with schema
      `(geo_id text PRIMARY KEY, state_fips text, geo_name text,
       b19013_001e integer, ingested_at timestamp)`.
- [ ] ≥3,000 rows landed (3,143 total US counties; cloud table must have
      ≥95% coverage).
- [ ] `geo_id` is the 5-digit county FIPS (state + county concatenated)
      — no ZCTAs, no 2-digit state codes.
- [ ] A new ingest entry point `app/sources/census/county_acs.py::ingest_county_acs_b19013(db)`
      hits Census API once, parses, batch-inserts.
- [ ] One-shot run script `scripts/ingest_acs_county_b19013.py` that
      calls the ingester against the configured DB.
- [ ] New Atlas layer `demo_acs_median_income` registered in
      `app/services/atlas/layers.py` — county grain, vintage 2023,
      reading from the new table.
- [ ] `EXCLUDED_BY_DESIGN["acs5_2023_b19013_as_county"]` entry removed.
- [ ] `GET /api/v1/atlas/layer/demo_acs_median_income` returns ≥3,000
      county values with a realistic legend (min ~$15k, max ~$200k).
- [ ] `GET /api/v1/atlas/place/48201` (Harris Co TX) now includes the
      ACS median income value (~$70-80k).

## Test Cases

| ID | Test | What it verifies |
|---|---|---|
| T1 | `test_ingest_produces_county_rows` | After running, table has ≥3,000 rows |
| T2 | `test_all_geo_ids_are_5_digit_county_fips` | Every row's geo_id matches `^[0-1][0-9]\|[2-5][0-9]\d{3}$` |
| T3 | `test_no_collision_with_zcta_table` | The ZCTA `acs5_2023_b19013` row count is unchanged |
| T4 | `test_layer_appears_in_registry` | `/atlas/layers` includes `demo_acs_median_income` |
| T5 | `test_layer_endpoint_returns_county_values` | `/atlas/layer/demo_acs_median_income` returns ≥3,000 values |
| T6 | `test_excluded_by_design_entry_removed` | `layers.EXCLUDED_BY_DESIGN` no longer lists the ACS deferral |
| T7 | `test_value_format_is_realistic` | Median income values fall in [10000, 250000] |

## Design Notes

### Ingest API call
The Census ACS API endpoint for county-grain B19013, 2023:
```
GET https://api.census.gov/data/2023/acs/acs5
    ?get=NAME,B19013_001E
    &for=county:*
    &in=state:*
    &key=<CENSUS_API_KEY>
```
Returns a 2D JSON array; first row is headers, subsequent rows are
`[NAME, B19013_001E, state, county]` tuples. ~3,143 records total.

### Table schema
```sql
CREATE TABLE IF NOT EXISTS acs5_county_2023_b19013 (
    geo_id      TEXT PRIMARY KEY,  -- 5-digit county FIPS (state||county)
    state_fips  TEXT NOT NULL,
    geo_name    TEXT,
    b19013_001e INTEGER,           -- median household income (USD)
    ingested_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_acs5_county_2023_b19013_state
    ON acs5_county_2023_b19013(state_fips);
```

### New layer in layers.py
```python
"demo_acs_median_income": LayerSpec(
    id="demo_acs_median_income",
    label="Median Household Income (ACS)",
    domain="demographics",
    grain="county",
    default_on=False,
    vintage="2023 ACS 5-year",
    coverage_note="3,143 counties",
    unit="USD",
    description="Median household income per ACS B19013 — county summary.",
    builder=_build_acs_median_income,
),
```
Builder reads `geo_id, b19013_001e` and emits the standard
`{geo_id: value}` shape with legend breaks at the 20/40/60/80 quintiles.

### Why a new dedicated table (and not modifying generate_table_name)
`generate_table_name` is on the hot path for every existing
ACS/Census ingest call. Changing its signature or output would touch
`batch_service`, `scheduler_service`, jobs router — all working code,
several months stable. SPEC_070's job is to add a county layer, not
restructure the Census table-naming convention. The grain-explicit
`acs5_county_*` namespace is reserved for this purpose; if more
county-grain ACS variables get added later (B25077 home value at
county, B23025 employment at county), they follow the same pattern.

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_070_acs_county_wealth.md` | Create | This file |
| `docs/specs/.active_spec` | Modify | → `SPEC_070_acs_county_wealth` |
| `tests/test_spec_070_acs_county_wealth.py` | Create | T1-T7 |
| `app/sources/census/county_acs.py` | Create | Focused ingest path |
| `scripts/ingest_acs_county_b19013.py` | Create | One-shot run script |
| `app/services/atlas/layers.py` | Modify | Add `demo_acs_median_income` layer; remove from EXCLUDED_BY_DESIGN |

## Verification (manual)

1. `python scripts/ingest_acs_county_b19013.py` against cloud — exits 0,
   reports ≥3,000 rows inserted.
2. `docker-compose restart api` to pick up new layer.
3. `curl http://localhost:8001/api/v1/atlas/layers | jq '.layers_by_domain.demographics'`
   shows the new layer.
4. `curl http://localhost:8001/api/v1/atlas/layer/demo_acs_median_income | jq '.values | length'`
   returns ≥3,000.
5. `curl http://localhost:8001/api/v1/atlas/place/48201 | jq '.layers.demo_acs_median_income'`
   shows Harris County's value (~$70-80k).
6. Open `/atlas.html` in browser — new layer appears in demographics
   group; toggling it choropleth-recolors the map with ACS income.

## Feedback History

_No corrections yet._
