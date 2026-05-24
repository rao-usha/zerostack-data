# SPEC 071 — USAspending county-aggregate federal-dollars layer

**Status:** Draft
**Task type:** collector (new focused ingest path) + atlas layer wire-up
**Date:** 2026-05-23
**Plan:** PLAN_067 §3 (Atlas Data Coverage Expansion — Phase 3a)
**Test file:** `tests/test_spec_071_usaspending_county.py`
**Builds on:** existing `app/sources/usaspending/`; SPEC_065 layer registry; SPEC_070 grain-explicit table-namespace pattern.

## Goal

Replace the unusable `usaspending_awards` 10k-row stub (zero dated rows,
no county field) with a real **county-grain federal-dollars layer** for
the Atlas. PLAN_067 §3 was explicit: "Target a bounded window (last 2-3
fiscal years), prime + sub awards, with action_date / period dates and
place_of_performance geocoded to county. Aggregate to county×NAICS×month
at ingest if raw rows are too heavy."

Path of least resistance — hit USAspending's `spending_by_geography`
endpoint, which **already aggregates server-side to county for a given
filter window in a single POST**, returning ~3,000 county rows for one
fiscal year. No volume concern; no millions of rows to land on
`db-f1-micro`. The bounded-window-then-aggregate plan reduces to "one
API call per (fiscal_year × award_type_group)" with this endpoint.

Re-rates PLAN_066 §5.1 economy/finance from 🟡 to ✅ for federal dollars;
removes `usaspending_awards` from `EXCLUDED_BY_DESIGN`.

## Pre-flight findings (logged 2026-05-23)

- `usaspending_awards`: 10,190 rows. **Zero rows have a non-null
  `period_of_performance_start` or `_end` date.** No
  `place_of_performance_county_*` column at all. Geographic info is
  city/state/zip only — un-joinable to county boundaries.
- `app/sources/usaspending/` already exists (client.py, ingest.py,
  metadata.py) and runs `/api/v2/search/spending_by_award/`. We do **not**
  reuse this path for SPEC_071 because: (a) it hits the per-award API
  rather than the much cheaper geography aggregate; (b) the stored
  schema is missing the county column we need; (c) modifying it risks
  breaking the existing `POST /usaspending/ingest` endpoint and
  scheduler hooks.
- Same grain-explicit table-namespace pattern as SPEC_070: write to a
  new dedicated table so legacy contracts are untouched.

## Scope decision — MVP and stretch

**MVP (this spec):**
- One new ingest path → one new table → one new Atlas layer.
- Bounded window: fiscal year 2024 (Oct 2023 – Sep 2024) by default;
  parameterizable. Re-runnable for additional years.
- Filter: prime contracts (`award_type_codes = ["A","B","C","D"]`).
  Grants/loans/sub-awards left for follow-on (smaller dollar dominance
  for an MVP; contracts are the bulk).
- Server-side county aggregation via `spending_by_geography`. Storage
  is one row per (county_fips, fiscal_year, award_type_group).

**Stretch — same spec if time permits, otherwise SPEC_071b:**
- Top-N awards table via `spending_by_award` (paginated) — captures
  individual award metadata for the place-panel drill-down and for the
  Recent Activity feature (SPEC_067).

**Explicitly out of scope:**
- Sub-awards (USAspending's sub-award files; volume + linkage non-trivial).
- All-time historical backfill (just FY2024 for MVP; can re-run for prior
  years on the same UPSERT key).
- Recipient-level entity resolution.

## Acceptance Criteria

- [ ] New table `usaspending_county_fy_totals` exists with schema
      `(geo_id text, fiscal_year int, award_type_group text,
       total_obligation numeric, award_count int, ingested_at timestamp,
       PRIMARY KEY (geo_id, fiscal_year, award_type_group))`.
- [ ] ≥2,500 rows landed for FY2024 contracts (not every county receives
      federal dollars, but most do).
- [ ] `geo_id` is the 5-digit county FIPS for every row; no nulls.
- [ ] `total_obligation` sums sanity-check against published USAspending
      aggregates (one or two known-large counties verified by hand).
- [ ] New ingest entry point
      `app/sources/usaspending/county_aggregate.py::ingest_county_fy_totals(db, fiscal_year, award_type_group)`.
- [ ] One-shot run script `scripts/ingest_usaspending_county.py`
      (FY2024 contracts by default).
- [ ] New Atlas layer `econ_federal_dollars` registered in
      `app/services/atlas/layers.py` — county grain, vintage "FY2024",
      reading the most recent fiscal year × contracts row per county.
- [ ] `EXCLUDED_BY_DESIGN["usaspending_awards"]` entry removed (with a
      pointer-comment to SPEC_071 the way SPEC_070 did).
- [ ] `GET /api/v1/atlas/layer/econ_federal_dollars` returns ≥2,500
      county values with realistic legend (counties like Arlington VA,
      Fairfax VA, Los Angeles CA top the list).
- [ ] `GET /api/v1/atlas/place/48201` (Harris Co TX) now includes the
      federal-dollars value.

## Test Cases

| ID | Test | What it verifies |
|---|---|---|
| T1 | `test_table_populated_for_fy2024` | After ingest, ≥2,500 contract rows for FY2024 |
| T2 | `test_all_geo_ids_are_5_digit_county_fips` | Every row's geo_id matches `^\d{5}$`; no nulls |
| T3 | `test_total_obligation_realistic` | Sum across all counties is in [$100B, $1T] for FY2024 contracts |
| T4 | `test_layer_appears_in_registry` | `/atlas/layers` includes `econ_federal_dollars` |
| T5 | `test_layer_endpoint_returns_county_values` | `/atlas/layer/econ_federal_dollars` returns ≥2,500 values |
| T6 | `test_excluded_by_design_removed_usaspending` | `EXCLUDED_BY_DESIGN` no longer lists `usaspending_awards` |
| T7 | `test_legacy_table_untouched` | `usaspending_awards` row count unchanged (10,190) |
| T8 | `test_no_zero_dollar_rows` | All rows have `total_obligation > 0` |

## Design Notes

### USAspending API call (the whole MVP in one request)
```
POST https://api.usaspending.gov/api/v2/search/spending_by_geography/
{
  "scope": "place_of_performance",
  "geo_layer": "county",
  "filters": {
    "time_period": [{"start_date": "2023-10-01", "end_date": "2024-09-30"}],
    "award_type_codes": ["A", "B", "C", "D"]
  },
  "subawards": false
}
```
Response: `{ scope, geo_layer, results: [{shape_code, aggregated_amount,
display_name, per_capita}, ...] }`. `shape_code` is the 5-digit county
FIPS. ~3,000 rows. Subsequent fiscal years are additional calls with
different `time_period`.

Counter is the `count` field if requested per the API doc; if not,
include `?subawards=false&filter=...&request_field=count` — verify in
implementation. Falls back to `null` if not exposed (layer just won't
show award_count in the place panel).

### Table schema
```sql
CREATE TABLE IF NOT EXISTS usaspending_county_fy_totals (
    geo_id            TEXT NOT NULL,                   -- 5-digit county FIPS
    fiscal_year       INTEGER NOT NULL,
    award_type_group  TEXT NOT NULL,                   -- 'contracts' | 'grants' | ...
    total_obligation  NUMERIC NOT NULL,
    award_count       INTEGER,
    ingested_at       TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (geo_id, fiscal_year, award_type_group)
);
CREATE INDEX IF NOT EXISTS idx_usaspending_county_fy_totals_year
    ON usaspending_county_fy_totals(fiscal_year);
```

### New layer in layers.py
```python
"econ_federal_dollars": LayerSpec(
    id="econ_federal_dollars",
    label="Federal Contract Dollars (FY)",
    domain="economy",
    grain="county", default_on=False,
    vintage="FY2024 contracts",
    coverage_note="~3,000 counties via USAspending geography aggregate",
    unit="USD",
    description="Federal prime contract obligations to recipients with "
                "place of performance in the county, per USAspending "
                "spending_by_geography. Most-recent fiscal year.",
    builder=_build_federal_dollars,
),
```
Builder selects the latest fiscal_year × `'contracts'` group.

### Why not reuse `app/sources/usaspending/ingest.py`
That path hits `/api/v2/search/spending_by_award/` — the per-award API.
For 3M-row coverage you'd need ~30,000 paginated calls; the existing
schema lacks county geocoding; and modifying its table schema would
break the live `POST /usaspending/ingest` endpoint + scheduler. SPEC_071
sits next to it as a focused county-aggregate path.

## Files to Create / Modify

| File | Action | Description |
|---|---|---|
| `docs/specs/SPEC_071_usaspending_county_federal_dollars.md` | Create | This file |
| `docs/specs/.active_spec` | Modify | → `SPEC_071_usaspending_county_federal_dollars` |
| `tests/test_spec_071_usaspending_county.py` | Create | T1-T8 |
| `app/sources/usaspending/county_aggregate.py` | Create | Focused ingest path |
| `scripts/ingest_usaspending_county.py` | Create | One-shot runner |
| `app/services/atlas/layers.py` | Modify | Add `econ_federal_dollars`; remove `usaspending_awards` from `EXCLUDED_BY_DESIGN` |

## Verification (manual)

1. `python scripts/ingest_usaspending_county.py --fy 2024 --group contracts`
   → exits 0, reports ≥2,500 rows inserted.
2. `docker-compose restart api`.
3. `curl http://localhost:8001/api/v1/atlas/layers | jq '.layers_by_domain.economy'`
   shows the new layer.
4. `curl http://localhost:8001/api/v1/atlas/layer/econ_federal_dollars | jq '.values | length'`
   returns ≥2,500.
5. `curl http://localhost:8001/api/v1/atlas/place/51013` (Arlington Co VA)
   shows a large federal-dollars value (Pentagon adjacency — should be
   tens of billions).
6. Open `/atlas.html` in browser; new layer in `economy` group; toggling
   it recolors the map with DC-metro hotspots clearly visible.

## Feedback History

_No corrections yet._
