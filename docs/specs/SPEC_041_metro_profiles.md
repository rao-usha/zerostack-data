# SPEC 041 — Metro Development Profiles Dataset

**Status:** Draft
**Task type:** service
**Date:** 2026-03-30
**Test file:** tests/test_spec_041_metro_profiles.py

## Goal

Build a metro-level development characteristics dataset covering the top 500 US CBSAs (Core Based Statistical Areas). Pull building permit velocity, house price appreciation, unemployment, and housing demographics from federal sources (Census BPS, FHFA HPI, BLS LAUS, Census ACS), then compute derived "build hostility" scores — a composite signal of how permissive or resistant a metro is to new development.

## Acceptance Criteria

- [ ] `metro_reference` table seeded with top 500 CBSAs (code, name, type, state, population rank)
- [ ] `metro_profiles` table populated with raw variables from ≥ 3 federal sources per metro
- [ ] Census BPS metro-level annual permit data ingested (1-unit, 2-4 unit, 5+ unit)
- [ ] FHFA HPI MSA-level data ingested (current index, YoY %, 5yr %)
- [ ] Census ACS CBSA-level data ingested (population, median HH income, housing units, cost burden %)
- [ ] BLS LAUS metro-level unemployment rate ingested
- [ ] Derived scores computed: supply_elasticity_score, permit_velocity_score, multifamily_score, build_hostility_score (all 0–100)
- [ ] `GET /api/v1/metro-profiles/` returns paginated list with scores
- [ ] `GET /api/v1/metro-profiles/{cbsa_code}` returns full detail with factor breakdown
- [ ] `GET /api/v1/metro-profiles/rankings` returns metros sorted by build_hostility_score
- [ ] `POST /api/v1/metro-profiles/ingest` triggers background data collection job

## Test Cases

| ID | Test Name | What It Verifies |
|----|-----------|------------------|
| T1 | test_metro_reference_seeded | metro_reference has ≥ 400 rows with valid CBSA codes |
| T2 | test_bps_metro_parser | Census BPS metro text file parsed correctly for permits by unit type |
| T3 | test_fhfa_msa_filter | FHFA CSV filtered to MSA geography type returns records with place_id |
| T4 | test_acs_cbsa_query | Census ACS returns population + income for a known CBSA |
| T5 | test_derived_scores_range | All computed scores are 0.0–100.0 |
| T6 | test_build_hostility_composite | High price + low permits → high hostility score |
| T7 | test_list_endpoint_paginated | /metro-profiles/ returns data array + total count |
| T8 | test_detail_endpoint | /metro-profiles/35620 returns NYC with factor breakdown |
| T9 | test_rankings_endpoint_sorted | /metro-profiles/rankings returns descending build_hostility_score |
| T10 | test_graceful_missing_source | Metro with missing one source still returns partial profile |

## Rubric Checklist

- [ ] Clear single responsibility (one domain concern per service)
- [ ] Async methods where I/O is involved
- [ ] Uses dependency injection for DB sessions and external clients
- [ ] All DB operations use parameterized queries
- [ ] Uses `null_preserving_upsert()` for enrichment workflows
- [ ] Error handling follows the error hierarchy (`RetryableError`, `FatalError`, etc.)
- [ ] Logging with structured context (source, operation, record counts)
- [ ] Has corresponding test file with mocked dependencies
- [ ] Tests cover happy path, error cases, and boundary conditions

## Design Notes

### Metro Universe
- Use Census CBSA definitions: `https://www2.census.gov/programs-surveys/metro-micro/geographies/reference-files/2023/delineation-files/`
- Embed top ~400 MSAs as a static reference list seeded at startup; extend with top micros to hit 500
- Key fields: `cbsa_code` (5-digit string), `cbsa_name`, `metro_type` (metropolitan/micropolitan), `state_abbr`, `population_rank`

### Census BPS Metro Data
- Annual metro-level permits: `https://www2.census.gov/econ/bps/Metro/ma{year}a.txt`
- Fixed-width text file, parses to: cbsa_code, name, 1-unit, 2-unit, 3-4 unit, 5+ unit counts
- Pull last 5 years for trend data

### FHFA HPI
- Single CSV: `https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv`
- Filter `hpi_type == "MSA"` → MSA-level records
- Compute YoY % and 5yr % from the time series
- `place_id` in FHFA maps to CBSA code (5-digit)

### Census ACS CBSA-level
- Query endpoint: `https://api.census.gov/data/{year}/acs/acs5?get={vars}&for=metropolitan+statistical+area/micropolitan+statistical+area:*`
- Variables: B01003_001E (population), B19013_001E (median HH income), B25001_001E (housing units), B25070_010E (severe rent burden 50%+)
- API key: `CENSUS_SURVEY_API_KEY` from env

### BLS LAUS Metro
- Series format: `LAUMT{7-digit-area-code}0000000000003` (unemployment rate)
- Need BLS area code → CBSA code mapping (embed top 400 mappings as static dict)
- Batch 50 series per BLS API call (limit with key)

### Derived Score Formulas
```
permit_velocity_score = normalize(permits_per_1000_units, all_metros)  # 0–100
multifamily_score     = normalize(permits_5plus / permits_total, all_metros)  # 0–100
supply_elasticity     = normalize(permits_per_1000_units / hpi_5yr_pct, all_metros)  # 0–100

build_hostility_score = 100 - (
    0.40 * supply_elasticity_score +
    0.30 * permit_velocity_score +
    0.20 * multifamily_score +
    0.10 * (100 - cost_burden_percentile)  # high burden = more need = hostility if supply low
)
```
All scores inverted so that high build_hostility = hard to build in.

### Graceful Degradation
- If a source is missing for a metro, set that source's columns to NULL
- Still compute scores from available sources, set `sources_available` JSON field
- API endpoints return `data_completeness_pct` per metro

## Files to Create/Modify

| File | Action | Description |
|------|--------|-------------|
| `app/core/models.py` | Modify | Add `MetroReference` and `MetroProfile` SQLAlchemy models |
| `app/sources/metro/__init__.py` | Create | Package init |
| `app/sources/metro/cbsa_reference.py` | Create | Static list of top 500 CBSAs |
| `app/sources/metro/client.py` | Create | MetroDataClient wrapping BPS, FHFA, ACS, BLS LAUS |
| `app/sources/metro/ingest.py` | Create | MetroProfileIngestor orchestrating all sources |
| `app/services/metro_profile_service.py` | Create | Derived score computation + ranking |
| `app/api/v1/metro_profiles.py` | Create | API router with 4 endpoints |
| `app/main.py` | Modify | Register metro_profiles router |

## Feedback History

_No corrections yet._
