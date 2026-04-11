# PLAN 051 — Metro Development Profiles Dataset

**Status:** Approved
**Spec:** SPEC_041_metro_profiles
**Date:** 2026-03-30

## Goal

Build a passive, federal-data-only dataset covering the top 500 US CBSAs with development characteristics: building permit velocity, house price dynamics, unemployment, and housing cost burden. Compute derived "build hostility" scores that signal how permissive or resistant each metro is to new construction.

## Why This Matters

No packaged dataset exists that synthesizes BPS + FHFA + ACS + BLS LAUS into a per-metro development risk signal. All sources are federal, free, and auto-refreshable. First-pass value for data center developers, PE real estate teams, and homebuilder investors.

## Data Sources

| Source | URL | Level | Key | Cadence |
|--------|-----|-------|-----|---------|
| Census BPS (metro) | `https://www2.census.gov/econ/bps/Metro/ma{year}a.txt` | MSA | None | Annual |
| FHFA HPI master | `https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv` | MSA/National/State | None | Monthly |
| Census ACS 5-year | `https://api.census.gov/data/2023/acs/acs5` | CBSA | CENSUS_SURVEY_API_KEY | Annual |
| BLS LAUS (metro) | `https://api.bls.gov/publicAPI/v2/timeseries/data/` | MSA | BLS_API_KEY | Monthly |

## Architecture

```
app/sources/metro/
├── __init__.py
├── cbsa_reference.py      # Static list of top 500 CBSAs (code, name, type, state, pop_rank)
├── client.py              # MetroDataClient — wraps BPS, FHFA, ACS, BLS calls
└── ingest.py              # MetroProfileIngestor — orchestrates all sources, upserts DB

app/services/
└── metro_profile_service.py   # Derives scores, builds rankings

app/api/v1/
└── metro_profiles.py          # 4 endpoints

app/core/models.py             # MetroReference + MetroProfile models
app/main.py                    # Register router
```

## Phase 1 — Models (1 file)

Add to `app/core/models.py`:

```python
class MetroReference(Base):
    __tablename__ = "metro_reference"
    cbsa_code      = Column(String(5), primary_key=True)
    cbsa_name      = Column(Text)
    metro_type     = Column(String(20))   # metropolitan | micropolitan
    state_abbr     = Column(String(10))   # primary state(s)
    population_rank = Column(Integer)
    created_at     = Column(DateTime, default=func.now())

class MetroProfile(Base):
    __tablename__ = "metro_profiles"
    id             = Column(Integer, primary_key=True, autoincrement=True)
    cbsa_code      = Column(String(5), ForeignKey("metro_reference.cbsa_code"), index=True)
    data_vintage   = Column(String(10))   # "2024" or "2024-Q3"

    # Census BPS (annual)
    permits_total          = Column(Integer)
    permits_1unit          = Column(Integer)
    permits_2to4           = Column(Integer)
    permits_5plus          = Column(Integer)
    permits_per_1000_units = Column(Numeric(10, 2))
    multifamily_share_pct  = Column(Numeric(5, 2))

    # FHFA HPI
    hpi_current            = Column(Numeric(10, 3))
    hpi_yoy_pct            = Column(Numeric(6, 2))
    hpi_5yr_pct            = Column(Numeric(6, 2))

    # Census ACS
    population             = Column(Integer)
    median_hh_income       = Column(Integer)
    housing_units_total    = Column(Integer)
    cost_burden_severe_pct = Column(Numeric(5, 2))

    # BLS LAUS
    unemployment_rate      = Column(Numeric(5, 2))
    labor_force_size       = Column(Integer)

    # Derived scores (0–100)
    permit_velocity_score  = Column(Numeric(5, 1))
    multifamily_score      = Column(Numeric(5, 1))
    supply_elasticity_score = Column(Numeric(5, 1))
    build_hostility_score  = Column(Numeric(5, 1))
    build_hostility_grade  = Column(String(1))    # A/B/C/D

    sources_available      = Column(JSON)   # ["bps", "fhfa", "acs", "laus"]
    data_completeness_pct  = Column(Numeric(5, 1))
    updated_at             = Column(DateTime, default=func.now(), onupdate=func.now())

    __table_args__ = (UniqueConstraint("cbsa_code", "data_vintage"),)
```

## Phase 2 — CBSA Reference Data

`app/sources/metro/cbsa_reference.py` — Embed the top 400 Metropolitan Statistical Areas (all of them) as a static list. Source: Census Bureau CBSA definitions 2023. Key: CBSA code (5-digit string), name, type, primary state, population rank.

Format: Python list of dicts, loaded at startup and upserted into `metro_reference`.

## Phase 3 — MetroDataClient

`app/sources/metro/client.py` with four async methods:

### `fetch_bps_metro(year: int) -> List[dict]`
- GET `https://www2.census.gov/econ/bps/Metro/ma{year}a.txt`
- Parse fixed-width text file:
  - Column offsets documented at Census BPS technical notes
  - Extract: CSA code, CBSA code, metro name, 1-unit, 2-4 unit, 3-4 unit, 5+ unit permit counts
- Return list of dicts keyed by cbsa_code

### `fetch_fhfa_msa() -> List[dict]`
- GET `https://www.fhfa.gov/hpi/download/monthly/hpi_master.csv`
- Parse CSV, filter `hpi_type == "MSA"`
- Group by place_id (= CBSA code), compute:
  - latest index value
  - YoY % = (latest - 1yr_ago) / 1yr_ago * 100
  - 5yr % = (latest - 5yr_ago) / 5yr_ago * 100
- Return one record per MSA with current + computed fields

### `fetch_acs_cbsa(variables: List[str], year: int = 2023) -> List[dict]`
- GET Census ACS endpoint with `for=metropolitan+statistical+area/micropolitan+statistical+area:*`
- Variables: B01003_001E, B19013_001E, B25001_001E, B25070_010E
- Parse array-of-arrays response → list of dicts
- Return keyed by CBSA GEOID (matches CBSA code)

### `fetch_bls_laus_metro(cbsa_codes: List[str]) -> List[dict]`
- Build BLS LAUS series IDs from a static BLS-area-code → CBSA-code mapping
- Series format: `LAUMT{7digit_bls_area}0000000000003`
- Batch 50 series per POST to BLS API
- Return dict keyed by cbsa_code with unemployment_rate and labor_force_size

## Phase 4 — MetroProfileIngestor

`app/sources/metro/ingest.py`:
1. Upsert `metro_reference` from CBSA reference list
2. Fetch BPS for last 3 years, use most recent available year
3. Fetch FHFA HPI for all MSAs
4. Fetch ACS for all CBSAs (population, income, housing units, cost burden)
5. Fetch BLS LAUS for all metros
6. For each CBSA: join all sources, call `MetroProfileService.compute_scores()`
7. `null_preserving_upsert()` into `metro_profiles` on (cbsa_code, data_vintage)

## Phase 5 — MetroProfileService (Scoring)

`app/services/metro_profile_service.py`:

```python
def compute_scores(profiles: List[dict]) -> List[dict]:
    # Step 1: compute permits_per_1000_units and multifamily_share_pct
    # Step 2: normalize each raw metric across all metros → 0–100 percentile rank
    # Step 3: compute composite build_hostility_score (inverted)
    # Step 4: assign grade A/B/C/D
    # Step 5: return enriched records
```

Normalization: `percentile_rank(value, all_values) * 100` — simple, no arbitrary thresholds.

Build hostility formula:
```
build_hostility = 100 - (
    0.40 * supply_elasticity_percentile +
    0.30 * permit_velocity_percentile +
    0.20 * multifamily_percentile +
    0.10 * low_cost_burden_percentile
)
```

Grades: 0-25 = A (very buildable), 26-50 = B, 51-75 = C, 76-100 = D (very hostile).

## Phase 6 — API Endpoints

`app/api/v1/metro_profiles.py`:

```
GET  /metro-profiles/                       # list, paginated, filter by state/type
GET  /metro-profiles/rankings               # sorted by build_hostility_score desc
GET  /metro-profiles/{cbsa_code}            # single metro detail + factor breakdown
POST /metro-profiles/ingest                 # trigger background ingest job
```

Response shape for list/rankings:
```json
{
  "total": 400,
  "data": [{
    "cbsa_code": "35620",
    "cbsa_name": "New York-Newark-Jersey City, NY-NJ-PA",
    "build_hostility_score": 91.2,
    "build_hostility_grade": "D",
    "permit_velocity_score": 8.1,
    "multifamily_score": 45.2,
    "supply_elasticity_score": 12.3,
    "hpi_yoy_pct": 4.2,
    "unemployment_rate": 4.1,
    "data_completeness_pct": 100.0
  }]
}
```

## Phase 7 — Wire Into main.py

Register router with tag "Metro Intelligence".

## Execution Order

1. Models → `models.py`
2. CBSA reference list → `cbsa_reference.py`
3. Client methods → `client.py`
4. Ingestor → `ingest.py`
5. Scoring service → `metro_profile_service.py`
6. API router → `metro_profiles.py`
7. Register in `main.py`
8. Restart + trigger ingest + verify

## Decisions

- **Static CBSA reference** rather than dynamic download — avoids dependency on Census crosswalk file format changes
- **Percentile ranks** for normalization rather than hardcoded thresholds — self-calibrating as more metros are added
- **`null_preserving_upsert`** — safe for incremental enrichment when some sources lag
- **Annual vintage** for V1 — BPS and ACS are annual; quarterly update of FHFA/LAUS can come in V2
- **No BLS LAUS for every metro in V1** — BLS area codes require a mapping; embed top 150 metros by hand for now, fill the rest with NULL
