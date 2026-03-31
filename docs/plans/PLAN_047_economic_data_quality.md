# PLAN_047 — Economic Data Quality & Governance

**Status:** Draft — awaiting approval
**Date:** 2026-03-30
**Depends on:** PLAN_046 (data bootstrap), existing DQ infrastructure (dq_base.py, post-ingestion hook)
**Feeds into:** PLAN_048 (data products trust scores)

---

## Problem

The DQ system covers PE, People, Site Intel, and 3PL data. **Zero DQ coverage for economic data** (FRED, BLS, BEA, Census) despite it being the foundation of the macro intelligence and PE data products layers.

Economic time series have unique failure modes that existing DQ checks don't catch:
- **Release lag violations** — CPI publishes on a known schedule (2nd week of month). If we're 3 weeks past the release date and the data isn't updated, something is broken
- **Series gaps** — A monthly time series missing January 2024 in an otherwise continuous series is a data defect, not a schema issue
- **Revision drift** — BEA revises historical state GDP data annually. Our DB may have stale vintages
- **Cross-source incoherence** — FRED UNRATE and BLS LNS14000000 are the same underlying data from different wrappers. They should agree to 0.1pp
- **Geographic coverage gaps** — BEA regional should have all 50 states. Missing 3 states is a silent failure

---

## What We're Building

### 1. `EconDQService` — Domain-Specific Provider

New file: `app/services/econ_dq_service.py`

Extends `BaseQualityProvider` (from `dq_base.py`). Gets invoked by the post-ingestion hook when source starts with `fred_`, `bls_`, `bea_`, or `census_`.

**8 Checks (by dimension):**

#### Freshness (Weight: 40% for economic data — higher than default 25%)
1. **`series_stale`** — Latest value older than expected release lag
   - FRED daily series (DFF, DGS10): stale if > 3 business days old
   - BLS monthly series (JOLTS, CES, CPI): stale if > 45 days since last period
   - BEA annual series (SAGDP2N): stale if > 14 months since last year-end
   - Census ACS: stale if > 13 months (ACS releases annually in ~September)
   - Severity: WARNING for < 2× expected lag, ERROR for > 2×

2. **`release_calendar_miss`** — Known official release missed
   - BLS publishes a release calendar at bls.gov/schedule. If CPI releases 2026-03-12 and we still have January data on 2026-03-15 → ERROR
   - Implementation: hardcoded release schedule per series type (monthly CPI = 2nd Tue/Wed of month; monthly Employment = 1st Friday)
   - Severity: ERROR

#### Completeness (Weight: 35%)
3. **`series_gap`** — Missing periods in a time series
   - For each series, check date continuity: all months present from earliest to latest
   - Detect gaps > 1 period (1 month for monthly, 1 quarter for quarterly)
   - Implementation: `LAG(date)` window function, flag where gap > expected frequency
   - Severity: ERROR for gaps > 3 months, WARNING for 1–3 month gaps

4. **`geographic_coverage`** — State/county coverage completeness
   - BEA regional tables (SAGDP2N, SAINC1): should have 50 states + DC = 51 rows per year
   - Census state tables (acs5_*_b01003): 51 rows (50 states + DC)
   - Flag if `COUNT(DISTINCT geo_fips)` for latest year < 45 (allows for PR + territories)
   - Severity: ERROR if < 45, WARNING if < 51

#### Validity (Weight: 15%)
5. **`outlier_value`** — Statistical outlier detection for economic indicators
   - Compute rolling mean ± 4σ for each series over last 5 years
   - Flag values outside band as potential data errors
   - Examples: GDP growth +50% QoQ, unemployment rate -5%, CPI YoY of 500%
   - Severity: ERROR if outside ±6σ, WARNING if ±4–6σ

6. **`invalid_range`** — Hard bounds violation
   - Unemployment rate: must be 0–100 (%)
   - GDP values: must be > 0 (can't have negative real GDP level)
   - CPI index: must be > 0
   - Employment levels: must be ≥ 0
   - Severity: ERROR

#### Consistency (Weight: 10%)
7. **`cross_source_coherence`** — Same underlying data from different source APIs
   - `FRED UNRATE` ≈ `BLS LNS14000000` (both = national unemployment rate, monthly)
   - `FRED CPIAUCSL` ≈ `BLS CUUR0000SA0` (both = CPI-U all items)
   - `FRED TOTALSA` ≈ `BLS CES1000000001` (auto employment)
   - Check: max deviation ≤ 0.2pp for same period; flag if diverges more
   - Only runs when both source tables are populated
   - Severity: WARNING (different vintages may diverge slightly)

8. **`revision_detected`** — Historical data changed since last ingest
   - Store `value_hash` (series_id + period + value) on each ingest
   - On next ingest, compare: if same period has a different value → revision flag
   - Log revision to `econ_data_revisions` table (new table, lightweight: series_id, period, old_value, new_value, detected_at)
   - Severity: INFO (revisions are normal and expected; tracked for transparency)

---

### 2. Weighted Scoring Override for Economic Data

Economic data should weight **freshness more heavily** than the default `dq_base.py` formula (which weights completeness 35% / freshness 25%).

Economic data scoring:
```python
ECON_SCORING_WEIGHTS = {
    "completeness": 0.25,   # ↓ from 0.35
    "freshness":    0.40,   # ↑ from 0.25 (economic signals are time-sensitive)
    "validity":     0.25,   # same
    "consistency":  0.10,   # same
}
```

Override via `get_scoring_weights()` on `EconDQService`.

---

### 3. Release Calendar SLA Targets

New constants file: `app/services/econ_release_calendar.py`

Maps series types to expected release lags (business days after period end):

| Series Type | Expected Lag | Official Release |
|-------------|-------------|-----------------|
| FRED daily series (DFF, DGS10) | 1 business day | T+1 |
| BLS Monthly Employment (CES/CPS) | 8 business days | 1st Friday of following month |
| BLS JOLTS | 35 calendar days | ~5th of second month following |
| BLS CPI | 12 business days | 2nd or 3rd week of following month |
| BEA GDP (preliminary) | 28 calendar days | Last week of following month |
| BEA Personal Income | 28 calendar days | Last week of following month |
| BEA Regional GDP | 14 months | Annual, September release |
| Census ACS 5-year | 13 months | Annual, December release |

The `series_stale` check uses these constants. Each lag is "expected max" — data arriving after this is flagged.

---

### 4. Post-Ingestion Hook Integration

In `app/core/dq_post_ingestion_hook.py`, add routing for economic sources:

```python
# Existing routing:
"people_*"    → PeopleQAService
"pe_*"        → PEDQService
"site_intel_*"→ SiteIntelDQService
"three_pl_*"  → ThreePLDQService

# Add:
"fred_*"      → EconDQService(series_type="fred")
"bls_*"       → EconDQService(series_type="bls")
"bea_*"       → EconDQService(series_type="bea")
"acs5_*"      → EconDQService(series_type="census")
```

EconDQService runs after every FRED/BLS/BEA/Census ingestion job. Results stored in existing `data_quality_reports` table.

---

### 5. BLS LAUS Integration (State Unemployment)

Extend the main BLS client to support Local Area Unemployment Statistics (LAUS):

**New method in `app/sources/bls/client.py`:**
```python
LAUS_STATE_SERIES = {
    "01": "Alabama", "02": "Alaska", "04": "Arizona", ...  # all 50 states + DC
}

def get_laus_series_ids(geo_type="state") -> List[str]:
    """Generate LAUS series IDs for all states."""
    # LASST{FIPS}0000000000003 = unemployment rate
    # LASST{FIPS}0000000000006 = labor force
    # LASST{FIPS}0000000000005 = employment
    return [f"LASST{fips}0000000000003" for fips in LAUS_STATE_SERIES.keys()]
```

**New endpoint in `app/api/v1/bls.py`:**
```
POST /bls/ingest/laus     — Ingest state-level unemployment (51 series)
GET  /bls/laus/latest     — Latest LAUS rates by state (used by /econ-snapshot/regional)
```

Table: stored in existing `bls_*` pattern → table name `bls_laus_state`

---

### 6. New Table: `econ_data_revisions`

Lightweight table to track when BEA/BLS retroactively revises historical values:

```python
class EconDataRevision(Base):
    __tablename__ = "econ_data_revisions"
    id             = Column(Integer, primary_key=True)
    source         = Column(String)          # "bea", "bls", "fred"
    table_name     = Column(String)          # e.g., "bea_regional"
    series_id      = Column(String)          # e.g., "SAGDP2N"
    geo_fips       = Column(String)          # for regional data
    period         = Column(String)          # "2022" or "2024-01"
    old_value      = Column(Numeric(20, 4))
    new_value      = Column(Numeric(20, 4))
    revision_pct   = Column(Numeric(10, 4))  # ((new-old)/old)*100
    detected_at    = Column(DateTime)
    ingestion_job_id = Column(Integer, ForeignKey("ingestion_jobs.id"))
```

This enables: "BEA revised California 2022 GDP from $3.6T to $3.7T — 2.8% upward revision."

---

### 7. API Endpoints — New Router

New file: `app/api/v1/econ_dq.py`

```
GET  /econ-dq/health                — DQ scores for all economic sources (FRED, BLS, BEA, Census)
GET  /econ-dq/freshness             — Per-series freshness status vs. release calendar
GET  /econ-dq/gaps/{source}         — Series continuity check results
GET  /econ-dq/coverage/{source}     — Geographic coverage metrics
GET  /econ-dq/revisions             — Historical revision log
GET  /econ-dq/coherence             — Cross-source coherence checks (FRED vs BLS)
POST /econ-dq/run/{source}          — Trigger DQ check for a specific source
GET  /econ-dq/sla                   — Release calendar SLA compliance
```

These power the DQ panel inside the `economic-intelligence.html` page (small "Data Quality" section at bottom: "FRED interest_rates: 94/100 · Last updated 2026-03-29 · All 6 series current").

---

## Files

| File | Action |
|------|--------|
| `app/services/econ_dq_service.py` | CREATE — EconDQService (8 checks) |
| `app/services/econ_release_calendar.py` | CREATE — Release lag constants |
| `app/core/dq_post_ingestion_hook.py` | MODIFY — Add econ source routing |
| `app/sources/bls/client.py` | MODIFY — Add LAUS series + get_laus_series_ids() |
| `app/sources/bls/metadata.py` | MODIFY — Add LAUS dataset config |
| `app/api/v1/bls.py` | MODIFY — Add /bls/ingest/laus + /bls/laus/latest endpoints |
| `app/api/v1/econ_dq.py` | CREATE — 8 DQ governance endpoints |
| `app/core/models.py` | MODIFY — Add EconDataRevision table |
| `app/core/database.py` | MODIFY — Import new model |
| `app/main.py` | MODIFY — Register econ_dq router |

---

## Implementation Phases

| Phase | What | Files |
|-------|------|-------|
| 1 | `EconDQService` — 8 checks, weighted scoring | `econ_dq_service.py`, `econ_release_calendar.py` |
| 2 | Wire into post-ingestion hook | `dq_post_ingestion_hook.py` |
| 3 | `EconDataRevision` model + revision detection | `models.py`, `database.py` |
| 4 | LAUS series in BLS client + ingest endpoint | `bls/client.py`, `bls/metadata.py`, `bls.py` |
| 5 | DQ governance endpoints | `econ_dq.py`, `main.py` |
| 6 | DQ health panel on economic-intelligence.html | Frontend (small addition to PLAN_046) |
| 7 | Tests — 12 unit tests covering all 8 checks + LAUS + revision detection | `tests/test_spec_XXX_econ_dq.py` |

---

## Verification

```bash
# After Phase 1+2: DQ runs automatically after next FRED ingest
POST /fred/ingest {"category": "interest_rates"}
# → DQ report auto-generated in data_quality_reports
GET /econ-dq/health
# → {"fred_interest_rates": {"score": 95, "checks": {...}}}

# After Phase 4: LAUS ingested
POST /bls/ingest/laus
GET /bls/laus/latest
# → [{"state": "Texas", "fips": "48", "unemployment_rate": 4.1, "period": "2026-02"}, ...]

# After Phase 5: Governance dashboard
GET /econ-dq/freshness
# → per-series staleness vs release calendar
GET /econ-dq/coherence
# → FRED UNRATE vs BLS LNS14000000 comparison
```
