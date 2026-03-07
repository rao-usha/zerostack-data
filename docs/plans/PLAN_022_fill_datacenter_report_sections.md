# PLAN 022 — Fill Empty Datacenter Report Sections

**Status: COMPLETE**

## Summary of Changes

Three categories of fixes were needed and applied:

### Phase 1: Fix Query Mismatches — DONE

**Root cause found:** `_get_power_analysis` queried non-existent `electricity_price` table (it existed but with wrong column names). This caused a PostgreSQL transaction failure, and since the `except` blocks didn't call `db.rollback()`, the session was poisoned — ALL subsequent queries silently failed.

**Fixes applied to `app/reports/templates/datacenter_site.py`:**

1. **Session rollback safety** — Added `_safe_db_call()` wrapper in `gather_data()` with automatic `db.rollback()` on failure. Also added `db.rollback()` to all 12 `except Exception:` blocks so individual method failures don't poison the session.

2. **Section 5 (Connectivity)** — `internet_exchange.state` is NULL for most records (only city populated). Changed `_get_connectivity()` to group IXs by city and use a subquery to find cities within a state's DC facilities. Changed return key from `ix_by_state` to `ix_by_city`. Updated rendering to show "Internet Exchanges by City".

3. **Section 8 (Tax & Incentives)** — `incentive_program` columns were wrong (`incentive_type` → `program_type`, `industry_focus` → `target_industries`, `max_value` → `max_benefit`, `duration_years` → `benefit_duration_years`). `incentive_deal` used `company` → `company_name`, `jobs_promised` → `jobs_announced`. Removed DC-company-only filter; now shows all state deals. Updated rendering column references.

4. **Section 10 (Workforce)** — Removed non-existent NAICS code `518210`. Now queries supersector codes `1022` (Information), `1024` (Professional & Business Services), `1013` (Manufacturing).

5. **Section 4 (Power/Price)** — Fixed `electricity_price` query: `commercial_rate` → `avg_price_cents_kwh`, `state` → `geography_name`, added `WHERE sector = 'commercial'`.

6. **Section 9 (Environmental Risk)** — Changed from non-existent `flood_zone` table to `national_risk_index` (3,564 rows). Shows county-level risk scores with tornado, hurricane, wildfire breakdowns. Changed return key from `risk_by_state` to `risk_by_county`. Updated data sources list to reference correct tables.

### Phase 2: Run Missing Collectors — DONE

Triggered all 5 domains via `POST /api/v1/site-intel/sites/collect`. All 6 jobs completed successfully.

**Also fixed a critical worker bug:** `app/worker/main.py` `claim_job()` used `QueueJobStatus.PENDING.name` (`"PENDING"`) but DB stores `QueueJobStatus.PENDING.value` (`"pending"`). Case mismatch meant workers could NEVER claim jobs. Fixed `.name` → `.value` in claim query and heartbeat cancellation check.

**Collection results:**
- `national_risk_index`: 3,564 rows (FEMA NRI)
- `environmental_facility`: 64,505 rows (EPA)
- `electricity_price`: 25,498 rows (EIA)
- `seismic_hazard`: 536, `fault_line`: 17 (USGS)
- `opportunity_zone`: 8,765, `foreign_trade_zone`: 105 (Incentives)
- Epoch DC: 0 (GitHub URL returned 404)
- Census BPS/Gov: 0 (no data returned for TX)

### Phase 3: Re-Score & Regenerate — DONE

- Re-scored TX counties
- Generated TX report (report_168, 59,652 bytes)
- Generated national report (report_169, 64,849 bytes)

## Final Report Section Status

| # | Section | Status | Data Source |
|---|---------|--------|-------------|
| 1 | Executive Summary | HAS DATA | Scoring results |
| 2 | Geographic Heat Map | HAS DATA | State averages |
| 3 | Top Candidate Counties | HAS DATA | Scoring results |
| 4 | Power Grid Analysis | HAS DATA | `electricity_price` (25K rows) |
| 5 | Connectivity & Fiber | HAS DATA | `internet_exchange` + `data_center_facility` |
| 6 | Regulatory & Permitting | EMPTY | Needs `county_regulatory_scores` (scorer not run) |
| 7 | Real Estate & Land | EMPTY | `industrial_site` (0 rows), `brownfield_site` (0 rows) |
| 8 | Tax & Incentives | HAS DATA | `incentive_program` (50) + `incentive_deal` (28) |
| 9 | Environmental & Risk | HAS DATA | `national_risk_index` (3,564 rows) |
| 10 | Workforce & Labor | HAS DATA | `industry_employment` (602 rows) |
| 11 | Existing DC Clusters | HAS DATA | `data_center_facility` (1,382 rows) |
| 12 | Capital Model | HAS DATA | Computed (no DB) |
| 13 | Deal Scenarios | HAS DATA | Computed from scores |
| 14 | Data Sources | HAS DATA | Table counts |

**Result: 12 of 14 sections now have data** (up from 7). The 2 remaining empty sections need data that doesn't exist in the DB yet (regulatory scorer, industrial site scraping).

## Files Changed

- `app/reports/templates/datacenter_site.py` — Query fixes, session rollback, table name corrections
- `app/worker/main.py` — Fixed `.name` → `.value` enum mismatch in claim_job and heartbeat

## Testing

- `pytest tests/test_datacenter_site_report.py -v` — 9/9 passed
- TX report regenerated and verified
- National report regenerated and verified
