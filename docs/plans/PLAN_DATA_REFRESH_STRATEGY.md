# PLAN: Master Data Refresh Strategy

**Status:** Approved & Implemented
**Created:** 2026-02-15
**Author:** Claude Code

---

## Overview

Nexdata has 40+ data sources across 4 collection systems (core ingestors, site intel, people, agentic). This document defines the unified refresh strategy: recommended frequencies, staggered time windows, and incremental update strategies.

All times are in **UTC** (Eastern Time = UTC - 5).

---

## Tier 1 — Daily (5:00–8:00 AM ET / 10:00–13:00 UTC)

| Source | Time (UTC) | Config | API Key | Volume | Notes |
|--------|-----------|--------|---------|--------|-------|
| Treasury FiscalData | 10:00 | `{"dataset": "daily_balance"}` | No | ~50 rows | Daily treasury statement |
| FRED (daily series) | 10:30 | `{"category": "interest_rates"}` | Yes | ~20 series | Only daily-frequency series |
| Prediction Markets | 11:00 | `{"sources": ["kalshi","polymarket"]}` | Varies | ~500 markets | Market odds snapshot |

---

## Tier 2 — Weekly (Mon–Wed early AM)

| Source | Day/Time (UTC) | Config | API Key | Volume | Notes |
|--------|---------------|--------|---------|--------|-------|
| FRED (all categories) | Mon 10:00 | `{"category": "all"}` | Yes | ~800K series | Full refresh of modified series |
| EIA Petroleum | Mon 11:00 | `{"dataset": "petroleum_weekly"}` | Yes | ~200 series | Weekly petroleum status |
| CFTC COT | Mon 12:00 | `{"report_type": "all"}` | No | ~500 rows | Weekly positions report |
| Freightos | Tue 10:00 | `{"index": "fbx"}` | No | ~50 routes | Freight rate indices |
| SCFI | Tue 10:30 | `{"index": "scfi"}` | No | ~20 routes | Shanghai freight index |
| Drewry | Tue 11:00 | `{"index": "drewry"}` | No | ~20 routes | Container indices |
| Web Traffic (Tranco) | Tue 12:00 | `{"list": "tranco_top1m"}` | No | ~10K entries | Ranking changes |
| GitHub Analytics | Wed 10:00 | `{}` | Yes | ~100 repos | Repo metrics |
| NOAA Weather | Wed 11:00 | `{"dataset": "daily_summaries"}` | Optional | Varies | Weather observations |

---

## Tier 3 — Monthly (spread across days 1–15)

| Source | Day/Time (UTC) | Config | API Key | Volume | Notes |
|--------|---------------|--------|---------|--------|-------|
| FEMA Disasters | 1st 11:00 | `{"dataset": "disasters"}` | No | ~100 records | New declarations |
| BEA GDP/Income | 3rd 11:00 | `{"dataset": "gdp"}` | Yes | ~500 series | National accounts |
| EIA Electricity | 4th 11:00 | `{"dataset": "electricity"}` | Yes | ~5K records | Utility rates |
| EIA Natural Gas | 4th 12:00 | `{"dataset": "natural_gas"}` | Yes | ~2K records | Gas prices |
| BLS Employment (CES) | 5th 13:00 | `{"dataset": "ces"}` | Yes | ~2K series | Jobs report (~1st Friday) |
| Data Commons | 5th 12:00 | `{}` | No | Varies | Unified public data |
| BLS CPI | 6th 13:00 | `{"dataset": "cpi"}` | Yes | ~1K series | Inflation data |
| BLS PPI | 7th 13:00 | `{"dataset": "ppi"}` | Yes | ~1K series | Producer prices |
| FDIC Banks | 8th 11:00 | `{"dataset": "financials"}` | No | ~5K banks | Bank call reports |
| SEC Form ADV | 9th 11:00 | `{}` | No | ~5K advisers | Adviser registrations |
| SEC Form D | 9th 12:00 | `{}` | No | ~2K offerings | Private placements |
| CMS Medicare | 10th 11:00 | `{"dataset": "utilization"}` | No | ~10K records | Healthcare utilization |
| App Store Rankings | 11th 11:00 | `{}` | Varies | ~1K apps | App metrics |
| FBI Crime | 12th 11:00 | `{"dataset": "ucr"}` | No | ~3K records | Crime statistics |
| IRS SOI | 13th 11:00 | `{"dataset": "zip_income"}` | No | ~30K records | Income by ZIP |
| Glassdoor | 14th 11:00 | `{}` | Yes | ~500 companies | Reviews/ratings |
| Yelp | 15th 11:00 | `{}` | Yes | ~1K businesses | Business listings |
| FCC Broadband | 15th 12:00 | `{}` | No | ~50K records | Broadband coverage |

---

## Tier 4 — Quarterly (1st two weeks of Jan/Apr/Jul/Oct)

| Source | Cron Expression | Config | API Key | Volume | Notes |
|--------|----------------|--------|---------|--------|-------|
| Census ACS | `0 8 2 1,4,7,10 *` | `{"survey": "acs5", "geo_level": "county"}` | Recommended | ~100K records | 5-year estimates |
| BEA Regional | `0 9 2 1,4,7,10 *` | `{"dataset": "regional"}` | Yes | ~50K records | Regional economic data |
| SEC 10-K/10-Q | `0 10 3 1,4,7,10 *` | `{"filing_type": "10-K,10-Q"}` | No | ~10K filings | Quarterly filings |
| SEC 13F Holdings | `0 11 3 1,4,7,10 *` | `{"filing_type": "13F"}` | No | ~50K holdings | Institutional holdings |
| USPTO Patents | `0 10 4 1,4,7,10 *` | `{}` | No | ~50K patents | Patent filings |
| US Trade | `0 11 4 1,4,7,10 *` | `{}` | No | ~20K records | Import/export data |
| BTS Transportation | `0 10 5 1,4,7,10 *` | `{}` | No | ~10K records | Transport statistics |
| International Econ | `0 11 5 1,4,7,10 *` | `{}` | No | ~20K records | World Bank/IMF/OECD |
| Real Estate (FHFA) | `0 10 6 1,4,7,10 *` | `{}` | No | ~5K records | House Price Index |
| OpenCorporates | `0 11 6 1,4,7,10 *` | `{}` | Yes | ~50K records | Company registry |
| USDA Agriculture | `0 10 7 1,4,7,10 *` | `{}` | No | ~10K records | Crop/livestock |

---

## Tier 5 — Quarterly Site Intel (weeks 1–2 of Jan/Apr/Jul/Oct, staggered)

| Domain | Cron Expression | Collectors | States | Est. Duration |
|--------|----------------|------------|--------|---------------|
| Power | `0 1 2 1,4,7,10 *` | EIA, HIFLD, NREL, ISOs | All 50 | ~2 hrs |
| Telecom | `0 1 3 1,4,7,10 *` | FCC, PeeringDB | All 50 | ~3 hrs |
| Transport | `0 1 4 1,4,7,10 *` | BTS, FRA, FAA, USACE | All 50 | ~2 hrs |
| Risk | `0 1 5 1,4,7,10 *` | FEMA, USGS, EPA Envirofacts | All 50 | ~4 hrs |
| Water Utilities | `0 1 6 1,4,7,10 *` | USGS Water, EPA SDWIS, EIA, OpenEI | All 50 | ~4 hrs |
| Incentives | `0 1 7 1,4,7,10 *` | CDFI OZ, FTZ Board, Good Jobs First | All 50 | ~1 hr |
| Logistics | `0 1 8 1,4,7,10 *` | FMCSA, 3PL, Census Trade, Port, Air Cargo | All 50 | ~3 hrs |
| Labor | `0 1 9 1,4,7,10 *` | BLS local stats | All 50 | ~2 hrs |

---

## Already Scheduled (No Changes Needed)

| System | Jobs | Current Schedule |
|--------|------|-----------------|
| People — Job Processor | Process pending jobs | Every 10 min |
| People — Website Refresh | 50 portfolio companies | Sundays 2 AM |
| People — SEC 8-K Check | 30 public companies | Weekdays 6 PM |
| People — News Scan | 50 companies | Daily 8 AM |
| People — Stuck Cleanup | Timeout check | Every 2 hrs |
| Agentic — Quarterly Refresh | All LP/FO investors | 1st of Jan/Apr/Jul/Oct 2 AM |
| Agentic — Weekly Stale Check | Stale investors (>180d) | Sundays 3 AM |
| Agentic — Queue Processor | Process refresh queue | Every 30 min |

---

## Incremental Strategy

| Pattern | Sources | Strategy |
|---------|---------|----------|
| **Date-range filter** | FRED, Treasury, BLS, BEA, EIA, SEC | Pass `start_date=last_run_at` in config |
| **Upsert-safe full refresh** | FEMA, FDIC, CMS, IRS, Census | Re-pull everything, upsert handles dedup |
| **Full refresh only** | Site intel, EPA, USGS | No incremental API — re-pull quarterly |
| **Already incremental** | People (daily 8-K), Agentic (stale check) | Already implemented |

### Notes on Incremental Updates

- Sources with date-range filters should use `last_run_at` from the schedule to limit API calls
- The scheduler tracks `last_run_at` per schedule and passes it in config for sources that support it
- Sources without incremental support use full refresh with `bulk_upsert()` (deduplication via unique constraints)
- For enrichment workflows, always use `null_preserving_upsert()` to avoid overwriting existing data with nulls

---

## Implementation Notes

1. **All new schedules start paused** (`is_active=False`) — activate via API
2. **QUARTERLY frequency** added to `ScheduleFrequency` enum
3. **Universal dispatcher** in `jobs.py` replaces the if/elif chain
4. **Site intel scheduler** runs quarterly domain-by-domain with 5-minute gaps
5. **Monthly logistics update** runs more frequently since freight rates change fast
