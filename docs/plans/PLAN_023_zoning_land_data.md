# PLAN 023 — Populate Zoning & Land Data Tables

**Status: PHASES 1-4 COMPLETE — Phase 5 pending**

## Checklist

### 1. Brownfield Sites (`brownfield_site`)
- [x] Collector exists: `EPAACRESCollector` in `app/sources/site_intel/risk/epa_acres_collector.py`
- [x] Run collector — 45,204 rows inserted
- [x] Verify rows inserted

### 2. Industrial Sites (`industrial_site`)
- [x] Collector exists: `StateEDOSitesCollector` in `app/sources/site_intel/incentives/state_edo_sites_collector.py`
- [x] Fixed missing unique constraint `(site_name, state)`
- [x] Run collector — 10 rows inserted
- [x] Verify rows inserted

### 3. Wetlands (`wetland`)
- [x] Collector created: `NWIWetlandCollector` in `app/sources/site_intel/risk/nwi_wetland_collector.py`
  - Uses ArcGIS REST `outStatistics` for server-side aggregation (not individual polygons)
  - Auto-subdivides bbox into quadrants (skips full-bbox to avoid 504 timeouts)
  - Fast 504 handling (no retry on gateway timeout)
- [x] Enum `SiteIntelSource.USFWS_NWI` already in `types.py`
- [x] Registered in `app/sources/site_intel/risk/__init__.py`
- [x] Fixed missing unique constraint `(nwi_code, state)`
- [x] Run nationally — 39,499 rows inserted across all 51 states (~4 hrs)
- [x] Verify rows inserted

### 4. Zoning Districts (`zoning_district`)
- [x] Collector created: `NZAZoningCollector` in `app/sources/site_intel/incentives/nza_zoning_collector.py`
  - Source: National Zoning Atlas via Mercatus Center ZIP downloads
  - Coverage: CO, HI, MT, NH, TN, TX, VA (7 states available)
- [x] Enum `SiteIntelSource.NATIONAL_ZONING_ATLAS` already in `types.py`
- [x] Fixed missing unique constraint `(jurisdiction, state, zone_code)`
- [x] Run all available states — 3,531 rows inserted (6 states)
- [x] Verify rows inserted

### 5. Re-score & Regenerate
- [ ] Re-score all counties with `force=true`
- [ ] Regenerate TX report — verify Section 7 has data
- [ ] Regenerate national report
- [ ] Confirm 14/14 sections populated
