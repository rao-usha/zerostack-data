# Plan: Data Collection Sprint

**Status:** IN PROGRESS
**Date:** 2026-01-25
**Goal:** Run existing collectors to populate comprehensive investor data

---

## Current State

| Metric | Current | After Sprint |
|--------|---------|--------------|
| Portfolio Holdings | 5,236 | 10,000+ |
| Form 990 Data | 0 | 150+ orgs |
| CAFR Data | 249 | 500+ records |
| LP Allocation History | 0 | 200+ records |
| FO Investments | 0 | 500+ deals |
| FO Websites | 28% | 60%+ |

---

## Phase 1: Run SEC 13F on More LPs (Priority 1)

**Target:** Expand 13F holdings from 4,291 to 8,000+

LPs with known CIKs that likely haven't been collected:
- US Public Pensions with CIKs (30+ funds)
- Endowments with CIKs (Yale, Harvard, Stanford, etc.)
- Sovereign Wealth Funds (GIC, Norges Bank)

**API Call:**
```bash
curl -X POST "http://localhost:8001/api/v1/lp-collection/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "lp_types": ["public_pension", "endowment", "sovereign_wealth"],
    "sources": ["sec_13f"],
    "mode": "full",
    "max_concurrent_lps": 3
  }'
```

---

## Phase 2: Run Form 990 on Endowments/Foundations (Priority 1)

**Target:** 150+ organizations with IRS 990 data

Categories:
- 82 University Endowments
- 68 Foundations

**API Call:**
```bash
curl -X POST "http://localhost:8001/api/v1/lp-collection/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "lp_types": ["endowment", "foundation"],
    "sources": ["form_990"],
    "mode": "full",
    "max_concurrent_lps": 5
  }'
```

---

## Phase 3: Run CAFR Parser on Public Pensions (Priority 1)

**Target:** 200 public pensions with CAFR data

This uses LLM to extract:
- Asset allocation (current and target)
- Performance returns (1/3/5/10 year)
- Manager list
- Fund values

**API Call:**
```bash
curl -X POST "http://localhost:8001/api/v1/lp-collection/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "lp_types": ["public_pension"],
    "sources": ["cafr"],
    "mode": "full",
    "max_concurrent_lps": 2
  }'
```

---

## Phase 4: Run Website Collector on All LPs (Priority 2)

**Target:** Refresh website data for governance, contacts, performance

```bash
curl -X POST "http://localhost:8001/api/v1/lp-collection/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "sources": ["website", "governance", "performance"],
    "mode": "incremental",
    "max_age_days": 30,
    "max_concurrent_lps": 5
  }'
```

---

## Phase 5: Family Office Data Collection (Priority 2)

### 5.1 Run Website Collector on FOs
```bash
curl -X POST "http://localhost:8001/api/v1/fo-collection/jobs" \
  -H "Content-Type: application/json" \
  -d '{
    "sources": ["website"],
    "mode": "full",
    "max_concurrent_fos": 5
  }'
```

### 5.2 Build Crunchbase/Deals Collector
Need to create: `app/sources/family_office_collection/deals_source.py`
- Collect recent investments from news
- Parse Crunchbase data (if API available)
- Track deal activity

---

## Phase 6: Fill Data Gaps (Priority 3)

After collection runs complete:
1. Query for LPs missing AUM → run targeted website collection
2. Query for LPs missing allocation → run CAFR collection
3. Query for FOs missing websites → web search to find URLs

---

## Execution Order

1. [ ] Start 13F collection job (background)
2. [ ] Start Form 990 collection job (background)
3. [ ] Start CAFR collection job (background)
4. [ ] Monitor jobs via `/api/v1/lp-collection/jobs/{id}`
5. [ ] Run website collection (incremental)
6. [ ] Build FO deals collector
7. [ ] Run FO collection jobs
8. [ ] Generate coverage report

---

## Success Metrics

| Metric | Target | Query |
|--------|--------|-------|
| Portfolio Holdings | 10,000+ | `SELECT COUNT(*) FROM portfolio_companies` |
| LPs with AUM | 500+ | `SELECT COUNT(*) FROM lp_fund WHERE aum_usd_billions IS NOT NULL` |
| LPs with Allocation | 200+ | `SELECT COUNT(DISTINCT lp_id) FROM lp_allocation_history` |
| LPs with Performance | 150+ | `SELECT COUNT(DISTINCT lp_id) FROM lp_performance_return` |
| FOs with Investments | 100+ | `SELECT COUNT(DISTINCT family_office_id) FROM family_office_investment` |

---

## Checkpoint

**Last completed:** Plan created
**Next action:** Start Phase 1 - SEC 13F collection job
**Resume:** Run curl commands to start collection jobs
