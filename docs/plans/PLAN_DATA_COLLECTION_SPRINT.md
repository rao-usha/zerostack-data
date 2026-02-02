# Plan: Data Collection Sprint

**Status:** ACTIVE - BUGS FIXED
**Date:** 2026-01-25 (Updated: 2026-02-02)
**Goal:** Run existing collectors to populate comprehensive investor data

---

## Sprint Results (2026-02-02)

| Metric | Before | After | Target | Notes |
|--------|--------|-------|--------|-------|
| Total LPs | 564 | 564 | - | Registry complete |
| LPs Collected Today | 0 | 17 | 50+ | Individual collection works |
| Endowments with Data | ~50 | 75 | 94 | 80% coverage |
| Public Pensions with Data | ~100 | 153 | 228 | 67% coverage |
| LP Key Contacts | 3,173 | 3,173 | 4,000+ | Good baseline |
| Governance Members | 35 | 38 | 200+ | Needs more collection |
| Form 990 Successes | 0 | 87 | 150+ | Working well |
| CAFR Successes | 0 | 10 | 200+ | Needs improvement |
| **SEC 13F Holdings** | 4,334 | 4,359 | 8,000+ | **FIXED** - Now working |

### Bugs Fixed (2026-02-02)

1. ✅ **SEC 13F XML Parsing** - Fixed regex pattern to find infotable files with numeric names (e.g., `46994.xml`)
   - Updated `_fetch_infotable_from_index()` in `sec_13f_source.py`
   - Fixed XML namespace handling in `_parse_13f_xml()`
   - Tested: CalPERS now returns 343 holdings, Gates Foundation returns 97 holdings

2. ✅ **FO Persistence Bug** - Fixed "property 'items_new' has no setter" error
   - Removed direct assignment to computed property
   - Set `item.is_new` in `_persist_deal()` and `_persist_contact()` methods

### Remaining Issues

- ⚠️ **Batch Job Timeouts** - Large batch jobs overwhelm API
  - **Recommendation**: Use individual LP collection instead of batch jobs
- ⚠️ **Many LPs don't file 13F** - Not all pensions/endowments file 13F forms

### What Works

- ✅ Form 990 collection (ProPublica API)
- ✅ Website collection for contacts
- ✅ Governance collection for board members
- ✅ Individual LP collection via `/collect/{lp_id}`
- ✅ **SEC 13F holdings extraction** (FIXED)
- ✅ **FO collection persistence** (FIXED)

---

## Original Plan

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

**Last completed:** 2026-02-02 Bug fixes
**Results:**
- ✅ SEC 13F XML parsing FIXED - CalPERS: 343 holdings, Gates Foundation: 97 holdings
- ✅ FO persistence bug FIXED - No more "items_new setter" error
- Form 990 collection working (87 successes, 48 items)
- CAFR collection partially working (10 successes)
- Total 13F holdings: 4,359 from 10 LPs

**Next action:**
1. Run SEC 13F collection on more LPs with known CIKs
2. Run batch Form 990 collection for endowments/foundations
3. Continue individual collection for high-value LPs

**To continue collection:**
```bash
# Collect individual LP (recommended approach)
curl -X POST "http://localhost:8001/api/v1/lp-collection/collect/{lp_id}" \
  -H "Content-Type: application/json" \
  -d '{"sources": ["form_990", "website", "governance"]}'
```
