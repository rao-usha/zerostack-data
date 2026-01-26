# Running Background Tasks

**Last Updated:** 2026-01-26 02:50 UTC

## Active Running Jobs

| Job ID | Status | Source | Target | Progress | Successful |
|--------|--------|--------|--------|----------|------------|
| 18 | running | website | all | 345/457 (75%) | 182 |
| 26 | running | governance, performance | all | 82/564 (15%) | 0 (needs debugging) |
| 30 | running | news | all | 471/564 (84%) | 228 |
| 34 | running | cafr | public_pension | 83/228 (36%) | 6 |
| 36 | running | website | insurance | 28/36 (78%) | 14 |
| 38 | running | website, news | sovereign_wealth | 0/43 (0%) | 0 |

## Pending Jobs (Will Start When Resources Free)

| Job ID | Source | Target |
|--------|--------|--------|
| 19 | website | all |
| 21 | form_990 | endowment, foundation |
| 23 | website | all |
| 25 | governance, performance | all |
| 27 | form_990 | endowment, foundation |
| 29 | news | all |
| 31 | website | asia, europe |
| 33 | cafr | public_pension |
| 35 | website | insurance |
| 37 | website, news | sovereign_wealth |

## Background Tasks (Claude CLI)

| Task ID | Description | Output File |
|---------|-------------|-------------|
| b3477d7 | FO website + news collection | `C:\Users\awron\AppData\Local\Temp\claude\...\b3477d7.output` |
| be43acd | Asia region collection | `C:\Users\awron\AppData\Local\Temp\claude\...\be43acd.output` |

## How to Monitor

```bash
# Check all running jobs
docker-compose exec postgres psql -U nexdata -d nexdata -c \
  "SELECT id, status, completed_lps, total_lps, successful_lps, sources
   FROM lp_collection_jobs WHERE status = 'running' ORDER BY id;"

# Check data counts
docker-compose exec postgres psql -U nexdata -d nexdata -c \
  "SELECT 'contacts', COUNT(*) FROM lp_key_contact
   UNION ALL SELECT 'governance', COUNT(*) FROM lp_governance_member
   UNION ALL SELECT 'lps_collected', (SELECT COUNT(*) FROM lp_fund WHERE last_collection_at IS NOT NULL);"

# Check coverage
curl -s "http://localhost:8001/api/v1/lp-collection/coverage" | python -m json.tool

# Tail background task output
tail -f "C:\Users\awron\AppData\Local\Temp\claude\C--Users-awron-projects-Nexdata\tasks\b3477d7.output"
```

## Current Data Snapshot

| Metric | Count | Target |
|--------|-------|--------|
| LP Contacts | 3,173 | 5,000+ |
| Governance Members | 28 | 200+ |
| LPs Collected | 396/564 (70%) | 500+ |
| 13F Holdings | 0 | 10,000+ |
| Family Offices | 308 | 308 |
| FO Contacts | 0 | 500+ |
| FO Investments | 0 | 500+ |

## Coverage by Region

| Region | Collected | Total | Coverage |
|--------|-----------|-------|----------|
| US | 294 | 377 | 78.0% |
| Europe | 44 | 79 | 55.7% |
| North America | 18 | 22 | 81.8% |
| Asia | 12 | 36 | 33.3% |
| Middle East | 7 | 11 | 63.6% |
| Oceania | 6 | 9 | 66.7% |
| Africa | 4 | 5 | 80.0% |
| South America | 4 | 11 | 36.4% |

## Coverage by Type

| Type | Collected | Total | Coverage |
|------|-----------|-------|----------|
| Endowment | 75 | 94 | 79.8% |
| Corporate Pension | 69 | 95 | 72.6% |
| Public Pension | 153 | 228 | 67.1% |
| Sovereign Wealth | 24 | 43 | 55.8% |
| Insurance | 19 | 36 | 52.8% |
| Foundation | 56 | 67 | 83.6% |

## Notes

- Job 26 (governance/performance): Currently 0% success rate - may need debugging
- CAFR collection requires LLM for PDF parsing - slower but higher quality data
- FO deals collector built but not persisting to database yet (returns items only)
