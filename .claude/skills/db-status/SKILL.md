---
name: db-status
description: Show a quick snapshot of database record counts across key tables. Use when the user wants to see database state, verify data was persisted, or check before/after a collection run.
allowed-tools:
  - Bash
argument-hint: "[table-name or domain]"
---

Query PostgreSQL for record counts across key Nexdata tables and present a summary.

## Behavior

1. If `$ARGUMENTS` specifies a domain or table, focus on that area. Otherwise show the full overview.

2. Run counts via psql against the database (host port 5434):
   ```bash
   docker exec nexdata-postgres-1 psql -U nexdata -d nexdata -c "QUERY"
   ```

3. **Core tables to check:**

   **People & Leadership:**
   - `people` — individual person records
   - `company_people` — person-to-company associations
   - `industrial_companies` — companies tracked
   - `org_chart_snapshots` — org chart data

   **PE Intelligence:**
   - `pe_firms` — PE/VC firms
   - `pe_people` / `pe_firm_people` — people at PE firms
   - `pe_deals` — deal records
   - `pe_deal_participants` — deal participant associations
   - `pe_portfolio_companies` — portfolio companies
   - `pe_fund_investments` — fund-to-company investment records
   - `pe_funds` — fund records
   - `pe_firm_news` — news items
   - `pe_company_financials` — portfolio company financials
   - `pe_company_leadership` — portfolio company leaders

   **Site Intelligence:**
   - `site_intel_collection_job` — site intel job records (no site_intel_results table)

   **Jobs & System:**
   - `ingestion_jobs` — total, by status (pending/running/success/failed)
   - `dataset_registry` — registered datasets

   **3PL / Logistics:**
   - `three_pl_company` — 3PL companies

4. Present results as a clean table. Include a "last updated" timestamp if the table has a `created_at` or `updated_at` column (use `MAX(updated_at)` or `MAX(created_at)`).

5. If a specific domain is requested (e.g., "pe" or "people"), show more detail for that domain — e.g., breakdowns by firm, by status, top firms by deal count, etc.

## Example output format

```
Table                    | Count  | Last Updated
-------------------------|--------|------------------
people                   | 1,247  | 2026-02-15 17:04
company_people           | 1,891  | 2026-02-15 17:04
industrial_companies     |   142  | 2026-02-14 18:36
pe_firms                 |    79  | 2026-02-15 17:04
pe_deals                 | 1,512  | 2026-02-15 17:04
...
```

## Important
- Database container is `nexdata-postgres-1`, user `nexdata`, database `nexdata`
- Some tables may not exist yet — handle "relation does not exist" errors gracefully
- Keep queries fast — COUNT(*) only, no full table scans with complex joins
