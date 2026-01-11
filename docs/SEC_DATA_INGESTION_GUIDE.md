# SEC Data Ingestion Guide - Getting Structured Financial Data

## Overview

The SEC implementation now includes **comprehensive financial data extraction** that breaks down SEC filings into structured, queryable data types. This goes beyond basic filing metadata to extract actual financial statements.

## What Data Types Are Available

### 1. Filing Metadata
**Tables:** `sec_10k`, `sec_10q`, `sec_8k`, etc.

Basic information about each filing:
- Filing date
- Report date  
- Company information
- Links to full filing documents

### 2. Financial Facts (XBRL)
**Table:** `sec_financial_facts`

Individual financial metrics from XBRL data:
- Every line item reported (Assets, Revenues, Net Income, etc.)
- Timestamped by fiscal period
- Includes units (USD, shares, etc.)
- Linked to source filing

### 3. Income Statements
**Table:** `sec_income_statement`

Normalized income statement data:
- Revenues
- Cost of Revenue / Gross Profit
- Operating Expenses (R&D, SG&A)
- Operating Income
- Interest Income/Expense
- Net Income
- Earnings Per Share (EPS)
- Weighted Average Shares

### 4. Balance Sheets
**Table:** `sec_balance_sheet`

Normalized balance sheet data:
- **Assets:** Cash, Investments, Receivables, Inventory, PP&E, Goodwill
- **Liabilities:** Payables, Short-term Debt, Long-term Debt
- **Equity:** Common Stock, Retained Earnings, Treasury Stock
- Total Assets / Liabilities / Equity

### 5. Cash Flow Statements
**Table:** `sec_cash_flow_statement`

Normalized cash flow data:
- **Operating Activities:** Net Income, Depreciation, Working Capital changes
- **Investing Activities:** CapEx, Acquisitions, Investment purchases/sales
- **Financing Activities:** Debt issued/repaid, Dividends, Stock repurchases
- Free Cash Flow (calculated)

---

## How to Ingest Data

### Option 1: Full Company Ingestion (Recommended)

Ingests **both** filings metadata AND financial data:

```bash
curl -X POST "http://localhost:8001/api/v1/sec/ingest/full-company" \
  -H "Content-Type: application/json" \
  -d '{
    "cik": "0000320193",
    "filing_types": ["10-K", "10-Q"],
    "start_date": "2020-01-01",
    "end_date": "2024-12-31"
  }'
```

**This creates 2 jobs:**
1. Filing metadata ingestion → Populates `sec_10k`, `sec_10q`, etc.
2. Financial data ingestion → Populates `sec_financial_facts`, `sec_income_statement`, etc.

### Option 2: Financial Data Only

If you just want structured financial data:

```bash
curl -X POST "http://localhost:8001/api/v1/sec/ingest/financial-data" \
  -H "Content-Type: application/json" \
  -d '{
    "cik": "0000320193"
  }'
```

### Option 3: Filing Metadata Only

If you just want filing metadata:

```bash
curl -X POST "http://localhost:8001/api/v1/sec/ingest/company" \
  -H "Content-Type: application/json" \
  -d '{
    "cik": "0000320193",
    "filing_types": ["10-K", "10-Q"],
    "start_date": "2020-01-01",
    "end_date": "2024-12-31"
  }'
```

---

## Start Ingesting: Major Companies

### Tech Companies

```bash
# Apple (AAPL)
curl -X POST "http://localhost:8001/api/v1/sec/ingest/full-company" \
  -H "Content-Type: application/json" \
  -d '{"cik": "0000320193", "filing_types": ["10-K", "10-Q"]}'

# Microsoft (MSFT)
curl -X POST "http://localhost:8001/api/v1/sec/ingest/full-company" \
  -H "Content-Type: application/json" \
  -d '{"cik": "0000789019", "filing_types": ["10-K", "10-Q"]}'

# Google (GOOGL)
curl -X POST "http://localhost:8001/api/v1/sec/ingest/full-company" \
  -H "Content-Type: application/json" \
  -d '{"cik": "0001652044", "filing_types": ["10-K", "10-Q"]}'

# Amazon (AMZN)
curl -X POST "http://localhost:8001/api/v1/sec/ingest/full-company" \
  -H "Content-Type: application/json" \
  -d '{"cik": "0001018724", "filing_types": ["10-K", "10-Q"]}'

# Tesla (TSLA)
curl -X POST "http://localhost:8001/api/v1/sec/ingest/full-company" \
  -H "Content-Type: application/json" \
  -d '{"cik": "0001318605", "filing_types": ["10-K", "10-Q"]}'

# NVIDIA (NVDA)
curl -X POST "http://localhost:8001/api/v1/sec/ingest/full-company" \
  -H "Content-Type: application/json" \
  -d '{"cik": "0001045810", "filing_types": ["10-K", "10-Q"]}'
```

### Financial Services

```bash
# JPMorgan Chase (JPM)
curl -X POST "http://localhost:8001/api/v1/sec/ingest/full-company" \
  -H "Content-Type: application/json" \
  -d '{"cik": "0000019617", "filing_types": ["10-K", "10-Q"]}'

# Bank of America (BAC)
curl -X POST "http://localhost:8001/api/v1/sec/ingest/full-company" \
  -H "Content-Type: application/json" \
  -d '{"cik": "0000070858", "filing_types": ["10-K", "10-Q"]}'

# Goldman Sachs (GS)
curl -X POST "http://localhost:8001/api/v1/sec/ingest/full-company" \
  -H "Content-Type: application/json" \
  -d '{"cik": "0000886982", "filing_types": ["10-K", "10-Q"]}'

# Wells Fargo (WFC)
curl -X POST "http://localhost:8001/api/v1/sec/ingest/full-company" \
  -H "Content-Type: application/json" \
  -d '{"cik": "0000072971", "filing_types": ["10-K", "10-Q"]}'
```

---

## Query the Financial Data

Once ingested, you can query the structured financial data:

### Example 1: Apple's Income Statement Trend

```sql
SELECT 
    fiscal_year,
    fiscal_period,
    revenues / 1e9 as revenues_billions,
    net_income / 1e9 as net_income_billions,
    (net_income / revenues * 100)::numeric(5,2) as profit_margin_pct,
    earnings_per_share_diluted as eps
FROM sec_income_statement
WHERE cik = '0000320193'
  AND fiscal_period IN ('Q1', 'Q2', 'Q3', 'Q4', 'FY')
ORDER BY fiscal_year DESC, fiscal_period DESC
LIMIT 20;
```

###Example 2: Balance Sheet Comparison Across Tech Giants

```sql
SELECT 
    company_name,
    period_end_date,
    total_assets / 1e9 as total_assets_billions,
    cash_and_equivalents / 1e9 as cash_billions,
    total_liabilities / 1e9 as total_liabilities_billions,
    stockholders_equity / 1e9 as equity_billions,
    (stockholders_equity / total_assets * 100)::numeric(5,2) as equity_ratio_pct
FROM sec_balance_sheet
WHERE cik IN ('0000320193', '0000789019', '0001652044')  -- Apple, Microsoft, Google
  AND fiscal_period = 'FY'
  AND fiscal_year >= 2020
ORDER BY company_name, fiscal_year DESC;
```

### Example 3: Free Cash Flow Analysis

```sql
SELECT 
    company_name,
    fiscal_year,
    cash_from_operations / 1e9 as operating_cf_billions,
    capital_expenditures / 1e9 as capex_billions,
    free_cash_flow / 1e9 as fcf_billions,
    dividends_paid / 1e9 as dividends_billions,
    stock_repurchased / 1e9 as buybacks_billions
FROM sec_cash_flow_statement
WHERE cik = '0000320193'
  AND fiscal_period = 'FY'
ORDER BY fiscal_year DESC
LIMIT 10;
```

### Example 4: Quarterly Revenue Growth

```sql
WITH quarterly_revenues AS (
    SELECT 
        company_name,
        fiscal_year,
        fiscal_period,
        revenues,
        LAG(revenues) OVER (ORDER BY fiscal_year, fiscal_period) as prev_quarter_revenue
    FROM sec_income_statement
    WHERE cik = '0000320193'
      AND fiscal_period IN ('Q1', 'Q2', 'Q3', 'Q4')
)
SELECT 
    fiscal_year,
    fiscal_period,
    revenues / 1e9 as revenue_billions,
    ((revenues - prev_quarter_revenue) / prev_quarter_revenue * 100)::numeric(5,2) as qoq_growth_pct
FROM quarterly_revenues
WHERE prev_quarter_revenue IS NOT NULL
ORDER BY fiscal_year DESC, fiscal_period DESC
LIMIT 12;
```

### Example 5: Find All Financial Metrics for a Period

```sql
SELECT 
    fact_name,
    fact_label,
    value,
    unit,
    fiscal_period
FROM sec_financial_facts
WHERE cik = '0000320193'
  AND fiscal_year = 2023
  AND fiscal_period = 'FY'
  AND namespace = 'us-gaap'
ORDER BY fact_name
LIMIT 100;
```

---

## Database Schema Details

### Financial Facts Table

```sql
CREATE TABLE sec_financial_facts (
    id SERIAL PRIMARY KEY,
    cik VARCHAR(10),
    company_name TEXT,
    fact_name TEXT,        -- e.g., "Assets", "Revenues"
    fact_label TEXT,       -- Human-readable label
    namespace VARCHAR(50), -- e.g., "us-gaap"
    value DECIMAL(20,2),   -- Numeric value
    unit VARCHAR(20),      -- e.g., "USD", "shares"
    period_end_date DATE,
    fiscal_year INTEGER,
    fiscal_period VARCHAR(10),  -- Q1, Q2, Q3, Q4, FY
    form_type VARCHAR(20),
    accession_number VARCHAR(20),
    filing_date DATE
);
```

### Income Statement Table

```sql
CREATE TABLE sec_income_statement (
    id SERIAL PRIMARY KEY,
    cik VARCHAR(10),
    company_name TEXT,
    ticker VARCHAR(10),
    period_end_date DATE,
    fiscal_year INTEGER,
    fiscal_period VARCHAR(10),
    
    -- Income statement line items
    revenues DECIMAL(20,2),
    cost_of_revenue DECIMAL(20,2),
    gross_profit DECIMAL(20,2),
    operating_expenses DECIMAL(20,2),
    research_and_development DECIMAL(20,2),
    selling_general_administrative DECIMAL(20,2),
    operating_income DECIMAL(20,2),
    interest_expense DECIMAL(20,2),
    net_income DECIMAL(20,2),
    earnings_per_share_basic DECIMAL(10,4),
    earnings_per_share_diluted DECIMAL(10,4),
    ...
);
```

---

## Check Ingestion Status

```bash
# Check job status
curl "http://localhost:8001/api/v1/jobs/{job_id}"

# Check all SEC jobs
curl "http://localhost:8001/api/v1/jobs?source=sec"
```

Or query the database directly:

```sql
-- Check recent SEC jobs
SELECT 
    id,
    config->>'type' as job_type,
    config->>'cik' as cik,
    status,
    rows_inserted,
    created_at,
    completed_at
FROM ingestion_jobs
WHERE source = 'sec'
ORDER BY created_at DESC
LIMIT 20;
```

---

## Batch Ingestion Script

If you want to ingest multiple companies programmatically:

```bash
#!/bin/bash

# Major tech companies
for cik in "0000320193" "0000789019" "0001652044" "0001018724" "0001318605" "0001045810"
do
    echo "Ingesting CIK: $cik"
    curl -X POST "http://localhost:8001/api/v1/sec/ingest/full-company" \
      -H "Content-Type: application/json" \
      -d "{\"cik\": \"$cik\", \"filing_types\": [\"10-K\", \"10-Q\"]}"
    echo ""
    sleep 2  # Be nice to the server
done

echo "All companies queued for ingestion!"
```

---

## Data Update Frequency

To keep data current:

1. **Annual Reports (10-K)**: Ingest once per year after filing deadline
2. **Quarterly Reports (10-Q)**: Ingest quarterly (45 days after quarter end)
3. **Material Events (8-K)**: Ingest as needed or set up daily/weekly refresh

### Automated Updates

Create a cron job or scheduled task:

```bash
# Daily at 6 AM - check for new filings
0 6 * * * /path/to/update_sec_filings.sh

# Where update_sec_filings.sh contains:
#!/bin/bash
for cik in $(cat /path/to/tracked_companies.txt); do
    curl -X POST "http://localhost:8001/api/v1/sec/ingest/full-company" \
      -H "Content-Type: application/json" \
      -d "{\"cik\": \"$cik\"}"
done
```

---

## Performance Notes

**Ingestion Speed:**
- Filing metadata: ~5-10 seconds per company (last 5 years)
- Financial data (XBRL): ~10-20 seconds per company (all available quarters)

**Rate Limits:**
- SEC enforces 10 requests/second
- Service uses 8 req/sec to be conservative
- For multiple companies, ingestion runs sequentially to respect limits

**Storage:**
- Filing metadata: ~100-500 rows per company (5 years of 10-K + 10-Q)
- Financial facts: ~5,000-20,000 rows per company (all metrics, all periods)
- Income statements: ~20-40 rows per company (quarterly + annual for 5+ years)
- Balance sheets: ~20-40 rows per company
- Cash flow statements: ~20-40 rows per company

---

##Usage Summary

1. **Start the service:**
   ```bash
   docker-compose up -d
   ```

2. **Ingest companies:**
   ```bash
   # Run the batch script above, or use API calls
   ```

3. **Query the data:**
   ```sql
   -- Use the SQL examples above
   ```

4. **Build analyses:**
   - Revenue trends
   - Profitability metrics
   - Balance sheet health
   - Cash flow analysis
   - Cross-company comparisons
   - Time-series analysis

---

## Next Steps

1. ✅ Ingest your target companies using the API
2. ✅ Query the structured financial data
3. ✅ Build dashboards/reports using the normalized tables
4. ✅ Update `SEC_COMPANIES_TRACKING.md` with what you've ingested
5. ✅ Set up automated refreshes for ongoing updates

The financial data is **normalized and ready for analysis** - no need to parse HTML or XML filings manually!

