# FDIC BankFind Suite - Quick Start Guide

## Overview

The FDIC BankFind Suite provides access to bank financial data, demographics, failed banks, and branch-level deposit data for all FDIC-insured U.S. banks (~4,700 active institutions).

**API Documentation:** https://banks.data.fdic.gov/docs/

**API Key:** ❌ NOT REQUIRED - All data is free and public!

## Available Datasets

| Dataset | Table | Description | Records |
|---------|-------|-------------|---------|
| Bank Financials | `fdic_bank_financials` | Balance sheets, income statements, 1,100+ metrics | Quarterly data |
| Institutions | `fdic_institutions` | Bank demographics, locations, charter info | ~4,700 banks |
| Failed Banks | `fdic_failed_banks` | Historical bank failures since 1934 | ~560 failures |
| Summary of Deposits | `fdic_summary_deposits` | Branch-level deposit data | ~85,000 branches/year |

## Quick Start

### 1. Ingest All Active Institutions

```bash
curl -X POST "http://localhost:8001/api/v1/fdic/institutions/ingest" \
  -H "Content-Type: application/json" \
  -d '{"active_only": true}'
```

### 2. Ingest Bank Financials (2023)

```bash
curl -X POST "http://localhost:8001/api/v1/fdic/financials/ingest" \
  -H "Content-Type: application/json" \
  -d '{"year": 2023}'
```

### 3. Ingest Failed Banks

```bash
curl -X POST "http://localhost:8001/api/v1/fdic/failed-banks/ingest" \
  -H "Content-Type: application/json" \
  -d '{}'
```

### 4. Search for Banks

```bash
curl "http://localhost:8001/api/v1/fdic/search?query=Chase"
```

## API Endpoints

### Ingestion Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/api/v1/fdic/financials/ingest` | Ingest bank financial data |
| POST | `/api/v1/fdic/institutions/ingest` | Ingest bank demographics |
| POST | `/api/v1/fdic/failed-banks/ingest` | Ingest failed banks list |
| POST | `/api/v1/fdic/deposits/ingest` | Ingest Summary of Deposits |
| POST | `/api/v1/fdic/all/ingest` | Ingest all datasets |

### Reference Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/fdic/reference/metrics` | Available financial metrics |
| GET | `/api/v1/fdic/reference/datasets` | Dataset information |
| GET | `/api/v1/fdic/reference/major-banks` | Major bank FDIC cert numbers |
| GET | `/api/v1/fdic/search` | Search banks by name/location |

## Ingestion Examples

### Ingest Specific Bank's Financials

```bash
# JPMorgan Chase (cert=628)
curl -X POST "http://localhost:8001/api/v1/fdic/financials/ingest" \
  -H "Content-Type: application/json" \
  -d '{"cert": 628}'
```

### Ingest California Banks Only

```bash
curl -X POST "http://localhost:8001/api/v1/fdic/institutions/ingest" \
  -H "Content-Type: application/json" \
  -d '{"state": "CA"}'
```

### Ingest 2008 Financial Crisis Failures

```bash
curl -X POST "http://localhost:8001/api/v1/fdic/failed-banks/ingest" \
  -H "Content-Type: application/json" \
  -d '{"year_start": 2008, "year_end": 2012}'
```

### Ingest Branch Deposits (⚠️ Large!)

```bash
# Filter by year and state to limit data
curl -X POST "http://localhost:8001/api/v1/fdic/deposits/ingest" \
  -H "Content-Type: application/json" \
  -d '{"year": 2023, "state": "NY"}'
```

## Major Bank FDIC Certificate Numbers

| Bank | FDIC Cert | Notes |
|------|-----------|-------|
| JPMorgan Chase Bank | 628 | Largest U.S. bank |
| Bank of America | 3510 | |
| Wells Fargo Bank | 3511 | |
| Citibank | 7213 | |
| U.S. Bank | 6548 | |
| PNC Bank | 6384 | |
| Truist Bank | 9846 | |
| Goldman Sachs Bank USA | 33124 | |
| TD Bank | 17100 | |
| Capital One | 33954 | |
| Silicon Valley Bank | 24735 | Failed March 2023 |
| Signature Bank | 57053 | Failed March 2023 |
| First Republic Bank | 59017 | Failed May 2023 |

## Key Financial Metrics

### Performance Ratios
- `ROA` - Return on Assets (%)
- `ROE` - Return on Equity (%)
- `NIM` - Net Interest Margin (%)
- `EEFFR` - Efficiency Ratio (%)

### Capital Ratios
- `RBC1RWAJ` - Tier 1 Risk-Based Capital Ratio (%)
- `RBCRWAJ` - Total Risk-Based Capital Ratio (%)
- `IDT1CER` - Tier 1 Leverage Ratio (%)

### Asset Quality
- `NCLNLSR` - Noncurrent Loans / Total Loans (%)
- `NTLNLSR` - Net Charge-offs / Total Loans (%)
- `LNRESNCR` - Loan Loss Reserve / Noncurrent Loans (%)

### Balance Sheet
- `ASSET` - Total Assets
- `DEP` - Total Deposits
- `EQ` - Total Equity Capital
- `LNLSNET` - Net Loans and Leases

### Income Statement
- `NETINC` - Net Income
- `INTINC` - Total Interest Income
- `INTEXP` - Total Interest Expense
- `NETII` - Net Interest Income

## Database Schema

### fdic_bank_financials

```sql
-- Key columns
cert INTEGER NOT NULL,           -- FDIC Certificate Number
name TEXT,                       -- Institution Name
repdte DATE NOT NULL,            -- Report Date

-- Balance Sheet
asset NUMERIC,                   -- Total Assets
lnlsnet NUMERIC,                 -- Net Loans and Leases
dep NUMERIC,                     -- Total Deposits
eq NUMERIC,                      -- Total Equity

-- Performance
netinc NUMERIC,                  -- Net Income
roa NUMERIC,                     -- Return on Assets (%)
roe NUMERIC,                     -- Return on Equity (%)
nim NUMERIC,                     -- Net Interest Margin (%)

-- Capital Ratios
rbc1rwaj NUMERIC,                -- Tier 1 Risk-Based Capital Ratio
rbcrwaj NUMERIC,                 -- Total Risk-Based Capital Ratio

-- Asset Quality
nclnlsr NUMERIC,                 -- Noncurrent Loans / Total Loans
lnatres NUMERIC,                 -- Loan Loss Allowance
```

### fdic_institutions

```sql
cert INTEGER NOT NULL UNIQUE,    -- FDIC Certificate Number
name TEXT NOT NULL,              -- Institution Name
active INTEGER,                  -- 1=Active, 0=Inactive
city TEXT,                       -- City
stalp TEXT,                      -- State (2-letter)
bkclass TEXT,                    -- Bank Class
charter TEXT,                    -- Charter Type
regagnt TEXT,                    -- Primary Regulator
asset NUMERIC,                   -- Total Assets
dep NUMERIC,                     -- Total Deposits
```

### fdic_failed_banks

```sql
cert INTEGER NOT NULL,           -- FDIC Certificate Number
name TEXT NOT NULL,              -- Institution Name
city TEXT,                       -- City
state TEXT,                      -- State
faildate DATE NOT NULL,          -- Failure Date
savession TEXT,                  -- Acquiring Institution
qbfasset NUMERIC,                -- Estimated Assets at Failure
qbfdep NUMERIC,                  -- Estimated Deposits at Failure
cost NUMERIC,                    -- Estimated Cost to FDIC
```

### fdic_summary_deposits

```sql
cert INTEGER NOT NULL,           -- FDIC Certificate Number
name TEXT,                       -- Institution Name
year INTEGER NOT NULL,           -- Report Year
brnum INTEGER,                   -- Branch Number
address TEXT,                    -- Branch Address
city TEXT,                       -- City
stalp TEXT,                      -- State
depsum NUMERIC,                  -- Branch Deposits
latitude NUMERIC,                -- Latitude
longitude NUMERIC,               -- Longitude
mainoff INTEGER,                 -- 1=Main Office, 0=Branch
```

## Example SQL Queries

### Top 10 Banks by Total Assets

```sql
SELECT cert, name, stalp, asset, dep, roa, roe
FROM fdic_bank_financials
WHERE repdte = (SELECT MAX(repdte) FROM fdic_bank_financials)
ORDER BY asset DESC
LIMIT 10;
```

### Banks with High NPL Ratios (Stress Indicator)

```sql
SELECT cert, name, stalp, asset, nclnlsr, lnresncr
FROM fdic_bank_financials
WHERE repdte = (SELECT MAX(repdte) FROM fdic_bank_financials)
  AND nclnlsr > 2.0  -- NPL > 2%
ORDER BY nclnlsr DESC;
```

### Bank Failures by Year

```sql
SELECT EXTRACT(YEAR FROM faildate) as year, 
       COUNT(*) as failures,
       SUM(qbfasset) as total_assets,
       SUM(cost) as total_cost
FROM fdic_failed_banks
GROUP BY year
ORDER BY year DESC;
```

### Market Share by State (Deposits)

```sql
SELECT stalp, 
       COUNT(DISTINCT cert) as banks,
       SUM(depsum) as total_deposits
FROM fdic_summary_deposits
WHERE year = 2023
GROUP BY stalp
ORDER BY total_deposits DESC;
```

### Active vs Failed Institutions

```sql
-- Count active banks
SELECT COUNT(*) as active_banks FROM fdic_institutions WHERE active = 1;

-- Count failures by decade
SELECT 
  FLOOR(EXTRACT(YEAR FROM faildate) / 10) * 10 as decade,
  COUNT(*) as failures
FROM fdic_failed_banks
GROUP BY decade
ORDER BY decade;
```

## Use Cases

### 1. Bank Risk Monitoring

Monitor key risk indicators:
- Capital ratios (Tier 1, leverage)
- Asset quality (NPL, charge-offs)
- Liquidity (loans/deposits ratio)
- Profitability trends

### 2. Market Research

- Bank footprint analysis by geography
- Deposit market share
- Branch network mapping
- Competitive analysis

### 3. Crisis Early Warning

- Track banks with deteriorating metrics
- Monitor failure trends
- Identify concentrated exposures

### 4. Investment Research

- Bank stock analysis
- Credit risk assessment
- M&A target identification

## Notes

### Data Freshness
- Financial data is quarterly (Mar 31, Jun 30, Sep 30, Dec 31)
- Institutions data is updated continuously
- Failed banks list is updated as failures occur
- Summary of Deposits is annual (June 30)

### Pagination
- FDIC API returns max 10,000 records per request
- Our client handles pagination automatically
- Large datasets (SOD) may take several minutes

### Rate Limits
- No official rate limits documented
- We use conservative defaults (5 requests/second)
- Bounded concurrency with semaphores

## Troubleshooting

### Empty Results
- Check filters (date ranges, cert numbers)
- Verify FDIC cert exists: `/api/v1/fdic/search?query=BankName`

### Slow Ingestion
- Summary of Deposits is very large (~85K+ branches)
- Use filters: `year`, `state`, `cert`

### Missing Data
- Some fields may be null for certain banks
- Historical data availability varies
