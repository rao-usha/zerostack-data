# Treasury FiscalData Quick Start Guide

## Overview

The Treasury FiscalData source provides access to U.S. Treasury fiscal and debt data from the official Treasury FiscalData API.

**API Documentation:** https://fiscaldata.treasury.gov/api-documentation/

**API Key:** ❌ NOT REQUIRED (1,000 requests per minute limit)

## Available Datasets

| Dataset | Table Name | Description |
|---------|------------|-------------|
| Debt Outstanding | `treasury_debt_outstanding` | Total public debt outstanding (Debt to the Penny) |
| Interest Rates | `treasury_interest_rates` | Average interest rates on Treasury securities |
| Monthly Statement | `treasury_monthly_statement` | Revenue and spending (Monthly Treasury Statement) |
| Auctions | `treasury_auctions` | Treasury securities auction results |
| Daily Balance | `treasury_daily_balance` | Daily Treasury statement (deposits/withdrawals) |

## Quick Start

### 1. Ingest Federal Debt Data

```bash
curl -X POST "http://localhost:8001/api/v1/treasury/debt/ingest" \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2020-01-01", "end_date": "2024-12-31"}'
```

### 2. Ingest Treasury Interest Rates

```bash
curl -X POST "http://localhost:8001/api/v1/treasury/interest-rates/ingest" \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2020-01-01", "end_date": "2024-12-31"}'
```

### 3. Ingest Revenue & Spending

```bash
curl -X POST "http://localhost:8001/api/v1/treasury/revenue-spending/ingest" \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2020-01-01", "end_date": "2024-12-31"}'
```

### 4. Ingest Auction Results

```bash
curl -X POST "http://localhost:8001/api/v1/treasury/auctions/ingest" \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2020-01-01", "end_date": "2024-12-31"}'
```

### 5. Ingest ALL Datasets

```bash
curl -X POST "http://localhost:8001/api/v1/treasury/all/ingest" \
  -H "Content-Type: application/json" \
  -d '{"start_date": "2020-01-01", "end_date": "2024-12-31"}'
```

## API Endpoints

### Ingestion Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/treasury/debt/ingest` | POST | Ingest debt outstanding data |
| `/api/v1/treasury/interest-rates/ingest` | POST | Ingest Treasury interest rates |
| `/api/v1/treasury/revenue-spending/ingest` | POST | Ingest monthly statement data |
| `/api/v1/treasury/auctions/ingest` | POST | Ingest auction results |
| `/api/v1/treasury/all/ingest` | POST | Ingest all datasets |

### Reference Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/v1/treasury/reference/datasets` | GET | List available datasets |
| `/api/v1/treasury/reference/security-types` | GET | List security types for rates/auctions |

## Request Parameters

### Common Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `start_date` | string | No | Start date (YYYY-MM-DD). Defaults to 5 years ago. |
| `end_date` | string | No | End date (YYYY-MM-DD). Defaults to today. |

### Interest Rates Additional Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `security_type` | string | No | Filter by security type |

**Security Types:**
- Treasury Bills
- Treasury Notes
- Treasury Bonds
- Treasury Inflation-Protected Securities (TIPS)
- Treasury Floating Rate Notes (FRN)

### Auctions Additional Parameters

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `security_type` | string | No | Filter by auction security type |

**Auction Security Types:**
- Bill (Treasury Bills)
- Note (Treasury Notes)
- Bond (Treasury Bonds)
- TIPS (Treasury Inflation-Protected Securities)
- FRN (Floating Rate Notes)
- CMB (Cash Management Bills)

## Database Schema

### treasury_debt_outstanding

```sql
SELECT record_date, tot_pub_debt_out_amt, debt_held_public_amt, intragov_hold_amt
FROM treasury_debt_outstanding
ORDER BY record_date DESC
LIMIT 10;
```

**Key Columns:**
- `record_date` - Date of the record
- `tot_pub_debt_out_amt` - Total public debt outstanding (in millions)
- `debt_held_public_amt` - Debt held by the public
- `intragov_hold_amt` - Intragovernmental holdings

### treasury_interest_rates

```sql
SELECT record_date, security_type_desc, security_desc, avg_interest_rate_amt
FROM treasury_interest_rates
WHERE security_type_desc = 'Treasury Notes'
ORDER BY record_date DESC
LIMIT 10;
```

**Key Columns:**
- `record_date` - Date of the record
- `security_type_desc` - Type of security (Bills, Notes, Bonds, etc.)
- `security_desc` - Specific security description
- `avg_interest_rate_amt` - Average interest rate

### treasury_monthly_statement

```sql
SELECT record_date, classification_desc, category_desc,
       current_month_net_rcpt_outly_amt,
       fiscal_year_to_date_net_rcpt_outly_amt
FROM treasury_monthly_statement
WHERE classification_desc = 'Receipts'
ORDER BY record_date DESC
LIMIT 10;
```

**Key Columns:**
- `record_date` - Date of the record
- `classification_desc` - Receipts or Outlays
- `category_desc` - Category of receipt/outlay
- `current_month_net_rcpt_outly_amt` - Current month amount
- `fiscal_year_to_date_net_rcpt_outly_amt` - YTD amount

### treasury_auctions

```sql
SELECT auction_date, security_type, security_term, cusip,
       high_investment_rate, bid_to_cover_ratio, total_accepted
FROM treasury_auctions
WHERE security_type = 'Note'
ORDER BY auction_date DESC
LIMIT 10;
```

**Key Columns:**
- `auction_date` - Date of the auction
- `security_type` - Type (Bill, Note, Bond, TIPS, FRN, CMB)
- `security_term` - Term (e.g., "10-Year", "3-Month")
- `cusip` - CUSIP identifier
- `high_investment_rate` - Winning rate
- `bid_to_cover_ratio` - Demand indicator
- `total_accepted` - Total amount accepted

## Example Queries

### Get Latest National Debt

```sql
SELECT 
    record_date,
    tot_pub_debt_out_amt / 1000000 AS debt_trillions,
    debt_held_public_amt / 1000000 AS public_debt_trillions,
    intragov_hold_amt / 1000000 AS intragov_trillions
FROM treasury_debt_outstanding
ORDER BY record_date DESC
LIMIT 1;
```

### Treasury Yield Curve

```sql
SELECT 
    record_date,
    security_desc,
    avg_interest_rate_amt
FROM treasury_interest_rates
WHERE record_date = (SELECT MAX(record_date) FROM treasury_interest_rates)
  AND security_type_desc = 'Treasury Notes'
ORDER BY security_desc;
```

### Monthly Deficit/Surplus

```sql
SELECT 
    record_date,
    SUM(CASE WHEN classification_desc = 'Receipts' THEN current_month_net_rcpt_outly_amt ELSE 0 END) AS receipts,
    SUM(CASE WHEN classification_desc = 'Outlays' THEN current_month_net_rcpt_outly_amt ELSE 0 END) AS outlays,
    SUM(CASE WHEN classification_desc = 'Receipts' THEN current_month_net_rcpt_outly_amt 
             WHEN classification_desc = 'Outlays' THEN -current_month_net_rcpt_outly_amt 
             ELSE 0 END) AS surplus_deficit
FROM treasury_monthly_statement
WHERE classification_desc IN ('Receipts', 'Outlays')
GROUP BY record_date
ORDER BY record_date DESC
LIMIT 12;
```

### Recent Auction Bid-to-Cover Ratios

```sql
SELECT 
    auction_date,
    security_type,
    security_term,
    bid_to_cover_ratio,
    high_investment_rate
FROM treasury_auctions
WHERE auction_date >= CURRENT_DATE - INTERVAL '30 days'
ORDER BY auction_date DESC;
```

## Use Cases

### 1. Federal Debt Tracking
Track the growth of the national debt over time, analyzing the breakdown between publicly held debt and intragovernmental holdings.

### 2. Interest Rate Analysis
Monitor Treasury rates across different maturities to analyze the yield curve and predict economic conditions.

### 3. Fiscal Policy Research
Analyze federal revenue and spending patterns to understand budget trends and fiscal policy impacts.

### 4. Fixed Income Trading
Use auction data to analyze demand for Treasury securities and predict future rate movements.

### 5. Economic Indicators
Combine Treasury data with other economic indicators for comprehensive macroeconomic analysis.

## Rate Limits

Treasury FiscalData API has generous rate limits:
- **1,000 requests per minute** without an API key
- No daily limits
- Up to 10,000 records per request

The Nexdata client uses conservative defaults:
- 5 concurrent requests
- Automatic pagination for large datasets
- Exponential backoff on errors

## Technical Implementation

### Source Files
- `app/sources/treasury/__init__.py` - Module initialization
- `app/sources/treasury/client.py` - API client with retry logic
- `app/sources/treasury/metadata.py` - Schema generation
- `app/sources/treasury/ingest.py` - Ingestion orchestration
- `app/api/v1/treasury.py` - FastAPI endpoints

### Key Features
- ✅ Typed columns (DATE, NUMERIC, TEXT) - no JSON blobs
- ✅ Job tracking via `ingestion_jobs` table
- ✅ Parameterized SQL queries only
- ✅ asyncio.Semaphore for bounded concurrency
- ✅ Exponential backoff with jitter for retries
- ✅ Idempotent table creation with ON CONFLICT handling

## Troubleshooting

### Common Issues

**Q: Ingestion seems slow**
A: Treasury data can have many records. The API returns up to 10,000 records per page, but large date ranges may require multiple pages.

**Q: Rate limit errors (429)**
A: The client automatically handles rate limits with exponential backoff. If you see persistent errors, reduce concurrency in your .env file.

**Q: Missing data for recent dates**
A: Some Treasury data (like monthly statements) is only updated monthly. Auction data may be delayed by a day or two.

## References

- [Treasury FiscalData API Documentation](https://fiscaldata.treasury.gov/api-documentation/)
- [Debt to the Penny Dataset](https://fiscaldata.treasury.gov/datasets/debt-to-the-penny/)
- [Average Interest Rates Dataset](https://fiscaldata.treasury.gov/datasets/average-interest-rates-treasury-securities/)
- [Monthly Treasury Statement](https://fiscaldata.treasury.gov/datasets/monthly-treasury-statement/)
- [Treasury Securities Auctions](https://fiscaldata.treasury.gov/datasets/treasury-securities-auctions-data/)
