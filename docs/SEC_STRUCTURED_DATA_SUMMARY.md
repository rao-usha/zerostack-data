# SEC Structured Financial Data - Implementation Complete

## ✅ What Was Built

I've enhanced the SEC EDGAR implementation to break down filings into logical, structured data types that can be queried and analyzed.

### 1. Enhanced Database Schema

**New Tables Created:**

- **`sec_financial_facts`** - Every individual XBRL fact (all financial metrics)
- **`sec_income_statement`** - Normalized income statement data
- **`sec_balance_sheet`** - Normalized balance sheet data
- **`sec_cash_flow_statement`** - Normalized cash flow data
- **`sec_filing_sections`** - Text sections from filings (ready for future use)

### 2. XBRL Parser

**File:** `app/sources/sec/xbrl_parser.py`

Parses SEC's Company Facts API (XBRL JSON data) into structured financial statements:
- Maps XBRL taxonomy to standardized column names
- Handles multiple accounting standards (US-GAAP, etc.)
- Extracts fiscal periods (Q1, Q2, Q3, Q4, FY)
- Calculates derived metrics (e.g., Free Cash Flow)

### 3. Enhanced Ingestion Logic

**File:** `app/sources/sec/ingest_xbrl.py`

- Fetches XBRL data from `/api/xbrl/companyfacts/CIK{cik}.json`
- Parses all financial facts
- Builds normalized financial statements
- Stores in typed database columns (no JSON blobs)
- Full job tracking and error handling

### 4. New API Endpoints

#### `/api/v1/sec/ingest/financial-data`
Ingest structured financial data (XBRL) for a company.

#### `/api/v1/sec/ingest/full-company`
Comprehensive ingestion: both filings AND financial data in one call.

---

## 📊 Logical Data Types Breakdown

### Type 1: Filing Metadata
**What:** Basic information about each SEC filing  
**Tables:** `sec_10k`, `sec_10q`, `sec_8k`, etc.  
**Use Cases:** Track filing dates, find specific documents, link to full filings

**Example Query:**
```sql
SELECT filing_date, filing_url 
FROM sec_10k 
WHERE cik = '0000320193' 
ORDER BY filing_date DESC;
```

### Type 2: All Financial Facts (Granular)
**What:** Every individual financial metric from XBRL  
**Table:** `sec_financial_facts`  
**Use Cases:** Deep-dive analysis, custom metric extraction, data science

**Example Query:**
```sql
SELECT fact_name, value, unit, fiscal_period
FROM sec_financial_facts
WHERE cik = '0000320193' 
  AND fiscal_year = 2023
  AND namespace = 'us-gaap';
```

### Type 3: Income Statements (Normalized)
**What:** Standardized income statement line items  
**Table:** `sec_income_statement`  
**Use Cases:** Revenue analysis, profitability trends, EPS tracking

**Includes:**
- Revenues
- Cost of Revenue / Gross Profit
- R&D, SG&A expenses
- Operating Income
- Net Income
- EPS (Basic & Diluted)

**Example Query:**
```sql
SELECT 
    fiscal_year,
    revenues / 1e9 as revenue_billions,
    net_income / 1e9 as net_income_billions,
    earnings_per_share_diluted as eps
FROM sec_income_statement
WHERE cik = '0000320193'
  AND fiscal_period = 'FY'
ORDER BY fiscal_year DESC;
```

### Type 4: Balance Sheets (Normalized)
**What:** Standardized balance sheet line items  
**Table:** `sec_balance_sheet`  
**Use Cases:** Asset analysis, debt levels, equity tracking, liquidity ratios

**Includes:**
- **Assets:** Cash, Investments, Receivables, PP&E, Goodwill
- **Liabilities:** Payables, Debt (short & long-term)
- **Equity:** Common Stock, Retained Earnings

**Example Query:**
```sql
SELECT 
    fiscal_year,
    total_assets / 1e9 as assets_billions,
    cash_and_equivalents / 1e9 as cash_billions,
    long_term_debt / 1e9 as debt_billions,
    stockholders_equity / 1e9 as equity_billions
FROM sec_balance_sheet
WHERE cik = '0000320193'
  AND fiscal_period = 'FY'
ORDER BY fiscal_year DESC;
```

### Type 5: Cash Flow Statements (Normalized)
**What:** Standardized cash flow line items  
**Table:** `sec_cash_flow_statement`  
**Use Cases:** Cash generation analysis, CapEx tracking, FCF analysis, dividend sustainability

**Includes:**
- **Operating CF:** Net Income, Depreciation, Working Capital
- **Investing CF:** CapEx, Acquisitions, Investments
- **Financing CF:** Debt, Dividends, Stock Buybacks
- **Free Cash Flow** (automatically calculated)

**Example Query:**
```sql
SELECT 
    fiscal_year,
    cash_from_operations / 1e9 as operating_cf_billions,
    capital_expenditures / 1e9 as capex_billions,
    free_cash_flow / 1e9 as fcf_billions,
    dividends_paid / 1e9 as dividends_billions
FROM sec_cash_flow_statement
WHERE cik = '0000320193'
  AND fiscal_period = 'FY'
ORDER BY fiscal_year DESC;
```

---

## 🚀 How to Use It

### Step 1: Start the Service
```bash
docker-compose up -d
```

### Step 2: Ingest Data for Companies

**Full ingestion (filings + financial data):**
```bash
curl -X POST "http://localhost:8000/api/v1/sec/ingest/full-company" \
  -H "Content-Type: application/json" \
  -d '{
    "cik": "0000320193",
    "filing_types": ["10-K", "10-Q"]
  }'
```

**Or use the batch script:**
```bash
# Edit ingest_sec_companies.py to add/remove companies
python ingest_sec_companies.py
```

### Step 3: Query the Structured Data

See `SEC_DATA_INGESTION_GUIDE.md` for detailed SQL query examples.

---

## 📈 Analysis Examples

### Example 1: Revenue Growth Rate
```sql
WITH yearly_revenue AS (
    SELECT 
        fiscal_year,
        revenues,
        LAG(revenues) OVER (ORDER BY fiscal_year) as prev_year_revenue
    FROM sec_income_statement
    WHERE cik = '0000320193' AND fiscal_period = 'FY'
)
SELECT 
    fiscal_year,
    revenues / 1e9 as revenue_billions,
    ((revenues - prev_year_revenue) / prev_year_revenue * 100)::numeric(5,2) as yoy_growth_pct
FROM yearly_revenue
WHERE prev_year_revenue IS NOT NULL
ORDER BY fiscal_year DESC;
```

### Example 2: Profitability Metrics
```sql
SELECT 
    fiscal_year,
    fiscal_period,
    (gross_profit / revenues * 100)::numeric(5,2) as gross_margin_pct,
    (operating_income / revenues * 100)::numeric(5,2) as operating_margin_pct,
    (net_income / revenues * 100)::numeric(5,2) as net_margin_pct
FROM sec_income_statement
WHERE cik = '0000320193'
ORDER BY fiscal_year DESC, fiscal_period DESC
LIMIT 20;
```

### Example 3: Debt-to-Equity Ratio
```sql
SELECT 
    fiscal_year,
    long_term_debt / 1e9 as debt_billions,
    stockholders_equity / 1e9 as equity_billions,
    (long_term_debt / NULLIF(stockholders_equity, 0))::numeric(5,2) as debt_to_equity_ratio
FROM sec_balance_sheet
WHERE cik = '0000320193'
  AND fiscal_period = 'FY'
ORDER BY fiscal_year DESC;
```

### Example 4: Capital Efficiency (ROIC)
```sql
SELECT 
    i.fiscal_year,
    i.operating_income / 1e9 as operating_income_billions,
    b.stockholders_equity / 1e9 as equity_billions,
    (i.operating_income / NULLIF(b.stockholders_equity, 0) * 100)::numeric(5,2) as roic_pct
FROM sec_income_statement i
JOIN sec_balance_sheet b ON 
    i.cik = b.cik AND 
    i.fiscal_year = b.fiscal_year AND 
    i.fiscal_period = b.fiscal_period
WHERE i.cik = '0000320193'
  AND i.fiscal_period = 'FY'
ORDER BY i.fiscal_year DESC;
```

### Example 5: Compare Multiple Companies
```sql
SELECT 
    company_name,
    fiscal_year,
    revenues / 1e9 as revenue_billions,
    net_income / 1e9 as net_income_billions,
    earnings_per_share_diluted as eps
FROM sec_income_statement
WHERE cik IN ('0000320193', '0000789019', '0001652044')  -- Apple, Microsoft, Google
  AND fiscal_period = 'FY'
  AND fiscal_year >= 2020
ORDER BY fiscal_year DESC, revenue_billions DESC;
```

---

## 🎯 Key Benefits

### 1. No Manual Parsing
- Financial data is **pre-parsed** from XBRL
- No need to scrape HTML or parse XML yourself
- Standardized column names across all companies

### 2. Ready for Analysis
- Normalized schema makes queries straightforward
- All financial data in proper numeric types (DECIMAL)
- Fiscal periods consistently labeled (Q1, Q2, Q3, Q4, FY)

### 3. Historical Data
- Multiple years of data available
- Both quarterly and annual periods
- Easy to calculate growth rates and trends

### 4. Cross-Company Comparisons
- Same schema for all companies
- Easy to JOIN and compare
- Build industry benchmarks

### 5. Time-Series Analysis
- Complete time series data
- Calculate moving averages, trends
- Identify inflection points

---

## 📁 Files Created/Modified

### New Files:
- `app/sources/sec/models.py` - Database models for financial data
- `app/sources/sec/xbrl_parser.py` - XBRL data parser
- `app/sources/sec/ingest_xbrl.py` - XBRL ingestion logic
- `ingest_sec_companies.py` - Batch ingestion script
- `SEC_DATA_INGESTION_GUIDE.md` - Comprehensive usage guide
- `SEC_STRUCTURED_DATA_SUMMARY.md` - This file

### Modified Files:
- `app/core/database.py` - Import SEC models
- `app/api/v1/sec.py` - Added XBRL endpoints
- `app/sources/sec/ingest.py` - Added XBRL imports

---

## ✅ Compliance Checklist

All rules followed:
- ✅ Typed database schema (DECIMAL, DATE, INTEGER columns)
- ✅ No JSON blobs for financial data
- ✅ Parameterized SQL queries
- ✅ Job tracking for all ingestion
- ✅ Idempotent operations (ON CONFLICT handling)
- ✅ Rate limiting (SEC 10 req/sec limit respected)
- ✅ Error handling with backoff
- ✅ Official API usage only (SEC Company Facts API)
- ✅ Public domain data
- ✅ Proper indexes for query performance

---

## 🎬 Next Steps

1. **Start ingesting companies:**
   ```bash
   # Use the API endpoints documented in SEC_DATA_INGESTION_GUIDE.md
   ```

2. **Query the financial data:**
   ```sql
   -- Use the SQL examples above
   ```

3. **Build your analyses:**
   - Revenue trends
   - Profitability ratios
   - Balance sheet health
   - Cash flow analysis
   - Peer comparisons

4. **Update tracking:**
   - Mark companies as ingested in `SEC_COMPANIES_TRACKING.md`

5. **Automate updates:**
   - Set up scheduled jobs to refresh data quarterly

---

## 📚 Documentation

- **Quick Start:** `SEC_QUICK_START.md`
- **Usage Guide:** `SEC_DATA_INGESTION_GUIDE.md`
- **Tracking:** `SEC_COMPANIES_TRACKING.md`
- **Implementation:** `SEC_IMPLEMENTATION_SUMMARY.md`
- **This Summary:** `SEC_STRUCTURED_DATA_SUMMARY.md`

---

The SEC data is now **fully structured and ready for analysis**. No more parsing HTML filings manually - just query the normalized financial statement tables! 🎉

