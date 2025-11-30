# 🎉 Ready to Ingest: 229 Companies

## ✅ What I've Built For You

### 1. Comprehensive Company List
**File**: `sec_companies_200.py`

**229 companies** organized across **11 sectors**:

| Sector | Count | Examples |
|--------|-------|----------|
| Technology | 36 | Apple, Microsoft, Google, Amazon, Tesla, NVIDIA |
| Financial Services | 34 | JPMorgan, Bank of America, Goldman Sachs, Visa |
| Healthcare | 31 | Johnson & Johnson, Pfizer, UnitedHealth, Merck |
| Consumer Discretionary | 27 | Walmart, Home Depot, McDonald's, Nike, Starbucks |
| Industrials | 25 | Boeing, Caterpillar, UPS, FedEx, General Electric |
| Communication Services | 16 | AT&T, Verizon, Netflix, Disney, Comcast |
| Consumer Staples | 15 | Procter & Gamble, Coca-Cola, PepsiCo |
| Energy | 15 | Exxon Mobil, Chevron, ConocoPhillips |
| Utilities | 10 | NextEra Energy, Duke Energy |
| Real Estate | 10 | American Tower, Prologis |
| Materials | 10 | Linde, Air Products, Dow |

### 2. Ingestion Scripts
- **`api_ingest_200.py`** - API-based batch ingestion (recommended)
- **`ingest_200_companies.py`** - Direct database ingestion (alternative)

### 3. Data Types Per Company
Each company gets:
- ✅ Filing metadata (10-K, 10-Q) → `sec_10k`, `sec_10q` tables
- ✅ All XBRL financial facts → `sec_financial_facts` table
- ✅ Normalized income statements → `sec_income_statement` table
- ✅ Normalized balance sheets → `sec_balance_sheet` table
- ✅ Normalized cash flows → `sec_cash_flow_statement` table

### 4. Documentation
- ✅ `GET_200_COMPANIES_README.md` - Full instructions
- ✅ `SEC_DATA_INGESTION_GUIDE.md` - Query examples and usage
- ✅ `SEC_STRUCTURED_DATA_SUMMARY.md` - Technical details
- ✅ `SEC_COMPANIES_TRACKING.md` - Track progress

## 🚀 Quick Start

### Start the Service
```bash
docker-compose up -d
```

### Run the Ingestion
```bash
python api_ingest_200.py
```

### What Happens
1. Processes companies in batches of 10
2. Shows progress for each company
3. Creates 2 jobs per company (filings + financial data)
4. Takes ~60-90 minutes total
5. Automatically handles rate limits and retries

### Monitor Progress
The script outputs:
```
[1/229] Apple Inc.
  Sector: Technology
  CIK: 0000320193
[Apple Inc.] Starting ingestion...
[Apple Inc.] ✓ Jobs created: 2
        - filings: job_id=1
        - financial_data: job_id=2

Progress: 10/229 (4.4%)
Success: 10 | Failed: 0 | Skipped: 0
Elapsed: 5.2 min | Rate: 1.9 companies/min | ETA: 115.3 min
```

## 📊 Expected Data Volume

### Per Company (~20-30 seconds each)
- Filing metadata: 50-200 rows
- Financial facts: 5,000-20,000 rows
- Financial statements: 60-120 rows

### Total (All 229 Companies)
- **Filing metadata**: ~10,000-40,000 rows
- **Financial facts**: ~1-2 million rows
- **Financial statements**: ~15,000-25,000 rows per statement type
- **Database size**: ~2-5 GB
- **Ingestion time**: ~60-120 minutes

## 🔍 Verify After Ingestion

```sql
-- Count companies ingested
SELECT COUNT(DISTINCT cik) FROM sec_10k;
-- Expected: 229 (or close to it)

-- Count income statements
SELECT COUNT(*) FROM sec_income_statement;
-- Expected: 15,000-25,000

-- Check specific company
SELECT * FROM sec_income_statement 
WHERE company_name ILIKE '%apple%' 
ORDER BY fiscal_year DESC, fiscal_period DESC 
LIMIT 10;
```

## 🎯 What You Can Analyze

With 229 companies of financial data:

### 1. Cross-Sector Comparisons
```sql
-- Compare profitability across sectors
-- (You'd need to add sector mapping)
SELECT 
    company_name,
    (net_income / revenues * 100)::numeric(5,2) as profit_margin_pct
FROM sec_income_statement
WHERE fiscal_year = 2023 AND fiscal_period = 'FY'
ORDER BY profit_margin_pct DESC
LIMIT 30;
```

### 2. Growth Leaders
```sql
-- Find fastest growing companies
WITH growth AS (
    SELECT 
        company_name,
        fiscal_year,
        revenues,
        LAG(revenues) OVER (PARTITION BY cik ORDER BY fiscal_year) as prev_revenue
    FROM sec_income_statement
    WHERE fiscal_period = 'FY'
)
SELECT 
    company_name,
    fiscal_year,
    revenues / 1e9 as revenue_billions,
    ((revenues - prev_revenue) / prev_revenue * 100)::numeric(5,2) as growth_pct
FROM growth
WHERE prev_revenue IS NOT NULL AND fiscal_year = 2023
ORDER BY growth_pct DESC
LIMIT 30;
```

### 3. Cash Flow Champions
```sql
-- Companies with strongest free cash flow
SELECT 
    company_name,
    fiscal_year,
    free_cash_flow / 1e9 as fcf_billions
FROM sec_cash_flow_statement
WHERE fiscal_period = 'FY' AND fiscal_year = 2023
ORDER BY free_cash_flow DESC
LIMIT 30;
```

### 4. Balance Sheet Strength
```sql
-- Companies with highest cash reserves
SELECT 
    company_name,
    period_end_date,
    cash_and_equivalents / 1e9 as cash_billions,
    total_assets / 1e9 as assets_billions,
    long_term_debt / 1e9 as debt_billions,
    (cash_and_equivalents / NULLIF(long_term_debt, 0))::numeric(5,2) as cash_to_debt_ratio
FROM sec_balance_sheet
WHERE fiscal_period = 'FY' AND fiscal_year = 2023
ORDER BY cash_and_equivalents DESC
LIMIT 30;
```

### 5. Dividend & Buyback Activity
```sql
-- Companies returning most cash to shareholders
SELECT 
    company_name,
    fiscal_year,
    dividends_paid / 1e9 as dividends_billions,
    stock_repurchased / 1e9 as buybacks_billions,
    (ABS(dividends_paid) + ABS(stock_repurchased)) / 1e9 as total_returned_billions
FROM sec_cash_flow_statement
WHERE fiscal_period = 'FY' AND fiscal_year = 2023
ORDER BY total_returned_billions DESC
LIMIT 30;
```

## 🎊 The Power of 229 Companies

You now have access to:
- **S&P 500 Coverage**: ~45% of the S&P 500
- **Market Cap Coverage**: Represents trillions in market capitalization
- **Sector Diversity**: All major sectors represented
- **Time Series**: 5+ years of quarterly and annual data
- **Structured Data**: Ready-to-query normalized financial statements

## 📈 Use Cases

1. **Investment Research**: Screen for value, growth, quality
2. **Sector Analysis**: Compare industries and identify leaders
3. **Academic Research**: Financial statement analysis at scale
4. **Dashboard Building**: Create financial monitoring dashboards
5. **Machine Learning**: Train models on financial data
6. **Benchmarking**: Compare your metrics against industry standards
7. **Due Diligence**: Deep dive into specific companies

## ⚡ Next Steps

1. ✅ **Start the service**: `docker-compose up -d`
2. ✅ **Run ingestion**: `python api_ingest_200.py`
3. ✅ **Wait**: ~60-90 minutes
4. ✅ **Verify**: Run SQL queries to check data
5. ✅ **Analyze**: Start exploring the financial data!

---

**Everything is ready. Just run the script and you'll have SEC data for 229 major U.S. companies!** 🚀

See `GET_200_COMPANIES_README.md` for detailed instructions and troubleshooting.

