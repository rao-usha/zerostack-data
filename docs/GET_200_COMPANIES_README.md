# SEC Data for 200+ Companies - Ready to Ingest

## ✅ What's Ready

I've prepared **229 companies** across 11 sectors for SEC data ingestion:

### Companies by Sector:
- **Technology**: 36 companies (Apple, Microsoft, Google, Amazon, Tesla, NVIDIA, etc.)
- **Financial Services**: 34 companies (JPMorgan, Bank of America, Goldman Sachs, Visa, etc.)
- **Healthcare**: 31 companies (Johnson & Johnson, Pfizer, UnitedHealth, Merck, etc.)
- **Consumer Discretionary**: 27 companies (Walmart, Home Depot, McDonald's, Nike, etc.)
- **Industrials**: 25 companies (Boeing, Caterpillar, UPS, FedEx, General Electric, etc.)
- **Communication Services**: 16 companies (AT&T, Verizon, Netflix, Disney, Comcast, etc.)
- **Consumer Staples**: 15 companies (Procter & Gamble, Coca-Cola, PepsiCo, etc.)
- **Energy**: 15 companies (Exxon Mobil, Chevron, ConocoPhillips, etc.)
- **Utilities**: 10 companies (NextEra Energy, Duke Energy, Southern Co, etc.)
- **Real Estate**: 10 companies (American Tower, Prologis, Crown Castle, etc.)
- **Materials**: 10 companies (Linde, Air Products, Dow, DuPont, etc.)

**Total: 229 companies**

## 📁 Files Created

1. **`sec_companies_200.py`** - Master list of 229 companies with CIK numbers
2. **`api_ingest_200.py`** - API-based batch ingestion script
3. **`ingest_200_companies.py`** - Direct database ingestion script (alternative)

## 🚀 How to Ingest All 229 Companies

### Option 1: Via API (Recommended)

**Step 1: Ensure service is running**
```bash
docker-compose up -d
# OR
uvicorn app.main:app --reload
```

**Step 2: Run the ingestion script**
```bash
python api_ingest_200.py
```

This will:
- Process companies in batches of 10
- Include progress tracking
- Show success/failure for each company
- Take approximately 60-90 minutes

### Option 2: Via Direct Database (If API isn't working)

```bash
python ingest_200_companies.py
```

### Option 3: Manual API Calls (For specific companies)

```bash
# Ingest a specific company
curl -X POST "http://localhost:8000/api/v1/sec/ingest/full-company" \
  -H "Content-Type: application/json" \
  -d '{
    "cik": "0000320193",
    "filing_types": ["10-K", "10-Q"]
  }'
```

## 📊 What Data Gets Ingested

For each company, the system ingests:

### 1. Filing Metadata
- **Tables**: `sec_10k`, `sec_10q`
- **Content**: Filing dates, document links, company info
- **Period**: Last 5-6 years (2019-2024)

### 2. Financial Facts (XBRL)
- **Table**: `sec_financial_facts`
- **Content**: Every individual financial metric reported
- **Volume**: ~5,000-20,000 rows per company

### 3. Income Statements
- **Table**: `sec_income_statement`
- **Content**: Revenues, expenses, net income, EPS
- **Period**: All available quarters and fiscal years

### 4. Balance Sheets
- **Table**: `sec_balance_sheet`
- **Content**: Assets, liabilities, equity
- **Period**: All available quarters and fiscal years

### 5. Cash Flow Statements
- **Table**: `sec_cash_flow_statement`
- **Content**: Operating, investing, financing cash flows
- **Period**: All available quarters and fiscal years

## ⏱️ Performance Estimates

- **Per Company**: ~20-30 seconds
- **Total Time**: ~60-120 minutes for all 229 companies
- **Rate Limiting**: Respects SEC's 10 req/sec limit (uses 8 req/sec)
- **Data Volume**: 
  - Filing metadata: ~50-200 rows per company
  - Financial facts: ~5,000-20,000 rows per company
  - Financial statements: ~60-120 rows per company (income + balance + cash flow)

**Total expected rows: 1-3 million rows** across all tables

## 🔍 Verify Ingested Data

Once ingestion is complete:

### Check Company Count
```sql
-- Count distinct companies in each table
SELECT COUNT(DISTINCT cik) as company_count FROM sec_10k;
SELECT COUNT(DISTINCT cik) as company_count FROM sec_income_statement;
SELECT COUNT(DISTINCT cik) as company_count FROM sec_balance_sheet;
SELECT COUNT(DISTINCT cik) as company_count FROM sec_cash_flow_statement;
```

### View Sample Data
```sql
-- See Apple's recent filings
SELECT filing_date, filing_type, filing_url 
FROM sec_10k 
WHERE cik = '0000320193' 
ORDER BY filing_date DESC 
LIMIT 5;

-- See Apple's income statements
SELECT fiscal_year, fiscal_period, 
       revenues/1e9 as revenue_billions,
       net_income/1e9 as net_income_billions,
       earnings_per_share_diluted as eps
FROM sec_income_statement
WHERE cik = '0000320193'
ORDER BY fiscal_year DESC, fiscal_period DESC
LIMIT 10;
```

### Check Ingestion Jobs
```sql
-- View recent SEC ingestion jobs
SELECT 
    id,
    config->>'cik' as cik,
    config->>'type' as job_type,
    status,
    rows_inserted,
    created_at,
    completed_at,
    (EXTRACT(EPOCH FROM (completed_at - started_at)))::int as duration_seconds
FROM ingestion_jobs
WHERE source = 'sec'
ORDER BY created_at DESC
LIMIT 50;

-- Count by status
SELECT status, COUNT(*) 
FROM ingestion_jobs 
WHERE source = 'sec' 
GROUP BY status;
```

## 📈 Analysis Examples

Once data is ingested, you can run powerful analyses:

### Revenue Comparison Across Tech Giants
```sql
SELECT 
    company_name,
    fiscal_year,
    revenues / 1e9 as revenue_billions,
    (revenues - LAG(revenues) OVER (PARTITION BY cik ORDER BY fiscal_year)) / 
        LAG(revenues) OVER (PARTITION BY cik ORDER BY fiscal_year) * 100 as yoy_growth_pct
FROM sec_income_statement
WHERE cik IN ('0000320193', '0000789019', '0001652044', '0001018724')  -- AAPL, MSFT, GOOGL, AMZN
  AND fiscal_period = 'FY'
ORDER BY fiscal_year DESC, revenue_billions DESC;
```

### Profitability Across Sectors
```sql
-- You'd need to add sector info to the tables or join with a sector mapping
SELECT 
    company_name,
    fiscal_year,
    (gross_profit / revenues * 100)::numeric(5,2) as gross_margin_pct,
    (operating_income / revenues * 100)::numeric(5,2) as operating_margin_pct,
    (net_income / revenues * 100)::numeric(5,2) as net_margin_pct
FROM sec_income_statement
WHERE fiscal_period = 'FY' AND fiscal_year = 2023
ORDER BY net_margin_pct DESC
LIMIT 20;
```

### Cash Flow Analysis
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
WHERE fiscal_period = 'FY' AND fiscal_year >= 2020
ORDER BY free_cash_flow DESC
LIMIT 30;
```

## 🎯 Update Strategy

### Initial Load (Now)
Run the full ingestion for all 229 companies to get historical data.

### Ongoing Updates
Set up quarterly or monthly refreshes:

```bash
# Create a cron job or scheduled task
# Example: Every Monday at 2 AM
0 2 * * 1 /path/to/python /path/to/api_ingest_200.py
```

Or use the API to refresh specific companies:

```bash
# Refresh just the tech giants weekly
for cik in "0000320193" "0000789019" "0001652044" "0001018724"
do
    curl -X POST "http://localhost:8000/api/v1/sec/ingest/full-company" \
      -H "Content-Type: application/json" \
      -d "{\"cik\": \"$cik\"}"
done
```

## 🛠️ Troubleshooting

### Service Not Running
```bash
# Check if service is running
curl http://localhost:8000/health

# Start with Docker
docker-compose up -d

# Or start manually
uvicorn app.main:app --reload
```

### Database Connection Issues
```bash
# Check database
docker-compose ps
docker-compose logs db

# Restart database
docker-compose restart db
```

### Rate Limiting
If you see 429 errors:
- The script automatically handles rate limiting
- SEC allows 10 req/sec, we use 8 req/sec
- Just let it continue, it will retry with backoff

### Individual Company Failures
Check the error log in the script output. Common issues:
- Company CIK doesn't exist (rare, all CIKs are verified)
- No XBRL data available (older companies may not have structured data)
- Temporary SEC API issues (retry later)

## 📊 Expected Results

After successful ingestion of all 229 companies:

- **`sec_10k`**: ~5,000-10,000 rows (annual reports)
- **`sec_10q`**: ~15,000-30,000 rows (quarterly reports)
- **`sec_financial_facts`**: ~1-2 million rows (all XBRL facts)
- **`sec_income_statement`**: ~15,000-25,000 rows (all periods)
- **`sec_balance_sheet`**: ~15,000-25,000 rows (all periods)
- **`sec_cash_flow_statement`**: ~15,000-25,000 rows (all periods)
- **`ingestion_jobs`**: ~450-500 jobs (2 per company: filings + financial data)

**Total Database Size**: ~2-5 GB

## 🎉 What You Can Do With This Data

1. **Financial Analysis**: Compare profitability, growth rates, margins across companies and sectors
2. **Time Series**: Track quarterly/annual trends over 5+ years
3. **Peer Comparisons**: Benchmark companies against industry peers
4. **Screening**: Find companies with specific financial characteristics
5. **Research**: Academic or investment research with structured data
6. **Dashboards**: Build financial dashboards and visualizations
7. **Machine Learning**: Train models on financial data
8. **Alerts**: Set up notifications when metrics change

## 📚 Additional Documentation

- **`SEC_QUICK_START.md`** - Quick start guide
- **`SEC_DATA_INGESTION_GUIDE.md`** - Comprehensive usage guide with SQL examples
- **`SEC_STRUCTURED_DATA_SUMMARY.md`** - Technical implementation details
- **`SEC_COMPANIES_TRACKING.md`** - Checklist for tracking ingestion progress

## 🚀 Ready to Start?

1. **Ensure service is running**: `docker-compose up -d`
2. **Run ingestion**: `python api_ingest_200.py`
3. **Monitor progress**: Watch the console output
4. **Verify data**: Run the SQL queries above
5. **Start analyzing**: Use the query examples in the documentation

---

**The system is ready to ingest 229 companies with comprehensive SEC filings and financial data!** 🎉

