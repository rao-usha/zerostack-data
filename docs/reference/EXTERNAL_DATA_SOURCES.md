# External Data Sources Checklist

Quick reference for all data sources available and their implementation status.

## 1. U.S. Census Bureau ✅ IMPLEMENTED

- [x] ACS 5-Year (2020–2023) — API Key Required
- [x] Decennial Census 2020 — API Key Required
- [x] PUMS — API Key Required
- [x] TIGER/Line GeoJSON — No API Key
- [x] Crosswalk Files (state/county/tract/zip) — No API Key

**Source:** `app/sources/census/`
**API Endpoints:** `/api/v1/census/*`, `/api/v1/geojson/*`
**Database Tables:** `acs5_YYYY_*`, `census_geojson_*`

---

## 2. Bureau of Labor Statistics (BLS) ✅ IMPLEMENTED

- [x] CPS (Current Population Survey) — API Key Optional ✅ IMPLEMENTED
- [x] CES (Current Employment Statistics) — API Key Optional ✅ IMPLEMENTED
- [x] OES (Occupational Employment Statistics) — API Key Optional ✅ IMPLEMENTED
- [x] JOLTS (Job Openings and Labor Turnover) — API Key Optional ✅ IMPLEMENTED
- [x] CPI (Consumer Price Index) — API Key Optional ✅ IMPLEMENTED
- [x] PPI (Producer Price Index) — API Key Optional ✅ IMPLEMENTED

**Source:** `app/sources/bls/`
**API Endpoints:**
- `POST /api/v1/bls/{dataset}/ingest` - Ingest by dataset (ces, cps, jolts, cpi, ppi, oes)
- `POST /api/v1/bls/series/ingest` - Ingest custom series IDs
- `POST /api/v1/bls/all/ingest` - Ingest all datasets
- `GET /api/v1/bls/reference/datasets` - Available datasets
- `GET /api/v1/bls/reference/series` - Common series IDs
- `GET /api/v1/bls/reference/quick` - Quick reference for popular series

**Database Tables:**
- `bls_ces_employment` - Employment, hours, earnings by industry
- `bls_cps_labor_force` - Unemployment rate, labor force participation
- `bls_jolts` - Job openings, hires, quits, layoffs
- `bls_cpi` - Consumer Price Index (CPI-U, Core CPI)
- `bls_ppi` - Producer Price Index
- `bls_oes` - Occupational Employment Statistics

**API Key:** Optional but recommended - https://data.bls.gov/registrationEngine/
- Without key: 25 queries/day, 10 years per query, 25 series per query
- With key: 500 queries/day, 20 years per query, 50 series per query

**Key Features:**
- Bounded concurrency with asyncio.Semaphore
- Exponential backoff with jitter for retries
- Automatic batching for large series requests
- Full job tracking via ingestion_jobs table

**Documentation:** `docs/BLS_QUICK_START.md`

---

## 3. Bureau of Economic Analysis (BEA) ✅ IMPLEMENTED

- [x] NIPA Tables (GDP, PCE, Investment) — API Key Required
- [x] Regional GDP — API Key Required
- [x] GDP by Industry — API Key Required
- [x] International Transactions — API Key Required
- [ ] Fixed Assets — API Key Required
- [ ] Input-Output Tables — API Key Required

**Source:** `app/sources/bea/`
**API Endpoints:** `/api/v1/bea/nipa/ingest`, `/api/v1/bea/regional/ingest`, `/api/v1/bea/gdp-industry/ingest`, `/api/v1/bea/international/ingest`
**Database Tables:** `bea_nipa`, `bea_regional`, `bea_gdp_industry`, `bea_international`
**API Key:** Required (free) - https://apps.bea.gov/api/signup/

---

## 4. Federal Reserve (FRED) ✅ IMPLEMENTED

- [x] Core Time Series — API Key Optional (Recommended)
- [x] H.15 Interest Rates — API Key Optional (Recommended)
- [x] Monetary Aggregates (M1, M2) — API Key Optional (Recommended)
- [x] Industrial Production — API Key Optional (Recommended)

**Source:** `app/sources/fred/`
**API Endpoints:** `/api/v1/fred/*`
**Database Tables:** `fred_series`, `fred_observations`

---

## 5. NOAA (Weather & Climate) ✅ IMPLEMENTED

- [x] Daily/Hourly Weather Observations — Token Required (Free)
- [x] Climate Normals — Token Required (Free)
- [ ] Storm Events Database — No API Key (CSV Downloads)
- [ ] NEXRAD Indexes — No API Key (AWS Open Data)

**Source:** `app/sources/noaa/`
**API Endpoints:** `/api/v1/noaa/*`
**Database Tables:** `noaa_observations`, `noaa_climate_normals`

---

## 6. EIA (Energy Information Administration) ✅ IMPLEMENTED

- [x] Petroleum & Gas Data — API Key Required
- [x] Electricity Data — API Key Required
- [x] Retail Gas Prices — API Key Required
- [x] STEO Projections — API Key Required

**Source:** `app/sources/eia/`
**API Endpoints:** `/api/v1/eia/*`
**Database Tables:** `eia_series`

---

## 7. USDA (Agriculture) ✅ IMPLEMENTED

- [x] Crop Production (corn, soybeans, wheat, etc.) — Requires USDA_API_KEY ✅ IMPLEMENTED
- [x] Crop Yields and Area Planted/Harvested — Requires USDA_API_KEY ✅ IMPLEMENTED
- [x] Prices Received by Farmers — Requires USDA_API_KEY ✅ IMPLEMENTED
- [x] Livestock Inventory — Requires USDA_API_KEY ✅ IMPLEMENTED
- [ ] WASDE (World Agricultural Supply and Demand) — Future
- [ ] Crop Progress (weekly) — Future
- [ ] Census of Agriculture — Future

**Source:** `app/sources/usda/`
**API Endpoints:**
- `POST /api/v1/usda/crop/ingest` - Ingest crop data by commodity
- `POST /api/v1/usda/livestock/ingest` - Ingest livestock inventory
- `POST /api/v1/usda/annual-summary/ingest` - Annual crop production summary
- `POST /api/v1/usda/all-major-crops/ingest` - All major crops at once
- `GET /api/v1/usda/reference/commodities` - Available commodities
- `GET /api/v1/usda/reference/crop-states` - Top producing states

**Database Tables:** `usda_crop_production`, `usda_livestock`

**API Key:** Required - Register free at https://quickstats.nass.usda.gov/api (set USDA_API_KEY env var)

**Key Features:**
- NASS QuickStats API integration
- Major crops: CORN, SOYBEANS, WHEAT, COTTON, RICE, OATS, BARLEY, SORGHUM
- Livestock: CATTLE, HOGS, SHEEP, CHICKENS
- National and state-level data
- Annual and seasonal statistics

---

## 8. CMS / HHS (Healthcare) ✅ IMPLEMENTED

- [x] Medicare Utilization — No API Key ✅ FULLY IMPLEMENTED
- [x] Hospital Cost Reports — No API Key ✅ FULLY IMPLEMENTED  
- [x] Drug Pricing Benchmarks — No API Key ✅ FULLY IMPLEMENTED

**Source:** `app/sources/cms/`
**API Endpoints:** `/api/v1/cms/*`
**Database Tables:** `cms_medicare_utilization`, `cms_hospital_cost_reports`, `cms_drug_pricing`

**Status:** ✅ Production-ready (~1,200 lines, 0 errors). CMS transitioned from Socrata to DKAN API format. Dataset IDs change with each data release. Configure current IDs in `app/sources/cms/metadata.py` from data.cms.gov.

**Documentation:** `docs/CMS_IMPLEMENTATION.md`, `docs/CMS_STATUS.md`

---

## 9. SEC EDGAR (Corporate Filings) ✅ IMPLEMENTED

- [x] 10-K Annual Reports — No API Key
- [x] 10-Q Quarterly Reports — No API Key
- [x] 8-K Current Reports — No API Key
- [x] S-1 / S-3 / S-4 Registration Statements — No API Key
- [x] XBRL Financial Data Extraction — No API Key
- [x] Form ADV (Investment Advisers) — No API Key
- [x] Company Facts API — No API Key

**Source:** `app/sources/sec/`
**API Endpoints:** `/api/v1/sec/*`
**Database Tables:** `sec_company_facts`, `sec_form_adv`, `sec_xbrl_*`

---

## 10. USPTO (Patents) ⚠️ NOT IMPLEMENTED

- [ ] Bulk Patent Text — No API Key
- [ ] Patent Metadata — No API Key
- [ ] Citation Graphs — No API Key

---

## 11. Federal Register & Regulations.gov ⚠️ NOT IMPLEMENTED

- [ ] Federal Register API (Rules, Notices, Presidential Documents) — No API Key
- [ ] Proposed Rules & Final Rules — No API Key
- [ ] Regulatory Impact Analyses — No API Key
- [ ] Presidential Documents — No API Key
- [ ] Public Comments (Regulations.gov API) — API Key Required (Free)
- [ ] Dockets & Supporting Materials — API Key Required (Free)

**APIs Available:**
- Federal Register API: https://www.federalregister.gov/developers/documentation/api/v1
- Regulations.gov API: https://open.gsa.gov/api/regulationsgov/

---

## 12. Real Estate / Housing ✅ IMPLEMENTED

- [x] FHFA House Price Index — No API Key
- [x] HUD Permits & Starts — No API Key
- [x] Redfin Data Dump — No API Key
- [x] OpenStreetMap Building Footprints — No API Key

**Source:** `app/sources/realestate/`
**API Endpoints:** `/api/v1/realestate/*`
**Database Tables:** `fhfa_house_price_index`, `hud_permits`, `redfin_market_data`, `osm_buildings`

---

## 13. Mobility & Consumer Activity ⚠️ NOT IMPLEMENTED

- [ ] Google Mobility Reports — No API Key
- [ ] Apple Mobility Trends — No API Key
- [ ] Census Retail Trade — No API Key
- [ ] BEA PCE (Consumer Spending) — API Key Required

---

## 14. Public Pension LP Investment Strategies ✅ IMPLEMENTED

**Source:** `app/sources/public_lp_strategies/`
**API Endpoints:** `/api/v1/public-lp/*`
**Database Tables:** `lp_document_library`, `lp_contacts`, `lp_strategies_summary`

### U.S. Mega Public Pension Funds

- [x] CalPERS
- [x] CalSTRS
- [x] New York State Common Retirement Fund (NYSCRF)
- [x] Texas Teachers Retirement System (TRS)
- [x] Florida SBA
- [x] Illinois Teachers' Retirement System (TRS Illinois)
- [x] Pennsylvania PSERS
- [x] Washington State Investment Board (WSIB)
- [x] New Jersey Division of Investment
- [x] Ohio Public Employees Retirement System (OPERS)
- [x] Ohio State Teachers Retirement System (STRS Ohio)
- [x] North Carolina Retirement Systems
- [ ] Georgia Teachers Retirement System
- [x] Virginia Retirement System (VRS)
- [x] Massachusetts PRIM
- [ ] Colorado PERA
- [x] Wisconsin Investment Board (SWIB)
- [ ] Minnesota State Board of Investment (SBI)
- [ ] Arizona State Retirement System (ASRS)
- [ ] Michigan Office of Retirement Services

### Other U.S. State / Municipal Funds

- [ ] New York City Retirement Systems
- [ ] Los Angeles Fire & Police Pensions
- [ ] Los Angeles City Employees' Retirement System
- [ ] San Francisco Employees' Retirement System
- [ ] Houston Firefighters' Relief & Retirement Fund
- [ ] Chicago Teachers' Pension Fund
- [ ] Kentucky Teachers' Retirement System
- [ ] Maryland State Retirement & Pension System
- [ ] Nevada PERS
- [ ] Alaska Permanent Fund Corporation

### U.S. University Endowments

- [x] Harvard Management Company
- [x] Yale Investments Office
- [x] Stanford Management Company
- [ ] MITIMCo
- [ ] Princeton University Investment Company (PRINCO)
- [ ] University of California Regents
- [ ] University of Michigan Endowment
- [ ] UTIMCO (University of Texas/Texas A&M)
- [ ] Northwestern University
- [ ] Duke University

### Canadian Pensions

- [x] CPP Investments (CPPIB)
- [x] Ontario Teachers' Pension Plan (OTPP)
- [x] Ontario Municipal Employees Retirement System (OMERS)
- [ ] British Columbia Investment Management Corporation (BCI)
- [x] Caisse de dépôt et placement du Québec (CDPQ)
- [ ] Public Sector Pension Investment Board (PSP Investments)

### European Public / Sovereign Funds

- [x] Norges Bank Investment Management (Norway GPFG)
- [ ] AP Funds (Sweden)
- [x] Dutch ABP (via APG)
- [ ] PFZW (via PGGM)
- [ ] UK USS (Universities Superannuation Scheme)
- [ ] Irish Strategic Investment Fund (ISIF)
- [ ] Finland Varma
- [ ] Finland Ilmarinen
- [ ] Denmark ATP

### Asia-Pacific Public Funds

- [x] AustralianSuper
- [x] Future Fund (Australia SWF)
- [x] New Zealand Super Fund
- [ ] GPIF Japan
- [x] GIC Singapore
- [ ] Temasek

### Middle East Sovereign Wealth Funds

- [x] ADIA (Abu Dhabi Investment Authority)
- [ ] Mubadala
- [ ] QIA (Qatar Investment Authority)
- [ ] PIF Saudi Arabia

### Latin America Public Funds

- [ ] Chile Pension Funds (AFP system)
- [ ] Mexico AFORES

---

## 15. Family Office Strategy Documents ✅ IMPLEMENTED

**Source:** `app/core/family_office_models.py`, SEC Form ADV integration
**API Endpoints:** `/api/v1/family-offices/*`, `/api/v1/sec/form-adv/*`
**Database Tables:** `family_offices`, `family_office_contacts`, `family_office_interactions`, `sec_form_adv`

**Two Complementary Systems:**

1. **SEC Form ADV System** — SEC-registered investment advisers only
2. **Family Office Tracking System** — All family offices (manual research)

**Current Data:** 22 family offices loaded (12 US, 3 Middle East, 7 Asia)

### U.S. Large Family Offices

- [x] Soros Fund Management
- [ ] Cohen Private Ventures (Steve Cohen)
- [x] MSD Capital / MSD Partners (Michael Dell)
- [x] Cascade Investment (Bill Gates)
- [x] Walton Family Office
- [x] Bezos Expeditions
- [x] Emerson Collective (Laurene Powell Jobs)
- [ ] Shad Khan Family Office
- [ ] Perot Investments
- [x] Pritzker Group
- [x] Ballmer Group
- [x] Arnold Ventures
- [x] Hewlett Foundation
- [x] Packard Foundation
- [x] Raine Group

### Europe Family Offices

- [ ] Cevian Capital
- [ ] LGT Group (Liechtenstein Royal Family)
- [ ] Bertelsmann / Mohn Family Office
- [ ] Reimann Family (JAB Holding Company)
- [ ] Agnelli Family (Exor)
- [ ] BMW Quandt Family Office
- [ ] Ferrero Family Office
- [ ] Heineken Family Office
- [ ] Hermès Family Office (Axile)

### Middle East & Asian Family Offices

- [x] Kingdom Holding Company (Alwaleed Bin Talal)
- [x] Olayan Group
- [x] Al-Futtaim Group
- [x] Mitsubishi Materials Corporation
- [x] Tata Trusts
- [x] Cheng Family Office (New World / Chow Tai Fook)
- [x] Lee Family Office (Samsung)
- [x] Kuok Group
- [x] Kyocera Family Office (Inamori)
- [x] Temasek Holdings

### Latin America Family Offices

- [ ] Safra Family Office
- [ ] Lemann Family (3G Capital)
- [ ] Marinho Family (Globo)
- [ ] Santo Domingo Family Office
- [ ] Paulmann Family (Cencosud)
- [ ] Luksic Family Office

---

## 16. International Economic Data ✅ IMPLEMENTED

**Source:** `app/sources/international_econ/`
**API Endpoints:** `/api/v1/international/*`
**Database Tables:** `intl_worldbank_*`, `intl_imf_*`, `intl_oecd_*`, `intl_bis_*`

### World Bank Open Data

- [x] World Development Indicators (WDI)
- [x] Countries Metadata
- [x] Indicators Metadata
- [ ] Global Economic Monitor
- [ ] Poverty & Inequality Data
- [ ] Doing Business Indicators
- [ ] Climate Change Data

### International Monetary Fund (IMF)

- [ ] World Economic Outlook (WEO)
- [ ] Balance of Payments Statistics
- [x] International Financial Statistics (IFS)
- [ ] Financial Soundness Indicators
- [ ] Exchange Rate Data

### OECD Data

- [x] Composite Leading Indicators (CLI)
- [x] Key Economic Indicators (KEI/MEI)
- [x] Trade Statistics (BATIS)
- [x] Labor Market Data (ALFS)
- [x] Tax Revenue Statistics

### Bank for International Settlements (BIS)

- [ ] International Banking Statistics
- [ ] Credit Gap Indicators
- [x] Effective Exchange Rates
- [x] Property Prices
- [ ] Debt Securities Statistics

---

## 17. Financial Institution Data ✅ MOSTLY IMPLEMENTED

### FDIC BankFind Suite ✅ IMPLEMENTED

- [x] Bank Financials — No API Key ✅ IMPLEMENTED
- [x] Bank Demographics (Institutions) — No API Key ✅ IMPLEMENTED
- [x] Failed Banks List — No API Key ✅ IMPLEMENTED
- [x] Summary of Deposits — No API Key ✅ IMPLEMENTED

**Source:** `app/sources/fdic/`
**API Endpoints:**
- `POST /api/v1/fdic/financials/ingest` - Bank balance sheets, income statements
- `POST /api/v1/fdic/institutions/ingest` - Bank demographics, locations
- `POST /api/v1/fdic/failed-banks/ingest` - Historical bank failures
- `POST /api/v1/fdic/deposits/ingest` - Branch-level deposit data
- `POST /api/v1/fdic/all/ingest` - All datasets at once
- `GET /api/v1/fdic/reference/metrics` - Financial metric codes
- `GET /api/v1/fdic/reference/datasets` - Dataset information
- `GET /api/v1/fdic/search` - Search banks by name/location

**Database Tables:**
- `fdic_bank_financials` - Balance sheets, income statements, 1,100+ metrics
- `fdic_institutions` - Bank demographics, ~4,700 active banks
- `fdic_failed_banks` - Historical failures since 1934
- `fdic_summary_deposits` - Branch-level deposit data

**API:** https://banks.data.fdic.gov/docs/
**API Key:** ❌ NOT REQUIRED

**Key Features:**
- Bounded concurrency with asyncio.Semaphore
- Exponential backoff with jitter for retries
- Automatic pagination for large datasets
- Full job tracking via ingestion_jobs table
- Typed columns (NUMERIC for financials, DATE for dates)
- 1,100+ financial metrics available

**Documentation:** `docs/FDIC_QUICK_START.md`

### FFIEC Central Data Repository

- [ ] Uniform Bank Performance Reports (UBPR) — No API Key
- [ ] Call Reports — No API Key
- [ ] Holding Company Reports — No API Key
- [ ] CRA Data — No API Key

### NCUA Credit Union Data

- [ ] Credit Union Call Reports — No API Key
- [ ] Credit Union Financials — No API Key
- [ ] Credit Union Demographics — No API Key

### Treasury FiscalData ✅ IMPLEMENTED

- [x] Federal Debt Data — No API Key ✅ IMPLEMENTED
- [x] Treasury Interest Rates — No API Key ✅ IMPLEMENTED
- [x] Federal Revenue & Spending — No API Key ✅ IMPLEMENTED
- [x] Treasury Auction Results — No API Key ✅ IMPLEMENTED
- [x] Daily Treasury Balance — No API Key ✅ IMPLEMENTED

**Source:** `app/sources/treasury/`
**API Endpoints:**
- `POST /api/v1/treasury/debt/ingest` - Federal debt outstanding
- `POST /api/v1/treasury/interest-rates/ingest` - Treasury security rates
- `POST /api/v1/treasury/revenue-spending/ingest` - Monthly Treasury Statement
- `POST /api/v1/treasury/auctions/ingest` - Auction results
- `POST /api/v1/treasury/all/ingest` - All datasets
- `GET /api/v1/treasury/reference/datasets` - Available datasets
- `GET /api/v1/treasury/reference/security-types` - Security type reference

**Database Tables:**
- `treasury_debt_outstanding` - Total public debt outstanding
- `treasury_interest_rates` - Average interest rates on Treasury securities
- `treasury_monthly_statement` - Revenue and spending
- `treasury_auctions` - Treasury securities auction results
- `treasury_daily_balance` - Daily Treasury statement

**API:** https://fiscaldata.treasury.gov/api-documentation/
**API Key:** ❌ NOT REQUIRED (1,000 requests per minute)

**Key Features:**
- Bounded concurrency with asyncio.Semaphore
- Exponential backoff with jitter for retries
- Automatic pagination for large datasets
- Full job tracking via ingestion_jobs table
- Typed columns (DATE, NUMERIC, TEXT)

**Documentation:** `docs/TREASURY_QUICK_START.md`

---

## 18. CFTC Commitments of Traders (COT) ✅ IMPLEMENTED

- [x] Legacy COT Reports ✅ **IMPLEMENTED** - 64,392 records (4 years)
- [x] Disaggregated COT Reports ✅ **IMPLEMENTED** - 40,304 records (3 years)
- [x] Traders in Financial Futures (TFF) ✅ **IMPLEMENTED** - 6,779 records (2 years)
- [ ] Supplemental COT Reports — Future
- [x] Concentration Ratios ✅ **INCLUDED** (in legacy/disaggregated data)

**📊 TOTAL: 111,475 COT records ingested**

**Source:** `app/sources/cftc_cot/`
**API Endpoints:**
- `POST /api/v1/cftc-cot/ingest` - Ingest COT data by year and report type
- `GET /api/v1/cftc-cot/reference/report-types` - Available report types
- `GET /api/v1/cftc-cot/reference/contracts` - Major futures contracts
- `GET /api/v1/cftc-cot/reference/commodity-groups` - Commodity categories

**Database Tables:**
- `cftc_cot_legacy_combined` - Commercial vs Non-commercial positions
- `cftc_cot_disaggregated_combined` - Producer, Swap Dealer, Managed Money, Other
- `cftc_cot_tff_combined` - Dealer, Asset Manager, Leveraged Funds

**API Key:** ❌ NOT REQUIRED (public weekly data from CFTC)

**Key Features:**
- Weekly positioning data released Tuesday afternoons
- Covers 100+ futures markets (energy, metals, grains, financials, currencies)
- Position breakdowns by trader category
- Net positions and weekly changes
- Concentration ratios (top 4/8 traders)
- Futures only and Futures+Options combined reports
- Historical data available (2006-present)

**Report Types:**
| Type | Description | Best For |
|------|-------------|----------|
| Legacy | Commercial vs Non-commercial | Hedger/Speculator analysis |
| Disaggregated | Producer, Swap, Managed Money | Detailed positioning |
| TFF | Dealer, Asset Manager, Leveraged | Financial futures |

**Use Cases:**
- Sentiment analysis (positioning extremes)
- Contrarian trading signals
- Tracking speculator vs hedger positions
- Analyzing commodity market trends

---

## 19. IRS Statistics of Income (SOI) ✅ IMPLEMENTED

- [x] Individual Income by ZIP Code — No API Key ✅ IMPLEMENTED
- [x] Individual Income by County — No API Key ✅ IMPLEMENTED
- [x] County-to-County Migration Data — No API Key ✅ IMPLEMENTED
- [x] Business Income by ZIP Code — No API Key ✅ IMPLEMENTED
- [ ] Corporate Income Tax Statistics — No API Key (Future)
- [ ] Partnership Statistics — No API Key (Future)
- [ ] Estate Tax Statistics — No API Key (Future)
- [ ] Tax-Exempt Organizations — No API Key (Future)

**Source:** `app/sources/irs_soi/`
**API Endpoints:**
- `POST /api/v1/irs-soi/zip-income/ingest` - Individual income by ZIP code
- `POST /api/v1/irs-soi/county-income/ingest` - Individual income by county
- `POST /api/v1/irs-soi/migration/ingest` - County-to-county migration flows
- `POST /api/v1/irs-soi/business-income/ingest` - Business income by ZIP
- `POST /api/v1/irs-soi/all/ingest` - All datasets at once
- `GET /api/v1/irs-soi/reference/agi-brackets` - AGI bracket definitions
- `GET /api/v1/irs-soi/reference/years` - Available tax years

**Database Tables:**
- `irs_soi_zip_income` - Income statistics by ZIP code and AGI bracket
- `irs_soi_county_income` - Income statistics by county FIPS and AGI bracket
- `irs_soi_migration` - County-to-county migration flows (inflow/outflow)
- `irs_soi_business_income` - Business/self-employment income by ZIP

**Data Source:** https://www.irs.gov/statistics/soi-tax-stats (Bulk CSV downloads)
**API Key:** ❌ NOT REQUIRED (public domain)
**Available Years:** 2017-2021

**Key Features:**
- Bounded concurrency with asyncio.Semaphore
- File caching to avoid re-downloads
- Batch inserts for large files (5,000 records/batch)
- AGI bracket classification (6 income levels)
- Full job tracking via ingestion_jobs table
- Typed columns (BIGINT for dollar amounts)

**Use Cases:**
- Income inequality analysis
- Tax base migration studies
- Real estate market research
- Business formation patterns
- Wealth distribution by geography

**Documentation:** `docs/IRS_SOI_QUICK_START.md`

---

## 20. OpenFEMA (Disaster & Emergency Data) ✅ IMPLEMENTED

- [x] Disaster Declarations — No API Key
- [x] Public Assistance Grants — No API Key
- [x] Hazard Mitigation Grants — No API Key
- [ ] Individual Assistance Data — No API Key
- [ ] NFIP Claims & Policies — No API Key
- [ ] Registration Intake Data — No API Key

**Source:** `app/sources/fema/`
**API Endpoints:** `/api/v1/fema/disasters/ingest`, `/api/v1/fema/public-assistance/ingest`, `/api/v1/fema/hazard-mitigation/ingest`
**Database Tables:** `fema_disaster_declarations`, `fema_pa_projects`, `fema_hma_projects`

---

## 21. EPA Environmental Data ⚠️ NOT IMPLEMENTED

### Envirofacts

- [ ] Air Quality Data (AQI, emissions) — No API Key
- [ ] Water Quality Data — No API Key
- [ ] Toxic Release Inventory (TRI) — No API Key
- [ ] Hazardous Waste Data — No API Key
- [ ] Facility Registry Service — No API Key

**API:** https://www.epa.gov/enviro/envirofacts-data-service-api

### AirNow

- [ ] Real-Time Air Quality — API Key Required (Free)
- [ ] Air Quality Forecasts — API Key Required (Free)
- [ ] Historical AQI Data — API Key Required (Free)

**API:** https://docs.airnowapi.org/

### ECHO (Enforcement & Compliance)

- [ ] Facility Compliance Status — No API Key
- [ ] Enforcement Actions — No API Key
- [ ] Permit Data — No API Key
- [ ] Inspection History — No API Key

**API:** https://echo.epa.gov/tools/web-services

---

## 22. FBI Crime Data (UCR/NIBRS) ✅ IMPLEMENTED

- [x] Uniform Crime Reports (UCR) — API Key Required (Free)
- [x] National Incident-Based Reporting (NIBRS) — API Key Required (Free)
- [x] Hate Crime Statistics — API Key Required (Free)
- [x] Law Enforcement Officers Killed (LEOKA) — API Key Required (Free)
- [ ] Cargo Theft Reports — API Key Required (Free)

**Source:** `app/sources/fbi_crime/`
**API Endpoints:** `/api/v1/fbi-crime/*`
**Database Tables:** `fbi_crime_estimates_national`, `fbi_crime_estimates_state`, `fbi_crime_summarized_agency`, `fbi_crime_nibrs_state`, `fbi_crime_hate_crime_national`, `fbi_crime_leoka_national`
**API Key:** Required (free) - https://api.data.gov/signup/

---

## 23. Bureau of Transportation Statistics (BTS) ✅ IMPLEMENTED

- [x] Border Crossing Data — No API Key
- [x] Vehicle Miles Traveled (VMT) — No API Key
- [x] Freight Analysis Framework (FAF5) — No API Key
- [ ] Airline On-Time Performance — No API Key
- [ ] Port Activity Data — No API Key
- [ ] Transportation Safety Data — No API Key
- [ ] Fuel Consumption Data — No API Key

**Source:** `app/sources/bts/`
**API Endpoints:** `/api/v1/bts/border-crossing/ingest`, `/api/v1/bts/vmt/ingest`, `/api/v1/bts/faf/ingest`
**Database Tables:** `bts_border_crossing`, `bts_vmt`, `bts_faf_regional`

---

## 24. U.S. International Trade Data ✅ IMPLEMENTED

- [x] Import/Export by HS Code — No API Key
- [x] Trade by Country — No API Key
- [x] State Export Data — No API Key
- [ ] Trade by Port — No API Key (different endpoint structure)
- [ ] Tariff Data — No API Key (via USITC)

**Source:** `app/sources/us_trade/`
**API Endpoints:** `/api/v1/us-trade/exports/hs/ingest`, `/api/v1/us-trade/imports/hs/ingest`, `/api/v1/us-trade/exports/state/ingest`
**Database Tables:** `us_trade_exports_hs`, `us_trade_imports_hs`, `us_trade_exports_state`
**Data Coverage:** 591,110 records (2019-2024), 254 countries, 96 HS chapters, 46 states

---

## 25. Alternative Data / Sentiment ✅ IMPLEMENTED

### Google Data Commons ✅ DATA INGESTED

- [x] Unified Public Data Graph — **API Key REQUIRED** (as of 2025)
- [x] Statistical Variables — **API Key REQUIRED**
- [x] Pre-Normalized Datasets — **API Key REQUIRED**

**Source:** `app/sources/data_commons/`
**API Endpoints:** `/api/v1/data-commons/*`
**API Key:** Get from https://apikeys.datacommons.org (NOT Google Cloud Console)
**Database Table:** `data_commons_observations`

**📊 Data Ingested: 41,528 records for all 51 US states/DC**

| Variable | Records | Date Range |
|----------|---------|------------|
| Unemployment rate | 32,946 | 1976 - 2025 |
| Total population | 6,440 | 1790 - 2024 |
| Median household income | 714 | 2010 - 2023 |
| Median age | 714 | 2010 - 2023 |
| Number of households | 714 | 2010 - 2023 |

**Coverage:** All 51 US states + DC, with historical time series dating back to 1790 for population data.

### Yelp Fusion

- [x] Business Listings — API Key Required (Free tier: 500 calls/day)
- [x] Business Reviews — API Key Required
- [x] Business Search — API Key Required

**Source:** `app/sources/yelp/`
**API Endpoints:** `/api/v1/yelp/*`
**Database Tables:** `yelp_businesses`, `yelp_categories`

### Google Trends

- [ ] Search Interest Over Time — No official API (pytrends library archived April 2025)
- [ ] Search Interest by Region — No official API
- [ ] Related Queries — No official API

**Note:** Google does not provide an official public API for Trends data.

---

## 26. Kaggle Competition Datasets ✅ IMPLEMENTED

- [x] M5 Forecasting (Walmart-style Retail Demand) — Kaggle API Key Required

**Source:** `app/sources/kaggle/`
**API Endpoints:** `/api/v1/kaggle/m5/*`
**Database Tables:** `m5_sales`, `m5_calendar`, `m5_prices`, `m5_items`
**Credentials:** Requires KAGGLE_USERNAME and KAGGLE_KEY environment variables
**Dataset Size:** ~60M sales records (3,049 products × 10 stores × 1,969 days)
**Competition:** https://www.kaggle.com/competitions/m5-forecasting-accuracy

---

## 27. FCC Broadband & Telecom Data ✅ IMPLEMENTED

### FCC Broadband Map API

- [x] Broadband Coverage by Geography — No API Key ✅ IMPLEMENTED
- [x] ISP Market Share — No API Key ✅ IMPLEMENTED
- [x] Download/Upload Speeds — No API Key ✅ IMPLEMENTED
- [x] Technology Type (Fiber, Cable, DSL, 5G) — No API Key ✅ IMPLEMENTED
- [x] Broadband Summary Statistics — No API Key ✅ IMPLEMENTED
- [ ] Ookla Speedtest Data — Future (CC BY-NC-SA license)

**Source:** `app/sources/fcc_broadband/`
**API Endpoints:**
- `POST /api/v1/fcc-broadband/state/ingest` - Ingest by state codes
- `POST /api/v1/fcc-broadband/all-states/ingest` - Ingest all 50 states + DC
- `POST /api/v1/fcc-broadband/county/ingest` - Ingest by county FIPS
- `GET /api/v1/fcc-broadband/reference/states` - State codes reference
- `GET /api/v1/fcc-broadband/reference/technologies` - Technology codes
- `GET /api/v1/fcc-broadband/reference/speed-tiers` - Speed classifications

**Database Tables:**
- `fcc_broadband_coverage` - Provider-level coverage data
- `fcc_broadband_summary` - Aggregated statistics by geography

**API:** https://broadbandmap.fcc.gov/data-download
**API Key:** ❌ NOT REQUIRED (public government data)
**Format:** REST API (JSON) + Bulk Downloads (CSV)
**Rate Limit:** ~60 requests/min recommended
**Coverage:** All U.S. states, counties, census blocks

**Key Features:**
- Technology breakdown: Fiber, Cable, DSL, Fixed Wireless, Satellite
- Speed tier classification: sub_broadband, basic, high_speed, gigabit
- Provider competition analysis: monopoly, duopoly, limited, competitive
- Digital divide metrics (coverage percentages)
- FCC broadband definition: 25 Mbps down / 3 Mbps up

**Use Cases:**
- Digital divide analysis
- ISP competition analysis
- Real estate investment research
- Policy analysis
- Network infrastructure planning

**Documentation:** `docs/FCC_BROADBAND_QUICK_START.md`

### FCC Form 477 Data

- [x] ISP Deployment Data — No API Key ✅ IMPLEMENTED
- [x] Technology Deployment — No API Key ✅ IMPLEMENTED

### Ookla Speedtest Open Dataset (Optional Enhancement)

- [ ] Speed Test Results — No API Key (Open Data)
- [ ] Network Performance by Geography — No API Key
- [ ] Mobile vs Fixed Broadband — No API Key

**Data:** https://www.ookla.com/ookla-for-good/open-data
**Format:** Shapefiles, CSV (quarterly releases)
**License:** Creative Commons Attribution-NonCommercial-ShareAlike 4.0

---

## 28. Additional Government Data ⚠️ NOT IMPLEMENTED

### USGS (U.S. Geological Survey)

- [ ] Earthquake Data — No API Key
- [ ] Water Resources Data — No API Key
- [ ] Mineral Resources Data — No API Key
- [ ] Land Use/Land Cover — No API Key

**API:** https://earthquake.usgs.gov/fdsnws/event/1/

### NASA Earthdata

- [ ] Satellite Imagery — Free Account Required
- [ ] Land/Ocean Temperature — Free Account Required
- [ ] Vegetation Indexes — Free Account Required
- [ ] Sea Level Data — Free Account Required

**API:** https://earthdata.nasa.gov/

### NIH/PubMed

- [ ] Research Publications — No API Key
- [ ] Clinical Trial Data — No API Key
- [ ] Grant Funding Data — No API Key

**API:** https://www.ncbi.nlm.nih.gov/home/develop/api/

### NSF Science Indicators

- [ ] R&D Spending Data — No API Key
- [ ] STEM Education Statistics — No API Key
- [ ] Science & Engineering Workforce — No API Key

**API:** https://ncses.nsf.gov/

---

## 29. Agentic Research Capabilities 🤖 IN DEVELOPMENT

**SPECIAL CATEGORY:** Multi-source agentic data collection (not a single API)

### LP/FO Portfolio & Deal Flow Research

- [ ] Portfolio company discovery (5+ sources per investor)
- [ ] Investment history tracking
- [ ] Co-investor network mapping
- [ ] Investment theme classification

**Approach:** Agentic (multi-step reasoning, adaptive navigation)
**Source:** `app/agentic/`
**API Endpoints:**
- `POST /api/v1/agentic/portfolio/collect` - Trigger collection for single investor
- `POST /api/v1/agentic/portfolio/batch` - Batch collection
- `GET /api/v1/agentic/portfolio/{investor_id}/summary` - Portfolio summary
- `GET /api/v1/agentic/jobs/{job_id}` - Job status with reasoning log

**Database Tables:**
- `portfolio_companies` - Investment holdings for LPs/FOs
- `co_investments` - Co-investor network (who invests together)
- `investor_themes` - Investment pattern classification
- `agentic_collection_jobs` - Job tracking with agent reasoning

**Agent Strategies:**
1. **SEC 13F Filings** (API) - Public equity holdings for investors >$100M
2. **Website Portfolio Scraping** (HTML parsing) - Official portfolio pages
3. **Annual Report Parsing** (PDF extraction) - CAFRs, investment reports
4. **Press Release & News Search** (LLM extraction) - Recent deals, announcements
5. **Portfolio Company Back-References** (Reverse search) - Companies listing their investors

**Key Features:**
- Multi-source synthesis and deduplication
- Confidence scoring based on source quality
- Agent reasoning logs for debugging
- Adaptive strategy selection based on investor type
- LLM-powered entity extraction from unstructured text

**Expected Coverage:**
- 80-100 LPs (60-75%) with 5+ portfolio companies
- 40-60 FOs (40-60%) with investment history
- 50+ co-investor relationships identified
- 3+ sources per investor on average

**Use Cases:**
- Understand LP/FO investment patterns
- Identify warm introductions through co-investors
- Find deal sourcing opportunities
- Due diligence and competitive intelligence

**Documentation:** `docs/AGENT_PROMPTS/agentic_portfolio_research_prompt.md`

**Status:** 🤖 **PLANNED** - Detailed implementation plan created, ready for development

**Estimated Timeline:** 4-6 weeks (Quick win: SEC 13F strategy in 2-3 days)

---

## 30. Private Company Intelligence 🤖 IN DEVELOPMENT

**SPECIAL CATEGORY:** Multi-source agentic company profiling (not a single API)

### Company Profile Enrichment

- [ ] Company basics (founding, location, industry, stage)
- [ ] Leadership team (CEO, CTO, CFO, founders)
- [ ] Funding history (VC rounds, investors, valuation)
- [ ] Revenue & employee estimates
- [ ] Growth signals (hiring, expansion, product launches)

**Approach:** Agentic (multi-source synthesis, adaptive enrichment)
**Source:** `app/agentic/company_strategies/`
**API Endpoints:**
- `POST /api/v1/companies/enrich` - Enrich single company
- `POST /api/v1/companies/batch/enrich` - Batch enrichment
- `POST /api/v1/companies/discover` - Discover companies by criteria
- `GET /api/v1/companies/{company_id}` - Full company profile
- `GET /api/v1/companies/search` - Search companies
- `GET /api/v1/companies/{company_id}/leadership` - Leadership team
- `GET /api/v1/companies/{company_id}/funding` - Funding history
- `GET /api/v1/companies/portfolio-companies` - Enriched portfolio companies for LP/FO

**Database Tables:**
- `private_companies` - Core company profiles
- `company_leadership` - Executives and key personnel
- `company_funding_rounds` - VC/PE funding history
- `company_metrics` - Revenue estimates, employee count (time series)
- `company_intelligence_jobs` - Job tracking with agent reasoning

**Agent Strategies:**
1. **Website Scraping** (HTML parsing) - About, Team, Press, Careers pages
2. **Funding APIs** (Crunchbase/PitchBook) - Funding history, investors, valuation
3. **SEC Form D** (API) - Private fundraising filings (>$1M raises)
4. **News Search** (LLM extraction) - Revenue estimates, growth stories, milestones
5. **Job Postings Analysis** (Structured scraping) - Hiring velocity, tech stack, locations
6. **Social/Product Signals** (AngelList, ProductHunt) - Startup-specific data

**Key Features:**
- Multi-source synthesis with conflict resolution
- Profile completeness scoring (0-100%)
- Data quality scoring based on source reliability
- Automatic integration with LP/FO portfolio companies
- Quarterly refresh mechanism for active monitoring
- LLM-powered entity extraction from unstructured text

**Expected Coverage:**
- Start with 500-1,000 companies (from LP/FO portfolios)
- 80-90% profile completeness for well-known companies
- 60-70% completeness for lesser-known private companies
- Average 4+ sources per company (validation)

**Use Cases:**
- M&A targeting (identify acquisition candidates)
- Competitive intelligence (track competitors)
- Due diligence (pre-screen companies)
- Portfolio monitoring (track LP/FO investments)
- Market research (understand private company landscape)

**Integration:**
- Links to `portfolio_companies` table (agentic portfolio research)
- Auto-enriches companies when discovered in LP/FO portfolios
- Enables queries like "show me all fintech companies in CalPERS portfolio with profiles"

**Documentation:** `docs/AGENT_PROMPTS/HANDOFF_private_company_intelligence.md`

**Status:** 🤖 **PLANNED** - Detailed implementation plan created, ready for development

**Estimated Timeline:** 4-5 weeks (Quick win: Website + SEC strategy in 1 week)

**Cost:** $0.10-0.20 per company (LLM + API costs)

---

## 31. Foot Traffic & Location Intelligence 🤖 PLANNED

**SPECIAL CATEGORY:** Multi-source foot traffic data aggregation (not a single API)

### Physical Location Activity Tracking

- [ ] Location discovery and POI enrichment
- [ ] Foot traffic time-series data (daily/weekly/monthly)
- [ ] Competitive benchmarking (compare chains)
- [ ] Trade area analysis
- [ ] Growth trend detection

**Approach:** Agentic (multi-source aggregation with validation)
**Source:** `app/agentic/traffic_strategies/`
**API Endpoints:**
- `POST /api/v1/foot-traffic/locations/discover` - Find locations to track
- `POST /api/v1/foot-traffic/locations/{id}/collect` - Collect traffic data
- `GET /api/v1/foot-traffic/locations/{id}/traffic` - Get time series
- `GET /api/v1/foot-traffic/brands/{brand}/aggregate` - Aggregate across brand
- `GET /api/v1/foot-traffic/compare` - Compare multiple brands

**Database Tables:**
- `locations` - Physical places (stores, restaurants, offices, venues)
- `foot_traffic_observations` - Time-series traffic data
- `location_metadata` - Hours, categories, trade areas, competitors
- `foot_traffic_collection_jobs` - Agent job tracking

**Data Sources:**
1. **Google Popular Times** (Free, scraping) - Hourly patterns, 60-80% coverage
2. **SafeGraph API** (Paid $100-500/mo) - Weekly visits, demographics, 80-90% coverage
3. **Placer.ai** (Paid $500-2K+/mo) - Retail analytics, competitive benchmarking
4. **Foursquare** (Freemium) - POI metadata, check-in data
5. **City Open Data** (Free) - Pedestrian counters in ~20-30 cities

**Key Features:**
- Multi-source validation (cross-check traffic data)
- Time-series analysis (trends, seasonality, growth rates)
- Competitive benchmarking (compare brands in same category)
- Trade area demographics (from Placer/SafeGraph)
- Alert system for traffic anomalies

**Use Cases:**
- Portfolio company monitoring (track foot traffic at portfolio companies' stores)
- Retail investment due diligence (evaluate chains before investing)
- Real estate analysis (assess property value by foot traffic)
- Competitive intelligence (compare foot traffic vs competitors)
- Early warning system (declining traffic = revenue risk)

**Expected Coverage:**
- 500-2,000 locations tracked (starting with portfolio companies)
- 80%+ data availability for major retail/restaurant chains
- Weekly/monthly traffic data back 1-2 years (with SafeGraph)

**Integration:**
- Links to `private_companies` table (retail/restaurant chains)
- Links to `portfolio_companies` (track portfolio company locations)

**Cost:** $0.05-0.20 per location per month ($25-400/month for 500-2,000 locations)

**Documentation:** `docs/AGENT_PROMPTS/HANDOFF_foot_traffic_intelligence.md`

**Status:** 🤖 **PLANNED** - Detailed implementation plan created

**Timeline:** 3-4 weeks

---

## 32. Management & Strategy Intelligence 🤖 PLANNED

**SPECIAL CATEGORY:** Multi-source management profiling and strategic analysis

### Leadership Quality & Strategic Direction Assessment

- [ ] Strategic initiative tracking (product launches, expansions, pivots)
- [ ] Management event monitoring (hires, departures, promotions)
- [ ] Operational metrics (employee sentiment, customer satisfaction)
- [ ] Executive profiling (backgrounds, tenure, track record)
- [ ] Strategic positioning (market focus, competitive strategy)

**Approach:** Agentic (LLM-powered synthesis from unstructured sources)
**Source:** `app/agentic/management_strategies/`
**API Endpoints:**
- `POST /api/v1/management/profile` - Profile company management & strategy
- `GET /api/v1/management/{company_id}/strategies` - Get strategic initiatives
- `GET /api/v1/management/{company_id}/events` - Get management events
- `GET /api/v1/management/{company_id}/metrics` - Get operational metrics
- `GET /api/v1/management/{company_id}/positioning` - Get strategic positioning

**Database Tables:**
- `company_strategies` - Strategic initiatives and pivots
- `management_events` - Leadership changes, hires, departures
- `operational_metrics` - Glassdoor ratings, employee sentiment, NPS
- `strategic_positioning` - Market positioning, competitive advantages
- `management_intelligence_jobs` - Agent job tracking

**Data Sources:**
1. **Company Press Releases/Blogs** (Free) - Strategic announcements, 70-90% coverage
2. **SEC Filings - MD&A** (Free) - Management discussion, public companies only
3. **Earnings Call Transcripts** (Free) - Strategic commentary, public companies
4. **Glassdoor/Indeed** (Scraping) - Employee sentiment, management ratings, 60-80% coverage
5. **Executive News Search** (LLM) - Leadership backgrounds, hires, departures
6. **Trade Publications** (Free/Paid) - Industry analysis, strategic shifts

**Key Features:**
- LLM-powered strategic theme extraction
- Management quality scoring (0-100 based on multiple factors)
- Strategic initiative classification (expansion, pivot, optimization)
- Executive background profiling (tenure, experience, track record)
- Alert system for major strategic shifts or management changes

**Strategic Signals Tracked:**
- Product launches and roadmap
- Geographic/market expansion
- Partnerships and acquisitions
- Business model pivots
- Leadership changes (significance scoring)
- Employee sentiment trends
- Competitive positioning shifts

**Use Cases:**
- Pre-investment due diligence (evaluate management quality)
- Portfolio monitoring (track strategic execution)
- Risk detection (executive departures, declining sentiment)
- Competitive intelligence (understand competitor strategies)
- Executive assessment (profile leadership teams)

**Expected Coverage:**
- 500-1,000 companies profiled
- 80%+ have strategic positioning data
- 60%+ have management quality scores
- 5+ strategic initiatives per company on average

**Integration:**
- Links to `private_companies` and `portfolio_companies`
- Links to `company_leadership` (executive profiling)
- Complements portfolio research (understand how investments are managed)

**Cost:** $0.15-0.30 per company (LLM extraction heavy)

**Documentation:** `docs/AGENT_PROMPTS/HANDOFF_management_strategy_intelligence.md`

**Status:** 🤖 **PLANNED** - Detailed implementation plan created

**Timeline:** 4 weeks

---

## 33. Prediction Market Intelligence 🤖 PLANNED

**SPECIAL CATEGORY:** Browser-based prediction market monitoring (not API-dependent)

### Market Consensus Tracking Across Platforms

- [ ] Kalshi markets (CFTC-regulated economic events)
- [ ] PredictIt markets (US political events)
- [ ] Polymarket markets (global events, crypto, business)
- [ ] Time-series probability tracking
- [ ] Alert system for significant shifts

**Approach:** Browser automation (agent navigates sites and extracts data)
**Source:** `app/agentic/prediction_markets/`
**API Endpoints:**
- `POST /api/v1/prediction-markets/monitor/all` - Monitor all platforms
- `POST /api/v1/prediction-markets/monitor/{platform}` - Single platform
- `GET /api/v1/prediction-markets/markets/top` - Top markets by volume/change
- `GET /api/v1/prediction-markets/markets/{id}/history` - Probability time series
- `GET /api/v1/prediction-markets/alerts` - Recent alerts
- `GET /api/v1/prediction-markets/dashboard` - Summary dashboard

**Database Tables:**
- `prediction_markets` - Market details (question, category, close date)
- `market_observations` - Time-series probability data
- `market_categories` - Classification (economics, politics, business)
- `market_alerts` - Significant probability shifts
- `prediction_market_jobs` - Agent job tracking

**Platforms Monitored:**
1. **Kalshi** (kalshi.com) - CFTC-regulated, economic events
   - Fed rate decisions, CPI, unemployment, GDP
   - Weather, climate events
   - High liquidity, real money markets
   
2. **PredictIt** (predictit.org) - Political prediction market
   - Presidential/Congressional elections
   - Legislative outcomes, appointments
   - Supreme Court decisions
   
3. **Polymarket** (polymarket.com) - Crypto-based, global
   - Business events (acquisitions, earnings)
   - Crypto markets (Bitcoin price, protocol launches)
   - International politics
   - Broader range than US-only platforms

**Key Features:**
- Browser-based extraction (no API keys needed)
- Hourly monitoring (automated)
- Probability change detection (>10% shifts trigger alerts)
- Time-series analysis (track trends over time)
- Cross-platform validation (same event on multiple platforms)
- Category classification (economics, politics, business)
- Link to sectors/companies (market impacts portfolio)

**Market Categories:**
- **Economics:** Fed decisions, recession, inflation, unemployment
- **Politics:** Elections, legislation, regulatory changes
- **Business:** Company acquisitions, earnings beats, product launches
- **Crypto:** Bitcoin price, Ethereum upgrades, protocol events
- **Climate:** Temperature, precipitation (affects commodities)

**Use Cases:**
- **Risk Management:** Quantify probability of adverse events
- **Scenario Planning:** "65% chance of rate cut" → adjust portfolio
- **Political Risk:** Track election probabilities for sector impacts
- **Market Timing:** Detect sentiment shifts before mainstream
- **Economic Forecasting:** Market consensus on Fed, recession, inflation

**Expected Coverage:**
- 50-100 markets tracked continuously
- Hourly updates on probabilities
- 90+ days historical data
- Alert on >10% probability shifts in 24 hours

**Integration:**
- Links to `market_categories` (relevant sectors/companies)
- Alerts can reference portfolio companies
- Dashboard shows markets relevant to portfolio

**Cost:** $0 (free - browser-based scraping, no APIs needed)

**Documentation:** `docs/AGENT_PROMPTS/HANDOFF_prediction_market_intelligence.md`

**Status:** 🤖 **PLANNED** - Detailed implementation plan created

**Timeline:** 4 weeks (Week 1: Kalshi, Week 2: Add other platforms, Week 3: Alerts, Week 4: Integration)

---

## Summary

**Total Data Sources:** 33 categories (28 traditional + 5 agentic)
**Fully Implemented:** 23 sources ✅
**Partially Implemented:** 2 sources (Alternative Data, Financial Institutions)
**Planned Agentic:** 5 agentic research capabilities 🤖
**Not Implemented:** 3 traditional sources

**Implementation Status:**
- ✅ **FULLY IMPLEMENTED (24):** Census, BLS, BEA, FRED, NOAA, EIA, USDA, CMS, SEC, Real Estate, Public LP Strategies, Family Offices, International Econ, OpenFEMA, FBI Crime, BTS, US Trade, Data Commons, Yelp, Kaggle, CFTC COT, FCC Broadband & Telecom, Treasury FiscalData, FDIC BankFind, **IRS SOI**
- ⚠️ **PARTIAL (2):** Alternative Data (Data Commons + Yelp yes, Google Trends deprecated), Financial Institutions (Treasury FiscalData + FDIC BankFind yes, FFIEC/NCUA not implemented)
- 🤖 **PLANNED AGENTIC (5):** 
  - Agentic Portfolio Research (LP/FO investments & deal flow) - 4-6 weeks
  - Private Company Intelligence (comprehensive company profiling) - 4-5 weeks
  - Foot Traffic & Location Intelligence (physical location activity) - 3-4 weeks
  - Management & Strategy Intelligence (leadership quality & strategy) - 4 weeks
  - Prediction Market Intelligence (betting markets for risk assessment) - 4 weeks
- ❌ **NOT IMPLEMENTED (3):** USPTO, Federal Register, Mobility, EPA, Additional Gov Data
