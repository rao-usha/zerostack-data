# Nexdata API Endpoint Reference

> **Base URL:** `http://localhost:8001/api/v1`
> **Swagger UI:** `http://localhost:8001/docs`
> **Total Endpoints:** ~560+  |  **Total Routers:** 100+

---

## Table of Contents

1. [API Key Requirements Summary](#api-key-requirements-summary)
2. [Macro Economic Data](#1-macro-economic-data)
3. [Trade & Commerce](#2-trade--commerce)
4. [Financial & Regulatory](#3-financial--regulatory)
5. [Energy & Agriculture](#4-energy--agriculture)
6. [Real Estate & Housing](#5-real-estate--housing)
7. [Alternative Data](#6-alternative-data)
8. [Site Intelligence Platform](#7-site-intelligence-platform)
9. [People & Org Charts](#8-people--org-charts)
10. [PE Intelligence](#9-pe-intelligence)
11. [Family Office & LP](#10-family-office--lp)
12. [Agentic Research & AI](#11-agentic-research--ai)
13. [Portfolio & Investment Tools](#12-portfolio--investment-tools)
14. [Infrastructure & Operations](#13-infrastructure--operations)
15. [Auth & User Management](#14-auth--user-management)

---

## API Key Requirements Summary

Most endpoints are **free / no API key needed**. The ones that do:

| Source | Env Variable | Required? | Status |
|--------|-------------|-----------|--------|
| **BEA** | `BEA_API_KEY` | Yes — all ingestion | Configured |
| **EIA** | `EIA_API_KEY` | Yes — all ingestion | Configured |
| **FRED** | `FRED_API_KEY` | Optional (higher rate limits) | Configured |
| **BLS** | `BLS_API_KEY` | Optional (higher rate limits) | Configured |
| **Census** | `CENSUS_SURVEY_API_KEY` | Yes — all ingestion | Configured |
| **FBI Crime** | `DATA_GOV_API` | Yes — all ingestion | Configured |
| **NOAA** | `NOAA_API_TOKEN` | Yes — ingestion & lookups | **Not set** |
| **USDA** | `USDA_API_KEY` | Yes — all ingestion | Configured |
| **Yelp** | `YELP_API_KEY` | Yes — all ingestion | **Not set** |
| **Kaggle** | `KAGGLE_USERNAME` + `KAGGLE_KEY` | Yes — download/ingest | Configured |
| **USPTO** | `USPTO_PATENTSVIEW_API_KEY` | Yes — ingestion only | **Not set** |
| **Data Commons** | `DATA_COMMONS_API_KEY` | Optional | Configured |
| **OpenAI** | `OPENAI_API_KEY` | For LLM-powered research | Configured |
| **Public API** | User-created API key | Yes — `/public/*` endpoints | Via `/api-keys` |

Keys are managed at **Settings > API Keys** in the UI, or via `PUT /api/v1/settings/api-keys`.

---

## 1. Macro Economic Data

### FRED (Federal Reserve Economic Data)
**Prefix:** `/fred` | **API Key:** Optional (`FRED_API_KEY` — higher rate limits)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/fred/ingest` | Ingest FRED category data (GDP, CPI, rates, etc.) | `category`, `series_ids`, `observation_start`, `observation_end` |
| POST | `/fred/ingest/batch` | Ingest multiple FRED categories at once | `categories[]`, `observation_start`, `observation_end` |
| GET | `/fred/categories` | List available FRED categories | — |
| GET | `/fred/series/{category}` | Get series IDs for a category | `category` |

**How it works:** Calls FRED API, stores series metadata in `fred_series` and time-series data in `fred_series_observations`. Categories include GDP, employment, inflation, interest rates, housing, and more. Each POST creates a background job — check status via `/jobs/{job_id}`.

---

### BEA (Bureau of Economic Analysis)
**Prefix:** `/bea` | **API Key:** Required (`BEA_API_KEY`)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/bea/nipa/ingest` | Ingest NIPA tables (GDP, PCE, income) | `table_name`, `frequency` (A/Q/M), `year` |
| POST | `/bea/regional/ingest` | Ingest regional economic data | `table_name`, `line_code`, `geo_fips`, `year` |
| POST | `/bea/gdp-industry/ingest` | Ingest GDP by industry | `table_id`, `frequency`, `year`, `industry` |
| POST | `/bea/international/ingest` | Ingest international transactions | `indicator`, `area_or_country`, `frequency`, `year` |
| GET | `/bea/datasets` | List BEA datasets and common tables | — |

**How it works:** Calls BEA REST API with your API key. Stores data in `bea_nipa_data`, `bea_regional_data`, `bea_industry_data`. NIPA frequency can be Annual, Quarterly, or Monthly.

---

### BLS (Bureau of Labor Statistics)
**Prefix:** `/bls` | **API Key:** Optional (`BLS_API_KEY` — 500 vs 25 daily requests)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/bls/{dataset}/ingest` | Ingest specific dataset (ces, cps, jolts, cpi, ppi, oes) | `dataset`, `start_year`, `end_year`, `series_ids` |
| POST | `/bls/series/ingest` | Ingest custom series by ID | `series_ids[]`, `start_year`, `end_year`, `dataset` |
| POST | `/bls/all/ingest` | Ingest all BLS datasets at once | `datasets[]`, `start_year`, `end_year` |
| GET | `/bls/reference/datasets` | List BLS datasets | — |
| GET | `/bls/reference/series` | Get common series | `dataset` |
| GET | `/bls/reference/series/{dataset}` | Series for specific dataset | `dataset` |
| GET | `/bls/reference/quick` | Popular series quick reference | — |

**How it works:** Calls BLS Public Data API. Without a key, limited to 25 requests/day with 10 series per request. With key, 500/day with 50 series. Stores in `bls_series`, `bls_series_data`, plus 8 dataset-specific tables.

---

### Treasury (US Treasury FiscalData)
**Prefix:** `/treasury` | **API Key:** None required (public API)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/treasury/debt/ingest` | Federal Debt Outstanding | `start_date`, `end_date` |
| POST | `/treasury/interest-rates/ingest` | Treasury Interest Rates & Yield Curves | `start_date`, `end_date`, `security_type` |
| POST | `/treasury/revenue-spending/ingest` | Monthly Treasury Statement | `start_date`, `end_date`, `classification` |
| POST | `/treasury/auctions/ingest` | Treasury Auction Results | `start_date`, `end_date`, `security_type` |
| POST | `/treasury/all/ingest` | Ingest all Treasury datasets | `start_date`, `end_date` |
| GET | `/treasury/reference/datasets` | Available datasets | — |
| GET | `/treasury/reference/security-types` | Security type codes | — |

**How it works:** Calls Treasury FiscalData API (api.fiscaldata.treasury.gov). No key needed. Stores in `treasury_yield_curves`, `treasury_securities`, `treasury_debt_to_penny`, `treasury_statements`, `treasury_exchange_rates`, `treasury_daily_balance`.

---

### Data Commons (Google)
**Prefix:** `/data-commons` | **API Key:** Optional (`DATA_COMMONS_API_KEY`)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/data-commons/stat-var/ingest` | Ingest statistical variable data | `variable_dcid`, `places[]` |
| POST | `/data-commons/place-stats/ingest` | Ingest variables for a single place | `place_dcid`, `variables[]` |
| POST | `/data-commons/us-states/ingest` | Ingest data for all US states | `variables[]` |
| GET | `/data-commons/variables` | List statistical variables | — |
| GET | `/data-commons/places` | List common places | — |

**How it works:** Calls Google Data Commons API. Variables use DCID format (e.g., `Count_Person`, `Median_Income_Person`). Places use geoId format (e.g., `geoId/06` for California). Stores in `data_commons_observations`.

---

### Prediction Markets
**Prefix:** `/prediction-markets` | **API Key:** None required

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/prediction-markets/monitor/all` | Monitor all platforms | `kalshi_categories[]`, `limit_per_platform` |
| POST | `/prediction-markets/monitor/kalshi` | Monitor Kalshi only | `categories[]`, `limit` |
| POST | `/prediction-markets/monitor/polymarket` | Monitor Polymarket only | `limit` |
| GET | `/prediction-markets/markets/top` | Top markets by volume | `platform`, `category`, `limit` |
| GET | `/prediction-markets/markets/category/{category}` | Markets by category | `category`, `limit` |
| GET | `/prediction-markets/markets/{market_id}` | Market details | — |
| GET | `/prediction-markets/markets/{market_id}/history` | Price/probability history | `days` |
| GET | `/prediction-markets/movers` | Biggest probability changes | `hours`, `limit` |
| GET | `/prediction-markets/dashboard` | Dashboard summary | — |
| GET | `/prediction-markets/alerts` | Market alerts | `severity`, `acknowledged`, `limit` |
| GET | `/prediction-markets/categories` | List categories | — |
| GET | `/prediction-markets/platforms` | Supported platforms | — |

**How it works:** Scrapes public APIs from Kalshi and Polymarket. Tracks contract prices, volume, and probability changes over time. Stores in `prediction_market_contracts`. No authentication needed — these are public markets.

---

### International Economics
**Prefix:** `/international` | **API Key:** None required (all public APIs)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/international/worldbank/wdi/ingest` | World Bank WDI indicators | `indicators[]`, `countries[]`, `start_year`, `end_year` |
| POST | `/international/worldbank/countries/ingest` | World Bank country metadata | — |
| POST | `/international/worldbank/indicators/ingest` | World Bank indicator metadata | `search`, `max_results` |
| GET | `/international/worldbank/indicators/common` | Common WDI indicators | — |
| POST | `/international/imf/ifs/ingest` | IMF International Financial Statistics | `indicator`, `countries[]`, `start_year`, `end_year` |
| POST | `/international/oecd/mei/ingest` | OECD Main Economic Indicators | `countries[]`, `subjects[]`, `start_period` |
| POST | `/international/oecd/kei/ingest` | OECD Key Economic Indicators | `countries[]`, `start_period` |
| POST | `/international/oecd/labor/ingest` | OECD Labor Statistics | `countries[]`, `start_period` |
| POST | `/international/oecd/trade/ingest` | OECD Trade in Services | `countries[]`, `start_period` |
| POST | `/international/oecd/tax/ingest` | OECD Tax Revenue Statistics | `countries[]`, `start_period` |
| POST | `/international/bis/eer/ingest` | BIS Effective Exchange Rates | `countries[]`, `eer_type` (R/N) |
| POST | `/international/bis/property/ingest` | BIS Property Price Data | `countries[]`, `start_period` |

**How it works:** Aggregates from 4 organizations (World Bank, IMF, OECD, BIS). All have free public APIs. Stores in `international_economic_data`, `international_trade_data`, `international_economic_indicators`. Countries use ISO 3166-1 alpha-3 codes (USA, GBR, DEU, etc.).

---

## 2. Trade & Commerce

### US Trade (Census Bureau)
**Prefix:** `/us-trade` | **API Key:** None required

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/us-trade/exports/hs/ingest` | US exports by HS code | `year`, `month`, `hs_code`, `country` |
| POST | `/us-trade/imports/hs/ingest` | US imports by HS code | `year`, `month`, `hs_code`, `country` |
| POST | `/us-trade/exports/state/ingest` | State-level export data | `year`, `month`, `state`, `hs_code` |
| POST | `/us-trade/port/ingest` | Trade by customs district | `year`, `trade_type`, `district` |
| POST | `/us-trade/summary/ingest` | Aggregated trade summary by country | `year`, `month` |
| GET | `/us-trade/datasets` | Available datasets | — |
| GET | `/us-trade/reference/hs-chapters` | HS commodity chapters (2-digit) | — |
| GET | `/us-trade/reference/countries` | Census country codes | — |
| GET | `/us-trade/reference/districts` | US Customs Districts | — |
| GET | `/us-trade/reference/states` | State FIPS codes | — |

**How it works:** Calls Census Bureau International Trade API. Uses HS (Harmonized System) codes for commodity classification. Stores in `us_trade_data`, `us_trade_partners`, `us_trade_commodities`, `us_trade_exports_state`.

---

### CFTC Commitments of Traders
**Prefix:** `/cftc-cot` | **API Key:** None required

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/cftc-cot/ingest` | Ingest COT report data | `year`, `report_type`, `combined` |
| GET | `/cftc-cot/reference/contracts` | Major futures contracts | — |
| GET | `/cftc-cot/reference/commodity-groups` | Commodity groupings | — |
| GET | `/cftc-cot/reference/report-types` | Report type descriptions | — |

**How it works:** Downloads from CFTC public data (Socrata). Shows commercial/non-commercial positioning in futures markets. Report types: Legacy, Disaggregated, Traders in Financial Futures. Stores in `cftc_cot_reports`, `cftc_cot_legacy_combined`.

---

### IRS Statistics of Income
**Prefix:** `/irs-soi` | **API Key:** None required

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/irs-soi/zip-income/ingest` | Income by ZIP code | `year`, `use_cache` |
| POST | `/irs-soi/county-income/ingest` | Income by county | `year`, `use_cache` |
| POST | `/irs-soi/migration/ingest` | County-to-county migration | `year`, `flow_type`, `use_cache` |
| POST | `/irs-soi/business-income/ingest` | Business income by ZIP | `year`, `use_cache` |
| POST | `/irs-soi/all/ingest` | All IRS SOI datasets | `year`, `use_cache` |
| GET | `/irs-soi/reference/agi-brackets` | AGI bracket definitions | — |
| GET | `/irs-soi/reference/years` | Available tax years | — |
| GET | `/irs-soi/datasets` | Dataset descriptions | — |

**How it works:** Downloads from IRS SOI public data files. Shows individual/business income distributions by geography and AGI bracket. Migration data shows where people are moving (and their income). Stores in `irs_soi_data`, `irs_soi_zip_income`, `irs_soi_migration`.

---

### Census Bureau
**Prefix:** `/census` | **API Key:** Required (`CENSUS_SURVEY_API_KEY`)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/census/state` | Ingest at STATE level | `survey`, `year`, `table_id`, `include_geojson` |
| POST | `/census/county` | Ingest at COUNTY level | `survey`, `year`, `table_id`, `state_fips` |
| POST | `/census/tract` | Ingest at TRACT level | `survey`, `year`, `table_id`, `state_fips`, `county_fips` |
| POST | `/census/zip` | Ingest at ZIP (ZCTA) level | `survey`, `year`, `table_id`, `state_fips` |
| POST | `/census/batch/state` | Batch: multiple years | `survey`, `years[]`, `table_id` |
| POST | `/census/batch/county` | Batch: county multi-year | `survey`, `years[]`, `table_id`, `state_fips` |
| GET | `/census/batch/status` | Batch job status | `job_ids` |
| GET | `/census/metadata/variables/{dataset_id}` | Variable definitions | `dataset_id` |
| GET | `/census/metadata/search` | Search variables | `dataset_id`, `query` |
| GET | `/census/metadata/column/{dataset_id}/{col}` | Column info | `dataset_id`, `column_name` |
| GET | `/census/metadata/datasets` | All datasets | — |

**How it works:** Calls Census Bureau API. Surveys include ACS 1-Year, ACS 5-Year, Decennial, Economic Census. Table IDs like `B01001` (age/sex), `B19013` (median income). Stores in `census_acs_data`, `census_geo_data`, `census_batch_*`.

---

## 3. Financial & Regulatory

### SEC (EDGAR)
**Prefix:** `/sec` | **API Key:** None required (public EDGAR)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/sec/ingest/company` | Ingest filings for a company | `cik`, `filing_types`, `start_date`, `end_date` |
| POST | `/sec/ingest/multiple` | Ingest for multiple companies | `ciks`, `filing_types` |
| POST | `/sec/ingest/financial-data` | Ingest XBRL financial data | `cik` |
| POST | `/sec/ingest/full-company` | Both filings + financials | `cik`, `filing_types` |
| POST | `/sec/ingest/industrial-companies` | XBRL for all industrial companies | — |
| GET | `/sec/supported-filing-types` | List filing types | — |
| GET | `/sec/common-companies` | Major company CIK numbers | — |
| POST | `/sec/form-adv/ingest/family-offices` | Form ADV for family offices | `family_office_names[]` |
| POST | `/sec/form-adv/ingest/crd` | Form ADV by CRD number | `crd_number` |
| GET | `/sec/form-adv/firms` | Query Form ADV firms | `limit`, `offset`, `family_office_only`, `state` |
| GET | `/sec/form-adv/firms/{crd_number}` | Firm detail by CRD | — |
| GET | `/sec/form-adv/stats` | Form ADV statistics | — |

**How it works:** Calls SEC EDGAR (no key needed, but rate-limited to 10 req/sec with User-Agent). Filing types include 10-K, 10-Q, 8-K, 13F, etc. XBRL extracts structured financial facts. Form ADV reveals investment advisor details including AUM, employee count, and whether they're a family office. Stores in `sec_filings`, `sec_xbrl_facts`, `sec_company_submissions`, `form_adv_firms`, `form_adv_personnel`.

---

### FDIC (Banking)
**Prefix:** `/fdic` | **API Key:** None required

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/fdic/financials/ingest` | Bank financial data | `cert`, `report_date`, `year`, `limit` |
| POST | `/fdic/institutions/ingest` | Bank institution info | `active_only`, `state`, `limit` |
| POST | `/fdic/failed-banks/ingest` | Failed banks list | `year_start`, `year_end`, `limit` |
| POST | `/fdic/deposits/ingest` | Summary of Deposits | `year`, `cert`, `state`, `limit` |
| POST | `/fdic/all/ingest` | All FDIC datasets | all options above |
| GET | `/fdic/reference/metrics` | Financial metrics | — |
| GET | `/fdic/reference/datasets` | Dataset info | — |
| GET | `/fdic/reference/major-banks` | Major bank cert numbers | — |
| GET | `/fdic/search` | Search banks by name/city | `query`, `active_only`, `limit` |

**How it works:** Calls FDIC BankFind API. No key needed. Stores in `fdic_institutions`, `fdic_financials`. Major bank cert numbers provided for quick lookups (JPMorgan = 628, BofA = 3510, etc.).

---

### FCC Broadband
**Prefix:** `/fcc-broadband` | **API Key:** None required

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/fcc-broadband/state/ingest` | Broadband coverage by state | `state_codes[]`, `include_summary` |
| POST | `/fcc-broadband/all-states/ingest` | All 50 states + DC | `include_summary` |
| POST | `/fcc-broadband/county/ingest` | Coverage by county | `county_fips_codes[]` |
| GET | `/fcc-broadband/reference/states` | State codes & FIPS | — |
| GET | `/fcc-broadband/reference/technologies` | Technology types | — |
| GET | `/fcc-broadband/reference/speed-tiers` | Speed tier classifications | — |
| GET | `/fcc-broadband/datasets` | Dataset descriptions | — |

---

## 4. Energy & Agriculture

### EIA (Energy Information Administration)
**Prefix:** `/eia` | **API Key:** Required (`EIA_API_KEY`)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/eia/petroleum/ingest` | Petroleum data (crude, products, stocks) | `subcategory`, `route`, `frequency`, `start`, `end`, `facets` |
| POST | `/eia/natural-gas/ingest` | Natural gas data | `subcategory`, `route`, `frequency` |
| POST | `/eia/electricity/ingest` | Electricity data (generation, capacity) | `subcategory`, `route`, `frequency` |
| POST | `/eia/retail-gas-prices/ingest` | Retail gasoline prices | `frequency`, `start`, `end` |
| POST | `/eia/steo/ingest` | Short-Term Energy Outlook projections | `frequency`, `start`, `end` |

**How it works:** Calls EIA API v2. Uses a hierarchical route structure (e.g., `petroleum/summary`, `electricity/retail-sales`). Facets filter by region, product, etc. Stores in `eia_series_data`, `eia_petroleum_data`, `eia_electricity_data`, `eia_coal_data`.

---

### USDA (Agriculture)
**Prefix:** `/usda` | **API Key:** Required (`USDA_API_KEY`)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/usda/crop/ingest` | Crop data for commodity | `commodity`, `year`, `state`, `all_stats` |
| POST | `/usda/livestock/ingest` | Livestock inventory | `commodity`, `year`, `state` |
| POST | `/usda/annual-summary/ingest` | Annual crop production summary | `year` |
| POST | `/usda/all-major-crops/ingest` | All major crops (CORN, SOYBEANS, WHEAT, COTTON, RICE) | `year` |
| GET | `/usda/reference/commodities` | Available commodities | — |
| GET | `/usda/reference/crop-states` | Top producing states | — |
| GET | `/usda/reference/state-fips` | State FIPS codes | — |

**How it works:** Calls USDA NASS QuickStats API. Stores in `usda_*` tables.

---

### NOAA (Climate/Weather)
**Prefix:** `/noaa` | **API Key:** Required (`NOAA_API_TOKEN`)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/noaa/ingest` | Ingest climate/weather data | `token`, `dataset_key`, `start_date`, `end_date`, `location_id`, `station_id` |
| GET | `/noaa/datasets` | List NOAA datasets | — |
| GET | `/noaa/datasets/{dataset_key}` | Dataset details | — |
| GET | `/noaa/locations` | Available locations | `token`, `dataset_id` |
| GET | `/noaa/stations` | Weather stations | `token`, `dataset_id`, `location_id` |
| GET | `/noaa/data-types` | Available data types | `token`, `dataset_id` |

**How it works:** Calls NOAA Climate Data Online API. Token passed as query parameter (not env var by default). Datasets include GHCND (daily), GSOM (monthly), GSOY (annual). Stores in `noaa_*` tables. **Note: NOAA_API_TOKEN not currently configured — get free token at ncdc.noaa.gov.**

---

### FEMA (Emergency Management)
**Prefix:** `/fema` | **API Key:** None required

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/fema/disasters/ingest` | Disaster declarations | `state`, `year`, `disaster_type`, `max_records` |
| POST | `/fema/public-assistance/ingest` | PA funded projects | `state`, `disaster_number`, `max_records` |
| POST | `/fema/hazard-mitigation/ingest` | Hazard Mitigation Assistance | `state`, `program_area`, `max_records` |
| GET | `/fema/datasets` | Available datasets | — |

---

### FBI Crime Statistics
**Prefix:** `/fbi-crime` | **API Key:** Required (`DATA_GOV_API`)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/fbi-crime/estimates/ingest` | Crime estimates | `scope`, `offenses`, `states` |
| POST | `/fbi-crime/summarized/ingest` | Summarized agency data | `states`, `offenses`, `since`, `until` |
| POST | `/fbi-crime/nibrs/ingest` | NIBRS incident data | `states`, `variables` |
| POST | `/fbi-crime/hate-crime/ingest` | Hate crime statistics | `states` |
| POST | `/fbi-crime/leoka/ingest` | Law Enforcement Officers K&A | `states` |
| POST | `/fbi-crime/ingest/all` | All FBI datasets | `datasets`, `include_states` |
| GET | `/fbi-crime/datasets` | Available datasets | — |
| GET | `/fbi-crime/offenses` | Offense types | — |
| GET | `/fbi-crime/states` | State abbreviations | — |

---

## 5. Real Estate & Housing

### Real Estate
**Prefix:** `/realestate` | **API Key:** None required

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/realestate/fhfa/ingest` | FHFA House Price Index | `geography_type`, `start_date`, `end_date` |
| POST | `/realestate/hud/ingest` | HUD Building Permits & Housing Starts | `geography_type`, `geography_id` |
| POST | `/realestate/redfin/ingest` | Redfin housing market data | `region_type`, `property_type` |
| POST | `/realestate/osm/ingest` | OpenStreetMap building footprints | `bounding_box`, `building_type`, `limit` |
| GET | `/realestate/fhfa/status/{job_id}` | Job status | — |
| GET | `/realestate/hud/status/{job_id}` | Job status | — |
| GET | `/realestate/redfin/status/{job_id}` | Job status | — |
| GET | `/realestate/osm/status/{job_id}` | Job status | — |
| GET | `/realestate/info` | Available data sources | — |

---

### CMS (Healthcare/Medicare)
**Prefix:** `/cms` | **API Key:** None required

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/cms/ingest/medicare-utilization` | Medicare Provider Utilization | `year`, `state`, `limit` |
| POST | `/cms/ingest/hospital-cost-reports` | Hospital Cost Reports (HCRIS) | `year`, `limit` |
| POST | `/cms/ingest/drug-pricing` | Medicare Part D Drug Spending | `year`, `brand_name`, `limit` |
| GET | `/cms/datasets` | All CMS datasets | — |
| GET | `/cms/datasets/{dataset_type}/schema` | Dataset schema | — |

---

## 6. Alternative Data

### Yelp Business Data
**Prefix:** `/yelp` | **API Key:** Required (`YELP_API_KEY`)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/yelp/businesses/ingest` | Ingest business listings for location | `location`, `term`, `categories`, `limit` |
| POST | `/yelp/businesses/multi-location/ingest` | Multiple locations at once | `locations[]`, `term`, `limit_per_location` |
| POST | `/yelp/categories/ingest` | All Yelp categories | — |
| GET | `/yelp/categories` | Common categories | — |
| GET | `/yelp/cities` | Major US cities | — |
| GET | `/yelp/api-limits` | Rate limit info | — |

---

### Kaggle (M5 Forecasting)
**Prefix:** `/kaggle` | **API Key:** Required (`KAGGLE_USERNAME` + `KAGGLE_KEY`)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/kaggle/m5/ingest` | Ingest M5 dataset | `force_download`, `limit_items` |
| POST | `/kaggle/m5/prepare-tables` | Create DB tables only | — |
| GET | `/kaggle/m5/info` | Dataset info | — |
| GET | `/kaggle/m5/files` | List available files | — |
| GET | `/kaggle/m5/schema` | DB schema | — |

---

### Foot Traffic & Location Intelligence
**Prefix:** `/foot-traffic` | **API Key:** Varies by strategy (SafeGraph, Foursquare, Google)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/foot-traffic/locations/discover` | Discover locations for a brand | `brand_name`, `city`, `state`, `latitude`, `longitude` |
| GET | `/foot-traffic/locations` | List tracked locations | `brand_name`, `city`, `state`, `category` |
| POST | `/foot-traffic/locations/{id}/collect` | Collect traffic data | `start_date`, `end_date` |
| POST | `/foot-traffic/collect` | Multi-location collection | `brand_name`, `city` |
| GET | `/foot-traffic/locations/{id}/traffic` | Traffic time series | `start_date`, `end_date`, `granularity` |
| GET | `/foot-traffic/brands/{brand}/aggregate` | Aggregated brand traffic | `city`, `state` |
| GET | `/foot-traffic/compare` | Compare brands | `brand_names[]`, `city` |
| GET | `/foot-traffic/sources` | Available data sources | — |

---

### USPTO (Patents)
**Prefix:** `/uspto` | **API Key:** Required for ingestion (`USPTO_PATENTSVIEW_API_KEY`)

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/uspto/ingest/assignee` | Ingest patents by assignee | `assignee_name`, `date_from`, `date_to`, `max_patents` |
| POST | `/uspto/ingest/cpc` | Ingest by CPC code | `cpc_code`, `date_from`, `date_to` |
| POST | `/uspto/ingest/search` | Ingest by text search | `search_query`, `date_from`, `date_to` |
| GET | `/uspto/patents` | Search local DB | `query`, `assignee`, `cpc_code`, `limit` |
| GET | `/uspto/patents/{patent_id}` | Get patent | — |
| GET | `/uspto/assignees` | Search assignees | `name`, `limit` |
| GET | `/uspto/inventors` | Search inventors | `name`, `limit` |
| GET | `/uspto/cpc-codes` | CPC classification codes | — |
| GET | `/uspto/major-assignees` | Major tech assignees | — |

---

### BTS (Transportation Statistics)
**Prefix:** `/bts` | **API Key:** None required

| Method | Path | Description | Key Params |
|--------|------|-------------|------------|
| POST | `/bts/border-crossing/ingest` | Border crossing data | `start_date`, `end_date`, `state`, `border`, `measure` |
| POST | `/bts/vmt/ingest` | Vehicle Miles Traveled | `start_date`, `end_date`, `state` |
| POST | `/bts/faf/ingest` | Freight Analysis Framework | `version` |
| GET | `/bts/datasets` | Available datasets | — |

---

## 7. Site Intelligence Platform

All site intel endpoints are **query-only** (no ingestion triggers here) and require **no API keys**. Data is populated via the site intel collection system (`POST /site-intel/sites/collect`).

### Power Infrastructure (`/site-intel/power`)
12 endpoints: `plants`, `plants/nearby`, `substations`, `substations/nearby`, `utilities`, `utilities/at-location`, `prices`, `prices/comparison`, `interconnection-queue`, `renewable-potential`, `summary`

Search power plants by state/fuel/capacity, find nearest substations, compare electricity prices, check solar/wind potential.

### Telecom Infrastructure (`/site-intel/telecom`)
9 endpoints: `broadband`, `broadband/at-location`, `ix`, `ix/nearby`, `data-centers`, `data-centers/nearby`, `submarine-cables`, `connectivity-score`, `summary`

Search broadband providers, find Internet Exchanges and data centers, get composite connectivity scores.

### Transport Infrastructure (`/site-intel/transport`)
10 endpoints: `intermodal`, `intermodal/nearby`, `rail/access`, `ports`, `ports/nearby`, `ports/{code}/throughput`, `airports`, `airports/nearby`, `heavy-haul`, `summary`

Find intermodal terminals, check rail access, search ports with throughput history, find cargo airports.

### Labor Markets (`/site-intel/labor`)
8 endpoints: `areas`, `wages`, `wages/comparison`, `employment`, `commute-shed`, `education`, `workforce-score`, `summary`

Search wages by occupation/area, compare across locations, get commute sheds and workforce scores.

### Risk Assessment (`/site-intel/risk`)
12 endpoints: `flood`, `flood/at-location`, `nri`, `nri/county/{fips}`, `nri/by-state/{state}`, `seismic/at-location`, `faults/nearby`, `climate/at-location`, `environmental/nearby`, `wetlands/at-location`, `score`, `summary`

Flood zones, seismic hazard, climate data, EPA facilities, FEMA National Risk Index, composite risk scores.

### Incentives & Zones (`/site-intel/incentives`)
12 endpoints: `opportunity-zones`, `opportunity-zones/at-location`, `ftz`, `ftz/nearby`, `programs`, `programs/by-state/{state}`, `deals`, `deals/benchmark`, `sites`, `sites/nearby`, `zoning/at-location`, `summary`

Opportunity Zones, Foreign Trade Zones, state incentive programs, disclosed incentive deals (Good Jobs First).

### Logistics & Supply Chain (`/site-intel/logistics`)
25 endpoints: container rates, truck rates, FMCSA carriers, port throughput, air cargo, trade gateways, 3PL companies, warehouse listings, freight rates, and summaries.

### Water & Utilities (`/site-intel/water-utilities`)
12 endpoints: USGS monitoring sites, EPA water systems + violations, gas pipelines, gas storage, utility electricity rates.

### Site Scoring & Collection (`/site-intel/sites`)
14 endpoints:

| Method | Path | Description |
|--------|------|-------------|
| POST | `/site-intel/sites/collect` | Trigger site intel data collection for all domains |
| GET | `/site-intel/sites/collect/status` | Collection status across all collectors |
| GET | `/site-intel/sites/watermarks` | Last-collected timestamps per source |
| DELETE | `/site-intel/sites/watermarks/{domain}/{source}` | Clear watermark to force re-sync |
| GET | `/site-intel/sites/collect/stream` | SSE stream for live progress |
| POST | `/site-intel/sites/collect-with-deps` | Collection with dependency ordering |
| POST | `/site-intel/sites/score` | Score a specific location |
| POST | `/site-intel/sites/compare` | Compare multiple locations |
| POST | `/site-intel/sites/report` | Generate site selection report |

---

## 8. People & Org Charts

All people endpoints require **no API keys** (data comes from web scraping + SEC EDGAR).

### People & Leadership (`/people`)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/people` | List people (search, company_id, title_level, is_board_member) |
| GET | `/people/{person_id}` | Detailed person info with roles, experience, education |
| GET | `/people/changes/feed` | Leadership changes feed |
| GET | `/people/search/executives` | Quick executive search by name |

### People Collection Jobs (`/people-jobs`)
| Method | Path | Description |
|--------|------|-------------|
| POST | `/people-jobs/test/{company_id}` | Run collection with diagnostics |
| POST | `/people-jobs/deep-collect/{company_id}` | Deep multi-phase collection (SEC + website + news + org chart) |
| POST | `/people-jobs/recursive-collect/{company_id}` | Recursive corporate structure discovery |
| POST | `/people-jobs/schedule` | Schedule collection job |
| POST | `/people-jobs/process` | Trigger processing of pending jobs |
| GET | `/people-jobs/` | List collection jobs |
| GET | `/people-jobs/stats` | Collection statistics |
| GET | `/people-jobs/alerts/recent` | Recent leadership change alerts |
| GET | `/people-jobs/digest/weekly` | Weekly leadership changes digest |

### Company Leadership (`/companies`)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/companies/{id}/leadership` | Current leadership team |
| GET | `/companies/{id}/leadership/history` | Change history |
| GET | `/companies/{id}/leadership/compare` | Compare across companies |
| POST | `/companies/{id}/leadership/refresh` | Trigger refresh |

### People Analytics (`/people-analytics`)
11 endpoints: industry stats, talent flow, trends, hot roles, company benchmarks, portfolio analytics.

### People Portfolios (`/people-portfolios`)
8 endpoints: CRUD for portfolios, add/remove companies, track leadership changes across portfolio.

### People Watchlists (`/people-watchlists`)
9 endpoints: CRUD for watchlists, add/remove people, get alerts for tracked people.

### People Reports (`/people-reports`)
6 endpoints: Management assessment reports, peer comparison reports, export to JSON/CSV.

### People Data Quality (`/people-data-quality`)
12 endpoints: Quality stats, freshness, duplicates, email inference, MX checks, company coverage.

### People Deduplication (`/people-dedup`)
5 endpoints: Scan for duplicates, list candidates, approve/reject merges, history.

---

## 9. PE Intelligence

All PE endpoints require **no API keys**.

### PE Firms (`/pe/firms`)
9 endpoints: List/search/create PE firms, get portfolio/funds/team, stats, delete.

### PE Portfolio Companies (`/pe/companies`)
9 endpoints: List/search/create companies, leadership, financials, valuations, competitors, news.

### PE Deals (`/pe/deals`)
6 endpoints: List/search/create deals, deal details with participants/advisors, stats, recent activity.

### PE People (`/pe/people`)
7 endpoints: List/search/create people, education, experience, deal involvement.

### PE Collection (`/pe/collection`)
| Method | Path | Description |
|--------|------|-------------|
| POST | `/pe/collection/collect` | Trigger PE data collection (specify entity_type, sources) |
| GET | `/pe/collection/sources` | List registered collection sources |

---

## 10. Family Office & LP

### Family Offices (`/family-offices`)
| Method | Path | Description |
|--------|------|-------------|
| POST | `/family-offices/` | Create/update family office |
| GET | `/family-offices/` | List with filters (region, country, status) |
| GET | `/family-offices/{id}` | Detailed info |
| GET | `/family-offices/stats/overview` | Overview stats |
| POST | `/family-offices/contacts/extract-from-websites` | Extract contacts from FO websites |
| GET | `/family-offices/{id}/contacts` | Get contacts |
| GET | `/family-offices/contacts/summary` | Contact coverage |
| GET | `/family-offices/contacts/export` | Export as CSV |

### FO Collection (`/fo-collection`)
10 endpoints: Seed FOs, create collection jobs, collect single FO, coverage stats, registry, contacts, sector grouping, active investors, comprehensive summaries.

### LP Collection (`/lp-collection`)
21 endpoints: Create collection jobs, collect single LP, stale data refresh, schedules (CRUD), coverage stats, governance, performance, allocation history, 13F holdings, external managers, contacts, summaries.

---

## 11. Agentic Research & AI

### Autonomous Research (`/agents`)
| Method | Path | Description | Notes |
|--------|------|-------------|-------|
| POST | `/agents/deep-research` | Multi-turn LLM research on a company | Uses OPENAI_API_KEY |
| POST | `/agents/research/company` | Autonomous research across all data sources | Uses OPENAI_API_KEY |
| POST | `/agents/research/batch` | Research multiple companies (max 10) | Uses OPENAI_API_KEY |
| GET | `/agents/research/{job_id}` | Get research results | — |
| GET | `/agents/research/company/{name}` | Get cached research | — |
| GET | `/agents/research/stats` | Agent statistics | — |
| GET | `/agents/sources` | Available data sources | — |

### Due Diligence (`/diligence`)
6 endpoints: Start diligence (standard/quick/deep templates), get reports, list jobs, stats.

### Competitive Intelligence (`/competitive`)
6 endpoints: Analyze competitive landscape, track movements, compare companies, moat assessment.

### Anomaly Detection (`/anomalies`)
9 endpoints: Recent anomalies, company-specific, deep investigation, pattern learning, scans.

### Market Scanner (`/market`)
9 endpoints: Market signals, trends, opportunities, intelligence briefs, signal history.

### News Monitor (`/monitors/news`)
12 endpoints: Watch items, personalized feed, AI digests, breaking alerts.

### Data Hunter (`/hunter`)
8 endpoints: Start hunts to fill data gaps, scan for gaps, entity-specific hunts, provenance.

### Report Generation (`/ai-reports`)
8 endpoints: Generate reports (company, industry, market, due diligence), custom templates, export.

### Multi-Agent Workflows (`/workflows`)
8 endpoints: Start workflows, custom templates, list available agents, execution stats.

### LLM Costs (`/llm-costs`)
3 endpoints: Cost summary, per-job costs, daily breakdown.

---

## 12. Portfolio & Investment Tools

### Search (`/search`)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/search` | Full-text search with fuzzy matching |
| GET | `/search/suggest` | Autocomplete suggestions |
| POST | `/search/reindex` | Trigger reindex |
| GET | `/search/stats` | Index statistics |

### Discovery (`/discover`) — 3 endpoints: Similar investors, recommendations, overlap analysis.

### Portfolio Alerts (`/alerts`) — 8 endpoints: Subscribe, manage subscriptions, get/acknowledge alerts.

### Watchlists (`/watchlists`) — 8 endpoints + 6 saved search endpoints.

### Analytics (`/analytics`) — 5 endpoints: Overview, investor analytics, trends, top movers, industry breakdown.

### Comparison (`/compare`) — 3 endpoints: Portfolio comparison, history, industry allocation.

### Network Graph (`/network`) — 5 endpoints: Co-investor graph, ego network, central nodes, clusters, path finding.

### Trends (`/trends`) — 6 endpoints: Sector trends, emerging sectors, geographic, stages, LP type comparison.

### Enrichment (`/enrichment`) — 10 endpoints: Company/investor enrichment, contacts, AUM history, preferences.

### Import (`/import`) — 6 endpoints: Upload CSV/Excel, preview, confirm, status, history, rollback.

### News Feed (`/news`) — 6 endpoints: Aggregated feed, company news, investor news, SEC filings.

### Reports (`/reports`) — 6 endpoints: Templates, generate, download, list, delete.

### Deals Pipeline (`/deals`) — 9 endpoints: CRUD for deals, pipeline summary, stages, activities.

### Benchmarks (`/benchmarks`) — 4 endpoints: Investor benchmarks, peer groups, sector benchmarks, diversification.

---

## 13. Infrastructure & Operations

### Jobs (`/jobs`)
| Method | Path | Description |
|--------|------|-------------|
| POST | `/jobs` | Create ingestion job |
| GET | `/jobs/{job_id}` | Job status and details |
| GET | `/jobs` | List jobs (filter by source, status) |
| GET | `/jobs/failed/summary` | Failed jobs summary |
| POST | `/jobs/{job_id}/retry` | Retry failed job |
| POST | `/jobs/retry/all` | Retry all eligible failed jobs |
| GET | `/jobs/{job_id}/validate` | Validate data quality for completed job |
| GET | `/jobs/monitoring/metrics` | Job metrics |
| GET | `/jobs/monitoring/health` | Source health status |
| GET | `/jobs/monitoring/alerts` | Active failure alerts |
| GET | `/jobs/monitoring/dashboard` | Comprehensive dashboard |

### Schedules (`/schedules`) — 21 endpoints: CRUD for schedules, activate/pause, manual run, history, default templates, stuck job cleanup, retry processing.

### Webhooks (`/webhooks`) — 12 endpoints: CRUD, activate/pause, test, delivery history/stats, alert checking.

### Job Chains (`/chains`) — 13 endpoints: CRUD for chains, execute, execution history, dependency management.

### Rate Limits (`/rate-limits`) — 12 endpoints: CRUD per source, enable/disable, stats, reset, defaults.

### Data Quality (`/data-quality`) — 15 endpoints: Rule CRUD, evaluate, results, reports.

### Templates (`/templates`) — 13 endpoints: Bulk ingestion template CRUD, execute, execution tracking, built-ins.

### Data Lineage (`/lineage`) — 18 endpoints: Node/edge CRUD, upstream/downstream tracing, impact analysis, dataset versioning.

### Export (`/export`)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/export/formats` | Supported formats (CSV, JSON, Parquet) |
| GET | `/export/tables` | All 331 tables with row counts |
| GET | `/export/tables/{name}/columns` | Column names |
| GET | `/export/tables/{name}/preview` | Paginated data preview with sorting |
| POST | `/export/jobs` | Create export job |
| GET | `/export/jobs/{id}/download` | Download file |
| POST | `/export/cleanup` | Clean up expired exports |

### GeoJSON (`/geojson`) — 5 endpoints: Boundary datasets, search, feature collections.

### Source Configs (`/source-configs`) — 4 endpoints: CRUD for per-source timeout, retry, rate limit configuration.

### Audit Trail (`/audit-trail`) — 2 endpoints: Query audit trail, summary.

### Settings (`/settings`)
| Method | Path | Description |
|--------|------|-------------|
| GET | `/settings/api-keys` | List all configurable API keys with status |
| PUT | `/settings/api-keys` | Save/update an API key |
| DELETE | `/settings/api-keys/{source}` | Remove a stored key |
| POST | `/settings/api-keys/{source}/test` | Test an API key |

---

## 14. Auth & User Management

### Authentication (`/auth`)
| Method | Path | Description | Auth Required |
|--------|------|-------------|---------------|
| POST | `/auth/register` | Register new user | No |
| POST | `/auth/login` | Login, get access token | No |
| POST | `/auth/logout` | Invalidate tokens | Bearer token |
| POST | `/auth/refresh` | Refresh access token | Refresh token |
| GET | `/auth/me` | Current user profile | Bearer token |
| PATCH | `/auth/me` | Update profile | Bearer token |
| POST | `/auth/password/change` | Change password | Bearer token |
| POST | `/auth/password/reset-request` | Request reset token | No |
| POST | `/auth/password/reset` | Reset with token | No |

### Workspaces (`/workspaces`) — 11 endpoints: CRUD for workspaces, member management, invitations. All require Bearer token.

### API Keys (`/api-keys`) — 6 endpoints: Create/list/update/revoke API keys, usage stats.

### Public API (`/public`) — 3 endpoints: `/public/investors`, `/public/investors/{id}`, `/public/search`. **Requires X-API-Key header.**

---

## GraphQL

**URL:** `http://localhost:8001/graphql`

A GraphQL layer sits on top of the REST API, providing flexible querying across all data domains. Access via the GraphiQL playground at `/graphql`.
