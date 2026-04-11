# Nexdata — API Keys & Manual Requirements

This document is the authoritative reference for every credential, external registration, and manual setup step required to run the Nexdata platform. Organized by priority.

**Last updated:** 2026-03-30

---

## 1. Critical — Platform Won't Start Without These

| Variable | What It Is | How to Get It | Notes |
|----------|-----------|---------------|-------|
| `DATABASE_URL` | PostgreSQL connection string | Local: auto-set by docker-compose | `postgresql://nexdata:nexdata_dev_password@postgres:5432/nexdata` |

### Manual step: docker-compose startup
```bash
docker-compose up -d --build   # first time
docker-compose restart api     # after code changes (wait 20-30s)
```
Tables are created automatically via `Base.metadata.create_all()` on first startup. No migration tool needed. If adding new models, just restart.

---

## 2. Government Data APIs — All Free, All Required for Core Features

These power the economic/demographic data layer. All are free with registration.

| Variable | Source | Sign-up URL | Daily Limit | Powers |
|----------|--------|-------------|-------------|--------|
| `CENSUS_SURVEY_API_KEY` | US Census Bureau | https://api.census.gov/data/key_signup.html | Unlimited (500/day without key) | ACS demographics, housing, economic census by geography |
| `FRED_API_KEY` | St. Louis Fed | https://fred.stlouisfed.org/docs/api/api_key.html | 120 req/min | GDP, unemployment, CPI, interest rates, housing, 800K+ series |
| `BLS_API_KEY` | Bureau of Labor Statistics | https://data.bls.gov/registrationEngine/ | 500/day (25 without key) | Employment, CPI, PPI, JOLTS, occupational wages |
| `BEA_API_KEY` | Bureau of Economic Analysis | https://apps.bea.gov/api/signup/ | 100 req/min | GDP by state/industry, personal income, PCE, regional accounts |
| `EIA_API_KEY` | Energy Information Admin | https://www.eia.gov/opendata/register.php | 5,000/hour | Energy prices, production, oil/gas/electricity |
| `USDA_API_KEY` | USDA (via data.gov) | https://api.data.gov/signup/ | 1,000/hour | Agricultural stats, crop production, livestock, prices |
| `NOAA_API_TOKEN` | NOAA Climate Data Online | https://www.ncdc.noaa.gov/cdo-web/token | 1,000/day | Weather observations, historical climate, normals |
| `DATA_GOV_API` | data.gov (FBI Crime, etc.) | https://api.data.gov/signup/ | 1,000/hour | FBI UCR crime stats, NHTSA vehicle data, other gov datasets |
| `NREL_API_KEY` | National Renewable Energy Lab | https://developer.nrel.gov/signup/ | 1,000/hour | EV charging stations (AFDC), solar/wind resources, grid data |
| `BTS_APP_TOKEN` | Bureau of Transportation Stats | https://data.transportation.gov/profile/edit/developer_settings | 4,000/hour (1,000 without) | Border crossings, freight flows, vehicle miles |
| `DATA_COMMONS_API_KEY` | Google Data Commons | https://apikeys.datacommons.org | Higher limits with key | Unified data from 200+ sources (Census, World Bank, CDC, WHO) |
| `SAM_GOV_API_KEY` | SAM.gov | https://sam.gov/content/entity-information | 10,000/day | Federal contractor registrations, NAICS codes, UEI numbers |

### Notes on government APIs
- **FRED** — technically works without a key (lower limits), but key is strongly recommended
- **BLS** — 25 queries/day without key severely limits usefulness; register for the free key
- **NREL** — powers the AFDC EV charging station data used in the Les Schwab report
- **All keys are emailed within minutes** of registration

---

## 3. LLM Providers — Required for Agentic Features

At least one of OpenAI or Anthropic is required to run the people collection pipeline, PE research agents, and report generation.

| Variable | Provider | Sign-up URL | Cost | Used For |
|----------|----------|-------------|------|----------|
| `OPENAI_API_KEY` | OpenAI | https://platform.openai.com/api-keys | Pay-per-use | Eval LLM judge (gpt-4o-mini), people extraction fallback, report writer |
| `ANTHROPIC_API_KEY` | Anthropic | https://console.anthropic.com/ | Pay-per-use | Claude agents, deep research, company analysis |
| `GEMINI_API_KEY` | Google | https://aistudio.google.com/app/apikey | Free tier + pay | Alternative LLM for research agents |
| `XAI_API_KEY` | xAI (Grok) | https://console.x.ai/ | Pay-per-use | High-speed reasoning alternative |
| `DEEPSEEK_API_KEY` | DeepSeek | https://platform.deepseek.com/api_keys | Pay-per-use | Cost-effective reasoning tasks |
| `GROQ_API_KEY` | Groq | https://console.groq.com/keys | Free tier + pay | Ultra-fast inference (low latency use cases) |
| `MISTRAL_API_KEY` | Mistral | https://console.mistral.ai/api-keys | Pay-per-use | European data-residency alternative |
| `COHERE_API_KEY` | Cohere | https://dashboard.cohere.com/api-keys | Free tier + pay | Embeddings, classification |
| `PERPLEXITY_API_KEY` | Perplexity | https://www.perplexity.ai/settings/api | Pay-per-use | Search-augmented LLM queries |

### Minimum viable LLM setup
- **People collection + evals:** `OPENAI_API_KEY` (gpt-4o-mini is cheap and sufficient)
- **Deep research agents:** `ANTHROPIC_API_KEY` (Claude Sonnet/Opus)
- The platform starts without any LLM key — only agentic features degrade gracefully

---

## 4. Web & Search — Required for People Collection

| Variable | Source | Sign-up URL | Cost | Used For |
|----------|--------|-------------|------|----------|
| `GOOGLE_API_KEY` | Google Cloud | https://console.cloud.google.com/apis/credentials | 100 free/day then $5/1K | People page discovery, company searches |
| `GOOGLE_CSE_ID` | Google Custom Search | https://programmablesearchengine.google.com/ | Same as above | Pairs with GOOGLE_API_KEY for CSE |
| `NEWSAPI_KEY` | NewsAPI | https://newsapi.org/register | 100/day free, paid plans | News articles for company monitoring |

### Manual step: Google CSE setup
1. Go to https://programmablesearchengine.google.com/
2. Create a new search engine scoped to "Search the entire web"
3. Copy the Search Engine ID to `GOOGLE_CSE_ID`
4. Enable "Custom Search API" in Google Cloud Console
5. Add `GOOGLE_API_KEY` and `GOOGLE_CSE_ID` to `.env`

---

## 5. Business & People Data — Optional Enrichment

| Variable | Source | Sign-up URL | Cost | Used For |
|----------|--------|-------------|------|----------|
| `YELP_API_KEY` | Yelp Fusion | https://www.yelp.com/developers/v3/manage_app | 500 free/day | Business search, ratings, hours for site intel |
| `OPENCORPORATES_API_KEY` | OpenCorporates | https://opencorporates.com/api_accounts/new | Free tier + paid | Global company registry, jurisdiction search |
| `GITHUB_TOKEN` | GitHub | https://github.com/settings/tokens | Free | GitHub org data, repo stats (used in tech stack intel) |
| `HUNTER_API_KEY` | Hunter.io | https://hunter.io/users/sign_up | 25 free/mo | Email finding for people collection |
| `CLEARBIT_API_KEY` | Clearbit | https://clearbit.com/ | Paid | Company enrichment, tech stack, headcount estimates |
| `ZOOMINFO_API_KEY` | ZoomInfo | https://www.zoominfo.com/ | Paid ($) | B2B contact data, decision maker profiles |
| `CRUNCHBASE_API_KEY` | Crunchbase | https://www.crunchbase.com/home | Paid | VC/PE funding data, startup profiles |
| `PITCHBOOK_API_KEY` | PitchBook | https://pitchbook.com/ | Paid ($$) | Comprehensive PE/VC deal data (premium alternative) |
| `LINKEDIN_API_KEY` | LinkedIn | https://www.linkedin.com/developers/ | Restricted | Professional profiles (restricted API access) |
| `SIMILARWEB_API_KEY` | SimilarWeb | https://www.similarweb.com/corp/developer/ | Paid | Web traffic analytics, competitive benchmarking |
| `PEERINGDB_API_KEY` | PeeringDB | https://www.peeringdb.com/register | Free | Network peering data for datacenter intel |

---

## 6. Financial Market Data — Optional

All freemium with meaningful free tiers.

| Variable | Source | Sign-up URL | Free Tier | Used For |
|----------|--------|-------------|-----------|---------|
| `FMP_API_KEY` | Financial Modeling Prep | https://site.financialmodelingprep.com/developer/docs | 250 req/day | Stock financials, earnings transcripts, ratios |
| `ALPHA_VANTAGE_API_KEY` | Alpha Vantage | https://www.alphavantage.co/support/#api-key | 25 req/day | Stock prices, forex, technical indicators |
| `POLYGON_API_KEY` | Polygon.io | https://polygon.io/dashboard/signup | Limited | Market data, options, crypto |
| `FINNHUB_API_KEY` | Finnhub | https://finnhub.io/register | 60 req/min | Stock data, company news, earnings calendar |
| `TIINGO_API_KEY` | Tiingo | https://www.tiingo.com/account/api/token | 50 req/hour | EOD prices, news, company fundamentals |
| `QUANDL_API_KEY` | Nasdaq Data Link | https://data.nasdaq.com/sign-up | 50 downloads/day | Alternative data, macro datasets |

---

## 7. Datasets & Other

| Variable | Source | Sign-up URL | Cost | Notes |
|----------|--------|-------------|------|-------|
| `KAGGLE_USERNAME` | Kaggle | https://www.kaggle.com/account | Free | Must also set `KAGGLE_KEY` |
| `KAGGLE_KEY` | Kaggle | https://www.kaggle.com/account → API section | Free | Download `kaggle.json` from profile |

### Manual step: Kaggle credentials
Either set env vars `KAGGLE_USERNAME` + `KAGGLE_KEY`, OR place `~/.kaggle/kaggle.json` on the host and mount it into the container.

---

## 8. No-Key-Required Sources

These work out of the box — no registration needed.

| Source | What It Provides | Rate Limit |
|--------|-----------------|-----------|
| SEC EDGAR | 10-K, 10-Q, 8-K, Form D, Form ADV, XBRL company facts | 10 req/sec |
| FDIC BankFind | Bank financials, failed banks, branch deposits | None stated |
| EPA ECHO | Environmental compliance, facility violations | None stated |
| US Trade (Census) | Import/export flows by commodity and country | None stated |
| USASpending | Federal contracts and grants | 1,000/hour |
| IRS SOI | Tax statistics by industry and geography | None stated |
| Treasury | Interest rates, debt levels, fiscal data | None stated |
| CMS / HHS | Medicare utilization, hospital costs | None stated |
| FCC Broadband | ISP coverage by geography | None stated |
| OSHA | Workplace injuries, violations, citations | None stated |
| FEMA | Disaster declarations, flood maps | None stated |
| USPTO (PatentsView) | Patent search, inventors, assignees | None stated (need free key signup) |
| NPPES | Healthcare provider registry | None stated |
| Court Listener | Federal court decisions and dockets | 5,000/day |
| BIS (Bank for International Settlements) | International banking, FX, property prices | None stated |
| OECD | International macroeconomic statistics | None stated |
| CFTC COT | Futures market commitment of traders | None stated |

---

## 9. Manual Setup Steps Beyond API Keys

### 9.1 PatentsView API Key
Required for USPTO patent ingestion. The `uspto/client.py` exists but needs a free key:
- Sign up at: https://patentsview-support.atlassian.net
- Add to `.env` as `PATENTSVIEW_API_KEY` (not yet in docker-compose — needs to be added)

### 9.2 Playwright / Browser Rendering
For JavaScript-rendered pages (some corporate websites, complex portals):
```bash
# In docker-compose.yml — set INSTALL_BROWSERS=1
# Adds ~500MB to Docker image
INSTALL_BROWSERS=1 docker-compose up --build -d
```
Default is `0` (disabled) to keep image size manageable.

### 9.3 Cloudflare Worker (Report Auth)
The Les Schwab investor report is proxied via a Cloudflare Worker with Basic Auth:
- Worker: `les-schwab-auth.alexius-892.workers.dev`
- Auth: `investor` / `OutsideIn2024`
- Deployed manually via `wrangler deploy` from `/tmp/les-schwab-worker/`
- Worker code is NOT in this repo — lives in Cloudflare dashboard

### 9.4 Google Cloud SQL Proxy (Production Only)
For GCP deployment:
1. Place GCP service account credentials at `~/.config/gcloud/application_default_credentials.json`
2. Start with `docker-compose --profile cloud up`
3. Update `DATABASE_URL` to use `cloudsqlproxy:5432`

### 9.5 Worker Scaling
The default docker-compose starts 1 worker. For parallel job throughput:
```bash
docker-compose up -d --scale worker=4
```
The `deploy.replicas: 6` setting in docker-compose.yml only applies to Docker Swarm, not standard compose.

---

## 10. Quick-Start Priority Order

To get a fully functional local instance:

**Must-have (15 minutes):**
1. `DATABASE_URL` — auto-set by docker-compose
2. `FRED_API_KEY` — free, instant email
3. `BLS_API_KEY` — free, instant email
4. `CENSUS_SURVEY_API_KEY` — free, instant email

**Should-have for full features (30 minutes total):**
5. `EIA_API_KEY` — free, instant
6. `BEA_API_KEY` — free, instant
7. `NREL_API_KEY` — free, instant (powers AFDC EV station data)
8. `OPENAI_API_KEY` — pay-per-use (needed for eval LLM judge + people extraction)
9. `ANTHROPIC_API_KEY` — pay-per-use (needed for Claude research agents)
10. `GOOGLE_API_KEY` + `GOOGLE_CSE_ID` — needed for people collection page discovery

**Optional for expanded data coverage:**
11. `KAGGLE_USERNAME` + `KAGGLE_KEY` — free datasets
12. `YELP_API_KEY` — business intelligence
13. `NOAA_API_TOKEN` — weather/climate data
14. `DATA_GOV_API` — FBI crime data and other gov datasets
15. `NEWSAPI_KEY` — company news monitoring

**Known gaps (not yet wired in docker-compose):**
- `PATENTSVIEW_API_KEY` — needs signup + docker-compose addition
- `NOAA_API_TOKEN` — in client.py but not in docker-compose env block
- `SAFEGRAPH_API_KEY`, `PLACER_API_KEY` — paid, not in docker-compose (foot traffic)

---

## 11. Environment Variable Template

Copy to `.env` and fill in:

```bash
# ── DATABASE ─────────────────────────────────────────────────────────────────
DATABASE_URL=postgresql://nexdata:nexdata_dev_password@postgres:5432/nexdata

# ── GOVERNMENT DATA (ALL FREE) ───────────────────────────────────────────────
CENSUS_SURVEY_API_KEY=
FRED_API_KEY=
BLS_API_KEY=
BEA_API_KEY=
EIA_API_KEY=
USDA_API_KEY=
NOAA_API_TOKEN=
DATA_GOV_API=
NREL_API_KEY=
BTS_APP_TOKEN=
DATA_COMMONS_API_KEY=
SAM_GOV_API_KEY=

# ── LLM PROVIDERS ────────────────────────────────────────────────────────────
OPENAI_API_KEY=
ANTHROPIC_API_KEY=
GEMINI_API_KEY=
XAI_API_KEY=
DEEPSEEK_API_KEY=
GROQ_API_KEY=
MISTRAL_API_KEY=
COHERE_API_KEY=
PERPLEXITY_API_KEY=

# ── WEB & SEARCH ─────────────────────────────────────────────────────────────
GOOGLE_API_KEY=
GOOGLE_CSE_ID=
NEWSAPI_KEY=

# ── BUSINESS DATA (OPTIONAL) ─────────────────────────────────────────────────
YELP_API_KEY=
OPENCORPORATES_API_KEY=
GITHUB_TOKEN=
HUNTER_API_KEY=
CLEARBIT_API_KEY=
CRUNCHBASE_API_KEY=
PITCHBOOK_API_KEY=
PEERINGDB_API_KEY=

# ── FINANCIAL MARKET DATA (OPTIONAL) ─────────────────────────────────────────
FMP_API_KEY=
ALPHA_VANTAGE_API_KEY=
POLYGON_API_KEY=
FINNHUB_API_KEY=
TIINGO_API_KEY=
QUANDL_API_KEY=

# ── DATASETS ─────────────────────────────────────────────────────────────────
KAGGLE_USERNAME=
KAGGLE_KEY=

# ── SYSTEM CONFIG ─────────────────────────────────────────────────────────────
MAX_CONCURRENCY=4
MAX_REQUESTS_PER_SECOND=5.0
LOG_LEVEL=INFO
WORKER_MODE=1
ENABLE_PLAYWRIGHT=0
```
