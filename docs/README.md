# Nexdata Documentation

> AI-powered investment intelligence platform with 100+ API endpoints across 25+ data sources.

---

## Quick Navigation

| I want to... | Go to |
|--------------|-------|
| Get started quickly | [Getting Started](getting-started/) |
| See API documentation | [API Docs](api/) |
| Learn about a data source | [Data Sources](data-sources/) |
| Demo for investors | [Demo Guide](demo/) |
| Understand the architecture | [Architecture](architecture/) |

---

## Documentation Structure

```
docs/
├── getting-started/     # Setup, quickstart, usage guides
├── api/                 # API documentation and references
├── data-sources/        # Per-source documentation
├── demo/                # Investor demo materials
├── architecture/        # System design and structure
├── plans/               # Implementation plans (T01-T50)
├── changelog/           # Release notes and status
└── internal/            # Development notes and prompts
```

---

## Getting Started

- [Quickstart Guide](getting-started/QUICKSTART.md) - Get up and running in 5 minutes
- [Getting Started](getting-started/GETTING_STARTED.md) - Detailed setup instructions
- [Usage Guide](getting-started/USAGE.md) - How to use the API
- [Quick Reference](getting-started/QUICK_REFERENCE.md) - Command cheat sheet

---

## API Documentation

- [API Overview](api/API_DOCUMENTATION.md) - Complete API reference
- [Swagger UI Guide](api/SWAGGER_UI_GUIDE.md) - Interactive API explorer
- [Finding APIs](api/FIND_YOUR_API_DOCS.md) - How to discover endpoints

### API References
- [Census Metadata](api/references/CENSUS_METADATA_API_REFERENCE.md)
- [Form ADV](api/references/FORM_ADV_API_REFERENCE.md)
- [Geographic APIs](api/references/GEOGRAPHIC_API_GUIDE.md)

---

## Key Features

### Agentic Intelligence (Phase 5)
The most powerful features - autonomous AI agents for investment research.

| Feature | Endpoint | Description |
|---------|----------|-------------|
| Company Research | `POST /agents/research/company` | AI researches any company across 9 data sources |
| Due Diligence | `POST /diligence/start` | Automated DD with risk scoring |
| News Monitor | `POST /monitors/news/watch` | Real-time news monitoring |
| Health Scores | `GET /scores/company/{name}` | ML-powered company scoring |

See [Demo Guide](demo/INVESTOR_DEMO.md) for live examples.

### Data Sources

| Category | Sources | Key Endpoints |
|----------|---------|---------------|
| **Regulatory** | SEC Form D, Form ADV, EDGAR | `/form-d/`, `/form-adv/`, `/sec/` |
| **Developer** | GitHub | `/github/org/{org}` |
| **Employee** | Glassdoor | `/glassdoor/company/{name}` |
| **Consumer** | App Store, Web Traffic | `/apps/`, `/web-traffic/` |
| **Economic** | FRED, BLS, Census, EIA | `/fred/`, `/bls/`, `/census/` |
| **Financial** | FDIC, Treasury | `/fdic/`, `/treasury/` |
| **Geographic** | GeoJSON, Census Geo | `/geojson/`, `/census/geo/` |

Full list: [External Data Sources](data-sources/EXTERNAL_DATA_SOURCES.md)

### Investment Workflows

| Feature | Endpoint | Description |
|---------|----------|-------------|
| Deal Pipeline | `/deals/` | Track deals through stages |
| Deal Predictions | `/predictions/` | ML win probability |
| Network Graph | `/network/` | Co-investor relationships |
| Trends | `/trends/` | Investment trend analysis |
| Benchmarks | `/benchmarks/` | Peer comparisons |
| Search | `/search/` | Full-text search |
| Watchlists | `/watchlists/` | Track companies/investors |

---

## Data Sources Documentation

### Government/Regulatory
- [SEC](data-sources/sec/) - EDGAR, Form D, company filings
- [Form ADV](data-sources/form-adv/) - Investment adviser data
- [Census](data-sources/census/) - Demographic and economic data
- [BLS](data-sources/bls/) - Labor statistics
- [FRED](data-sources/fred/) - Federal Reserve economic data
- [EIA](data-sources/eia/) - Energy data
- [Treasury](data-sources/treasury/) - Treasury data
- [FDIC](data-sources/fdic/) - Bank data
- [FCC](data-sources/fcc/) - Broadband data
- [IRS](data-sources/irs/) - Tax statistics
- [NOAA](data-sources/noaa/) - Weather data

### Alternative Data
- [Family Offices](data-sources/family-offices/) - Family office tracking
- [Foot Traffic](data-sources/foot-traffic/) - Location intelligence
- [Real Estate](data-sources/realestate/) - Property data
- [GeoJSON](data-sources/geojson/) - Geographic boundaries

### Overview
- [All External Sources](data-sources/EXTERNAL_DATA_SOURCES.md) - Complete list

---

## Demo & Presentations

For investor meetings and demos:

- [Investor Demo Guide](demo/INVESTOR_DEMO.md) - Full walkthrough with commands
- [Quick Commands](demo/QUICK_COMMANDS.md) - Copy-paste ready commands
- [Feature Highlights](demo/FEATURE_HIGHLIGHTS.md) - Business value summary
- [Original Demo](demo/DEMO.md) - Basic demo script

---

## Architecture

- [Directory Structure](architecture/DIRECTORY_STRUCTURE.md) - Codebase organization
- [System Guide](architecture/COMPLETE_SYSTEM_GUIDE.md) - Full system documentation
- [Database Backup](architecture/DATABASE_BACKUP_GUIDE.md) - Backup procedures
- [Project Organization](architecture/PROJECT_ORGANIZATION.md) - Code structure

---

## Development

### Implementation Plans
All feature plans are in [plans/](plans/). Key phases:
- **Phase 1-2**: Core infrastructure and user features
- **Phase 3**: Investment intelligence (T21-T30)
- **Phase 4**: Data expansion and ML (T31-T40)
- **Phase 5**: Agentic AI (T41-T50) - Current

### Internal Documentation
Development notes and prompts: [internal/](internal/)

### Changelog
Release notes and status: [changelog/](changelog/)

---

## Quick Links

| Resource | URL |
|----------|-----|
| API Docs (Swagger) | http://localhost:8001/docs |
| GraphQL Playground | http://localhost:8001/graphql |
| Health Check | http://localhost:8001/health |
| GitHub Repo | https://github.com/rao-usha/zerostack-data |
