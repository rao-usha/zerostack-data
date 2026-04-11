# Nexdata Documentation

> AI-powered investment intelligence platform — 28+ data sources, 200+ database tables, 8 signal chains, autonomous DD agents.

---

## Directory Structure

```
docs/
├── strategy/        # Business strategy, GTM, competitive intel, customer targets
├── guides/          # Setup, usage, and user guides
├── reference/       # API reference, data source catalog, technical docs
├── plans/           # Implementation plans (active: 054-058)
│   └── completed/   # Archived completed plans (001-053)
├── specs/           # Spec-first development specs + skeleton tests
├── data-sources/    # Per-source documentation (BLS, Census, FRED, etc.)
└── archive/         # Historical docs, old status reports, superseded content
```

---

## Strategy (Start Here)

| Document | What It Covers |
|----------|---------------|
| [Product Strategy Q2 2026](strategy/PRODUCT_STRATEGY_2026_Q2.md) | Master strategy: 3 options, 90-day roadmap, pricing, moat analysis |
| [Customer Target Playbook](strategy/CUSTOMER_TARGET_PLAYBOOK_2026.md) | Who to sell to first — 3 beachheads, 50+ named targets, outreach timeline |
| [Competitive Landscape](strategy/COMPETITIVE_LANDSCAPE_2026.md) | 22 competitors mapped across 3 tiers |
| [Buyer Persona Analysis](strategy/BUYER_PERSONA_ANALYSIS.md) | 3 buyer personas, buying journey, sales playbook |
| [TAM Analysis](strategy/TAM_PE_DATA_INTELLIGENCE_2026.md) | $8B TAM, comparable company revenue, M&A multiples |
| [Pricing & GTM Research](strategy/PE_PRICING_AND_GTM_RESEARCH.md) | Incumbent pricing, revenue models, financial projections |
| [PE Firm Target List](strategy/PE_FIRM_TARGET_LIST_2026.md) | 23 PE firms with names, people, and outreach strategies |
| [Early Adopter Targets](strategy/EARLY_ADOPTER_TARGET_RESEARCH.md) | Family offices, independent sponsors, search funds |
| [Operating Partner Ecosystem](strategy/OPERATING_PARTNER_ECOSYSTEM_RESEARCH.md) | Operating partner firms, channel partners, conferences |
| [Product Review](strategy/PRODUCT_REVIEW_2026_04_06.md) | Full codebase audit and gap analysis |
| [Demo Script](strategy/DEMO_SCRIPT.md) | Demo talking points and flow |

---

## Guides

| Guide | Description |
|-------|-------------|
| [Quickstart](guides/QUICKSTART.md) | Get up and running in 5 minutes |
| [Getting Started](guides/GETTING_STARTED.md) | Detailed setup instructions |
| [Usage Guide](guides/USAGE.md) | How to use the API |
| [Testing Guide](guides/PLAN_052_TESTING_GUIDE.md) | Running and writing tests |
| [Synthetic Data Guide](guides/SYNTHETIC_DATA_USER_GUIDE.md) | Understanding synthetic vs. real data |
| [Sanity Check Guide](guides/SANITY_CHECK_GUIDE.md) | Verifying system health |
| [People Platform](guides/PEOPLE_PLATFORM_GUIDE.md) | People collection pipeline |
| [People Deployment](guides/PEOPLE_DEPLOYMENT_GUIDE.md) | Deploying people collection |

---

## Reference

| Document | Description |
|----------|-------------|
| [API Endpoint Reference](reference/API_ENDPOINT_REFERENCE.md) | All API endpoints cataloged |
| [API Keys & Requirements](reference/API_KEYS_AND_REQUIREMENTS.md) | Which keys are needed per source |
| [External Data Sources](reference/EXTERNAL_DATA_SOURCES.md) | All 28+ data source details |
| [D3 Visualization Research](reference/D3_VISUALIZATION_RESEARCH.md) | D3 viz implementation notes |
| [Swagger UI](http://localhost:8001/docs) | Interactive API documentation |

---

## Plans & Specs

- **Active plans** (054-058): `plans/`
- **Completed plans** (001-053 + unnumbered): `plans/completed/`
- **Specs**: `specs/` — spec-first development with skeleton tests
- **Active spec pointer**: `specs/.active_spec`

---

## Data Sources

Per-source documentation in `data-sources/`: BLS, Census, CMS, EIA, FCC, FDIC, FRED, Form ADV, Geojson, IRS, NOAA, People, Real Estate, SEC, Treasury, and more.
