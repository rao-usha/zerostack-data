---
name: domains
description: "Reference: canonical domain classification for all Nexdata tables. Not executable — documents how tables map to business domains."
---

# Canonical Domain Classification

**Source of truth:** `app/core/domains.py`

All reports, skills, and dashboards must use `classify_table()` from this module
to group tables into domains. Never maintain a separate domain mapping.

## The 12 Domains (checked in order, first match wins)

| # | Key | Label | Prefix Patterns |
|---|-----|-------|-----------------|
| 1 | `pe_intel` | PE Intelligence | `pe_`, `deals`, `deal_`, `exit_readiness_`, `acquisition_target_`, `diligence_`, `hunt_job` |
| 2 | `people` | People & Org Charts | `people`, `company_people`, `org_chart_`, `leadership_`, `industrial_companies` |
| 3 | `family_office_lp` | Family Office & LP | `family_office`, `lp_`, `investor_`, `co_invest`, `portfolio_` |
| 4 | `site_intel` | Site Intelligence | ~55 infrastructure prefixes (airports, ports, rail, power, water, warehouses, etc.) |
| 5 | `macro_economic` | Macro Economic | `fred_`, `bea_`, `bls_`, `treasury_`, `intl_`, `data_commons_` |
| 6 | `trade_commerce` | Trade & Commerce | `us_trade_`, `cftc_cot_`, `irs_soi_`, `acs5_`, `bts_`, `dunl_`, `trade_gateway_`, `census_variable_`, `foreign_trade_zone` |
| 7 | `financial_regulatory` | Financial & Regulatory | `sec_`, `fdic_`, `fcc_`, `form_adv`, `form_d` |
| 8 | `energy_agriculture` | Energy & Agriculture | `usda_`, `electricity_`, `eia_` |
| 9 | `real_estate` | Real Estate | `realestate_`, `hud_` |
| 10 | `healthcare` | Healthcare | `cms_`, `fda_`, `medspa_`, `zip_medspa_` |
| 11 | `alt_data` | Alternative Data | `m5_`, `job_posting`, `app_store_`, `glassdoor_`, `github_`, `prediction_market`, `foot_traffic_`, `company_web_`, `company_app_`, `company_ats_`, `company_health_`, `company_score`, `company_enrichment`, `competitive_`, `news_`, `hiring_velocity`, `market_` |
| 12 | `platform` | Platform | Catch-all for unmatched tables |

## Key Ordering Rules

- **PE before People**: `pe_people` starts with `pe_` so it lands in PE Intelligence
- **People before Family/LP**: `people_collection_jobs` → People, not caught by later rules
- **Family/LP before Site Intel**: `portfolio_companies` → Family Office & LP via `portfolio_` prefix
- **Site Intel before Macro**: `fema_`, `noaa_`, `natural_gas_*` are infrastructure, not macro indicators
- **Platform is catch-all**: `ingestion_*`, `reports`, `llm_usage`, etc.

## Usage

```python
from app.core.domains import classify_table, DOMAIN_LABELS, DOMAIN_COLORS

domain_key = classify_table("pe_firms")          # → "pe_intel"
label = DOMAIN_LABELS[domain_key]                 # → "PE Intelligence"
color = DOMAIN_COLORS[domain_key]                 # → "#319795"
```

## Adding New Tables

When creating a new table, add its prefix to the appropriate domain's `prefixes`
list in `app/core/domains.py`. If no existing domain fits, consider whether the
table belongs to an existing domain with a new prefix, or if it truly belongs
in Platform (catch-all).
