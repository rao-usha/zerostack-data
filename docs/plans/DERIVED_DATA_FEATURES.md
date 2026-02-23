# Derived Data Features — Competitive Moat Checklist

> **Principle:** Any single dataset is a commodity. The cross-reference is the moat. Every month of temporal data makes it harder to replicate.

---

## Tier 1: High-Moat (Cross-Domain, Hard to Replicate)

### 1. Job Posting Leading Indicators
- [x] **Hiring Velocity Score** — Rate-of-change in open postings vs BLS industry baseline. Company posting 40% more than sector avg = expansion signal.
  - Sources: `job_posting_snapshots` + `bls_ces_employment` + `industrial_companies.naics_code`
- [ ] **Hiring-to-Revenue Divergence** — Compare job posting growth to SEC revenue growth. Hiring up + revenue flat = pivot or trouble. Hiring down + revenue up = margin expansion.
  - Sources: `job_posting_snapshots` + `sec_income_statement` (via `industrial_companies.cik`)
- [ ] **Skills Mix Shift Detection** — Track *types* of roles over time. Spike in "compliance" or "restructuring" = distress. Spike in "data engineer" = digital transformation.
  - Sources: `job_postings.requirements` (skills_extractor) + `job_posting_snapshots.by_department`
- [ ] **Stealth Expansion Detection** — Company posts jobs in city with no known facilities. Cross-ref with industrial site listings + incentive deals.
  - Sources: `job_postings.location` + `industrial_site` + `incentive_deal` + `industrial_companies.headquarters_city`

### 2. PE Deal Prediction Score
- [ ] **Acquisition Target Score** — Companies where: founder aging (people), revenue growing but org thin (people + SEC), PE firms in sector fundraising (pe_funds), company hired "VP Corp Dev" (job postings).
  - Sources: `people` + `sec_income_statement` + `pe_funds` + `job_postings`
- [ ] **Exit Readiness Score** — Portfolio cos where: EBITDA expanding (pe_company_financials), hired CFO from public co (people pipeline), posting "investor relations" roles.
  - Sources: `pe_company_financials` + `people` + `leadership_changes` + `job_postings`
- [ ] **Deal Timing Signal** — PE firm stops posting at portfolio co + company starts posting senior leadership = exit preparation.
  - Sources: `job_posting_snapshots` + `pe_fund_investments` + `pe_portfolio_companies`

### 3. Site Selection Intelligence Composite
- [ ] **Total Cost of Operations Score** — Combine electricity prices + labor wages (OES) + freight rates + utility rates + tax incentives into $/unit cost by geography.
  - Sources: `electricity_price` + `occupational_wage` + `freight_rate_index` + `utility_rate` + `incentive_program`
- [ ] **Infrastructure Readiness Index** — Power capacity headroom + fiber availability + water capacity + transport access, scored per site.
  - Sources: `power_plant` + `interconnection_queue` + `broadband_availability` + `public_water_system` + `intermodal_terminal`
- [ ] **Regulatory Risk Score** — Environmental violations + flood zone + seismic + climate + zoning, combined into single risk number.
  - Sources: `environmental_facility` + `flood_zone` + `seismic_hazard` + `climate_data` + `zoning_district`

### 4. Executive Movement Graph
- [ ] **Leadership Instability Score** — C-suite turnover rate by company/sector. High turnover correlates with operational issues or incoming acquisition.
  - Sources: `leadership_changes` + `company_people` + `industrial_companies`
- [ ] **Talent Flow Network** — Which companies are net exporters vs importers of talent? Map executive migrations between PE portcos, public cos, competitors.
  - Sources: `people_experience` + `company_people` + `pe_portfolio_companies`
- [ ] **"Smart Money Follows Smart People"** — 3+ PE-backed portco execs move to same company within 12 months = deal incoming.
  - Sources: `people_experience` + `pe_firm_people` + `leadership_changes`

---

## Tier 2: Strong Moat (Temporal — Value Grows Over Time)

### 5. Economic Regime Detector
- [ ] **Composite Leading Indicator** — Proprietary version combining JOLTS + freight rates + energy prices + yield curve + prediction market probabilities.
  - Sources: `bls_jolts` + `freight_rate_index` + `eia_*` + `treasury_*` + `market_observations`
- [ ] **Sector Rotation Signal** — BLS employment shifts + FRED industrial production + EIA energy consumption align = rotation underway.
  - Sources: `bls_ces_employment` + `fred_*` + `eia_*`

### 6. Supply Chain Stress Index
- [ ] **Corridor Congestion Score** — Port throughput (TEU trends) + freight corridor truck AADT + warehouse vacancy + trucking lane rates = corridor health metric.
  - Sources: `port_throughput_monthly` + `freight_corridor` + `warehouse_listing` + `trucking_lane_rate`
- [ ] **Carrier Risk Score** — FMCSA safety scores + inspection rates + power unit changes + driver counts. Deteriorating scores + losing drivers = supply chain risk.
  - Sources: `carrier_safety` + `motor_carrier`

### 7. LP Capital Flow Predictor
- [ ] **Allocation Gap Analysis** — LP target vs current allocation = capital that *must* be deployed. CalPERS 3% underweight PE = billions seeking a home.
  - Sources: `lp_asset_class_target_allocation` + `lp_performance_return`
- [ ] **Manager Concentration Risk** — Which GPs have too much LP capital from same sources? Diversification pressure = fundraising difficulty.
  - Sources: `lp_manager_or_vehicle_exposure` + `pe_funds`
- [ ] **Vintage Year Timing** — LP pacing models + J-curve from fund performance = when LPs need to commit new capital.
  - Sources: `lp_asset_class_projection` + `pe_fund_performance`

---

## Tier 3: Network Effects (Value Increases With Scale)

### 8. Co-Investment Network Intelligence
- [ ] **Co-Investment Affinity Score** — Which firms consistently co-invest? Predict likely syndicate partners for new deals.
  - Sources: `pe_deal_participants` + `pe_deals` + `co_investments`
- [ ] **"Who's Looking at What"** — 3+ PE firms post same sector analyst roles simultaneously = sector hot, deals incoming.
  - Sources: `job_postings` (filtered to PE firms) + `pe_firms`

### 9. Geographic Arbitrage Detector
- [ ] **Cost Arbitrage Map** — Real-time heat map of "same output, lower cost" opportunities by geography.
  - Sources: `occupational_wage` + `industrial_site` + `incentive_program` + `electricity_price` + `utility_rate`
- [ ] **Migration-Adjusted Labor Supply** — Census commute flows + ACS demographics + educational attainment = where workforce is going, not where it is.
  - Sources: `commute_flow` + Census ACS tables + `educational_attainment`

### 10. Alternative Credit Signal
- [ ] **Private Company Health Score** — For companies without public financials: hiring trends + foot traffic + digital presence + review sentiment as revenue momentum proxy.
  - Sources: `job_posting_snapshots` + `foot_traffic_observations` + `web_traffic_tranco` + Yelp data
- [ ] **Distress Detection** — Job postings dropping + foot traffic declining + negative reviews + website traffic falling = early warning before any filing shows it.
  - Sources: `job_posting_snapshots` + `foot_traffic_observations` + `web_traffic_tranco` + Yelp data

---

## Implementation Priority (PE Demo Focus)

| Priority | Feature | Effort | Demo Impact |
|----------|---------|--------|-------------|
| **P0** | Hiring Velocity Score | Medium | High — "nobody else has this" signal |
| **P1** | Private Company Health Score | Medium | High — PE firms desperate for private co intel |
| **P1** | LP Allocation Gap | Low | High — direct revenue signal for GPs |
| **P2** | Exit Readiness Score | Medium | High — disposition story differentiator |
| **P2** | Acquisition Target Score | High | High — acquisition story differentiator |
| **P3** | Skills Mix Shift Detection | Low | Medium — adds depth to job posting story |
| **P3** | Leadership Instability Score | Low | Medium — leverages existing people data |
| **P4** | Total Cost of Operations | High | Medium — site selection use case |
| **P4** | Supply Chain Stress Index | Medium | Medium — logistics vertical |
| **P5** | Everything else | Varies | Build as data accumulates over time |

---

*Last updated: 2026-02-23*
