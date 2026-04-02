# PLAN_052 — Signal Chain Testing Guide

All 8 cross-source signal chains are live. This guide walks through testing each one with real data.

**Prerequisites:** API running on `localhost:8001`, database populated with seed data.

```bash
BASE=http://localhost:8001/api/v1
```

---

## Chain 1 — Deal Environment Score

**What it does:** Scores 9 PE sectors (0-100) on deal attractiveness using 7 macro factors from FRED, BLS, BEA, and EIA.

```bash
# All 9 sectors ranked
curl -s "$BASE/pe/macro/deal-scores" | python -m json.tool

# Single sector detail (shows all 7 factor breakdowns)
curl -s "$BASE/pe/macro/deal-scores/energy" | python -m json.tool
curl -s "$BASE/pe/macro/deal-scores/healthcare" | python -m json.tool

# LBO entry scorer (macro-adjusted IRR estimate)
curl -s -X POST "$BASE/pe/macro/lbo-score" \
  -H "Content-Type: application/json" \
  -d '{"sector": "technology", "entry_ev_ebitda": 12.0, "leverage_debt_ebitda": 5.0, "hold_years": 5}'
```

**What to check:**
- 7 factors per sector: Rate environment, Yield curve, Sector labor, Consumer confidence, CPI, Energy costs, Sector GDP growth
- `macro_inputs` includes `oil_price`, `natgas_price`, `energy_cost_yoy_pct`, `sector_gdp_growth_pct`
- Energy sector should score lowest (mining GDP contracting)
- Healthcare/Tech should score highest (strong GDP growth)

---

## Chain 2 — Company Diligence Composite

**What it does:** Scores any company (0-100) across 6 risk/health factors by fuzzy-matching against 8 public data sources.

```bash
# Major defense contractor (high gov dependency)
curl -s -X POST "$BASE/diligence/score" \
  -H "Content-Type: application/json" \
  -d '{"company_name": "Leidos", "state": "VA"}'

# Chemical company (EPA penalties expected)
curl -s -X POST "$BASE/diligence/score" \
  -H "Content-Type: application/json" \
  -d '{"company_name": "Air Products"}'

# Large industrial (EPA + job postings match)
curl -s -X POST "$BASE/diligence/score" \
  -H "Content-Type: application/json" \
  -d '{"company_name": "Bosch"}'

# Quick score by name (GET)
curl -s "$BASE/diligence/score/3M"

# Unknown company (should return 50 C with 0% confidence)
curl -s -X POST "$BASE/diligence/score" \
  -H "Content-Type: application/json" \
  -d '{"company_name": "Fake Company Inc"}'
```

**What to check:**
- 6 factors: Revenue concentration, Environmental risk, Safety risk, Legal exposure, Innovation capacity, Growth momentum
- `confidence` reflects % of sources matched (0.0-1.0)
- `red_flags` array highlights material concerns
- `sources_matched` vs `sources_empty` shows data coverage
- Low confidence = score pulled toward 50 (neutral)
- Try companies you know: defense contractors should flag gov dependency, chemical companies should flag EPA

**Data sources currently active:** USAspending (10K awards), EPA ECHO (1M facilities), FDIC (23K records), Job Postings (21K). OSHA/CourtListener/USPTO/SAM.gov are wired but empty — scores improve automatically when ingested.

---

## Chain 3 — GP Pipeline Score + LP→GP Graph

**What it does:** Ranks PE firms by how LP-favored they are (5 signals), and builds a bipartite LP→GP relationship network.

```bash
# All GPs ranked by LP pipeline strength
curl -s "$BASE/pe/gp-pipeline/scores?limit=15" | python -m json.tool

# Single GP detail with LP base listing
curl -s "$BASE/pe/gp-pipeline/scores/1" | python -m json.tool   # Blackstone
curl -s "$BASE/pe/gp-pipeline/scores/3" | python -m json.tool   # Apollo

# Full LP→GP graph (all edges)
curl -s "$BASE/pe/gp-pipeline/graph" | python -m json.tool

# Filter graph by relationship strength
curl -s "$BASE/pe/gp-pipeline/graph?min_strength=60" | python -m json.tool

# LP network for a specific GP
curl -s "$BASE/pe/gp-pipeline/graph/gp/1" | python -m json.tool  # Blackstone's LPs

# GP network for a specific LP (find LP IDs from graph first)
curl -s "$BASE/pe/gp-pipeline/graph/lp/1" | python -m json.tool

# LP overlap: which GPs share LPs with Blackstone?
curl -s "$BASE/pe/gp-pipeline/overlap/1" | python -m json.tool
curl -s "$BASE/pe/gp-pipeline/overlap/2" | python -m json.tool   # KKR
```

**What to check:**
- 5 signals per GP: LP breadth, Tier-1 LP concentration, Re-up rate, Commitment momentum, Capital density
- Mega-GPs (Blackstone, KKR, Apollo) should score 75-85 A/B
- `lp_base` array shows each LP with type, tier-1 flag, vintages, commitment trend
- Overlap endpoint: mega-GPs should have ~100% LP overlap with each other (same pension funds commit to all)
- Graph edges have `relationship_strength` (0-100) based on vintages × commitment size × trend

---

## Chain 4 — Executive Signal

**What it does:** Detects leadership transition signals (C-suite/VP hiring activity) for deal sourcing.

```bash
# Scan all companies for executive signals (ranked by transition score)
curl -s "$BASE/exec-signals/scan?limit=15" | python -m json.tool

# Single company detail
curl -s "$BASE/exec-signals/company/177" | python -m json.tool   # Visa
curl -s "$BASE/exec-signals/company/236" | python -m json.tool   # Bosch
curl -s "$BASE/exec-signals/company/160" | python -m json.tool   # Cloudflare
```

**What to check:**
- 3 signals: Management buildup, Senior hiring intensity, Hiring velocity
- `flags` array: `succession_in_progress` (C-suite openings), `management_buildup` (VP+ openings)
- Companies with 2+ C-suite roles open should score 80+
- `details` shows exact counts: csuite_open, vp_open, director_open, total_open, senior_pct
- Visa should lead (17 C-suite roles)

---

## Chain 5 — Unified Site Score

**What it does:** Scores any lat/lng location (0-100) across 5 factors with configurable use-case weights.

```bash
# Score individual locations
# Ashburn VA (Data Center Alley)
curl -s -X POST "$BASE/site-intel/sites/unified-score" \
  -H "Content-Type: application/json" \
  -d '{"lat": 39.0438, "lng": -77.4874, "use_case": "datacenter"}'

# Dallas TX
curl -s -X POST "$BASE/site-intel/sites/unified-score" \
  -H "Content-Type: application/json" \
  -d '{"lat": 32.7767, "lng": -96.797, "use_case": "datacenter"}'

# NYC
curl -s -X POST "$BASE/site-intel/sites/unified-score" \
  -H "Content-Type: application/json" \
  -d '{"lat": 40.7128, "lng": -74.006, "use_case": "datacenter"}'

# Same location, different use case (manufacturing vs warehouse)
curl -s -X POST "$BASE/site-intel/sites/unified-score" \
  -H "Content-Type: application/json" \
  -d '{"lat": 33.4484, "lng": -112.074, "use_case": "manufacturing"}'

curl -s -X POST "$BASE/site-intel/sites/unified-score" \
  -H "Content-Type: application/json" \
  -d '{"lat": 33.4484, "lng": -112.074, "use_case": "warehouse"}'

# Compare multiple locations head-to-head
curl -s -X POST "$BASE/site-intel/sites/unified-compare" \
  -H "Content-Type: application/json" \
  -d '{
    "locations": [
      {"name": "Ashburn VA", "lat": 39.0438, "lng": -77.4874},
      {"name": "Dallas TX", "lat": 32.7767, "lng": -96.797},
      {"name": "NYC", "lat": 40.7128, "lng": -74.006},
      {"name": "Phoenix AZ", "lat": 33.4484, "lng": -112.074}
    ],
    "use_case": "datacenter",
    "radius_miles": 50
  }'

# Adjust search radius
curl -s -X POST "$BASE/site-intel/sites/unified-score" \
  -H "Content-Type: application/json" \
  -d '{"lat": 39.0438, "lng": -77.4874, "radius_miles": 100, "use_case": "datacenter"}'
```

**What to check:**
- 5 factors: Power access, Climate risk, Workforce, Connectivity, Regulatory & incentives
- `raw_metrics` shows underlying data: total MW nearby, substation count, electricity price, NRI risk score, unemployment rate, broadband providers, etc.
- `coverage` shows how many factors had real data (1.0 = all 5)
- Use-case weights change the ranking: datacenter emphasizes power+connectivity, warehouse emphasizes regulatory
- Ashburn VA should score well for datacenter (power hub, DC cluster)
- `use_case` options: `general`, `datacenter`, `manufacturing`, `warehouse`

---

## Chain 6 — Portfolio Macro Stress

**What it does:** Applies current macro conditions to individual PE portfolio holdings, producing per-company stress scores.

```bash
# Full portfolio stress report for a PE firm
curl -s "$BASE/pe/stress/1" > /tmp/stress.json   # Blackstone (large, pipe to file)
python -c "
import json
d = json.load(open('/tmp/stress.json'))
print(f'Firm: {d[\"firm_name\"]}')
print(f'Portfolio stress: {d[\"portfolio_stress\"]}/100')
print(f'Holdings: {d[\"holdings_scored\"]}')
print(f'Distribution: {d[\"distribution\"]}')
print(f'Macro: {d[\"macro_summary\"]}')
print()
# Top 5 most stressed
for h in d['holdings'][:5]:
    f = h['financials']
    lev = f'{f[\"debt_to_ebitda\"]:.1f}x' if f['debt_to_ebitda'] else 'N/A'
    print(f'  {h[\"company_name\"]:30} | {h[\"stress_score\"]:>3} {h[\"stress_grade\"]} | {h[\"sector\"]:12} | Leverage={lev}')
print()
# Least stressed
for h in d['holdings'][-3:]:
    print(f'  {h[\"company_name\"]:30} | {h[\"stress_score\"]:>3} {h[\"stress_grade\"]} | {h[\"sector\"]}')
"

# Single holding deep dive
curl -s "$BASE/pe/stress/holding/733" | python -m json.tool   # Ingram Micro (high leverage)
curl -s "$BASE/pe/stress/holding/1001" | python -m json.tool  # Medline (healthcare)

# Try other PE firms
curl -s "$BASE/pe/stress/2" > /tmp/kkr.json   # KKR
curl -s "$BASE/pe/stress/3" > /tmp/apollo.json # Apollo
```

**What to check:**
- 3 stress components per holding: Rate stress (leverage × rates), Margin stress (CPI/energy vs. margins), Sector headwind (inverted Chain 1 score)
- `financials` object shows underlying data: revenue, EBITDA margin, debt/EBITDA, interest coverage
- Holdings with `has_financials: true` have more nuanced scoring (leverage-based rate stress vs. sector defaults)
- Energy holdings should be most stressed (sector headwind from GDP contraction)
- Healthcare/Tech holdings should be least stressed (sector tailwind)
- `macro_summary` shows current macro inputs (FFR, CPI, energy, oil price)

---

## Chain 7 — Healthcare Practice Profiles

**What it does:** Scores med-spa/healthcare practices for PE acquisition with 5-factor profiles.

```bash
# Screen all practices (ranked by acquisition score)
curl -s "$BASE/healthcare/profiles?limit=10" | python -m json.tool

# Filter by state
curl -s "$BASE/healthcare/profiles?state=CA&limit=10" | python -m json.tool
curl -s "$BASE/healthcare/profiles?state=NY&limit=10" | python -m json.tool

# Filter by minimum score
curl -s "$BASE/healthcare/profiles?min_score=70&limit=20" | python -m json.tool

# Single prospect detail
curl -s "$BASE/healthcare/profiles/168" | python -m json.tool   # Calabasas Med Spa
curl -s "$BASE/healthcare/profiles/71" | python -m json.tool    # Next Health
```

**What to check:**
- 5 factors: Market attractiveness (ZIP affluence), Clinical credibility (physician oversight + NPPES), Competitive position (Yelp rating + reviews), Revenue potential (estimated revenue), Multi-unit potential (location count)
- Physician oversight (`has_physician_oversight: true`) boosts clinical credibility to 95
- Multi-location chains score high on multi-unit (LaserAway = 28 locations = 95)
- But LaserAway has no physician oversight → 25 clinical credibility (risk flag)
- Premium ZIPs (Beverly Hills, Upper East Side) score 95+ on market attractiveness
- `details` object includes: estimated_revenue, nppes_provider_count, competitor_count_in_zip, services_offered, review_velocity_30d

---

## Chain 8 — Roll-Up Market Attractiveness

**What it does:** Scores US counties for PE roll-up potential in any NAICS industry using Census CBP + IRS income data.

```bash
# View methodology
curl -s "$BASE/rollup-intel/methodology" | python -m json.tool

# Screen a NAICS code (triggers CBP collection + scoring if not cached)
# 621111 = Physician offices
curl -s -X POST "$BASE/rollup-intel/screen" \
  -H "Content-Type: application/json" \
  -d '{"naics_code": "621111"}'

# Get rankings for a NAICS (after screening)
curl -s "$BASE/rollup-intel/rankings/621111?limit=10" | python -m json.tool

# Filter by state
curl -s "$BASE/rollup-intel/rankings/621111?state=CA&limit=10" | python -m json.tool

# Single county detail
curl -s "$BASE/rollup-intel/market/621111/06037" | python -m json.tool  # LA County

# Find bolt-on acquisition targets near a platform
curl -s -X POST "$BASE/rollup-intel/find-addons" \
  -H "Content-Type: application/json" \
  -d '{"naics_code": "621111", "county_fips": "06037", "radius_miles": 50}'
```

**NAICS codes to try:**
- `621111` — Physician offices
- `621210` — Dental offices
- `621340` — Physical therapy
- `722511` — Full-service restaurants
- `811111` — General automotive repair
- `238220` — Plumbing/HVAC contractors

**What to check:**
- 5 sub-scores: Fragmentation (35%), Market size (25%), Affluence (20%), Growth (10%), Labor (10%)
- High fragmentation (many small businesses, low HHI) = better roll-up opportunity
- Wealthy counties should score high on affluence
- Rankings show national_rank and state_rank per county
- `screen` endpoint triggers data collection if not cached — may take 10-30s on first run

---

## Quick Health Check

```bash
# Verify all endpoints are registered
curl -s http://localhost:8001/docs | grep -oE '/api/v1/[^"]+' | sort -u | head -30

# Overall API health
curl -s http://localhost:8001/health | python -m json.tool
```

---

## Troubleshooting

```bash
# Check API logs for errors
docker-compose logs api --tail 30

# Restart after code changes
docker-compose restart api && sleep 30

# Check database connectivity
docker-compose exec -T api python -c "
from app.core.database import get_session_factory
db = get_session_factory()()
print('DB connected:', db.execute(__import__('sqlalchemy').text('SELECT 1')).scalar())
db.close()
"
```
