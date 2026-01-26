# Nexdata Investor Demo Guide

> Quick reference for demonstrating Nexdata's AI-powered investment intelligence platform.

**Base URL:** `http://localhost:8001`

---

## 1. The "Wow" Demo: Autonomous Company Research (30 seconds)

**What it does:** Ask the AI to research any company - it autonomously queries 9+ data sources, synthesizes findings, and returns a comprehensive profile.

```bash
# Start research on any company
curl -X POST http://localhost:8001/api/v1/agents/research/company \
  -H "Content-Type: application/json" \
  -d '{"company_name": "Stripe"}'

# Response: job_id for tracking
# {"status": "started", "job_id": "research_abc123", ...}

# Check results (usually ready in 2-5 seconds)
curl http://localhost:8001/api/v1/agents/research/research_abc123
```

**Key talking points:**
- Queries SEC filings, GitHub, Glassdoor, App Store, web traffic, news - all in parallel
- Synthesizes into unified profile with confidence scores
- Identifies data gaps automatically
- Results cached for 7 days

---

## 2. Automated Due Diligence (The Killer Feature)

**What it does:** Run a complete due diligence report on any investment target in under 60 seconds.

```bash
# Start DD process
curl -X POST http://localhost:8001/api/v1/diligence/start \
  -H "Content-Type: application/json" \
  -d '{"company_name": "Stripe", "template": "standard"}'

# Check results
curl http://localhost:8001/api/v1/diligence/dd_abc123
```

**What you get:**
- Risk score (0-100) with level (low/moderate/high/critical)
- Red flag detection across 6 categories
- Executive summary with recommendation
- Category-by-category analysis
- Structured DD memo ready for investment committee

**Show the output:**
```json
{
  "risk_score": 36.7,
  "risk_level": "moderate",
  "memo": {
    "executive_summary": "Stripe presents a moderate risk profile...",
    "recommendation": "Proceed with caution - Monitor identified concerns",
    "strengths": [...],
    "concerns": [...],
    "red_flags": [...]
  }
}
```

---

## 3. Company Health Scoring (ML-Powered)

**What it does:** Quantifies company health into actionable scores using machine learning.

```bash
curl http://localhost:8001/api/v1/scores/company/Stripe
```

**Output:**
```json
{
  "company_name": "Stripe",
  "composite_score": 72.5,
  "tier": "B",
  "category_scores": {
    "growth": 85,
    "stability": 70,
    "market": 65,
    "tech": 80
  },
  "confidence": 0.75
}
```

**Key points:**
- Weighted scoring: Growth (30%), Stability (25%), Market (25%), Tech (20%)
- A-F tier system for quick assessment
- Confidence score based on data availability

---

## 4. Data Breadth: 25+ Sources

**Show the variety of data sources:**

```bash
# SEC Form D (Private placements)
curl "http://localhost:8001/api/v1/form-d/search?issuer_name=stripe"

# GitHub Analytics (Developer velocity)
curl http://localhost:8001/api/v1/github/org/stripe

# Glassdoor (Employee sentiment)
curl http://localhost:8001/api/v1/glassdoor/company/Stripe

# App Store Rankings
curl "http://localhost:8001/api/v1/apps/search?q=stripe"

# Web Traffic (Tranco rankings)
curl http://localhost:8001/api/v1/web-traffic/domain/stripe.com
```

**Talking point:** "We aggregate data that would take an analyst days to compile manually."

---

## 5. Portfolio Intelligence

### Co-investor Network
```bash
# See who invests alongside a specific investor
curl http://localhost:8001/api/v1/network/investor/123

# Find the most connected investors
curl http://localhost:8001/api/v1/network/central
```

### Investment Trends
```bash
# See where money is flowing
curl http://localhost:8001/api/v1/trends/sectors
curl http://localhost:8001/api/v1/trends/emerging
```

### Portfolio Comparison
```bash
# Compare two investors side-by-side
curl -X POST http://localhost:8001/api/v1/compare/portfolios \
  -H "Content-Type: application/json" \
  -d '{"investor_ids": [1, 2]}'
```

---

## 6. Deal Flow & Predictions

### Track Deals Through Pipeline
```bash
# Create a deal
curl -X POST http://localhost:8001/api/v1/deals \
  -H "Content-Type: application/json" \
  -d '{"company_name": "Acme Corp", "stage": "sourced", "sector": "fintech"}'

# Get pipeline summary
curl http://localhost:8001/api/v1/deals/pipeline
```

### Predictive Deal Scoring
```bash
# Get win probability for a deal
curl http://localhost:8001/api/v1/predictions/deal/123

# See pipeline ranked by probability
curl http://localhost:8001/api/v1/predictions/pipeline
```

---

## 7. Search & Discovery

### Full-Text Search (Typo-tolerant)
```bash
# Search across all data
curl "http://localhost:8001/api/v1/search?q=fintech%20payments"

# Autocomplete suggestions
curl "http://localhost:8001/api/v1/search/suggest?prefix=strip"
```

### Find Similar Investors
```bash
curl http://localhost:8001/api/v1/discover/similar/123
```

---

## 8. API & Developer Experience

### Show the API Docs
Open in browser: `http://localhost:8001/docs`

**Talking points:**
- Full OpenAPI/Swagger documentation
- 100+ endpoints organized by category
- GraphQL available at `/graphql`

### Public API with Auth
```bash
# Generate API key
curl -X POST http://localhost:8001/api/v1/api-keys \
  -H "Content-Type: application/json" \
  -d '{"name": "Demo Key", "scopes": ["read"]}'

# Use with rate limiting
curl -H "X-API-Key: nxd_abc123" http://localhost:8001/api/v1/public/investors
```

---

## Quick Demo Script (5 minutes)

1. **Open** `http://localhost:8001/docs` - "Here's our API - 100+ endpoints"

2. **Run company research:**
   ```bash
   curl -X POST http://localhost:8001/api/v1/agents/research/company \
     -d '{"company_name": "OpenAI"}' -H "Content-Type: application/json"
   ```
   "Watch it query 9 sources in parallel..."

3. **Show DD in action:**
   ```bash
   curl -X POST http://localhost:8001/api/v1/diligence/start \
     -d '{"company_name": "Anthropic", "template": "quick"}' -H "Content-Type: application/json"
   ```
   "Automated due diligence with risk scoring..."

4. **Show the score:**
   ```bash
   curl http://localhost:8001/api/v1/scores/company/Stripe
   ```
   "ML-powered health scores..."

5. **Show data breadth:**
   ```bash
   curl http://localhost:8001/api/v1/github/org/openai
   curl http://localhost:8001/api/v1/glassdoor/company/OpenAI
   ```
   "GitHub velocity, Glassdoor sentiment, SEC filings, all unified..."

---

## Key Metrics to Mention

- **550+ API endpoints** across 40+ data sources
- **60+ database tables** for comprehensive data storage
- **8 autonomous AI agents** for research, DD, and analysis
- **9 data sources** queried in parallel for company research
- **6 risk categories** analyzed in due diligence
- **4 report templates** for instant document generation
- **Sub-second response times** for most queries
- **7-day caching** for expensive operations

---

## 9. NEW: AI-Powered Features (The Showstoppers)

### Anomaly Detection
```bash
# Scan for anomalies
curl -X POST http://localhost:8001/api/v1/anomalies/scan \
  -H "Content-Type: application/json" \
  -d '{"company_name": "Stripe"}'

# Get recent anomalies
curl http://localhost:8001/api/v1/anomalies/recent
```

**Talking point:** "AI monitors your portfolio 24/7 and alerts you to unusual patterns."

### AI Report Generation
```bash
# Generate a company profile report
curl -X POST http://localhost:8001/api/v1/ai-reports/generate \
  -H "Content-Type: application/json" \
  -d '{"report_type": "company_profile", "entity_name": "Stripe"}'

# See available templates
curl http://localhost:8001/api/v1/ai-reports/templates
```

**Talking point:** "Generate investor memos, DD reports, and company profiles instantly."

### Competitive Intelligence
```bash
# Full competitive landscape
curl http://localhost:8001/api/v1/competitive/Stripe

# Moat assessment
curl http://localhost:8001/api/v1/competitive/Stripe/moat
```

**Talking point:** "Automated competitive analysis with moat scoring - network effects, switching costs, brand."

---

## Differentiators

1. **Agentic AI** - Not just data aggregation, 8 autonomous research agents
2. **Due Diligence Automation** - What takes analysts days, done in seconds
3. **AI Report Writer** - Instant document generation from live data
4. **Anomaly Detection** - Proactive alerts when things change
5. **Unified Data Model** - SEC + GitHub + Glassdoor + App Store all connected
6. **Investment-Focused** - Built for PE/VC workflows, not generic business intel
7. **API-First** - Easy to integrate into existing tools and workflows

---

## If Asked: "What's the Tech Stack?"

- **Backend:** FastAPI (Python) - async, high-performance
- **Database:** PostgreSQL with full-text search
- **AI/ML:** Custom scoring models, pattern detection
- **Infrastructure:** Docker, easily deployable
- **API:** REST + GraphQL, fully documented
