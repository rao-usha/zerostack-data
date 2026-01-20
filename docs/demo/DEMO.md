# Nexdata Demo

> Automate what analysts spend days doing. Show it in 30 seconds.

---

## The 30-Second "Holy Shit" Moment

Run this:

```bash
curl -X POST localhost:8001/api/v1/agents/research/company \
  -H "Content-Type: application/json" \
  -d '{"company_name": "Stripe"}'
```

In 5 seconds, the AI:
1. Queries SEC filings (Form D, 13F)
2. Pulls GitHub activity (repos, contributors, velocity)
3. Checks Glassdoor (ratings, reviews, red flags)
4. Grabs App Store rankings
5. Analyzes web traffic
6. Scans recent news
7. Checks corporate registry

**Returns a complete company profile.** What an analyst does in 4 hours, done in 5 seconds.

---

## The 5-Minute Story Demo

### Setup: The Scenario

*"You're an associate at a VC firm. Your partner just got a cold email from a startup. They want $5M. You have 30 minutes before the partner meeting. What do you do?"*

### Act 1: Research (1 min)

```bash
curl -X POST localhost:8001/api/v1/agents/research/company \
  -d '{"company_name": "Anthropic"}' -H "Content-Type: application/json"
```

Show the response:
- Funding history from SEC
- GitHub activity metrics
- Employee sentiment from Glassdoor
- News coverage analysis
- Corporate registration details

*"That used to be half a day of Googling. Now it's one API call."*

### Act 2: Due Diligence (2 min)

```bash
curl -X POST localhost:8001/api/v1/diligence/start \
  -d '{"company_name": "Anthropic", "template": "standard"}' -H "Content-Type: application/json"
```

Show the DD output:
- **Risk Score**: 0-100 with clear level
- **Red Flags**: Categorized by type (financial, legal, team, market, ops, tech)
- **Executive Summary**: One paragraph for the partner
- **IC Memo**: Ready to present

*"A full due diligence memo. In 60 seconds. With sources."*

### Act 3: Health Score (30 sec)

```bash
curl localhost:8001/api/v1/scores/company/Stripe
```

```json
{
  "composite_score": 72.5,
  "tier": "B",
  "category_scores": {
    "growth": 85,
    "stability": 70,
    "market": 65,
    "tech": 80
  }
}
```

*"Quantified. Comparable. Every company on the same scale."*

### Act 4: The Data Breadth (1 min)

Show the Swagger UI: `localhost:8001/docs`

Scroll through the endpoints:
- `/api/v1/fred/` - 800,000 economic time series
- `/api/v1/sec/` - Every public filing
- `/api/v1/census/` - Demographic data for every ZIP code
- `/api/v1/github/` - Developer velocity metrics
- `/api/v1/glassdoor/` - Employee sentiment
- `/api/v1/form-d/` - Private placement data

*"25 data sources. 100+ endpoints. One API."*

### Act 5: The Kicker (30 sec)

```bash
curl "localhost:8001/api/v1/search?q=fintech%20AI"
```

Full-text search across everything. Typo-tolerant.

Then:
```bash
curl localhost:8001/api/v1/discover/similar/123
```

*"Find companies similar to ones you already like. Pattern matching at scale."*

---

## One-Liner Demos

### Investment Research
```bash
curl -X POST localhost:8001/api/v1/agents/research/company -d '{"company_name": "OpenAI"}'
```

### Due Diligence
```bash
curl -X POST localhost:8001/api/v1/diligence/start -d '{"company_name": "Databricks", "template": "quick"}'
```

### Company Scoring
```bash
curl localhost:8001/api/v1/scores/company/Snowflake
```

### Investor Network
```bash
curl localhost:8001/api/v1/network/investor/123
```

### Sector Trends
```bash
curl localhost:8001/api/v1/trends/sectors
```

### Portfolio Comparison
```bash
curl -X POST localhost:8001/api/v1/compare/portfolios -d '{"investor_ids": [1, 2]}'
```

---

## Data Source Highlights

| Want This? | We Have It |
|------------|-----------|
| Funding rounds | SEC Form D, Crunchbase-style |
| Employee sentiment | Glassdoor ratings + reviews |
| Developer velocity | GitHub commits, contributors, repos |
| Consumer traction | App Store rankings, web traffic |
| Economic context | FRED (800K series), Census, BLS |
| Regulatory filings | SEC 10-K, 13F, Form ADV |

---

## URLs

| What | Where |
|------|-------|
| API Docs | http://localhost:8001/docs |
| GraphQL | http://localhost:8001/graphql |
| Health Check | http://localhost:8001/health |

---

## If You Only Have 60 Seconds

1. Run company research:
   ```bash
   curl -X POST localhost:8001/api/v1/agents/research/company -d '{"company_name": "Stripe"}'
   ```
2. Wait 5 seconds
3. Show the response
4. Say: *"9 data sources. 5 seconds. What used to take half a day."*

---

## What NOT To Do

- ❌ Don't demo with fake company names (use real ones: Stripe, Anthropic, OpenAI)
- ❌ Don't show the ingestion features first (boring - lead with agents)
- ❌ Don't explain the architecture (they don't care)
- ❌ Don't apologize for missing data (there's always more coming)
- ❌ Don't show the database schema (it's an API, not a database)

---

## The Pitch (If Asked)

*"Nexdata is what happens when you connect an AI to every public data source that matters for investment research. SEC filings, GitHub, Glassdoor, app rankings, economic data - all unified."*

*"It's not a database. It's an army of research analysts that work in seconds, not days."*

---

## Backup: Data Ingestion Demo

If they want to see the infrastructure:

```bash
# Ingest FRED data
curl -X POST localhost:8001/api/v1/fred/ingest \
  -d '{"series_ids": ["GDP", "UNRATE", "CPIAUCSL"], "observation_start": "2020-01-01"}'

# Check job status
curl localhost:8001/api/v1/jobs

# Query the data
curl localhost:8001/api/v1/fred/series/GDP/observations
```

*"Every ingestion tracked. Rate limited. Retried automatically. Type-safe storage."*

---

## Competitive Positioning

| Ask | Answer |
|-----|--------|
| "How is this different from PitchBook?" | "PitchBook is a database you search. Nexdata is an AI that researches for you." |
| "Why not just use ChatGPT?" | "ChatGPT doesn't have real-time SEC filings, GitHub metrics, or Glassdoor data." |
| "What about CB Insights?" | "They aggregate. We automate. There's no autonomous research agent in CB Insights." |
