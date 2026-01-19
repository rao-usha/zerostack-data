# Nexdata Query Skill

You have access to the Nexdata API running at `http://localhost:8001`. Use this to answer questions about investors, portfolio companies, deals, and market trends.

## Quick Start

**Python client** (preferred):
```python
from scripts.nexdata_client import NexdataClient
client = NexdataClient()

# Or use convenience functions:
from scripts.nexdata_client import search_investors, get_company_score, get_portfolio
```

**Direct curl**:
```bash
curl -s "http://localhost:8001/api/v1/search?q=Sequoia&type=investor" | python -m json.tool
```

---

## Common Query Patterns

### "What companies did [Investor] invest in?"

```python
from scripts.nexdata_client import search_investors, get_portfolio

# 1. Find the investor
investors = search_investors("Sequoia")
investor_id = investors[0]["id"]

# 2. Get their portfolio
portfolio = get_portfolio(investor_id)
for company in portfolio["companies"]:
    print(f"- {company['name']} ({company.get('sector', 'Unknown')})")
```

### "How healthy is [Company]?"

```python
from scripts.nexdata_client import get_company_score, get_glassdoor_data, get_github_metrics

# Get health score
score = get_company_score("Stripe")
print(f"Health: {score['composite_score']}/100 ({score['tier']} tier)")

# Get employee sentiment
glassdoor = get_glassdoor_data("Stripe")
print(f"Glassdoor: {glassdoor.get('overall_rating', 'N/A')}/5")

# Get dev velocity (if tech company)
github = get_github_metrics("stripe")
print(f"Dev velocity: {github.get('velocity_score', 'N/A')}/100")
```

### "Compare [Investor A] and [Investor B]"

```python
from scripts.nexdata_client import search_investors, compare_investors

# Find both investors
inv1 = search_investors("Andreessen Horowitz")[0]
inv2 = search_investors("Sequoia")[0]

# Compare portfolios
comparison = compare_investors(inv1["id"], inv2["id"])
print(f"Overlap: {comparison['overlap_count']} companies ({comparison['similarity']:.0%})")
print(f"Shared: {', '.join(comparison['shared_companies'][:5])}")
```

### "What's the deal pipeline looking like?"

```python
from scripts.nexdata_client import get_pipeline_insights, get_scored_pipeline

# Get pipeline health
insights = get_pipeline_insights()
health = insights["pipeline_health"]
print(f"Active deals: {health['total_active_deals']}")
print(f"Expected value: ${health['expected_value_millions']:.1f}M")

# Get high-probability deals
pipeline = get_scored_pipeline(min_probability=0.6)
for deal in pipeline["deals"][:5]:
    print(f"- {deal['company_name']}: {deal['win_probability']*100:.0f}% ({deal['tier']})")
```

### "What are the hot sectors right now?"

```python
from scripts.nexdata_client import NexdataClient
client = NexdataClient()

# Get emerging sectors
emerging = client.get_emerging_sectors()
for sector in emerging.get("sectors", [])[:5]:
    print(f"- {sector['name']}: {sector['momentum']}")

# Get sector trends
trends = client.get_sector_trends()
```

### "Research [Company] across all sources"

```python
from scripts.nexdata_client import research_company, get_research_result

# Start research (runs in background)
job = research_company("Stripe")
print(f"Job started: {job['job_id']}")

# Or get cached results
result = get_research_result("Stripe")
print(f"Sources queried: {len(result['sources'])}")
print(f"Data completeness: {result['completeness']:.0%}")
```

---

## API Reference (Curated Endpoints)

### Search
| Endpoint | Description |
|----------|-------------|
| `GET /search?q={query}&type={investor\|company}` | Full-text search |
| `GET /search/suggest?q={prefix}` | Autocomplete suggestions |

### Investors
| Endpoint | Description |
|----------|-------------|
| `GET /investors/{id}` | Investor details |
| `GET /investors/{id}/portfolio` | Portfolio companies |
| `GET /discover/similar/{id}` | Find similar investors |
| `POST /compare/portfolios` | Compare two portfolios |

### Companies
| Endpoint | Description |
|----------|-------------|
| `GET /companies/{id}` | Company details |
| `GET /scores/company/{name}` | Health score (0-100) |
| `GET /enrichment/companies/{name}` | Enriched data |
| `GET /news/company/{name}` | Company news |

### Deals & Predictions
| Endpoint | Description |
|----------|-------------|
| `GET /deals` | List deals (filter by stage, sector) |
| `GET /deals/{id}` | Deal details |
| `GET /predictions/deal/{id}` | Win probability |
| `GET /predictions/pipeline` | Scored pipeline |
| `GET /predictions/insights` | Pipeline health |

### Analytics & Trends
| Endpoint | Description |
|----------|-------------|
| `GET /analytics/overview` | System stats |
| `GET /analytics/industry-breakdown` | Industry distribution |
| `GET /trends/sectors` | Sector allocation trends |
| `GET /trends/emerging` | Hot sectors |

### Data Sources
| Endpoint | Description |
|----------|-------------|
| `GET /github/org/{org}` | GitHub metrics |
| `GET /glassdoor/company/{name}` | Glassdoor ratings |
| `GET /web-traffic/domain/{domain}` | Web traffic |
| `GET /form-d/search?issuer_name={name}` | SEC Form D filings |
| `GET /form-adv/search?name={name}` | Investment advisers |

### Research (Agentic)
| Endpoint | Description |
|----------|-------------|
| `POST /agents/research/company` | Start research job |
| `GET /agents/research/company/{name}` | Get cached research |
| `GET /agents/research/{job_id}` | Check job status |

---

## Response Examples

### Company Score Response
```json
{
  "company_name": "Stripe",
  "composite_score": 85.2,
  "tier": "A",
  "confidence": 0.92,
  "category_scores": {
    "growth": 88.0,
    "stability": 82.0,
    "market_position": 90.0,
    "tech_velocity": 81.0
  }
}
```

### Deal Prediction Response
```json
{
  "deal_id": 123,
  "company_name": "TechCo",
  "win_probability": 0.72,
  "confidence": "high",
  "tier": "B",
  "scores": {
    "company_score": 78.5,
    "deal_score": 65.0,
    "pipeline_score": 80.0,
    "pattern_score": 70.0
  },
  "strengths": ["Strong company health", "Active pipeline"],
  "risks": ["High valuation"],
  "recommendations": ["Schedule founder meeting"]
}
```

### Pipeline Insights Response
```json
{
  "pipeline_health": {
    "total_active_deals": 25,
    "total_pipeline_value_millions": 150.5,
    "expected_value_millions": 67.7,
    "avg_win_probability": 0.45
  },
  "risk_alerts": [
    {"deal_id": 78, "company_name": "SlowCo", "alert": "Stalled 30+ days"}
  ],
  "opportunities": [
    {"deal_id": 123, "company_name": "TechCo", "insight": "Ready for term sheet"}
  ]
}
```

---

## Tips

1. **Always search first** - Don't assume IDs. Use `search_investors()` or `search_companies()` to find entities.

2. **Use the Python client** - It handles errors, has type hints, and is more readable than curl.

3. **Check multiple sources** - For company health, combine `get_company_score()`, `get_glassdoor_data()`, and `get_github_metrics()`.

4. **Pipeline questions** - Use `get_pipeline_insights()` for overview, `get_scored_pipeline()` for deal list.

5. **Research jobs are async** - `research_company()` starts a background job. Use `get_research_result()` to get cached results.
