# People & Org Chart Intelligence Platform - Getting Started Guide

A comprehensive guide to using the People & Org Chart Intelligence Platform for PE operating teams.

---

## Table of Contents

1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Core Concepts](#core-concepts)
4. [Use Cases](#use-cases)
5. [API Examples](#api-examples)
6. [Data Collection](#data-collection)
7. [Best Practices](#best-practices)

---

## Overview

The People & Org Chart Intelligence Platform enables PE operating teams to:

- **View** portfolio company leadership teams
- **Compare** teams across peer companies (benchmarking)
- **Monitor** leadership changes and announcements
- **Track** key executives via watchlists
- **Analyze** talent flow and industry trends

### Key Features

| Feature | Description |
|---------|-------------|
| Leadership Profiles | Comprehensive executive profiles with contact info, bio, experience |
| Org Charts | Hierarchical org structures with reporting relationships |
| Change Tracking | Real-time detection of hires, departures, promotions |
| Peer Benchmarking | Compare leadership teams across similar companies |
| Portfolio Views | Aggregate views across portfolio companies |
| Watchlists | Track specific executives of interest |
| Alerts & Digests | Email alerts for leadership changes |
| Data Quality | Scoring and enrichment tracking |

---

## Quick Start

### 1. Start the API Server

```bash
# Using Docker (recommended)
docker-compose up -d

# Or run directly
uvicorn app.main:app --reload --port 8001
```

### 2. Verify the API is Running

```bash
curl http://localhost:8001/api/v1/health
```

### 3. Seed Sample Data (Optional)

```bash
python scripts/seed_industrial_companies.py
```

### 4. Explore the API

Open Swagger UI: http://localhost:8001/docs

---

## Core Concepts

### People

A **Person** represents an executive, manager, or board member. Key attributes:

- **Identity**: Full name, first/last name, suffix
- **Contact**: Email, phone, LinkedIn URL, photo
- **Location**: City, state, country
- **Bio**: Professional biography
- **Data Quality**: Confidence score, last verified date

### Companies

An **Industrial Company** represents a target company. Key attributes:

- **Identity**: Name, legal name, DBA names
- **Website**: Main site, leadership page, careers page
- **Classification**: Industry segment, sub-segment, NAICS/SIC
- **Size**: Employee count, revenue
- **Ownership**: Public, private, PE-backed

### Company-Person Relationships

A **CompanyPerson** links people to companies with role details:

- **Role**: Title, normalized title, title level
- **Hierarchy**: Reports to, management level, direct reports
- **Board**: Board member, chair, committee membership
- **Employment**: Start date, end date, tenure
- **Compensation**: Salary, total comp (for public companies)

### Leadership Changes

A **LeadershipChange** tracks executive movements:

- **Change Types**: hire, departure, promotion, demotion, retirement, board_appointment
- **Context**: Old title, new title, old company
- **Dates**: Announced date, effective date, detected date
- **Significance**: C-suite flag, board flag, significance score (1-10)

### Portfolios

A **Portfolio** groups companies for collective tracking:

- **PE Portfolio**: Companies owned by a PE fund
- **Watchlist**: Companies of interest
- **Peer Group**: Similar companies for benchmarking

### Peer Sets

A **Peer Set** defines comparison groups:

- **Criteria**: Revenue range, employee count, industry
- **Primary Company**: The company being benchmarked
- **Peers**: Companies to compare against

### Watchlists

A **Watchlist** tracks specific executives:

- **People**: List of executives to monitor
- **Tags**: Categorization (e.g., "cfo_candidate", "knows_well")
- **Alerts**: Notifications when watched people change roles

---

## Use Cases

### 1. Portfolio Leadership Overview

**Goal:** Get a quick view of all executives across your portfolio.

```python
import requests

# Get portfolio companies
portfolio = requests.get(
    "http://localhost:8001/api/v1/people-portfolios/1/companies"
).json()

# Get leadership for each company
for company in portfolio:
    leadership = requests.get(
        f"http://localhost:8001/api/v1/companies/{company['id']}/leadership"
    ).json()
    print(f"\n{company['name']}: {leadership['c_suite_count']} C-suite")
    for exec in leadership['leadership'][:5]:
        print(f"  - {exec['title']}: {exec['full_name']}")
```

### 2. Peer Benchmarking

**Goal:** Compare your portfolio company's team against competitors.

```python
# Get benchmark score
benchmark = requests.get(
    "http://localhost:8001/api/v1/people-analytics/benchmark/1"
).json()

print(f"Team Score: {benchmark['team_score']}/100")
print(f"  Completeness: {benchmark['components']['completeness']}/25")
print(f"  Depth: {benchmark['components']['depth']}/25")
print(f"  Tenure: {benchmark['components']['tenure']}/25")
print(f"  Board: {benchmark['components']['board']}/25")

# Compare with peers
comparison = requests.get(
    "http://localhost:8001/api/v1/peer-sets/1/comparison"
).json()

print(f"\nvs Peer Average:")
print(f"  C-Suite: {comparison['comparison']['c_suite_count']['primary']} vs {comparison['comparison']['c_suite_count']['peer_avg']}")
```

### 3. Change Monitoring

**Goal:** Stay informed about leadership changes in your portfolio.

```python
# Get recent alerts
alerts = requests.get(
    "http://localhost:8001/api/v1/people-jobs/alerts/portfolio/1",
    params={"days": 7, "c_suite_only": True}
).json()

print(f"C-Suite Changes This Week: {alerts['total_alerts']}")
for alert in alerts['alerts']:
    print(f"  {alert['change_type'].upper()}: {alert['person_name']} - {alert['new_title']} @ {alert['company_name']}")
```

### 4. Executive Tracking

**Goal:** Track specific executives you're interested in.

```python
# Create a watchlist
watchlist = requests.post(
    "http://localhost:8001/api/v1/people-watchlists/",
    json={"name": "CFO Candidates", "description": "Potential CFO hires"}
).json()

# Add executives
requests.post(
    f"http://localhost:8001/api/v1/people-watchlists/{watchlist['id']}/people",
    json={"person_id": 25, "tags": ["cfo_candidate"]}
)

# Get alerts for watched people
watchlist_alerts = requests.get(
    f"http://localhost:8001/api/v1/people-jobs/alerts/watchlist/{watchlist['id']}"
).json()
```

### 5. Weekly Digest

**Goal:** Get a weekly summary of all leadership changes.

```python
# Generate weekly digest
digest = requests.get(
    "http://localhost:8001/api/v1/people-jobs/digest/weekly",
    params={"portfolio_id": 1}
).json()

print(f"Weekly Digest - {digest['period']}")
print(f"  Total Changes: {digest['summary']['total_changes']}")
print(f"  C-Suite: {digest['summary']['c_suite_changes']}")
print(f"  Companies Affected: {digest['summary']['companies_affected']}")

print("\nHighlights:")
for highlight in digest['highlights'][:5]:
    print(f"  - {highlight['person_name']}: {highlight['change_type']} to {highlight['new_title']}")
```

---

## API Examples

### Python Client

```python
import requests

class PeopleClient:
    def __init__(self, base_url="http://localhost:8001/api/v1"):
        self.base_url = base_url

    def get_company_leadership(self, company_id):
        return requests.get(f"{self.base_url}/companies/{company_id}/leadership").json()

    def search_people(self, query):
        return requests.get(f"{self.base_url}/people/search", params={"q": query}).json()

    def get_portfolio_analytics(self, portfolio_id, days=90):
        return requests.get(
            f"{self.base_url}/people-analytics/portfolio/{portfolio_id}",
            params={"days": days}
        ).json()

    def get_recent_alerts(self, portfolio_id=None, days=7):
        if portfolio_id:
            return requests.get(
                f"{self.base_url}/people-jobs/alerts/portfolio/{portfolio_id}",
                params={"days": days}
            ).json()
        return requests.get(
            f"{self.base_url}/people-jobs/alerts/recent",
            params={"days": days}
        ).json()

# Usage
client = PeopleClient()
leadership = client.get_company_leadership(1)
alerts = client.get_recent_alerts(portfolio_id=1)
```

### cURL Examples

```bash
# Get company leadership
curl "http://localhost:8001/api/v1/companies/1/leadership"

# Search for executives
curl "http://localhost:8001/api/v1/people/search?q=CFO&industry=distribution"

# Get industry stats
curl "http://localhost:8001/api/v1/people-analytics/industry/distribution?days=90"

# Schedule a collection job
curl -X POST "http://localhost:8001/api/v1/people-jobs/schedule" \
  -H "Content-Type: application/json" \
  -d '{"job_type": "website_crawl", "company_ids": [1, 2, 3]}'

# Get weekly digest
curl "http://localhost:8001/api/v1/people-jobs/digest/weekly?portfolio_id=1"
```

---

## Data Collection

### Collection Methods

The platform collects leadership data from multiple sources:

| Source | Job Type | Frequency | Data |
|--------|----------|-----------|------|
| Company Websites | `website_crawl` | Weekly | Names, titles, photos, bios |
| SEC EDGAR | `sec_parse` | Daily | NEOs, board, compensation |
| News/Press Releases | `news_scan` | Daily | Changes, announcements |
| SEC 8-K Filings | `sec_8k_check` | Daily | Executive changes |

### Scheduling Jobs

```python
# Schedule website refresh for portfolio companies
requests.post(
    "http://localhost:8001/api/v1/people-jobs/schedule",
    json={
        "job_type": "website_crawl",
        "priority": "portfolio",
        "limit": 50
    }
)

# Schedule SEC check for public companies
requests.post(
    "http://localhost:8001/api/v1/people-jobs/schedule",
    json={
        "job_type": "sec_8k_check",
        "priority": "public",
        "limit": 30
    }
)
```

### Monitoring Job Status

```python
# Get job stats
stats = requests.get(
    "http://localhost:8001/api/v1/people-jobs/stats",
    params={"days": 7}
).json()

print(f"Jobs: {stats['total_jobs']} ({stats['success_rate']}% success)")
print(f"People Found: {stats['total_people_found']}")
print(f"People Created: {stats['total_people_created']}")
```

---

## Best Practices

### 1. Portfolio Setup

- Create portfolios for each PE fund
- Add all portfolio companies with investment dates
- Set up a peer set for each portfolio company

### 2. Data Quality

- Monitor the enrichment queue regularly
- Prioritize LinkedIn URL collection for better matching
- Review potential duplicates weekly

### 3. Change Monitoring

- Subscribe to weekly digests for each portfolio
- Set up C-suite-only alerts for high-priority companies
- Use watchlists for executives you're actively tracking

### 4. Benchmarking

- Create peer sets with 5-10 similar companies
- Include both competitors and aspirational peers
- Update peer sets as companies grow/change

### 5. API Usage

- Use pagination for large result sets
- Cache frequently-accessed data
- Batch operations when possible

---

## Troubleshooting

### Common Issues

**No leadership data for a company**
- Check if the company has a leadership page URL configured
- Schedule a website crawl job for the company
- Verify the website is accessible

**Low data quality scores**
- Check the enrichment queue for missing fields
- Infer emails using the email inferrer endpoint
- Add LinkedIn URLs manually if available

**Missing change alerts**
- Verify the collection jobs are running successfully
- Check the job stats for failures
- Increase the lookback period

### Getting Help

- API Documentation: http://localhost:8001/docs
- Swagger UI: http://localhost:8001/docs
- ReDoc: http://localhost:8001/redoc

---

## Next Steps

1. **Set up your portfolio** - Create portfolios and add companies
2. **Configure peer sets** - Define comparison groups
3. **Schedule collection jobs** - Start gathering data
4. **Monitor changes** - Subscribe to alerts and digests
5. **Analyze trends** - Use analytics endpoints for insights
