# T34: GitHub Repository Analytics

## Goal
Track developer activity as a proxy for tech company health, providing insights into engineering velocity, team growth, and project momentum.

## Why This Matters
- GitHub activity correlates with engineering team health and product velocity
- Commit frequency, contributor growth, and issue resolution are leading indicators
- Useful for due diligence on tech companies and portfolio monitoring
- Open source activity reveals company investment in developer ecosystem

## API Source
**GitHub REST API v3 / GraphQL API v4**
- Base URL: `https://api.github.com`
- Auth: Personal Access Token (PAT) or GitHub App
- Rate Limits: 5,000 requests/hour (authenticated), 60/hour (unauthenticated)
- Docs: https://docs.github.com/en/rest

## Endpoints to Implement

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/github/org/{org}` | GET | Organization overview (repos, members, activity summary) |
| `/github/org/{org}/repos` | GET | Repository list with metrics (stars, forks, language) |
| `/github/org/{org}/activity` | GET | Activity trends (commits, PRs, issues over time) |
| `/github/org/{org}/contributors` | GET | Top contributors and growth trends |
| `/github/org/{org}/score` | GET | Developer velocity score (0-100) |
| `/github/repo/{owner}/{repo}` | GET | Single repository details |

## Data Model

### Table: `github_organizations`
```sql
CREATE TABLE github_organizations (
    id SERIAL PRIMARY KEY,
    login VARCHAR(100) NOT NULL UNIQUE,
    name VARCHAR(255),
    description TEXT,
    blog VARCHAR(500),
    location VARCHAR(255),
    email VARCHAR(255),
    twitter_username VARCHAR(100),
    public_repos INTEGER,
    public_gists INTEGER,
    followers INTEGER,
    following INTEGER,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    -- Computed metrics
    total_stars INTEGER DEFAULT 0,
    total_forks INTEGER DEFAULT 0,
    total_contributors INTEGER DEFAULT 0,
    velocity_score INTEGER,
    last_fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Table: `github_repositories`
```sql
CREATE TABLE github_repositories (
    id SERIAL PRIMARY KEY,
    github_id BIGINT UNIQUE,
    org_login VARCHAR(100),
    name VARCHAR(255) NOT NULL,
    full_name VARCHAR(500) NOT NULL UNIQUE,
    description TEXT,
    homepage VARCHAR(500),
    language VARCHAR(100),
    languages JSONB,  -- {"Python": 50000, "JavaScript": 30000}
    stars INTEGER DEFAULT 0,
    forks INTEGER DEFAULT 0,
    watchers INTEGER DEFAULT 0,
    open_issues INTEGER DEFAULT 0,
    size_kb INTEGER,
    default_branch VARCHAR(100),
    is_fork BOOLEAN DEFAULT FALSE,
    is_archived BOOLEAN DEFAULT FALSE,
    is_private BOOLEAN DEFAULT FALSE,
    topics JSONB,  -- ["machine-learning", "python"]
    license_name VARCHAR(100),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    pushed_at TIMESTAMP,
    last_fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Table: `github_activity_snapshots`
```sql
CREATE TABLE github_activity_snapshots (
    id SERIAL PRIMARY KEY,
    org_login VARCHAR(100) NOT NULL,
    snapshot_date DATE NOT NULL,
    -- Weekly metrics
    commits_count INTEGER DEFAULT 0,
    prs_opened INTEGER DEFAULT 0,
    prs_merged INTEGER DEFAULT 0,
    issues_opened INTEGER DEFAULT 0,
    issues_closed INTEGER DEFAULT 0,
    contributors_active INTEGER DEFAULT 0,
    -- Cumulative metrics
    total_stars INTEGER DEFAULT 0,
    total_forks INTEGER DEFAULT 0,
    UNIQUE(org_login, snapshot_date)
);
```

### Table: `github_contributors`
```sql
CREATE TABLE github_contributors (
    id SERIAL PRIMARY KEY,
    org_login VARCHAR(100) NOT NULL,
    username VARCHAR(100) NOT NULL,
    avatar_url VARCHAR(500),
    contributions INTEGER DEFAULT 0,
    repos_contributed JSONB,  -- ["repo1", "repo2"]
    first_contribution_at TIMESTAMP,
    last_contribution_at TIMESTAMP,
    UNIQUE(org_login, username)
);
```

## Files to Create

```
app/sources/github/
├── __init__.py
├── client.py      # GitHub API client with rate limiting
└── ingest.py      # GitHubAnalyticsService

app/api/v1/github.py  # REST endpoints
```

## Implementation Details

### GitHubClient (`client.py`)
```python
class GitHubClient:
    """GitHub API client with rate limiting and caching."""

    BASE_URL = "https://api.github.com"

    async def get_organization(self, org: str) -> Dict
    async def get_org_repos(self, org: str, page: int = 1) -> List[Dict]
    async def get_repo_details(self, owner: str, repo: str) -> Dict
    async def get_repo_languages(self, owner: str, repo: str) -> Dict
    async def get_repo_contributors(self, owner: str, repo: str) -> List[Dict]
    async def get_commit_activity(self, owner: str, repo: str) -> List[Dict]
    async def get_repo_stats(self, owner: str, repo: str) -> Dict
```

### Velocity Score Calculation
Developer velocity score (0-100) based on:
- **Commit frequency** (30%): Weekly commits vs historical average
- **PR velocity** (25%): PRs merged / PRs opened ratio, time to merge
- **Issue resolution** (20%): Issues closed / issues opened ratio
- **Contributor growth** (15%): New contributors over time
- **Release cadence** (10%): Releases per quarter

### Rate Limiting Strategy
- Use conditional requests (If-None-Match) to save quota
- Implement token rotation for higher limits
- Cache responses with 1-hour TTL for org/repo data
- Queue requests with exponential backoff on 403

## Example Responses

### GET /github/org/openai
```json
{
  "login": "openai",
  "name": "OpenAI",
  "description": "OpenAI's mission is to ensure AGI benefits all of humanity.",
  "location": "San Francisco, CA",
  "public_repos": 185,
  "followers": 25000,
  "created_at": "2015-12-11",
  "metrics": {
    "total_stars": 450000,
    "total_forks": 75000,
    "total_contributors": 1200,
    "top_repos": ["openai-python", "whisper", "CLIP"],
    "primary_languages": ["Python", "TypeScript", "Jupyter Notebook"]
  },
  "velocity_score": 85
}
```

### GET /github/org/openai/activity
```json
{
  "org": "openai",
  "period": "last_12_weeks",
  "weekly_activity": [
    {
      "week": "2026-01-06",
      "commits": 342,
      "prs_opened": 45,
      "prs_merged": 38,
      "issues_opened": 120,
      "issues_closed": 95,
      "active_contributors": 67
    }
  ],
  "trends": {
    "commit_trend": "increasing",
    "contributor_trend": "stable",
    "issue_resolution_rate": 0.79
  }
}
```

### GET /github/org/openai/score
```json
{
  "org": "openai",
  "velocity_score": 85,
  "breakdown": {
    "commit_frequency": 90,
    "pr_velocity": 82,
    "issue_resolution": 79,
    "contributor_growth": 88,
    "release_cadence": 85
  },
  "percentile": 95,
  "comparison": "Top 5% of tech organizations by developer velocity"
}
```

## Environment Variables
```bash
GITHUB_TOKEN=ghp_xxxxxxxxxxxx  # Personal Access Token
# Or for higher rate limits:
GITHUB_APP_ID=123456
GITHUB_APP_PRIVATE_KEY=-----BEGIN RSA PRIVATE KEY-----...
```

## Dependencies
- `httpx` - Async HTTP client (already in project)
- No new dependencies required

## Testing Plan
1. Test with public orgs: `openai`, `microsoft`, `google`, `facebook`
2. Verify rate limit handling with burst requests
3. Test velocity score calculation with known orgs
4. Verify activity trends are accurate vs GitHub UI

## Acceptance Criteria
- [ ] Can fetch organization details and metrics
- [ ] Can list repositories with stars, forks, languages
- [ ] Activity trends show weekly commit/PR/issue data
- [ ] Velocity score calculated and explained
- [ ] Rate limiting works correctly (no 403 errors)
- [ ] All 6 endpoints return valid responses

## Plan Status
- [ ] Approved by user
