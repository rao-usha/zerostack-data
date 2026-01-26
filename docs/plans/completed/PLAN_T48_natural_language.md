# Plan T48: Natural Language Query - Revised

**Task ID:** T48
**Status:** COMPLETE
**Agent:** Tab 1
**Date:** 2026-01-19

---

## Approval

- [x] Approved by user (2026-01-19) - Option B+C with curated endpoints

---

## Research Summary

After researching MCP criticism and alternatives, here's what I found:

### MCP Criticisms ([sources](#sources))

1. **Context Window Bloat**: MCP tools consume 20%+ of context before any work begins
2. **No Composability**: Can't store intermediate results - entire payloads pass through context as text
3. **Security Issues**: 43% of MCP servers have command injection vulnerabilities; most lack auth
4. **"Full Circle" Problem**: LLMs now reliably call API specs, so why reinvent OpenAPI?
5. **Overhead**: Extra server process to run and maintain

### Key Insight

> "If LLMs can now reliably call API specs, then MCP feels like it's unnecessarily re-inventing OpenAPI's wheel." - Theo Browne

Since you're already using Claude Code, and Claude Code can:
- Read OpenAPI specs and documentation
- Make HTTP calls via curl/httpx
- Understand API responses
- Chain multiple calls together

**The simplest solution might be: good documentation + Claude Code's native capabilities.**

---

## Revised Approach Options

### Option A: Enhanced CLAUDE.md (Simplest)

Add Nexdata API documentation directly to your project's CLAUDE.md file.

**How it works:**
1. Add API overview and key endpoints to CLAUDE.md
2. Claude Code reads this on every session
3. You ask: "What fintech companies did Sequoia invest in?"
4. Claude Code makes the API calls directly (curl or Python)

**Example CLAUDE.md addition:**
```markdown
## Nexdata API (localhost:8001)

### Key Endpoints
- `GET /api/v1/search?q={query}&type=investor` - Search investors
- `GET /api/v1/investors/{id}/portfolio` - Get portfolio
- `GET /api/v1/scores/company/{name}` - Get company health score
- `GET /api/v1/predictions/deal/{id}` - Get deal win probability

### Example Queries
# Search for Sequoia
curl -s "http://localhost:8001/api/v1/search?q=Sequoia&type=investor"

# Get portfolio
curl -s "http://localhost:8001/api/v1/investors/123/portfolio"
```

**Pros:**
- Zero infrastructure - no servers to run
- Claude Code already does this naturally
- Easiest to maintain
- Works with any Claude interface

**Cons:**
- Limited context space in CLAUDE.md
- Manual documentation maintenance

---

### Option B: Claude Skill with Progressive Disclosure

Create a `/nexdata` slash command that loads relevant API context on demand.

**How it works:**
1. Create a skill file with comprehensive API docs
2. User types `/nexdata` or asks a data question
3. Skill loads relevant endpoint documentation
4. Claude makes API calls with full context

**Example skill file (`skills/nexdata.md`):**
```markdown
# Nexdata Query Skill

You have access to the Nexdata API at localhost:8001.

## Available Operations

### Investor Queries
- Search: `GET /api/v1/search?q={name}&type=investor`
- Details: `GET /api/v1/investors/{id}`
- Portfolio: `GET /api/v1/investors/{id}/portfolio`
- Similar: `GET /api/v1/discover/similar/{id}`

### Company Queries
- Search: `GET /api/v1/search?q={name}&type=company`
- Score: `GET /api/v1/scores/company/{name}`
- Enrichment: `GET /api/v1/enrichment/companies/{name}`
- News: `GET /api/v1/news/company/{name}`

[... more endpoints ...]

## Workflow
1. Parse user question to identify entities and intent
2. Make appropriate API calls using curl
3. Synthesize results into natural language answer
```

**Pros:**
- Progressive disclosure - only loads when needed
- Full documentation available
- Reusable across sessions
- Native Claude Code feature

**Cons:**
- Requires skill file maintenance
- Still uses context when loaded

---

### Option C: Python Helper Module

Create a simple Python module that Claude can import and use.

**How it works:**
1. Create `nexdata_client.py` with helper functions
2. Claude imports it in Python code blocks
3. Functions handle API calls and return structured data

**Example (`scripts/nexdata_client.py`):**
```python
"""Nexdata API client for Claude Code."""
import httpx

BASE_URL = "http://localhost:8001/api/v1"

def search_investors(query: str, limit: int = 10):
    """Search for investors by name."""
    r = httpx.get(f"{BASE_URL}/search", params={"q": query, "type": "investor", "limit": limit})
    return r.json()

def get_portfolio(investor_id: int):
    """Get investor's portfolio companies."""
    r = httpx.get(f"{BASE_URL}/investors/{investor_id}/portfolio")
    return r.json()

def get_company_score(name: str):
    """Get company health score (0-100)."""
    r = httpx.get(f"{BASE_URL}/scores/company/{name}")
    return r.json()

def compare_investors(id1: int, id2: int):
    """Compare two investor portfolios."""
    r = httpx.post(f"{BASE_URL}/compare/portfolios", json={"investor_ids": [id1, id2]})
    return r.json()
```

**Usage by Claude:**
```python
from scripts.nexdata_client import search_investors, get_portfolio

# Find Sequoia
investors = search_investors("Sequoia")
sequoia_id = investors["results"][0]["id"]

# Get their fintech holdings
portfolio = get_portfolio(sequoia_id)
fintech = [c for c in portfolio["companies"] if c["sector"] == "fintech"]
```

**Pros:**
- Type hints help Claude understand the API
- Reusable, testable code
- No context bloat - just imports
- Can add caching, error handling

**Cons:**
- Requires writing/maintaining client code
- Claude needs to know about it

---

### Option D: OpenAPI Spec Reference

Simply point Claude to the OpenAPI spec and let it figure out calls.

**How it works:**
1. Add to CLAUDE.md: "API spec at http://localhost:8001/openapi.json"
2. Claude fetches and parses spec when needed
3. Claude constructs appropriate API calls

**Pros:**
- Spec is always current (auto-generated by FastAPI)
- No manual documentation
- Full endpoint coverage

**Cons:**
- Large spec (50+ endpoints) may overwhelm context
- Less guided than curated docs

---

### Option E: MCP Server (Full Protocol)

Build an MCP server wrapping the API (original plan).

**Pros:**
- Standard protocol
- Works with any MCP client

**Cons:**
- Extra server to run
- Context bloat from tool definitions
- Security concerns
- Maintenance overhead
- May be overkill given simpler alternatives

---

## Recommendation

**Start with Option B (Claude Skill) + Option C (Python Helper)**

This hybrid gives you:
1. **Skill file** - Natural language interface with `/nexdata` command
2. **Python client** - Programmatic access when Claude needs to write code

No extra servers, no MCP overhead, leverages Claude Code's native abilities.

---

## Implementation Plan (Options B + C)

### Step 1: Create Python Client
```
scripts/
└── nexdata_client.py    # ~200 lines, core API functions
```

Functions to include:
- `search_investors(query)` / `search_companies(query)`
- `get_investor(id)` / `get_company(id)`
- `get_portfolio(investor_id)`
- `get_company_score(name)`
- `compare_investors(id1, id2)`
- `get_trends(sector=None)`
- `get_deal_prediction(deal_id)`
- `get_pipeline_insights()`

### Step 2: Create Skill File
```
.claude/skills/nexdata.md    # Comprehensive API guide
```

Contents:
- API overview
- Common query patterns
- Endpoint reference (curated, not exhaustive)
- Example workflows

### Step 3: Update CLAUDE.md
Add brief reference:
```markdown
## Nexdata API
- Python client: `scripts/nexdata_client.py`
- Full docs: `/nexdata` skill
- API running at localhost:8001
```

### Step 4: Test
- Ask natural language questions
- Verify Claude uses the client/skill appropriately
- Iterate on documentation

---

## Files to Create

| File | Purpose |
|------|---------|
| `scripts/nexdata_client.py` | Python API client |
| `.claude/skills/nexdata.md` | Skill with full API guide |
| Update `CLAUDE.md` | Brief API reference |

---

## Success Criteria

- [x] Python client covers 15+ key endpoints (20+ functions)
- [x] Skill file documents common workflows
- [x] Claude can answer "What companies did X invest in?"
- [x] Claude can answer "How healthy is company Y?"
- [x] Claude can compare two investors
- [x] No extra servers required

---

## Questions for User

1. **Approach**: Does Option B+C (Skill + Python Client) sound right, or prefer something simpler/different?

2. **Scope**: Should the Python client cover all 50+ endpoints, or a curated set of ~15 most useful ones?

3. **Location**: Should the skill go in `.claude/skills/` or somewhere else?

---

## Sources

- [MCP is Unnecessary - Tim Kellogg](https://timkellogg.me/blog/2025/04/27/mcp-is-unnecessary)
- [Was MCP a Mistake? - AI Engineering Report](https://www.aiengineering.report/p/was-mcp-a-mistake-the-internet-weighs)
- [Why MCP Won't Kill APIs - Zuplo](https://zuplo.com/blog/why-mcp-wont-kill-apis)
- [Why OpenAPI Should Be Foundation for MCP - Nordic APIs](https://nordicapis.com/why-openapi-should-be-the-foundation-for-your-mcp-server/)
- [MCP vs Function Calling - Descope](https://www.descope.com/blog/post/mcp-vs-function-calling)
- [MCP Enterprise Challenges - Xenoss](https://xenoss.io/blog/mcp-model-context-protocol-enterprise-use-cases-implementation-challenges)
