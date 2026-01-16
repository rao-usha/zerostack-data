# Plan T21: Co-investor Network Graph

**Task ID:** T21
**Status:** PLANNING
**Agent:** Tab 1
**Date:** 2026-01-16

---

## Goal

Build a network graph data structure showing investor relationships based on shared portfolio investments. Enable visualization of co-investor networks, relationship strength, and investor clusters.

---

## Why This Matters

1. **Relationship Discovery**: See which investors frequently co-invest together
2. **Network Effects**: Identify central/influential investors in the ecosystem
3. **Deal Flow**: Understand syndication patterns and investor groups
4. **Due Diligence**: Research potential co-investors for deals

---

## Data Sources

Leveraging existing tables:
- `co_investments` - Direct co-investor relationships from research
- `portfolio_companies` - Shared companies imply relationships

---

## Design

### Network Model

```
Node: Investor (LP or Family Office)
  - id, type, name
  - metrics: degree, centrality, cluster_id

Edge: Co-investment relationship
  - source_id, target_id
  - weight (# shared investments)
  - shared_companies[]
  - first_coinvestment_date
  - last_coinvestment_date
```

### Key Metrics

1. **Degree**: Number of co-investor connections
2. **Weighted Degree**: Sum of relationship weights
3. **Betweenness Centrality**: How often investor is on shortest path between others
4. **Cluster Coefficient**: How interconnected an investor's co-investors are

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/network/investor/{id}` | GET | Co-investor network for specific investor |
| `/network/graph` | GET | Full network data for visualization |
| `/network/clusters` | GET | Detected investor clusters |
| `/network/central` | GET | Most connected/central investors |
| `/network/path` | GET | Shortest path between two investors |

---

## Implementation

### 1. Network Engine (`app/network/graph.py`)

```python
class NetworkEngine:
    """Co-investor network analysis engine."""

    def build_network(self) -> Dict:
        """Build full co-investor network from database."""

    def get_investor_network(self, investor_id: int, investor_type: str, depth: int = 1) -> Dict:
        """Get ego network for specific investor."""

    def calculate_centrality(self) -> List[Dict]:
        """Calculate centrality metrics for all investors."""

    def detect_clusters(self) -> List[Dict]:
        """Detect investor clusters using community detection."""

    def find_path(self, source_id: int, target_id: int) -> List[Dict]:
        """Find shortest co-investment path between investors."""
```

### 2. Data Aggregation

Two sources of co-investment data:

**Source A: co_investments table**
```sql
SELECT primary_investor_id, primary_investor_type,
       co_investor_name, co_investment_count
FROM co_investments
```

**Source B: Shared portfolio companies**
```sql
-- Find investors who share portfolio companies
SELECT DISTINCT
    a.investor_id as investor_a,
    a.investor_type as type_a,
    b.investor_id as investor_b,
    b.investor_type as type_b,
    COUNT(DISTINCT a.company_name) as shared_count
FROM portfolio_companies a
JOIN portfolio_companies b
    ON LOWER(a.company_name) = LOWER(b.company_name)
    AND (a.investor_id != b.investor_id OR a.investor_type != b.investor_type)
WHERE a.current_holding = 1 AND b.current_holding = 1
GROUP BY a.investor_id, a.investor_type, b.investor_id, b.investor_type
```

### 3. Response Format

```json
{
  "nodes": [
    {
      "id": "lp_1",
      "type": "lp",
      "name": "CalPERS",
      "degree": 15,
      "centrality": 0.85,
      "cluster_id": 1
    }
  ],
  "edges": [
    {
      "source": "lp_1",
      "target": "fo_5",
      "weight": 3,
      "shared_companies": ["Stripe", "SpaceX", "OpenAI"]
    }
  ],
  "clusters": [
    {
      "id": 1,
      "name": "Tech-focused LPs",
      "members": ["lp_1", "lp_3", "fo_5"],
      "common_sectors": ["Technology", "AI/ML"]
    }
  ],
  "stats": {
    "total_nodes": 49,
    "total_edges": 127,
    "avg_degree": 5.2,
    "density": 0.11
  }
}
```

---

## Files to Create

### 1. `app/network/__init__.py`
Empty init file for package.

### 2. `app/network/graph.py`
Network analysis engine with all computation logic.

### 3. `app/api/v1/network.py`
FastAPI router with 5 endpoints.

---

## Testing Plan

1. Start server: `docker-compose up --build -d`
2. Test endpoints:
   - `GET /api/v1/network/investor/1?investor_type=lp`
   - `GET /api/v1/network/graph?limit=100`
   - `GET /api/v1/network/clusters`
   - `GET /api/v1/network/central?limit=10`
   - `GET /api/v1/network/path?source_id=1&source_type=lp&target_id=5&target_type=fo`

---

## Success Criteria

- [ ] Network engine builds graph from co_investments + shared portfolios
- [ ] Investor ego network returns correct co-investors with weights
- [ ] Full graph export works for visualization tools
- [ ] Centrality metrics calculated correctly
- [ ] Cluster detection groups similar investors
- [ ] Path finding works between any two connected investors

---

## Approval

- [x] **Approved by user** (2026-01-16)

## Implementation Notes

- Built network from co_investments table + portfolio_companies shared holdings
- Simple connected components for clustering (no external graph libraries)
- BFS for path finding between investors
- Centrality based on normalized degree

---

*Plan created: 2026-01-16*
*Completed: 2026-01-16*
