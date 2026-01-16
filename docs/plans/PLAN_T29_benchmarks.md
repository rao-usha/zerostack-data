# Plan T29: Market Benchmarks

**Task ID:** T29
**Status:** PLANNING
**Agent:** Tab 1
**Date:** 2026-01-16
**Dependencies:** T23 (Investment Trend Analysis) ✅

---

## Goal

Compare LP performance and allocations against market benchmarks, enabling investors to understand how their portfolios stack up against peers.

---

## Why This Matters

1. **Peer Comparison**: See how portfolio allocations compare to similar investors
2. **Diversification Analysis**: Identify over/under-concentration vs benchmarks
3. **Best Practices**: Learn from top-performing peer allocation patterns
4. **Gap Analysis**: Find sectors/stages where investor is under-represented

---

## Design

### Peer Group Construction

Investors are grouped by:
- **Type**: LP (pension, endowment, foundation, sovereign) vs Family Office
- **Size**: AUM buckets (small <$1B, medium $1-10B, large >$10B)
- **Geography**: Region of investor headquarters

### API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/benchmarks/investor/{id}` | GET | Investor vs their peer benchmark |
| `/benchmarks/peer-group/{id}` | GET | Detailed peer group comparison |
| `/benchmarks/sectors` | GET | Sector allocation benchmarks by LP type |
| `/benchmarks/diversification` | GET | Diversification scores ranking |

### Benchmark Metrics

```python
class BenchmarkService:
    """Market benchmark calculation service."""

    def get_peer_group(self, investor_id: int, investor_type: str) -> List[Dict]:
        """Find peer investors based on type and size."""

    def calculate_sector_benchmark(self, peer_group: List[int]) -> Dict:
        """Calculate median sector allocations for peer group."""

    def calculate_diversification_score(self, investor_id: int) -> Dict:
        """
        Score portfolio diversification (0-100):
        - Sector diversification (HHI-based)
        - Stage diversification
        - Geographic diversification
        """

    def compare_to_benchmark(self, investor_id: int) -> Dict:
        """Compare investor allocations to peer benchmark."""

    def get_benchmark_trends(self, peer_type: str) -> Dict:
        """Get benchmark allocation trends over time."""
```

---

## Response Formats

### Investor vs Benchmark
```json
{
  "investor_id": 1,
  "investor_name": "CalPERS",
  "peer_group": {
    "type": "Public Pension",
    "size_bucket": "Large (>$10B)",
    "peer_count": 15
  },
  "sector_comparison": [
    {
      "sector": "Technology",
      "investor_allocation": 0.35,
      "benchmark_median": 0.28,
      "benchmark_p25": 0.20,
      "benchmark_p75": 0.35,
      "variance": "+7%",
      "position": "above_median"
    }
  ],
  "diversification": {
    "investor_score": 72,
    "benchmark_median": 68,
    "percentile": 65
  }
}
```

### Sector Benchmarks
```json
{
  "benchmarks_by_type": [
    {
      "investor_type": "Public Pension",
      "sample_size": 15,
      "sector_allocations": [
        {"sector": "Technology", "median": 0.28, "p25": 0.20, "p75": 0.35},
        {"sector": "Healthcare", "median": 0.22, "p25": 0.15, "p75": 0.28}
      ]
    }
  ],
  "overall_market": {
    "sample_size": 50,
    "sector_allocations": [...]
  }
}
```

### Diversification Ranking
```json
{
  "rankings": [
    {
      "rank": 1,
      "investor_id": 5,
      "investor_name": "Yale Endowment",
      "investor_type": "Endowment",
      "diversification_score": 89,
      "sector_count": 12,
      "hhi": 0.08
    }
  ],
  "score_distribution": {
    "mean": 65,
    "median": 68,
    "std_dev": 15
  }
}
```

---

## Implementation

### 1. `app/analytics/benchmarks.py`
Main benchmarking service with:
- Peer group construction logic
- HHI (Herfindahl-Hirschman Index) calculation for concentration
- Percentile calculations for benchmark comparisons
- Integration with T23 TrendAnalysisService for allocation data

### 2. `app/api/v1/benchmarks.py`
REST API endpoints for benchmark queries.

---

## Files to Create/Modify

1. `app/analytics/benchmarks.py` - New benchmark service
2. `app/api/v1/benchmarks.py` - New API router
3. `app/main.py` - Register benchmarks router

---

## Testing Plan

1. Start server: `docker-compose up --build -d`
2. Test endpoints:
   - `GET /api/v1/benchmarks/investor/1?investor_type=lp`
   - `GET /api/v1/benchmarks/peer-group/1?investor_type=lp`
   - `GET /api/v1/benchmarks/sectors`
   - `GET /api/v1/benchmarks/diversification`

---

## Success Criteria

- [ ] Peer groups are correctly constructed by type/size
- [ ] Sector benchmarks show median, P25, P75 allocations
- [ ] Diversification scores use HHI calculation
- [ ] Investor comparison shows variance from benchmark
- [ ] All 4 endpoints return properly formatted data

---

## Approval

- [x] **Approved by user** (2026-01-16)

---

*Plan created: 2026-01-16*
