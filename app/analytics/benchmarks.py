"""
Market Benchmarks Service.

T29: Compare LP performance and allocations against market benchmarks,
enabling peer comparison and diversification analysis.
"""

import logging
import math
from typing import Dict, List, Optional, Any
from collections import defaultdict
from statistics import median, stdev

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# AUM size buckets for peer grouping
AUM_BUCKETS = {
    "small": (0, 1_000_000_000),           # < $1B
    "medium": (1_000_000_000, 10_000_000_000),  # $1B - $10B
    "large": (10_000_000_000, float('inf'))     # > $10B
}


class BenchmarkService:
    """
    Market benchmark calculation service.

    Provides peer group construction, allocation benchmarks,
    and diversification scoring for investor comparison.
    """

    def __init__(self, db: Session):
        self.db = db

    def _parse_aum(self, aum_str: Optional[str]) -> Optional[int]:
        """Parse AUM string to integer."""
        if not aum_str:
            return None

        if isinstance(aum_str, (int, float)):
            return int(aum_str)

        # Remove common prefixes/suffixes
        aum = str(aum_str).replace("$", "").replace(",", "").strip()

        multiplier = 1
        if aum.endswith("B") or aum.endswith("b"):
            multiplier = 1_000_000_000
            aum = aum[:-1]
        elif aum.endswith("M") or aum.endswith("m"):
            multiplier = 1_000_000
            aum = aum[:-1]
        elif aum.endswith("T") or aum.endswith("t"):
            multiplier = 1_000_000_000_000
            aum = aum[:-1]

        try:
            return int(float(aum) * multiplier)
        except (ValueError, TypeError):
            return None

    def _get_size_bucket(self, aum: Optional[int]) -> str:
        """Determine size bucket from AUM."""
        if not aum:
            return "unknown"

        for bucket, (min_val, max_val) in AUM_BUCKETS.items():
            if min_val <= aum < max_val:
                return bucket

        return "unknown"

    def _get_bucket_label(self, bucket: str) -> str:
        """Get human-readable bucket label."""
        labels = {
            "small": "Small (<$1B)",
            "medium": "Medium ($1B-$10B)",
            "large": "Large (>$10B)",
            "unknown": "Unknown"
        }
        return labels.get(bucket, bucket)

    def get_investor_info(self, investor_id: int, investor_type: str) -> Optional[Dict]:
        """Get investor information."""
        if investor_type == "lp":
            query = text("""
                SELECT id, name, lp_type as subtype, jurisdiction as location
                FROM lp_fund WHERE id = :id
            """)
        else:
            query = text("""
                SELECT id, name, type as subtype, region as location,
                       estimated_aum as aum
                FROM family_offices WHERE id = :id
            """)

        result = self.db.execute(query, {"id": investor_id})
        row = result.mappings().fetchone()
        return dict(row) if row else None

    def get_peer_group(self, investor_id: int, investor_type: str) -> Dict:
        """
        Find peer investors based on type and size.

        Peers are investors of the same type (LP subtype or family office)
        in similar AUM size buckets.
        """
        investor = self.get_investor_info(investor_id, investor_type)
        if not investor:
            return {"error": "Investor not found", "peers": []}

        investor_subtype = investor.get("subtype", "").lower()

        # Get all investors of same type
        if investor_type == "lp":
            query = text("""
                SELECT id, name, lp_type as subtype, jurisdiction as location
                FROM lp_fund
                WHERE id != :investor_id
            """)
        else:
            query = text("""
                SELECT id, name, type as subtype, region as location,
                       estimated_aum as aum
                FROM family_offices
                WHERE id != :investor_id
            """)

        result = self.db.execute(query, {"investor_id": investor_id})
        all_investors = list(result.mappings())

        # Filter by same subtype
        peers = []
        for inv in all_investors:
            inv_subtype = (inv.get("subtype") or "").lower()
            if inv_subtype == investor_subtype or not investor_subtype:
                peers.append({
                    "id": inv["id"],
                    "name": inv["name"],
                    "subtype": inv.get("subtype"),
                    "location": inv.get("location")
                })

        return {
            "investor_id": investor_id,
            "investor_name": investor.get("name"),
            "investor_type": investor_type,
            "subtype": investor.get("subtype"),
            "peer_count": len(peers),
            "peers": peers[:20]  # Limit to top 20
        }

    def get_portfolio_allocations(self, investor_id: int, investor_type: str) -> Dict:
        """Get sector allocations for an investor's portfolio."""
        query = text("""
            SELECT company_industry, COUNT(*) as count
            FROM portfolio_companies
            WHERE investor_id = :investor_id
              AND investor_type = :investor_type
              AND current_holding = 1
              AND company_industry IS NOT NULL
            GROUP BY company_industry
            ORDER BY count DESC
        """)

        result = self.db.execute(query, {
            "investor_id": investor_id,
            "investor_type": investor_type
        })
        rows = list(result.mappings())

        total = sum(r["count"] for r in rows)
        if total == 0:
            return {"sectors": [], "total": 0}

        allocations = []
        for row in rows:
            allocations.append({
                "sector": row["company_industry"],
                "count": row["count"],
                "allocation": round(row["count"] / total, 4)
            })

        return {"sectors": allocations, "total": total}

    def calculate_sector_benchmark(self, investor_type: str, subtype: Optional[str] = None) -> Dict:
        """
        Calculate median sector allocations for a peer group.

        Returns P25, median, P75 for each sector.
        """
        # Get all investors of this type
        if investor_type == "lp":
            query = text("""
                SELECT id, lp_type as subtype FROM lp_fund
            """)
        else:
            query = text("""
                SELECT id, type as subtype FROM family_offices
            """)

        result = self.db.execute(query)
        investors = list(result.mappings())

        # Filter by subtype if specified
        if subtype:
            investors = [i for i in investors if (i.get("subtype") or "").lower() == subtype.lower()]

        if not investors:
            return {"benchmarks": [], "sample_size": 0}

        # Get allocations for each investor
        sector_allocations = defaultdict(list)

        for inv in investors:
            allocs = self.get_portfolio_allocations(inv["id"], investor_type)
            for sector_data in allocs.get("sectors", []):
                sector_allocations[sector_data["sector"]].append(sector_data["allocation"])

        # Calculate percentiles for each sector
        benchmarks = []
        for sector, allocations in sector_allocations.items():
            if len(allocations) < 2:
                continue

            sorted_allocs = sorted(allocations)
            n = len(sorted_allocs)

            p25_idx = int(n * 0.25)
            p50_idx = int(n * 0.50)
            p75_idx = int(n * 0.75)

            benchmarks.append({
                "sector": sector,
                "p25": round(sorted_allocs[p25_idx], 4),
                "median": round(sorted_allocs[p50_idx], 4),
                "p75": round(sorted_allocs[min(p75_idx, n-1)], 4),
                "sample_size": n
            })

        # Sort by median allocation descending
        benchmarks.sort(key=lambda x: x["median"], reverse=True)

        return {
            "investor_type": investor_type,
            "subtype": subtype,
            "sample_size": len(investors),
            "benchmarks": benchmarks[:15]  # Top 15 sectors
        }

    def calculate_hhi(self, allocations: List[float]) -> float:
        """
        Calculate Herfindahl-Hirschman Index (HHI).

        HHI = sum of squared market shares
        Range: 0 to 1 (0 = perfectly diversified, 1 = concentrated)
        """
        if not allocations:
            return 1.0

        return sum(a ** 2 for a in allocations)

    def calculate_diversification_score(self, investor_id: int, investor_type: str) -> Dict:
        """
        Calculate diversification score for an investor.

        Score is based on:
        - Sector HHI (inverted to 0-100 where 100 = most diversified)
        - Number of unique sectors
        - Number of holdings
        """
        allocs = self.get_portfolio_allocations(investor_id, investor_type)
        sectors = allocs.get("sectors", [])

        if not sectors:
            return {
                "score": 0,
                "hhi": 1.0,
                "sector_count": 0,
                "holding_count": 0,
                "breakdown": {}
            }

        allocations = [s["allocation"] for s in sectors]
        hhi = self.calculate_hhi(allocations)

        # Convert HHI to score (0-100, where 100 = perfectly diversified)
        # HHI of 1/n (equal distribution) is considered "perfect"
        n = len(sectors)
        min_hhi = 1 / n if n > 0 else 1

        # Normalize: score = 100 * (1 - (hhi - min_hhi) / (1 - min_hhi))
        if hhi <= min_hhi:
            hhi_score = 100
        else:
            hhi_score = 100 * (1 - (hhi - min_hhi) / (1 - min_hhi))

        # Bonus for more sectors (up to 20 points)
        sector_bonus = min(n * 2, 20)

        # Combined score (capped at 100)
        total_score = min(hhi_score * 0.8 + sector_bonus, 100)

        return {
            "score": round(total_score, 1),
            "hhi": round(hhi, 4),
            "sector_count": n,
            "holding_count": allocs.get("total", 0),
            "breakdown": {
                "hhi_score": round(hhi_score, 1),
                "sector_bonus": sector_bonus
            }
        }

    def compare_to_benchmark(self, investor_id: int, investor_type: str) -> Dict:
        """
        Compare investor allocations to their peer benchmark.
        """
        investor = self.get_investor_info(investor_id, investor_type)
        if not investor:
            return {"error": "Investor not found"}

        # Get investor's allocations
        investor_allocs = self.get_portfolio_allocations(investor_id, investor_type)
        investor_sectors = {s["sector"]: s["allocation"] for s in investor_allocs.get("sectors", [])}

        # Get peer benchmark
        subtype = investor.get("subtype")
        benchmark = self.calculate_sector_benchmark(investor_type, subtype)

        # Get peer group info
        peer_group = self.get_peer_group(investor_id, investor_type)

        # Get diversification score
        div_score = self.calculate_diversification_score(investor_id, investor_type)

        # Compare each sector
        comparisons = []
        for bench in benchmark.get("benchmarks", []):
            sector = bench["sector"]
            investor_alloc = investor_sectors.get(sector, 0)
            bench_median = bench["median"]

            variance = investor_alloc - bench_median
            variance_pct = f"{variance * 100:+.1f}%"

            if investor_alloc > bench["p75"]:
                position = "above_p75"
            elif investor_alloc > bench_median:
                position = "above_median"
            elif investor_alloc > bench["p25"]:
                position = "below_median"
            elif investor_alloc > 0:
                position = "below_p25"
            else:
                position = "not_invested"

            comparisons.append({
                "sector": sector,
                "investor_allocation": round(investor_alloc, 4),
                "benchmark_median": bench_median,
                "benchmark_p25": bench["p25"],
                "benchmark_p75": bench["p75"],
                "variance": variance_pct,
                "position": position
            })

        return {
            "investor_id": investor_id,
            "investor_name": investor.get("name"),
            "investor_type": investor_type,
            "peer_group": {
                "type": investor.get("subtype") or investor_type,
                "peer_count": peer_group.get("peer_count", 0)
            },
            "sector_comparison": comparisons[:10],
            "diversification": {
                "investor_score": div_score["score"],
                "hhi": div_score["hhi"],
                "sector_count": div_score["sector_count"]
            }
        }

    def get_all_sector_benchmarks(self) -> Dict:
        """Get sector benchmarks for all investor types."""
        benchmarks_by_type = []

        # LP subtypes
        lp_types = ["Public Pension", "Corporate Pension", "Endowment",
                    "Foundation", "Sovereign Wealth Fund", "Insurance"]

        for lp_type in lp_types:
            bench = self.calculate_sector_benchmark("lp", lp_type)
            if bench.get("sample_size", 0) > 0:
                benchmarks_by_type.append({
                    "investor_type": f"LP - {lp_type}",
                    "sample_size": bench["sample_size"],
                    "sector_allocations": bench["benchmarks"][:10]
                })

        # Family offices
        fo_bench = self.calculate_sector_benchmark("family_office")
        if fo_bench.get("sample_size", 0) > 0:
            benchmarks_by_type.append({
                "investor_type": "Family Office",
                "sample_size": fo_bench["sample_size"],
                "sector_allocations": fo_bench["benchmarks"][:10]
            })

        # Overall market
        overall = self.calculate_sector_benchmark("lp")
        fo_overall = self.calculate_sector_benchmark("family_office")

        return {
            "benchmarks_by_type": benchmarks_by_type,
            "overall_market": {
                "lp_sample_size": overall.get("sample_size", 0),
                "family_office_sample_size": fo_overall.get("sample_size", 0),
                "lp_sectors": overall.get("benchmarks", [])[:10],
                "family_office_sectors": fo_overall.get("benchmarks", [])[:10]
            }
        }

    def get_diversification_rankings(self, investor_type: Optional[str] = None,
                                      limit: int = 20) -> Dict:
        """
        Get diversification score rankings for all investors.
        """
        rankings = []

        # Get LPs
        if investor_type is None or investor_type == "lp":
            query = text("SELECT id, name, lp_type as subtype FROM lp_fund")
            result = self.db.execute(query)
            for row in result.mappings():
                score = self.calculate_diversification_score(row["id"], "lp")
                if score["holding_count"] > 0:
                    rankings.append({
                        "investor_id": row["id"],
                        "investor_name": row["name"],
                        "investor_type": "lp",
                        "subtype": row.get("subtype"),
                        "diversification_score": score["score"],
                        "sector_count": score["sector_count"],
                        "holding_count": score["holding_count"],
                        "hhi": score["hhi"]
                    })

        # Get Family Offices
        if investor_type is None or investor_type == "family_office":
            query = text("SELECT id, name, type as subtype FROM family_offices")
            result = self.db.execute(query)
            for row in result.mappings():
                score = self.calculate_diversification_score(row["id"], "family_office")
                if score["holding_count"] > 0:
                    rankings.append({
                        "investor_id": row["id"],
                        "investor_name": row["name"],
                        "investor_type": "family_office",
                        "subtype": row.get("subtype"),
                        "diversification_score": score["score"],
                        "sector_count": score["sector_count"],
                        "holding_count": score["holding_count"],
                        "hhi": score["hhi"]
                    })

        # Sort by score descending
        rankings.sort(key=lambda x: x["diversification_score"], reverse=True)

        # Add ranks
        for i, r in enumerate(rankings):
            r["rank"] = i + 1

        # Calculate distribution stats
        scores = [r["diversification_score"] for r in rankings]
        distribution = {}
        if scores:
            distribution = {
                "mean": round(sum(scores) / len(scores), 1),
                "median": round(sorted(scores)[len(scores) // 2], 1),
                "min": round(min(scores), 1),
                "max": round(max(scores), 1)
            }
            if len(scores) > 1:
                distribution["std_dev"] = round(stdev(scores), 1)

        return {
            "rankings": rankings[:limit],
            "total_investors": len(rankings),
            "score_distribution": distribution
        }
