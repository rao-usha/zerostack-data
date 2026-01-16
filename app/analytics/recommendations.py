"""
Investor Similarity and Recommendations Engine (T18).

Provides:
- Jaccard similarity between investor portfolios
- Find similar investors
- Company recommendations based on similar investor holdings
- Portfolio overlap analysis
"""

import logging
from typing import Optional, List, Set, Dict, Any
from dataclasses import dataclass, field
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


@dataclass
class SimilarInvestor:
    """A similar investor with similarity metrics."""
    investor_id: int
    investor_type: str
    name: str
    similarity_score: float  # 0.0 to 1.0
    overlap_count: int
    overlap_companies: List[str] = field(default_factory=list)


@dataclass
class CompanyRecommendation:
    """A recommended company based on similar investor holdings."""
    company_name: str
    company_industry: Optional[str]
    held_by_count: int
    held_by_names: List[str] = field(default_factory=list)
    confidence: float = 0.0


@dataclass
class PortfolioOverlap:
    """Overlap analysis between two investor portfolios."""
    investor_a_id: int
    investor_a_name: str
    investor_a_holdings: int
    investor_b_id: int
    investor_b_name: str
    investor_b_holdings: int
    overlap_count: int
    overlap_percentage_a: float
    overlap_percentage_b: float
    jaccard_similarity: float
    shared_companies: List[str] = field(default_factory=list)


class RecommendationEngine:
    """
    Engine for computing investor similarity and generating recommendations.

    Uses Jaccard index for similarity:
        J(A, B) = |A ∩ B| / |A ∪ B|

    Where A and B are sets of companies held by investors.
    """

    def __init__(self, db: Session):
        self.db = db

    def get_investor_info(self, investor_id: int) -> Optional[Dict[str, Any]]:
        """Get basic investor information."""
        result = self.db.execute(text("""
            SELECT id, name, lp_type, jurisdiction
            FROM lp_fund
            WHERE id = :investor_id
        """), {"investor_id": investor_id})

        row = result.fetchone()
        if not row:
            return None

        return {
            "id": row.id,
            "name": row.name,
            "investor_type": row.lp_type,
            "location": row.jurisdiction
        }

    def get_investor_holdings(self, investor_id: int) -> Set[str]:
        """Get set of company names held by an investor."""
        result = self.db.execute(text("""
            SELECT DISTINCT company_name
            FROM portfolio_companies
            WHERE investor_id = :investor_id
              AND company_name IS NOT NULL
              AND company_name != ''
        """), {"investor_id": investor_id})

        return {row.company_name for row in result}

    def _calculate_jaccard(self, set_a: Set[str], set_b: Set[str]) -> float:
        """
        Calculate Jaccard similarity index between two sets.

        J(A, B) = |A ∩ B| / |A ∪ B|

        Returns 0.0 if both sets are empty.
        """
        if not set_a and not set_b:
            return 0.0

        intersection = len(set_a & set_b)
        union = len(set_a | set_b)

        if union == 0:
            return 0.0

        return intersection / union

    def get_similar_investors(
        self,
        investor_id: int,
        investor_type: Optional[str] = None,
        limit: int = 10,
        min_overlap: int = 1
    ) -> List[SimilarInvestor]:
        """
        Find investors with similar portfolios based on Jaccard similarity.

        Args:
            investor_id: Target investor to find similar ones for
            investor_type: Optional filter (public_pension, sovereign_wealth, etc.)
            limit: Maximum number of similar investors to return
            min_overlap: Minimum number of shared holdings required

        Returns:
            List of similar investors sorted by similarity score (descending)
        """
        # Get target investor's holdings
        target_holdings = self.get_investor_holdings(investor_id)

        if not target_holdings:
            logger.warning(f"No holdings found for investor {investor_id}")
            return []

        # Build query for other investors
        type_filter = ""
        params = {
            "investor_id": investor_id,
            "min_overlap": min_overlap,
            "limit": limit
        }

        if investor_type:
            type_filter = "AND l.lp_type = :investor_type"
            params["investor_type"] = investor_type

        # Use SQL to compute overlap efficiently
        # This query finds investors with at least min_overlap shared companies
        result = self.db.execute(text(f"""
            WITH target_holdings AS (
                SELECT DISTINCT company_name
                FROM portfolio_companies
                WHERE investor_id = :investor_id
                  AND company_name IS NOT NULL
                  AND company_name != ''
            ),
            investor_overlaps AS (
                SELECT
                    p.investor_id,
                    COUNT(DISTINCT p.company_name) as total_holdings,
                    COUNT(DISTINCT CASE WHEN t.company_name IS NOT NULL THEN p.company_name END) as overlap_count
                FROM portfolio_companies p
                LEFT JOIN target_holdings t ON t.company_name = p.company_name
                WHERE p.investor_id != :investor_id
                  AND p.company_name IS NOT NULL
                  AND p.company_name != ''
                GROUP BY p.investor_id
                HAVING COUNT(DISTINCT CASE WHEN t.company_name IS NOT NULL THEN p.company_name END) >= :min_overlap
            )
            SELECT
                io.investor_id,
                l.name,
                l.lp_type,
                io.total_holdings,
                io.overlap_count
            FROM investor_overlaps io
            JOIN lp_fund l ON l.id = io.investor_id
            WHERE 1=1 {type_filter}
            ORDER BY io.overlap_count DESC
            LIMIT :limit
        """), params)

        similar_investors = []
        target_holdings_count = len(target_holdings)

        for row in result:
            # Calculate Jaccard: intersection / union
            # intersection = overlap_count
            # union = target_holdings + other_holdings - overlap
            union_size = target_holdings_count + row.total_holdings - row.overlap_count

            if union_size > 0:
                jaccard = row.overlap_count / union_size
            else:
                jaccard = 0.0

            # Get sample of overlapping companies
            overlap_result = self.db.execute(text("""
                SELECT DISTINCT p.company_name
                FROM portfolio_companies p
                WHERE p.investor_id = :other_id
                  AND p.company_name IN (
                      SELECT company_name FROM portfolio_companies
                      WHERE investor_id = :target_id
                  )
                LIMIT 5
            """), {"other_id": row.investor_id, "target_id": investor_id})

            overlap_companies = [r.company_name for r in overlap_result]

            similar_investors.append(SimilarInvestor(
                investor_id=row.investor_id,
                investor_type=row.lp_type,
                name=row.name,
                similarity_score=jaccard,
                overlap_count=row.overlap_count,
                overlap_companies=overlap_companies
            ))

        # Sort by Jaccard similarity (may differ from overlap count ordering)
        similar_investors.sort(key=lambda x: x.similarity_score, reverse=True)

        return similar_investors[:limit]

    def get_recommended_companies(
        self,
        investor_id: int,
        similar_count: int = 10,
        limit: int = 20
    ) -> List[CompanyRecommendation]:
        """
        Get company recommendations based on what similar investors hold.

        "Investors like X also invest in Y"

        Args:
            investor_id: Target investor to get recommendations for
            similar_count: Number of similar investors to consider
            limit: Maximum recommendations to return

        Returns:
            List of recommended companies sorted by popularity among similar investors
        """
        # Get similar investors
        similar_investors = self.get_similar_investors(
            investor_id,
            limit=similar_count,
            min_overlap=1
        )

        if not similar_investors:
            logger.info(f"No similar investors found for {investor_id}")
            return []

        similar_ids = [s.investor_id for s in similar_investors]
        similar_names = {s.investor_id: s.name for s in similar_investors}

        # Get target investor's current holdings
        target_holdings = self.get_investor_holdings(investor_id)

        if not target_holdings:
            return []

        # Find companies held by similar investors but not by target
        result = self.db.execute(text("""
            SELECT
                p.company_name,
                p.company_industry,
                COUNT(DISTINCT p.investor_id) as held_by_count,
                ARRAY_AGG(DISTINCT p.investor_id) as investor_ids
            FROM portfolio_companies p
            WHERE p.investor_id = ANY(:similar_ids)
              AND p.company_name IS NOT NULL
              AND p.company_name != ''
              AND p.company_name NOT IN (
                  SELECT company_name
                  FROM portfolio_companies
                  WHERE investor_id = :target_id
              )
            GROUP BY p.company_name, p.company_industry
            ORDER BY held_by_count DESC, p.company_name
            LIMIT :limit
        """), {
            "similar_ids": similar_ids,
            "target_id": investor_id,
            "limit": limit
        })

        recommendations = []
        max_similar = len(similar_investors)

        for row in result:
            # Confidence based on how many similar investors hold it
            confidence = row.held_by_count / max_similar if max_similar > 0 else 0.0

            # Get names of investors who hold this company
            held_by_names = [
                similar_names.get(inv_id, f"Investor {inv_id}")
                for inv_id in (row.investor_ids or [])
                if inv_id in similar_names
            ]

            recommendations.append(CompanyRecommendation(
                company_name=row.company_name,
                company_industry=row.company_industry,
                held_by_count=row.held_by_count,
                held_by_names=held_by_names[:5],  # Limit to first 5 names
                confidence=confidence
            ))

        return recommendations

    def get_portfolio_overlap(
        self,
        investor_a_id: int,
        investor_b_id: int
    ) -> Optional[PortfolioOverlap]:
        """
        Analyze portfolio overlap between two investors.

        Args:
            investor_a_id: First investor ID
            investor_b_id: Second investor ID

        Returns:
            PortfolioOverlap with detailed comparison, or None if investors not found
        """
        # Get investor info
        investor_a = self.get_investor_info(investor_a_id)
        investor_b = self.get_investor_info(investor_b_id)

        if not investor_a or not investor_b:
            return None

        # Get holdings
        holdings_a = self.get_investor_holdings(investor_a_id)
        holdings_b = self.get_investor_holdings(investor_b_id)

        # Calculate overlap
        shared = holdings_a & holdings_b
        overlap_count = len(shared)

        # Calculate percentages
        pct_a = (overlap_count / len(holdings_a) * 100) if holdings_a else 0.0
        pct_b = (overlap_count / len(holdings_b) * 100) if holdings_b else 0.0

        # Calculate Jaccard
        jaccard = self._calculate_jaccard(holdings_a, holdings_b)

        # Get list of shared companies (limited for response size)
        shared_list = sorted(list(shared))[:50]

        return PortfolioOverlap(
            investor_a_id=investor_a_id,
            investor_a_name=investor_a["name"],
            investor_a_holdings=len(holdings_a),
            investor_b_id=investor_b_id,
            investor_b_name=investor_b["name"],
            investor_b_holdings=len(holdings_b),
            overlap_count=overlap_count,
            overlap_percentage_a=round(pct_a, 2),
            overlap_percentage_b=round(pct_b, 2),
            jaccard_similarity=round(jaccard, 4),
            shared_companies=shared_list
        )
