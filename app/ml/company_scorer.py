"""
Company Scoring Model.

Quantifies portfolio company health into actionable 0-100 scores
using weighted signals from multiple data sources.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Model configuration
MODEL_VERSION = "v1.0"

# Category weights
WEIGHTS = {
    "growth": 0.30,
    "stability": 0.25,
    "market_position": 0.25,
    "tech_velocity": 0.20,
}

# Tier thresholds
TIER_THRESHOLDS = [
    (80, "A"),  # 80-100
    (60, "B"),  # 60-79
    (40, "C"),  # 40-59
    (20, "D"),  # 20-39
    (0, "F"),  # 0-19
]


class CompanyScorer:
    """
    Company scoring engine.

    Aggregates signals from enrichment, GitHub, and web traffic
    into composite scores with category breakdowns.
    """

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Ensure scoring tables exist."""
        create_scores = text("""
            CREATE TABLE IF NOT EXISTS company_scores (
                id SERIAL PRIMARY KEY,
                company_name VARCHAR(255) NOT NULL,
                composite_score FLOAT NOT NULL,
                growth_score FLOAT,
                stability_score FLOAT,
                market_score FLOAT,
                tech_score FLOAT,
                confidence FLOAT,
                tier VARCHAR(1),
                explanation JSONB,
                data_sources JSONB,
                scored_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                model_version VARCHAR(20) DEFAULT 'v1.0',
                UNIQUE(company_name, model_version)
            )
        """)

        create_index = text("""
            CREATE INDEX IF NOT EXISTS idx_company_scores_composite
            ON company_scores(composite_score DESC)
        """)

        try:
            self.db.execute(create_scores)
            self.db.execute(create_index)
            self.db.commit()
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    def _get_tier(self, score: float) -> str:
        """Get tier letter for a score."""
        for threshold, tier in TIER_THRESHOLDS:
            if score >= threshold:
                return tier
        return "F"

    def _normalize_score(self, value: float, min_val: float, max_val: float) -> float:
        """Normalize a value to 0-100 scale."""
        if max_val == min_val:
            return 50.0
        normalized = ((value - min_val) / (max_val - min_val)) * 100
        return max(0.0, min(100.0, normalized))

    def _get_enrichment_data(self, company_name: str) -> Optional[Dict]:
        """Get company enrichment data from T22."""
        query = text("""
            SELECT * FROM company_enrichment
            WHERE LOWER(company_name) = LOWER(:name)
        """)
        try:
            result = self.db.execute(query, {"name": company_name})
            row = result.mappings().fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.warning(f"Error fetching enrichment data: {e}")
            return None

    def _get_github_data(self, company_name: str) -> Optional[Dict]:
        """Get GitHub organization data from T34."""
        # Try exact match first, then fuzzy
        query = text("""
            SELECT * FROM github_organizations
            WHERE LOWER(name) = LOWER(:name)
               OR LOWER(login) = LOWER(:name)
            LIMIT 1
        """)
        try:
            result = self.db.execute(query, {"name": company_name})
            row = result.mappings().fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.warning(f"Error fetching GitHub data: {e}")
            return None

    def _get_web_traffic_rank(self, company_name: str) -> Optional[int]:
        """
        Get Tranco rank for company domain.

        Note: This is a simplified lookup. In production,
        would use domain mapping or live Tranco API.
        """
        # Try to infer domain from company name
        # Check if we have cached traffic data
        # For now, return None - would integrate with T35 client
        return None

    def _calculate_growth_score(
        self, enrichment: Optional[Dict], github: Optional[Dict]
    ) -> tuple[float, List[str], List[str]]:
        """
        Calculate growth score (0-100).

        Signals:
        - Employee growth YoY
        - Funding recency (months since last funding)
        - Contributor growth (GitHub)
        """
        scores = []
        strengths = []
        improvements = []

        # Employee growth
        if enrichment and enrichment.get("employee_growth_yoy"):
            growth = enrichment["employee_growth_yoy"]
            # 20%+ growth = 100, 0% = 50, negative = lower
            emp_score = self._normalize_score(growth, -0.2, 0.3)
            scores.append(emp_score)
            if growth > 0.15:
                strengths.append("Strong employee growth")
            elif growth < 0:
                improvements.append("Declining employee count")

        # Funding recency
        if enrichment and enrichment.get("last_funding_date"):
            try:
                last_funding = enrichment["last_funding_date"]
                if isinstance(last_funding, str):
                    last_funding = datetime.fromisoformat(last_funding)
                months_ago = (datetime.now() - last_funding).days / 30
                # Recent funding (< 12 months) = 100, > 36 months = 0
                funding_score = self._normalize_score(36 - months_ago, 0, 36)
                scores.append(funding_score)
                if months_ago < 12:
                    strengths.append("Recent funding activity")
                elif months_ago > 24:
                    improvements.append("No recent funding")
            except (ValueError, TypeError):
                pass

        # GitHub contributor trend (if available)
        if github and github.get("total_contributors"):
            contributors = github["total_contributors"]
            # More contributors generally = better growth signal
            contrib_score = self._normalize_score(contributors, 0, 100)
            scores.append(contrib_score)
            if contributors > 50:
                strengths.append("Large contributor base")

        if not scores:
            return 50.0, strengths, improvements  # Default to neutral

        return sum(scores) / len(scores), strengths, improvements

    def _calculate_stability_score(
        self, enrichment: Optional[Dict]
    ) -> tuple[float, List[str], List[str]]:
        """
        Calculate stability score (0-100).

        Signals:
        - Revenue presence
        - Net income (profitability)
        - Company status (active vs troubled)
        """
        scores = []
        strengths = []
        improvements = []

        # Has revenue data (indicates established business)
        if enrichment and enrichment.get("latest_revenue"):
            revenue = enrichment["latest_revenue"]
            # Normalize revenue: $1M = 20, $100M = 60, $1B+ = 100
            if revenue > 0:
                import math

                log_revenue = math.log10(max(revenue, 1))
                rev_score = self._normalize_score(log_revenue, 5, 10)  # $100K to $10B
                scores.append(rev_score)
                if revenue > 100_000_000:
                    strengths.append("Strong revenue base")

        # Profitability
        if enrichment and enrichment.get("latest_net_income") is not None:
            net_income = enrichment["latest_net_income"]
            if net_income > 0:
                scores.append(80.0)
                strengths.append("Profitable")
            elif net_income < 0:
                # Loss is not always bad for growth companies
                scores.append(40.0)
                improvements.append("Currently unprofitable")

        # Company status
        if enrichment:
            status = enrichment.get("company_status", "active")
            if status == "active":
                scores.append(70.0)
            elif status == "ipo":
                scores.append(90.0)
                strengths.append("Public company (IPO)")
            elif status == "acquired":
                scores.append(60.0)
            elif status in ("bankrupt", "defunct"):
                scores.append(10.0)
                improvements.append("Company no longer active")

        if not scores:
            return 50.0, strengths, improvements

        return sum(scores) / len(scores), strengths, improvements

    def _calculate_market_score(
        self,
        enrichment: Optional[Dict],
        github: Optional[Dict],
        tranco_rank: Optional[int],
    ) -> tuple[float, List[str], List[str]]:
        """
        Calculate market position score (0-100).

        Signals:
        - Web traffic rank (Tranco)
        - GitHub stars (community mindshare)
        - Industry presence
        """
        scores = []
        strengths = []
        improvements = []

        # Tranco rank (lower = better)
        if tranco_rank:
            # Top 1000 = 100, Top 10K = 80, Top 100K = 60, Top 1M = 40
            import math

            log_rank = math.log10(max(tranco_rank, 1))
            rank_score = self._normalize_score(6 - log_rank, 0, 6)  # 1 to 1M
            scores.append(rank_score)
            if tranco_rank < 10000:
                strengths.append(f"High web traffic (Tranco #{tranco_rank})")

        # GitHub stars
        if github and github.get("total_stars"):
            stars = github["total_stars"]
            import math

            # 100 stars = 40, 1K = 60, 10K = 80, 100K+ = 100
            log_stars = math.log10(max(stars, 1))
            star_score = self._normalize_score(log_stars, 1, 5)
            scores.append(star_score)
            if stars > 10000:
                strengths.append(f"Strong GitHub presence ({stars:,} stars)")

        # Has industry classification
        if enrichment and enrichment.get("sector"):
            scores.append(60.0)  # Bonus for being classifiable

        if not scores:
            return 50.0, strengths, improvements

        return sum(scores) / len(scores), strengths, improvements

    def _calculate_tech_score(
        self, github: Optional[Dict]
    ) -> tuple[float, List[str], List[str]]:
        """
        Calculate tech velocity score (0-100).

        Signals:
        - GitHub velocity score (from T34)
        - Contributor count
        - Repository activity
        """
        scores = []
        strengths = []
        improvements = []

        if not github:
            return 50.0, strengths, ["No GitHub data available"]

        # Velocity score (already 0-100 from T34)
        if github.get("velocity_score"):
            velocity = github["velocity_score"]
            scores.append(velocity)
            if velocity > 70:
                strengths.append("High developer velocity")
            elif velocity < 30:
                improvements.append("Low development activity")

        # Public repos count
        if github.get("public_repos"):
            repos = github["public_repos"]
            repo_score = self._normalize_score(repos, 0, 50)
            scores.append(repo_score)

        # Total contributors
        if github.get("total_contributors"):
            contrib = github["total_contributors"]
            contrib_score = self._normalize_score(contrib, 0, 100)
            scores.append(contrib_score)
            if contrib > 100:
                strengths.append("Large engineering team")

        if not scores:
            return 50.0, strengths, improvements

        return sum(scores) / len(scores), strengths, improvements

    def _calculate_confidence(
        self,
        enrichment: Optional[Dict],
        github: Optional[Dict],
        tranco_rank: Optional[int],
    ) -> tuple[float, List[str]]:
        """
        Calculate confidence score based on data availability.

        Returns confidence (0-1) and list of data sources used.
        """
        confidence = 0.0
        sources = []

        # Enrichment data (40% of confidence)
        if enrichment:
            sources.append("enrichment")
            confidence += 0.25
            if enrichment.get("latest_revenue"):
                confidence += 0.10
            if enrichment.get("employee_count"):
                confidence += 0.05

        # GitHub data (35% of confidence)
        if github:
            sources.append("github")
            confidence += 0.25
            if github.get("velocity_score"):
                confidence += 0.10

        # Web traffic data (25% of confidence)
        if tranco_rank:
            sources.append("tranco")
            confidence += 0.25

        return min(confidence, 1.0), sources

    def score_company(
        self, company_name: str, force_refresh: bool = False
    ) -> Dict[str, Any]:
        """
        Calculate comprehensive score for a company.

        Args:
            company_name: Company name to score
            force_refresh: Bypass cache and recalculate

        Returns:
            Scoring result with breakdown and explanation
        """
        # Check cache first (unless force refresh)
        if not force_refresh:
            cached = self._get_cached_score(company_name)
            if cached:
                return cached

        # Gather data from all sources
        enrichment = self._get_enrichment_data(company_name)
        github = self._get_github_data(company_name)
        tranco_rank = self._get_web_traffic_rank(company_name)

        # Calculate category scores
        growth_score, growth_strengths, growth_improvements = (
            self._calculate_growth_score(enrichment, github)
        )
        stability_score, stability_strengths, stability_improvements = (
            self._calculate_stability_score(enrichment)
        )
        market_score, market_strengths, market_improvements = (
            self._calculate_market_score(enrichment, github, tranco_rank)
        )
        tech_score, tech_strengths, tech_improvements = self._calculate_tech_score(
            github
        )

        # Calculate composite score
        composite = (
            growth_score * WEIGHTS["growth"]
            + stability_score * WEIGHTS["stability"]
            + market_score * WEIGHTS["market_position"]
            + tech_score * WEIGHTS["tech_velocity"]
        )

        # Calculate confidence
        confidence, sources = self._calculate_confidence(
            enrichment, github, tranco_rank
        )

        # Compile explanation
        all_strengths = (
            growth_strengths + stability_strengths + market_strengths + tech_strengths
        )
        all_improvements = (
            growth_improvements
            + stability_improvements
            + market_improvements
            + tech_improvements
        )

        result = {
            "company_name": company_name,
            "composite_score": round(composite, 1),
            "tier": self._get_tier(composite),
            "category_scores": {
                "growth": round(growth_score, 1),
                "stability": round(stability_score, 1),
                "market_position": round(market_score, 1),
                "tech_velocity": round(tech_score, 1),
            },
            "confidence": round(confidence, 2),
            "explanation": {
                "top_strengths": all_strengths[:5],  # Top 5
                "areas_for_improvement": all_improvements[:5],
                "data_sources_used": sources,
            },
            "scored_at": datetime.utcnow().isoformat() + "Z",
            "model_version": MODEL_VERSION,
        }

        # Cache the result
        self._save_score(result)

        return result

    def _get_cached_score(self, company_name: str) -> Optional[Dict]:
        """Get cached score if fresh (< 7 days old)."""
        query = text("""
            SELECT * FROM company_scores
            WHERE LOWER(company_name) = LOWER(:name)
              AND model_version = :version
              AND scored_at > NOW() - INTERVAL '7 days'
            ORDER BY scored_at DESC
            LIMIT 1
        """)

        try:
            result = self.db.execute(
                query, {"name": company_name, "version": MODEL_VERSION}
            )
            row = result.mappings().fetchone()

            if row:
                return {
                    "company_name": row["company_name"],
                    "composite_score": row["composite_score"],
                    "tier": row["tier"],
                    "category_scores": {
                        "growth": row["growth_score"],
                        "stability": row["stability_score"],
                        "market_position": row["market_score"],
                        "tech_velocity": row["tech_score"],
                    },
                    "confidence": row["confidence"],
                    "explanation": row["explanation"],
                    "scored_at": row["scored_at"].isoformat() + "Z"
                    if row["scored_at"]
                    else None,
                    "model_version": row["model_version"],
                    "cached": True,
                }
        except Exception as e:
            logger.warning(f"Error fetching cached score: {e}")

        return None

    def _save_score(self, result: Dict) -> None:
        """Save score to cache table."""
        import json

        query = text("""
            INSERT INTO company_scores (
                company_name, composite_score, growth_score, stability_score,
                market_score, tech_score, confidence, tier, explanation,
                data_sources, scored_at, model_version
            ) VALUES (
                :name, :composite, :growth, :stability, :market, :tech,
                :confidence, :tier, CAST(:explanation AS jsonb),
                CAST(:sources AS jsonb), NOW(), :version
            )
            ON CONFLICT (company_name, model_version) DO UPDATE SET
                composite_score = EXCLUDED.composite_score,
                growth_score = EXCLUDED.growth_score,
                stability_score = EXCLUDED.stability_score,
                market_score = EXCLUDED.market_score,
                tech_score = EXCLUDED.tech_score,
                confidence = EXCLUDED.confidence,
                tier = EXCLUDED.tier,
                explanation = EXCLUDED.explanation,
                data_sources = EXCLUDED.data_sources,
                scored_at = NOW()
        """)

        try:
            self.db.execute(
                query,
                {
                    "name": result["company_name"],
                    "composite": result["composite_score"],
                    "growth": result["category_scores"]["growth"],
                    "stability": result["category_scores"]["stability"],
                    "market": result["category_scores"]["market_position"],
                    "tech": result["category_scores"]["tech_velocity"],
                    "confidence": result["confidence"],
                    "tier": result["tier"],
                    "explanation": json.dumps(result["explanation"]),
                    "sources": json.dumps(result["explanation"]["data_sources_used"]),
                    "version": MODEL_VERSION,
                },
            )
            self.db.commit()
        except Exception as e:
            logger.warning(f"Error saving score: {e}")
            self.db.rollback()

    def score_portfolio(
        self,
        investor_id: int,
        min_score: Optional[float] = None,
        tier: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Score all companies in an investor's portfolio.

        Args:
            investor_id: Investor ID
            min_score: Filter by minimum score
            tier: Filter by tier (A, B, C, D, F)

        Returns:
            Portfolio scoring summary
        """
        # Get investor info and portfolio companies
        investor_query = text("""
            SELECT id, name FROM lp_funds WHERE id = :id
            UNION
            SELECT id, name FROM family_offices WHERE id = :id
            LIMIT 1
        """)

        portfolio_query = text("""
            SELECT DISTINCT company_name
            FROM portfolio_companies
            WHERE investor_id = :id
        """)

        try:
            investor_result = self.db.execute(investor_query, {"id": investor_id})
            investor = investor_result.mappings().fetchone()

            if not investor:
                return {"error": f"Investor {investor_id} not found"}

            portfolio_result = self.db.execute(portfolio_query, {"id": investor_id})
            companies = [row["company_name"] for row in portfolio_result.mappings()]

        except Exception as e:
            logger.error(f"Error fetching portfolio: {e}")
            return {"error": str(e)}

        # Score each company
        scored_companies = []
        tier_distribution = {"A": 0, "B": 0, "C": 0, "D": 0, "F": 0}
        total_score = 0.0

        for company_name in companies:
            score_result = self.score_company(company_name)
            company_tier = score_result["tier"]
            company_score = score_result["composite_score"]

            tier_distribution[company_tier] += 1
            total_score += company_score

            # Apply filters
            if min_score and company_score < min_score:
                continue
            if tier and company_tier != tier:
                continue

            scored_companies.append(
                {
                    "company_name": company_name,
                    "score": company_score,
                    "tier": company_tier,
                }
            )

        # Sort by score descending
        scored_companies.sort(key=lambda x: x["score"], reverse=True)

        avg_score = total_score / len(companies) if companies else 0.0

        return {
            "investor_id": investor_id,
            "investor_name": investor["name"],
            "portfolio_summary": {
                "total_companies": len(companies),
                "scored_companies": len(scored_companies),
                "average_score": round(avg_score, 1),
                "tier_distribution": tier_distribution,
            },
            "companies": scored_companies,
        }

    def get_rankings(
        self,
        order: str = "top",
        limit: int = 20,
        sector: Optional[str] = None,
        min_confidence: float = 0.0,
    ) -> Dict[str, Any]:
        """
        Get top or bottom scored companies.

        Args:
            order: "top" or "bottom"
            limit: Number of results
            sector: Filter by sector
            min_confidence: Minimum confidence threshold

        Returns:
            Rankings list
        """
        order_clause = "DESC" if order == "top" else "ASC"

        # Join with enrichment for sector filter
        query = text(f"""
            SELECT cs.company_name, cs.composite_score, cs.tier,
                   cs.confidence, ce.sector
            FROM company_scores cs
            LEFT JOIN company_enrichment ce ON LOWER(cs.company_name) = LOWER(ce.company_name)
            WHERE cs.model_version = :version
              AND cs.confidence >= :min_confidence
              {'AND LOWER(ce.sector) = LOWER(:sector)' if sector else ''}
            ORDER BY cs.composite_score {order_clause}
            LIMIT :limit
        """)

        params = {
            "version": MODEL_VERSION,
            "min_confidence": min_confidence,
            "limit": limit,
        }
        if sector:
            params["sector"] = sector

        try:
            result = self.db.execute(query, params)
            rows = result.mappings().fetchall()

            rankings = []
            for i, row in enumerate(rows, 1):
                rankings.append(
                    {
                        "rank": i,
                        "company_name": row["company_name"],
                        "score": row["composite_score"],
                        "tier": row["tier"],
                        "sector": row["sector"],
                        "confidence": row["confidence"],
                    }
                )

            return {
                "order": order,
                "limit": limit,
                "filters": {
                    "sector": sector,
                    "min_confidence": min_confidence,
                },
                "rankings": rankings,
            }

        except Exception as e:
            logger.error(f"Error fetching rankings: {e}")
            return {"error": str(e), "rankings": []}

    @staticmethod
    def get_methodology() -> Dict[str, Any]:
        """Return scoring methodology documentation."""
        return {
            "model_version": MODEL_VERSION,
            "last_updated": "2026-01-19",
            "description": "Company health scoring model aggregating signals from multiple data sources",
            "categories": [
                {
                    "name": "growth",
                    "weight": WEIGHTS["growth"],
                    "description": "Measures company growth trajectory",
                    "signals": [
                        "employee_growth_yoy - Year-over-year employee growth rate",
                        "funding_recency - Months since last funding round",
                        "contributor_growth - GitHub contributor trend",
                    ],
                },
                {
                    "name": "stability",
                    "weight": WEIGHTS["stability"],
                    "description": "Measures business fundamentals and sustainability",
                    "signals": [
                        "revenue - Revenue scale (logarithmic)",
                        "profitability - Net income positive/negative",
                        "company_status - Active, IPO, acquired, or defunct",
                    ],
                },
                {
                    "name": "market_position",
                    "weight": WEIGHTS["market_position"],
                    "description": "Measures market presence and mindshare",
                    "signals": [
                        "tranco_rank - Web traffic ranking (lower = better)",
                        "github_stars - Open source community engagement",
                        "industry_presence - Industry classification confidence",
                    ],
                },
                {
                    "name": "tech_velocity",
                    "weight": WEIGHTS["tech_velocity"],
                    "description": "Measures engineering activity and velocity",
                    "signals": [
                        "github_velocity - Developer activity score (0-100)",
                        "contributor_count - Number of active contributors",
                        "repo_count - Number of public repositories",
                    ],
                },
            ],
            "tier_definitions": {
                "A": {
                    "range": "80-100",
                    "interpretation": "Strong performance across all metrics",
                },
                "B": {
                    "range": "60-79",
                    "interpretation": "Solid fundamentals, some areas for improvement",
                },
                "C": {
                    "range": "40-59",
                    "interpretation": "Average performance, mixed signals",
                },
                "D": {
                    "range": "20-39",
                    "interpretation": "Weak performance, concerns present",
                },
                "F": {"range": "0-19", "interpretation": "Critical issues, high risk"},
            },
            "confidence_calculation": {
                "description": "Confidence reflects data completeness",
                "components": {
                    "enrichment_data": "Up to 40% (revenue, employees, funding)",
                    "github_data": "Up to 35% (velocity score, contributors)",
                    "web_traffic": "Up to 25% (Tranco ranking)",
                },
            },
            "caching": {
                "ttl_days": 7,
                "refresh_triggers": [
                    "Manual refresh request",
                    "Underlying data update",
                ],
            },
        }
