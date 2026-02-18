"""
Agentic Competitive Intelligence (T44)

AI-powered agent that identifies competitors, builds comparison matrices,
tracks competitive movements, and generates moat assessments.
"""

import logging
import math
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================


class MarketPosition(str, Enum):
    LEADER = "leader"
    CHALLENGER = "challenger"
    FOLLOWER = "follower"
    NICHE = "niche"


class MovementType(str, Enum):
    FUNDING = "funding"
    HIRING = "hiring"
    PRODUCT = "product"
    PARTNERSHIP = "partnership"
    ACQUISITION = "acquisition"
    LEADERSHIP = "leadership"


# Similarity signal weights
SIMILARITY_WEIGHTS = {
    "sector": 0.30,
    "employee_size": 0.15,
    "funding_stage": 0.20,
    "shared_investors": 0.20,
    "tech_stack": 0.15,
}

# Moat category weights
MOAT_WEIGHTS = {
    "network_effects": 0.25,
    "switching_costs": 0.25,
    "brand": 0.20,
    "cost_advantages": 0.15,
    "technology": 0.15,
}

# Cache TTL
ANALYSIS_CACHE_HOURS = 24


# =============================================================================
# COMPETITIVE INTEL AGENT
# =============================================================================


class CompetitiveIntelAgent:
    """
    Competitive intelligence analysis agent.

    Identifies competitors, builds comparison matrices, tracks movements,
    and assesses competitive moats.
    """

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Create tables if they don't exist."""
        create_analyses = text("""
            CREATE TABLE IF NOT EXISTS competitive_analyses (
                id SERIAL PRIMARY KEY,
                company_name VARCHAR(255) NOT NULL,
                company_sector VARCHAR(100),
                competitors JSONB,
                comparison_matrix JSONB,
                moat_assessment JSONB,
                market_position VARCHAR(20),
                confidence FLOAT,
                data_sources JSONB,
                analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP,
                UNIQUE(company_name)
            )
        """)

        create_movements = text("""
            CREATE TABLE IF NOT EXISTS competitive_movements (
                id SERIAL PRIMARY KEY,
                company_name VARCHAR(255) NOT NULL,
                movement_type VARCHAR(50) NOT NULL,
                description TEXT,
                impact_score FLOAT,
                source VARCHAR(100),
                detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        create_indexes = text("""
            CREATE INDEX IF NOT EXISTS idx_comp_movements_company
                ON competitive_movements(company_name);
            CREATE INDEX IF NOT EXISTS idx_comp_movements_type
                ON competitive_movements(movement_type);
            CREATE INDEX IF NOT EXISTS idx_comp_movements_detected
                ON competitive_movements(detected_at DESC);
        """)

        try:
            self.db.execute(create_analyses)
            self.db.execute(create_movements)
            self.db.execute(create_indexes)
            self.db.commit()
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    # -------------------------------------------------------------------------
    # COMPANY DATA RETRIEVAL
    # -------------------------------------------------------------------------

    def _get_company_profile(self, company_name: str) -> Optional[Dict]:
        """Get company profile from enrichment data."""
        try:
            query = text("""
                SELECT * FROM company_enrichment
                WHERE LOWER(company_name) = LOWER(:name)
                LIMIT 1
            """)
            result = self.db.execute(query, {"name": company_name})
            row = result.mappings().fetchone()

            if row:
                data = dict(row)
                # Normalize field names
                data["name"] = data.get("company_name") or company_name
                data["employees"] = data.get("employee_count")
                return data

            # Try research cache
            query = text("""
                SELECT profile FROM research_cache
                WHERE LOWER(company_name) = LOWER(:name)
            """)
            result = self.db.execute(query, {"name": company_name})
            row = result.fetchone()
            if row and row[0]:
                return row[0]

            return None
        except Exception as e:
            logger.warning(f"Error getting company profile: {e}")
            self.db.rollback()
            return None

    def _get_company_score(self, company_name: str) -> Optional[Dict]:
        """Get company health score."""
        try:
            query = text("""
                SELECT composite_score, growth_score, stability_score,
                       market_score, tech_score, tier, confidence
                FROM company_scores
                WHERE LOWER(company_name) = LOWER(:name)
                ORDER BY scored_at DESC
                LIMIT 1
            """)
            result = self.db.execute(query, {"name": company_name})
            row = result.mappings().fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.debug(f"Error getting company score: {e}")
            self.db.rollback()
            return None

    def _get_web_traffic(self, company_name: str) -> Optional[Dict]:
        """Get web traffic data."""
        try:
            # Try to find domain from enrichment
            profile = self._get_company_profile(company_name)
            domain = None
            if profile:
                domain = profile.get("domain") or profile.get("website")
                if domain:
                    domain = (
                        domain.replace("https://", "")
                        .replace("http://", "")
                        .split("/")[0]
                    )

            if not domain:
                return None

            query = text("""
                SELECT domain, tranco_rank, category
                FROM web_traffic_rankings
                WHERE domain = :domain OR domain LIKE :domain_pattern
                ORDER BY fetched_at DESC
                LIMIT 1
            """)
            result = self.db.execute(
                query, {"domain": domain, "domain_pattern": f"%{domain}%"}
            )
            row = result.mappings().fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.debug(f"Error getting web traffic: {e}")
            self.db.rollback()
            return None

    def _get_github_metrics(self, company_name: str) -> Optional[Dict]:
        """Get GitHub organization metrics."""
        try:
            # Common org name patterns
            org_candidates = [
                company_name.lower().replace(" ", ""),
                company_name.lower().replace(" ", "-"),
                company_name.lower(),
            ]

            for org in org_candidates:
                query = text("""
                    SELECT org_name, public_repos, total_stars, total_forks,
                           total_contributors, primary_languages
                    FROM github_org_metrics
                    WHERE LOWER(org_name) = :org
                    ORDER BY fetched_at DESC
                    LIMIT 1
                """)
                result = self.db.execute(query, {"org": org})
                row = result.mappings().fetchone()
                if row:
                    return dict(row)

            return None
        except Exception as e:
            logger.debug(f"Error getting github metrics: {e}")
            self.db.rollback()
            return None

    def _get_glassdoor_data(self, company_name: str) -> Optional[Dict]:
        """Get Glassdoor ratings."""
        try:
            query = text("""
                SELECT company_name, overall_rating, ceo_approval,
                       recommend_to_friend, review_count
                FROM glassdoor_ratings
                WHERE LOWER(company_name) LIKE LOWER(:pattern)
                ORDER BY fetched_at DESC
                LIMIT 1
            """)
            result = self.db.execute(query, {"pattern": f"%{company_name}%"})
            row = result.mappings().fetchone()
            return dict(row) if row else None
        except Exception as e:
            logger.debug(f"Error getting glassdoor data: {e}")
            self.db.rollback()
            return None

    # -------------------------------------------------------------------------
    # COMPETITOR DISCOVERY
    # -------------------------------------------------------------------------

    def find_competitors(self, company_name: str, max_results: int = 10) -> List[Dict]:
        """
        Find competitors using multiple signals.

        Signals considered:
        - Same sector/industry
        - Similar employee count
        - Similar funding stage
        - Overlapping investors
        - Similar tech stack
        """
        # Get target company profile
        target = self._get_company_profile(company_name)
        if not target:
            logger.warning(f"No profile found for {company_name}")
            return []

        target_sector = target.get("sector") or target.get("industry")
        target_employees = target.get("employees") or target.get("employee_count")
        target_funding = target.get("total_funding") or target.get("funding_total")

        # Query candidates from same sector
        candidates = self._query_sector_companies(target_sector, limit=100)

        # Score each candidate
        scored = []
        for candidate in candidates:
            cand_name = candidate.get("name") or candidate.get("company_name")
            if not cand_name or cand_name.lower() == company_name.lower():
                continue

            similarity, signals = self._score_similarity(target, candidate)

            if similarity >= 0.25:  # Minimum threshold
                relationship = self._determine_relationship(target, candidate)
                strengths, weaknesses = self._analyze_strengths_weaknesses(
                    target, candidate
                )

                scored.append(
                    {
                        "name": cand_name,
                        "similarity_score": round(similarity, 2),
                        "relationship": relationship,
                        "signals": signals,
                        "strengths": strengths,
                        "weaknesses": weaknesses,
                    }
                )

        # Sort by similarity and return top N
        scored.sort(key=lambda x: x["similarity_score"], reverse=True)
        return scored[:max_results]

    def _query_sector_companies(
        self, sector: Optional[str], limit: int = 100
    ) -> List[Dict]:
        """Query companies in the same sector."""
        if not sector:
            return []

        try:
            query = text("""
                SELECT company_name as name, sector, industry,
                       employee_count as employees, total_funding
                FROM company_enrichment
                WHERE LOWER(sector) = LOWER(:sector)
                   OR LOWER(industry) = LOWER(:sector)
                LIMIT :limit
            """)
            result = self.db.execute(query, {"sector": sector, "limit": limit})
            return [dict(row) for row in result.mappings()]
        except Exception as e:
            logger.debug(f"Error querying sector companies: {e}")
            self.db.rollback()
            return []

    def _score_similarity(self, target: Dict, candidate: Dict) -> Tuple[float, Dict]:
        """
        Score similarity between target and candidate.

        Returns (score, signals_dict).
        """
        signals = {}
        total_score = 0.0

        # Sector match
        target_sector = (target.get("sector") or "").lower()
        cand_sector = (candidate.get("sector") or "").lower()
        if target_sector and cand_sector:
            if target_sector == cand_sector:
                signals["sector"] = {"match": True, "score": 1.0}
                total_score += SIMILARITY_WEIGHTS["sector"]
            elif target_sector in cand_sector or cand_sector in target_sector:
                signals["sector"] = {"match": "partial", "score": 0.5}
                total_score += SIMILARITY_WEIGHTS["sector"] * 0.5

        # Employee size similarity
        target_emp = target.get("employees") or target.get("employee_count") or 0
        cand_emp = candidate.get("employees") or candidate.get("employee_count") or 0
        if target_emp > 0 and cand_emp > 0:
            ratio = min(target_emp, cand_emp) / max(target_emp, cand_emp)
            if ratio >= 0.5:  # Within 50%
                signals["employee_size"] = {
                    "target": target_emp,
                    "candidate": cand_emp,
                    "ratio": round(ratio, 2),
                }
                total_score += SIMILARITY_WEIGHTS["employee_size"] * ratio

        # Funding stage similarity
        target_funding = target.get("total_funding") or 0
        cand_funding = candidate.get("total_funding") or 0
        if target_funding > 0 and cand_funding > 0:
            # Use log scale for funding comparison
            log_target = math.log10(target_funding + 1)
            log_cand = math.log10(cand_funding + 1)
            ratio = min(log_target, log_cand) / max(log_target, log_cand)
            if ratio >= 0.7:
                signals["funding_stage"] = {
                    "target": target_funding,
                    "candidate": cand_funding,
                    "score": round(ratio, 2),
                }
                total_score += SIMILARITY_WEIGHTS["funding_stage"] * ratio

        # Shared investors (check portfolio overlap)
        shared = self._find_shared_investors(target.get("name"), candidate.get("name"))
        if shared:
            investor_score = min(len(shared) / 3, 1.0)  # Cap at 3 shared
            signals["shared_investors"] = {
                "count": len(shared),
                "investors": shared[:5],
            }
            total_score += SIMILARITY_WEIGHTS["shared_investors"] * investor_score

        # Tech stack similarity (via GitHub)
        target_github = self._get_github_metrics(target.get("name", ""))
        cand_github = self._get_github_metrics(candidate.get("name", ""))
        if target_github and cand_github:
            target_langs = set(target_github.get("primary_languages") or [])
            cand_langs = set(cand_github.get("primary_languages") or [])
            if target_langs and cand_langs:
                overlap = len(target_langs & cand_langs)
                union = len(target_langs | cand_langs)
                if union > 0:
                    jaccard = overlap / union
                    if jaccard > 0.2:
                        signals["tech_stack"] = {
                            "shared_languages": list(target_langs & cand_langs),
                            "jaccard": round(jaccard, 2),
                        }
                        total_score += SIMILARITY_WEIGHTS["tech_stack"] * jaccard

        return total_score, signals

    def _find_shared_investors(
        self, company1: Optional[str], company2: Optional[str]
    ) -> List[str]:
        """Find investors that have invested in both companies."""
        if not company1 or not company2:
            return []

        query = text("""
            SELECT DISTINCT i1.investor_name
            FROM portfolio_holdings h1
            JOIN investors i1 ON h1.investor_id = i1.id
            JOIN portfolio_holdings h2 ON h1.investor_id = h2.investor_id
            WHERE LOWER(h1.company_name) LIKE LOWER(:c1)
              AND LOWER(h2.company_name) LIKE LOWER(:c2)
            LIMIT 10
        """)
        try:
            result = self.db.execute(
                query, {"c1": f"%{company1}%", "c2": f"%{company2}%"}
            )
            return [row[0] for row in result if row[0]]
        except Exception:
            return []

    def _determine_relationship(self, target: Dict, competitor: Dict) -> str:
        """Determine competitive relationship type."""
        target_emp = target.get("employees") or 0
        comp_emp = competitor.get("employees") or 0
        target_funding = target.get("total_funding") or 0
        comp_funding = competitor.get("total_funding") or 0

        # Similar size = direct competitor
        if target_emp > 0 and comp_emp > 0:
            ratio = min(target_emp, comp_emp) / max(target_emp, comp_emp)
            if ratio > 0.5:
                return "direct"

        # Much larger = incumbent
        if comp_emp > target_emp * 3 or comp_funding > target_funding * 5:
            return "incumbent"

        # Much smaller = emerging
        if target_emp > comp_emp * 3 or target_funding > comp_funding * 5:
            return "emerging"

        return "indirect"

    def _analyze_strengths_weaknesses(
        self, target: Dict, competitor: Dict
    ) -> Tuple[List[str], List[str]]:
        """Analyze competitor's strengths and weaknesses relative to target."""
        strengths = []
        weaknesses = []

        # Employee comparison
        target_emp = target.get("employees") or 0
        comp_emp = competitor.get("employees") or 0
        if comp_emp > target_emp * 1.5:
            strengths.append("larger team")
        elif target_emp > comp_emp * 1.5:
            weaknesses.append("smaller team")

        # Funding comparison
        target_funding = target.get("total_funding") or 0
        comp_funding = competitor.get("total_funding") or 0
        if comp_funding > target_funding * 1.5:
            strengths.append("better funded")
        elif target_funding > comp_funding * 1.5:
            weaknesses.append("less funding")

        # Score comparison
        target_score = self._get_company_score(target.get("name", ""))
        comp_score = self._get_company_score(competitor.get("name", ""))
        if target_score and comp_score:
            if (comp_score.get("tech_score") or 0) > (
                target_score.get("tech_score") or 0
            ) + 10:
                strengths.append("stronger tech")
            elif (target_score.get("tech_score") or 0) > (
                comp_score.get("tech_score") or 0
            ) + 10:
                weaknesses.append("weaker tech")

            if (comp_score.get("growth_score") or 0) > (
                target_score.get("growth_score") or 0
            ) + 10:
                strengths.append("faster growth")
            elif (target_score.get("growth_score") or 0) > (
                comp_score.get("growth_score") or 0
            ) + 10:
                weaknesses.append("slower growth")

        # Traffic comparison
        target_traffic = self._get_web_traffic(target.get("name", ""))
        comp_traffic = self._get_web_traffic(competitor.get("name", ""))
        if target_traffic and comp_traffic:
            target_rank = target_traffic.get("tranco_rank") or 999999
            comp_rank = comp_traffic.get("tranco_rank") or 999999
            if comp_rank < target_rank * 0.5:
                strengths.append("higher traffic")
            elif target_rank < comp_rank * 0.5:
                weaknesses.append("lower traffic")

        return strengths[:5], weaknesses[:5]

    # -------------------------------------------------------------------------
    # COMPARISON MATRIX
    # -------------------------------------------------------------------------

    def build_comparison_matrix(self, companies: List[str]) -> Dict:
        """
        Build standardized comparison matrix for companies.

        Returns metrics table with normalized values.
        """
        metrics = [
            "health_score",
            "growth_score",
            "stability_score",
            "employees",
            "funding",
            "traffic_rank",
            "github_stars",
            "glassdoor_rating",
        ]

        data = {}
        for company in companies:
            data[company] = self._get_company_metrics(company)

        return {
            "metrics": metrics,
            "data": data,
            "rankings": self._compute_rankings(data, metrics),
        }

    def _get_company_metrics(self, company_name: str) -> Dict:
        """Get all metrics for a company."""
        metrics = {}

        # Scores
        score = self._get_company_score(company_name)
        if score:
            metrics["health_score"] = score.get("composite_score")
            metrics["growth_score"] = score.get("growth_score")
            metrics["stability_score"] = score.get("stability_score")

        # Profile data
        profile = self._get_company_profile(company_name)
        if profile:
            metrics["employees"] = profile.get("employees") or profile.get(
                "employee_count"
            )
            metrics["funding"] = profile.get("total_funding")

        # Traffic
        traffic = self._get_web_traffic(company_name)
        if traffic:
            metrics["traffic_rank"] = traffic.get("tranco_rank")

        # GitHub
        github = self._get_github_metrics(company_name)
        if github:
            metrics["github_stars"] = github.get("total_stars")

        # Glassdoor
        glassdoor = self._get_glassdoor_data(company_name)
        if glassdoor:
            metrics["glassdoor_rating"] = glassdoor.get("overall_rating")

        return metrics

    def _compute_rankings(self, data: Dict, metrics: List[str]) -> Dict:
        """Compute rankings for each metric."""
        rankings = {}

        for metric in metrics:
            # Get values for this metric
            values = []
            for company, company_metrics in data.items():
                val = company_metrics.get(metric)
                if val is not None:
                    values.append((company, val))

            if not values:
                continue

            # Sort (higher is better for most, except traffic_rank)
            reverse = metric != "traffic_rank"
            values.sort(key=lambda x: x[1], reverse=reverse)
            rankings[metric] = [v[0] for v in values]

        # Overall ranking (by health_score if available)
        if "health_score" in rankings:
            rankings["overall"] = rankings["health_score"]

        return rankings

    # -------------------------------------------------------------------------
    # MOAT ASSESSMENT
    # -------------------------------------------------------------------------

    def assess_moat(self, company_name: str, competitors: List[str]) -> Dict:
        """
        Assess competitive moat for a company.

        Categories:
        - Network effects
        - Switching costs
        - Brand recognition
        - Cost advantages
        - Technology lead
        """
        profile = self._get_company_profile(company_name)
        score_data = self._get_company_score(company_name)
        traffic = self._get_web_traffic(company_name)
        github = self._get_github_metrics(company_name)

        # Compute category scores
        scores = {
            "network_effects": self._score_network_effects(profile, traffic),
            "switching_costs": self._score_switching_costs(profile),
            "brand": self._score_brand(profile, traffic),
            "cost_advantages": self._score_cost_advantages(profile, score_data),
            "technology": self._score_technology(github, score_data),
        }

        # Compute overall moat score
        overall = sum(scores[cat] * MOAT_WEIGHTS[cat] for cat in scores)

        # Determine moat strength
        if overall >= 75:
            moat_strength = "strong"
        elif overall >= 50:
            moat_strength = "moderate"
        elif overall >= 25:
            moat_strength = "weak"
        else:
            moat_strength = "none"

        # Generate summary
        summary = self._generate_moat_summary(company_name, scores, moat_strength)

        return {
            "overall_moat": moat_strength,
            "overall_score": round(overall, 1),
            "scores": {k: round(v, 1) for k, v in scores.items()},
            "summary": summary,
        }

    def _score_network_effects(
        self, profile: Optional[Dict], traffic: Optional[Dict]
    ) -> float:
        """Score network effects based on user base and integrations."""
        score = 50.0  # Base score

        if traffic:
            rank = traffic.get("tranco_rank") or 999999
            if rank < 1000:
                score += 40
            elif rank < 10000:
                score += 25
            elif rank < 100000:
                score += 10

        if profile:
            # Enterprise customers suggest B2B network effects
            employees = profile.get("employees") or 0
            if employees > 1000:
                score += 10

        return min(100, score)

    def _score_switching_costs(self, profile: Optional[Dict]) -> float:
        """Score switching costs based on business model signals."""
        score = 40.0  # Base score

        if profile:
            sector = (profile.get("sector") or "").lower()
            # High switching cost sectors
            if any(
                s in sector for s in ["enterprise", "saas", "fintech", "healthcare"]
            ):
                score += 30

            # Large customer base suggests integration depth
            employees = profile.get("employees") or 0
            if employees > 500:
                score += 15
            elif employees > 100:
                score += 10

        return min(100, score)

    def _score_brand(self, profile: Optional[Dict], traffic: Optional[Dict]) -> float:
        """Score brand recognition."""
        score = 30.0  # Base score

        if traffic:
            rank = traffic.get("tranco_rank") or 999999
            if rank < 5000:
                score += 50
            elif rank < 50000:
                score += 30
            elif rank < 500000:
                score += 15

        if profile:
            funding = profile.get("total_funding") or 0
            if funding > 100_000_000:
                score += 20
            elif funding > 10_000_000:
                score += 10

        return min(100, score)

    def _score_cost_advantages(
        self, profile: Optional[Dict], score_data: Optional[Dict]
    ) -> float:
        """Score cost advantages."""
        score = 40.0  # Base score

        if profile:
            funding = profile.get("total_funding") or 0
            employees = profile.get("employees") or 1
            # Revenue per employee proxy
            if funding > 0 and employees > 0:
                funding_per_emp = funding / employees
                if funding_per_emp > 500_000:
                    score += 25
                elif funding_per_emp > 100_000:
                    score += 15

        if score_data:
            stability = score_data.get("stability_score") or 50
            if stability > 70:
                score += 20
            elif stability > 50:
                score += 10

        return min(100, score)

    def _score_technology(
        self, github: Optional[Dict], score_data: Optional[Dict]
    ) -> float:
        """Score technology lead."""
        score = 40.0  # Base score

        if github:
            stars = github.get("total_stars") or 0
            if stars > 10000:
                score += 35
            elif stars > 1000:
                score += 25
            elif stars > 100:
                score += 15

            contributors = github.get("total_contributors") or 0
            if contributors > 100:
                score += 15

        if score_data:
            tech_score = score_data.get("tech_score") or 50
            if tech_score > 70:
                score += 15
            elif tech_score > 50:
                score += 5

        return min(100, score)

    def _generate_moat_summary(
        self, company_name: str, scores: Dict[str, float], strength: str
    ) -> str:
        """Generate text summary of moat assessment."""
        # Find top strengths
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top_strengths = [s for s, v in sorted_scores[:2] if v >= 60]
        weaknesses = [s for s, v in sorted_scores if v < 40]

        summary_parts = [f"{company_name} has a {strength} competitive moat."]

        if top_strengths:
            strength_names = {
                "network_effects": "network effects",
                "switching_costs": "high switching costs",
                "brand": "brand recognition",
                "cost_advantages": "cost efficiencies",
                "technology": "technology leadership",
            }
            strengths_text = " and ".join(
                strength_names.get(s, s) for s in top_strengths
            )
            summary_parts.append(f"Key strengths include {strengths_text}.")

        if weaknesses and strength != "strong":
            weakness_names = {
                "network_effects": "limited network effects",
                "switching_costs": "low switching costs",
                "brand": "brand awareness",
                "cost_advantages": "cost structure",
                "technology": "technology differentiation",
            }
            weak_text = ", ".join(weakness_names.get(w, w) for w in weaknesses[:2])
            summary_parts.append(f"Areas to improve: {weak_text}.")

        return " ".join(summary_parts)

    # -------------------------------------------------------------------------
    # MOVEMENT TRACKING
    # -------------------------------------------------------------------------

    def detect_movements(self, company_name: str, days: int = 30) -> List[Dict]:
        """Detect recent competitive movements for a company."""
        movements = []

        # Check news matches for movement keywords
        query = text("""
            SELECT news_title, news_url, event_type, impact_score, created_at
            FROM news_matches
            WHERE (LOWER(watch_value) LIKE LOWER(:pattern)
                   OR LOWER(news_title) LIKE LOWER(:pattern))
              AND created_at > NOW() - INTERVAL ':days days'
            ORDER BY impact_score DESC, created_at DESC
            LIMIT 20
        """)

        try:
            result = self.db.execute(
                query, {"pattern": f"%{company_name}%", "days": days}
            )
            for row in result.mappings():
                movement_type = self._classify_movement(
                    row.get("event_type"), row.get("news_title", "")
                )
                movements.append(
                    {
                        "company": company_name,
                        "type": movement_type,
                        "description": row["news_title"],
                        "impact_score": row.get("impact_score") or 0.5,
                        "detected_at": row["created_at"].isoformat()
                        if row.get("created_at")
                        else None,
                    }
                )
        except Exception as e:
            logger.warning(f"Error detecting movements: {e}")

        # Check for funding from Form D
        try:
            query = text("""
                SELECT issuer_name, total_amount_sold, date_of_first_sale
                FROM form_d_filings
                WHERE LOWER(issuer_name) LIKE LOWER(:pattern)
                  AND date_of_first_sale > NOW() - INTERVAL ':days days'
                ORDER BY date_of_first_sale DESC
                LIMIT 5
            """)
            result = self.db.execute(
                query, {"pattern": f"%{company_name}%", "days": days}
            )
            for row in result.mappings():
                amount = row.get("total_amount_sold") or 0
                movements.append(
                    {
                        "company": company_name,
                        "type": MovementType.FUNDING.value,
                        "description": f"SEC Form D filing: ${amount:,.0f} raised",
                        "impact_score": 0.8 if amount > 10_000_000 else 0.6,
                        "detected_at": row["date_of_first_sale"].isoformat()
                        if row.get("date_of_first_sale")
                        else None,
                    }
                )
        except Exception as e:
            logger.debug("Failed to fetch Form D movements for %s: %s", company_name, e)

        return movements

    def _classify_movement(self, event_type: Optional[str], title: str) -> str:
        """Classify movement type from event type and title."""
        title_lower = title.lower()

        if (
            event_type == "funding"
            or "funding" in title_lower
            or "raises" in title_lower
        ):
            return MovementType.FUNDING.value
        if (
            event_type == "acquisition"
            or "acquires" in title_lower
            or "merger" in title_lower
        ):
            return MovementType.ACQUISITION.value
        if (
            "hires" in title_lower
            or "appoints" in title_lower
            or "joins" in title_lower
        ):
            return MovementType.HIRING.value
        if (
            "launches" in title_lower
            or "announces" in title_lower
            or "product" in title_lower
        ):
            return MovementType.PRODUCT.value
        if "partnership" in title_lower or "partners" in title_lower:
            return MovementType.PARTNERSHIP.value
        if "ceo" in title_lower or "cfo" in title_lower or "executive" in title_lower:
            return MovementType.LEADERSHIP.value

        return "news"

    def track_competitor_movements(self, companies: List[str], days: int = 30) -> Dict:
        """Track movements for a list of competitors."""
        all_movements = []
        summary = {
            "funding_total": 0,
            "funding_count": 0,
            "hires_announced": 0,
            "products_launched": 0,
            "partnerships": 0,
        }

        for company in companies:
            movements = self.detect_movements(company, days)
            all_movements.extend(movements)

            for m in movements:
                if m["type"] == MovementType.FUNDING.value:
                    summary["funding_count"] += 1
                elif m["type"] == MovementType.HIRING.value:
                    summary["hires_announced"] += 1
                elif m["type"] == MovementType.PRODUCT.value:
                    summary["products_launched"] += 1
                elif m["type"] == MovementType.PARTNERSHIP.value:
                    summary["partnerships"] += 1

        # Sort by impact and date
        all_movements.sort(
            key=lambda x: (x.get("impact_score", 0), x.get("detected_at", "")),
            reverse=True,
        )

        return {
            "movements": all_movements[:50],
            "summary": summary,
        }

    # -------------------------------------------------------------------------
    # MAIN ANALYSIS
    # -------------------------------------------------------------------------

    def analyze(
        self,
        company_name: str,
        max_competitors: int = 10,
        include_movements: bool = True,
    ) -> Dict:
        """
        Run full competitive analysis for a company.

        Returns comprehensive competitive landscape.
        """
        # Get target profile
        profile = self._get_company_profile(company_name)
        sector = profile.get("sector") if profile else None

        # Find competitors
        competitors = self.find_competitors(company_name, max_competitors)
        competitor_names = [c["name"] for c in competitors]

        # Build comparison matrix
        all_companies = [company_name] + competitor_names
        comparison = self.build_comparison_matrix(all_companies)

        # Assess moat
        moat = self.assess_moat(company_name, competitor_names)

        # Determine market position
        position = self._determine_market_position(
            company_name, competitor_names, comparison
        )

        # Track movements if requested
        movements = None
        if include_movements and competitor_names:
            movements = self.track_competitor_movements(all_companies, days=30)

        # Calculate confidence
        data_sources = []
        if profile:
            data_sources.append("enrichment")
        if self._get_company_score(company_name):
            data_sources.append("scoring")
        if self._get_web_traffic(company_name):
            data_sources.append("web_traffic")
        if self._get_github_metrics(company_name):
            data_sources.append("github")

        confidence = len(data_sources) / 5  # Max 5 sources

        result = {
            "company": company_name,
            "sector": sector,
            "market_position": position,
            "competitors": competitors,
            "comparison_matrix": comparison,
            "moat_assessment": moat,
            "analyzed_at": datetime.utcnow().isoformat() + "Z",
            "confidence": round(confidence, 2),
            "data_sources": data_sources,
        }

        if movements:
            result["recent_movements"] = movements

        # Cache the analysis
        self._cache_analysis(company_name, result)

        return result

    def _determine_market_position(
        self, company_name: str, competitors: List[str], comparison: Dict
    ) -> str:
        """Determine market position relative to competitors."""
        rankings = comparison.get("rankings", {})
        overall = rankings.get("overall", [])

        if not overall:
            return MarketPosition.NICHE.value

        position = (
            overall.index(company_name) if company_name in overall else len(overall)
        )
        total = len(overall)

        if position == 0:
            return MarketPosition.LEADER.value
        elif position < total * 0.33:
            return MarketPosition.CHALLENGER.value
        elif position < total * 0.66:
            return MarketPosition.FOLLOWER.value
        else:
            return MarketPosition.NICHE.value

    def _cache_analysis(self, company_name: str, result: Dict) -> None:
        """Cache analysis result."""
        import json

        expires = datetime.utcnow() + timedelta(hours=ANALYSIS_CACHE_HOURS)

        query = text("""
            INSERT INTO competitive_analyses
            (company_name, company_sector, competitors, comparison_matrix,
             moat_assessment, market_position, confidence, data_sources, expires_at)
            VALUES
            (:name, :sector, :competitors, :matrix, :moat, :position,
             :confidence, :sources, :expires)
            ON CONFLICT (company_name) DO UPDATE SET
                company_sector = :sector,
                competitors = :competitors,
                comparison_matrix = :matrix,
                moat_assessment = :moat,
                market_position = :position,
                confidence = :confidence,
                data_sources = :sources,
                analyzed_at = CURRENT_TIMESTAMP,
                expires_at = :expires
        """)

        try:
            self.db.execute(
                query,
                {
                    "name": company_name,
                    "sector": result.get("sector"),
                    "competitors": json.dumps(result.get("competitors", [])),
                    "matrix": json.dumps(result.get("comparison_matrix", {})),
                    "moat": json.dumps(result.get("moat_assessment", {})),
                    "position": result.get("market_position"),
                    "confidence": result.get("confidence"),
                    "sources": json.dumps(result.get("data_sources", [])),
                    "expires": expires,
                },
            )
            self.db.commit()
        except Exception as e:
            logger.warning(f"Failed to cache analysis: {e}")
            self.db.rollback()

    def get_cached_analysis(self, company_name: str) -> Optional[Dict]:
        """Get cached analysis if not expired."""
        query = text("""
            SELECT * FROM competitive_analyses
            WHERE LOWER(company_name) = LOWER(:name)
              AND expires_at > NOW()
        """)

        result = self.db.execute(query, {"name": company_name})
        row = result.mappings().fetchone()

        if row:
            return {
                "company": row["company_name"],
                "sector": row["company_sector"],
                "market_position": row["market_position"],
                "competitors": row["competitors"] or [],
                "comparison_matrix": row["comparison_matrix"] or {},
                "moat_assessment": row["moat_assessment"] or {},
                "confidence": row["confidence"],
                "data_sources": row["data_sources"] or [],
                "analyzed_at": row["analyzed_at"].isoformat() + "Z"
                if row["analyzed_at"]
                else None,
                "cached": True,
            }

        return None

    def compare_companies(self, companies: List[str]) -> Dict:
        """
        Compare specific companies directly.

        Useful when you already know which companies to compare.
        """
        comparison = self.build_comparison_matrix(companies)

        return {
            "companies": companies,
            "comparison_matrix": comparison,
            "rankings": comparison.get("rankings", {}),
        }
