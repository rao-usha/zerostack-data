"""
Deal Radar — Convergence Intelligence Engine.

Queries 5 public data sources (EPA ECHO, IRS SOI migration, US Trade,
Public Water Systems, IRS SOI income) and computes per-region signal
scores (0-100). Produces a composite convergence score that identifies
geographic investment opportunities.

Follows the FundConvictionScorer pattern: weighted multi-signal → 0-100
composite → grade → cluster classification.
"""

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import text, func
from sqlalchemy.orm import Session

from app.core.convergence_models import (
    ConvergenceCluster,
    ConvergenceRegion,
    ConvergenceSignal,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Region Definitions
# =============================================================================

REGION_DEFINITIONS = {
    "pacnw": {
        "label": "Pacific NW",
        "states": ["WA", "OR"],
        "center_lat": 46.0,
        "center_lon": -122.5,
        "map_x": 72,
        "map_y": 58,
    },
    "cal": {
        "label": "California",
        "states": ["CA"],
        "center_lat": 36.8,
        "center_lon": -119.4,
        "map_x": 62,
        "map_y": 138,
    },
    "mtnw": {
        "label": "Mountain West",
        "states": ["MT", "WY", "CO", "UT", "ID", "NV"],
        "center_lat": 42.5,
        "center_lon": -111.0,
        "map_x": 162,
        "map_y": 86,
    },
    "sw": {
        "label": "Southwest",
        "states": ["AZ", "NM"],
        "center_lat": 34.0,
        "center_lon": -110.5,
        "map_x": 155,
        "map_y": 170,
    },
    "plains": {
        "label": "Great Plains",
        "states": ["ND", "SD", "NE", "KS", "OK"],
        "center_lat": 40.5,
        "center_lon": -99.0,
        "map_x": 258,
        "map_y": 96,
    },
    "texas": {
        "label": "Texas",
        "states": ["TX"],
        "center_lat": 31.0,
        "center_lon": -99.0,
        "map_x": 245,
        "map_y": 210,
    },
    "mw": {
        "label": "Midwest",
        "states": ["MN", "IA", "MO", "WI", "IL", "IN"],
        "center_lat": 41.5,
        "center_lon": -89.0,
        "map_x": 325,
        "map_y": 94,
    },
    "appalachia": {
        "label": "Appalachia",
        "states": ["WV", "KY", "VA"],
        "center_lat": 38.0,
        "center_lon": -80.5,
        "map_x": 394,
        "map_y": 154,
    },
    "southeast": {
        "label": "Southeast",
        "states": ["AL", "GA", "SC", "NC", "TN", "MS", "AR", "LA"],
        "center_lat": 33.5,
        "center_lon": -86.0,
        "map_x": 376,
        "map_y": 196,
    },
    "grtlakes": {
        "label": "Great Lakes",
        "states": ["MI", "OH", "PA"],
        "center_lat": 41.0,
        "center_lon": -82.0,
        "map_x": 370,
        "map_y": 65,
    },
    "midatl": {
        "label": "Mid-Atlantic",
        "states": ["NY", "NJ", "DE", "MD", "DC"],
        "center_lat": 40.0,
        "center_lon": -75.0,
        "map_x": 448,
        "map_y": 116,
    },
    "ne": {
        "label": "Northeast",
        "states": ["CT", "MA", "RI", "VT", "NH", "ME"],
        "center_lat": 43.0,
        "center_lon": -71.5,
        "map_x": 468,
        "map_y": 60,
    },
    "florida": {
        "label": "Florida",
        "states": ["FL"],
        "center_lat": 27.8,
        "center_lon": -81.5,
        "map_x": 415,
        "map_y": 245,
    },
}

# Map connections for the frontend network visualization
REGION_CONNECTIONS = [
    ["pacnw", "cal"], ["cal", "sw"], ["mtnw", "sw"], ["mtnw", "plains"],
    ["sw", "texas"], ["plains", "mw"], ["plains", "texas"], ["mw", "grtlakes"],
    ["mw", "appalachia"], ["texas", "southeast"], ["southeast", "florida"],
    ["appalachia", "southeast"], ["appalachia", "midatl"], ["grtlakes", "ne"],
    ["midatl", "ne"], ["grtlakes", "midatl"], ["pacnw", "mtnw"], ["cal", "mtnw"],
]

# Signal type definitions
SIGNAL_TYPES = {
    "epa": {"color": "#e24b4a", "label": "EPA / environmental"},
    "irs": {"color": "#7f77dd", "label": "IRS migration"},
    "trade": {"color": "#1d9e75", "label": "Trade & commerce"},
    "water": {"color": "#ba7517", "label": "Water systems"},
    "macro": {"color": "#378add", "label": "Macro / income"},
}


# =============================================================================
# Data classes
# =============================================================================


@dataclass
class RegionScores:
    epa: float = 0.0
    irs: float = 0.0
    trade: float = 0.0
    water: float = 0.0
    macro: float = 0.0


@dataclass
class RegionResult:
    region_id: str
    label: str
    scores: RegionScores
    convergence_score: float
    convergence_grade: str
    cluster_status: str
    active_signals: List[str]
    signal_count: int


@dataclass
class ThesisResult:
    region_id: str
    thesis_text: str
    opportunity_score: float
    urgency_score: float
    risk_score: float


# =============================================================================
# Scoring utilities (pure functions, no DB dependency)
# =============================================================================


def compute_convergence(scores: RegionScores) -> float:
    """Compute convergence score from individual signal scores.

    Formula matches the mockup: avg * (1 + above_60_count * 0.1)
    """
    vals = [scores.epa, scores.irs, scores.trade, scores.water, scores.macro]
    above_60 = sum(1 for v in vals if v >= 60)
    avg = sum(vals) / 5
    return round(avg * (1 + above_60 * 0.1))


def classify_cluster(score: float) -> str:
    """Classify cluster status from convergence score."""
    if score >= 72:
        return "HOT"
    if score >= 58:
        return "ACTIVE"
    if score >= 44:
        return "WATCH"
    return "LOW"


def grade_score(score: float) -> str:
    """Assign letter grade from convergence score."""
    if score >= 72:
        return "A"
    if score >= 58:
        return "B"
    if score >= 44:
        return "C"
    if score >= 30:
        return "D"
    return "F"


def get_active_signals(scores: RegionScores) -> List[str]:
    """Return list of signal names with score >= 60."""
    result = []
    if scores.epa >= 60:
        result.append("EPA")
    if scores.irs >= 60:
        result.append("IRS")
    if scores.trade >= 60:
        result.append("Trade")
    if scores.water >= 60:
        result.append("Water")
    if scores.macro >= 60:
        result.append("Macro")
    return result


def _clamp(val: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, val))


def _normalize(value: float, low: float, high: float) -> float:
    """Normalize a value to 0-100 range based on expected low/high bounds."""
    if high <= low:
        return 0.0
    return _clamp((value - low) / (high - low) * 100)


# =============================================================================
# Convergence Engine
# =============================================================================


class ConvergenceEngine:
    """
    Core scoring engine — queries 5 data sources by region,
    computes per-signal scores and composite convergence.
    """

    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def scan_all_regions(self) -> List[RegionResult]:
        """Run full convergence scan across all 13 regions."""
        batch_id = f"scan_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        results = []

        for region_id, defn in REGION_DEFINITIONS.items():
            result = self._score_region(region_id, defn)
            results.append(result)
            self._persist_region(result, defn)
            self._persist_signals(result, batch_id)

            # Create cluster record if threshold met
            if result.convergence_score >= 44:
                self._persist_cluster(result)

        try:
            self.db.commit()
        except Exception as e:
            logger.error("Failed to commit scan results: %s", e)
            self.db.rollback()

        return results

    def score_region(self, region_id: str) -> Optional[RegionResult]:
        """Score a single region."""
        defn = REGION_DEFINITIONS.get(region_id)
        if not defn:
            return None
        result = self._score_region(region_id, defn)
        self._persist_region(result, defn)
        try:
            self.db.commit()
        except Exception as e:
            logger.error("Failed to commit region score: %s", e)
            self.db.rollback()
        return result

    def get_all_regions(self) -> List[Dict[str, Any]]:
        """Get all persisted region scores."""
        regions = self.db.query(ConvergenceRegion).order_by(
            ConvergenceRegion.convergence_score.desc()
        ).all()

        if not regions:
            # No scan run yet — return definitions with zero scores
            return [
                {
                    "region_id": rid,
                    "label": defn["label"],
                    "states": defn["states"],
                    "map_x": defn["map_x"],
                    "map_y": defn["map_y"],
                    "epa_score": 0, "irs_migration_score": 0,
                    "trade_score": 0, "water_score": 0, "macro_score": 0,
                    "convergence_score": 0, "convergence_grade": "F",
                    "cluster_status": "LOW", "active_signals": [],
                    "signal_count": 0, "scored_at": None,
                }
                for rid, defn in REGION_DEFINITIONS.items()
            ]

        result = []
        for r in regions:
            d = r.to_dict()
            defn = REGION_DEFINITIONS.get(r.region_id, {})
            d["map_x"] = defn.get("map_x", 0)
            d["map_y"] = defn.get("map_y", 0)
            result.append(d)
        return result

    def get_clusters(self, min_score: float = 44) -> List[Dict[str, Any]]:
        """Get active clusters above threshold."""
        clusters = self.db.query(ConvergenceRegion).filter(
            ConvergenceRegion.convergence_score >= min_score
        ).order_by(
            ConvergenceRegion.convergence_score.desc()
        ).all()
        result = []
        for c in clusters:
            d = c.to_dict()
            defn = REGION_DEFINITIONS.get(c.region_id, {})
            d["map_x"] = defn.get("map_x", 0)
            d["map_y"] = defn.get("map_y", 0)
            result.append(d)
        return result

    def get_recent_signals(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent signal events for the live feed."""
        signals = self.db.query(ConvergenceSignal).order_by(
            ConvergenceSignal.detected_at.desc()
        ).limit(limit).all()
        return [s.to_dict() for s in signals]

    def get_stats(self) -> Dict[str, Any]:
        """Get dashboard stats: total signals, active clusters, new 24h."""
        total_signals = self.db.query(func.count(ConvergenceSignal.id)).scalar() or 0
        active_clusters = self.db.query(func.count(ConvergenceRegion.id)).filter(
            ConvergenceRegion.convergence_score >= 44
        ).scalar() or 0
        # Count signals from the latest batch as "new"
        latest_batch = self.db.query(ConvergenceSignal.batch_id).order_by(
            ConvergenceSignal.detected_at.desc()
        ).limit(1).scalar()
        new_signals = 0
        if latest_batch:
            new_signals = self.db.query(func.count(ConvergenceSignal.id)).filter(
                ConvergenceSignal.batch_id == latest_batch
            ).scalar() or 0

        return {
            "total_signals": total_signals,
            "active_clusters": active_clusters,
            "new_24h": new_signals,
            "total_records": total_signals,  # alias for frontend
        }

    async def generate_thesis(self, region_id: str) -> Optional[ThesisResult]:
        """Generate AI investment thesis for a region using Claude."""
        defn = REGION_DEFINITIONS.get(region_id)
        if not defn:
            return None

        # Check for cached thesis
        existing = self.db.query(ConvergenceCluster).filter(
            ConvergenceCluster.region_id == region_id,
            ConvergenceCluster.thesis_text.isnot(None),
        ).order_by(ConvergenceCluster.detected_at.desc()).first()

        if existing and existing.thesis_text:
            return ThesisResult(
                region_id=region_id,
                thesis_text=existing.thesis_text,
                opportunity_score=existing.opportunity_score or 50,
                urgency_score=existing.urgency_score or 50,
                risk_score=existing.risk_score or 50,
            )

        # Get current scores
        region = self.db.query(ConvergenceRegion).filter(
            ConvergenceRegion.region_id == region_id
        ).first()

        if not region:
            return None

        scores = RegionScores(
            epa=region.epa_score or 0,
            irs=region.irs_migration_score or 0,
            trade=region.trade_score or 0,
            water=region.water_score or 0,
            macro=region.macro_score or 0,
        )
        active = get_active_signals(scores)
        conv_score = region.convergence_score or 0

        # Build prompt
        signal_details = []
        if scores.epa >= 60:
            signal_details.append(f"EPA violation score {scores.epa:.0f}/100")
        if scores.irs >= 60:
            signal_details.append(f"IRS migration signal {scores.irs:.0f}/100")
        if scores.trade >= 60:
            signal_details.append(f"trade stress {scores.trade:.0f}/100")
        if scores.water >= 60:
            signal_details.append(f"water system stress {scores.water:.0f}/100")
        if scores.macro >= 60:
            signal_details.append(f"macro/income signal {scores.macro:.0f}/100")

        try:
            from app.agentic.llm_client import LLMClient

            llm = LLMClient(
                provider="anthropic",
                model="claude-3-5-haiku-20241022",
                max_tokens=600,
                temperature=0.3,
            )

            system_prompt = (
                "You are a private equity investment analyst. Given convergent public data "
                "signals for a US region, generate a sharp 3-sentence investment thesis. "
                "Then output a JSON block on a new line with scores. "
                'Format: thesis text\\n{"opportunity":N,"urgency":N,"risk":N} '
                "where N is 0-100. No markdown, no backticks."
            )
            user_prompt = (
                f"Region: {defn['label']}\n"
                f"Convergence score: {conv_score:.0f}/100\n"
                f"Active signals: {', '.join(signal_details) if signal_details else 'none above threshold'}\n\n"
                "Generate the investment thesis and JSON scores."
            )

            response = await llm.complete(user_prompt, system_prompt=system_prompt)
            raw = response.content.strip()

            # Parse thesis and scores
            thesis = ""
            score_data = None
            for line in raw.split("\n"):
                t = line.strip()
                if t.startswith("{"):
                    try:
                        score_data = json.loads(t)
                    except json.JSONDecodeError:
                        pass
                elif len(t) > 20:
                    thesis = t

            opp = score_data.get("opportunity", conv_score) if score_data else conv_score
            urg = score_data.get("urgency", round(conv_score * 0.85)) if score_data else round(conv_score * 0.85)
            risk = score_data.get("risk", round(conv_score * 0.55)) if score_data else round(conv_score * 0.55)

            result = ThesisResult(
                region_id=region_id,
                thesis_text=thesis or raw.split("\n")[0],
                opportunity_score=opp,
                urgency_score=urg,
                risk_score=risk,
            )

            # Cache the thesis
            cluster = ConvergenceCluster(
                region_id=region_id,
                convergence_score=conv_score,
                signal_count=len(active),
                active_signals=active,
                cluster_status=classify_cluster(conv_score),
                thesis_text=result.thesis_text,
                opportunity_score=result.opportunity_score,
                urgency_score=result.urgency_score,
                risk_score=result.risk_score,
            )
            self.db.add(cluster)
            self.db.commit()

            return result

        except Exception as e:
            logger.warning("Thesis generation failed for %s: %s", region_id, e)
            # Return fallback
            return ThesisResult(
                region_id=region_id,
                thesis_text=(
                    f"{defn['label']} shows convergence score {conv_score:.0f}/100 "
                    f"with {len(active)} active signals ({', '.join(active)}). "
                    "Connect an API key to enable AI-powered thesis generation."
                ),
                opportunity_score=conv_score,
                urgency_score=round(conv_score * 0.85),
                risk_score=round(conv_score * 0.55),
            )

    # ------------------------------------------------------------------
    # Signal Scorers (private)
    # ------------------------------------------------------------------

    def _score_region(self, region_id: str, defn: Dict) -> RegionResult:
        """Compute all signal scores for a region."""
        states = defn["states"]
        scores = RegionScores(
            epa=self._score_epa(states),
            irs=self._score_irs_migration(states),
            trade=self._score_trade(states),
            water=self._score_water(states),
            macro=self._score_macro(states),
        )
        conv = compute_convergence(scores)
        active = get_active_signals(scores)

        return RegionResult(
            region_id=region_id,
            label=defn["label"],
            scores=scores,
            convergence_score=conv,
            convergence_grade=grade_score(conv),
            cluster_status=classify_cluster(conv),
            active_signals=active,
            signal_count=len(active),
        )

    def _score_epa(self, states: List[str]) -> float:
        """Score EPA environmental signal for a set of states.

        Higher violations/penalties = higher signal (distressed = opportunity).
        Normalized against national benchmarks.
        """
        try:
            result = self.db.execute(text("""
                SELECT
                    COUNT(*) as facility_count,
                    COALESCE(SUM(violation_count), 0) as total_violations,
                    COALESCE(SUM(penalty_amount), 0) as total_penalties,
                    COALESCE(AVG(violation_count), 0) as avg_violations
                FROM epa_echo_facilities
                WHERE state = ANY(:states)
            """), {"states": states}).mappings().first()

            if not result or result["facility_count"] == 0:
                return 0.0

            # Score components (calibrated to actual data ranges):
            # 1. Violation density (violations per facility) — actual avg ~0.5
            violation_density = result["avg_violations"]
            density_score = _normalize(float(violation_density), 0, 1.5)

            # 2. Penalty magnitude (avg penalty per facility)
            avg_penalty = float(result["total_penalties"]) / max(result["facility_count"], 1)
            penalty_score = _normalize(avg_penalty, 0, 10000)

            # 3. Total violation volume — scaled to region size
            volume_score = _normalize(float(result["total_violations"]), 0, 50000)

            # Weighted composite
            score = density_score * 0.4 + penalty_score * 0.3 + volume_score * 0.3
            return _clamp(round(score, 1))

        except Exception as e:
            logger.warning("EPA scorer failed for %s: %s", states, e)
            return 0.0

    def _score_irs_migration(self, states: List[str]) -> float:
        """Score IRS migration signal for a set of states.

        High net migration (in or out) = high signal.
        Uses absolute magnitude — both growth and distress are signals.
        """
        try:
            # IRS migration data: orig_state_abbr is populated for both flows,
            # dest_state_abbr may be NULL. Query by orig_state for both directions.
            inflow = self.db.execute(text("""
                SELECT COALESCE(SUM(num_returns), 0) as total_returns,
                       COALESCE(SUM(total_agi), 0) as total_agi
                FROM irs_soi_migration
                WHERE orig_state_abbr = ANY(:states)
                  AND flow_type = 'inflow'
            """), {"states": states}).mappings().first()

            outflow = self.db.execute(text("""
                SELECT COALESCE(SUM(num_returns), 0) as total_returns,
                       COALESCE(SUM(total_agi), 0) as total_agi
                FROM irs_soi_migration
                WHERE orig_state_abbr = ANY(:states)
                  AND flow_type = 'outflow'
            """), {"states": states}).mappings().first()

            if not inflow and not outflow:
                return 0.0

            in_returns = inflow["total_returns"] if inflow else 0
            out_returns = outflow["total_returns"] if outflow else 0
            in_agi = inflow["total_agi"] if inflow else 0
            out_agi = outflow["total_agi"] if outflow else 0

            total_flow = in_returns + out_returns
            if total_flow == 0:
                return 0.0

            # Net migration rate (absolute — both directions signal opportunity)
            net_rate = abs(in_returns - out_returns) / max(total_flow, 1)
            rate_score = _normalize(net_rate, 0, 0.15)

            # AGI magnitude (absolute income movement, in thousands)
            net_agi = abs(in_agi - out_agi)
            agi_score = _normalize(float(net_agi), 0, 500_000)

            # Volume score — calibrated to actual data range
            volume_score = _normalize(total_flow, 0, 100_000)

            score = rate_score * 0.4 + agi_score * 0.35 + volume_score * 0.25
            return _clamp(round(score, 1))

        except Exception as e:
            logger.warning("IRS migration scorer failed for %s: %s", states, e)
            return 0.0

    def _score_trade(self, states: List[str]) -> float:
        """Score trade signal for a set of states.

        Based on export volume and concentration.
        """
        try:
            result = self.db.execute(text("""
                SELECT
                    COUNT(DISTINCT country_code) as country_count,
                    COALESCE(SUM(value_monthly), 0) as total_exports,
                    COUNT(DISTINCT hs_code) as commodity_count
                FROM us_trade_exports_state
                WHERE state_code = ANY(:states)
            """), {"states": states}).mappings().first()

            if not result or result["total_exports"] == 0:
                return 0.0

            # Export volume
            volume_score = _normalize(result["total_exports"], 0, 10_000_000_000)

            # Trade diversity (number of trading partners)
            diversity_score = _normalize(result["country_count"], 0, 150)

            # Commodity breadth
            commodity_score = _normalize(result["commodity_count"], 0, 100)

            score = volume_score * 0.5 + diversity_score * 0.3 + commodity_score * 0.2
            return _clamp(round(score, 1))

        except Exception as e:
            logger.warning("Trade scorer failed for %s: %s", states, e)
            return 0.0

    def _score_water(self, states: List[str]) -> float:
        """Score water system stress for a set of states.

        Higher violations and health-based issues = higher signal.
        """
        try:
            # Water system stats
            systems = self.db.execute(text("""
                SELECT
                    COUNT(*) as system_count,
                    COALESCE(SUM(population_served), 0) as total_pop
                FROM public_water_system
                WHERE state = ANY(:states)
            """), {"states": states}).mappings().first()

            # Violation stats
            violations = self.db.execute(text("""
                SELECT
                    COUNT(*) as violation_count,
                    COALESCE(SUM(CASE WHEN is_health_based THEN 1 ELSE 0 END), 0) as health_violations
                FROM water_system_violation v
                JOIN public_water_system p ON v.pwsid = p.pwsid
                WHERE p.state = ANY(:states)
            """), {"states": states}).mappings().first()

            sys_count = systems["system_count"] if systems else 0
            if sys_count == 0:
                return 0.0

            viol_count = violations["violation_count"] if violations else 0
            health_viols = violations["health_violations"] if violations else 0

            # Violation density (violations per 100 systems)
            density = (viol_count / sys_count) * 100
            density_score = _normalize(density, 0, 50)

            # Health-based violation ratio
            health_ratio = health_viols / max(viol_count, 1)
            health_score = _normalize(health_ratio, 0, 0.5)

            # Population at risk
            pop = systems["total_pop"] if systems else 0
            pop_score = _normalize(pop, 0, 20_000_000)

            score = density_score * 0.4 + health_score * 0.35 + pop_score * 0.25
            return _clamp(round(score, 1))

        except Exception as e:
            logger.warning("Water scorer failed for %s: %s", states, e)
            return 0.0

    def _score_macro(self, states: List[str]) -> float:
        """Score macro/income signal for a set of states.

        Based on income distribution, capital gains concentration,
        and business income — indicators of economic dynamism.
        """
        try:
            # Sum across all AGI classes (no class '0' total row in data)
            result = self.db.execute(text("""
                SELECT
                    COALESCE(SUM(num_returns), 0) as total_returns,
                    COALESCE(SUM(total_agi), 0) as total_agi,
                    COALESCE(SUM(total_capital_gains), 0) as total_capgains,
                    COALESCE(SUM(total_business_income), 0) as total_biz_income,
                    CASE WHEN SUM(num_returns) > 0
                         THEN SUM(total_agi) * 1000.0 / SUM(num_returns)
                         ELSE 0 END as mean_agi
                FROM irs_soi_zip_income
                WHERE state_abbr = ANY(:states)
            """), {"states": states}).mappings().first()

            if not result or result["total_returns"] == 0:
                return 0.0

            # Average AGI level
            agi_score = _normalize(float(result["mean_agi"]), 30000, 120000)

            # Capital gains concentration (capgains as % of AGI)
            total_agi = max(float(result["total_agi"]), 1)
            capgains_ratio = float(result["total_capgains"]) / total_agi
            capgains_score = _normalize(capgains_ratio, 0, 0.15)

            # Business income concentration
            biz_ratio = float(result["total_biz_income"]) / total_agi
            biz_score = _normalize(biz_ratio, 0, 0.10)

            # Volume
            volume_score = _normalize(result["total_returns"], 0, 5_000_000)

            score = agi_score * 0.3 + capgains_score * 0.25 + biz_score * 0.25 + volume_score * 0.2
            return _clamp(round(score, 1))

        except Exception as e:
            logger.warning("Macro scorer failed for %s: %s", states, e)
            return 0.0

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    def _persist_region(self, result: RegionResult, defn: Dict):
        """Upsert region scores to the database."""
        existing = self.db.query(ConvergenceRegion).filter(
            ConvergenceRegion.region_id == result.region_id
        ).first()

        if existing:
            existing.epa_score = result.scores.epa
            existing.irs_migration_score = result.scores.irs
            existing.trade_score = result.scores.trade
            existing.water_score = result.scores.water
            existing.macro_score = result.scores.macro
            existing.convergence_score = result.convergence_score
            existing.convergence_grade = result.convergence_grade
            existing.cluster_status = result.cluster_status
            existing.active_signals = result.active_signals
            existing.signal_count = result.signal_count
            existing.scored_at = datetime.utcnow()
        else:
            region = ConvergenceRegion(
                region_id=result.region_id,
                label=result.label,
                states=defn["states"],
                center_lat=defn["center_lat"],
                center_lon=defn["center_lon"],
                epa_score=result.scores.epa,
                irs_migration_score=result.scores.irs,
                trade_score=result.scores.trade,
                water_score=result.scores.water,
                macro_score=result.scores.macro,
                convergence_score=result.convergence_score,
                convergence_grade=result.convergence_grade,
                cluster_status=result.cluster_status,
                active_signals=result.active_signals,
                signal_count=result.signal_count,
                scored_at=datetime.utcnow(),
            )
            self.db.add(region)

    def _persist_signals(self, result: RegionResult, batch_id: str):
        """Persist individual signal events for the live feed."""
        score_map = {
            "epa": (result.scores.epa, "EPA / environmental"),
            "irs": (result.scores.irs, "IRS migration"),
            "trade": (result.scores.trade, "Trade & commerce"),
            "water": (result.scores.water, "Water systems"),
            "macro": (result.scores.macro, "Macro / income"),
        }

        for sig_type, (score, label) in score_map.items():
            if score > 0:  # Only persist non-zero signals
                desc_templates = {
                    "epa": f"EPA environmental signal — {result.label}: score {score:.0f}",
                    "irs": f"IRS migration signal — {result.label}: score {score:.0f}",
                    "trade": f"Trade signal — {result.label}: score {score:.0f}",
                    "water": f"Water system stress — {result.label}: score {score:.0f}",
                    "macro": f"Macro/income signal — {result.label}: score {score:.0f}",
                }
                signal = ConvergenceSignal(
                    region_id=result.region_id,
                    signal_type=sig_type,
                    score=score,
                    description=desc_templates[sig_type],
                    batch_id=batch_id,
                )
                self.db.add(signal)

    def _persist_cluster(self, result: RegionResult):
        """Persist cluster event for regions above threshold."""
        cluster = ConvergenceCluster(
            region_id=result.region_id,
            convergence_score=result.convergence_score,
            signal_count=result.signal_count,
            active_signals=result.active_signals,
            cluster_status=result.cluster_status,
        )
        self.db.add(cluster)
