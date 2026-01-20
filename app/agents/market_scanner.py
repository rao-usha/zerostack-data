"""
Agentic Market Scanner (T49)

AI agent that scans all data sources to identify market trends,
emerging patterns, and investment opportunities.
"""

import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple
from collections import defaultdict

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS & CONSTANTS
# =============================================================================

class SignalType(str, Enum):
    """Types of market signals."""
    SECTOR_MOMENTUM = "sector_momentum"
    GEOGRAPHIC_SHIFT = "geographic_shift"
    TALENT_FLOW = "talent_flow"
    FUNDING_SURGE = "funding_surge"
    SENTIMENT_SHIFT = "sentiment_shift"
    ACTIVITY_SPIKE = "activity_spike"


class SignalDirection(str, Enum):
    """Signal direction indicators."""
    ACCELERATING = "accelerating"
    DECELERATING = "decelerating"
    STABLE = "stable"
    REVERSING = "reversing"


class TrendStage(str, Enum):
    """Stages of market trends."""
    EARLY = "early"
    EMERGING = "emerging"
    MAINSTREAM = "mainstream"
    DECLINING = "declining"


class ScanStatus(str, Enum):
    """Scan job status."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


# Sector categories for grouping
SECTOR_CATEGORIES = {
    "fintech": ["fintech", "financial", "payments", "banking", "insurance"],
    "healthcare": ["healthcare", "health", "medical", "biotech", "pharma"],
    "ai_ml": ["ai", "artificial intelligence", "machine learning", "ml", "deep learning"],
    "enterprise": ["enterprise", "saas", "b2b", "software"],
    "consumer": ["consumer", "retail", "ecommerce", "marketplace"],
    "climate": ["climate", "cleantech", "energy", "sustainability", "green"],
    "crypto": ["crypto", "blockchain", "web3", "defi"],
    "real_estate": ["real estate", "proptech", "property"],
    "education": ["education", "edtech", "learning"],
    "transportation": ["transportation", "logistics", "mobility", "automotive"],
}

# Region mapping
REGION_MAPPING = {
    "CA": "US West", "WA": "US West", "OR": "US West", "NV": "US West",
    "NY": "US Northeast", "MA": "US Northeast", "CT": "US Northeast", "NJ": "US Northeast",
    "TX": "US South", "FL": "US South", "GA": "US South",
    "IL": "US Midwest", "OH": "US Midwest", "MI": "US Midwest",
    "UK": "Europe", "Germany": "Europe", "France": "Europe",
    "China": "Asia Pacific", "Japan": "Asia Pacific", "Singapore": "Asia Pacific",
    "India": "Asia Pacific", "Australia": "Asia Pacific",
}


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class MarketSignal:
    """A detected market signal."""
    signal_id: str
    signal_type: str
    category: str
    direction: str
    strength: float  # 0-1
    confidence: float  # 0-1
    description: str
    data_points: List[Dict[str, Any]] = field(default_factory=list)
    first_detected: Optional[datetime] = None
    trend: str = "new"  # new, strengthening, weakening, stable


@dataclass
class MarketTrend:
    """An identified market trend."""
    trend_id: str
    name: str
    sectors: List[str]
    momentum: float
    stage: str
    supporting_signals: List[str]
    description: str
    historical_comparison: Optional[str] = None


@dataclass
class MarketOpportunity:
    """A spotted investment opportunity."""
    opportunity_id: str
    opportunity_type: str
    title: str
    thesis: str
    confidence: float
    signals: List[str]
    recommended_actions: List[str]


@dataclass
class MarketBrief:
    """Weekly market intelligence brief."""
    brief_id: str
    period_start: date
    period_end: date
    summary: str
    top_signals: List[Dict]
    emerging_patterns: List[Dict]
    sector_spotlight: Dict
    geographic_shifts: List[Dict]
    early_warnings: List[Dict]


# =============================================================================
# MARKET SCANNER AGENT
# =============================================================================

class MarketScannerAgent:
    """
    AI agent that scans market data to identify trends and opportunities.

    Analyzes data from:
    - Form D filings (funding activity)
    - GitHub (developer interest)
    - Glassdoor (talent signals)
    - App Store (consumer interest)
    - Web Traffic (market attention)
    - News (events, sentiment)
    - Company Scores (health trends)
    """

    # Cache TTL in seconds (1 hour)
    CACHE_TTL = 3600

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Create necessary database tables."""

        # Market scans table
        create_scans = text("""
            CREATE TABLE IF NOT EXISTS market_scans (
                id SERIAL PRIMARY KEY,
                scan_id VARCHAR(50) UNIQUE NOT NULL,
                scan_type VARCHAR(20) DEFAULT 'scheduled',
                started_at TIMESTAMP DEFAULT NOW(),
                completed_at TIMESTAMP,
                status VARCHAR(20) DEFAULT 'running',
                sources_scanned JSONB DEFAULT '[]',
                signals_detected INT DEFAULT 0,
                results JSONB,
                error_message TEXT
            )
        """)

        # Market signals table
        create_signals = text("""
            CREATE TABLE IF NOT EXISTS market_signals (
                id SERIAL PRIMARY KEY,
                signal_id VARCHAR(50) UNIQUE NOT NULL,
                signal_type VARCHAR(50) NOT NULL,
                category VARCHAR(100),
                direction VARCHAR(20),
                strength FLOAT,
                confidence FLOAT,
                description TEXT,
                data_points JSONB,
                first_detected TIMESTAMP,
                last_updated TIMESTAMP DEFAULT NOW(),
                status VARCHAR(20) DEFAULT 'active',
                scan_id VARCHAR(50)
            )
        """)

        # Market briefs table
        create_briefs = text("""
            CREATE TABLE IF NOT EXISTS market_briefs (
                id SERIAL PRIMARY KEY,
                brief_id VARCHAR(50) UNIQUE NOT NULL,
                period_start DATE,
                period_end DATE,
                brief_type VARCHAR(20) DEFAULT 'weekly',
                summary TEXT,
                sections JSONB,
                signals_included JSONB,
                generated_at TIMESTAMP DEFAULT NOW()
            )
        """)

        # Create indexes
        create_indexes = text("""
            CREATE INDEX IF NOT EXISTS idx_market_signals_type ON market_signals(signal_type);
            CREATE INDEX IF NOT EXISTS idx_market_signals_category ON market_signals(category);
            CREATE INDEX IF NOT EXISTS idx_market_signals_status ON market_signals(status);
            CREATE INDEX IF NOT EXISTS idx_market_scans_status ON market_scans(status);
            CREATE INDEX IF NOT EXISTS idx_market_briefs_period ON market_briefs(period_start, period_end);
        """)

        try:
            self.db.execute(create_scans)
            self.db.execute(create_signals)
            self.db.execute(create_briefs)
            self.db.execute(create_indexes)
            self.db.commit()
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            self.db.rollback()

    # -------------------------------------------------------------------------
    # Main Scan Methods
    # -------------------------------------------------------------------------

    def run_scan(self, scan_type: str = "scheduled") -> Dict[str, Any]:
        """
        Run a comprehensive market scan.

        Returns scan results with detected signals.
        """
        scan_id = f"scan_{uuid.uuid4().hex[:12]}"

        # Create scan record
        self._create_scan_record(scan_id, scan_type)

        try:
            signals = []
            sources_scanned = []

            # Scan each data source
            sector_signals = self._scan_sector_momentum()
            signals.extend(sector_signals)
            sources_scanned.append("form_d")

            geo_signals = self._scan_geographic_shifts()
            signals.extend(geo_signals)
            sources_scanned.append("geographic")

            talent_signals = self._scan_talent_flows()
            signals.extend(talent_signals)
            sources_scanned.append("glassdoor")

            funding_signals = self._scan_funding_activity()
            signals.extend(funding_signals)
            sources_scanned.append("funding")

            activity_signals = self._scan_activity_spikes()
            signals.extend(activity_signals)
            sources_scanned.append("activity")

            # Save signals to database
            for signal in signals:
                self._save_signal(signal, scan_id)

            # Update scan record
            results = {
                "signals": [self._signal_to_dict(s) for s in signals],
                "total_signals": len(signals),
                "by_type": self._group_signals_by_type(signals),
                "by_category": self._group_signals_by_category(signals),
            }

            self._complete_scan(scan_id, sources_scanned, len(signals), results)

            return {
                "scan_id": scan_id,
                "status": "completed",
                "scan_timestamp": datetime.utcnow().isoformat() + "Z",
                **results
            }

        except Exception as e:
            logger.error(f"Scan failed: {e}")
            self._fail_scan(scan_id, str(e))
            raise

    def get_current_signals(self, limit: int = 50) -> Dict[str, Any]:
        """Get current active market signals (cached)."""

        # Check for recent scan
        recent_scan = self._get_recent_scan()

        if recent_scan and recent_scan.get("results"):
            return {
                "scan_timestamp": recent_scan.get("completed_at"),
                "cached": True,
                **recent_scan.get("results", {})
            }

        # Run new scan if no recent results
        return self.run_scan("auto")

    def get_trends(self, period_days: int = 30) -> Dict[str, Any]:
        """Analyze emerging trends from signals."""

        # Get signals from the period
        signals = self._get_signals_for_period(period_days)

        trends = []

        # Group signals by sector and analyze
        sector_signals = defaultdict(list)
        for signal in signals:
            if signal.get("category"):
                sector_signals[signal["category"]].append(signal)

        for sector, sector_sigs in sector_signals.items():
            if len(sector_sigs) >= 2:
                # Calculate momentum
                strengths = [s.get("strength", 0) for s in sector_sigs]
                avg_strength = sum(strengths) / len(strengths) if strengths else 0

                # Determine stage
                if avg_strength > 0.7:
                    stage = TrendStage.MAINSTREAM.value
                elif avg_strength > 0.5:
                    stage = TrendStage.EMERGING.value
                elif avg_strength > 0.3:
                    stage = TrendStage.EARLY.value
                else:
                    stage = TrendStage.DECLINING.value

                trend = {
                    "trend_id": f"trend_{uuid.uuid4().hex[:8]}",
                    "name": f"{sector.replace('_', ' ').title()} Activity",
                    "sectors": [sector],
                    "momentum": round(avg_strength, 2),
                    "stage": stage,
                    "signal_count": len(sector_sigs),
                    "supporting_signals": [s.get("signal_id") for s in sector_sigs[:5]],
                    "description": self._generate_trend_description(sector, sector_sigs, avg_strength)
                }
                trends.append(trend)

        # Sort by momentum
        trends.sort(key=lambda x: x["momentum"], reverse=True)

        return {
            "period": f"{period_days}d",
            "trends": trends[:20],
            "total_trends": len(trends),
            "analysis_timestamp": datetime.utcnow().isoformat() + "Z"
        }

    def get_opportunities(self) -> Dict[str, Any]:
        """Identify investment opportunities from market signals."""

        opportunities = []

        # Get recent signals
        signals = self._get_signals_for_period(30)

        # Look for sector rotation opportunities
        sector_rotations = self._identify_sector_rotations(signals)
        opportunities.extend(sector_rotations)

        # Look for contrarian opportunities
        contrarian_opps = self._identify_contrarian_opportunities(signals)
        opportunities.extend(contrarian_opps)

        # Look for momentum opportunities
        momentum_opps = self._identify_momentum_opportunities(signals)
        opportunities.extend(momentum_opps)

        # Sort by confidence
        opportunities.sort(key=lambda x: x.get("confidence", 0), reverse=True)

        return {
            "opportunities": opportunities[:15],
            "total_found": len(opportunities),
            "generated_at": datetime.utcnow().isoformat() + "Z"
        }

    def generate_brief(self, period_type: str = "weekly") -> Dict[str, Any]:
        """Generate market intelligence brief."""

        # Determine period
        today = date.today()
        if period_type == "weekly":
            period_start = today - timedelta(days=7)
        elif period_type == "daily":
            period_start = today - timedelta(days=1)
        else:  # monthly
            period_start = today - timedelta(days=30)

        brief_id = f"brief_{period_type[:1]}{today.strftime('%W')}_{today.year}"

        # Check for existing brief
        existing = self._get_existing_brief(brief_id)
        if existing:
            return existing

        # Get signals for period
        days = (today - period_start).days
        signals = self._get_signals_for_period(days)

        # Build sections
        top_signals = sorted(signals, key=lambda x: x.get("strength", 0), reverse=True)[:10]

        emerging = [s for s in signals if s.get("strength", 0) > 0.5 and s.get("direction") == "accelerating"]

        sector_data = self._build_sector_spotlight(signals)

        geo_shifts = [s for s in signals if s.get("signal_type") == SignalType.GEOGRAPHIC_SHIFT.value]

        early_warnings = [s for s in signals if s.get("direction") == "decelerating" and s.get("strength", 0) > 0.3]

        # Generate summary
        summary = self._generate_brief_summary(signals, period_type)

        brief = {
            "brief_id": brief_id,
            "period": {
                "start": period_start.isoformat(),
                "end": today.isoformat()
            },
            "brief_type": period_type,
            "summary": summary,
            "sections": {
                "top_signals": [self._signal_summary(s) for s in top_signals],
                "emerging_patterns": [self._signal_summary(s) for s in emerging[:5]],
                "sector_spotlight": sector_data,
                "geographic_shifts": [self._signal_summary(s) for s in geo_shifts[:5]],
                "early_warnings": [self._signal_summary(s) for s in early_warnings[:5]]
            },
            "stats": {
                "total_signals": len(signals),
                "accelerating": len([s for s in signals if s.get("direction") == "accelerating"]),
                "decelerating": len([s for s in signals if s.get("direction") == "decelerating"])
            },
            "generated_at": datetime.utcnow().isoformat() + "Z"
        }

        # Save brief
        self._save_brief(brief)

        return brief

    def get_history(self, limit: int = 20) -> Dict[str, Any]:
        """Get historical scans and briefs."""

        scans = self._get_recent_scans(limit)
        briefs = self._get_recent_briefs(limit)

        return {
            "scans": scans,
            "briefs": briefs,
            "total_scans": len(scans),
            "total_briefs": len(briefs)
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get scanner statistics."""

        try:
            stats_query = text("""
                SELECT
                    (SELECT COUNT(*) FROM market_scans) as total_scans,
                    (SELECT COUNT(*) FROM market_scans WHERE status = 'completed') as completed_scans,
                    (SELECT COUNT(*) FROM market_signals) as total_signals,
                    (SELECT COUNT(*) FROM market_signals WHERE status = 'active') as active_signals,
                    (SELECT COUNT(*) FROM market_briefs) as total_briefs,
                    (SELECT MAX(completed_at) FROM market_scans WHERE status = 'completed') as last_scan
            """)

            result = self.db.execute(stats_query).fetchone()

            return {
                "total_scans": result[0] or 0,
                "completed_scans": result[1] or 0,
                "total_signals": result[2] or 0,
                "active_signals": result[3] or 0,
                "total_briefs": result[4] or 0,
                "last_scan": result[5].isoformat() if result[5] else None
            }
        except Exception as e:
            logger.error(f"Error getting stats: {e}")
            self.db.rollback()
            return {}

    # -------------------------------------------------------------------------
    # Signal Detection Methods
    # -------------------------------------------------------------------------

    def _scan_sector_momentum(self) -> List[MarketSignal]:
        """Detect sector momentum from Form D and other sources."""
        signals = []

        try:
            # Query Form D filings by industry
            query = text("""
                SELECT
                    industry_group,
                    COUNT(*) as filing_count,
                    SUM(CAST(total_offering_amount AS FLOAT)) as total_amount
                FROM form_d_filings
                WHERE filing_date >= NOW() - INTERVAL '30 days'
                AND industry_group IS NOT NULL
                GROUP BY industry_group
                ORDER BY filing_count DESC
                LIMIT 20
            """)

            result = self.db.execute(query).fetchall()

            # Get previous period for comparison
            prev_query = text("""
                SELECT
                    industry_group,
                    COUNT(*) as filing_count
                FROM form_d_filings
                WHERE filing_date >= NOW() - INTERVAL '60 days'
                AND filing_date < NOW() - INTERVAL '30 days'
                AND industry_group IS NOT NULL
                GROUP BY industry_group
            """)

            prev_result = self.db.execute(prev_query).fetchall()
            prev_counts = {row[0]: row[1] for row in prev_result}

            for row in result:
                industry = row[0]
                current_count = row[1]
                total_amount = row[2] or 0
                prev_count = prev_counts.get(industry, 0)

                if prev_count > 0:
                    change_pct = (current_count - prev_count) / prev_count
                else:
                    change_pct = 1.0 if current_count > 0 else 0

                # Detect significant momentum
                if abs(change_pct) >= 0.2:
                    category = self._map_industry_to_category(industry)
                    direction = SignalDirection.ACCELERATING.value if change_pct > 0 else SignalDirection.DECELERATING.value
                    strength = min(abs(change_pct), 1.0)

                    signal = MarketSignal(
                        signal_id=f"sig_{uuid.uuid4().hex[:12]}",
                        signal_type=SignalType.SECTOR_MOMENTUM.value,
                        category=category,
                        direction=direction,
                        strength=round(strength, 2),
                        confidence=0.7 if current_count >= 5 else 0.5,
                        description=f"{industry}: {int(change_pct*100)}% change in Form D filings ({current_count} filings, ${total_amount/1e6:.1f}M total)",
                        data_points=[{
                            "industry": industry,
                            "current_count": current_count,
                            "previous_count": prev_count,
                            "change_pct": round(change_pct * 100, 1),
                            "total_amount": total_amount
                        }]
                    )
                    signals.append(signal)

        except Exception as e:
            logger.warning(f"Error scanning sector momentum: {e}")
            self.db.rollback()

        return signals

    def _scan_geographic_shifts(self) -> List[MarketSignal]:
        """Detect geographic investment shifts."""
        signals = []

        try:
            # Query Form D filings by state
            query = text("""
                SELECT
                    issuer_state,
                    COUNT(*) as filing_count,
                    SUM(CAST(total_offering_amount AS FLOAT)) as total_amount
                FROM form_d_filings
                WHERE filing_date >= NOW() - INTERVAL '30 days'
                AND issuer_state IS NOT NULL
                GROUP BY issuer_state
                ORDER BY filing_count DESC
                LIMIT 20
            """)

            result = self.db.execute(query).fetchall()

            # Get previous period
            prev_query = text("""
                SELECT
                    issuer_state,
                    COUNT(*) as filing_count
                FROM form_d_filings
                WHERE filing_date >= NOW() - INTERVAL '60 days'
                AND filing_date < NOW() - INTERVAL '30 days'
                AND issuer_state IS NOT NULL
                GROUP BY issuer_state
            """)

            prev_result = self.db.execute(prev_query).fetchall()
            prev_counts = {row[0]: row[1] for row in prev_result}

            for row in result:
                state = row[0]
                current_count = row[1]
                prev_count = prev_counts.get(state, 0)

                if prev_count > 0:
                    change_pct = (current_count - prev_count) / prev_count
                else:
                    change_pct = 0.5 if current_count >= 3 else 0

                # Detect significant shifts
                if abs(change_pct) >= 0.3 and current_count >= 3:
                    region = REGION_MAPPING.get(state, "Other")
                    direction = SignalDirection.ACCELERATING.value if change_pct > 0 else SignalDirection.DECELERATING.value

                    signal = MarketSignal(
                        signal_id=f"sig_{uuid.uuid4().hex[:12]}",
                        signal_type=SignalType.GEOGRAPHIC_SHIFT.value,
                        category=region,
                        direction=direction,
                        strength=round(min(abs(change_pct), 1.0), 2),
                        confidence=0.65,
                        description=f"{state} ({region}): {int(change_pct*100)}% change in investment activity",
                        data_points=[{
                            "state": state,
                            "region": region,
                            "current_count": current_count,
                            "previous_count": prev_count,
                            "change_pct": round(change_pct * 100, 1)
                        }]
                    )
                    signals.append(signal)

        except Exception as e:
            logger.warning(f"Error scanning geographic shifts: {e}")
            self.db.rollback()

        return signals

    def _scan_talent_flows(self) -> List[MarketSignal]:
        """Detect talent movement signals from Glassdoor data."""
        signals = []

        try:
            # Query companies with significant rating changes
            query = text("""
                SELECT
                    company_name,
                    overall_rating,
                    ceo_approval,
                    recommend_percent,
                    employee_count_estimate
                FROM glassdoor_companies
                WHERE updated_at >= NOW() - INTERVAL '30 days'
                ORDER BY overall_rating DESC
                LIMIT 50
            """)

            result = self.db.execute(query).fetchall()

            # Aggregate by rating bands
            high_rated = [r for r in result if r[1] and r[1] >= 4.0]
            low_rated = [r for r in result if r[1] and r[1] < 3.0]

            if len(high_rated) >= 5:
                signal = MarketSignal(
                    signal_id=f"sig_{uuid.uuid4().hex[:12]}",
                    signal_type=SignalType.TALENT_FLOW.value,
                    category="high_performers",
                    direction=SignalDirection.STABLE.value,
                    strength=0.6,
                    confidence=0.7,
                    description=f"{len(high_rated)} companies with 4.0+ Glassdoor rating - strong talent magnets",
                    data_points=[{
                        "company": r[0],
                        "rating": r[1],
                        "ceo_approval": r[2]
                    } for r in high_rated[:10]]
                )
                signals.append(signal)

            if len(low_rated) >= 3:
                signal = MarketSignal(
                    signal_id=f"sig_{uuid.uuid4().hex[:12]}",
                    signal_type=SignalType.TALENT_FLOW.value,
                    category="talent_risk",
                    direction=SignalDirection.DECELERATING.value,
                    strength=0.5,
                    confidence=0.6,
                    description=f"{len(low_rated)} companies with sub-3.0 rating - potential talent exodus",
                    data_points=[{
                        "company": r[0],
                        "rating": r[1]
                    } for r in low_rated[:5]]
                )
                signals.append(signal)

        except Exception as e:
            logger.warning(f"Error scanning talent flows: {e}")
            self.db.rollback()

        return signals

    def _scan_funding_activity(self) -> List[MarketSignal]:
        """Detect funding surge patterns."""
        signals = []

        try:
            # Large offerings in recent period
            query = text("""
                SELECT
                    industry_group,
                    COUNT(*) as large_deals,
                    AVG(CAST(total_offering_amount AS FLOAT)) as avg_amount
                FROM form_d_filings
                WHERE filing_date >= NOW() - INTERVAL '30 days'
                AND CAST(total_offering_amount AS FLOAT) > 10000000
                AND industry_group IS NOT NULL
                GROUP BY industry_group
                HAVING COUNT(*) >= 2
                ORDER BY large_deals DESC
            """)

            result = self.db.execute(query).fetchall()

            for row in result:
                industry = row[0]
                deal_count = row[1]
                avg_amount = row[2] or 0

                category = self._map_industry_to_category(industry)

                signal = MarketSignal(
                    signal_id=f"sig_{uuid.uuid4().hex[:12]}",
                    signal_type=SignalType.FUNDING_SURGE.value,
                    category=category,
                    direction=SignalDirection.ACCELERATING.value,
                    strength=min(deal_count / 10, 1.0),
                    confidence=0.75,
                    description=f"{industry}: {deal_count} large deals (>${10}M) with ${avg_amount/1e6:.1f}M average",
                    data_points=[{
                        "industry": industry,
                        "large_deal_count": deal_count,
                        "avg_amount": avg_amount
                    }]
                )
                signals.append(signal)

        except Exception as e:
            logger.warning(f"Error scanning funding activity: {e}")
            self.db.rollback()

        return signals

    def _scan_activity_spikes(self) -> List[MarketSignal]:
        """Detect activity spikes in GitHub, App Store, etc."""
        signals = []

        try:
            # GitHub activity
            github_query = text("""
                SELECT
                    org_name,
                    total_stars,
                    total_forks,
                    total_contributors,
                    velocity_score
                FROM github_org_metrics
                WHERE fetched_at >= NOW() - INTERVAL '7 days'
                AND velocity_score > 70
                ORDER BY velocity_score DESC
                LIMIT 10
            """)

            result = self.db.execute(github_query).fetchall()

            if len(result) >= 3:
                signal = MarketSignal(
                    signal_id=f"sig_{uuid.uuid4().hex[:12]}",
                    signal_type=SignalType.ACTIVITY_SPIKE.value,
                    category="developer_activity",
                    direction=SignalDirection.ACCELERATING.value,
                    strength=0.7,
                    confidence=0.8,
                    description=f"{len(result)} organizations with high developer velocity (70+ score)",
                    data_points=[{
                        "org": r[0],
                        "stars": r[1],
                        "velocity": r[4]
                    } for r in result[:5]]
                )
                signals.append(signal)

        except Exception as e:
            logger.warning(f"Error scanning activity spikes: {e}")
            self.db.rollback()

        # App Store signals
        try:
            app_query = text("""
                SELECT
                    app_name,
                    developer_name,
                    avg_rating,
                    rating_count
                FROM app_store_apps
                WHERE avg_rating >= 4.5
                AND rating_count >= 1000
                ORDER BY rating_count DESC
                LIMIT 10
            """)

            result = self.db.execute(app_query).fetchall()

            if len(result) >= 3:
                signal = MarketSignal(
                    signal_id=f"sig_{uuid.uuid4().hex[:12]}",
                    signal_type=SignalType.ACTIVITY_SPIKE.value,
                    category="consumer_apps",
                    direction=SignalDirection.STABLE.value,
                    strength=0.6,
                    confidence=0.7,
                    description=f"{len(result)} highly-rated apps with strong user engagement",
                    data_points=[{
                        "app": r[0],
                        "developer": r[1],
                        "rating": r[2],
                        "count": r[3]
                    } for r in result[:5]]
                )
                signals.append(signal)

        except Exception as e:
            logger.warning(f"Error scanning app activity: {e}")
            self.db.rollback()

        return signals

    # -------------------------------------------------------------------------
    # Opportunity Detection
    # -------------------------------------------------------------------------

    def _identify_sector_rotations(self, signals: List[Dict]) -> List[Dict]:
        """Identify sector rotation opportunities."""
        opportunities = []

        # Find sectors with declining attention but stable fundamentals
        sector_signals = defaultdict(list)
        for s in signals:
            if s.get("category"):
                sector_signals[s["category"]].append(s)

        for sector, sigs in sector_signals.items():
            accelerating = [s for s in sigs if s.get("direction") == "accelerating"]
            decelerating = [s for s in sigs if s.get("direction") == "decelerating"]

            # Rotation signal: mixed momentum
            if accelerating and decelerating:
                opp = {
                    "opportunity_id": f"opp_{uuid.uuid4().hex[:8]}",
                    "type": "sector_rotation",
                    "title": f"{sector.replace('_', ' ').title()} Rotation",
                    "thesis": f"Mixed signals in {sector} - potential rotation opportunity",
                    "confidence": 0.5,
                    "signals": [s.get("signal_id") for s in sigs[:3]],
                    "recommended_actions": [
                        f"Monitor {sector} Form D filings",
                        "Track company score changes",
                        "Watch for funding announcements"
                    ]
                }
                opportunities.append(opp)

        return opportunities

    def _identify_contrarian_opportunities(self, signals: List[Dict]) -> List[Dict]:
        """Find contrarian opportunities in declining areas."""
        opportunities = []

        declining = [s for s in signals if s.get("direction") == "decelerating" and s.get("strength", 0) > 0.3]

        for s in declining[:3]:
            opp = {
                "opportunity_id": f"opp_{uuid.uuid4().hex[:8]}",
                "type": "contrarian",
                "title": f"Contrarian: {s.get('category', 'Unknown')}",
                "thesis": "Declining attention may present value opportunity",
                "confidence": 0.4,
                "signals": [s.get("signal_id")],
                "recommended_actions": [
                    "Investigate fundamentals",
                    "Check for structural vs cyclical decline",
                    "Monitor for reversal signals"
                ]
            }
            opportunities.append(opp)

        return opportunities

    def _identify_momentum_opportunities(self, signals: List[Dict]) -> List[Dict]:
        """Find momentum opportunities in accelerating areas."""
        opportunities = []

        accelerating = [s for s in signals if s.get("direction") == "accelerating" and s.get("strength", 0) > 0.5]

        for s in accelerating[:5]:
            opp = {
                "opportunity_id": f"opp_{uuid.uuid4().hex[:8]}",
                "type": "momentum",
                "title": f"Momentum: {s.get('category', 'Unknown')}",
                "thesis": f"Strong momentum with {int(s.get('strength', 0)*100)}% strength",
                "confidence": min(s.get("strength", 0) + 0.2, 0.9),
                "signals": [s.get("signal_id")],
                "recommended_actions": [
                    "Increase monitoring frequency",
                    "Identify leading companies",
                    "Watch for overheating signals"
                ]
            }
            opportunities.append(opp)

        return opportunities

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------

    def _map_industry_to_category(self, industry: str) -> str:
        """Map industry name to category."""
        if not industry:
            return "other"

        industry_lower = industry.lower()

        for category, keywords in SECTOR_CATEGORIES.items():
            if any(kw in industry_lower for kw in keywords):
                return category

        return "other"

    def _signal_to_dict(self, signal: MarketSignal) -> Dict[str, Any]:
        """Convert signal to dictionary."""
        return {
            "signal_id": signal.signal_id,
            "signal_type": signal.signal_type,
            "category": signal.category,
            "direction": signal.direction,
            "strength": signal.strength,
            "confidence": signal.confidence,
            "description": signal.description,
            "data_points": signal.data_points,
            "trend": signal.trend
        }

    def _signal_summary(self, signal: Dict) -> Dict:
        """Create brief signal summary."""
        return {
            "signal_id": signal.get("signal_id"),
            "type": signal.get("signal_type"),
            "category": signal.get("category"),
            "strength": signal.get("strength"),
            "description": signal.get("description", "")[:200]
        }

    def _group_signals_by_type(self, signals: List[MarketSignal]) -> Dict[str, int]:
        """Group signals by type."""
        groups = defaultdict(int)
        for s in signals:
            groups[s.signal_type] += 1
        return dict(groups)

    def _group_signals_by_category(self, signals: List[MarketSignal]) -> Dict[str, int]:
        """Group signals by category."""
        groups = defaultdict(int)
        for s in signals:
            if s.category:
                groups[s.category] += 1
        return dict(groups)

    def _generate_trend_description(self, sector: str, signals: List, strength: float) -> str:
        """Generate description for a trend."""
        direction = "accelerating" if strength > 0.5 else "stable"
        return f"{sector.replace('_', ' ').title()} showing {direction} activity with {len(signals)} supporting signals"

    def _generate_brief_summary(self, signals: List, period_type: str) -> str:
        """Generate brief summary text."""
        total = len(signals)
        accelerating = len([s for s in signals if s.get("direction") == "accelerating"])
        decelerating = len([s for s in signals if s.get("direction") == "decelerating"])

        sentiment = "bullish" if accelerating > decelerating else "cautious" if decelerating > accelerating else "mixed"

        return f"Market scan detected {total} signals this {period_type.replace('ly', '')}. " \
               f"{accelerating} accelerating vs {decelerating} decelerating signals suggest {sentiment} sentiment. " \
               f"Key themes include sector momentum shifts and geographic activity changes."

    def _build_sector_spotlight(self, signals: List[Dict]) -> Dict:
        """Build sector spotlight section."""
        sector_signals = defaultdict(list)
        for s in signals:
            if s.get("category"):
                sector_signals[s["category"]].append(s)

        if not sector_signals:
            return {"sector": "none", "signals": []}

        # Find hottest sector
        hottest = max(sector_signals.items(), key=lambda x: len(x[1]))

        return {
            "sector": hottest[0],
            "signal_count": len(hottest[1]),
            "avg_strength": round(sum(s.get("strength", 0) for s in hottest[1]) / len(hottest[1]), 2),
            "key_signals": [self._signal_summary(s) for s in hottest[1][:3]]
        }

    # -------------------------------------------------------------------------
    # Database Operations
    # -------------------------------------------------------------------------

    def _create_scan_record(self, scan_id: str, scan_type: str) -> None:
        """Create a new scan record."""
        try:
            query = text("""
                INSERT INTO market_scans (scan_id, scan_type, status)
                VALUES (:scan_id, :scan_type, 'running')
            """)
            self.db.execute(query, {"scan_id": scan_id, "scan_type": scan_type})
            self.db.commit()
        except Exception as e:
            logger.error(f"Error creating scan record: {e}")
            self.db.rollback()

    def _complete_scan(self, scan_id: str, sources: List[str], signal_count: int, results: Dict) -> None:
        """Mark scan as complete."""
        try:
            import json
            query = text("""
                UPDATE market_scans
                SET status = 'completed',
                    completed_at = NOW(),
                    sources_scanned = :sources,
                    signals_detected = :count,
                    results = :results
                WHERE scan_id = :scan_id
            """)
            self.db.execute(query, {
                "scan_id": scan_id,
                "sources": json.dumps(sources),
                "count": signal_count,
                "results": json.dumps(results)
            })
            self.db.commit()
        except Exception as e:
            logger.error(f"Error completing scan: {e}")
            self.db.rollback()

    def _fail_scan(self, scan_id: str, error: str) -> None:
        """Mark scan as failed."""
        try:
            query = text("""
                UPDATE market_scans
                SET status = 'failed', error_message = :error
                WHERE scan_id = :scan_id
            """)
            self.db.execute(query, {"scan_id": scan_id, "error": error})
            self.db.commit()
        except Exception as e:
            logger.error(f"Error failing scan: {e}")
            self.db.rollback()

    def _save_signal(self, signal: MarketSignal, scan_id: str) -> None:
        """Save a signal to the database."""
        try:
            import json
            query = text("""
                INSERT INTO market_signals
                (signal_id, signal_type, category, direction, strength, confidence, description, data_points, first_detected, scan_id)
                VALUES (:signal_id, :signal_type, :category, :direction, :strength, :confidence, :description, :data_points, NOW(), :scan_id)
                ON CONFLICT (signal_id) DO UPDATE SET
                    strength = EXCLUDED.strength,
                    direction = EXCLUDED.direction,
                    last_updated = NOW()
            """)
            self.db.execute(query, {
                "signal_id": signal.signal_id,
                "signal_type": signal.signal_type,
                "category": signal.category,
                "direction": signal.direction,
                "strength": signal.strength,
                "confidence": signal.confidence,
                "description": signal.description,
                "data_points": json.dumps(signal.data_points),
                "scan_id": scan_id
            })
            self.db.commit()
        except Exception as e:
            logger.error(f"Error saving signal: {e}")
            self.db.rollback()

    def _save_brief(self, brief: Dict) -> None:
        """Save a brief to the database."""
        try:
            import json
            query = text("""
                INSERT INTO market_briefs
                (brief_id, period_start, period_end, brief_type, summary, sections, signals_included)
                VALUES (:brief_id, :period_start, :period_end, :brief_type, :summary, :sections, :signals_included)
                ON CONFLICT (brief_id) DO UPDATE SET
                    summary = EXCLUDED.summary,
                    sections = EXCLUDED.sections,
                    generated_at = NOW()
            """)
            self.db.execute(query, {
                "brief_id": brief["brief_id"],
                "period_start": brief["period"]["start"],
                "period_end": brief["period"]["end"],
                "brief_type": brief["brief_type"],
                "summary": brief["summary"],
                "sections": json.dumps(brief["sections"]),
                "signals_included": json.dumps([])
            })
            self.db.commit()
        except Exception as e:
            logger.error(f"Error saving brief: {e}")
            self.db.rollback()

    def _get_recent_scan(self) -> Optional[Dict]:
        """Get most recent completed scan."""
        try:
            query = text("""
                SELECT scan_id, completed_at, results
                FROM market_scans
                WHERE status = 'completed'
                AND completed_at >= NOW() - INTERVAL '1 hour'
                ORDER BY completed_at DESC
                LIMIT 1
            """)
            result = self.db.execute(query).fetchone()

            if result:
                return {
                    "scan_id": result[0],
                    "completed_at": result[1].isoformat() if result[1] else None,
                    "results": result[2]
                }
            return None
        except Exception as e:
            logger.error(f"Error getting recent scan: {e}")
            self.db.rollback()
            return None

    def _get_signals_for_period(self, days: int) -> List[Dict]:
        """Get signals for a time period."""
        try:
            query = text("""
                SELECT signal_id, signal_type, category, direction, strength, confidence, description, data_points
                FROM market_signals
                WHERE first_detected >= NOW() - INTERVAL ':days days'
                OR last_updated >= NOW() - INTERVAL ':days days'
                ORDER BY strength DESC
            """.replace(":days", str(days)))

            result = self.db.execute(query).fetchall()

            return [{
                "signal_id": r[0],
                "signal_type": r[1],
                "category": r[2],
                "direction": r[3],
                "strength": r[4],
                "confidence": r[5],
                "description": r[6],
                "data_points": r[7]
            } for r in result]
        except Exception as e:
            logger.warning(f"Error getting signals: {e}")
            self.db.rollback()
            return []

    def _get_existing_brief(self, brief_id: str) -> Optional[Dict]:
        """Check for existing brief."""
        try:
            query = text("""
                SELECT brief_id, period_start, period_end, brief_type, summary, sections, generated_at
                FROM market_briefs
                WHERE brief_id = :brief_id
            """)
            result = self.db.execute(query, {"brief_id": brief_id}).fetchone()

            if result:
                return {
                    "brief_id": result[0],
                    "period": {"start": str(result[1]), "end": str(result[2])},
                    "brief_type": result[3],
                    "summary": result[4],
                    "sections": result[5],
                    "generated_at": result[6].isoformat() if result[6] else None,
                    "cached": True
                }
            return None
        except Exception as e:
            logger.error(f"Error getting brief: {e}")
            self.db.rollback()
            return None

    def _get_recent_scans(self, limit: int) -> List[Dict]:
        """Get recent scans."""
        try:
            query = text("""
                SELECT scan_id, scan_type, started_at, completed_at, status, signals_detected
                FROM market_scans
                ORDER BY started_at DESC
                LIMIT :limit
            """)
            result = self.db.execute(query, {"limit": limit}).fetchall()

            return [{
                "scan_id": r[0],
                "scan_type": r[1],
                "started_at": r[2].isoformat() if r[2] else None,
                "completed_at": r[3].isoformat() if r[3] else None,
                "status": r[4],
                "signals_detected": r[5]
            } for r in result]
        except Exception as e:
            logger.error(f"Error getting scans: {e}")
            self.db.rollback()
            return []

    def _get_recent_briefs(self, limit: int) -> List[Dict]:
        """Get recent briefs."""
        try:
            query = text("""
                SELECT brief_id, period_start, period_end, brief_type, generated_at
                FROM market_briefs
                ORDER BY generated_at DESC
                LIMIT :limit
            """)
            result = self.db.execute(query, {"limit": limit}).fetchall()

            return [{
                "brief_id": r[0],
                "period_start": str(r[1]),
                "period_end": str(r[2]),
                "brief_type": r[3],
                "generated_at": r[4].isoformat() if r[4] else None
            } for r in result]
        except Exception as e:
            logger.error(f"Error getting briefs: {e}")
            self.db.rollback()
            return []
