"""
Agentic News Monitor (T43)

AI-powered news monitoring that tracks watched entities,
scores relevance/impact, generates digests, and alerts on breaking news.
"""

import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# =============================================================================
# CONSTANTS
# =============================================================================

class WatchType(str, Enum):
    COMPANY = "company"
    INVESTOR = "investor"
    SECTOR = "sector"
    KEYWORD = "keyword"


class EventType(str, Enum):
    FILING = "filing"
    FUNDING = "funding"
    ACQUISITION = "acquisition"
    IPO = "ipo"
    PARTNERSHIP = "partnership"
    LEADERSHIP = "leadership"
    REGULATORY = "regulatory"
    PRODUCT = "product"
    NEWS = "news"


# Impact scores by event type
EVENT_IMPACT_SCORES = {
    EventType.ACQUISITION: 0.9,
    EventType.FUNDING: 0.85,
    EventType.IPO: 0.9,
    EventType.REGULATORY: 0.85,
    EventType.LEADERSHIP: 0.7,
    EventType.PARTNERSHIP: 0.6,
    EventType.PRODUCT: 0.5,
    EventType.FILING: 0.4,
    EventType.NEWS: 0.3,
}

# Keywords for event type detection
EVENT_KEYWORDS = {
    EventType.FUNDING: ["raises", "funding", "series", "investment", "valuation", "round"],
    EventType.ACQUISITION: ["acquires", "acquisition", "merger", "buys", "purchased", "deal"],
    EventType.IPO: ["ipo", "public", "listing", "nasdaq", "nyse", "goes public"],
    EventType.REGULATORY: ["sec", "charges", "fine", "investigation", "compliance", "enforcement"],
    EventType.LEADERSHIP: ["ceo", "cfo", "appoints", "resigns", "executive", "board"],
    EventType.PARTNERSHIP: ["partnership", "partners", "collaboration", "alliance", "joint venture"],
    EventType.PRODUCT: ["launches", "announces", "release", "product", "feature", "update"],
    EventType.FILING: ["13f", "13d", "10-k", "10-q", "8-k", "form d", "filing"],
}

# Sentiment keywords
POSITIVE_KEYWORDS = [
    "growth", "profit", "success", "wins", "award", "expansion", "record",
    "breakthrough", "innovative", "leading", "best", "exceeds", "outperforms"
]
NEGATIVE_KEYWORDS = [
    "loss", "decline", "layoffs", "lawsuit", "fraud", "investigation", "fails",
    "bankruptcy", "downturn", "crisis", "scandal", "breach", "default"
]

# Breaking news thresholds
BREAKING_IMPACT_THRESHOLD = 0.8
BREAKING_RELEVANCE_THRESHOLD = 0.7


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class WatchItem:
    """A watch list item."""
    id: int
    watch_type: str
    watch_value: str
    event_types: List[str] = field(default_factory=list)
    min_relevance: float = 0.5
    alert_enabled: bool = True
    digest_enabled: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class NewsMatch:
    """A news item matched to a watch item."""
    id: int
    news_title: str
    news_url: Optional[str]
    news_source: Optional[str]
    news_published_at: Optional[datetime]
    watch_item_id: int
    watch_type: str
    watch_value: str
    match_type: str  # exact, related, keyword
    relevance_score: float
    impact_score: float
    sentiment: float
    event_type: Optional[str]
    summary: Optional[str]
    is_breaking: bool = False
    is_alerted: bool = False
    is_read: bool = False
    created_at: Optional[datetime] = None


@dataclass
class Digest:
    """A generated news digest."""
    id: int
    period_type: str
    period_start: datetime
    period_end: datetime
    summary: str
    highlights: List[Dict]
    stats: Dict
    generated_at: datetime


# =============================================================================
# NEWS MONITOR CLASS
# =============================================================================

class NewsMonitor:
    """
    Agentic news monitoring service.

    Tracks watched entities, matches news, scores relevance/impact,
    generates digests, and identifies breaking news alerts.
    """

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self):
        """Create tables if they don't exist."""
        create_watch_items = text("""
            CREATE TABLE IF NOT EXISTS news_watch_items (
                id SERIAL PRIMARY KEY,
                watch_type VARCHAR(20) NOT NULL,
                watch_value VARCHAR(255) NOT NULL,
                event_types TEXT[],
                min_relevance FLOAT DEFAULT 0.5,
                alert_enabled BOOLEAN DEFAULT TRUE,
                digest_enabled BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(watch_type, watch_value)
            )
        """)

        create_matches = text("""
            CREATE TABLE IF NOT EXISTS news_matches (
                id SERIAL PRIMARY KEY,
                news_title TEXT NOT NULL,
                news_url TEXT,
                news_source VARCHAR(50),
                news_published_at TIMESTAMP,
                watch_item_id INTEGER REFERENCES news_watch_items(id) ON DELETE CASCADE,
                watch_type VARCHAR(20),
                watch_value VARCHAR(255),
                match_type VARCHAR(20),
                relevance_score FLOAT,
                impact_score FLOAT,
                sentiment FLOAT,
                event_type VARCHAR(50),
                summary TEXT,
                is_breaking BOOLEAN DEFAULT FALSE,
                is_alerted BOOLEAN DEFAULT FALSE,
                is_read BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        create_digests = text("""
            CREATE TABLE IF NOT EXISTS news_digests (
                id SERIAL PRIMARY KEY,
                period_type VARCHAR(20) NOT NULL,
                period_start TIMESTAMP NOT NULL,
                period_end TIMESTAMP NOT NULL,
                summary TEXT,
                highlights JSONB,
                stats JSONB,
                generated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(period_type, period_start)
            )
        """)

        # Create indexes
        create_indexes = text("""
            CREATE INDEX IF NOT EXISTS idx_news_matches_watch ON news_matches(watch_item_id);
            CREATE INDEX IF NOT EXISTS idx_news_matches_breaking ON news_matches(is_breaking, is_alerted);
            CREATE INDEX IF NOT EXISTS idx_news_matches_created ON news_matches(created_at DESC);
        """)

        try:
            self.db.execute(create_watch_items)
            self.db.execute(create_matches)
            self.db.execute(create_digests)
            self.db.execute(create_indexes)
            self.db.commit()
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    # -------------------------------------------------------------------------
    # WATCH LIST MANAGEMENT
    # -------------------------------------------------------------------------

    def add_watch(
        self,
        watch_type: str,
        watch_value: str,
        event_types: Optional[List[str]] = None,
        min_relevance: float = 0.5,
        alert_enabled: bool = True,
        digest_enabled: bool = True,
    ) -> WatchItem:
        """Add an item to the watch list."""
        # Validate watch type
        if watch_type not in [wt.value for wt in WatchType]:
            raise ValueError(f"Invalid watch_type: {watch_type}")

        # Check for existing
        existing = self._get_watch_by_value(watch_type, watch_value)
        if existing:
            raise ValueError(f"Watch item already exists: {watch_type}/{watch_value}")

        query = text("""
            INSERT INTO news_watch_items
            (watch_type, watch_value, event_types, min_relevance, alert_enabled, digest_enabled)
            VALUES (:watch_type, :watch_value, :event_types, :min_relevance, :alert_enabled, :digest_enabled)
            RETURNING id, created_at, updated_at
        """)

        result = self.db.execute(query, {
            "watch_type": watch_type,
            "watch_value": watch_value,
            "event_types": event_types or [],
            "min_relevance": min_relevance,
            "alert_enabled": alert_enabled,
            "digest_enabled": digest_enabled,
        })
        row = result.fetchone()
        self.db.commit()

        return WatchItem(
            id=row[0],
            watch_type=watch_type,
            watch_value=watch_value,
            event_types=event_types or [],
            min_relevance=min_relevance,
            alert_enabled=alert_enabled,
            digest_enabled=digest_enabled,
            created_at=row[1],
            updated_at=row[2],
        )

    def _get_watch_by_value(self, watch_type: str, watch_value: str) -> Optional[WatchItem]:
        """Get watch item by type and value."""
        query = text("""
            SELECT * FROM news_watch_items
            WHERE watch_type = :watch_type AND LOWER(watch_value) = LOWER(:watch_value)
        """)
        result = self.db.execute(query, {"watch_type": watch_type, "watch_value": watch_value})
        row = result.mappings().fetchone()
        if row:
            return WatchItem(
                id=row["id"],
                watch_type=row["watch_type"],
                watch_value=row["watch_value"],
                event_types=row["event_types"] or [],
                min_relevance=row["min_relevance"],
                alert_enabled=row["alert_enabled"],
                digest_enabled=row["digest_enabled"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
        return None

    def get_watch(self, watch_id: int) -> Optional[WatchItem]:
        """Get watch item by ID."""
        query = text("SELECT * FROM news_watch_items WHERE id = :id")
        result = self.db.execute(query, {"id": watch_id})
        row = result.mappings().fetchone()
        if row:
            return WatchItem(
                id=row["id"],
                watch_type=row["watch_type"],
                watch_value=row["watch_value"],
                event_types=row["event_types"] or [],
                min_relevance=row["min_relevance"],
                alert_enabled=row["alert_enabled"],
                digest_enabled=row["digest_enabled"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            )
        return None

    def list_watches(self) -> List[WatchItem]:
        """List all watch items."""
        query = text("SELECT * FROM news_watch_items ORDER BY created_at DESC")
        result = self.db.execute(query)
        watches = []
        for row in result.mappings():
            watches.append(WatchItem(
                id=row["id"],
                watch_type=row["watch_type"],
                watch_value=row["watch_value"],
                event_types=row["event_types"] or [],
                min_relevance=row["min_relevance"],
                alert_enabled=row["alert_enabled"],
                digest_enabled=row["digest_enabled"],
                created_at=row["created_at"],
                updated_at=row["updated_at"],
            ))
        return watches

    def update_watch(
        self,
        watch_id: int,
        event_types: Optional[List[str]] = None,
        min_relevance: Optional[float] = None,
        alert_enabled: Optional[bool] = None,
        digest_enabled: Optional[bool] = None,
    ) -> Optional[WatchItem]:
        """Update a watch item."""
        updates = []
        params = {"id": watch_id}

        if event_types is not None:
            updates.append("event_types = :event_types")
            params["event_types"] = event_types
        if min_relevance is not None:
            updates.append("min_relevance = :min_relevance")
            params["min_relevance"] = min_relevance
        if alert_enabled is not None:
            updates.append("alert_enabled = :alert_enabled")
            params["alert_enabled"] = alert_enabled
        if digest_enabled is not None:
            updates.append("digest_enabled = :digest_enabled")
            params["digest_enabled"] = digest_enabled

        if not updates:
            return self.get_watch(watch_id)

        updates.append("updated_at = CURRENT_TIMESTAMP")
        query = text(f"UPDATE news_watch_items SET {', '.join(updates)} WHERE id = :id")
        self.db.execute(query, params)
        self.db.commit()

        return self.get_watch(watch_id)

    def remove_watch(self, watch_id: int) -> bool:
        """Remove a watch item."""
        query = text("DELETE FROM news_watch_items WHERE id = :id")
        result = self.db.execute(query, {"id": watch_id})
        self.db.commit()
        return result.rowcount > 0

    # -------------------------------------------------------------------------
    # NEWS SCORING
    # -------------------------------------------------------------------------

    def _detect_event_type(self, title: str, body: Optional[str] = None) -> str:
        """Detect event type from news text."""
        text_to_check = (title + " " + (body or "")).lower()

        for event_type, keywords in EVENT_KEYWORDS.items():
            for keyword in keywords:
                if keyword in text_to_check:
                    return event_type.value

        return EventType.NEWS.value

    def _score_relevance(
        self,
        title: str,
        body: Optional[str],
        watch: WatchItem,
    ) -> Tuple[float, str]:
        """
        Score how relevant a news item is to a watch item.

        Returns (score, match_type).
        """
        watch_value_lower = watch.watch_value.lower()
        title_lower = title.lower()
        body_lower = (body or "").lower()

        # Exact match in title
        if watch_value_lower in title_lower:
            return 0.95, "exact"

        # Exact match in body
        if watch_value_lower in body_lower:
            return 0.75, "exact"

        # For keywords, check word boundaries
        if watch.watch_type == WatchType.KEYWORD.value:
            pattern = r'\b' + re.escape(watch_value_lower) + r'\b'
            if re.search(pattern, title_lower):
                return 0.9, "keyword"
            if re.search(pattern, body_lower):
                return 0.7, "keyword"

        # For sectors, check related terms
        if watch.watch_type == WatchType.SECTOR.value:
            sector_terms = self._get_sector_terms(watch.watch_value)
            for term in sector_terms:
                if term.lower() in title_lower:
                    return 0.8, "related"
                if term.lower() in body_lower:
                    return 0.6, "related"

        # Partial match (substring)
        if len(watch_value_lower) > 3:
            words = watch_value_lower.split()
            matches = sum(1 for w in words if w in title_lower or w in body_lower)
            if matches > 0:
                score = 0.4 + (0.2 * matches / len(words))
                return min(score, 0.7), "related"

        return 0.0, "none"

    def _get_sector_terms(self, sector: str) -> List[str]:
        """Get related terms for a sector."""
        sector_map = {
            "fintech": ["payments", "banking", "finance", "lending", "crypto", "blockchain"],
            "healthtech": ["healthcare", "medical", "health", "biotech", "pharma", "telemedicine"],
            "ai": ["artificial intelligence", "machine learning", "ml", "deep learning", "llm"],
            "saas": ["software", "cloud", "subscription", "enterprise"],
            "ecommerce": ["retail", "shopping", "marketplace", "commerce"],
            "climate": ["cleantech", "sustainability", "renewable", "green", "carbon"],
        }
        return sector_map.get(sector.lower(), [sector])

    def _score_impact(self, title: str, event_type: str) -> float:
        """Score the business impact of a news item."""
        base_score = EVENT_IMPACT_SCORES.get(
            EventType(event_type) if event_type in [e.value for e in EventType] else EventType.NEWS,
            0.3
        )

        # Boost for specific high-impact keywords
        title_lower = title.lower()
        if any(word in title_lower for word in ["billion", "major", "breaking", "urgent"]):
            base_score = min(base_score + 0.1, 1.0)

        return base_score

    def _analyze_sentiment(self, title: str, body: Optional[str] = None) -> float:
        """
        Analyze sentiment of news text.

        Returns -1 (negative) to 1 (positive).
        """
        text_to_check = (title + " " + (body or "")).lower()

        positive_count = sum(1 for word in POSITIVE_KEYWORDS if word in text_to_check)
        negative_count = sum(1 for word in NEGATIVE_KEYWORDS if word in text_to_check)

        total = positive_count + negative_count
        if total == 0:
            return 0.0  # Neutral

        return (positive_count - negative_count) / total

    def _generate_summary(self, title: str, event_type: str) -> str:
        """Generate a brief summary of the news item."""
        # Simple summary based on event type
        event_summaries = {
            EventType.FUNDING.value: "Funding announcement",
            EventType.ACQUISITION.value: "M&A activity",
            EventType.IPO.value: "IPO/public listing",
            EventType.REGULATORY.value: "Regulatory update",
            EventType.LEADERSHIP.value: "Leadership change",
            EventType.PARTNERSHIP.value: "Partnership announcement",
            EventType.PRODUCT.value: "Product news",
            EventType.FILING.value: "SEC filing",
        }
        prefix = event_summaries.get(event_type, "News")
        return f"{prefix}: {title[:100]}..." if len(title) > 100 else f"{prefix}: {title}"

    # -------------------------------------------------------------------------
    # NEWS MATCHING
    # -------------------------------------------------------------------------

    def match_news_item(
        self,
        title: str,
        url: Optional[str] = None,
        source: Optional[str] = None,
        published_at: Optional[datetime] = None,
        body: Optional[str] = None,
    ) -> List[NewsMatch]:
        """
        Match a single news item against all watch items.

        Returns list of matches that meet relevance threshold.
        """
        watches = self.list_watches()
        matches = []

        event_type = self._detect_event_type(title, body)
        impact_score = self._score_impact(title, event_type)
        sentiment = self._analyze_sentiment(title, body)

        for watch in watches:
            # Filter by event type if specified
            if watch.event_types and event_type not in watch.event_types:
                continue

            relevance_score, match_type = self._score_relevance(title, body, watch)

            # Skip if below threshold
            if relevance_score < watch.min_relevance:
                continue

            # Check if breaking
            is_breaking = (
                relevance_score >= BREAKING_RELEVANCE_THRESHOLD and
                impact_score >= BREAKING_IMPACT_THRESHOLD and
                watch.alert_enabled
            )

            summary = self._generate_summary(title, event_type)

            # Store the match
            match = self._store_match(
                title=title,
                url=url,
                source=source,
                published_at=published_at,
                watch=watch,
                match_type=match_type,
                relevance_score=relevance_score,
                impact_score=impact_score,
                sentiment=sentiment,
                event_type=event_type,
                summary=summary,
                is_breaking=is_breaking,
            )
            if match:
                matches.append(match)

        return matches

    def _store_match(
        self,
        title: str,
        url: Optional[str],
        source: Optional[str],
        published_at: Optional[datetime],
        watch: WatchItem,
        match_type: str,
        relevance_score: float,
        impact_score: float,
        sentiment: float,
        event_type: str,
        summary: str,
        is_breaking: bool,
    ) -> Optional[NewsMatch]:
        """Store a news match in the database."""
        # Check for duplicate (same title + watch)
        check_query = text("""
            SELECT id FROM news_matches
            WHERE news_title = :title AND watch_item_id = :watch_id
        """)
        existing = self.db.execute(check_query, {
            "title": title,
            "watch_id": watch.id
        }).fetchone()

        if existing:
            return None  # Already matched

        query = text("""
            INSERT INTO news_matches
            (news_title, news_url, news_source, news_published_at, watch_item_id,
             watch_type, watch_value, match_type, relevance_score, impact_score,
             sentiment, event_type, summary, is_breaking)
            VALUES
            (:title, :url, :source, :published_at, :watch_id,
             :watch_type, :watch_value, :match_type, :relevance, :impact,
             :sentiment, :event_type, :summary, :is_breaking)
            RETURNING id, created_at
        """)

        result = self.db.execute(query, {
            "title": title,
            "url": url,
            "source": source,
            "published_at": published_at,
            "watch_id": watch.id,
            "watch_type": watch.watch_type,
            "watch_value": watch.watch_value,
            "match_type": match_type,
            "relevance": relevance_score,
            "impact": impact_score,
            "sentiment": sentiment,
            "event_type": event_type,
            "summary": summary,
            "is_breaking": is_breaking,
        })
        row = result.fetchone()
        self.db.commit()

        return NewsMatch(
            id=row[0],
            news_title=title,
            news_url=url,
            news_source=source,
            news_published_at=published_at,
            watch_item_id=watch.id,
            watch_type=watch.watch_type,
            watch_value=watch.watch_value,
            match_type=match_type,
            relevance_score=relevance_score,
            impact_score=impact_score,
            sentiment=sentiment,
            event_type=event_type,
            summary=summary,
            is_breaking=is_breaking,
            created_at=row[1],
        )

    def process_news_feed(self, news_items: List[Dict]) -> Dict[str, Any]:
        """
        Process a batch of news items, matching against watches.

        Args:
            news_items: List of dicts with keys: title, url, source, published_at, body

        Returns:
            Summary of matches found.
        """
        total_matches = 0
        breaking_count = 0

        for item in news_items:
            matches = self.match_news_item(
                title=item.get("title", ""),
                url=item.get("url"),
                source=item.get("source"),
                published_at=item.get("published_at"),
                body=item.get("body") or item.get("description"),
            )
            total_matches += len(matches)
            breaking_count += sum(1 for m in matches if m.is_breaking)

        return {
            "processed": len(news_items),
            "matches": total_matches,
            "breaking": breaking_count,
        }

    # -------------------------------------------------------------------------
    # PERSONALIZED FEED
    # -------------------------------------------------------------------------

    def get_personalized_feed(
        self,
        days: int = 7,
        min_relevance: float = 0.5,
        event_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """Get news matched to watch list, sorted by relevance."""
        params = {
            "days": days,
            "min_relevance": min_relevance,
            "limit": limit,
            "offset": offset,
        }

        where_clauses = [
            "created_at > NOW() - INTERVAL ':days days'",
            "relevance_score >= :min_relevance",
        ]

        if event_type:
            where_clauses.append("event_type = :event_type")
            params["event_type"] = event_type

        where_sql = " AND ".join(where_clauses)

        # Get items
        query = text(f"""
            SELECT * FROM news_matches
            WHERE {where_sql}
            ORDER BY relevance_score DESC, created_at DESC
            LIMIT :limit OFFSET :offset
        """)

        result = self.db.execute(query, params)
        items = []
        for row in result.mappings():
            items.append({
                "id": row["id"],
                "title": row["news_title"],
                "url": row["news_url"],
                "source": row["news_source"],
                "published_at": row["news_published_at"].isoformat() if row["news_published_at"] else None,
                "matched_watch": {
                    "id": row["watch_item_id"],
                    "type": row["watch_type"],
                    "value": row["watch_value"],
                },
                "relevance_score": row["relevance_score"],
                "impact_score": row["impact_score"],
                "sentiment": row["sentiment"],
                "event_type": row["event_type"],
                "summary": row["summary"],
                "is_breaking": row["is_breaking"],
                "is_read": row["is_read"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            })

        # Get counts
        count_query = text(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN is_read = FALSE THEN 1 ELSE 0 END) as unread
            FROM news_matches
            WHERE {where_sql}
        """)
        counts = self.db.execute(count_query, params).fetchone()

        return {
            "items": items,
            "total": counts[0] or 0,
            "unread": counts[1] or 0,
        }

    def mark_as_read(self, match_ids: List[int]) -> int:
        """Mark news matches as read."""
        if not match_ids:
            return 0
        query = text("""
            UPDATE news_matches SET is_read = TRUE
            WHERE id = ANY(:ids)
        """)
        result = self.db.execute(query, {"ids": match_ids})
        self.db.commit()
        return result.rowcount

    # -------------------------------------------------------------------------
    # BREAKING ALERTS
    # -------------------------------------------------------------------------

    def get_breaking_alerts(
        self,
        acknowledged: Optional[bool] = None,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """Get breaking/high-impact news alerts."""
        params = {"limit": limit}
        where_clauses = ["is_breaking = TRUE"]

        if acknowledged is not None:
            where_clauses.append("is_alerted = :acknowledged")
            params["acknowledged"] = acknowledged

        where_sql = " AND ".join(where_clauses)

        query = text(f"""
            SELECT * FROM news_matches
            WHERE {where_sql}
            ORDER BY created_at DESC
            LIMIT :limit
        """)

        result = self.db.execute(query, params)
        alerts = []
        for row in result.mappings():
            alerts.append({
                "id": row["id"],
                "title": row["news_title"],
                "url": row["news_url"],
                "impact_score": row["impact_score"],
                "event_type": row["event_type"],
                "matched_watch": {
                    "type": row["watch_type"],
                    "value": row["watch_value"],
                },
                "summary": row["summary"],
                "published_at": row["news_published_at"].isoformat() if row["news_published_at"] else None,
                "acknowledged": row["is_alerted"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            })

        # Count unacknowledged
        count_query = text("""
            SELECT COUNT(*) FROM news_matches WHERE is_breaking = TRUE AND is_alerted = FALSE
        """)
        unack = self.db.execute(count_query).scalar() or 0

        return {
            "alerts": alerts,
            "unacknowledged": unack,
        }

    def acknowledge_alert(self, match_id: int) -> bool:
        """Mark an alert as acknowledged."""
        query = text("""
            UPDATE news_matches SET is_alerted = TRUE
            WHERE id = :id AND is_breaking = TRUE
        """)
        result = self.db.execute(query, {"id": match_id})
        self.db.commit()
        return result.rowcount > 0

    # -------------------------------------------------------------------------
    # DIGEST GENERATION
    # -------------------------------------------------------------------------

    def generate_digest(
        self,
        period_type: str = "daily",
        target_date: Optional[date] = None,
    ) -> Digest:
        """Generate a news digest for a period."""
        if target_date is None:
            target_date = date.today()

        if period_type == "daily":
            period_start = datetime.combine(target_date, datetime.min.time())
            period_end = period_start + timedelta(days=1)
        elif period_type == "weekly":
            # Start of week (Monday)
            days_since_monday = target_date.weekday()
            week_start = target_date - timedelta(days=days_since_monday)
            period_start = datetime.combine(week_start, datetime.min.time())
            period_end = period_start + timedelta(days=7)
        else:
            raise ValueError(f"Invalid period_type: {period_type}")

        # Get matches for period
        query = text("""
            SELECT * FROM news_matches
            WHERE created_at >= :start AND created_at < :end
            ORDER BY relevance_score DESC, impact_score DESC
        """)
        result = self.db.execute(query, {"start": period_start, "end": period_end})
        matches = list(result.mappings())

        # Generate highlights (top 5 by impact)
        highlights = []
        seen_titles = set()
        for m in sorted(matches, key=lambda x: x["impact_score"], reverse=True)[:5]:
            if m["news_title"] not in seen_titles:
                highlights.append({
                    "title": m["news_title"],
                    "impact": "high" if m["impact_score"] >= 0.8 else "medium" if m["impact_score"] >= 0.5 else "low",
                    "event_type": m["event_type"],
                    "summary": m["summary"],
                })
                seen_titles.add(m["news_title"])

        # Calculate stats
        by_category = {}
        sentiment_counts = {"positive": 0, "neutral": 0, "negative": 0}
        by_source = {}

        for m in matches:
            # By event type
            et = m["event_type"] or "other"
            by_category[et] = by_category.get(et, 0) + 1

            # By sentiment
            if m["sentiment"] > 0.2:
                sentiment_counts["positive"] += 1
            elif m["sentiment"] < -0.2:
                sentiment_counts["negative"] += 1
            else:
                sentiment_counts["neutral"] += 1

            # By source
            src = m["news_source"] or "unknown"
            by_source[src] = by_source.get(src, 0) + 1

        stats = {
            "total_matches": len(matches),
            "by_category": by_category,
            "sentiment": sentiment_counts,
            "by_source": by_source,
        }

        # Generate summary text
        summary = self._generate_digest_summary(period_type, target_date, highlights, stats)

        # Store digest
        digest = self._store_digest(
            period_type=period_type,
            period_start=period_start,
            period_end=period_end,
            summary=summary,
            highlights=highlights,
            stats=stats,
        )

        return digest

    def _generate_digest_summary(
        self,
        period_type: str,
        target_date: date,
        highlights: List[Dict],
        stats: Dict,
    ) -> str:
        """Generate a text summary for the digest."""
        date_str = target_date.strftime("%B %d, %Y")
        period_name = "Daily" if period_type == "daily" else "Weekly"

        lines = [f"# {period_name} News Digest - {date_str}", ""]

        if highlights:
            lines.append("## Top Stories")
            for i, h in enumerate(highlights, 1):
                lines.append(f"{i}. **{h['title'][:80]}** - {h.get('event_type', 'news')}")
            lines.append("")

        total = stats.get("total_matches", 0)
        lines.append(f"## Summary")
        lines.append(f"Total matched news items: {total}")
        lines.append("")

        if stats.get("by_category"):
            lines.append("### By Category")
            for cat, count in sorted(stats["by_category"].items(), key=lambda x: -x[1]):
                lines.append(f"- {cat}: {count}")
            lines.append("")

        sentiment = stats.get("sentiment", {})
        if any(sentiment.values()):
            lines.append("### Sentiment")
            pos = sentiment.get("positive", 0)
            neu = sentiment.get("neutral", 0)
            neg = sentiment.get("negative", 0)
            total_sent = pos + neu + neg
            if total_sent > 0:
                lines.append(f"- Positive: {pos} ({pos*100//total_sent}%)")
                lines.append(f"- Neutral: {neu} ({neu*100//total_sent}%)")
                lines.append(f"- Negative: {neg} ({neg*100//total_sent}%)")

        return "\n".join(lines)

    def _store_digest(
        self,
        period_type: str,
        period_start: datetime,
        period_end: datetime,
        summary: str,
        highlights: List[Dict],
        stats: Dict,
    ) -> Digest:
        """Store or update a digest."""
        import json

        # Upsert
        query = text("""
            INSERT INTO news_digests (period_type, period_start, period_end, summary, highlights, stats)
            VALUES (:period_type, :period_start, :period_end, :summary, :highlights, :stats)
            ON CONFLICT (period_type, period_start) DO UPDATE SET
                summary = :summary,
                highlights = :highlights,
                stats = :stats,
                generated_at = CURRENT_TIMESTAMP
            RETURNING id, generated_at
        """)

        result = self.db.execute(query, {
            "period_type": period_type,
            "period_start": period_start,
            "period_end": period_end,
            "summary": summary,
            "highlights": json.dumps(highlights),
            "stats": json.dumps(stats),
        })
        row = result.fetchone()
        self.db.commit()

        return Digest(
            id=row[0],
            period_type=period_type,
            period_start=period_start,
            period_end=period_end,
            summary=summary,
            highlights=highlights,
            stats=stats,
            generated_at=row[1],
        )

    def get_digest(
        self,
        period_type: str = "daily",
        target_date: Optional[date] = None,
    ) -> Optional[Digest]:
        """Get an existing digest."""
        if target_date is None:
            target_date = date.today()

        if period_type == "daily":
            period_start = datetime.combine(target_date, datetime.min.time())
        else:
            days_since_monday = target_date.weekday()
            week_start = target_date - timedelta(days=days_since_monday)
            period_start = datetime.combine(week_start, datetime.min.time())

        query = text("""
            SELECT * FROM news_digests
            WHERE period_type = :period_type AND period_start = :period_start
        """)
        result = self.db.execute(query, {
            "period_type": period_type,
            "period_start": period_start,
        })
        row = result.mappings().fetchone()

        if row:
            return Digest(
                id=row["id"],
                period_type=row["period_type"],
                period_start=row["period_start"],
                period_end=row["period_end"],
                summary=row["summary"],
                highlights=row["highlights"] or [],
                stats=row["stats"] or {},
                generated_at=row["generated_at"],
            )
        return None

    # -------------------------------------------------------------------------
    # STATISTICS
    # -------------------------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        """Get monitoring statistics."""
        # Watch count
        watch_count = self.db.execute(
            text("SELECT COUNT(*) FROM news_watch_items")
        ).scalar() or 0

        # Match counts
        match_stats = self.db.execute(text("""
            SELECT
                COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '1 day') as today,
                COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days') as week,
                COUNT(*) FILTER (WHERE is_read = FALSE) as unread,
                COUNT(*) FILTER (WHERE is_breaking = TRUE AND is_alerted = FALSE) as pending_alerts
            FROM news_matches
        """)).fetchone()

        # Top sources
        top_sources = self.db.execute(text("""
            SELECT news_source, COUNT(*) as count
            FROM news_matches
            WHERE created_at > NOW() - INTERVAL '7 days'
            GROUP BY news_source
            ORDER BY count DESC
            LIMIT 5
        """)).fetchall()

        # Top event types
        top_events = self.db.execute(text("""
            SELECT event_type, COUNT(*) as count
            FROM news_matches
            WHERE created_at > NOW() - INTERVAL '7 days'
            GROUP BY event_type
            ORDER BY count DESC
            LIMIT 5
        """)).fetchall()

        return {
            "watch_items": watch_count,
            "matches_today": match_stats[0] or 0,
            "matches_this_week": match_stats[1] or 0,
            "unread": match_stats[2] or 0,
            "pending_alerts": match_stats[3] or 0,
            "top_sources": [{"source": r[0] or "unknown", "count": r[1]} for r in top_sources],
            "top_event_types": [{"type": r[0] or "other", "count": r[1]} for r in top_events],
        }

    # -------------------------------------------------------------------------
    # NEWS PROCESSING
    # -------------------------------------------------------------------------

    def process_recent_news(self, days: int = 1) -> Dict[str, Any]:
        """
        Process recent news from the news aggregator against watch list.

        Fetches news from the NewsAggregator (if available) and matches
        against all watch items.

        Returns summary of processing results.
        """
        news_items = []

        # Try to get news from news feed tables
        try:
            query = text("""
                SELECT DISTINCT title, link as url, source, published_at, description as body
                FROM news_items
                WHERE published_at > NOW() - INTERVAL ':days days'
                ORDER BY published_at DESC
                LIMIT 500
            """)
            result = self.db.execute(query, {"days": days})
            for row in result.mappings():
                news_items.append({
                    "title": row["title"],
                    "url": row.get("url"),
                    "source": row.get("source"),
                    "published_at": row.get("published_at"),
                    "body": row.get("body"),
                })
        except Exception as e:
            logger.warning(f"Could not fetch from news_items table: {e}")

        # Also try SEC filings as news
        try:
            query = text("""
                SELECT form_type || ': ' || company_name as title,
                       filing_url as url,
                       'sec_edgar' as source,
                       filed_at as published_at,
                       description as body
                FROM sec_filings
                WHERE filed_at > NOW() - INTERVAL ':days days'
                ORDER BY filed_at DESC
                LIMIT 200
            """)
            result = self.db.execute(query, {"days": days})
            for row in result.mappings():
                news_items.append({
                    "title": row["title"],
                    "url": row.get("url"),
                    "source": row.get("source"),
                    "published_at": row.get("published_at"),
                    "body": row.get("body"),
                })
        except Exception as e:
            logger.debug(f"Could not fetch from sec_filings table: {e}")

        if not news_items:
            return {
                "news_processed": 0,
                "matches_created": 0,
                "alerts_triggered": 0,
            }

        # Process through the feed
        result = self.process_news_feed(news_items)

        return {
            "news_processed": result["processed"],
            "matches_created": result["matches"],
            "alerts_triggered": result["breaking"],
        }
