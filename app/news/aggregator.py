"""
News Aggregator Service.

T24: Aggregates news from multiple sources (SEC EDGAR, Google News)
and provides unified access to news items.
"""

import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from sqlalchemy import text, Column, Integer, String, Text, Float, DateTime, ARRAY
from sqlalchemy.orm import Session
from sqlalchemy.ext.declarative import declarative_base

from app.news.sources.sec_rss import SECEdgarSource
from app.news.sources.google_news import GoogleNewsSource

logger = logging.getLogger(__name__)

Base = declarative_base()


class NewsItem(Base):
    """SQLAlchemy model for news items."""

    __tablename__ = "news_items"

    id = Column(Integer, primary_key=True)
    source = Column(String(50), nullable=False)
    source_id = Column(String(255))
    title = Column(Text, nullable=False)
    summary = Column(Text)
    url = Column(Text)
    published_at = Column(DateTime)
    fetched_at = Column(DateTime, default=datetime.utcnow)

    # Classification
    event_type = Column(String(50))
    filing_type = Column(String(20))

    # Entity references
    company_name = Column(String(255))
    company_ticker = Column(String(20))
    investor_id = Column(Integer)
    investor_type = Column(String(50))

    # Relevance
    relevance_score = Column(Float, default=0.5)


@dataclass
class NewsFilters:
    """Filters for news queries."""

    event_type: Optional[str] = None
    filing_type: Optional[str] = None
    source: Optional[str] = None
    company_name: Optional[str] = None
    investor_id: Optional[int] = None
    investor_type: Optional[str] = None
    days: int = 7
    limit: int = 50
    offset: int = 0


class NewsAggregator:
    """
    News aggregation service.

    Fetches news from multiple sources and provides unified access.
    """

    def __init__(self, db: Session):
        self.db = db
        self.sec_source = SECEdgarSource()
        self.google_source = GoogleNewsSource()

    def _ensure_table(self):
        """Ensure news_items table exists."""
        create_sql = """
        CREATE TABLE IF NOT EXISTS news_items (
            id SERIAL PRIMARY KEY,
            source VARCHAR(50) NOT NULL,
            source_id VARCHAR(255),
            title TEXT NOT NULL,
            summary TEXT,
            url TEXT,
            published_at TIMESTAMP,
            fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            event_type VARCHAR(50),
            filing_type VARCHAR(20),
            company_name VARCHAR(255),
            company_ticker VARCHAR(20),
            investor_id INTEGER,
            investor_type VARCHAR(50),
            relevance_score FLOAT DEFAULT 0.5,
            UNIQUE(source, source_id)
        );

        CREATE INDEX IF NOT EXISTS idx_news_published ON news_items(published_at DESC);
        CREATE INDEX IF NOT EXISTS idx_news_company ON news_items(company_name);
        CREATE INDEX IF NOT EXISTS idx_news_investor ON news_items(investor_id, investor_type);
        CREATE INDEX IF NOT EXISTS idx_news_type ON news_items(event_type);
        """
        try:
            self.db.execute(text(create_sql))
            self.db.commit()
        except Exception as e:
            logger.error(f"Error creating news_items table: {e}")
            self.db.rollback()

    async def refresh_all(self) -> Dict[str, Any]:
        """
        Refresh news from all sources.

        Returns:
            Dict with counts per source.
        """
        self._ensure_table()

        results = {
            "sec_edgar": 0,
            "google_news": 0,
            "total_new": 0,
            "refreshed_at": datetime.utcnow().isoformat(),
        }

        # Fetch from SEC EDGAR
        try:
            sec_items = await self.sec_source.fetch()
            sec_count = self._store_items(sec_items)
            results["sec_edgar"] = sec_count
            results["total_new"] += sec_count
        except Exception as e:
            logger.error(f"Error fetching SEC news: {e}")
            results["sec_edgar_error"] = str(e)

        # Fetch from Google News
        try:
            # Get company names from portfolio for targeted search
            company_names = self._get_portfolio_companies()[:10]
            google_items = await self.google_source.fetch(company_names=company_names)
            google_count = self._store_items(google_items)
            results["google_news"] = google_count
            results["total_new"] += google_count
        except Exception as e:
            logger.error(f"Error fetching Google News: {e}")
            results["google_news_error"] = str(e)

        # Close async clients
        await self.sec_source.close()
        await self.google_source.close()

        return results

    async def refresh_source(self, source_name: str) -> Dict[str, Any]:
        """Refresh a specific source."""
        self._ensure_table()

        result = {
            "source": source_name,
            "new_items": 0,
            "refreshed_at": datetime.utcnow().isoformat(),
        }

        try:
            if source_name == "sec_edgar":
                items = await self.sec_source.fetch()
                await self.sec_source.close()
            elif source_name == "google_news":
                company_names = self._get_portfolio_companies()[:10]
                items = await self.google_source.fetch(company_names=company_names)
                await self.google_source.close()
            else:
                result["error"] = f"Unknown source: {source_name}"
                return result

            result["new_items"] = self._store_items(items)

        except Exception as e:
            logger.error(f"Error refreshing {source_name}: {e}")
            result["error"] = str(e)

        return result

    def _store_items(self, items: List[Dict]) -> int:
        """Store news items in database, handling duplicates."""
        new_count = 0

        for item in items:
            try:
                # Use upsert pattern
                insert_sql = text("""
                    INSERT INTO news_items (
                        source, source_id, title, summary, url,
                        published_at, event_type, filing_type,
                        company_name, company_ticker, investor_id,
                        investor_type, relevance_score, fetched_at
                    ) VALUES (
                        :source, :source_id, :title, :summary, :url,
                        :published_at, :event_type, :filing_type,
                        :company_name, :company_ticker, :investor_id,
                        :investor_type, :relevance_score, :fetched_at
                    )
                    ON CONFLICT (source, source_id) DO NOTHING
                """)

                result = self.db.execute(
                    insert_sql,
                    {
                        "source": item["source"],
                        "source_id": item["source_id"],
                        "title": item["title"],
                        "summary": item.get("summary"),
                        "url": item.get("url"),
                        "published_at": item.get("published_at"),
                        "event_type": item.get("event_type"),
                        "filing_type": item.get("filing_type"),
                        "company_name": item.get("company_name"),
                        "company_ticker": item.get("company_ticker"),
                        "investor_id": item.get("investor_id"),
                        "investor_type": item.get("investor_type"),
                        "relevance_score": item.get("relevance_score", 0.5),
                        "fetched_at": datetime.utcnow(),
                    },
                )

                if result.rowcount > 0:
                    new_count += 1

            except Exception as e:
                logger.error(f"Error storing news item: {e}")
                continue

        self.db.commit()
        return new_count

    def _get_portfolio_companies(self) -> List[str]:
        """Get company names from portfolio for targeted news search."""
        try:
            result = self.db.execute(
                text("""
                SELECT DISTINCT company_name
                FROM portfolio_companies
                WHERE company_name IS NOT NULL
                    AND company_name != ''
                ORDER BY company_name
                LIMIT 50
            """)
            )
            return [row[0] for row in result.fetchall()]
        except Exception:
            return []

    def get_feed(self, filters: NewsFilters) -> Dict[str, Any]:
        """
        Get news feed with filters.

        Args:
            filters: NewsFilters object with query parameters.

        Returns:
            Dict with items, total count, and metadata.
        """
        self._ensure_table()

        # Build WHERE clause
        conditions = []
        params = {}

        # Date filter
        cutoff = datetime.utcnow() - timedelta(days=filters.days)
        conditions.append("published_at >= :cutoff")
        params["cutoff"] = cutoff

        if filters.event_type:
            conditions.append("event_type = :event_type")
            params["event_type"] = filters.event_type

        if filters.filing_type:
            conditions.append("filing_type = :filing_type")
            params["filing_type"] = filters.filing_type

        if filters.source:
            conditions.append("source = :source")
            params["source"] = filters.source

        if filters.company_name:
            conditions.append("LOWER(company_name) LIKE LOWER(:company_name)")
            params["company_name"] = f"%{filters.company_name}%"

        if filters.investor_id:
            conditions.append("investor_id = :investor_id")
            params["investor_id"] = filters.investor_id

        if filters.investor_type:
            conditions.append("investor_type = :investor_type")
            params["investor_type"] = filters.investor_type

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        # Count total
        count_sql = text(f"SELECT COUNT(*) FROM news_items WHERE {where_clause}")
        total = self.db.execute(count_sql, params).scalar() or 0

        # Fetch items
        query_sql = text(f"""
            SELECT id, source, source_id, title, summary, url,
                   published_at, event_type, filing_type,
                   company_name, company_ticker, investor_id,
                   investor_type, relevance_score
            FROM news_items
            WHERE {where_clause}
            ORDER BY published_at DESC
            LIMIT :limit OFFSET :offset
        """)
        params["limit"] = filters.limit
        params["offset"] = filters.offset

        result = self.db.execute(query_sql, params)

        items = []
        for row in result.mappings():
            items.append(
                {
                    "id": row["id"],
                    "source": row["source"],
                    "title": row["title"],
                    "summary": row["summary"],
                    "url": row["url"],
                    "published_at": row["published_at"].isoformat()
                    if row["published_at"]
                    else None,
                    "event_type": row["event_type"],
                    "filing_type": row["filing_type"],
                    "company_name": row["company_name"],
                    "company_ticker": row["company_ticker"],
                    "investor_id": row["investor_id"],
                    "investor_type": row["investor_type"],
                    "relevance_score": row["relevance_score"],
                }
            )

        return {
            "items": items,
            "total": total,
            "page": filters.offset // filters.limit + 1,
            "page_size": filters.limit,
            "has_more": (filters.offset + filters.limit) < total,
        }

    def get_company_news(
        self,
        company_name: str,
        days: int = 30,
        limit: int = 20,
    ) -> List[Dict]:
        """Get news for a specific company."""
        filters = NewsFilters(
            company_name=company_name,
            days=days,
            limit=limit,
        )
        result = self.get_feed(filters)
        return result["items"]

    def get_investor_news(
        self,
        investor_id: int,
        investor_type: str,
        days: int = 30,
        limit: int = 20,
    ) -> List[Dict]:
        """Get news mentioning a specific investor."""
        filters = NewsFilters(
            investor_id=investor_id,
            investor_type=investor_type,
            days=days,
            limit=limit,
        )
        result = self.get_feed(filters)
        return result["items"]

    def get_filings(
        self,
        filing_type: Optional[str] = None,
        days: int = 7,
        limit: int = 50,
    ) -> Dict[str, Any]:
        """Get SEC filings feed."""
        filters = NewsFilters(
            source="sec_edgar",
            event_type="filing",
            filing_type=filing_type,
            days=days,
            limit=limit,
        )
        return self.get_feed(filters)

    def get_sources(self) -> List[Dict[str, Any]]:
        """Get available news sources with stats."""
        self._ensure_table()

        sources = []

        # Get stats per source
        stats_sql = text("""
            SELECT
                source,
                COUNT(*) as total_items,
                MAX(published_at) as latest_item,
                MAX(fetched_at) as last_fetch
            FROM news_items
            GROUP BY source
        """)

        try:
            result = self.db.execute(stats_sql)
            for row in result.mappings():
                sources.append(
                    {
                        "name": row["source"],
                        "total_items": row["total_items"],
                        "latest_item": row["latest_item"].isoformat()
                        if row["latest_item"]
                        else None,
                        "last_fetch": row["last_fetch"].isoformat()
                        if row["last_fetch"]
                        else None,
                    }
                )
        except Exception as e:
            logger.error(f"Error getting source stats: {e}")

        # Add sources with no data yet
        known_sources = {"sec_edgar", "google_news"}
        existing = {s["name"] for s in sources}
        for source in known_sources - existing:
            sources.append(
                {
                    "name": source,
                    "total_items": 0,
                    "latest_item": None,
                    "last_fetch": None,
                }
            )

        return sources
