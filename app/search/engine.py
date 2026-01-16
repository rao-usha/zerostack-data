"""
Full-text search engine with PostgreSQL FTS and fuzzy matching.

Provides unified search across investors, portfolio companies, and co-investors.
"""

import time
import logging
import re
from enum import Enum
from typing import Optional, List, Dict, Any
from dataclasses import dataclass, field
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.core.database import get_engine

logger = logging.getLogger(__name__)


class SearchResultType(str, Enum):
    """Types of searchable entities."""
    INVESTOR = "investor"
    COMPANY = "company"
    CO_INVESTOR = "co_investor"


@dataclass
class SearchResult:
    """A single search result."""
    id: int
    entity_id: int
    result_type: str
    name: str
    description: Optional[str]
    relevance_score: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    highlight: Optional[str] = None


@dataclass
class SearchSuggestion:
    """An autocomplete suggestion."""
    text: str
    type: str
    id: int
    entity_id: int
    score: float


@dataclass
class SearchFacets:
    """Aggregated facet counts for filtering."""
    result_types: Dict[str, int] = field(default_factory=dict)
    industries: Dict[str, int] = field(default_factory=dict)
    investor_types: Dict[str, int] = field(default_factory=dict)
    locations: Dict[str, int] = field(default_factory=dict)


@dataclass
class SearchResponse:
    """Complete search response with results, facets, and metadata."""
    results: List[SearchResult]
    facets: SearchFacets
    total: int
    page: int
    page_size: int
    query: str
    search_time_ms: float


class SearchEngine:
    """
    Full-text search engine using PostgreSQL.

    Features:
    - Full-text search with relevance ranking (ts_rank_cd)
    - Fuzzy matching for typo tolerance (pg_trgm)
    - Faceted filtering by type, industry, location
    - Autocomplete suggestions
    """

    # Minimum trigram similarity threshold for fuzzy matches
    FUZZY_THRESHOLD = 0.3

    def __init__(self, db: Session):
        self.db = db

    def ensure_schema(self) -> None:
        """
        Create search index table and required extensions.

        Safe to call multiple times (idempotent).
        """
        engine = get_engine()

        with engine.connect() as conn:
            # Enable pg_trgm extension for fuzzy matching
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
            conn.commit()

            # Create search_index table
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS search_index (
                    id SERIAL PRIMARY KEY,
                    entity_type VARCHAR(50) NOT NULL,
                    entity_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    name_normalized TEXT NOT NULL,
                    description TEXT,
                    industry VARCHAR(255),
                    investor_type VARCHAR(100),
                    location VARCHAR(255),
                    metadata JSONB DEFAULT '{}',
                    search_vector TSVECTOR,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(entity_type, entity_id)
                )
            """))
            conn.commit()

            # Create indexes (IF NOT EXISTS for idempotency)
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_search_vector ON search_index USING GIN(search_vector)",
                "CREATE INDEX IF NOT EXISTS idx_search_entity_type ON search_index(entity_type)",
                "CREATE INDEX IF NOT EXISTS idx_search_industry ON search_index(industry)",
                "CREATE INDEX IF NOT EXISTS idx_search_investor_type ON search_index(investor_type)",
                "CREATE INDEX IF NOT EXISTS idx_search_location ON search_index(location)",
                "CREATE INDEX IF NOT EXISTS idx_search_name_trgm ON search_index USING GIN(name_normalized gin_trgm_ops)",
            ]

            for idx_sql in indexes:
                try:
                    conn.execute(text(idx_sql))
                    conn.commit()
                except Exception as e:
                    logger.warning(f"Index creation warning (may already exist): {e}")
                    conn.rollback()

            # Create trigger function for auto-updating search_vector
            conn.execute(text("""
                CREATE OR REPLACE FUNCTION update_search_vector()
                RETURNS TRIGGER AS $$
                BEGIN
                    NEW.search_vector =
                        setweight(to_tsvector('english', COALESCE(NEW.name, '')), 'A') ||
                        setweight(to_tsvector('english', COALESCE(NEW.description, '')), 'B') ||
                        setweight(to_tsvector('english', COALESCE(NEW.industry, '')), 'C');
                    NEW.name_normalized = lower(regexp_replace(COALESCE(NEW.name, ''), '[^a-zA-Z0-9 ]', '', 'g'));
                    NEW.updated_at = NOW();
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql
            """))
            conn.commit()

            # Create trigger (drop first to avoid duplicate)
            conn.execute(text("""
                DROP TRIGGER IF EXISTS trig_search_vector_update ON search_index
            """))
            conn.commit()

            conn.execute(text("""
                CREATE TRIGGER trig_search_vector_update
                BEFORE INSERT OR UPDATE ON search_index
                FOR EACH ROW EXECUTE FUNCTION update_search_vector()
            """))
            conn.commit()

            logger.info("Search index schema ensured")

    def reindex(self, entity_type: Optional[SearchResultType] = None) -> Dict[str, int]:
        """
        Populate search index from source tables.

        Args:
            entity_type: Optional filter to reindex only specific type

        Returns:
            Dict with counts per entity type indexed
        """
        self.ensure_schema()

        counts = {}

        # Reindex investors (lp_fund)
        if entity_type is None or entity_type == SearchResultType.INVESTOR:
            count = self._index_investors()
            counts["investor"] = count
            logger.info(f"Indexed {count} investors")

        # Reindex portfolio companies
        if entity_type is None or entity_type == SearchResultType.COMPANY:
            count = self._index_companies()
            counts["company"] = count
            logger.info(f"Indexed {count} companies")

        # Reindex co-investors
        if entity_type is None or entity_type == SearchResultType.CO_INVESTOR:
            count = self._index_co_investors()
            counts["co_investor"] = count
            logger.info(f"Indexed {count} co-investors")

        return counts

    def _index_investors(self) -> int:
        """Index all investors from lp_fund table."""
        # Delete existing investor entries
        self.db.execute(text(
            "DELETE FROM search_index WHERE entity_type = 'investor'"
        ))

        # Insert from lp_fund
        result = self.db.execute(text("""
            INSERT INTO search_index (entity_type, entity_id, name, name_normalized, description, investor_type, location, metadata)
            SELECT
                'investor',
                id,
                name,
                lower(regexp_replace(COALESCE(name, ''), '[^a-zA-Z0-9 ]', '', 'g')),
                formal_name,
                lp_type,
                jurisdiction,
                jsonb_build_object(
                    'formal_name', formal_name,
                    'website_url', website_url
                )
            FROM lp_fund
            ON CONFLICT (entity_type, entity_id) DO UPDATE SET
                name = EXCLUDED.name,
                name_normalized = EXCLUDED.name_normalized,
                description = EXCLUDED.description,
                investor_type = EXCLUDED.investor_type,
                location = EXCLUDED.location,
                metadata = EXCLUDED.metadata
            RETURNING id
        """))

        self.db.commit()
        return result.rowcount

    def _index_companies(self) -> int:
        """Index all portfolio companies."""
        # Delete existing company entries
        self.db.execute(text(
            "DELETE FROM search_index WHERE entity_type = 'company'"
        ))

        # Insert from portfolio_companies (deduplicated by company_name)
        result = self.db.execute(text("""
            INSERT INTO search_index (entity_type, entity_id, name, name_normalized, description, industry, location, metadata)
            SELECT DISTINCT ON (company_name)
                'company',
                id,
                company_name,
                lower(regexp_replace(COALESCE(company_name, ''), '[^a-zA-Z0-9 ]', '', 'g')),
                company_industry,
                company_industry,
                company_location,
                jsonb_build_object(
                    'website', company_website,
                    'stage', company_stage,
                    'ticker', company_ticker,
                    'investment_type', investment_type
                )
            FROM portfolio_companies
            WHERE company_name IS NOT NULL AND company_name != ''
            ORDER BY company_name, id
            ON CONFLICT (entity_type, entity_id) DO UPDATE SET
                name = EXCLUDED.name,
                name_normalized = EXCLUDED.name_normalized,
                description = EXCLUDED.description,
                industry = EXCLUDED.industry,
                location = EXCLUDED.location,
                metadata = EXCLUDED.metadata
            RETURNING id
        """))

        self.db.commit()
        return result.rowcount

    def _index_co_investors(self) -> int:
        """Index all co-investors."""
        # Delete existing co_investor entries
        self.db.execute(text(
            "DELETE FROM search_index WHERE entity_type = 'co_investor'"
        ))

        # Insert from co_investments (deduplicated by co_investor_name)
        result = self.db.execute(text("""
            INSERT INTO search_index (entity_type, entity_id, name, name_normalized, description, investor_type, metadata)
            SELECT DISTINCT ON (co_investor_name)
                'co_investor',
                id,
                co_investor_name,
                lower(regexp_replace(COALESCE(co_investor_name, ''), '[^a-zA-Z0-9 ]', '', 'g')),
                co_investor_type,
                co_investor_type,
                jsonb_build_object(
                    'deal_count', (SELECT COUNT(*) FROM co_investments c2 WHERE c2.co_investor_name = co_investments.co_investor_name)
                )
            FROM co_investments
            WHERE co_investor_name IS NOT NULL AND co_investor_name != ''
            ORDER BY co_investor_name, id
            ON CONFLICT (entity_type, entity_id) DO UPDATE SET
                name = EXCLUDED.name,
                name_normalized = EXCLUDED.name_normalized,
                description = EXCLUDED.description,
                investor_type = EXCLUDED.investor_type,
                metadata = EXCLUDED.metadata
            RETURNING id
        """))

        self.db.commit()
        return result.rowcount

    def search(
        self,
        query: str,
        result_types: Optional[List[str]] = None,
        industry: Optional[str] = None,
        investor_type: Optional[str] = None,
        location: Optional[str] = None,
        page: int = 1,
        page_size: int = 20,
        fuzzy: bool = True
    ) -> SearchResponse:
        """
        Execute full-text search with optional filters.

        Args:
            query: Search query string
            result_types: Filter by entity types (investor, company, co_investor)
            industry: Filter by industry
            investor_type: Filter by investor type (for investors only)
            location: Filter by location
            page: Page number (1-indexed)
            page_size: Results per page
            fuzzy: Enable fuzzy matching for typos

        Returns:
            SearchResponse with results, facets, and metadata
        """
        start_time = time.time()

        # Normalize query
        query = query.strip()
        normalized_query = re.sub(r'[^a-zA-Z0-9 ]', '', query.lower())

        # Build WHERE clause
        where_clauses = []
        params = {
            "query": query,
            "normalized_query": normalized_query,
            "offset": (page - 1) * page_size,
            "limit": page_size,
            "fuzzy_threshold": self.FUZZY_THRESHOLD
        }

        # Type filter
        if result_types:
            where_clauses.append("entity_type = ANY(:result_types)")
            params["result_types"] = result_types

        # Industry filter
        if industry:
            where_clauses.append("industry ILIKE :industry")
            params["industry"] = f"%{industry}%"

        # Investor type filter
        if investor_type:
            where_clauses.append("investor_type ILIKE :investor_type")
            params["investor_type"] = f"%{investor_type}%"

        # Location filter
        if location:
            where_clauses.append("location ILIKE :location")
            params["location"] = f"%{location}%"

        # Build search condition
        if query:
            if fuzzy:
                # Combined FTS + fuzzy matching
                search_condition = """
                    (search_vector @@ plainto_tsquery('english', :query)
                     OR similarity(name_normalized, :normalized_query) > :fuzzy_threshold)
                """
            else:
                # FTS only
                search_condition = "search_vector @@ plainto_tsquery('english', :query)"
            where_clauses.append(search_condition)

        where_sql = " AND ".join(where_clauses) if where_clauses else "TRUE"

        # Build ORDER BY for relevance
        if query:
            if fuzzy:
                order_sql = """
                    CASE
                        WHEN search_vector @@ plainto_tsquery('english', :query)
                        THEN ts_rank_cd(search_vector, plainto_tsquery('english', :query)) + 0.5
                        ELSE similarity(name_normalized, :normalized_query)
                    END DESC
                """
            else:
                order_sql = "ts_rank_cd(search_vector, plainto_tsquery('english', :query)) DESC"
        else:
            order_sql = "name ASC"

        # Get total count
        count_sql = f"SELECT COUNT(*) FROM search_index WHERE {where_sql}"
        total_result = self.db.execute(text(count_sql), params)
        total = total_result.scalar() or 0

        # Get results
        if query and fuzzy:
            relevance_sql = """
                CASE
                    WHEN search_vector @@ plainto_tsquery('english', :query)
                    THEN ts_rank_cd(search_vector, plainto_tsquery('english', :query)) + 0.5
                    ELSE similarity(name_normalized, :normalized_query)
                END
            """
        elif query:
            relevance_sql = "ts_rank_cd(search_vector, plainto_tsquery('english', :query))"
        else:
            relevance_sql = "1.0"

        results_sql = f"""
            SELECT
                id,
                entity_type,
                entity_id,
                name,
                description,
                industry,
                investor_type,
                location,
                metadata,
                {relevance_sql} as relevance_score
            FROM search_index
            WHERE {where_sql}
            ORDER BY {order_sql}
            LIMIT :limit OFFSET :offset
        """

        results_data = self.db.execute(text(results_sql), params)

        results = []
        for row in results_data:
            results.append(SearchResult(
                id=row.id,
                entity_id=row.entity_id,
                result_type=row.entity_type,
                name=row.name,
                description=row.description,
                relevance_score=float(row.relevance_score) if row.relevance_score else 0.0,
                metadata={
                    "industry": row.industry,
                    "investor_type": row.investor_type,
                    "location": row.location,
                    **(row.metadata or {})
                }
            ))

        # Get facets (only if we have results or no query)
        facets = self._get_facets(where_sql, params)

        elapsed_ms = (time.time() - start_time) * 1000

        return SearchResponse(
            results=results,
            facets=facets,
            total=total,
            page=page,
            page_size=page_size,
            query=query,
            search_time_ms=round(elapsed_ms, 2)
        )

    def _get_facets(self, where_sql: str, params: dict) -> SearchFacets:
        """Get aggregated facet counts."""
        facets = SearchFacets()

        # Entity type facets
        type_sql = f"""
            SELECT entity_type, COUNT(*) as cnt
            FROM search_index
            WHERE {where_sql}
            GROUP BY entity_type
        """
        type_results = self.db.execute(text(type_sql), params)
        for row in type_results:
            facets.result_types[row.entity_type] = row.cnt

        # Industry facets (top 10)
        industry_sql = f"""
            SELECT industry, COUNT(*) as cnt
            FROM search_index
            WHERE {where_sql} AND industry IS NOT NULL AND industry != ''
            GROUP BY industry
            ORDER BY cnt DESC
            LIMIT 10
        """
        industry_results = self.db.execute(text(industry_sql), params)
        for row in industry_results:
            facets.industries[row.industry] = row.cnt

        # Investor type facets
        inv_type_sql = f"""
            SELECT investor_type, COUNT(*) as cnt
            FROM search_index
            WHERE {where_sql} AND investor_type IS NOT NULL AND investor_type != ''
            GROUP BY investor_type
            ORDER BY cnt DESC
            LIMIT 10
        """
        inv_type_results = self.db.execute(text(inv_type_sql), params)
        for row in inv_type_results:
            facets.investor_types[row.investor_type] = row.cnt

        # Location facets (top 10)
        location_sql = f"""
            SELECT location, COUNT(*) as cnt
            FROM search_index
            WHERE {where_sql} AND location IS NOT NULL AND location != ''
            GROUP BY location
            ORDER BY cnt DESC
            LIMIT 10
        """
        location_results = self.db.execute(text(location_sql), params)
        for row in location_results:
            facets.locations[row.location] = row.cnt

        return facets

    def suggest(
        self,
        prefix: str,
        limit: int = 10,
        result_types: Optional[List[str]] = None
    ) -> List[SearchSuggestion]:
        """
        Get autocomplete suggestions for a prefix.

        Args:
            prefix: Search prefix (minimum 1 character)
            limit: Maximum number of suggestions
            result_types: Optional filter by entity types

        Returns:
            List of suggestions ranked by relevance
        """
        if not prefix or len(prefix) < 1:
            return []

        normalized_prefix = re.sub(r'[^a-zA-Z0-9 ]', '', prefix.lower())

        params = {
            "prefix": f"{normalized_prefix}%",
            "normalized_prefix": normalized_prefix,
            "limit": limit,
            "fuzzy_threshold": self.FUZZY_THRESHOLD
        }

        where_clauses = [
            "(name_normalized LIKE :prefix OR similarity(name_normalized, :normalized_prefix) > :fuzzy_threshold)"
        ]

        if result_types:
            where_clauses.append("entity_type = ANY(:result_types)")
            params["result_types"] = result_types

        where_sql = " AND ".join(where_clauses)

        sql = f"""
            SELECT
                id,
                entity_type,
                entity_id,
                name,
                CASE
                    WHEN name_normalized LIKE :prefix THEN 1.0
                    ELSE similarity(name_normalized, :normalized_prefix)
                END as score
            FROM search_index
            WHERE {where_sql}
            ORDER BY
                CASE WHEN name_normalized LIKE :prefix THEN 0 ELSE 1 END,
                score DESC,
                name ASC
            LIMIT :limit
        """

        results = self.db.execute(text(sql), params)

        suggestions = []
        for row in results:
            suggestions.append(SearchSuggestion(
                text=row.name,
                type=row.entity_type,
                id=row.id,
                entity_id=row.entity_id,
                score=float(row.score) if row.score else 0.0
            ))

        return suggestions

    def get_stats(self) -> Dict[str, Any]:
        """Get search index statistics."""
        stats = {
            "total_indexed": 0,
            "by_type": {},
            "last_updated": None
        }

        # Check if table exists
        try:
            result = self.db.execute(text("""
                SELECT
                    entity_type,
                    COUNT(*) as cnt,
                    MAX(updated_at) as last_updated
                FROM search_index
                GROUP BY entity_type
            """))

            for row in result:
                stats["by_type"][row.entity_type] = row.cnt
                stats["total_indexed"] += row.cnt
                if row.last_updated:
                    if stats["last_updated"] is None or row.last_updated > stats["last_updated"]:
                        stats["last_updated"] = row.last_updated

            if stats["last_updated"]:
                stats["last_updated"] = stats["last_updated"].isoformat()

        except Exception as e:
            logger.warning(f"Could not get search stats: {e}")
            stats["error"] = str(e)

        return stats
