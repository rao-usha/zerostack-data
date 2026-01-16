"""
Watchlist Service for T20: Saved Searches & Watchlists.

Provides CRUD operations for watchlists, watchlist items, and saved searches.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Dict, Any

from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class Watchlist:
    """Watchlist data model."""
    id: int
    user_id: str
    name: str
    description: Optional[str]
    is_public: bool
    item_count: int
    created_at: datetime
    updated_at: datetime


@dataclass
class WatchlistItem:
    """Watchlist item data model."""
    id: int
    watchlist_id: int
    entity_type: str
    entity_id: int
    entity_name: str
    entity_details: Dict[str, Any]
    note: Optional[str]
    added_at: datetime


@dataclass
class SavedSearch:
    """Saved search data model."""
    id: int
    user_id: str
    name: str
    query: Optional[str]
    filters: Dict[str, Any]
    execution_count: int
    last_executed_at: Optional[datetime]
    created_at: datetime
    updated_at: datetime


# =============================================================================
# Schema Setup
# =============================================================================


SCHEMA_SQL = """
-- Watchlists table
CREATE TABLE IF NOT EXISTS watchlists (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    is_public BOOLEAN DEFAULT FALSE,
    item_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Watchlist items table
CREATE TABLE IF NOT EXISTS watchlist_items (
    id SERIAL PRIMARY KEY,
    watchlist_id INTEGER NOT NULL REFERENCES watchlists(id) ON DELETE CASCADE,
    entity_type VARCHAR(50) NOT NULL,
    entity_id INTEGER NOT NULL,
    note TEXT,
    added_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(watchlist_id, entity_type, entity_id)
);

-- Saved searches table
CREATE TABLE IF NOT EXISTS saved_searches (
    id SERIAL PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    query TEXT,
    filters JSONB DEFAULT '{}',
    execution_count INTEGER DEFAULT 0,
    last_executed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
"""

INDEX_SQL = """
-- Indexes for watchlists
CREATE INDEX IF NOT EXISTS idx_watchlists_user ON watchlists(user_id);
CREATE INDEX IF NOT EXISTS idx_watchlists_public ON watchlists(is_public) WHERE is_public = TRUE;

-- Indexes for watchlist_items
CREATE INDEX IF NOT EXISTS idx_watchlist_items_watchlist ON watchlist_items(watchlist_id);
CREATE INDEX IF NOT EXISTS idx_watchlist_items_entity ON watchlist_items(entity_type, entity_id);

-- Indexes for saved_searches
CREATE INDEX IF NOT EXISTS idx_saved_searches_user ON saved_searches(user_id);
CREATE INDEX IF NOT EXISTS idx_saved_searches_name ON saved_searches(user_id, name);
"""

TRIGGER_SQL = """
-- Function to update item_count
CREATE OR REPLACE FUNCTION update_watchlist_item_count()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        UPDATE watchlists SET item_count = item_count + 1, updated_at = NOW()
        WHERE id = NEW.watchlist_id;
    ELSIF TG_OP = 'DELETE' THEN
        UPDATE watchlists SET item_count = item_count - 1, updated_at = NOW()
        WHERE id = OLD.watchlist_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

-- Drop and recreate trigger to avoid errors
DROP TRIGGER IF EXISTS trig_watchlist_item_count ON watchlist_items;
CREATE TRIGGER trig_watchlist_item_count
AFTER INSERT OR DELETE ON watchlist_items
FOR EACH ROW EXECUTE FUNCTION update_watchlist_item_count();
"""


# =============================================================================
# Watchlist Service
# =============================================================================


class WatchlistService:
    """Service for managing watchlists, items, and saved searches."""

    def __init__(self, db: Session):
        self.db = db

    def ensure_schema(self) -> None:
        """Create tables, indexes, and triggers if they don't exist."""
        try:
            # Create tables
            for statement in SCHEMA_SQL.split(';'):
                statement = statement.strip()
                if statement:
                    self.db.execute(text(statement))

            # Create indexes
            for statement in INDEX_SQL.split(';'):
                statement = statement.strip()
                if statement:
                    self.db.execute(text(statement))

            # Create trigger
            self.db.execute(text(TRIGGER_SQL))
            self.db.commit()
            logger.info("T20 schema ensured successfully")
        except Exception as e:
            self.db.rollback()
            logger.error(f"Error ensuring schema: {e}")
            raise

    # =========================================================================
    # Watchlist CRUD
    # =========================================================================

    def create_watchlist(
        self,
        user_id: str,
        name: str,
        description: Optional[str] = None
    ) -> Watchlist:
        """Create a new watchlist."""
        self.ensure_schema()

        result = self.db.execute(
            text("""
                INSERT INTO watchlists (user_id, name, description)
                VALUES (:user_id, :name, :description)
                RETURNING id, user_id, name, description, is_public, item_count, created_at, updated_at
            """),
            {"user_id": user_id, "name": name, "description": description}
        )
        self.db.commit()
        row = result.fetchone()

        return Watchlist(
            id=row[0],
            user_id=row[1],
            name=row[2],
            description=row[3],
            is_public=row[4],
            item_count=row[5],
            created_at=row[6],
            updated_at=row[7]
        )

    def get_watchlist(self, watchlist_id: int) -> Optional[Watchlist]:
        """Get a watchlist by ID."""
        self.ensure_schema()

        result = self.db.execute(
            text("""
                SELECT id, user_id, name, description, is_public, item_count, created_at, updated_at
                FROM watchlists WHERE id = :id
            """),
            {"id": watchlist_id}
        )
        row = result.fetchone()

        if not row:
            return None

        return Watchlist(
            id=row[0],
            user_id=row[1],
            name=row[2],
            description=row[3],
            is_public=row[4],
            item_count=row[5],
            created_at=row[6],
            updated_at=row[7]
        )

    def list_watchlists(self, user_id: str) -> List[Watchlist]:
        """List all watchlists for a user."""
        self.ensure_schema()

        result = self.db.execute(
            text("""
                SELECT id, user_id, name, description, is_public, item_count, created_at, updated_at
                FROM watchlists
                WHERE user_id = :user_id
                ORDER BY updated_at DESC
            """),
            {"user_id": user_id}
        )

        return [
            Watchlist(
                id=row[0],
                user_id=row[1],
                name=row[2],
                description=row[3],
                is_public=row[4],
                item_count=row[5],
                created_at=row[6],
                updated_at=row[7]
            )
            for row in result.fetchall()
        ]

    def update_watchlist(
        self,
        watchlist_id: int,
        name: Optional[str] = None,
        description: Optional[str] = None
    ) -> Optional[Watchlist]:
        """Update a watchlist."""
        updates = []
        params = {"id": watchlist_id}

        if name is not None:
            updates.append("name = :name")
            params["name"] = name
        if description is not None:
            updates.append("description = :description")
            params["description"] = description

        if not updates:
            return self.get_watchlist(watchlist_id)

        updates.append("updated_at = NOW()")

        result = self.db.execute(
            text(f"""
                UPDATE watchlists
                SET {', '.join(updates)}
                WHERE id = :id
                RETURNING id, user_id, name, description, is_public, item_count, created_at, updated_at
            """),
            params
        )
        self.db.commit()
        row = result.fetchone()

        if not row:
            return None

        return Watchlist(
            id=row[0],
            user_id=row[1],
            name=row[2],
            description=row[3],
            is_public=row[4],
            item_count=row[5],
            created_at=row[6],
            updated_at=row[7]
        )

    def delete_watchlist(self, watchlist_id: int) -> bool:
        """Delete a watchlist and all its items (cascade)."""
        result = self.db.execute(
            text("DELETE FROM watchlists WHERE id = :id RETURNING id"),
            {"id": watchlist_id}
        )
        self.db.commit()
        return result.fetchone() is not None

    # =========================================================================
    # Watchlist Items
    # =========================================================================

    def _resolve_entity_name(self, entity_type: str, entity_id: int) -> tuple:
        """Resolve entity name and details from the database."""
        if entity_type == "investor":
            result = self.db.execute(
                text("""
                    SELECT name, lp_type, jurisdiction
                    FROM lp_fund WHERE id = :id
                """),
                {"id": entity_id}
            )
            row = result.fetchone()
            if row:
                return row[0], {
                    "investor_type": row[1],
                    "location": row[2]
                }
        elif entity_type == "company":
            result = self.db.execute(
                text("""
                    SELECT company_name, company_industry
                    FROM portfolio_companies WHERE id = :id
                """),
                {"id": entity_id}
            )
            row = result.fetchone()
            if row:
                return row[0], {
                    "industry": row[1]
                }

        return f"Unknown {entity_type} #{entity_id}", {}

    def add_item(
        self,
        watchlist_id: int,
        entity_type: str,
        entity_id: int,
        note: Optional[str] = None
    ) -> Optional[WatchlistItem]:
        """Add an item to a watchlist."""
        try:
            result = self.db.execute(
                text("""
                    INSERT INTO watchlist_items (watchlist_id, entity_type, entity_id, note)
                    VALUES (:watchlist_id, :entity_type, :entity_id, :note)
                    RETURNING id, watchlist_id, entity_type, entity_id, note, added_at
                """),
                {
                    "watchlist_id": watchlist_id,
                    "entity_type": entity_type,
                    "entity_id": entity_id,
                    "note": note
                }
            )
            self.db.commit()
            row = result.fetchone()

            if not row:
                return None

            entity_name, entity_details = self._resolve_entity_name(entity_type, entity_id)

            return WatchlistItem(
                id=row[0],
                watchlist_id=row[1],
                entity_type=row[2],
                entity_id=row[3],
                entity_name=entity_name,
                entity_details=entity_details,
                note=row[4],
                added_at=row[5]
            )
        except IntegrityError:
            self.db.rollback()
            return None  # Duplicate or invalid watchlist_id

    def list_items(
        self,
        watchlist_id: int,
        page: int = 1,
        page_size: int = 50
    ) -> tuple:
        """List items in a watchlist with pagination."""
        offset = (page - 1) * page_size

        # Get total count
        count_result = self.db.execute(
            text("SELECT COUNT(*) FROM watchlist_items WHERE watchlist_id = :watchlist_id"),
            {"watchlist_id": watchlist_id}
        )
        total = count_result.scalar() or 0

        # Get items
        result = self.db.execute(
            text("""
                SELECT id, watchlist_id, entity_type, entity_id, note, added_at
                FROM watchlist_items
                WHERE watchlist_id = :watchlist_id
                ORDER BY added_at DESC
                LIMIT :limit OFFSET :offset
            """),
            {"watchlist_id": watchlist_id, "limit": page_size, "offset": offset}
        )

        items = []
        for row in result.fetchall():
            entity_name, entity_details = self._resolve_entity_name(row[2], row[3])
            items.append(WatchlistItem(
                id=row[0],
                watchlist_id=row[1],
                entity_type=row[2],
                entity_id=row[3],
                entity_name=entity_name,
                entity_details=entity_details,
                note=row[4],
                added_at=row[5]
            ))

        return items, total

    def remove_item(self, watchlist_id: int, item_id: int) -> bool:
        """Remove an item from a watchlist."""
        result = self.db.execute(
            text("""
                DELETE FROM watchlist_items
                WHERE id = :item_id AND watchlist_id = :watchlist_id
                RETURNING id
            """),
            {"item_id": item_id, "watchlist_id": watchlist_id}
        )
        self.db.commit()
        return result.fetchone() is not None

    def remove_item_by_entity(
        self,
        watchlist_id: int,
        entity_type: str,
        entity_id: int
    ) -> bool:
        """Remove an item by entity type and ID."""
        result = self.db.execute(
            text("""
                DELETE FROM watchlist_items
                WHERE watchlist_id = :watchlist_id
                AND entity_type = :entity_type
                AND entity_id = :entity_id
                RETURNING id
            """),
            {
                "watchlist_id": watchlist_id,
                "entity_type": entity_type,
                "entity_id": entity_id
            }
        )
        self.db.commit()
        return result.fetchone() is not None

    # =========================================================================
    # Saved Searches
    # =========================================================================

    def create_saved_search(
        self,
        user_id: str,
        name: str,
        query: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> SavedSearch:
        """Create a new saved search."""
        self.ensure_schema()

        import json
        filters_json = json.dumps(filters or {})

        result = self.db.execute(
            text("""
                INSERT INTO saved_searches (user_id, name, query, filters)
                VALUES (:user_id, :name, :query, CAST(:filters AS jsonb))
                RETURNING id, user_id, name, query, filters, execution_count,
                          last_executed_at, created_at, updated_at
            """),
            {
                "user_id": user_id,
                "name": name,
                "query": query or "",
                "filters": filters_json
            }
        )
        self.db.commit()
        row = result.fetchone()

        return SavedSearch(
            id=row[0],
            user_id=row[1],
            name=row[2],
            query=row[3],
            filters=row[4] or {},
            execution_count=row[5],
            last_executed_at=row[6],
            created_at=row[7],
            updated_at=row[8]
        )

    def get_saved_search(self, search_id: int) -> Optional[SavedSearch]:
        """Get a saved search by ID."""
        self.ensure_schema()

        result = self.db.execute(
            text("""
                SELECT id, user_id, name, query, filters, execution_count,
                       last_executed_at, created_at, updated_at
                FROM saved_searches WHERE id = :id
            """),
            {"id": search_id}
        )
        row = result.fetchone()

        if not row:
            return None

        return SavedSearch(
            id=row[0],
            user_id=row[1],
            name=row[2],
            query=row[3],
            filters=row[4] or {},
            execution_count=row[5],
            last_executed_at=row[6],
            created_at=row[7],
            updated_at=row[8]
        )

    def list_saved_searches(
        self,
        user_id: str,
        name_filter: Optional[str] = None
    ) -> List[SavedSearch]:
        """List saved searches for a user."""
        self.ensure_schema()

        if name_filter:
            result = self.db.execute(
                text("""
                    SELECT id, user_id, name, query, filters, execution_count,
                           last_executed_at, created_at, updated_at
                    FROM saved_searches
                    WHERE user_id = :user_id AND name ILIKE :name_filter
                    ORDER BY updated_at DESC
                """),
                {"user_id": user_id, "name_filter": f"%{name_filter}%"}
            )
        else:
            result = self.db.execute(
                text("""
                    SELECT id, user_id, name, query, filters, execution_count,
                           last_executed_at, created_at, updated_at
                    FROM saved_searches
                    WHERE user_id = :user_id
                    ORDER BY updated_at DESC
                """),
                {"user_id": user_id}
            )

        return [
            SavedSearch(
                id=row[0],
                user_id=row[1],
                name=row[2],
                query=row[3],
                filters=row[4] or {},
                execution_count=row[5],
                last_executed_at=row[6],
                created_at=row[7],
                updated_at=row[8]
            )
            for row in result.fetchall()
        ]

    def update_saved_search(
        self,
        search_id: int,
        name: Optional[str] = None,
        query: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None
    ) -> Optional[SavedSearch]:
        """Update a saved search."""
        import json

        updates = []
        params = {"id": search_id}

        if name is not None:
            updates.append("name = :name")
            params["name"] = name
        if query is not None:
            updates.append("query = :query")
            params["query"] = query
        if filters is not None:
            updates.append("filters = CAST(:filters AS jsonb)")
            params["filters"] = json.dumps(filters)

        if not updates:
            return self.get_saved_search(search_id)

        updates.append("updated_at = NOW()")

        result = self.db.execute(
            text(f"""
                UPDATE saved_searches
                SET {', '.join(updates)}
                WHERE id = :id
                RETURNING id, user_id, name, query, filters, execution_count,
                          last_executed_at, created_at, updated_at
            """),
            params
        )
        self.db.commit()
        row = result.fetchone()

        if not row:
            return None

        return SavedSearch(
            id=row[0],
            user_id=row[1],
            name=row[2],
            query=row[3],
            filters=row[4] or {},
            execution_count=row[5],
            last_executed_at=row[6],
            created_at=row[7],
            updated_at=row[8]
        )

    def delete_saved_search(self, search_id: int) -> bool:
        """Delete a saved search."""
        result = self.db.execute(
            text("DELETE FROM saved_searches WHERE id = :id RETURNING id"),
            {"id": search_id}
        )
        self.db.commit()
        return result.fetchone() is not None

    def record_execution(self, search_id: int) -> None:
        """Record that a saved search was executed."""
        self.db.execute(
            text("""
                UPDATE saved_searches
                SET execution_count = execution_count + 1,
                    last_executed_at = NOW(),
                    updated_at = NOW()
                WHERE id = :id
            """),
            {"id": search_id}
        )
        self.db.commit()
