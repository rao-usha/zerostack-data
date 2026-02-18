"""
App Store Client for iOS and Android app metrics.

Uses iTunes Search API for iOS data (free, no auth required).
Provides storage for Google Play data via manual entry.
"""

import logging
import httpx
from datetime import datetime
from typing import Dict, List, Optional, Any
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# iTunes API endpoints
ITUNES_SEARCH_URL = "https://itunes.apple.com/search"
ITUNES_LOOKUP_URL = "https://itunes.apple.com/lookup"


class AppStoreClient:
    """
    Client for app store data from iOS App Store and Google Play.

    Features:
    - iOS app search and lookup via iTunes API
    - App metrics tracking (ratings, reviews, rankings)
    - Historical ranking storage
    - Company app portfolio management
    """

    def __init__(self, db: Session):
        self.db = db
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Ensure app store tables exist."""
        create_apps = text("""
            CREATE TABLE IF NOT EXISTS app_store_apps (
                id SERIAL PRIMARY KEY,
                app_id VARCHAR(50) NOT NULL,
                store VARCHAR(20) NOT NULL,
                app_name VARCHAR(255) NOT NULL,
                bundle_id VARCHAR(255),
                developer_name VARCHAR(255),
                developer_id VARCHAR(50),
                description TEXT,
                category VARCHAR(100),
                subcategory VARCHAR(100),
                price FLOAT DEFAULT 0,
                currency VARCHAR(10) DEFAULT 'USD',
                current_rating FLOAT,
                rating_count INTEGER,
                current_version VARCHAR(50),
                release_date DATE,
                last_updated DATE,
                minimum_os_version VARCHAR(20),
                content_rating VARCHAR(50),
                app_icon_url TEXT,
                app_url TEXT,
                screenshots JSONB,
                languages JSONB,
                in_app_purchases BOOLEAN DEFAULT FALSE,
                file_size_bytes BIGINT,
                retrieved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(app_id, store)
            )
        """)

        create_rankings = text("""
            CREATE TABLE IF NOT EXISTS app_store_rankings (
                id SERIAL PRIMARY KEY,
                app_id VARCHAR(50) NOT NULL,
                store VARCHAR(20) NOT NULL,
                country VARCHAR(10) DEFAULT 'us',
                category VARCHAR(100),
                rank_type VARCHAR(50),
                rank_position INTEGER,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        create_rating_history = text("""
            CREATE TABLE IF NOT EXISTS app_store_rating_history (
                id SERIAL PRIMARY KEY,
                app_id VARCHAR(50) NOT NULL,
                store VARCHAR(20) NOT NULL,
                rating FLOAT,
                rating_count INTEGER,
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        create_company_apps = text("""
            CREATE TABLE IF NOT EXISTS company_app_portfolios (
                id SERIAL PRIMARY KEY,
                company_name VARCHAR(255) NOT NULL,
                app_id VARCHAR(50) NOT NULL,
                store VARCHAR(20) NOT NULL,
                relationship VARCHAR(50) DEFAULT 'owner',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(company_name, app_id, store)
            )
        """)

        create_index = text("""
            CREATE INDEX IF NOT EXISTS idx_app_store_apps_developer
            ON app_store_apps(developer_name)
        """)

        try:
            self.db.execute(create_apps)
            self.db.execute(create_rankings)
            self.db.execute(create_rating_history)
            self.db.execute(create_company_apps)
            self.db.execute(create_index)
            self.db.commit()
        except Exception as e:
            logger.warning(f"Table creation warning: {e}")
            self.db.rollback()

    async def search_ios_apps(
        self, query: str, country: str = "us", limit: int = 25, entity: str = "software"
    ) -> List[Dict[str, Any]]:
        """
        Search iOS App Store using iTunes API.

        Args:
            query: Search term
            country: Country code (us, gb, etc.)
            limit: Max results (1-200)
            entity: Entity type (software, iPadSoftware, macSoftware)

        Returns:
            List of app results
        """
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    ITUNES_SEARCH_URL,
                    params={
                        "term": query,
                        "country": country,
                        "entity": entity,
                        "limit": min(limit, 200),
                    },
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()

                results = []
                for app in data.get("results", []):
                    results.append(self._parse_itunes_app(app))

                return results

            except Exception as e:
                logger.error(f"iTunes search error: {e}")
                return []

    async def lookup_ios_app(
        self, app_id: str, country: str = "us"
    ) -> Optional[Dict[str, Any]]:
        """
        Look up iOS app by ID using iTunes API.

        Args:
            app_id: iTunes app ID
            country: Country code

        Returns:
            App data or None
        """
        async with httpx.AsyncClient() as client:
            try:
                response = await client.get(
                    ITUNES_LOOKUP_URL,
                    params={"id": app_id, "country": country},
                    timeout=30,
                )
                response.raise_for_status()
                data = response.json()

                results = data.get("results", [])
                if results:
                    app_data = self._parse_itunes_app(results[0])
                    # Save to database
                    self._save_app(app_data, "ios")
                    return app_data

                return None

            except Exception as e:
                logger.error(f"iTunes lookup error: {e}")
                return None

    def _parse_itunes_app(self, data: Dict) -> Dict[str, Any]:
        """Parse iTunes API response into standardized format."""
        return {
            "app_id": str(data.get("trackId", "")),
            "app_name": data.get("trackName", ""),
            "bundle_id": data.get("bundleId", ""),
            "developer_name": data.get("artistName", ""),
            "developer_id": str(data.get("artistId", "")),
            "description": data.get("description", ""),
            "category": data.get("primaryGenreName", ""),
            "categories": data.get("genres", []),
            "price": data.get("price", 0),
            "currency": data.get("currency", "USD"),
            "current_rating": data.get("averageUserRating"),
            "rating_count": data.get("userRatingCount"),
            "current_version": data.get("version", ""),
            "release_date": data.get("releaseDate", "")[:10]
            if data.get("releaseDate")
            else None,
            "last_updated": data.get("currentVersionReleaseDate", "")[:10]
            if data.get("currentVersionReleaseDate")
            else None,
            "minimum_os_version": data.get("minimumOsVersion", ""),
            "content_rating": data.get("contentAdvisoryRating", ""),
            "app_icon_url": data.get("artworkUrl512") or data.get("artworkUrl100", ""),
            "app_url": data.get("trackViewUrl", ""),
            "screenshots": data.get("screenshotUrls", []),
            "languages": data.get("languageCodesISO2A", []),
            "in_app_purchases": bool(data.get("isGameCenterEnabled"))
            or "In-App Purchases" in str(data.get("features", [])),
            "file_size_bytes": data.get("fileSizeBytes"),
            "store": "ios",
        }

    def _save_app(self, app_data: Dict, store: str) -> None:
        """Save app data to database."""
        import json

        query = text("""
            INSERT INTO app_store_apps (
                app_id, store, app_name, bundle_id, developer_name, developer_id,
                description, category, price, currency, current_rating, rating_count,
                current_version, release_date, last_updated, minimum_os_version,
                content_rating, app_icon_url, app_url, screenshots, languages,
                in_app_purchases, file_size_bytes, retrieved_at
            ) VALUES (
                :app_id, :store, :app_name, :bundle_id, :developer_name, :developer_id,
                :description, :category, :price, :currency, :current_rating, :rating_count,
                :current_version, :release_date, :last_updated, :minimum_os_version,
                :content_rating, :app_icon_url, :app_url, CAST(:screenshots AS jsonb),
                CAST(:languages AS jsonb), :in_app_purchases, :file_size_bytes, NOW()
            )
            ON CONFLICT (app_id, store) DO UPDATE SET
                app_name = EXCLUDED.app_name,
                bundle_id = EXCLUDED.bundle_id,
                developer_name = EXCLUDED.developer_name,
                description = EXCLUDED.description,
                category = EXCLUDED.category,
                price = EXCLUDED.price,
                current_rating = EXCLUDED.current_rating,
                rating_count = EXCLUDED.rating_count,
                current_version = EXCLUDED.current_version,
                last_updated = EXCLUDED.last_updated,
                app_icon_url = EXCLUDED.app_icon_url,
                screenshots = EXCLUDED.screenshots,
                file_size_bytes = EXCLUDED.file_size_bytes,
                retrieved_at = NOW()
        """)

        try:
            release_date = None
            if app_data.get("release_date"):
                try:
                    release_date = datetime.strptime(
                        app_data["release_date"], "%Y-%m-%d"
                    ).date()
                except (ValueError, TypeError):
                    pass

            last_updated = None
            if app_data.get("last_updated"):
                try:
                    last_updated = datetime.strptime(
                        app_data["last_updated"], "%Y-%m-%d"
                    ).date()
                except (ValueError, TypeError):
                    pass

            self.db.execute(
                query,
                {
                    "app_id": app_data["app_id"],
                    "store": store,
                    "app_name": app_data["app_name"],
                    "bundle_id": app_data.get("bundle_id"),
                    "developer_name": app_data.get("developer_name"),
                    "developer_id": app_data.get("developer_id"),
                    "description": app_data.get("description"),
                    "category": app_data.get("category"),
                    "price": app_data.get("price", 0),
                    "currency": app_data.get("currency", "USD"),
                    "current_rating": app_data.get("current_rating"),
                    "rating_count": app_data.get("rating_count"),
                    "current_version": app_data.get("current_version"),
                    "release_date": release_date,
                    "last_updated": last_updated,
                    "minimum_os_version": app_data.get("minimum_os_version"),
                    "content_rating": app_data.get("content_rating"),
                    "app_icon_url": app_data.get("app_icon_url"),
                    "app_url": app_data.get("app_url"),
                    "screenshots": json.dumps(app_data.get("screenshots", [])),
                    "languages": json.dumps(app_data.get("languages", [])),
                    "in_app_purchases": app_data.get("in_app_purchases", False),
                    "file_size_bytes": app_data.get("file_size_bytes"),
                },
            )

            # Record rating history
            if app_data.get("current_rating"):
                self._record_rating(
                    app_data["app_id"],
                    store,
                    app_data["current_rating"],
                    app_data.get("rating_count"),
                )

            self.db.commit()
        except Exception as e:
            logger.error(f"Error saving app: {e}")
            self.db.rollback()

    def _record_rating(
        self, app_id: str, store: str, rating: float, rating_count: Optional[int]
    ) -> None:
        """Record rating history entry."""
        query = text("""
            INSERT INTO app_store_rating_history (app_id, store, rating, rating_count)
            VALUES (:app_id, :store, :rating, :rating_count)
        """)
        self.db.execute(
            query,
            {
                "app_id": app_id,
                "store": store,
                "rating": rating,
                "rating_count": rating_count,
            },
        )

    def get_app(self, app_id: str, store: str = "ios") -> Optional[Dict[str, Any]]:
        """
        Get app from database.

        Args:
            app_id: App ID
            store: Store (ios, android)

        Returns:
            App data or None
        """
        query = text("""
            SELECT * FROM app_store_apps
            WHERE app_id = :app_id AND store = :store
        """)

        result = self.db.execute(query, {"app_id": app_id, "store": store})
        row = result.mappings().fetchone()

        if not row:
            return None

        return {
            "app_id": row["app_id"],
            "store": row["store"],
            "app_name": row["app_name"],
            "bundle_id": row["bundle_id"],
            "developer": {
                "name": row["developer_name"],
                "id": row["developer_id"],
            },
            "description": row["description"],
            "category": row["category"],
            "pricing": {
                "price": row["price"],
                "currency": row["currency"],
                "in_app_purchases": row["in_app_purchases"],
            },
            "ratings": {
                "current": row["current_rating"],
                "count": row["rating_count"],
            },
            "version": {
                "current": row["current_version"],
                "release_date": row["release_date"].isoformat()
                if row["release_date"]
                else None,
                "last_updated": row["last_updated"].isoformat()
                if row["last_updated"]
                else None,
                "minimum_os": row["minimum_os_version"],
            },
            "content_rating": row["content_rating"],
            "media": {
                "icon_url": row["app_icon_url"],
                "app_url": row["app_url"],
                "screenshots": row["screenshots"],
            },
            "languages": row["languages"],
            "file_size_bytes": row["file_size_bytes"],
            "retrieved_at": row["retrieved_at"].isoformat() + "Z"
            if row["retrieved_at"]
            else None,
        }

    def get_rating_history(
        self, app_id: str, store: str = "ios", limit: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Get rating history for an app.

        Args:
            app_id: App ID
            store: Store
            limit: Max records

        Returns:
            List of rating history entries
        """
        query = text("""
            SELECT rating, rating_count, recorded_at
            FROM app_store_rating_history
            WHERE app_id = :app_id AND store = :store
            ORDER BY recorded_at DESC
            LIMIT :limit
        """)

        result = self.db.execute(
            query,
            {
                "app_id": app_id,
                "store": store,
                "limit": limit,
            },
        )

        return [
            {
                "rating": row["rating"],
                "rating_count": row["rating_count"],
                "recorded_at": row["recorded_at"].isoformat() + "Z"
                if row["recorded_at"]
                else None,
            }
            for row in result.mappings()
        ]

    def record_ranking(
        self,
        app_id: str,
        store: str,
        rank_position: int,
        rank_type: str = "top_free",
        category: Optional[str] = None,
        country: str = "us",
    ) -> None:
        """
        Record app ranking position.

        Args:
            app_id: App ID
            store: Store
            rank_position: Ranking position
            rank_type: Type (top_free, top_paid, top_grossing)
            category: Category (or overall)
            country: Country code
        """
        query = text("""
            INSERT INTO app_store_rankings (
                app_id, store, country, category, rank_type, rank_position
            ) VALUES (
                :app_id, :store, :country, :category, :rank_type, :rank_position
            )
        """)

        self.db.execute(
            query,
            {
                "app_id": app_id,
                "store": store,
                "country": country,
                "category": category,
                "rank_type": rank_type,
                "rank_position": rank_position,
            },
        )
        self.db.commit()

    def get_ranking_history(
        self,
        app_id: str,
        store: str = "ios",
        rank_type: Optional[str] = None,
        limit: int = 30,
    ) -> List[Dict[str, Any]]:
        """
        Get ranking history for an app.

        Args:
            app_id: App ID
            store: Store
            rank_type: Filter by rank type
            limit: Max records

        Returns:
            List of ranking history entries
        """
        conditions = ["app_id = :app_id", "store = :store"]
        params = {"app_id": app_id, "store": store, "limit": limit}

        if rank_type:
            conditions.append("rank_type = :rank_type")
            params["rank_type"] = rank_type

        where_clause = " AND ".join(conditions)

        query = text(f"""
            SELECT country, category, rank_type, rank_position, recorded_at
            FROM app_store_rankings
            WHERE {where_clause}
            ORDER BY recorded_at DESC
            LIMIT :limit
        """)

        result = self.db.execute(query, params)

        return [
            {
                "country": row["country"],
                "category": row["category"],
                "rank_type": row["rank_type"],
                "rank_position": row["rank_position"],
                "recorded_at": row["recorded_at"].isoformat() + "Z"
                if row["recorded_at"]
                else None,
            }
            for row in result.mappings()
        ]

    def link_app_to_company(
        self,
        company_name: str,
        app_id: str,
        store: str = "ios",
        relationship: str = "owner",
    ) -> Dict[str, Any]:
        """
        Link an app to a company.

        Args:
            company_name: Company name
            app_id: App ID
            store: Store
            relationship: Relationship type (owner, subsidiary, acquired)

        Returns:
            Result dict
        """
        query = text("""
            INSERT INTO company_app_portfolios (company_name, app_id, store, relationship)
            VALUES (:company_name, :app_id, :store, :relationship)
            ON CONFLICT (company_name, app_id, store) DO UPDATE SET
                relationship = EXCLUDED.relationship
        """)

        self.db.execute(
            query,
            {
                "company_name": company_name,
                "app_id": app_id,
                "store": store,
                "relationship": relationship,
            },
        )
        self.db.commit()

        return {
            "status": "linked",
            "company_name": company_name,
            "app_id": app_id,
            "store": store,
            "relationship": relationship,
        }

    def get_company_apps(self, company_name: str) -> Dict[str, Any]:
        """
        Get all apps linked to a company.

        Args:
            company_name: Company name

        Returns:
            Company app portfolio
        """
        query = text("""
            SELECT cap.app_id, cap.store, cap.relationship,
                   asa.app_name, asa.category, asa.current_rating,
                   asa.rating_count, asa.price, asa.app_icon_url
            FROM company_app_portfolios cap
            LEFT JOIN app_store_apps asa ON cap.app_id = asa.app_id AND cap.store = asa.store
            WHERE LOWER(cap.company_name) = LOWER(:company_name)
        """)

        result = self.db.execute(query, {"company_name": company_name})
        rows = result.mappings().fetchall()

        apps = []
        for row in rows:
            apps.append(
                {
                    "app_id": row["app_id"],
                    "store": row["store"],
                    "relationship": row["relationship"],
                    "app_name": row["app_name"],
                    "category": row["category"],
                    "rating": row["current_rating"],
                    "rating_count": row["rating_count"],
                    "price": row["price"],
                    "icon_url": row["app_icon_url"],
                }
            )

        return {
            "company_name": company_name,
            "app_count": len(apps),
            "apps": apps,
        }

    def search_apps_by_developer(self, developer_name: str) -> List[Dict[str, Any]]:
        """
        Search apps by developer name.

        Args:
            developer_name: Developer/company name

        Returns:
            List of apps
        """
        query = text("""
            SELECT app_id, store, app_name, category, current_rating,
                   rating_count, price, app_icon_url
            FROM app_store_apps
            WHERE LOWER(developer_name) LIKE LOWER(:pattern)
            ORDER BY rating_count DESC NULLS LAST
        """)

        result = self.db.execute(query, {"pattern": f"%{developer_name}%"})

        return [
            {
                "app_id": row["app_id"],
                "store": row["store"],
                "app_name": row["app_name"],
                "category": row["category"],
                "rating": row["current_rating"],
                "rating_count": row["rating_count"],
                "price": row["price"],
                "icon_url": row["app_icon_url"],
            }
            for row in result.mappings()
        ]

    def get_top_apps(
        self,
        store: str = "ios",
        category: Optional[str] = None,
        sort_by: str = "rating_count",
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        Get top apps from database.

        Args:
            store: Store filter
            category: Category filter
            sort_by: Sort field (rating_count, current_rating)
            limit: Max results

        Returns:
            List of top apps
        """
        conditions = ["store = :store"]
        params = {"store": store, "limit": limit}

        if category:
            conditions.append("LOWER(category) = LOWER(:category)")
            params["category"] = category

        where_clause = " AND ".join(conditions)

        # Validate sort field
        sort_column = "rating_count" if sort_by == "rating_count" else "current_rating"

        query = text(f"""
            SELECT app_id, store, app_name, developer_name, category,
                   current_rating, rating_count, price, app_icon_url
            FROM app_store_apps
            WHERE {where_clause}
            ORDER BY {sort_column} DESC NULLS LAST
            LIMIT :limit
        """)

        result = self.db.execute(query, params)

        return [
            {
                "app_id": row["app_id"],
                "store": row["store"],
                "app_name": row["app_name"],
                "developer": row["developer_name"],
                "category": row["category"],
                "rating": row["current_rating"],
                "rating_count": row["rating_count"],
                "price": row["price"],
                "icon_url": row["app_icon_url"],
            }
            for row in result.mappings()
        ]

    def upsert_android_app(self, app_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Add or update Android app data (manual entry).

        Args:
            app_data: App data dict

        Returns:
            Saved app data
        """
        app_data["store"] = "android"
        self._save_app(app_data, "android")
        return self.get_app(app_data["app_id"], "android")

    def compare_apps(self, app_ids: List[Dict[str, str]]) -> Dict[str, Any]:
        """
        Compare multiple apps.

        Args:
            app_ids: List of {"app_id": str, "store": str}

        Returns:
            Comparison data
        """
        comparison = []

        for app_ref in app_ids:
            app = self.get_app(app_ref["app_id"], app_ref.get("store", "ios"))
            if app:
                comparison.append(
                    {
                        "app_id": app["app_id"],
                        "store": app["store"],
                        "app_name": app["app_name"],
                        "developer": app["developer"]["name"],
                        "category": app["category"],
                        "rating": app["ratings"]["current"],
                        "rating_count": app["ratings"]["count"],
                        "price": app["pricing"]["price"],
                        "last_updated": app["version"]["last_updated"],
                    }
                )
            else:
                comparison.append(
                    {
                        "app_id": app_ref["app_id"],
                        "store": app_ref.get("store", "ios"),
                        "error": "Not found",
                    }
                )

        # Sort by rating count
        comparison.sort(key=lambda x: x.get("rating_count") or 0, reverse=True)

        return {
            "app_count": len(comparison),
            "comparison": comparison,
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        stats_query = text("""
            SELECT
                COUNT(*) as total_apps,
                COUNT(*) FILTER (WHERE store = 'ios') as ios_apps,
                COUNT(*) FILTER (WHERE store = 'android') as android_apps,
                COUNT(DISTINCT developer_name) as unique_developers,
                AVG(current_rating) as avg_rating,
                SUM(rating_count) as total_ratings
            FROM app_store_apps
        """)

        result = self.db.execute(stats_query)
        row = result.mappings().fetchone()

        return {
            "total_apps": row["total_apps"],
            "ios_apps": row["ios_apps"],
            "android_apps": row["android_apps"],
            "unique_developers": row["unique_developers"],
            "avg_rating": round(row["avg_rating"], 2) if row["avg_rating"] else None,
            "total_ratings": row["total_ratings"],
        }
