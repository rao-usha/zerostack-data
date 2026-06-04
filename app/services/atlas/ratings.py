"""SPEC_101 — Google Places ratings layered onto SPEC_100's CBP count.

Public API:
  fetch_ratings(db, geo_id, naics) -> Optional[RatingSummary]

Returns the median/mean/distribution of Google Places ratings for
locations matching `naics` near the focal county's centroid. Returns
None when:
  - GOOGLE_PLACES_API_KEY is unset
  - naics has no Google Place Type mapping
  - the focal county has no known centroid
  - Google returns 0 places
  - any soft-fail in the underlying client

ToS-driven design:
  - Cache results in `google_places_cache` for up to 30 days.
  - Cache stores aggregates only (no place_id leakage).
  - Never display review TEXT — rating + count only.
  - Caller must show "Powered by Google" attribution when displaying.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from statistics import mean, median
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# ToS-mandated max cache age.
_CACHE_MAX_AGE = timedelta(days=30)

# Search radius around the focal county centroid. ~5 mi captures the
# county-scale neighborhood without straying into other counties.
_DEFAULT_RADIUS_M = 8_000.0


@dataclass
class RatingSummary:
    count: int
    mean_rating: Optional[float]
    median_rating: Optional[float]
    rating_distribution: Dict[int, int] = field(default_factory=dict)
    total_user_ratings: int = 0
    source: str = "google_places"

    def to_response(self) -> Dict[str, Any]:
        """The dict shape the /atlas/competition endpoint embeds."""
        return {
            "count": int(self.count),
            "mean_rating": (round(self.mean_rating, 2)
                             if self.mean_rating is not None else None),
            "median_rating": (round(self.median_rating, 2)
                               if self.median_rating is not None else None),
            "rating_distribution": dict(self.rating_distribution),
            "total_user_ratings": int(self.total_user_ratings),
            "source": self.source,
            "attribution": "Powered by Google",
        }


# ─── Cache table ───────────────────────────────────────────────────────


def _create_cache_table_sql() -> str:
    """SPEC_101 cache table. Aggregates only — no place_id retention."""
    return """
    CREATE TABLE IF NOT EXISTS google_places_cache (
        geo_id        TEXT NOT NULL,
        naics_code    TEXT NOT NULL,
        asked_at      TIMESTAMP NOT NULL DEFAULT NOW(),
        summary_json  JSONB NOT NULL,
        PRIMARY KEY (geo_id, naics_code)
    );
    CREATE INDEX IF NOT EXISTS idx_gpc_asked_at ON google_places_cache(asked_at);
    """


def _ensure_table(db: Session) -> None:
    for stmt in _create_cache_table_sql().strip().split(";"):
        if stmt.strip():
            db.execute(text(stmt))
    db.commit()


def _read_cache(db: Session, geo_id: str, naics: str,
                 max_age: timedelta = _CACHE_MAX_AGE,
                 ) -> Optional[RatingSummary]:
    try:
        row = db.execute(text("""
            SELECT summary_json, asked_at
            FROM google_places_cache
            WHERE geo_id = :g AND naics_code = :n
        """), {"g": geo_id, "n": naics}).first()
    except Exception as exc:  # noqa: BLE001
        # First call before _ensure_table — pretend miss.
        logger.debug("ratings cache miss (no table yet): %s", exc)
        db.rollback()
        return None
    if not row:
        return None
    summary_json, asked_at = row
    if asked_at and (datetime.utcnow() - asked_at) >= max_age:
        return None
    payload = (json.loads(summary_json) if isinstance(summary_json, str)
                else summary_json)
    return RatingSummary(
        count=int(payload.get("count", 0)),
        mean_rating=payload.get("mean_rating"),
        median_rating=payload.get("median_rating"),
        rating_distribution={int(k): int(v)
                              for k, v in (payload.get("rating_distribution")
                                            or {}).items()},
        total_user_ratings=int(payload.get("total_user_ratings", 0)),
        source=payload.get("source", "google_places"),
    )


def _write_cache(db: Session, geo_id: str, naics: str,
                 summary: RatingSummary) -> None:
    try:
        _ensure_table(db)
        db.execute(text("""
            INSERT INTO google_places_cache (geo_id, naics_code, asked_at, summary_json)
            VALUES (:g, :n, NOW(), CAST(:j AS JSONB))
            ON CONFLICT (geo_id, naics_code) DO UPDATE
            SET asked_at = NOW(),
                summary_json = EXCLUDED.summary_json
        """), {"g": geo_id, "n": naics,
                "j": json.dumps(asdict(summary))})
        db.commit()
    except Exception as exc:  # noqa: BLE001
        logger.warning("ratings cache write failed: %s", exc)
        db.rollback()


# ─── Aggregator ────────────────────────────────────────────────────────


def _aggregate(places: List[Dict[str, Any]]) -> RatingSummary:
    """Build a RatingSummary from a list of Google place dicts.
    Discards individual place_id values after counting them."""
    ratings: List[float] = []
    total_user = 0
    dist: Dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    for p in places:
        r = p.get("rating")
        if r is None:
            continue
        try:
            r = float(r)
        except (TypeError, ValueError):
            continue
        ratings.append(r)
        bucket = max(1, min(5, int(round(r))))
        dist[bucket] += 1
        try:
            total_user += int(p.get("userRatingCount") or 0)
        except (TypeError, ValueError):
            pass
    return RatingSummary(
        count=len(ratings),
        mean_rating=(mean(ratings) if ratings else None),
        median_rating=(median(ratings) if ratings else None),
        rating_distribution=dist,
        total_user_ratings=total_user,
    )


# ─── Public entry point ───────────────────────────────────────────────


def fetch_ratings(db: Session, geo_id: str, naics: Optional[str],
                  radius_m: float = _DEFAULT_RADIUS_M,
                  max_results: int = 20,
                  ) -> Optional[RatingSummary]:
    """Top-level: return aggregate ratings for (geo_id, naics) or None.
    See module docstring for the soft-fail conditions."""
    from app.services.atlas.google_place_categories import (
        naics_to_google_category,
    )
    from app.sources.google_places import GooglePlacesClient
    if not naics:
        return None
    category = naics_to_google_category(naics)
    if not category:
        return None

    cached = _read_cache(db, geo_id, naics)
    if cached is not None:
        return cached

    cli = GooglePlacesClient()
    if not cli.has_key:
        return None

    # Need the focal county centroid for the searchNearby call.
    from app.services.atlas.trade_area import county_centroids
    centroids = county_centroids(db)
    centroid = centroids.get(geo_id)
    if not centroid:
        return None
    lat, lon, _name = centroid

    places = cli.search_nearby(
        lat=lat, lon=lon,
        radius_m=radius_m,
        included_types=[category],
        max_count=max_results,
    )
    if not places:
        return None

    summary = _aggregate(places)
    _write_cache(db, geo_id, naics, summary)
    return summary
