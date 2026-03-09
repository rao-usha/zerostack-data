"""
Vertical Discovery Collector — generic Yelp search + ZIP score + persist.

Generalized from MedSpaDiscoveryCollector. Parameterized by VerticalConfig
so the same engine works for dental, vet, HVAC, car wash, PT, etc.
"""

import logging
import math
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sources.vertical_discovery.configs import (
    DEFAULT_PRICE_SCORE,
    GRADE_THRESHOLDS,
    PRICE_SCORE_MAP,
    VerticalConfig,
)
from app.sources.vertical_discovery.metadata import generate_create_prospects_sql
from app.sources.yelp.client import YelpClient
from app.sources.yelp.metadata import parse_business_search_response

logger = logging.getLogger(__name__)


class VerticalDiscoveryCollector:
    """Discover and score acquisition prospects for any vertical via Yelp + ZIP data."""

    def __init__(self, db: Session, config: VerticalConfig):
        self.db = db
        self.config = config
        self._ensure_tables()

    # ------------------------------------------------------------------
    # Table setup
    # ------------------------------------------------------------------

    def _ensure_tables(self) -> None:
        from app.core.database import get_engine
        try:
            engine = get_engine()
            raw_conn = engine.raw_connection()
            try:
                cursor = raw_conn.cursor()
                cursor.execute(generate_create_prospects_sql(self.config))
                raw_conn.commit()
            finally:
                raw_conn.close()
        except Exception as e:
            logger.warning(f"{self.config.table_name} table creation warning: {e}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_grade(score: float) -> str:
        for threshold, grade in GRADE_THRESHOLDS:
            if score >= threshold:
                return grade
        return "F"

    @staticmethod
    def _percentile_rank(values: List[float]) -> List[float]:
        n = len(values)
        if n == 0:
            return []
        indexed = sorted(range(n), key=lambda i: values[i])
        ranks = [0.0] * n
        for rank_pos, original_idx in enumerate(indexed):
            ranks[original_idx] = (rank_pos / (n - 1)) * 100.0 if n > 1 else 50.0
        return ranks

    # ------------------------------------------------------------------
    # Discovery pipeline
    # ------------------------------------------------------------------

    async def discover(
        self,
        api_key: str,
        limit: int = 100,
        states: Optional[List[str]] = None,
        search_terms: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Main discovery: fetch top ZIPs → Yelp search → score → persist."""

        # 1. Fetch qualifying ZIPs (from IRS SOI data — no vertical-specific scorer needed)
        top_zips = self._fetch_top_zips(limit=limit, states=states)
        if not top_zips:
            return {"error": "No qualifying ZIPs found", "total_discovered": 0}

        batch_id = f"{self.config.slug}_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}"
        terms = search_terms or self.config.search_terms

        # 2. Build ZIP lookup for scoring context
        zip_lookup: Dict[str, Dict] = {z["zip_code"]: z for z in top_zips}

        # 3. Search Yelp and dedup
        all_businesses: List[Dict] = []
        client = YelpClient(api_key=api_key)

        async with client:
            for z in top_zips:
                for term in terms:
                    try:
                        results = await client.search_businesses(
                            location=z["zip_code"],
                            term=term,
                            categories=self.config.yelp_categories,
                            limit=50,
                        )
                        parsed = parse_business_search_response(results)
                        all_businesses.extend(parsed.get("businesses", []))
                    except Exception as e:
                        logger.warning(
                            f"Yelp search failed for ZIP {z['zip_code']}, "
                            f"term '{term}': {e}"
                        )

        # Dedup by yelp_id
        seen_ids: set = set()
        unique: List[Dict] = []
        for biz in all_businesses:
            bid = biz.get("id")
            if bid and bid not in seen_ids:
                seen_ids.add(bid)
                unique.append(biz)

        if not unique:
            return {
                "batch_id": batch_id,
                "total_discovered": 0,
                "zips_searched": len(top_zips),
            }

        # 4. Score prospects
        scored = self._score_prospects(unique, zip_lookup)

        # 5. Persist
        self._bulk_save(scored, batch_id=batch_id, search_term=", ".join(terms))

        # 6. Summary
        grade_dist = Counter(r["acquisition_grade"] for r in scored)
        return {
            "vertical": self.config.slug,
            "batch_id": batch_id,
            "zips_searched": len(top_zips),
            "total_discovered": len(all_businesses),
            "unique_businesses": len(unique),
            "scored_and_saved": len(scored),
            "grade_distribution": dict(grade_dist),
        }

    # ------------------------------------------------------------------
    # ZIP selection (IRS SOI based — works for all verticals)
    # ------------------------------------------------------------------

    def _fetch_top_zips(
        self,
        limit: int = 100,
        states: Optional[List[str]] = None,
        min_returns: int = 50,
    ) -> List[Dict]:
        """Fetch top ZIPs by AGI from IRS SOI data."""
        where = ["total_returns > :min_ret"]
        params: Dict[str, Any] = {"min_ret": min_returns, "lim": limit}

        if states:
            placeholders = ", ".join(f":s{i}" for i in range(len(states)))
            where.append(f"state_abbr IN ({placeholders})")
            for i, s in enumerate(states):
                params[f"s{i}"] = s.upper()

        where_sql = " AND ".join(where)

        query = text(f"""
            SELECT zip_code, state_abbr, avg_agi, total_returns,
                   PERCENT_RANK() OVER (ORDER BY avg_agi) * 100 AS overall_score
            FROM irs_soi_zip_income
            WHERE {where_sql}
            ORDER BY avg_agi DESC
            LIMIT :lim
        """)

        try:
            rows = self.db.execute(query, params).mappings().fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.error(f"Error fetching top ZIPs: {e}")
            self.db.rollback()
            return []

    # ------------------------------------------------------------------
    # Prospect scoring
    # ------------------------------------------------------------------

    def _score_prospects(
        self, businesses: List[Dict], zip_lookup: Dict[str, Dict]
    ) -> List[Dict]:
        """Score each business as an acquisition prospect."""
        weights = self.config.prospect_weights

        # Count competitors per ZIP
        zip_counts: Dict[str, int] = Counter()
        for biz in businesses:
            loc = biz.get("location", {})
            zc = loc.get("zip_code", "")[:5]
            if zc:
                zip_counts[zc] += 1

        # Collect raw signals
        rating_raw = []
        review_raw = []
        for biz in businesses:
            rating_raw.append(float(biz.get("rating") or 0))
            review_raw.append(float(biz.get("review_count") or 0))

        # Log-transform reviews for percentile ranking
        review_log = [math.log(r + 1) for r in review_raw]

        rating_pct = self._percentile_rank(rating_raw)
        review_pct = self._percentile_rank(review_log)

        records = []
        for i, biz in enumerate(businesses):
            loc = biz.get("location", {})
            zc = loc.get("zip_code", "")[:5]
            zip_data = zip_lookup.get(zc, {})

            # Sub-scores
            affluence_sub = float(zip_data.get("overall_score", 50))
            rating_sub = rating_pct[i]
            review_sub = review_pct[i]

            # Low competition: inverse of competitor count
            comp_count = zip_counts.get(zc, 1)
            competition_sub = max(0, 100 - (comp_count - 1) * 15)

            # Price tier
            price = biz.get("price")
            price_sub = PRICE_SCORE_MAP.get(price, DEFAULT_PRICE_SCORE)

            # Weighted composite
            overall = (
                affluence_sub * weights.get("zip_affluence", 0.25)
                + rating_sub * weights.get("yelp_rating", 0.25)
                + review_sub * weights.get("review_volume", 0.20)
                + competition_sub * weights.get("low_competition", 0.15)
                + price_sub * weights.get("price_tier", 0.10)
            )
            overall = max(0.0, min(100.0, round(overall, 2)))
            grade = self._get_grade(overall)

            # Build categories array
            cats = biz.get("categories", [])
            cat_aliases = [c.get("alias", "") for c in cats] if isinstance(cats, list) else []

            addr_parts = loc.get("display_address", [])
            address = ", ".join(addr_parts) if isinstance(addr_parts, list) else str(addr_parts)

            records.append({
                "yelp_id": biz.get("id"),
                "name": biz.get("name", ""),
                "alias": biz.get("alias"),
                "rating": biz.get("rating"),
                "review_count": biz.get("review_count", 0),
                "price": price,
                "phone": biz.get("phone"),
                "url": biz.get("url"),
                "image_url": biz.get("image_url"),
                "latitude": biz.get("coordinates", {}).get("latitude"),
                "longitude": biz.get("coordinates", {}).get("longitude"),
                "address": address,
                "city": loc.get("city"),
                "state": loc.get("state"),
                "zip_code": zc,
                "categories": (
                    "{" + ",".join('"' + a.replace('"', '\\"') + '"' for a in cat_aliases) + "}"
                ),
                "is_closed": biz.get("is_closed", False),
                "zip_overall_score": zip_data.get("overall_score"),
                "zip_grade": None,
                "zip_affluence_density": None,
                "zip_total_returns": zip_data.get("total_returns"),
                "zip_avg_agi": zip_data.get("avg_agi"),
                "acquisition_score": overall,
                "acquisition_grade": grade,
                "zip_affluence_sub": round(affluence_sub, 2),
                "yelp_rating_sub": round(rating_sub, 2),
                "review_volume_sub": round(review_sub, 2),
                "low_competition_sub": round(competition_sub, 2),
                "price_tier_sub": round(price_sub, 2),
                "competitor_count_in_zip": comp_count,
            })

        return records

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _bulk_save(
        self, records: List[Dict], batch_id: str, search_term: str
    ) -> None:
        if not records:
            return

        t = self.config.table_name
        upsert_sql = text(f"""
            INSERT INTO {t} (
                yelp_id, name, alias, rating, review_count, price, phone,
                url, image_url, latitude, longitude, address, city, state,
                zip_code, categories, is_closed,
                zip_overall_score, zip_grade, zip_affluence_density,
                zip_total_returns, zip_avg_agi,
                acquisition_score, acquisition_grade,
                zip_affluence_sub, yelp_rating_sub, review_volume_sub,
                low_competition_sub, price_tier_sub, competitor_count_in_zip,
                search_term, batch_id, model_version
            ) VALUES (
                :yelp_id, :name, :alias, :rating, :review_count, :price, :phone,
                :url, :image_url, :latitude, :longitude, :address, :city, :state,
                :zip_code, :categories, :is_closed,
                :zip_overall_score, :zip_grade, :zip_affluence_density,
                :zip_total_returns, :zip_avg_agi,
                :acquisition_score, :acquisition_grade,
                :zip_affluence_sub, :yelp_rating_sub, :review_volume_sub,
                :low_competition_sub, :price_tier_sub, :competitor_count_in_zip,
                :search_term, :batch_id, :model_version
            )
            ON CONFLICT (yelp_id) DO UPDATE SET
                rating = EXCLUDED.rating,
                review_count = EXCLUDED.review_count,
                price = EXCLUDED.price,
                is_closed = EXCLUDED.is_closed,
                acquisition_score = EXCLUDED.acquisition_score,
                acquisition_grade = EXCLUDED.acquisition_grade,
                zip_affluence_sub = EXCLUDED.zip_affluence_sub,
                yelp_rating_sub = EXCLUDED.yelp_rating_sub,
                review_volume_sub = EXCLUDED.review_volume_sub,
                low_competition_sub = EXCLUDED.low_competition_sub,
                price_tier_sub = EXCLUDED.price_tier_sub,
                competitor_count_in_zip = EXCLUDED.competitor_count_in_zip,
                updated_at = NOW()
        """)

        batch_size = 500
        total_saved = 0
        try:
            for start in range(0, len(records), batch_size):
                batch = records[start:start + batch_size]
                for rec in batch:
                    rec["search_term"] = search_term
                    rec["batch_id"] = batch_id
                    rec["model_version"] = self.config.model_version
                    self.db.execute(upsert_sql, rec)
                self.db.commit()
                total_saved += len(batch)
            logger.info(
                f"Saved {total_saved} {self.config.display_name} prospects"
            )
        except Exception as e:
            logger.error(f"Error saving {self.config.slug} prospects: {e}")
            self.db.rollback()
