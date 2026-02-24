"""
Med-Spa Discovery Collector — Yelp search + ZIP scorer integration.

Reads top-scoring ZIP codes from zip_medspa_scores, searches Yelp for
med-spa businesses in each, scores every discovered business as an
acquisition prospect, and persists a ranked list into medspa_prospects.
"""

import logging
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sources.medspa_discovery.metadata import (
    DEFAULT_PRICE_SCORE,
    DEFAULT_SEARCH_TERMS,
    GRADE_THRESHOLDS,
    MEDSPA_CATEGORIES,
    MODEL_VERSION,
    PRICE_SCORE_MAP,
    PROSPECT_WEIGHTS,
    generate_create_medspa_prospects_sql,
)
from app.sources.yelp.client import YelpClient
from app.sources.yelp.metadata import parse_business_search_response

logger = logging.getLogger(__name__)


class MedSpaDiscoveryCollector:
    """Discover and score med-spa acquisition prospects via Yelp + ZIP data."""

    def __init__(self, db: Session):
        self.db = db
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
                cursor.execute(generate_create_medspa_prospects_sql())
                raw_conn.commit()
            finally:
                raw_conn.close()
        except Exception as e:
            logger.warning(f"medspa_prospects table creation warning: {e}")

    # ------------------------------------------------------------------
    # Helpers (reused from ZipMedSpaScorer pattern)
    # ------------------------------------------------------------------

    @staticmethod
    def _get_grade(score: float) -> str:
        for threshold, grade in GRADE_THRESHOLDS:
            if score >= threshold:
                return grade
        return "F"

    @staticmethod
    def _percentile_rank(values: List[float]) -> List[float]:
        """Return percentile ranks (0-100) for a list of values."""
        n = len(values)
        if n == 0:
            return []
        indexed = sorted(range(n), key=lambda i: values[i])
        ranks = [0.0] * n
        for rank_pos, original_idx in enumerate(indexed):
            ranks[original_idx] = (rank_pos / (n - 1)) * 100.0 if n > 1 else 50.0
        return ranks

    # ------------------------------------------------------------------
    # ZIP data retrieval
    # ------------------------------------------------------------------

    def _fetch_top_zips(
        self,
        limit: int = 100,
        states: Optional[List[str]] = None,
        min_grade: str = "B",
        min_score: float = 0.0,
    ) -> List[Dict[str, Any]]:
        """Query zip_medspa_scores for top ZIPs by overall_score."""

        # Map min_grade to acceptable grades
        grade_order = ["A", "B", "C", "D", "F"]
        try:
            grade_idx = grade_order.index(min_grade.upper())
        except ValueError:
            grade_idx = 1  # default to B
        acceptable_grades = grade_order[: grade_idx + 1]

        where_clauses = [
            "model_version = :ver",
            "overall_score >= :min_score",
        ]
        params: Dict[str, Any] = {
            "ver": MODEL_VERSION,
            "min_score": min_score,
            "lim": limit,
        }

        if states:
            placeholders = ", ".join(f":st{i}" for i in range(len(states)))
            where_clauses.append(f"state_abbr IN ({placeholders})")
            for i, st in enumerate(states):
                params[f"st{i}"] = st.upper()

        grade_placeholders = ", ".join(f":gr{i}" for i in range(len(acceptable_grades)))
        where_clauses.append(f"grade IN ({grade_placeholders})")
        for i, gr in enumerate(acceptable_grades):
            params[f"gr{i}"] = gr

        where_sql = " AND ".join(where_clauses)

        query = text(f"""
            SELECT * FROM (
                SELECT DISTINCT ON (zip_code)
                       zip_code, state_abbr, overall_score, grade,
                       affluence_density_score, total_returns, avg_agi
                FROM zip_medspa_scores
                WHERE {where_sql}
                ORDER BY zip_code, score_date DESC
            ) ranked
            ORDER BY ranked.overall_score DESC
            LIMIT :lim
        """)

        try:
            rows = self.db.execute(query, params).mappings().fetchall()
        except Exception as e:
            logger.error(f"Error fetching top ZIPs: {e}")
            self.db.rollback()
            return []

        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Yelp search per ZIP
    # ------------------------------------------------------------------

    async def _search_zip(
        self,
        client: YelpClient,
        zip_code: str,
        terms: List[str],
    ) -> List[Dict[str, Any]]:
        """Search Yelp for med-spa businesses in a single ZIP code."""
        all_results: List[Dict[str, Any]] = []

        for term in terms:
            try:
                raw = await client.search_businesses(
                    location=zip_code,
                    term=term,
                    categories=MEDSPA_CATEGORIES,
                    limit=50,
                    sort_by="best_match",
                )
                parsed = parse_business_search_response(
                    raw, search_location=zip_code, search_term=term
                )
                all_results.extend(parsed)
            except Exception as e:
                logger.warning(f"Yelp search failed for ZIP {zip_code} term='{term}': {e}")

        return all_results

    # ------------------------------------------------------------------
    # Scoring
    # ------------------------------------------------------------------

    def _score_prospects(
        self,
        prospects: List[Dict[str, Any]],
        zip_lookup: Dict[str, Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """Compute acquisition scores with percentile ranks + weights."""
        if not prospects:
            return []

        # Normalize ZIP codes to 5 chars (Yelp sometimes returns ZIP+4)
        for p in prospects:
            zc = p.get("zip_code") or ""
            p["zip_code"] = zc[:5] if len(zc) > 5 else zc

        # Compute competitor_count_in_zip
        zip_counts: Counter = Counter(p.get("zip_code") for p in prospects)
        for p in prospects:
            p["competitor_count_in_zip"] = zip_counts.get(p.get("zip_code"), 0)

        # --- Raw values for percentile ranking ---
        review_counts = [float(p.get("review_count") or 0) for p in prospects]
        competitor_counts = [float(p.get("competitor_count_in_zip", 0)) for p in prospects]

        review_pcts = self._percentile_rank(review_counts)
        competition_pcts = self._percentile_rank(competitor_counts)

        scored = []
        for i, p in enumerate(prospects):
            biz_zip = p.get("zip_code")
            zip_info = zip_lookup.get(biz_zip, {})

            # Sub-score: ZIP affluence (use the ZIP's overall_score, already 0-100)
            zip_aff = float(zip_info.get("overall_score") or 50.0)

            # Sub-score: Yelp rating
            rating = float(p.get("rating") or 0)
            yelp_rating_sub = (rating / 5.0) * 100.0

            # Sub-score: Review volume (percentile)
            review_vol_sub = review_pcts[i]

            # Sub-score: Low competition (inverse percentile)
            low_comp_sub = 100.0 - competition_pcts[i]

            # Sub-score: Price tier
            price_str = p.get("price")
            price_sub = PRICE_SCORE_MAP.get(price_str, DEFAULT_PRICE_SCORE)

            # Weighted composite
            overall = (
                zip_aff * PROSPECT_WEIGHTS["zip_affluence"]
                + yelp_rating_sub * PROSPECT_WEIGHTS["yelp_rating"]
                + review_vol_sub * PROSPECT_WEIGHTS["review_volume"]
                + low_comp_sub * PROSPECT_WEIGHTS["low_competition"]
                + price_sub * PROSPECT_WEIGHTS["price_tier"]
            )
            overall = max(0.0, min(100.0, round(overall, 2)))
            grade = self._get_grade(overall)

            # Build full address string
            addr_parts = [p.get("address1")]
            if p.get("city"):
                addr_parts.append(p["city"])
            if p.get("state"):
                addr_parts.append(p["state"])
            address = ", ".join(part for part in addr_parts if part)

            scored.append({
                "yelp_id": p["yelp_id"],
                "name": p["name"],
                "alias": p.get("alias"),
                "rating": rating if rating > 0 else None,
                "review_count": p.get("review_count", 0),
                "price": price_str,
                "phone": p.get("phone"),
                "url": p.get("url"),
                "image_url": p.get("image_url"),
                "latitude": p.get("latitude"),
                "longitude": p.get("longitude"),
                "address": address,
                "city": p.get("city"),
                "state": p.get("state"),
                "zip_code": biz_zip,
                "categories": p.get("categories", []),
                "is_closed": p.get("is_closed", False),
                # ZIP score context
                "zip_overall_score": zip_info.get("overall_score"),
                "zip_grade": zip_info.get("grade"),
                "zip_affluence_density": zip_info.get("affluence_density_score"),
                "zip_total_returns": zip_info.get("total_returns"),
                "zip_avg_agi": zip_info.get("avg_agi"),
                # Acquisition score
                "acquisition_score": overall,
                "acquisition_grade": grade,
                # Sub-scores
                "zip_affluence_sub": round(zip_aff, 2),
                "yelp_rating_sub": round(yelp_rating_sub, 2),
                "review_volume_sub": round(review_vol_sub, 2),
                "low_competition_sub": round(low_comp_sub, 2),
                "price_tier_sub": round(float(price_sub), 2),
                # Competition
                "competitor_count_in_zip": p["competitor_count_in_zip"],
            })

        return scored

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def _bulk_save(self, records: List[Dict], batch_id: str, search_term: str) -> None:
        """Upsert scored prospect records into medspa_prospects."""
        if not records:
            return

        upsert_sql = text("""
            INSERT INTO medspa_prospects (
                yelp_id, name, alias, rating, review_count, price, phone,
                url, image_url, latitude, longitude, address, city, state,
                zip_code, categories, is_closed,
                zip_overall_score, zip_grade, zip_affluence_density,
                zip_total_returns, zip_avg_agi,
                acquisition_score, acquisition_grade,
                zip_affluence_sub, yelp_rating_sub, review_volume_sub,
                low_competition_sub, price_tier_sub,
                competitor_count_in_zip,
                search_term, batch_id, model_version, updated_at
            ) VALUES (
                :yelp_id, :name, :alias, :rating, :review_count, :price, :phone,
                :url, :image_url, :latitude, :longitude, :address, :city, :state,
                :zip_code, :categories, :is_closed,
                :zip_overall_score, :zip_grade, :zip_affluence_density,
                :zip_total_returns, :zip_avg_agi,
                :acquisition_score, :acquisition_grade,
                :zip_affluence_sub, :yelp_rating_sub, :review_volume_sub,
                :low_competition_sub, :price_tier_sub,
                :competitor_count_in_zip,
                :search_term, :batch_id, :model_version, NOW()
            )
            ON CONFLICT (yelp_id) DO UPDATE SET
                name = EXCLUDED.name,
                alias = EXCLUDED.alias,
                rating = EXCLUDED.rating,
                review_count = EXCLUDED.review_count,
                price = EXCLUDED.price,
                phone = EXCLUDED.phone,
                url = EXCLUDED.url,
                image_url = EXCLUDED.image_url,
                latitude = EXCLUDED.latitude,
                longitude = EXCLUDED.longitude,
                address = EXCLUDED.address,
                city = EXCLUDED.city,
                state = EXCLUDED.state,
                zip_code = EXCLUDED.zip_code,
                categories = EXCLUDED.categories,
                is_closed = EXCLUDED.is_closed,
                zip_overall_score = EXCLUDED.zip_overall_score,
                zip_grade = EXCLUDED.zip_grade,
                zip_affluence_density = EXCLUDED.zip_affluence_density,
                zip_total_returns = EXCLUDED.zip_total_returns,
                zip_avg_agi = EXCLUDED.zip_avg_agi,
                acquisition_score = EXCLUDED.acquisition_score,
                acquisition_grade = EXCLUDED.acquisition_grade,
                zip_affluence_sub = EXCLUDED.zip_affluence_sub,
                yelp_rating_sub = EXCLUDED.yelp_rating_sub,
                review_volume_sub = EXCLUDED.review_volume_sub,
                low_competition_sub = EXCLUDED.low_competition_sub,
                price_tier_sub = EXCLUDED.price_tier_sub,
                competitor_count_in_zip = EXCLUDED.competitor_count_in_zip,
                search_term = EXCLUDED.search_term,
                batch_id = EXCLUDED.batch_id,
                model_version = EXCLUDED.model_version,
                updated_at = NOW()
        """)

        batch_size = 500
        total_saved = 0
        try:
            for start in range(0, len(records), batch_size):
                batch = records[start : start + batch_size]
                for rec in batch:
                    # Ensure categories is cast properly for TEXT[]
                    params = dict(rec)
                    params["batch_id"] = batch_id
                    params["search_term"] = search_term
                    params["model_version"] = MODEL_VERSION
                    # Convert Python list to PostgreSQL TEXT[] literal
                    cats = params.get("categories")
                    if isinstance(cats, list):
                        params["categories"] = "{" + ",".join(str(c) for c in cats) + "}"
                    elif cats is None:
                        params["categories"] = "{}"
                    self.db.execute(upsert_sql, params)
                self.db.commit()
                total_saved += len(batch)
            logger.info(f"Saved {total_saved} med-spa prospects")
            print(f"[medspa-discovery] Saved {total_saved} prospects to DB", flush=True)
        except Exception as e:
            logger.error(f"Error bulk-saving med-spa prospects: {e}")
            print(f"[medspa-discovery] SAVE ERROR: {e}", flush=True)
            import traceback; traceback.print_exc()
            self.db.rollback()

    # ------------------------------------------------------------------
    # Main orchestrator
    # ------------------------------------------------------------------

    async def discover(
        self,
        api_key: str,
        limit: int = 100,
        states: Optional[List[str]] = None,
        min_grade: str = "B",
        min_score: float = 0.0,
        search_terms: Optional[List[str]] = None,
        max_api_calls: int = 400,
    ) -> Dict[str, Any]:
        """
        Main discovery flow: fetch top ZIPs -> search Yelp -> score -> persist.

        Returns summary with counts, grade distribution, and top prospects.
        """
        terms = search_terms or DEFAULT_SEARCH_TERMS
        calls_per_zip = len(terms)

        # 1. Fetch top ZIPs
        top_zips = self._fetch_top_zips(
            limit=limit,
            states=states,
            min_grade=min_grade,
            min_score=min_score,
        )
        if not top_zips:
            return {
                "error": "No qualifying ZIP codes found. Run ZIP scorer first.",
                "total_discovered": 0,
            }

        # 2. Budget check
        max_zips = min(len(top_zips), max_api_calls // calls_per_zip)
        zips_to_search = top_zips[:max_zips]

        batch_id = f"medspa_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}"
        logger.info(
            f"Med-spa discovery: searching {len(zips_to_search)} ZIPs "
            f"({calls_per_zip} calls/ZIP, batch={batch_id})"
        )

        # 3. Build ZIP lookup for scoring context
        zip_lookup: Dict[str, Dict[str, Any]] = {}
        for z in top_zips:
            zip_lookup[z["zip_code"]] = z

        # 4. Search Yelp for each ZIP
        client = YelpClient(api_key=api_key)
        all_businesses: List[Dict[str, Any]] = []
        api_calls_used = 0

        async with client:
            for idx, z in enumerate(zips_to_search):
                zip_code = z["zip_code"]
                results = await self._search_zip(client, zip_code, terms)
                all_businesses.extend(results)
                api_calls_used += calls_per_zip

                if (idx + 1) % 10 == 0:
                    logger.info(
                        f"  ...searched {idx + 1}/{len(zips_to_search)} ZIPs, "
                        f"{len(all_businesses)} results so far"
                    )

        # 5. Dedup by yelp_id (keep first occurrence)
        seen_ids: set = set()
        unique_businesses: List[Dict[str, Any]] = []
        for biz in all_businesses:
            yid = biz.get("yelp_id")
            if yid and yid not in seen_ids:
                seen_ids.add(yid)
                unique_businesses.append(biz)

        logger.info(
            f"Found {len(all_businesses)} total, {len(unique_businesses)} unique businesses"
        )

        if not unique_businesses:
            return {
                "batch_id": batch_id,
                "total_discovered": 0,
                "unique_businesses": 0,
                "api_calls_used": api_calls_used,
                "zips_searched": len(zips_to_search),
            }

        # 6. Score prospects
        scored = self._score_prospects(unique_businesses, zip_lookup)

        # 7. Persist
        search_term_str = ", ".join(terms)
        self._bulk_save(scored, batch_id=batch_id, search_term=search_term_str)

        # 8. Build summary
        grade_dist: Dict[str, int] = {}
        for rec in scored:
            g = rec["acquisition_grade"]
            grade_dist[g] = grade_dist.get(g, 0) + 1

        top_10 = sorted(scored, key=lambda r: r["acquisition_score"], reverse=True)[:10]

        return {
            "batch_id": batch_id,
            "total_discovered": len(all_businesses),
            "unique_businesses": len(unique_businesses),
            "zips_searched": len(zips_to_search),
            "api_calls_used": api_calls_used,
            "grade_distribution": grade_dist,
            "top_10": [
                {
                    "yelp_id": r["yelp_id"],
                    "name": r["name"],
                    "city": r["city"],
                    "state": r["state"],
                    "zip_code": r["zip_code"],
                    "acquisition_score": r["acquisition_score"],
                    "acquisition_grade": r["acquisition_grade"],
                    "rating": r["rating"],
                    "review_count": r["review_count"],
                }
                for r in top_10
            ],
        }

    # ------------------------------------------------------------------
    # Static info
    # ------------------------------------------------------------------

    @staticmethod
    def get_methodology() -> Dict[str, Any]:
        """Return scoring methodology documentation."""
        return {
            "model_version": MODEL_VERSION,
            "description": (
                "Med-Spa Acquisition Prospect Score identifies and ranks "
                "med-spa businesses as PE acquisition targets by combining "
                "ZIP-level affluence data (IRS SOI) with Yelp business signals. "
                "Each sub-score is 0-100; the composite is a weighted sum."
            ),
            "use_case": (
                "Discover high-potential med-spa acquisition targets for a "
                "PE roll-up strategy by searching Yelp in the wealthiest ZIP codes."
            ),
            "sub_scores": [
                {
                    "name": "zip_affluence",
                    "weight": PROSPECT_WEIGHTS["zip_affluence"],
                    "description": (
                        "Overall ZIP med-spa revenue potential score (from "
                        "zip_medspa_scores). Already 0-100 percentile ranked."
                    ),
                },
                {
                    "name": "yelp_rating",
                    "weight": PROSPECT_WEIGHTS["yelp_rating"],
                    "description": (
                        "Yelp star rating scaled to 0-100. Higher-rated "
                        "businesses are better-run acquisition targets."
                    ),
                },
                {
                    "name": "review_volume",
                    "weight": PROSPECT_WEIGHTS["review_volume"],
                    "description": (
                        "Percentile rank of review count. More reviews = "
                        "more established/visible business."
                    ),
                },
                {
                    "name": "low_competition",
                    "weight": PROSPECT_WEIGHTS["low_competition"],
                    "description": (
                        "Inverse percentile of competitor count in ZIP. "
                        "Fewer competitors = more market upside."
                    ),
                },
                {
                    "name": "price_tier",
                    "weight": PROSPECT_WEIGHTS["price_tier"],
                    "description": (
                        "Yelp price tier mapped to score: $$$$ = 100, "
                        "$$$ = 75, $$ = 50, $ = 25, unknown = 37.5. "
                        "Premium positioning signals higher revenue per customer."
                    ),
                },
            ],
            "grade_thresholds": {
                "A": ">=80 — Top acquisition target",
                "B": ">=65 — Strong prospect",
                "C": ">=50 — Moderate prospect",
                "D": ">=35 — Below-average prospect",
                "F": "<35 — Weak prospect",
            },
            "data_sources": [
                "Yelp Fusion API (business search, ratings, reviews, pricing)",
                "ZIP Med-Spa Scores (IRS SOI income data, 27,604 ZIPs)",
            ],
        }

    @staticmethod
    def get_api_budget() -> Dict[str, Any]:
        """Return Yelp API budget info and estimates."""
        return {
            "daily_limit": YelpClient.DEFAULT_DAILY_LIMIT,
            "calls_per_zip": len(DEFAULT_SEARCH_TERMS),
            "search_terms": DEFAULT_SEARCH_TERMS,
            "categories_filter": MEDSPA_CATEGORIES,
            "max_results_per_call": 50,
            "estimate": {
                "10_zips": len(DEFAULT_SEARCH_TERMS) * 10,
                "50_zips": len(DEFAULT_SEARCH_TERMS) * 50,
                "100_zips": len(DEFAULT_SEARCH_TERMS) * 100,
                "200_zips": len(DEFAULT_SEARCH_TERMS) * 200,
            },
            "note": (
                "Yelp free tier allows 500 API calls/day. Each ZIP requires "
                f"{len(DEFAULT_SEARCH_TERMS)} calls (one per search term). "
                "Use max_api_calls param to stay within budget."
            ),
        }
