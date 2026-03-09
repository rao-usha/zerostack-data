"""
Vertical Enrichment Pipeline — generic NPPES + density + revenue enrichment.

Generalized from MedSpaEnrichmentPipeline. Skips NPPES phase for
non-healthcare verticals. Revenue model uses per-vertical benchmarks.
"""

import logging
import math
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sources.vertical_discovery.configs import (
    SATURATION_THRESHOLDS,
    VerticalConfig,
)

logger = logging.getLogger(__name__)


class VerticalEnrichmentPipeline:
    """Three-phase enrichment: NPPES (if healthcare) → Density → Revenue."""

    def __init__(self, db: Session, config: VerticalConfig):
        self.db = db
        self.config = config

    # ------------------------------------------------------------------
    # Orchestrator
    # ------------------------------------------------------------------

    def enrich_all(self, force: bool = False) -> Dict[str, Any]:
        """Run all enrichment phases in order."""
        results: Dict[str, Any] = {"vertical": self.config.slug}

        if self.config.has_nppes_enrichment:
            results["nppes"] = self.enrich_nppes(force=force)
        else:
            results["nppes"] = {"skipped": True, "reason": "Non-healthcare vertical"}

        results["density"] = self.enrich_competitive_density(force=force)
        results["revenue"] = self.estimate_revenue(force=force)
        return results

    # ------------------------------------------------------------------
    # Phase 1A: NPPES provider cross-reference
    # ------------------------------------------------------------------

    def enrich_nppes(self, force: bool = False) -> Dict[str, Any]:
        """Match prospects against NPPES providers (healthcare verticals only)."""
        if not self.config.has_nppes_enrichment:
            return {"skipped": True, "reason": "Non-healthcare vertical"}

        t = self.config.table_name
        taxonomy_codes = self.config.nppes_taxonomy_codes or []

        # Load prospects
        where = "1=1" if force else "nppes_enriched_at IS NULL"
        prospects = self._safe_query(
            f"SELECT yelp_id, name, address, zip_code, state FROM {t} WHERE {where}", {}
        )
        if not prospects:
            return {"enriched": 0, "note": "No prospects to enrich"}

        # Load NPPES providers for matching taxonomy codes
        if not taxonomy_codes:
            return {"enriched": 0, "note": "No taxonomy codes configured"}

        placeholders = ", ".join(f":tc{i}" for i in range(len(taxonomy_codes)))
        params = {f"tc{i}": tc for i, tc in enumerate(taxonomy_codes)}
        providers = self._safe_query(
            f"""
            SELECT npi, provider_name, provider_first_name, provider_last_name,
                   practice_address, practice_city, practice_state, practice_zip,
                   taxonomy_code, taxonomy_description, credential
            FROM nppes_providers
            WHERE taxonomy_code IN ({placeholders})
            """,
            params,
        )

        if not providers:
            return {"enriched": 0, "providers_loaded": 0}

        # Build ZIP → providers index
        zip_providers: Dict[str, List[Dict]] = {}
        for p in providers:
            z = (p.get("practice_zip") or "")[:5]
            if z:
                zip_providers.setdefault(z, []).append(p)

        # Match: tier1 (name), tier2 (address), tier3 (ZIP)
        enriched = 0
        tier_counts = {"tier1": 0, "tier2": 0, "tier3": 0, "no_match": 0}

        for prospect in prospects:
            zc = (prospect.get("zip_code") or "")[:5]
            local_providers = zip_providers.get(zc, [])

            matched = False
            provider_count = len(local_providers)

            # Tier 1: Name similarity
            for p in local_providers:
                org_name = p.get("provider_name") or ""
                if org_name and _similarity_ratio(prospect["name"], org_name) >= 0.60:
                    self._update_nppes_match(prospect["yelp_id"], p, provider_count, 0.85)
                    tier_counts["tier1"] += 1
                    matched = True
                    break

            if not matched and local_providers:
                # Tier 3: ZIP proximity (at least some provider exists)
                self._update_nppes_zip(prospect["yelp_id"], provider_count)
                tier_counts["tier3"] += 1
                matched = True

            if not matched:
                tier_counts["no_match"] += 1
            else:
                enriched += 1

        return {
            "enriched": enriched,
            "providers_loaded": len(providers),
            "tier_counts": tier_counts,
        }

    def _update_nppes_match(
        self, yelp_id: str, provider: Dict, count: int, confidence: float
    ) -> None:
        t = self.config.table_name
        cred = provider.get("credential") or ""
        try:
            self.db.execute(
                text(f"""
                    UPDATE {t} SET
                        has_physician_oversight = true,
                        nppes_provider_count = :cnt,
                        nppes_match_confidence = :conf,
                        medical_director_name = :name,
                        nppes_enriched_at = NOW()
                    WHERE yelp_id = :yid
                """),
                {
                    "cnt": count,
                    "conf": confidence,
                    "name": _format_provider_name(provider),
                    "yid": yelp_id,
                },
            )
            self.db.commit()
        except Exception:
            self.db.rollback()

    def _update_nppes_zip(self, yelp_id: str, count: int) -> None:
        t = self.config.table_name
        try:
            self.db.execute(
                text(f"""
                    UPDATE {t} SET
                        nppes_provider_count = :cnt,
                        nppes_match_confidence = 0.30,
                        nppes_enriched_at = NOW()
                    WHERE yelp_id = :yid
                """),
                {"cnt": count, "yid": yelp_id},
            )
            self.db.commit()
        except Exception:
            self.db.rollback()

    # ------------------------------------------------------------------
    # Phase 1C: Competitive density
    # ------------------------------------------------------------------

    def enrich_competitive_density(self, force: bool = False) -> Dict[str, Any]:
        """Compute market saturation using IRS filer counts per ZIP."""
        t = self.config.table_name
        where = "1=1" if force else "density_enriched_at IS NULL"

        prospects = self._safe_query(
            f"SELECT yelp_id, zip_code FROM {t} WHERE {where}", {}
        )
        if not prospects:
            return {"enriched": 0}

        # Get competitor counts per ZIP
        zip_counts = self._safe_query(
            f"SELECT zip_code, COUNT(*) AS cnt FROM {t} GROUP BY zip_code", {}
        )
        count_map = {r["zip_code"]: r["cnt"] for r in zip_counts}

        # Get IRS filer counts per ZIP
        filer_map: Dict[str, int] = {}
        filer_rows = self._safe_query(
            "SELECT zip_code, total_returns FROM irs_soi_zip_income", {}
        )
        for r in filer_rows:
            filer_map[r["zip_code"]] = r["total_returns"] or 0

        enriched = 0
        by_category: Dict[str, int] = {}

        for p in prospects:
            zc = p["zip_code"]
            biz_count = count_map.get(zc, 1)
            filers = filer_map.get(zc, 0)

            if filers > 0:
                per_10k = (biz_count / filers) * 10000
                category = "Unknown"
                for threshold, label in SATURATION_THRESHOLDS:
                    if per_10k <= threshold:
                        category = label
                        break
            else:
                per_10k = None
                category = "Unknown"

            by_category[category] = by_category.get(category, 0) + 1

            try:
                self.db.execute(
                    text(f"""
                        UPDATE {t} SET
                            zip_total_filers = :filers,
                            businesses_per_10k_filers = :per10k,
                            market_saturation_index = :cat,
                            density_enriched_at = NOW()
                        WHERE yelp_id = :yid
                    """),
                    {
                        "filers": filers if filers > 0 else None,
                        "per10k": round(per_10k, 2) if per_10k is not None else None,
                        "cat": category,
                        "yid": p["yelp_id"],
                    },
                )
                enriched += 1
            except Exception:
                self.db.rollback()

        self.db.commit()
        return {
            "enriched": enriched,
            "by_saturation": by_category,
        }

    # ------------------------------------------------------------------
    # Phase 1B: Revenue estimation
    # ------------------------------------------------------------------

    def estimate_revenue(self, force: bool = False) -> Dict[str, Any]:
        """Multiplicative revenue model using per-vertical benchmarks."""
        t = self.config.table_name
        benchmarks = self.config.revenue_benchmarks
        median_agi = self.config.national_median_agi

        where = "1=1" if force else "revenue_estimated_at IS NULL"
        prospects = self._safe_query(
            f"""SELECT yelp_id, price, review_count, rating,
                       zip_avg_agi, competitor_count_in_zip,
                       has_physician_oversight
                FROM {t} WHERE {where}""",
            {},
        )
        if not prospects:
            return {"estimated": 0}

        # Median review count for normalization
        reviews = [p["review_count"] or 0 for p in prospects]
        median_reviews = sorted(reviews)[len(reviews) // 2] if reviews else 50

        estimated = 0
        revenues = []

        for p in prospects:
            price = p.get("price")
            base = benchmarks.get(price, benchmarks.get(None, 500_000))

            # Review factor
            rc = p.get("review_count") or 0
            review_factor = min(2.0, max(0.5,
                math.log(rc + 1) / math.log(max(median_reviews, 2) + 1)
            ))

            # Affluence factor
            avg_agi = float(p.get("zip_avg_agi") or median_agi)
            affluence_factor = min(1.5, max(0.7, avg_agi / median_agi))

            # Competition factor
            comp = p.get("competitor_count_in_zip") or 1
            competition_factor = 1 / (1 + 0.1 * (comp - 1))

            # Physician factor (healthcare only)
            if self.config.has_nppes_enrichment and p.get("has_physician_oversight"):
                physician_factor = 1.15
            else:
                physician_factor = 1.0

            revenue = base * review_factor * affluence_factor * competition_factor * physician_factor
            revenue = round(revenue, 2)
            low = round(revenue * 0.7, 2)
            high = round(revenue * 1.4, 2)

            # Confidence
            signals = sum([
                rc > 10,
                avg_agi != median_agi,
                price is not None,
                p.get("has_physician_oversight") is not None,
            ])
            confidence = "high" if signals >= 3 else ("medium" if signals >= 2 else "low")

            revenues.append(revenue)

            try:
                self.db.execute(
                    text(f"""
                        UPDATE {t} SET
                            estimated_annual_revenue = :rev,
                            revenue_estimate_low = :low,
                            revenue_estimate_high = :high,
                            revenue_confidence = :conf,
                            revenue_estimated_at = NOW()
                        WHERE yelp_id = :yid
                    """),
                    {
                        "rev": revenue, "low": low, "high": high,
                        "conf": confidence, "yid": p["yelp_id"],
                    },
                )
                estimated += 1
            except Exception:
                self.db.rollback()

        self.db.commit()

        avg_rev = sum(revenues) / len(revenues) if revenues else 0
        return {
            "estimated": estimated,
            "avg_revenue": round(avg_rev, 2),
            "min_revenue": round(min(revenues), 2) if revenues else 0,
            "max_revenue": round(max(revenues), 2) if revenues else 0,
        }

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _safe_query(self, query_str: str, params: Dict) -> List[Dict]:
        try:
            rows = self.db.execute(text(query_str), params).mappings().fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.debug(f"Query returned no results: {e}")
            self.db.rollback()
            return []


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _similarity_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _format_provider_name(provider: Dict) -> Optional[str]:
    first = provider.get("provider_first_name") or ""
    last = provider.get("provider_last_name") or ""
    if first and last:
        cred = provider.get("credential") or ""
        name = f"{first} {last}"
        if cred:
            name += f", {cred}"
        return name
    return provider.get("provider_name")
