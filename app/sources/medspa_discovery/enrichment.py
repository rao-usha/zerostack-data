"""
Med-Spa Prospect Enrichment Pipeline.

Three-phase enrichment using existing DB data (no external API calls):
  Phase 1A: NPPES Medical Provider Cross-Reference (physician oversight)
  Phase 1B: Revenue Estimation Model (multiplicative formula)
  Phase 1C: Enhanced Competitive Density (market saturation via IRS filers)
"""

import logging
import math
import time
from collections import defaultdict
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sources.medspa_discovery.metadata import (
    NATIONAL_MEDIAN_AGI,
    REVENUE_BENCHMARKS,
    REVENUE_MODEL_VERSION,
    SATURATION_THRESHOLDS,
    generate_enrichment_migration_sql,
    generate_snapshot_table_sql,
)
from app.sources.nppes.metadata import AESTHETIC_TAXONOMY_CODES

logger = logging.getLogger(__name__)


def _similarity_ratio(a: str, b: str) -> float:
    """Return SequenceMatcher ratio between two strings."""
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()


def _tokenize_address(addr: str) -> set:
    """Extract significant tokens from an address string for comparison."""
    if not addr:
        return set()
    # Normalize: lowercase, strip punctuation, split
    import re
    tokens = re.sub(r"[^\w\s]", "", addr.lower()).split()
    # Remove common address noise words
    noise = {"suite", "ste", "unit", "apt", "floor", "fl", "bldg", "building", "rm", "room"}
    return {t for t in tokens if t not in noise and len(t) > 1 and not t.isdigit()}


class MedSpaEnrichmentPipeline:
    """Enriches medspa prospects with NPPES, revenue, and density data."""

    def __init__(self, db: Session):
        self.db = db

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def ensure_columns(self) -> None:
        """Run idempotent migration to add all enrichment columns."""
        try:
            self.db.execute(text(generate_enrichment_migration_sql()))
            self.db.execute(text(generate_snapshot_table_sql()))
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    # ------------------------------------------------------------------
    # Phase 1A: NPPES Medical Provider Cross-Reference
    # ------------------------------------------------------------------

    def enrich_nppes(self, force: bool = False) -> Dict[str, Any]:
        """
        Cross-reference medspa prospects with NPPES providers.

        3-tier matching (first match wins per prospect):
          1. ZIP + fuzzy name match (conf 0.85-0.95)
          2. ZIP + address token overlap (conf 0.70-0.85)
          3. ZIP proximity — aesthetic providers exist in ZIP (conf 0.50-0.65)

        Returns summary with match counts per tier.
        """
        self.ensure_columns()
        start = time.time()

        # Load prospects needing enrichment
        where = "" if force else "WHERE nppes_enriched_at IS NULL"
        prospects = self.db.execute(text(f"""
            SELECT yelp_id, name, address, zip_code
            FROM medspa_prospects
            {where}
        """)).fetchall()

        if not prospects:
            return {"total": 0, "enriched": 0, "message": "No prospects to enrich"}

        prospect_list = [
            {"yelp_id": r[0], "name": r[1], "address": r[2], "zip_code": r[3]}
            for r in prospects
        ]

        # Collect all prospect ZIP codes
        prospect_zips = {p["zip_code"] for p in prospect_list if p["zip_code"]}

        if not prospect_zips:
            return {"total": len(prospect_list), "enriched": 0, "message": "No ZIP codes on prospects"}

        # Load NPPES providers in those ZIPs with aesthetic taxonomy codes
        tax_placeholders = ", ".join(f":tax{i}" for i in range(len(AESTHETIC_TAXONOMY_CODES)))
        zip_placeholders = ", ".join(f":zip{i}" for i in range(len(prospect_zips)))

        params: Dict[str, Any] = {}
        for i, code in enumerate(AESTHETIC_TAXONOMY_CODES):
            params[f"tax{i}"] = code
        for i, zc in enumerate(sorted(prospect_zips)):
            params[f"zip{i}"] = zc[:5]

        provider_rows = self.db.execute(text(f"""
            SELECT npi, legal_name, first_name, last_name, credential,
                   dba_name, practice_address_line1, practice_zip,
                   taxonomy_code, taxonomy_description
            FROM nppes_providers
            WHERE taxonomy_code IN ({tax_placeholders})
              AND LEFT(practice_zip, 5) IN ({zip_placeholders})
              AND status = 'A'
        """), params).fetchall()

        # Index providers by 5-digit ZIP
        providers_by_zip: Dict[str, List[Dict]] = defaultdict(list)
        for r in provider_rows:
            prov = {
                "npi": r[0], "legal_name": r[1], "first_name": r[2],
                "last_name": r[3], "credential": r[4], "dba_name": r[5],
                "address": r[6], "zip": (r[7] or "")[:5],
                "taxonomy_code": r[8], "taxonomy_desc": r[9],
            }
            providers_by_zip[prov["zip"]].append(prov)

        logger.info(
            f"NPPES enrichment: {len(prospect_list)} prospects, "
            f"{len(provider_rows)} providers in {len(providers_by_zip)} ZIPs"
        )

        # Match each prospect
        now = datetime.utcnow()
        tier_counts = {"tier1_name": 0, "tier2_address": 0, "tier3_zip": 0, "no_match": 0}
        batch_updates = []

        for p in prospect_list:
            pzip = (p["zip_code"] or "")[:5]
            zip_providers = providers_by_zip.get(pzip, [])

            match = self._nppes_match(p, zip_providers)
            match["nppes_enriched_at"] = now
            match["yelp_id"] = p["yelp_id"]

            tier_counts[match.pop("_tier")] += 1
            batch_updates.append(match)

        # Persist
        self._apply_nppes_updates(batch_updates)

        duration_ms = int((time.time() - start) * 1000)
        enriched = tier_counts["tier1_name"] + tier_counts["tier2_address"] + tier_counts["tier3_zip"]

        result = {
            "total": len(prospect_list),
            "enriched": enriched,
            "providers_loaded": len(provider_rows),
            "match_tiers": tier_counts,
            "duration_ms": duration_ms,
        }
        logger.info(f"NPPES enrichment complete: {result}")
        return result

    def _nppes_match(
        self, prospect: Dict, zip_providers: List[Dict]
    ) -> Dict[str, Any]:
        """
        Try 3-tier matching for a single prospect against ZIP providers.

        Returns dict of column values to update.
        """
        if not zip_providers:
            return {
                "has_physician_oversight": False,
                "nppes_provider_count": 0,
                "nppes_provider_credentials": None,
                "nppes_match_confidence": None,
                "medical_director_name": None,
                "_tier": "no_match",
            }

        prospect_name = prospect.get("name") or ""
        prospect_addr = prospect.get("address") or ""

        # Tier 1: ZIP + fuzzy name match
        best_name_sim = 0.0
        best_name_prov = None
        for prov in zip_providers:
            # Compare against both legal_name and dba_name
            for compare_name in [prov.get("dba_name"), prov.get("legal_name")]:
                if not compare_name:
                    continue
                sim = _similarity_ratio(prospect_name, compare_name)
                if sim > best_name_sim:
                    best_name_sim = sim
                    best_name_prov = prov

        if best_name_sim >= 0.60 and best_name_prov:
            creds = self._collect_credentials(zip_providers, match_provider=best_name_prov)
            director = self._format_provider_name(best_name_prov)
            confidence = min(0.95, 0.85 + (best_name_sim - 0.60) * 0.25)
            return {
                "has_physician_oversight": True,
                "nppes_provider_count": len(zip_providers),
                "nppes_provider_credentials": creds,
                "nppes_match_confidence": round(confidence, 3),
                "medical_director_name": director,
                "_tier": "tier1_name",
            }

        # Tier 2: ZIP + address token overlap
        prospect_tokens = _tokenize_address(prospect_addr)
        if prospect_tokens:
            best_overlap = 0.0
            best_addr_prov = None
            for prov in zip_providers:
                prov_tokens = _tokenize_address(prov.get("address") or "")
                if not prov_tokens:
                    continue
                overlap = len(prospect_tokens & prov_tokens) / max(
                    len(prospect_tokens), len(prov_tokens)
                )
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_addr_prov = prov

            if best_overlap >= 0.40 and best_addr_prov:
                creds = self._collect_credentials(zip_providers, match_provider=best_addr_prov)
                director = self._format_provider_name(best_addr_prov)
                confidence = round(0.70 + best_overlap * 0.15, 3)
                return {
                    "has_physician_oversight": True,
                    "nppes_provider_count": len(zip_providers),
                    "nppes_provider_credentials": creds,
                    "nppes_match_confidence": min(0.85, confidence),
                    "medical_director_name": director,
                    "_tier": "tier2_address",
                }

        # Tier 3: ZIP proximity — aesthetic providers exist in ZIP
        if zip_providers:
            creds = self._collect_credentials(zip_providers)
            confidence = round(0.50 + min(0.15, len(zip_providers) * 0.02), 3)
            return {
                "has_physician_oversight": None,  # unknown — just proximity
                "nppes_provider_count": len(zip_providers),
                "nppes_provider_credentials": creds,
                "nppes_match_confidence": confidence,
                "medical_director_name": None,
                "_tier": "tier3_zip",
            }

        return {
            "has_physician_oversight": False,
            "nppes_provider_count": 0,
            "nppes_provider_credentials": None,
            "nppes_match_confidence": None,
            "medical_director_name": None,
            "_tier": "no_match",
        }

    @staticmethod
    def _collect_credentials(
        providers: List[Dict], match_provider: Optional[Dict] = None
    ) -> List[str]:
        """Collect unique credentials from providers, prioritizing the matched one."""
        creds = []
        seen = set()
        # Put matched provider's credential first
        if match_provider and match_provider.get("credential"):
            c = match_provider["credential"].strip()
            if c:
                creds.append(c)
                seen.add(c.upper())
        for prov in providers:
            c = (prov.get("credential") or "").strip()
            if c and c.upper() not in seen:
                creds.append(c)
                seen.add(c.upper())
        return creds if creds else []

    @staticmethod
    def _format_provider_name(prov: Dict) -> Optional[str]:
        """Format provider name with credential."""
        first = prov.get("first_name") or ""
        last = prov.get("last_name") or ""
        cred = prov.get("credential") or ""
        name = f"{first} {last}".strip()
        if not name:
            name = prov.get("legal_name") or ""
        if name and cred:
            return f"{name}, {cred}"
        return name or None

    def _apply_nppes_updates(self, updates: List[Dict]) -> None:
        """Batch UPDATE prospects with NPPES enrichment data."""
        if not updates:
            return

        for u in updates:
            # Convert credentials list to PG array literal
            creds = u.get("nppes_provider_credentials")
            if isinstance(creds, list) and creds:
                u["nppes_provider_credentials"] = "{" + ",".join(
                    '"' + c.replace('"', '\\"') + '"' for c in creds
                ) + "}"
            else:
                u["nppes_provider_credentials"] = None

            self.db.execute(
                text("""
                    UPDATE medspa_prospects
                    SET has_physician_oversight = :has_physician_oversight,
                        nppes_provider_count = :nppes_provider_count,
                        nppes_provider_credentials = :nppes_provider_credentials,
                        nppes_match_confidence = :nppes_match_confidence,
                        medical_director_name = :medical_director_name,
                        nppes_enriched_at = :nppes_enriched_at
                    WHERE yelp_id = :yelp_id
                """),
                u,
            )

        self.db.commit()
        logger.info(f"Persisted NPPES enrichment for {len(updates)} prospects")

    # ------------------------------------------------------------------
    # Phase 1C: Enhanced Competitive Density
    # ------------------------------------------------------------------

    def enrich_competitive_density(self, force: bool = False) -> Dict[str, Any]:
        """
        Calculate market saturation using IRS tax filer counts from zip_medspa_scores.

        For each prospect's ZIP: medspas_per_10k = (medspa_count / total_returns) * 10000
        Then classify as Undersaturated / Balanced / Saturated / Oversaturated.
        """
        self.ensure_columns()
        start = time.time()

        where = "" if force else "WHERE density_enriched_at IS NULL"
        prospects = self.db.execute(text(f"""
            SELECT yelp_id, zip_code, zip_total_returns, competitor_count_in_zip
            FROM medspa_prospects
            {where}
        """)).fetchall()

        if not prospects:
            return {"total": 0, "enriched": 0, "message": "No prospects to enrich"}

        prospect_list = [
            {
                "yelp_id": r[0], "zip_code": r[1],
                "total_returns": r[2], "competitors": r[3] or 0,
            }
            for r in prospects
        ]

        # Also count medspas per ZIP from the full table (more accurate than competitor_count_in_zip)
        zip_medspa_counts = {}
        count_rows = self.db.execute(text("""
            SELECT zip_code, COUNT(*) as cnt
            FROM medspa_prospects
            WHERE zip_code IS NOT NULL
            GROUP BY zip_code
        """)).fetchall()
        for r in count_rows:
            zip_medspa_counts[r[0]] = r[1]

        now = datetime.utcnow()
        saturation_dist: Dict[str, int] = defaultdict(int)
        updates = []

        for p in prospect_list:
            zc = p["zip_code"]
            total_returns = p["total_returns"]
            medspa_count = zip_medspa_counts.get(zc, p["competitors"])

            if total_returns and total_returns > 0:
                per_10k = round((medspa_count / total_returns) * 10000, 2)
            else:
                per_10k = None

            saturation = self._classify_saturation(per_10k)
            saturation_dist[saturation or "Unknown"] += 1

            updates.append({
                "yelp_id": p["yelp_id"],
                "zip_total_filers": total_returns,
                "medspas_per_10k_filers": per_10k,
                "market_saturation_index": saturation,
                "density_enriched_at": now,
            })

        # Persist
        for u in updates:
            self.db.execute(
                text("""
                    UPDATE medspa_prospects
                    SET zip_total_filers = :zip_total_filers,
                        medspas_per_10k_filers = :medspas_per_10k_filers,
                        market_saturation_index = :market_saturation_index,
                        density_enriched_at = :density_enriched_at
                    WHERE yelp_id = :yelp_id
                """),
                u,
            )
        self.db.commit()

        duration_ms = int((time.time() - start) * 1000)
        enriched = sum(1 for u in updates if u["medspas_per_10k_filers"] is not None)

        result = {
            "total": len(prospect_list),
            "enriched": enriched,
            "saturation_distribution": dict(saturation_dist),
            "duration_ms": duration_ms,
        }
        logger.info(f"Density enrichment complete: {result}")
        return result

    @staticmethod
    def _classify_saturation(per_10k: Optional[float]) -> Optional[str]:
        """Classify market saturation based on medspas per 10k filers."""
        if per_10k is None:
            return None
        for threshold, label in SATURATION_THRESHOLDS:
            if per_10k <= threshold:
                return label
        return "Oversaturated"

    # ------------------------------------------------------------------
    # Phase 1B: Revenue Estimation Model
    # ------------------------------------------------------------------

    def estimate_revenue(self, force: bool = False) -> Dict[str, Any]:
        """
        Estimate annual revenue using multiplicative model:
          base * review_factor * affluence_factor * competition_factor * physician_factor

        Runs after NPPES enrichment (uses has_physician_oversight).
        """
        self.ensure_columns()
        start = time.time()

        where = "" if force else "WHERE revenue_estimated_at IS NULL"
        prospects = self.db.execute(text(f"""
            SELECT yelp_id, price, review_count, zip_avg_agi,
                   competitor_count_in_zip, has_physician_oversight
            FROM medspa_prospects
            {where}
        """)).fetchall()

        if not prospects:
            return {"total": 0, "enriched": 0, "message": "No prospects to estimate"}

        prospect_list = [
            {
                "yelp_id": r[0], "price": r[1], "review_count": r[2] or 0,
                "zip_avg_agi": float(r[3]) if r[3] else None,
                "competitors": r[4] or 0, "has_physician": r[5],
            }
            for r in prospects
        ]

        # Compute median review count across all prospects for the review factor
        all_reviews = self.db.execute(text(
            "SELECT review_count FROM medspa_prospects WHERE review_count > 0"
        )).fetchall()
        review_counts = sorted([r[0] for r in all_reviews])
        median_reviews = review_counts[len(review_counts) // 2] if review_counts else 50

        now = datetime.utcnow()
        confidence_dist: Dict[str, int] = defaultdict(int)
        updates = []

        for p in prospect_list:
            est = self._compute_revenue(p, median_reviews)
            est["yelp_id"] = p["yelp_id"]
            est["revenue_model_version"] = REVENUE_MODEL_VERSION
            est["revenue_estimated_at"] = now
            confidence_dist[est["revenue_confidence"]] += 1
            updates.append(est)

        # Persist
        for u in updates:
            self.db.execute(
                text("""
                    UPDATE medspa_prospects
                    SET estimated_annual_revenue = :estimated_annual_revenue,
                        revenue_estimate_low = :revenue_estimate_low,
                        revenue_estimate_high = :revenue_estimate_high,
                        revenue_confidence = :revenue_confidence,
                        revenue_model_version = :revenue_model_version,
                        revenue_estimated_at = :revenue_estimated_at
                    WHERE yelp_id = :yelp_id
                """),
                u,
            )
        self.db.commit()

        duration_ms = int((time.time() - start) * 1000)
        revenues = [u["estimated_annual_revenue"] for u in updates if u["estimated_annual_revenue"]]

        result = {
            "total": len(prospect_list),
            "enriched": len(revenues),
            "median_review_count_used": median_reviews,
            "revenue_stats": {
                "min": min(revenues) if revenues else 0,
                "max": max(revenues) if revenues else 0,
                "avg": round(sum(revenues) / len(revenues), 2) if revenues else 0,
            },
            "confidence_distribution": dict(confidence_dist),
            "duration_ms": duration_ms,
        }
        logger.info(f"Revenue estimation complete: {result}")
        return result

    @staticmethod
    def _compute_revenue(prospect: Dict, median_reviews: int) -> Dict[str, Any]:
        """
        Compute revenue estimate for a single prospect.

        Formula:
          base = REVENUE_BENCHMARKS[price_tier]
          review_factor = log(reviews+1) / log(median+1)  [capped 0.5-2.0]
          affluence_factor = zip_avg_agi / national_median  [capped 0.7-1.5]
          competition_factor = 1 / (1 + 0.1 * competitors)
          physician_factor = 1.15 if has_physician else 1.0
        """
        price = prospect.get("price")
        base = REVENUE_BENCHMARKS.get(price, REVENUE_BENCHMARKS[None])

        # Review factor
        reviews = max(prospect.get("review_count", 0), 0)
        if median_reviews > 0:
            review_factor = math.log(reviews + 1) / math.log(median_reviews + 1)
        else:
            review_factor = 1.0
        review_factor = max(0.5, min(2.0, review_factor))

        # Affluence factor
        agi = prospect.get("zip_avg_agi")
        if agi and agi > 0:
            affluence_factor = agi / NATIONAL_MEDIAN_AGI
        else:
            affluence_factor = 1.0
        affluence_factor = max(0.7, min(1.5, affluence_factor))

        # Competition factor
        competitors = prospect.get("competitors", 0)
        competition_factor = 1.0 / (1.0 + 0.1 * competitors)

        # Physician oversight factor
        has_physician = prospect.get("has_physician")
        physician_factor = 1.15 if has_physician else 1.0

        estimated = base * review_factor * affluence_factor * competition_factor * physician_factor
        estimated = round(estimated, 2)
        low = round(estimated * 0.65, 2)
        high = round(estimated * 1.40, 2)

        # Confidence based on data quality
        signals = 0
        if price:
            signals += 1
        if reviews > 10:
            signals += 1
        if agi and agi > 0:
            signals += 1
        if has_physician is not None:
            signals += 1

        if signals >= 4:
            confidence = "high"
        elif signals >= 2:
            confidence = "medium"
        else:
            confidence = "low"

        return {
            "estimated_annual_revenue": estimated,
            "revenue_estimate_low": low,
            "revenue_estimate_high": high,
            "revenue_confidence": confidence,
        }

    # ------------------------------------------------------------------
    # Run All Phase 1
    # ------------------------------------------------------------------

    def enrich_all(self, force: bool = False) -> Dict[str, Any]:
        """Run all Phase 1 enrichment steps in order."""
        self.ensure_columns()
        start = time.time()

        results = {}

        # 1A: NPPES cross-reference
        logger.info("Starting Phase 1A: NPPES cross-reference...")
        results["nppes"] = self.enrich_nppes(force=force)

        # 1C: Competitive density (before revenue, though order doesn't matter)
        logger.info("Starting Phase 1C: Competitive density...")
        results["density"] = self.enrich_competitive_density(force=force)

        # 1B: Revenue estimation (after NPPES — uses has_physician_oversight)
        logger.info("Starting Phase 1B: Revenue estimation...")
        results["revenue"] = self.estimate_revenue(force=force)

        duration_ms = int((time.time() - start) * 1000)
        results["total_duration_ms"] = duration_ms

        logger.info(f"All Phase 1 enrichment complete in {duration_ms}ms")
        return results
