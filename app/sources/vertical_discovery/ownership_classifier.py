"""
Vertical Ownership Classifier — generic multi-stage pipeline.

Generalized from MedSpaOwnershipClassifier. Classifies each prospect as:
  Independent, Multi-Site, PE-Backed, or Public

Pipeline priority (first match wins):
  1. PE cross-reference (fuzzy match vs pe_portfolio_companies)
  2. Public cross-reference (ticker IS NOT NULL)
  3. Phone clustering (shared phone → Multi-Site)
  4. Name clustering (token-blocked fuzzy match among prospects)
  5. Pattern heuristics (regex: #N, "Brand - City", franchise keywords)
  6. Default → Independent
"""

import logging
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.sources.vertical_discovery.configs import VerticalConfig
from app.core.safe_sql import qi

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OWNERSHIP_TYPES = ("Independent", "Multi-Site", "PE-Backed", "Public")

OWNERSHIP_PENALTIES = {
    "Independent": 0,
    "Multi-Site": -5,
    "PE-Backed": -15,
    "Public": -20,
}

CHAIN_KEYWORDS = re.compile(
    r"\b(franchise|franchised|location|locations|chain)\b", re.IGNORECASE
)
BRAND_CITY_PATTERN = re.compile(
    r"^(.{3,30})\s*[-|:]\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?$"
)
STORE_NUMBER_PATTERN = re.compile(
    r"#\d+|No\.\s*\d+|\bStore\s+\d+\b", re.IGNORECASE
)
LOCATION_SUFFIX = re.compile(
    r"\s*[-–—|:]\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2}\s*$"
)


class VerticalOwnershipClassifier:
    """6-stage ownership classification pipeline for any vertical."""

    def __init__(self, db: Session, config: VerticalConfig):
        self.db = db
        self.config = config

    def classify_all(self, force: bool = False) -> Dict[str, Any]:
        """Run the full classification pipeline."""
        t = self.config.table_name

        where = "1=1" if force else "classified_at IS NULL"
        prospects = self._safe_query(
            f"SELECT yelp_id, name, phone, acquisition_score FROM {qi(t)} WHERE {where}",
            {},
        )
        if not prospects:
            return {"classified": 0, "note": "No prospects to classify"}

        # Load PE companies for cross-reference
        pe_companies = self._safe_query(
            "SELECT company_name, ticker FROM pe_portfolio_companies", {}
        )
        pe_names = [
            c["company_name"] for c in pe_companies if c.get("company_name")
        ]
        public_names = [
            c["company_name"]
            for c in pe_companies
            if c.get("company_name") and c.get("ticker")
        ]

        # Track classifications
        classifications: Dict[str, Dict] = {}
        stage_counts: Dict[str, int] = defaultdict(int)

        # Stage 1: PE cross-reference
        for p in prospects:
            if p["yelp_id"] in classifications:
                continue
            clean = LOCATION_SUFFIX.sub("", p["name"])
            for pe_name in pe_names:
                if pe_name in public_names:
                    continue  # Handle in stage 2
                if _fuzzy_match(clean, pe_name, threshold=0.75):
                    classifications[p["yelp_id"]] = {
                        "ownership_type": "PE-Backed",
                        "parent_entity": pe_name,
                        "confidence": 0.80,
                        "stage": "pe_crossref",
                    }
                    stage_counts["pe_crossref"] += 1
                    break

        # Stage 2: Public cross-reference
        for p in prospects:
            if p["yelp_id"] in classifications:
                continue
            clean = LOCATION_SUFFIX.sub("", p["name"])
            for pub_name in public_names:
                if _fuzzy_match(clean, pub_name, threshold=0.75):
                    classifications[p["yelp_id"]] = {
                        "ownership_type": "Public",
                        "parent_entity": pub_name,
                        "confidence": 0.85,
                        "stage": "public_crossref",
                    }
                    stage_counts["public_crossref"] += 1
                    break

        # Stage 3: Phone clustering
        phone_groups: Dict[str, List[Dict]] = defaultdict(list)
        for p in prospects:
            if p["yelp_id"] in classifications:
                continue
            phone = (p.get("phone") or "").strip()
            if phone and len(phone) >= 10:
                phone_groups[phone].append(p)

        for phone, group in phone_groups.items():
            if len(group) >= 2:
                for p in group:
                    if p["yelp_id"] not in classifications:
                        classifications[p["yelp_id"]] = {
                            "ownership_type": "Multi-Site",
                            "parent_entity": group[0]["name"],
                            "location_count": len(group),
                            "confidence": 0.75,
                            "stage": "phone_cluster",
                        }
                        stage_counts["phone_cluster"] += 1

        # Stage 4: Name clustering
        unclassified = [
            p for p in prospects if p["yelp_id"] not in classifications
        ]
        name_groups = _cluster_by_name(unclassified, threshold=0.80)
        for group in name_groups:
            if len(group) >= 2:
                brand = _extract_brand_core([p["name"] for p in group])
                for p in group:
                    if p["yelp_id"] not in classifications:
                        classifications[p["yelp_id"]] = {
                            "ownership_type": "Multi-Site",
                            "parent_entity": brand,
                            "location_count": len(group),
                            "confidence": 0.70,
                            "stage": "name_cluster",
                        }
                        stage_counts["name_cluster"] += 1

        # Stage 5: Pattern heuristics
        for p in prospects:
            if p["yelp_id"] in classifications:
                continue
            name = p["name"]
            if STORE_NUMBER_PATTERN.search(name):
                classifications[p["yelp_id"]] = {
                    "ownership_type": "Multi-Site",
                    "parent_entity": STORE_NUMBER_PATTERN.sub("", name).strip(),
                    "confidence": 0.65,
                    "stage": "pattern_heuristic",
                }
                stage_counts["pattern_heuristic"] += 1
            elif BRAND_CITY_PATTERN.match(name):
                classifications[p["yelp_id"]] = {
                    "ownership_type": "Multi-Site",
                    "parent_entity": BRAND_CITY_PATTERN.match(name).group(1).strip(),
                    "confidence": 0.55,
                    "stage": "pattern_heuristic",
                }
                stage_counts["pattern_heuristic"] += 1

        # Stage 6: Default → Independent
        for p in prospects:
            if p["yelp_id"] not in classifications:
                classifications[p["yelp_id"]] = {
                    "ownership_type": "Independent",
                    "parent_entity": None,
                    "confidence": 0.50,
                    "stage": "default",
                }
                stage_counts["default"] += 1

        # Persist
        self._save_classifications(classifications, prospects)

        # Summary
        by_type: Dict[str, int] = defaultdict(int)
        for c in classifications.values():
            by_type[c["ownership_type"]] += 1

        return {
            "vertical": self.config.slug,
            "classified": len(classifications),
            "by_type": dict(by_type),
            "by_stage": dict(stage_counts),
        }

    def _save_classifications(
        self, classifications: Dict[str, Dict], prospects: List[Dict]
    ) -> None:
        t = self.config.table_name
        score_lookup = {p["yelp_id"]: p.get("acquisition_score", 0) for p in prospects}

        for yelp_id, c in classifications.items():
            penalty = OWNERSHIP_PENALTIES.get(c["ownership_type"], 0)
            base_score = float(score_lookup.get(yelp_id, 0) or 0)
            adjusted = max(0, round(base_score + penalty, 2))

            try:
                self.db.execute(
                    text(f"""
                        UPDATE {qi(t)} SET
                            ownership_type = :otype,
                            parent_entity = :parent,
                            location_count = :lcount,
                            classification_confidence = :conf,
                            classified_at = :cat,
                            adjusted_acquisition_score = :adj
                        WHERE yelp_id = :yid
                    """),
                    {
                        "otype": c["ownership_type"],
                        "parent": c.get("parent_entity"),
                        "lcount": c.get("location_count", 1),
                        "conf": c["confidence"],
                        "cat": datetime.utcnow(),
                        "adj": adjusted,
                        "yid": yelp_id,
                    },
                )
            except Exception:
                self.db.rollback()

        self.db.commit()

    def _safe_query(self, query_str: str, params: Dict) -> List[Dict]:
        try:
            rows = self.db.execute(text(query_str), params).mappings().fetchall()
            return [dict(r) for r in rows]
        except Exception as e:
            logger.debug(f"Query error: {e}")
            self.db.rollback()
            return []


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------

def _fuzzy_match(a: str, b: str, threshold: float = 0.75) -> bool:
    from difflib import SequenceMatcher
    return SequenceMatcher(None, a.lower(), b.lower()).ratio() >= threshold


def _cluster_by_name(
    prospects: List[Dict], threshold: float = 0.80
) -> List[List[Dict]]:
    """Group prospects by name similarity using token-blocked comparison."""
    from difflib import SequenceMatcher

    clusters: List[List[Dict]] = []
    assigned: set = set()

    for i, p in enumerate(prospects):
        if i in assigned:
            continue
        cluster = [p]
        assigned.add(i)
        clean_i = LOCATION_SUFFIX.sub("", p["name"]).lower()

        for j in range(i + 1, len(prospects)):
            if j in assigned:
                continue
            clean_j = LOCATION_SUFFIX.sub("", prospects[j]["name"]).lower()
            if SequenceMatcher(None, clean_i, clean_j).ratio() >= threshold:
                cluster.append(prospects[j])
                assigned.add(j)

        clusters.append(cluster)

    return clusters


def _extract_brand_core(names: List[str]) -> str:
    """Find common brand prefix from a list of names."""
    if not names:
        return ""
    cleaned = [LOCATION_SUFFIX.sub("", n).strip() for n in names]
    if len(cleaned) == 1:
        return cleaned[0]

    # Word-level common prefix
    words_list = [n.split() for n in cleaned]
    prefix = []
    for word_group in zip(*words_list):
        if len(set(w.lower() for w in word_group)) == 1:
            prefix.append(word_group[0])
        else:
            break

    if prefix:
        return " ".join(prefix)
    return min(cleaned, key=len)
