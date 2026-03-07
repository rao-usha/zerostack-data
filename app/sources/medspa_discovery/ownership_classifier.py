"""
Med-Spa Prospect Ownership Classifier.

Multi-stage pipeline that classifies each medspa_prospect as:
  Independent, Multi-Site, PE-Backed, or Public

Uses data already in the database — no external API calls needed.
Pipeline priority (first match wins):
  1. PE cross-reference (fuzzy match vs pe_portfolio_companies)
  2. Public cross-reference (ticker IS NOT NULL in pe_portfolio_companies)
  3. Phone clustering (shared phone → Multi-Site)
  4. Name clustering (token-blocked fuzzy match among prospects)
  5. Pattern heuristics (regex: #N, "Brand - City", franchise keywords)
  6. Default → Independent
"""

import logging
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlalchemy import text
from sqlalchemy.orm import Session

from app.agentic.fuzzy_matcher import CompanyNameMatcher, similarity_ratio
from app.sources.medspa_discovery.metadata import generate_ownership_migration_sql

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

OWNERSHIP_TYPES = ("Independent", "Multi-Site", "PE-Backed", "Public")

# Scoring penalties applied to adjusted_acquisition_score
OWNERSHIP_PENALTIES = {
    "Independent": 0,
    "Multi-Site": -5,
    "PE-Backed": -15,
    "Public": -20,
}

# Franchise / chain keywords that signal multi-site
CHAIN_KEYWORDS = re.compile(
    r"\b(franchise|franchised|location|locations|chain)\b", re.IGNORECASE
)

# Pattern: "Brand - City" or "Brand | City" or "Brand: City"
BRAND_CITY_PATTERN = re.compile(
    r"^(.{3,30})\s*[-|:]\s*[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?$"
)

# Pattern: store number like "#123" or "No. 5"
STORE_NUMBER_PATTERN = re.compile(r"#\d+|No\.\s*\d+|\bStore\s+\d+\b", re.IGNORECASE)

# Location suffixes to strip before PE matching (e.g., "Next Health - West Hollywood")
# Handles: "Brand - City", "Brand - SF", "Brand | LA", "Brand: Austin"
LOCATION_SUFFIX_PATTERN = re.compile(
    r"\s*[-|:]\s*[A-Z][\w\s]*$"
)

# Words to skip when building blocking keys
STOP_WORDS = frozenset({
    "the", "a", "an", "and", "of", "in", "at", "by", "for", "med", "spa",
    "medical", "medspa", "aesthetics", "aesthetic", "skin", "beauty",
    "wellness", "clinic", "center", "centre", "studio", "institute",
    "laser", "cosmetic", "dermatology", "plastic", "surgery",
})


@dataclass
class Classification:
    """Ownership classification result for a single prospect."""
    yelp_id: str
    ownership_type: str
    parent_entity: Optional[str] = None
    location_count: int = 1
    confidence: float = 0.50
    stage: str = "default"  # which pipeline stage classified it


@dataclass
class ClassificationSummary:
    """Summary of a classification run."""
    total: int = 0
    classified: int = 0
    skipped: int = 0
    by_type: Dict[str, int] = field(default_factory=dict)
    by_stage: Dict[str, int] = field(default_factory=dict)
    duration_ms: int = 0


class MedSpaOwnershipClassifier:
    """Classifies medspa prospects by ownership type."""

    def __init__(self, db: Session):
        self.db = db
        self._pe_matcher = CompanyNameMatcher(similarity_threshold=0.80)
        self._name_matcher = CompanyNameMatcher(similarity_threshold=0.75)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def ensure_columns(self) -> None:
        """Run idempotent migration to add ownership columns."""
        try:
            self.db.execute(text(generate_ownership_migration_sql()))
            self.db.commit()
        except Exception:
            self.db.rollback()
            raise

    def classify_all(self, force: bool = False) -> Dict[str, Any]:
        """
        Run the full classification pipeline.

        Args:
            force: If True, re-classify already-classified prospects.

        Returns:
            Summary dict with counts by type and stage.
        """
        self.ensure_columns()
        start = time.time()

        # Load prospects
        where = "" if force else "WHERE ownership_type IS NULL"
        rows = self.db.execute(text(f"""
            SELECT yelp_id, name, phone
            FROM medspa_prospects
            {where}
            ORDER BY acquisition_score DESC
        """)).fetchall()

        prospects = [
            {"yelp_id": r[0], "name": r[1], "phone": r[2]}
            for r in rows
        ]

        if not prospects:
            return {"total": 0, "classified": 0, "message": "No unclassified prospects"}

        logger.info(f"Classifying {len(prospects)} medspa prospects")

        # Load PE companies for cross-reference
        pe_companies = self._load_pe_companies()

        # Classification map: yelp_id -> Classification
        classifications: Dict[str, Classification] = {}
        remaining = list(prospects)

        # Stage 1: PE cross-reference
        remaining = self._classify_pe_backed(remaining, pe_companies, classifications)

        # Stage 2: Public cross-reference
        remaining = self._classify_public(remaining, pe_companies, classifications)

        # Stage 3: Phone clustering
        remaining = self._cluster_by_phone(remaining, classifications)

        # Stage 4: Name clustering
        remaining = self._cluster_by_name(remaining, classifications)

        # Stage 5: Pattern heuristics
        remaining = self._classify_by_pattern(remaining, classifications)

        # Stage 6: Default — everything unmatched is Independent
        for p in remaining:
            classifications[p["yelp_id"]] = Classification(
                yelp_id=p["yelp_id"],
                ownership_type="Independent",
                confidence=0.50,
                stage="default",
            )

        # Persist
        self._apply_classifications(classifications)

        # Build summary
        duration_ms = int((time.time() - start) * 1000)
        summary = self._build_summary(classifications, duration_ms)

        logger.info(
            f"Classification complete: {summary['classified']} prospects in {duration_ms}ms — "
            f"{summary['by_type']}"
        )

        return summary

    # ------------------------------------------------------------------
    # Stage 1: PE Cross-Reference
    # ------------------------------------------------------------------

    def _classify_pe_backed(
        self,
        prospects: List[Dict],
        pe_companies: List[Dict],
        out: Dict[str, Classification],
    ) -> List[Dict]:
        """Match prospects against PE portfolio companies (PE-backed only)."""
        pe_backed = [c for c in pe_companies if not c.get("ticker")]
        if not pe_backed:
            return prospects

        pe_names = {c["name"]: c for c in pe_backed}
        pe_name_list = list(pe_names.keys())
        remaining = []

        for p in prospects:
            match_result = self._match_against_companies(p["name"], pe_name_list)
            if match_result:
                pe_name, similarity = match_result
                pe_co = pe_names[pe_name]
                parent = pe_co.get("pe_firm") or pe_name
                out[p["yelp_id"]] = Classification(
                    yelp_id=p["yelp_id"],
                    ownership_type="PE-Backed",
                    parent_entity=parent,
                    confidence=min(0.95, similarity),
                    stage="pe_crossref",
                )
            else:
                remaining.append(p)

        logger.info(f"PE cross-ref: {len(prospects) - len(remaining)} matched")
        return remaining

    # ------------------------------------------------------------------
    # Stage 2: Public Cross-Reference
    # ------------------------------------------------------------------

    def _classify_public(
        self,
        prospects: List[Dict],
        pe_companies: List[Dict],
        out: Dict[str, Classification],
    ) -> List[Dict]:
        """Match prospects against public companies (ticker IS NOT NULL)."""
        public = [c for c in pe_companies if c.get("ticker")]
        if not public:
            return prospects

        pub_names = {c["name"]: c for c in public}
        pub_name_list = list(pub_names.keys())
        remaining = []

        for p in prospects:
            match_result = self._match_against_companies(p["name"], pub_name_list)
            if match_result:
                pub_name, similarity = match_result
                pub_co = pub_names[pub_name]
                out[p["yelp_id"]] = Classification(
                    yelp_id=p["yelp_id"],
                    ownership_type="Public",
                    parent_entity=pub_co.get("ticker") or pub_name,
                    confidence=0.95,
                    stage="public_crossref",
                )
            else:
                remaining.append(p)

        logger.info(f"Public cross-ref: {len(prospects) - len(remaining)} matched")
        return remaining

    # ------------------------------------------------------------------
    # Stage 3: Phone Clustering
    # ------------------------------------------------------------------

    def _cluster_by_phone(
        self,
        prospects: List[Dict],
        out: Dict[str, Classification],
    ) -> List[Dict]:
        """Group prospects by normalized phone number; 2+ = Multi-Site."""
        phone_groups: Dict[str, List[Dict]] = defaultdict(list)
        no_phone = []

        for p in prospects:
            norm = self._normalize_phone(p.get("phone"))
            if norm:
                phone_groups[norm].append(p)
            else:
                no_phone.append(p)

        remaining = list(no_phone)
        multi_count = 0

        for phone, group in phone_groups.items():
            if len(group) >= 2:
                # Find brand core from the group names
                parent = self._extract_brand_core([g["name"] for g in group])
                for p in group:
                    out[p["yelp_id"]] = Classification(
                        yelp_id=p["yelp_id"],
                        ownership_type="Multi-Site",
                        parent_entity=parent,
                        location_count=len(group),
                        confidence=0.90,
                        stage="phone_cluster",
                    )
                multi_count += len(group)
            else:
                remaining.extend(group)

        logger.info(f"Phone clustering: {multi_count} prospects in multi-site groups")
        return remaining

    # ------------------------------------------------------------------
    # Stage 4: Name Clustering (token-blocked)
    # ------------------------------------------------------------------

    def _cluster_by_name(
        self,
        prospects: List[Dict],
        out: Dict[str, Classification],
    ) -> List[Dict]:
        """
        Fuzzy-match prospect names against each other using token-based blocking.

        Blocking: group by first significant word, only compare within blocks.
        """
        # Build blocking index
        blocks: Dict[str, List[Dict]] = defaultdict(list)
        for p in prospects:
            key = self._blocking_key(p["name"])
            if key:
                blocks[key].append(p)

        classified_ids: Set[str] = set()
        clusters: List[List[Dict]] = []

        for key, block in blocks.items():
            if len(block) < 2:
                continue

            # Find clusters within this block
            used: Set[int] = set()
            for i in range(len(block)):
                if i in used:
                    continue
                cluster = [block[i]]
                used.add(i)
                name_i = self._name_matcher.normalize(block[i]["name"])
                for j in range(i + 1, len(block)):
                    if j in used:
                        continue
                    name_j = self._name_matcher.normalize(block[j]["name"])
                    sim = similarity_ratio(name_i, name_j)
                    if sim >= 0.75:
                        cluster.append(block[j])
                        used.add(j)
                if len(cluster) >= 2:
                    clusters.append(cluster)
                    for p in cluster:
                        classified_ids.add(p["yelp_id"])

        # Apply classifications for clusters
        for cluster in clusters:
            names = [p["name"] for p in cluster]
            parent = self._extract_brand_core(names)
            confidence = 0.70 if len(cluster) == 2 else min(0.95, 0.70 + 0.05 * len(cluster))
            for p in cluster:
                out[p["yelp_id"]] = Classification(
                    yelp_id=p["yelp_id"],
                    ownership_type="Multi-Site",
                    parent_entity=parent,
                    location_count=len(cluster),
                    confidence=confidence,
                    stage="name_cluster",
                )

        remaining = [p for p in prospects if p["yelp_id"] not in classified_ids]
        logger.info(
            f"Name clustering: {len(classified_ids)} prospects in "
            f"{len(clusters)} clusters"
        )
        return remaining

    # ------------------------------------------------------------------
    # Stage 5: Pattern Heuristics
    # ------------------------------------------------------------------

    def _classify_by_pattern(
        self,
        prospects: List[Dict],
        out: Dict[str, Classification],
    ) -> List[Dict]:
        """Regex-based heuristics for chain naming patterns."""
        remaining = []

        for p in prospects:
            name = p["name"]

            # Store number: "Brand #123"
            if STORE_NUMBER_PATTERN.search(name):
                brand = STORE_NUMBER_PATTERN.sub("", name).strip().rstrip("-|: ")
                out[p["yelp_id"]] = Classification(
                    yelp_id=p["yelp_id"],
                    ownership_type="Multi-Site",
                    parent_entity=brand or name,
                    confidence=0.65,
                    stage="pattern",
                )
                continue

            # "Brand - City" pattern
            m = BRAND_CITY_PATTERN.match(name)
            if m:
                brand = m.group(1).strip()
                out[p["yelp_id"]] = Classification(
                    yelp_id=p["yelp_id"],
                    ownership_type="Multi-Site",
                    parent_entity=brand,
                    confidence=0.65,
                    stage="pattern",
                )
                continue

            # Franchise keywords in name
            if CHAIN_KEYWORDS.search(name):
                out[p["yelp_id"]] = Classification(
                    yelp_id=p["yelp_id"],
                    ownership_type="Multi-Site",
                    parent_entity=name,
                    confidence=0.65,
                    stage="pattern",
                )
                continue

            remaining.append(p)

        classified = len(prospects) - len(remaining)
        logger.info(f"Pattern heuristics: {classified} matched")
        return remaining

    # ------------------------------------------------------------------
    # Persist
    # ------------------------------------------------------------------

    def _apply_classifications(self, classifications: Dict[str, Classification]) -> None:
        """Batch UPDATE prospects with classification results."""
        if not classifications:
            return

        now = datetime.utcnow()
        batch_size = 500

        items = list(classifications.values())
        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            # Use individual updates — reliable across all PG versions
            for c in batch:
                self.db.execute(
                    text("""
                        UPDATE medspa_prospects
                        SET ownership_type = :otype,
                            parent_entity = :parent,
                            location_count = :loc_count,
                            classification_confidence = :confidence,
                            classified_at = :classified_at,
                            adjusted_acquisition_score = acquisition_score + :penalty
                        WHERE yelp_id = :yelp_id
                    """),
                    {
                        "otype": c.ownership_type,
                        "parent": c.parent_entity,
                        "loc_count": c.location_count,
                        "confidence": c.confidence,
                        "classified_at": now,
                        "penalty": OWNERSHIP_PENALTIES.get(c.ownership_type, 0),
                        "yelp_id": c.yelp_id,
                    },
                )

        self.db.commit()
        logger.info(f"Persisted {len(classifications)} classifications")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_pe_companies(self) -> List[Dict]:
        """Load PE portfolio companies for cross-reference."""
        try:
            rows = self.db.execute(text("""
                SELECT name, ticker, current_pe_owner
                FROM pe_portfolio_companies
                WHERE name IS NOT NULL
            """)).fetchall()
            return [
                {"name": r[0], "ticker": r[1], "pe_firm": r[2]}
                for r in rows
            ]
        except Exception as e:
            logger.warning(f"Could not load PE companies: {e}")
            self.db.rollback()
            return []

    @staticmethod
    def _normalize_phone(phone: Optional[str]) -> Optional[str]:
        """Normalize phone to digits only, strip leading 1."""
        if not phone:
            return None
        digits = re.sub(r"\D", "", phone)
        if len(digits) == 11 and digits.startswith("1"):
            digits = digits[1:]
        return digits if len(digits) >= 10 else None

    def _match_against_companies(
        self, prospect_name: str, company_names: List[str]
    ) -> Optional[Tuple[str, float]]:
        """
        Try to match a prospect name against a list of company names.

        Tries multiple strategies:
        1. Strip location suffix ("Brand - City") then fuzzy match
        2. Check if any company name is a prefix of the prospect name
        """
        clean_name = self._strip_location_suffix(prospect_name)

        # Strategy 1: Direct fuzzy match on cleaned name
        matches = self._pe_matcher.find_matches(clean_name, company_names, top_n=1)
        if matches:
            return matches[0]

        # Strategy 2: Check if company name is a prefix (e.g., "Massage Envy" in "Massage Envy Tucson")
        norm_prospect = self._pe_matcher.normalize(prospect_name)
        for co_name in company_names:
            norm_co = self._pe_matcher.normalize(co_name)
            if norm_co and norm_prospect.startswith(norm_co) and len(norm_co) >= 5:
                return (co_name, 0.85)

        return None

    @staticmethod
    def _strip_location_suffix(name: str) -> str:
        """Strip location suffixes like ' - West Hollywood' for PE matching."""
        stripped = LOCATION_SUFFIX_PATTERN.sub("", name).strip()
        return stripped if stripped else name

    @staticmethod
    def _blocking_key(name: str) -> Optional[str]:
        """Extract first significant word for blocking."""
        words = re.sub(r"[^\w\s]", "", name.lower()).split()
        for w in words:
            if w not in STOP_WORDS and len(w) > 2:
                return w
        return None

    @staticmethod
    def _extract_brand_core(names: List[str]) -> str:
        """
        Extract the common brand name from a list of similar names.

        Uses longest common prefix of normalized names, falling back to
        the shortest name.
        """
        if not names:
            return ""
        if len(names) == 1:
            return names[0]

        # Normalize: strip location suffixes
        cleaned = [LOCATION_SUFFIX_PATTERN.sub("", n).strip() for n in names]
        cleaned = [n for n in cleaned if n]
        if not cleaned:
            return names[0]

        # Find longest common prefix (word-level)
        words_list = [n.split() for n in cleaned]
        prefix_words = []
        for word_group in zip(*words_list):
            if len(set(w.lower() for w in word_group)) == 1:
                prefix_words.append(word_group[0])
            else:
                break

        if prefix_words:
            return " ".join(prefix_words)

        # Fallback: shortest name
        return min(cleaned, key=len)

    def _build_summary(
        self, classifications: Dict[str, Classification], duration_ms: int
    ) -> Dict[str, Any]:
        """Build summary dict from classifications."""
        by_type: Dict[str, int] = defaultdict(int)
        by_stage: Dict[str, int] = defaultdict(int)

        for c in classifications.values():
            by_type[c.ownership_type] += 1
            by_stage[c.stage] += 1

        return {
            "total": len(classifications),
            "classified": len(classifications),
            "by_type": dict(by_type),
            "by_stage": dict(by_stage),
            "duration_ms": duration_ms,
        }
