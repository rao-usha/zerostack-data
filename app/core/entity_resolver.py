"""
Entity Resolution Service (T37)

Intelligent matching and deduplication of entities (companies, investors)
across multiple data sources. Assigns canonical entity IDs, tracks aliases,
and supports merge/split operations with full audit trail.

Features:
- Multi-stage matching: exact identifiers -> domain -> name+location -> name-only
- Configurable confidence thresholds
- Manual override support
- Merge/split with rollback capability
"""
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from sqlalchemy import (
    Column, Integer, String, Float, Boolean, Text, DateTime, JSON,
    ForeignKey, Index, UniqueConstraint, and_, or_, func
)
from sqlalchemy.orm import Session

from app.core.models import Base
from app.agentic.fuzzy_matcher import CompanyNameMatcher, similarity_ratio

logger = logging.getLogger(__name__)


# =============================================================================
# ENUMS
# =============================================================================

class EntityType(str, Enum):
    """Supported entity types."""
    COMPANY = "company"
    INVESTOR = "investor"
    PERSON = "person"


class MatchMethod(str, Enum):
    """How a match was determined."""
    EXACT_CIK = "exact_cik"
    EXACT_CRD = "exact_crd"
    EXACT_TICKER = "exact_ticker"
    EXACT_CUSIP = "exact_cusip"
    EXACT_LEI = "exact_lei"
    DOMAIN_MATCH = "domain_match"
    NAME_LOCATION_MATCH = "name_location_match"
    NAME_ONLY_MATCH = "name_only_match"
    MANUAL = "manual"
    NEW_ENTITY = "new_entity"


class MergeAction(str, Enum):
    """Types of merge history actions."""
    CREATE = "create"
    MERGE = "merge"
    SPLIT = "split"
    UPDATE = "update"
    ADD_ALIAS = "add_alias"


# =============================================================================
# DATABASE MODELS
# =============================================================================

class CanonicalEntity(Base):
    """
    Master entity records with canonical names and identifiers.

    Each unique real-world entity (company, investor) gets one record.
    All name variants are stored in entity_aliases.
    """
    __tablename__ = "canonical_entities"

    id = Column(Integer, primary_key=True, autoincrement=True)
    entity_type = Column(String(50), nullable=False, index=True)  # company, investor, person
    canonical_name = Column(String(500), nullable=False)
    normalized_name = Column(String(500), nullable=False, index=True)

    # Standard identifiers (from various sources)
    cik = Column(String(20), nullable=True, index=True)  # SEC CIK
    crd = Column(String(20), nullable=True, index=True)  # SEC CRD (advisers)
    ticker = Column(String(20), nullable=True, index=True)
    cusip = Column(String(20), nullable=True, index=True)
    lei = Column(String(50), nullable=True, index=True)  # Legal Entity Identifier
    ein = Column(String(20), nullable=True)  # Employer ID Number

    # Web presence
    website = Column(String(500), nullable=True)
    domain = Column(String(255), nullable=True, index=True)  # Normalized domain

    # Location
    city = Column(String(200), nullable=True)
    state = Column(String(100), nullable=True, index=True)
    country = Column(String(100), nullable=True, index=True)

    # Classification
    industry = Column(String(255), nullable=True, index=True)
    entity_subtype = Column(String(100), nullable=True)  # e.g., 'public_pension', 'startup'

    # Statistics
    alias_count = Column(Integer, default=1)
    source_count = Column(Integer, default=1)

    # Metadata
    is_verified = Column(Boolean, default=False)  # Human-verified
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at = Column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_by = Column(String(100), default='system')

    __table_args__ = (
        Index('idx_canonical_type_name', 'entity_type', 'normalized_name'),
        Index('idx_canonical_identifiers', 'cik', 'crd', 'ticker'),
    )

    def __repr__(self) -> str:
        return f"<CanonicalEntity(id={self.id}, type={self.entity_type}, name='{self.canonical_name}')>"


class EntityAlias(Base):
    """
    All known name variants for a canonical entity.

    Tracks where each alias came from and match confidence.
    """
    __tablename__ = "entity_aliases"

    id = Column(Integer, primary_key=True, autoincrement=True)
    canonical_entity_id = Column(Integer, ForeignKey('canonical_entities.id', ondelete='CASCADE'),
                                  nullable=False, index=True)

    alias_name = Column(String(500), nullable=False)
    normalized_alias = Column(String(500), nullable=False, index=True)

    # Source tracking
    source_type = Column(String(100), nullable=True)  # sec_13f, form_d, form_adv, manual, etc.
    source_id = Column(String(255), nullable=True)  # ID in source system
    source_table = Column(String(100), nullable=True)  # Source table name

    # Match metadata
    match_confidence = Column(Float, nullable=True)  # 0.0 to 1.0
    match_method = Column(String(50), nullable=True)  # How this alias was linked
    is_manual_override = Column(Boolean, default=False)
    is_primary = Column(Boolean, default=False)  # Is this the canonical name?

    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)
    created_by = Column(String(100), default='system')

    __table_args__ = (
        UniqueConstraint('canonical_entity_id', 'normalized_alias', name='uq_entity_alias'),
        Index('idx_alias_normalized', 'normalized_alias'),
    )

    def __repr__(self) -> str:
        return f"<EntityAlias(id={self.id}, entity={self.canonical_entity_id}, alias='{self.alias_name}')>"


class EntityMergeHistory(Base):
    """
    Audit trail for all entity modifications.

    Tracks merges, splits, and updates for accountability and rollback.
    """
    __tablename__ = "entity_merge_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    action = Column(String(20), nullable=False, index=True)  # create, merge, split, update

    # For merges: source merged INTO target
    source_entity_id = Column(Integer, nullable=True)
    target_entity_id = Column(Integer, nullable=True)

    # For splits: which aliases moved
    affected_aliases = Column(JSON, nullable=True)

    # Metadata
    reason = Column(Text, nullable=True)
    performed_by = Column(String(100), default='system')
    performed_at = Column(DateTime, nullable=False, default=datetime.utcnow)

    # Rollback support - snapshot before action
    previous_state = Column(JSON, nullable=True)

    __table_args__ = (
        Index('idx_merge_history_action', 'action', 'performed_at'),
    )

    def __repr__(self) -> str:
        return f"<EntityMergeHistory(id={self.id}, action={self.action}, at={self.performed_at})>"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ResolutionResult:
    """Result of entity resolution."""
    canonical_entity_id: int
    canonical_name: str
    entity_type: str
    match_confidence: float
    match_method: str
    is_new: bool
    alternatives: List[Dict[str, Any]]


@dataclass
class DuplicateCandidate:
    """A potential duplicate pair."""
    entity_a_id: int
    entity_a_name: str
    entity_b_id: int
    entity_b_name: str
    confidence: float
    match_method: str


@dataclass
class MergeResult:
    """Result of a merge operation."""
    success: bool
    merged_entity_id: int
    aliases_transferred: int
    merge_history_id: int
    error: Optional[str] = None


# =============================================================================
# ENTITY RESOLVER
# =============================================================================

class EntityResolver:
    """
    Entity resolution engine with multi-stage matching.

    Matching stages (in order of confidence):
    1. Exact identifier match (CIK, CRD, ticker, CUSIP, LEI) - confidence: 1.0
    2. Domain match (same website domain) - confidence: 0.95
    3. Name + location match (fuzzy name + same state/country) - confidence: 0.85-0.95
    4. Name-only match (fuzzy name without location) - confidence: 0.70-0.85

    Thresholds:
    - Auto-merge: confidence >= 0.90
    - Review queue: 0.70 <= confidence < 0.90
    - No match: confidence < 0.70
    """

    # Confidence thresholds
    AUTO_MERGE_THRESHOLD = 0.90
    REVIEW_THRESHOLD = 0.70

    # Match confidence by method
    CONFIDENCE_SCORES = {
        MatchMethod.EXACT_CIK: 1.0,
        MatchMethod.EXACT_CRD: 1.0,
        MatchMethod.EXACT_TICKER: 1.0,
        MatchMethod.EXACT_CUSIP: 1.0,
        MatchMethod.EXACT_LEI: 1.0,
        MatchMethod.DOMAIN_MATCH: 0.95,
        MatchMethod.NAME_LOCATION_MATCH: 0.90,
        MatchMethod.NAME_ONLY_MATCH: 0.75,
        MatchMethod.MANUAL: 1.0,
    }

    def __init__(
        self,
        db: Session,
        name_matcher: Optional[CompanyNameMatcher] = None,
        fuzzy_threshold: float = 0.85
    ):
        """
        Initialize the resolver.

        Args:
            db: Database session
            name_matcher: Optional custom name matcher
            fuzzy_threshold: Threshold for fuzzy name matching
        """
        self.db = db
        self.name_matcher = name_matcher or CompanyNameMatcher(
            similarity_threshold=fuzzy_threshold
        )
        self.fuzzy_threshold = fuzzy_threshold

    # -------------------------------------------------------------------------
    # NORMALIZATION
    # -------------------------------------------------------------------------

    def normalize_name(self, name: str) -> str:
        """Normalize entity name for matching."""
        return self.name_matcher.normalize(name)

    @staticmethod
    def normalize_domain(url_or_domain: str) -> Optional[str]:
        """Extract and normalize domain from URL or domain string."""
        if not url_or_domain:
            return None

        # Add scheme if missing for parsing
        if not url_or_domain.startswith(('http://', 'https://')):
            url_or_domain = 'https://' + url_or_domain

        try:
            parsed = urlparse(url_or_domain)
            domain = parsed.netloc.lower()

            # Remove www prefix
            if domain.startswith('www.'):
                domain = domain[4:]

            # Remove port
            if ':' in domain:
                domain = domain.split(':')[0]

            return domain if domain else None
        except Exception:
            return None

    @staticmethod
    def normalize_identifier(value: Optional[str]) -> Optional[str]:
        """Normalize an identifier (remove whitespace, uppercase)."""
        if not value:
            return None
        return value.strip().upper()

    # -------------------------------------------------------------------------
    # RESOLUTION
    # -------------------------------------------------------------------------

    def resolve(
        self,
        name: str,
        entity_type: str,
        *,
        cik: Optional[str] = None,
        crd: Optional[str] = None,
        ticker: Optional[str] = None,
        cusip: Optional[str] = None,
        lei: Optional[str] = None,
        website: Optional[str] = None,
        state: Optional[str] = None,
        country: Optional[str] = None,
        industry: Optional[str] = None,
        source_type: Optional[str] = None,
        source_id: Optional[str] = None,
        auto_create: bool = True
    ) -> ResolutionResult:
        """
        Resolve an entity name to a canonical entity.

        Attempts matching in order of confidence:
        1. Exact identifier match
        2. Domain match
        3. Name + location match
        4. Name-only match

        Args:
            name: Entity name to resolve
            entity_type: 'company', 'investor', or 'person'
            cik, crd, ticker, cusip, lei: Known identifiers
            website: Entity website
            state, country: Location info
            industry: Industry classification
            source_type: Source of this entity reference
            source_id: ID in source system
            auto_create: Create new entity if no match found

        Returns:
            ResolutionResult with canonical entity info
        """
        normalized_name = self.normalize_name(name)
        domain = self.normalize_domain(website)

        # Normalize identifiers
        cik = self.normalize_identifier(cik)
        crd = self.normalize_identifier(crd)
        ticker = self.normalize_identifier(ticker)
        cusip = self.normalize_identifier(cusip)
        lei = self.normalize_identifier(lei)

        alternatives = []

        # Stage 1: Exact identifier match
        if any([cik, crd, ticker, cusip, lei]):
            match = self._match_by_identifier(
                entity_type, cik=cik, crd=crd, ticker=ticker, cusip=cusip, lei=lei
            )
            if match:
                entity, method = match
                self._add_alias_if_new(entity.id, name, normalized_name,
                                       source_type, source_id, 1.0, method.value)
                return ResolutionResult(
                    canonical_entity_id=entity.id,
                    canonical_name=entity.canonical_name,
                    entity_type=entity.entity_type,
                    match_confidence=1.0,
                    match_method=method.value,
                    is_new=False,
                    alternatives=[]
                )

        # Stage 2: Domain match
        if domain:
            match = self._match_by_domain(entity_type, domain)
            if match:
                confidence = self.CONFIDENCE_SCORES[MatchMethod.DOMAIN_MATCH]
                self._add_alias_if_new(match.id, name, normalized_name,
                                       source_type, source_id, confidence,
                                       MatchMethod.DOMAIN_MATCH.value)
                return ResolutionResult(
                    canonical_entity_id=match.id,
                    canonical_name=match.canonical_name,
                    entity_type=match.entity_type,
                    match_confidence=confidence,
                    match_method=MatchMethod.DOMAIN_MATCH.value,
                    is_new=False,
                    alternatives=[]
                )

        # Stage 3: Name + location match
        if state or country:
            matches = self._match_by_name_location(
                entity_type, normalized_name, state, country
            )
            if matches:
                best_match, confidence = matches[0]

                # Collect alternatives
                for alt_match, alt_conf in matches[1:4]:
                    alternatives.append({
                        'id': alt_match.id,
                        'canonical_name': alt_match.canonical_name,
                        'confidence': alt_conf
                    })

                if confidence >= self.AUTO_MERGE_THRESHOLD:
                    self._add_alias_if_new(best_match.id, name, normalized_name,
                                           source_type, source_id, confidence,
                                           MatchMethod.NAME_LOCATION_MATCH.value)
                    return ResolutionResult(
                        canonical_entity_id=best_match.id,
                        canonical_name=best_match.canonical_name,
                        entity_type=best_match.entity_type,
                        match_confidence=confidence,
                        match_method=MatchMethod.NAME_LOCATION_MATCH.value,
                        is_new=False,
                        alternatives=alternatives
                    )

        # Stage 4: Name-only match
        matches = self._match_by_name_only(entity_type, normalized_name)
        if matches:
            best_match, confidence = matches[0]

            # Collect alternatives
            for alt_match, alt_conf in matches[1:4]:
                alternatives.append({
                    'id': alt_match.id,
                    'canonical_name': alt_match.canonical_name,
                    'confidence': alt_conf
                })

            if confidence >= self.AUTO_MERGE_THRESHOLD:
                self._add_alias_if_new(best_match.id, name, normalized_name,
                                       source_type, source_id, confidence,
                                       MatchMethod.NAME_ONLY_MATCH.value)
                return ResolutionResult(
                    canonical_entity_id=best_match.id,
                    canonical_name=best_match.canonical_name,
                    entity_type=best_match.entity_type,
                    match_confidence=confidence,
                    match_method=MatchMethod.NAME_ONLY_MATCH.value,
                    is_new=False,
                    alternatives=alternatives
                )
            elif confidence >= self.REVIEW_THRESHOLD:
                # Found potential match but below auto-merge threshold
                # Still return it but note it needs review
                alternatives.insert(0, {
                    'id': best_match.id,
                    'canonical_name': best_match.canonical_name,
                    'confidence': confidence,
                    'needs_review': True
                })

        # No confident match found - create new entity if allowed
        if auto_create:
            new_entity = self._create_entity(
                name=name,
                normalized_name=normalized_name,
                entity_type=entity_type,
                cik=cik,
                crd=crd,
                ticker=ticker,
                cusip=cusip,
                lei=lei,
                website=website,
                domain=domain,
                state=state,
                country=country,
                industry=industry,
                source_type=source_type,
                source_id=source_id
            )
            return ResolutionResult(
                canonical_entity_id=new_entity.id,
                canonical_name=new_entity.canonical_name,
                entity_type=new_entity.entity_type,
                match_confidence=1.0,
                match_method=MatchMethod.NEW_ENTITY.value,
                is_new=True,
                alternatives=alternatives
            )

        # No match and auto_create=False
        return ResolutionResult(
            canonical_entity_id=0,
            canonical_name="",
            entity_type=entity_type,
            match_confidence=0.0,
            match_method="no_match",
            is_new=False,
            alternatives=alternatives
        )

    # -------------------------------------------------------------------------
    # MATCHING METHODS
    # -------------------------------------------------------------------------

    def _match_by_identifier(
        self,
        entity_type: str,
        cik: Optional[str] = None,
        crd: Optional[str] = None,
        ticker: Optional[str] = None,
        cusip: Optional[str] = None,
        lei: Optional[str] = None
    ) -> Optional[Tuple[CanonicalEntity, MatchMethod]]:
        """Match by exact identifier."""
        query = self.db.query(CanonicalEntity).filter(
            CanonicalEntity.entity_type == entity_type
        )

        # Check each identifier
        if cik:
            match = query.filter(CanonicalEntity.cik == cik).first()
            if match:
                return (match, MatchMethod.EXACT_CIK)

        if crd:
            match = query.filter(CanonicalEntity.crd == crd).first()
            if match:
                return (match, MatchMethod.EXACT_CRD)

        if ticker:
            match = query.filter(CanonicalEntity.ticker == ticker).first()
            if match:
                return (match, MatchMethod.EXACT_TICKER)

        if cusip:
            match = query.filter(CanonicalEntity.cusip == cusip).first()
            if match:
                return (match, MatchMethod.EXACT_CUSIP)

        if lei:
            match = query.filter(CanonicalEntity.lei == lei).first()
            if match:
                return (match, MatchMethod.EXACT_LEI)

        return None

    def _match_by_domain(
        self,
        entity_type: str,
        domain: str
    ) -> Optional[CanonicalEntity]:
        """Match by website domain."""
        return self.db.query(CanonicalEntity).filter(
            CanonicalEntity.entity_type == entity_type,
            CanonicalEntity.domain == domain
        ).first()

    def _match_by_name_location(
        self,
        entity_type: str,
        normalized_name: str,
        state: Optional[str],
        country: Optional[str]
    ) -> List[Tuple[CanonicalEntity, float]]:
        """Match by fuzzy name + location."""
        # Get candidates with matching location
        query = self.db.query(CanonicalEntity).filter(
            CanonicalEntity.entity_type == entity_type
        )

        if state:
            query = query.filter(
                func.upper(CanonicalEntity.state) == state.upper()
            )
        if country:
            query = query.filter(
                func.upper(CanonicalEntity.country) == country.upper()
            )

        candidates = query.limit(500).all()

        # Score by name similarity
        matches = []
        for candidate in candidates:
            sim = similarity_ratio(normalized_name, candidate.normalized_name)
            if sim >= self.fuzzy_threshold:
                # Boost confidence for location match
                confidence = min(sim + 0.05, 1.0)
                matches.append((candidate, confidence))

        # Sort by confidence descending
        matches.sort(key=lambda x: x[1], reverse=True)
        return matches

    def _match_by_name_only(
        self,
        entity_type: str,
        normalized_name: str
    ) -> List[Tuple[CanonicalEntity, float]]:
        """Match by fuzzy name only."""
        # Check aliases first (more comprehensive)
        alias_matches = self.db.query(EntityAlias, CanonicalEntity).join(
            CanonicalEntity,
            EntityAlias.canonical_entity_id == CanonicalEntity.id
        ).filter(
            CanonicalEntity.entity_type == entity_type
        ).all()

        matches = []
        seen_ids = set()

        for alias, entity in alias_matches:
            if entity.id in seen_ids:
                continue

            sim = similarity_ratio(normalized_name, alias.normalized_alias)
            if sim >= self.fuzzy_threshold:
                matches.append((entity, sim))
                seen_ids.add(entity.id)

        # Also check canonical names directly
        entities = self.db.query(CanonicalEntity).filter(
            CanonicalEntity.entity_type == entity_type
        ).all()

        for entity in entities:
            if entity.id in seen_ids:
                continue

            sim = similarity_ratio(normalized_name, entity.normalized_name)
            if sim >= self.fuzzy_threshold:
                matches.append((entity, sim))
                seen_ids.add(entity.id)

        # Sort by confidence descending
        matches.sort(key=lambda x: x[1], reverse=True)
        return matches

    # -------------------------------------------------------------------------
    # ENTITY CREATION
    # -------------------------------------------------------------------------

    def _create_entity(
        self,
        name: str,
        normalized_name: str,
        entity_type: str,
        **kwargs
    ) -> CanonicalEntity:
        """Create a new canonical entity."""
        entity = CanonicalEntity(
            entity_type=entity_type,
            canonical_name=name,
            normalized_name=normalized_name,
            cik=kwargs.get('cik'),
            crd=kwargs.get('crd'),
            ticker=kwargs.get('ticker'),
            cusip=kwargs.get('cusip'),
            lei=kwargs.get('lei'),
            website=kwargs.get('website'),
            domain=kwargs.get('domain'),
            state=kwargs.get('state'),
            country=kwargs.get('country'),
            industry=kwargs.get('industry'),
            alias_count=1,
            source_count=1
        )
        self.db.add(entity)
        self.db.flush()

        # Add primary alias
        alias = EntityAlias(
            canonical_entity_id=entity.id,
            alias_name=name,
            normalized_alias=normalized_name,
            source_type=kwargs.get('source_type'),
            source_id=kwargs.get('source_id'),
            match_confidence=1.0,
            match_method=MatchMethod.NEW_ENTITY.value,
            is_primary=True
        )
        self.db.add(alias)

        # Record creation in history
        history = EntityMergeHistory(
            action=MergeAction.CREATE.value,
            target_entity_id=entity.id,
            reason=f"New entity created from source: {kwargs.get('source_type', 'unknown')}",
            performed_by='system'
        )
        self.db.add(history)

        self.db.commit()

        logger.info(f"Created new canonical entity: {entity.id} - {name}")
        return entity

    def _add_alias_if_new(
        self,
        entity_id: int,
        alias_name: str,
        normalized_alias: str,
        source_type: Optional[str],
        source_id: Optional[str],
        confidence: float,
        match_method: str
    ) -> bool:
        """Add alias if it doesn't already exist for this entity."""
        existing = self.db.query(EntityAlias).filter(
            EntityAlias.canonical_entity_id == entity_id,
            EntityAlias.normalized_alias == normalized_alias
        ).first()

        if existing:
            return False

        alias = EntityAlias(
            canonical_entity_id=entity_id,
            alias_name=alias_name,
            normalized_alias=normalized_alias,
            source_type=source_type,
            source_id=source_id,
            match_confidence=confidence,
            match_method=match_method
        )
        self.db.add(alias)

        # Update alias count
        self.db.query(CanonicalEntity).filter(
            CanonicalEntity.id == entity_id
        ).update({
            CanonicalEntity.alias_count: CanonicalEntity.alias_count + 1
        })

        self.db.commit()
        logger.debug(f"Added alias '{alias_name}' to entity {entity_id}")
        return True

    # -------------------------------------------------------------------------
    # ALIASES
    # -------------------------------------------------------------------------

    def get_aliases(self, entity_id: int) -> List[EntityAlias]:
        """Get all aliases for a canonical entity."""
        return self.db.query(EntityAlias).filter(
            EntityAlias.canonical_entity_id == entity_id
        ).order_by(EntityAlias.is_primary.desc(), EntityAlias.created_at).all()

    def add_manual_alias(
        self,
        entity_id: int,
        alias_name: str,
        performed_by: str = 'manual'
    ) -> EntityAlias:
        """Manually add an alias to an entity."""
        normalized = self.normalize_name(alias_name)

        # Check if alias already exists
        existing = self.db.query(EntityAlias).filter(
            EntityAlias.canonical_entity_id == entity_id,
            EntityAlias.normalized_alias == normalized
        ).first()

        if existing:
            raise ValueError(f"Alias '{alias_name}' already exists for entity {entity_id}")

        alias = EntityAlias(
            canonical_entity_id=entity_id,
            alias_name=alias_name,
            normalized_alias=normalized,
            source_type='manual',
            match_confidence=1.0,
            match_method=MatchMethod.MANUAL.value,
            is_manual_override=True,
            created_by=performed_by
        )
        self.db.add(alias)

        # Update alias count
        self.db.query(CanonicalEntity).filter(
            CanonicalEntity.id == entity_id
        ).update({
            CanonicalEntity.alias_count: CanonicalEntity.alias_count + 1
        })

        # Record in history
        history = EntityMergeHistory(
            action=MergeAction.ADD_ALIAS.value,
            target_entity_id=entity_id,
            affected_aliases=[alias_name],
            reason=f"Manual alias addition",
            performed_by=performed_by
        )
        self.db.add(history)

        self.db.commit()
        return alias

    # -------------------------------------------------------------------------
    # MERGE / SPLIT
    # -------------------------------------------------------------------------

    def merge_entities(
        self,
        source_entity_id: int,
        target_entity_id: int,
        reason: Optional[str] = None,
        performed_by: str = 'system'
    ) -> MergeResult:
        """
        Merge source entity into target entity.

        All aliases from source are transferred to target.
        Source entity is deleted.
        """
        if source_entity_id == target_entity_id:
            return MergeResult(
                success=False,
                merged_entity_id=0,
                aliases_transferred=0,
                merge_history_id=0,
                error="Cannot merge entity into itself"
            )

        source = self.db.query(CanonicalEntity).filter(
            CanonicalEntity.id == source_entity_id
        ).first()

        target = self.db.query(CanonicalEntity).filter(
            CanonicalEntity.id == target_entity_id
        ).first()

        if not source or not target:
            return MergeResult(
                success=False,
                merged_entity_id=0,
                aliases_transferred=0,
                merge_history_id=0,
                error="Source or target entity not found"
            )

        # Snapshot source state for rollback
        source_aliases = self.get_aliases(source_entity_id)
        previous_state = {
            'source_entity': {
                'id': source.id,
                'canonical_name': source.canonical_name,
                'entity_type': source.entity_type,
                'cik': source.cik,
                'crd': source.crd,
                'ticker': source.ticker
            },
            'source_aliases': [
                {'alias_name': a.alias_name, 'source_type': a.source_type}
                for a in source_aliases
            ]
        }

        # Transfer aliases
        transferred = 0
        for alias in source_aliases:
            # Check if alias already exists on target
            existing = self.db.query(EntityAlias).filter(
                EntityAlias.canonical_entity_id == target_entity_id,
                EntityAlias.normalized_alias == alias.normalized_alias
            ).first()

            if existing:
                # Delete duplicate
                self.db.delete(alias)
            else:
                # Transfer to target
                alias.canonical_entity_id = target_entity_id
                alias.is_primary = False  # Only target's primary stays primary
                transferred += 1

        # Merge identifiers (prefer non-null values)
        if source.cik and not target.cik:
            target.cik = source.cik
        if source.crd and not target.crd:
            target.crd = source.crd
        if source.ticker and not target.ticker:
            target.ticker = source.ticker
        if source.cusip and not target.cusip:
            target.cusip = source.cusip
        if source.lei and not target.lei:
            target.lei = source.lei
        if source.website and not target.website:
            target.website = source.website
            target.domain = source.domain

        # Update counts
        target.alias_count = self.db.query(EntityAlias).filter(
            EntityAlias.canonical_entity_id == target_entity_id
        ).count()
        target.source_count += source.source_count

        # Record in history
        history = EntityMergeHistory(
            action=MergeAction.MERGE.value,
            source_entity_id=source_entity_id,
            target_entity_id=target_entity_id,
            reason=reason or f"Merged {source.canonical_name} into {target.canonical_name}",
            performed_by=performed_by,
            previous_state=previous_state
        )
        self.db.add(history)
        self.db.flush()

        # Delete source entity
        self.db.delete(source)
        self.db.commit()

        logger.info(
            f"Merged entity {source_entity_id} into {target_entity_id}, "
            f"transferred {transferred} aliases"
        )

        return MergeResult(
            success=True,
            merged_entity_id=target_entity_id,
            aliases_transferred=transferred,
            merge_history_id=history.id
        )

    def split_entity(
        self,
        entity_id: int,
        aliases_to_split: List[str],
        new_entity_name: str,
        reason: Optional[str] = None,
        performed_by: str = 'system'
    ) -> Tuple[CanonicalEntity, int]:
        """
        Split aliases from an entity into a new entity.

        Args:
            entity_id: Source entity to split from
            aliases_to_split: List of alias names to move
            new_entity_name: Name for the new entity
            reason: Reason for split
            performed_by: Who performed the split

        Returns:
            Tuple of (new entity, history ID)
        """
        source = self.db.query(CanonicalEntity).filter(
            CanonicalEntity.id == entity_id
        ).first()

        if not source:
            raise ValueError(f"Entity {entity_id} not found")

        # Normalize aliases to split
        normalized_to_split = {self.normalize_name(a) for a in aliases_to_split}

        # Find matching aliases
        source_aliases = self.get_aliases(entity_id)
        aliases_to_move = [
            a for a in source_aliases
            if a.normalized_alias in normalized_to_split
        ]

        if not aliases_to_move:
            raise ValueError("No matching aliases found to split")

        # Snapshot for rollback
        previous_state = {
            'source_entity_id': entity_id,
            'aliases_moved': [a.alias_name for a in aliases_to_move]
        }

        # Create new entity
        normalized_new = self.normalize_name(new_entity_name)
        new_entity = CanonicalEntity(
            entity_type=source.entity_type,
            canonical_name=new_entity_name,
            normalized_name=normalized_new,
            alias_count=len(aliases_to_move),
            source_count=1,
            created_by=performed_by
        )
        self.db.add(new_entity)
        self.db.flush()

        # Move aliases to new entity
        for alias in aliases_to_move:
            alias.canonical_entity_id = new_entity.id

        # Set primary alias on new entity
        aliases_to_move[0].is_primary = True

        # Update source alias count
        source.alias_count = self.db.query(EntityAlias).filter(
            EntityAlias.canonical_entity_id == entity_id
        ).count()

        # Record in history
        history = EntityMergeHistory(
            action=MergeAction.SPLIT.value,
            source_entity_id=entity_id,
            target_entity_id=new_entity.id,
            affected_aliases=aliases_to_split,
            reason=reason or f"Split {len(aliases_to_move)} aliases into new entity",
            performed_by=performed_by,
            previous_state=previous_state
        )
        self.db.add(history)

        self.db.commit()

        logger.info(
            f"Split {len(aliases_to_move)} aliases from entity {entity_id} "
            f"into new entity {new_entity.id}"
        )

        return new_entity, history.id

    # -------------------------------------------------------------------------
    # DUPLICATE DETECTION
    # -------------------------------------------------------------------------

    def find_duplicates(
        self,
        entity_type: Optional[str] = None,
        min_confidence: float = 0.70,
        max_confidence: float = 0.90,
        limit: int = 50
    ) -> List[DuplicateCandidate]:
        """
        Find potential duplicate entities for review.

        Returns pairs with confidence in the review range.
        """
        query = self.db.query(CanonicalEntity)

        if entity_type:
            query = query.filter(CanonicalEntity.entity_type == entity_type)

        entities = query.order_by(CanonicalEntity.id).all()

        duplicates = []
        seen_pairs = set()

        for i, entity_a in enumerate(entities):
            if len(duplicates) >= limit:
                break

            for entity_b in entities[i+1:]:
                if len(duplicates) >= limit:
                    break

                # Skip if already seen
                pair_key = (min(entity_a.id, entity_b.id), max(entity_a.id, entity_b.id))
                if pair_key in seen_pairs:
                    continue
                seen_pairs.add(pair_key)

                # Calculate similarity
                sim = similarity_ratio(
                    entity_a.normalized_name,
                    entity_b.normalized_name
                )

                if min_confidence <= sim < max_confidence:
                    # Determine match method
                    if entity_a.state and entity_b.state and entity_a.state == entity_b.state:
                        method = MatchMethod.NAME_LOCATION_MATCH.value
                    else:
                        method = MatchMethod.NAME_ONLY_MATCH.value

                    duplicates.append(DuplicateCandidate(
                        entity_a_id=entity_a.id,
                        entity_a_name=entity_a.canonical_name,
                        entity_b_id=entity_b.id,
                        entity_b_name=entity_b.canonical_name,
                        confidence=sim,
                        match_method=method
                    ))

        # Sort by confidence descending
        duplicates.sort(key=lambda x: x.confidence, reverse=True)
        return duplicates[:limit]

    # -------------------------------------------------------------------------
    # LOOKUP
    # -------------------------------------------------------------------------

    def get_entity(self, entity_id: int) -> Optional[CanonicalEntity]:
        """Get a canonical entity by ID."""
        return self.db.query(CanonicalEntity).filter(
            CanonicalEntity.id == entity_id
        ).first()

    def search_entities(
        self,
        query: str,
        entity_type: Optional[str] = None,
        limit: int = 20
    ) -> List[CanonicalEntity]:
        """Search entities by name."""
        normalized = self.normalize_name(query)

        db_query = self.db.query(CanonicalEntity)

        if entity_type:
            db_query = db_query.filter(CanonicalEntity.entity_type == entity_type)

        # Search in canonical names
        db_query = db_query.filter(
            CanonicalEntity.normalized_name.ilike(f"%{normalized}%")
        )

        return db_query.limit(limit).all()

    def get_entity_by_identifier(
        self,
        identifier_type: str,
        identifier_value: str,
        entity_type: Optional[str] = None
    ) -> Optional[CanonicalEntity]:
        """Get entity by a specific identifier."""
        query = self.db.query(CanonicalEntity)

        if entity_type:
            query = query.filter(CanonicalEntity.entity_type == entity_type)

        identifier_value = self.normalize_identifier(identifier_value)

        if identifier_type == 'cik':
            query = query.filter(CanonicalEntity.cik == identifier_value)
        elif identifier_type == 'crd':
            query = query.filter(CanonicalEntity.crd == identifier_value)
        elif identifier_type == 'ticker':
            query = query.filter(CanonicalEntity.ticker == identifier_value)
        elif identifier_type == 'cusip':
            query = query.filter(CanonicalEntity.cusip == identifier_value)
        elif identifier_type == 'lei':
            query = query.filter(CanonicalEntity.lei == identifier_value)
        else:
            raise ValueError(f"Unknown identifier type: {identifier_type}")

        return query.first()

    # -------------------------------------------------------------------------
    # STATISTICS
    # -------------------------------------------------------------------------

    def get_stats(self) -> Dict[str, Any]:
        """Get entity resolution statistics."""
        total = self.db.query(CanonicalEntity).count()

        by_type = self.db.query(
            CanonicalEntity.entity_type,
            func.count(CanonicalEntity.id)
        ).group_by(CanonicalEntity.entity_type).all()

        total_aliases = self.db.query(EntityAlias).count()

        recent_merges = self.db.query(EntityMergeHistory).filter(
            EntityMergeHistory.action == MergeAction.MERGE.value
        ).count()

        return {
            'total_entities': total,
            'by_type': {t: c for t, c in by_type},
            'total_aliases': total_aliases,
            'total_merges': recent_merges,
            'avg_aliases_per_entity': round(total_aliases / max(total, 1), 2)
        }
