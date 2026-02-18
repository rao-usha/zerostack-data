"""
Ingestion orchestration for the public_lp_strategies source.

High-level entry points for:
- Registering LP funds
- Registering documents
- Storing text sections
- Upserting strategy snapshots and related data

These functions are deterministic and idempotent where possible.
They do NOT fetch or parse documents; they assume structured inputs are provided.
"""

import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy import text

from app.core.models import (
    LpFund,
    LpDocument,
    LpDocumentTextSection,
    LpStrategySnapshot,
    LpAssetClassTargetAllocation,
    LpAssetClassProjection,
    LpManagerOrVehicleExposure,
    LpStrategyThematicTag,
    LpKeyContact,
    DatasetRegistry,
)
from app.sources.public_lp_strategies.types import (
    LpFundInput,
    LpDocumentInput,
    DocumentTextSectionInput,
    StrategySnapshotInput,
    AssetClassAllocationInput,
    AssetClassProjectionInput,
    ThematicTagInput,
    LpKeyContactInput,
    ContactExtractionResult,
)

logger = logging.getLogger(__name__)


# =============================================================================
# LP FUND REGISTRATION
# =============================================================================


def register_lp_fund(db: Session, fund_input: LpFundInput) -> LpFund:
    """
    Register or retrieve an LP fund.

    Idempotent: If fund with same name exists, returns existing fund.
    Otherwise creates new fund.

    Args:
        db: Database session
        fund_input: LP fund data

    Returns:
        LpFund instance (existing or newly created)
    """
    # Check if fund already exists
    existing = db.query(LpFund).filter(LpFund.name == fund_input.name).first()

    if existing:
        logger.info(f"LP fund '{fund_input.name}' already exists with id={existing.id}")
        return existing

    # Create new fund
    fund = LpFund(
        name=fund_input.name,
        formal_name=fund_input.formal_name,
        lp_type=fund_input.lp_type,
        jurisdiction=fund_input.jurisdiction,
        website_url=fund_input.website_url,
    )
    db.add(fund)
    db.commit()
    db.refresh(fund)

    logger.info(f"Registered new LP fund: {fund_input.name} (id={fund.id})")
    return fund


# =============================================================================
# DOCUMENT REGISTRATION
# =============================================================================


def register_lp_document(db: Session, document_input: LpDocumentInput) -> LpDocument:
    """
    Register an LP document.

    Creates a new document record. Does not check for duplicates
    (assumes caller manages document uniqueness if needed).

    Args:
        db: Database session
        document_input: Document data

    Returns:
        LpDocument instance
    """
    document = LpDocument(
        lp_id=document_input.lp_id,
        title=document_input.title,
        document_type=document_input.document_type,
        program=document_input.program,
        report_period_start=document_input.report_period_start,
        report_period_end=document_input.report_period_end,
        fiscal_year=document_input.fiscal_year,
        fiscal_quarter=document_input.fiscal_quarter,
        source_url=document_input.source_url,
        file_format=document_input.file_format,
        raw_file_location=document_input.raw_file_location,
    )
    db.add(document)
    db.commit()
    db.refresh(document)

    logger.info(
        f"Registered document: '{document_input.title}' "
        f"(id={document.id}, lp_id={document_input.lp_id}, "
        f"FY{document_input.fiscal_year}-{document_input.fiscal_quarter})"
    )
    return document


# =============================================================================
# TEXT SECTION STORAGE
# =============================================================================


def store_document_text_sections(
    db: Session, document_id: int, sections: List[DocumentTextSectionInput]
) -> List[LpDocumentTextSection]:
    """
    Store text sections for a document.

    Creates new text section records for the given document.

    Args:
        db: Database session
        document_id: FK to lp_document.id
        sections: List of text sections

    Returns:
        List of created LpDocumentTextSection instances
    """
    created_sections = []

    for section_input in sections:
        section = LpDocumentTextSection(
            document_id=document_id,
            section_name=section_input.section_name,
            page_start=section_input.page_start,
            page_end=section_input.page_end,
            sequence_order=section_input.sequence_order,
            text=section_input.text,
            embedding_vector=section_input.embedding_vector,
            language=section_input.language,
        )
        db.add(section)
        created_sections.append(section)

    db.commit()

    for section in created_sections:
        db.refresh(section)

    logger.info(
        f"Stored {len(created_sections)} text sections for document_id={document_id}"
    )
    return created_sections


# =============================================================================
# STRATEGY SNAPSHOT UPSERT
# =============================================================================


def upsert_strategy_snapshot(
    db: Session, strategy_input: StrategySnapshotInput
) -> LpStrategySnapshot:
    """
    Upsert an LP strategy snapshot.

    Idempotent: If a snapshot exists for the same (lp_id, program, fiscal_year, fiscal_quarter),
    updates the existing record. Otherwise creates new record.

    Args:
        db: Database session
        strategy_input: Strategy snapshot data

    Returns:
        LpStrategySnapshot instance (existing or newly created)
    """
    # Check for existing snapshot
    existing = (
        db.query(LpStrategySnapshot)
        .filter(
            LpStrategySnapshot.lp_id == strategy_input.lp_id,
            LpStrategySnapshot.program == strategy_input.program,
            LpStrategySnapshot.fiscal_year == strategy_input.fiscal_year,
            LpStrategySnapshot.fiscal_quarter == strategy_input.fiscal_quarter,
        )
        .first()
    )

    if existing:
        # Update existing snapshot
        existing.strategy_date = strategy_input.strategy_date
        existing.primary_document_id = strategy_input.primary_document_id
        existing.summary_text = strategy_input.summary_text
        existing.risk_positioning = strategy_input.risk_positioning
        existing.liquidity_profile = strategy_input.liquidity_profile
        existing.tilt_description = strategy_input.tilt_description
        db.commit()
        db.refresh(existing)

        logger.info(
            f"Updated strategy snapshot: lp_id={strategy_input.lp_id}, "
            f"program={strategy_input.program}, FY{strategy_input.fiscal_year}-{strategy_input.fiscal_quarter}"
        )
        return existing

    # Create new snapshot
    snapshot = LpStrategySnapshot(
        lp_id=strategy_input.lp_id,
        program=strategy_input.program,
        fiscal_year=strategy_input.fiscal_year,
        fiscal_quarter=strategy_input.fiscal_quarter,
        strategy_date=strategy_input.strategy_date,
        primary_document_id=strategy_input.primary_document_id,
        summary_text=strategy_input.summary_text,
        risk_positioning=strategy_input.risk_positioning,
        liquidity_profile=strategy_input.liquidity_profile,
        tilt_description=strategy_input.tilt_description,
    )
    db.add(snapshot)
    db.commit()
    db.refresh(snapshot)

    logger.info(
        f"Created strategy snapshot: id={snapshot.id}, lp_id={strategy_input.lp_id}, "
        f"program={strategy_input.program}, FY{strategy_input.fiscal_year}-{strategy_input.fiscal_quarter}"
    )
    return snapshot


# =============================================================================
# ASSET CLASS ALLOCATION UPSERT
# =============================================================================


def upsert_asset_class_allocations(
    db: Session, strategy_id: int, allocations: List[AssetClassAllocationInput]
) -> List[LpAssetClassTargetAllocation]:
    """
    Upsert asset class allocations for a strategy.

    For each (strategy_id, asset_class) combination:
    - If record exists, updates it
    - Otherwise creates new record

    Args:
        db: Database session
        strategy_id: FK to lp_strategy_snapshot.id
        allocations: List of allocation data

    Returns:
        List of LpAssetClassTargetAllocation instances
    """
    results = []

    for alloc_input in allocations:
        # Check for existing allocation
        existing = (
            db.query(LpAssetClassTargetAllocation)
            .filter(
                LpAssetClassTargetAllocation.strategy_id == strategy_id,
                LpAssetClassTargetAllocation.asset_class == alloc_input.asset_class,
            )
            .first()
        )

        if existing:
            # Update existing
            existing.target_weight_pct = (
                str(alloc_input.target_weight_pct)
                if alloc_input.target_weight_pct is not None
                else None
            )
            existing.min_weight_pct = (
                str(alloc_input.min_weight_pct)
                if alloc_input.min_weight_pct is not None
                else None
            )
            existing.max_weight_pct = (
                str(alloc_input.max_weight_pct)
                if alloc_input.max_weight_pct is not None
                else None
            )
            existing.current_weight_pct = (
                str(alloc_input.current_weight_pct)
                if alloc_input.current_weight_pct is not None
                else None
            )
            existing.benchmark_weight_pct = (
                str(alloc_input.benchmark_weight_pct)
                if alloc_input.benchmark_weight_pct is not None
                else None
            )
            existing.source_section_id = alloc_input.source_section_id
            results.append(existing)
        else:
            # Create new
            allocation = LpAssetClassTargetAllocation(
                strategy_id=strategy_id,
                asset_class=alloc_input.asset_class,
                target_weight_pct=str(alloc_input.target_weight_pct)
                if alloc_input.target_weight_pct is not None
                else None,
                min_weight_pct=str(alloc_input.min_weight_pct)
                if alloc_input.min_weight_pct is not None
                else None,
                max_weight_pct=str(alloc_input.max_weight_pct)
                if alloc_input.max_weight_pct is not None
                else None,
                current_weight_pct=str(alloc_input.current_weight_pct)
                if alloc_input.current_weight_pct is not None
                else None,
                benchmark_weight_pct=str(alloc_input.benchmark_weight_pct)
                if alloc_input.benchmark_weight_pct is not None
                else None,
                source_section_id=alloc_input.source_section_id,
            )
            db.add(allocation)
            results.append(allocation)

    db.commit()

    for item in results:
        db.refresh(item)

    logger.info(
        f"Upserted {len(results)} asset class allocations for strategy_id={strategy_id}"
    )
    return results


# =============================================================================
# ASSET CLASS PROJECTION UPSERT
# =============================================================================


def upsert_asset_class_projections(
    db: Session, strategy_id: int, projections: List[AssetClassProjectionInput]
) -> List[LpAssetClassProjection]:
    """
    Upsert asset class projections for a strategy.

    For each (strategy_id, asset_class, projection_horizon) combination:
    - If record exists, updates it
    - Otherwise creates new record

    Args:
        db: Database session
        strategy_id: FK to lp_strategy_snapshot.id
        projections: List of projection data

    Returns:
        List of LpAssetClassProjection instances
    """
    results = []

    for proj_input in projections:
        # Check for existing projection
        existing = (
            db.query(LpAssetClassProjection)
            .filter(
                LpAssetClassProjection.strategy_id == strategy_id,
                LpAssetClassProjection.asset_class == proj_input.asset_class,
                LpAssetClassProjection.projection_horizon
                == proj_input.projection_horizon,
            )
            .first()
        )

        if existing:
            # Update existing
            existing.net_flow_projection_amount = (
                str(proj_input.net_flow_projection_amount)
                if proj_input.net_flow_projection_amount is not None
                else None
            )
            existing.commitment_plan_amount = (
                str(proj_input.commitment_plan_amount)
                if proj_input.commitment_plan_amount is not None
                else None
            )
            existing.expected_return_pct = (
                str(proj_input.expected_return_pct)
                if proj_input.expected_return_pct is not None
                else None
            )
            existing.expected_volatility_pct = (
                str(proj_input.expected_volatility_pct)
                if proj_input.expected_volatility_pct is not None
                else None
            )
            existing.source_section_id = proj_input.source_section_id
            results.append(existing)
        else:
            # Create new
            projection = LpAssetClassProjection(
                strategy_id=strategy_id,
                asset_class=proj_input.asset_class,
                projection_horizon=proj_input.projection_horizon,
                net_flow_projection_amount=str(proj_input.net_flow_projection_amount)
                if proj_input.net_flow_projection_amount is not None
                else None,
                commitment_plan_amount=str(proj_input.commitment_plan_amount)
                if proj_input.commitment_plan_amount is not None
                else None,
                expected_return_pct=str(proj_input.expected_return_pct)
                if proj_input.expected_return_pct is not None
                else None,
                expected_volatility_pct=str(proj_input.expected_volatility_pct)
                if proj_input.expected_volatility_pct is not None
                else None,
                source_section_id=proj_input.source_section_id,
            )
            db.add(projection)
            results.append(projection)

    db.commit()

    for item in results:
        db.refresh(item)

    logger.info(
        f"Upserted {len(results)} asset class projections for strategy_id={strategy_id}"
    )
    return results


# =============================================================================
# THEMATIC TAG UPSERT
# =============================================================================


def upsert_thematic_tags(
    db: Session, strategy_id: int, tags: List[ThematicTagInput]
) -> List[LpStrategyThematicTag]:
    """
    Upsert thematic tags for a strategy.

    For each (strategy_id, theme) combination:
    - If record exists, updates it
    - Otherwise creates new record

    Args:
        db: Database session
        strategy_id: FK to lp_strategy_snapshot.id
        tags: List of thematic tags

    Returns:
        List of LpStrategyThematicTag instances
    """
    results = []

    for tag_input in tags:
        # Check for existing tag
        existing = (
            db.query(LpStrategyThematicTag)
            .filter(
                LpStrategyThematicTag.strategy_id == strategy_id,
                LpStrategyThematicTag.theme == tag_input.theme,
            )
            .first()
        )

        if existing:
            # Update existing
            existing.relevance_score = (
                str(tag_input.relevance_score)
                if tag_input.relevance_score is not None
                else None
            )
            existing.source_section_id = tag_input.source_section_id
            results.append(existing)
        else:
            # Create new
            tag = LpStrategyThematicTag(
                strategy_id=strategy_id,
                theme=tag_input.theme,
                relevance_score=str(tag_input.relevance_score)
                if tag_input.relevance_score is not None
                else None,
                source_section_id=tag_input.source_section_id,
            )
            db.add(tag)
            results.append(tag)

    db.commit()

    for item in results:
        db.refresh(item)

    logger.info(f"Upserted {len(results)} thematic tags for strategy_id={strategy_id}")
    return results


# =============================================================================
# HIGH-LEVEL ORCHESTRATION
# =============================================================================


def ingest_lp_strategy_document(
    db: Session,
    lp_name: str,
    document_input: LpDocumentInput,
    text_sections: List[DocumentTextSectionInput],
    strategy_input: StrategySnapshotInput,
    allocations: List[AssetClassAllocationInput],
    projections: List[AssetClassProjectionInput],
    thematic_tags: List[ThematicTagInput],
) -> Dict[str, Any]:
    """
    High-level orchestration for ingesting a complete LP strategy document.

    This function:
    1. Ensures LP fund exists
    2. Registers document
    3. Stores text sections
    4. Upserts strategy snapshot
    5. Upserts allocations, projections, and tags

    Args:
        db: Database session
        lp_name: LP fund name (for lookup/creation)
        document_input: Document metadata
        text_sections: Parsed text sections
        strategy_input: Strategy snapshot data
        allocations: Asset class allocations
        projections: Asset class projections
        thematic_tags: Thematic tags

    Returns:
        Dictionary with:
        - lp_fund_id
        - document_id
        - strategy_id
        - sections_count
        - allocations_count
        - projections_count
        - tags_count
    """
    from app.sources.public_lp_strategies.config import KNOWN_LP_FUNDS

    # 1. Ensure LP fund exists
    fund_config = KNOWN_LP_FUNDS.get(lp_name)
    if not fund_config:
        raise ValueError(
            f"Unknown LP fund: {lp_name}. Must be in KNOWN_LP_FUNDS or pre-registered."
        )

    fund_input = LpFundInput(**fund_config)
    lp_fund = register_lp_fund(db, fund_input)

    # Update document_input with lp_id
    document_input.lp_id = lp_fund.id

    # Update strategy_input with lp_id
    strategy_input.lp_id = lp_fund.id

    # 2. Register document
    document = register_lp_document(db, document_input)

    # 3. Store text sections
    sections = store_document_text_sections(db, document.id, text_sections)

    # Update strategy_input with primary_document_id
    strategy_input.primary_document_id = document.id

    # 4. Upsert strategy snapshot
    strategy = upsert_strategy_snapshot(db, strategy_input)

    # 5. Upsert allocations
    alloc_results = upsert_asset_class_allocations(db, strategy.id, allocations)

    # 6. Upsert projections
    proj_results = upsert_asset_class_projections(db, strategy.id, projections)

    # 7. Upsert thematic tags
    tag_results = upsert_thematic_tags(db, strategy.id, thematic_tags)

    logger.info(
        f"Successfully ingested LP strategy document: "
        f"lp={lp_name}, program={strategy.program}, "
        f"FY{strategy.fiscal_year}-{strategy.fiscal_quarter}"
    )

    return {
        "lp_fund_id": lp_fund.id,
        "document_id": document.id,
        "strategy_id": strategy.id,
        "sections_count": len(sections),
        "allocations_count": len(alloc_results),
        "projections_count": len(proj_results),
        "tags_count": len(tag_results),
    }


# =============================================================================
# LP KEY CONTACT INGESTION
# =============================================================================


def register_lp_contact(db: Session, contact_input: LpKeyContactInput) -> LpKeyContact:
    """
    Register or update an LP key contact.

    Deduplicates by (lp_id, full_name, email).
    If contact exists, updates fields if new data has higher confidence.

    Args:
        db: Database session
        contact_input: Contact input data

    Returns:
        LpKeyContact instance
    """
    from app.sources.public_lp_strategies.contact_validation import is_likely_duplicate

    # Check for existing contact
    existing = (
        db.query(LpKeyContact)
        .filter(
            LpKeyContact.lp_id == contact_input.lp_id,
            LpKeyContact.full_name == contact_input.full_name,
        )
        .first()
    )

    if existing:
        # Update if new confidence is higher or if fields are missing
        should_update = False

        confidence_order = {"high": 3, "medium": 2, "low": 1}
        new_conf = confidence_order.get(contact_input.confidence_level, 1)
        existing_conf = confidence_order.get(existing.confidence_level, 1)

        if new_conf >= existing_conf:
            # Update fields that are newly provided
            if contact_input.email and not existing.email:
                existing.email = contact_input.email
                should_update = True
            if contact_input.phone and not existing.phone:
                existing.phone = contact_input.phone
                should_update = True
            if contact_input.title and not existing.title:
                existing.title = contact_input.title
                should_update = True
            if contact_input.role_category and not existing.role_category:
                existing.role_category = contact_input.role_category
                should_update = True
            if contact_input.linkedin_url and not existing.linkedin_url:
                existing.linkedin_url = contact_input.linkedin_url
                should_update = True

            # Always update source if new confidence is higher
            if new_conf > existing_conf:
                existing.source_type = contact_input.source_type
                existing.source_url = contact_input.source_url
                existing.confidence_level = contact_input.confidence_level
                should_update = True

        if should_update:
            existing.updated_at = datetime.utcnow()
            db.commit()
            db.refresh(existing)
            logger.info(f"Updated LP contact: {existing.full_name} (ID: {existing.id})")
        else:
            logger.info(
                f"LP contact already exists (no update): {existing.full_name} (ID: {existing.id})"
            )

        return existing

    # Create new contact
    contact = LpKeyContact(
        lp_id=contact_input.lp_id,
        full_name=contact_input.full_name,
        title=contact_input.title,
        role_category=contact_input.role_category,
        email=contact_input.email,
        phone=contact_input.phone,
        linkedin_url=contact_input.linkedin_url,
        source_document_id=contact_input.source_document_id,
        source_type=contact_input.source_type,
        source_url=contact_input.source_url,
        confidence_level=contact_input.confidence_level,
        is_verified=contact_input.is_verified,
        collected_date=datetime.utcnow(),
    )

    db.add(contact)
    db.commit()
    db.refresh(contact)

    logger.info(
        f"Registered new LP contact: {contact.full_name} (ID: {contact.id}) at {contact.lp_id}"
    )

    return contact


def batch_register_lp_contacts(
    db: Session, contacts_input: List[LpKeyContactInput]
) -> ContactExtractionResult:
    """
    Batch register multiple LP contacts.

    Args:
        db: Database session
        contacts_input: List of contact inputs

    Returns:
        ContactExtractionResult with counts and errors
    """
    result = ContactExtractionResult(
        contacts_found=len(contacts_input),
        contacts_inserted=0,
        contacts_skipped=0,
        errors=[],
    )

    for contact_input in contacts_input:
        try:
            register_lp_contact(db, contact_input)
            result.contacts_inserted += 1
        except Exception as e:
            logger.error(f"Error registering contact {contact_input.full_name}: {e}")
            result.errors.append(f"{contact_input.full_name}: {str(e)}")
            result.contacts_skipped += 1

    logger.info(
        f"Batch contact registration complete: "
        f"found={result.contacts_found}, inserted={result.contacts_inserted}, "
        f"skipped={result.contacts_skipped}"
    )

    return result


def get_lp_contacts(db: Session, lp_id: int) -> List[LpKeyContact]:
    """
    Get all contacts for a specific LP.

    Args:
        db: Database session
        lp_id: LP fund ID

    Returns:
        List of LpKeyContact instances
    """
    contacts = (
        db.query(LpKeyContact)
        .filter(LpKeyContact.lp_id == lp_id)
        .order_by(LpKeyContact.role_category, LpKeyContact.full_name)
        .all()
    )

    return contacts


def get_contacts_by_role(db: Session, role_category: str) -> List[LpKeyContact]:
    """
    Get all contacts with a specific role across all LPs.

    Args:
        db: Database session
        role_category: Role to filter by (e.g., 'CIO', 'CFO')

    Returns:
        List of LpKeyContact instances
    """
    contacts = (
        db.query(LpKeyContact)
        .filter(LpKeyContact.role_category == role_category)
        .order_by(LpKeyContact.lp_id, LpKeyContact.full_name)
        .all()
    )

    return contacts
