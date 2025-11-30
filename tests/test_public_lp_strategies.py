"""
Unit tests for the public_lp_strategies source.

Tests cover:
1. Model creation and relationships
2. Idempotent upserts
3. Query functions
4. Input validation
"""
import pytest
from datetime import datetime, date
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.models import (
    Base,
    LpFund,
    LpDocument,
    LpDocumentTextSection,
    LpStrategySnapshot,
    LpAssetClassTargetAllocation,
    LpAssetClassProjection,
    LpStrategyThematicTag,
)
from app.sources.public_lp_strategies.types import (
    LpFundInput,
    LpDocumentInput,
    DocumentTextSectionInput,
    StrategySnapshotInput,
    AssetClassAllocationInput,
    AssetClassProjectionInput,
    ThematicTagInput,
)
from app.sources.public_lp_strategies.ingest import (
    register_lp_fund,
    register_lp_document,
    store_document_text_sections,
    upsert_strategy_snapshot,
    upsert_asset_class_allocations,
    upsert_asset_class_projections,
    upsert_thematic_tags,
)
from app.sources.public_lp_strategies.config import (
    PROGRAM_PRIVATE_EQUITY,
    ASSET_CLASS_PRIVATE_EQUITY,
    HORIZON_3_YEAR,
    THEME_AI,
)


@pytest.fixture(scope="function")
def test_lp_db():
    """
    Create an in-memory SQLite database for testing LP models.
    
    Fresh database for each test.
    """
    # Use in-memory SQLite
    engine = create_engine("sqlite:///:memory:")
    
    # Create all tables
    Base.metadata.create_all(bind=engine)
    
    # Create session factory
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    # Create session
    db = TestingSessionLocal()
    
    try:
        yield db
    finally:
        db.close()
        Base.metadata.drop_all(bind=engine)


# =============================================================================
# TEST MODEL CREATION
# =============================================================================


def test_create_lp_fund(test_lp_db):
    """Test creating an LP fund record."""
    fund = LpFund(
        name="TestFund",
        formal_name="Test Pension Fund",
        lp_type="public_pension",
        jurisdiction="CA",
        website_url="https://test.gov",
    )
    test_lp_db.add(fund)
    test_lp_db.commit()
    
    # Query back
    retrieved = test_lp_db.query(LpFund).filter(LpFund.name == "TestFund").first()
    assert retrieved is not None
    assert retrieved.formal_name == "Test Pension Fund"
    assert retrieved.lp_type == "public_pension"
    assert retrieved.jurisdiction == "CA"


def test_create_lp_document(test_lp_db):
    """Test creating an LP document record."""
    # Create fund first
    fund = LpFund(
        name="TestFund",
        formal_name="Test Fund",
        lp_type="public_pension",
    )
    test_lp_db.add(fund)
    test_lp_db.commit()
    
    # Create document
    document = LpDocument(
        lp_id=fund.id,
        title="Q3 2025 Investment Committee Report",
        document_type="investment_committee_presentation",
        program="private_equity",
        fiscal_year=2025,
        fiscal_quarter="Q3",
        source_url="https://test.gov/doc.pdf",
        file_format="pdf",
    )
    test_lp_db.add(document)
    test_lp_db.commit()
    
    # Query back
    retrieved = test_lp_db.query(LpDocument).filter(LpDocument.lp_id == fund.id).first()
    assert retrieved is not None
    assert retrieved.title == "Q3 2025 Investment Committee Report"
    assert retrieved.program == "private_equity"
    assert retrieved.fiscal_year == 2025
    assert retrieved.fiscal_quarter == "Q3"


def test_create_strategy_snapshot(test_lp_db):
    """Test creating a strategy snapshot record."""
    # Create fund
    fund = LpFund(name="TestFund", formal_name="Test", lp_type="public_pension")
    test_lp_db.add(fund)
    test_lp_db.commit()
    
    # Create snapshot
    snapshot = LpStrategySnapshot(
        lp_id=fund.id,
        program="private_equity",
        fiscal_year=2025,
        fiscal_quarter="Q3",
        summary_text="Test strategy",
        risk_positioning="risk_on",
    )
    test_lp_db.add(snapshot)
    test_lp_db.commit()
    
    # Query back
    retrieved = test_lp_db.query(LpStrategySnapshot).filter(
        LpStrategySnapshot.lp_id == fund.id
    ).first()
    assert retrieved is not None
    assert retrieved.program == "private_equity"
    assert retrieved.fiscal_year == 2025
    assert retrieved.fiscal_quarter == "Q3"
    assert retrieved.summary_text == "Test strategy"


def test_unique_constraint_strategy_snapshot(test_lp_db):
    """Test unique constraint on (lp_id, program, fiscal_year, fiscal_quarter)."""
    # Create fund
    fund = LpFund(name="TestFund", formal_name="Test", lp_type="public_pension")
    test_lp_db.add(fund)
    test_lp_db.commit()
    
    # Create first snapshot
    snapshot1 = LpStrategySnapshot(
        lp_id=fund.id,
        program="private_equity",
        fiscal_year=2025,
        fiscal_quarter="Q3",
    )
    test_lp_db.add(snapshot1)
    test_lp_db.commit()
    
    # Try to create duplicate - should fail
    snapshot2 = LpStrategySnapshot(
        lp_id=fund.id,
        program="private_equity",
        fiscal_year=2025,
        fiscal_quarter="Q3",
    )
    test_lp_db.add(snapshot2)
    
    with pytest.raises(Exception):  # SQLAlchemy will raise IntegrityError
        test_lp_db.commit()


# =============================================================================
# TEST INGESTION FUNCTIONS
# =============================================================================


def test_register_lp_fund_idempotent(test_lp_db):
    """Test that register_lp_fund is idempotent."""
    fund_input = LpFundInput(
        name="CalPERS",
        formal_name="California Public Employees' Retirement System",
        lp_type="public_pension",
        jurisdiction="CA",
    )
    
    # Register first time
    fund1 = register_lp_fund(test_lp_db, fund_input)
    assert fund1.id is not None
    
    # Register again - should return same fund
    fund2 = register_lp_fund(test_lp_db, fund_input)
    assert fund2.id == fund1.id
    
    # Verify only one record exists
    count = test_lp_db.query(LpFund).filter(LpFund.name == "CalPERS").count()
    assert count == 1


def test_register_lp_document(test_lp_db):
    """Test registering an LP document."""
    # Create fund first
    fund = LpFund(name="TestFund", formal_name="Test", lp_type="public_pension")
    test_lp_db.add(fund)
    test_lp_db.commit()
    
    # Register document
    doc_input = LpDocumentInput(
        lp_id=fund.id,
        title="Test Document",
        document_type="investment_committee_presentation",
        program="private_equity",
        fiscal_year=2025,
        fiscal_quarter="Q3",
        source_url="https://test.gov/doc.pdf",
        file_format="pdf",
    )
    
    document = register_lp_document(test_lp_db, doc_input)
    assert document.id is not None
    assert document.lp_id == fund.id
    assert document.title == "Test Document"


def test_store_text_sections(test_lp_db):
    """Test storing document text sections."""
    # Create fund and document
    fund = LpFund(name="TestFund", formal_name="Test", lp_type="public_pension")
    test_lp_db.add(fund)
    test_lp_db.commit()
    
    document = LpDocument(
        lp_id=fund.id,
        title="Test Doc",
        document_type="investment_committee_presentation",
        program="private_equity",
        source_url="https://test.gov/doc.pdf",
        file_format="pdf",
    )
    test_lp_db.add(document)
    test_lp_db.commit()
    
    # Store sections
    sections_input = [
        DocumentTextSectionInput(
            section_name="Executive Summary",
            sequence_order=1,
            text="This is the executive summary.",
        ),
        DocumentTextSectionInput(
            section_name="Allocations",
            sequence_order=2,
            text="Private equity: 25%",
        ),
    ]
    
    sections = store_document_text_sections(test_lp_db, document.id, sections_input)
    assert len(sections) == 2
    assert sections[0].section_name == "Executive Summary"
    assert sections[1].section_name == "Allocations"


def test_upsert_strategy_snapshot_create(test_lp_db):
    """Test upserting a strategy snapshot (create case)."""
    # Create fund
    fund = LpFund(name="TestFund", formal_name="Test", lp_type="public_pension")
    test_lp_db.add(fund)
    test_lp_db.commit()
    
    # Upsert (create)
    strategy_input = StrategySnapshotInput(
        lp_id=fund.id,
        program="private_equity",
        fiscal_year=2025,
        fiscal_quarter="Q3",
        summary_text="New strategy",
        risk_positioning="risk_on",
    )
    
    snapshot = upsert_strategy_snapshot(test_lp_db, strategy_input)
    assert snapshot.id is not None
    assert snapshot.summary_text == "New strategy"


def test_upsert_strategy_snapshot_update(test_lp_db):
    """Test upserting a strategy snapshot (update case)."""
    # Create fund
    fund = LpFund(name="TestFund", formal_name="Test", lp_type="public_pension")
    test_lp_db.add(fund)
    test_lp_db.commit()
    
    # Create initial snapshot
    strategy_input = StrategySnapshotInput(
        lp_id=fund.id,
        program="private_equity",
        fiscal_year=2025,
        fiscal_quarter="Q3",
        summary_text="Original strategy",
        risk_positioning="neutral",
    )
    snapshot1 = upsert_strategy_snapshot(test_lp_db, strategy_input)
    original_id = snapshot1.id
    
    # Upsert with updated data
    strategy_input.summary_text = "Updated strategy"
    strategy_input.risk_positioning = "risk_on"
    snapshot2 = upsert_strategy_snapshot(test_lp_db, strategy_input)
    
    # Should update existing record, not create new one
    assert snapshot2.id == original_id
    assert snapshot2.summary_text == "Updated strategy"
    assert snapshot2.risk_positioning == "risk_on"
    
    # Verify only one record exists
    count = test_lp_db.query(LpStrategySnapshot).filter(
        LpStrategySnapshot.lp_id == fund.id
    ).count()
    assert count == 1


def test_upsert_asset_class_allocations(test_lp_db):
    """Test upserting asset class allocations."""
    # Create fund and snapshot
    fund = LpFund(name="TestFund", formal_name="Test", lp_type="public_pension")
    test_lp_db.add(fund)
    test_lp_db.commit()
    
    snapshot = LpStrategySnapshot(
        lp_id=fund.id,
        program="private_equity",
        fiscal_year=2025,
        fiscal_quarter="Q3",
    )
    test_lp_db.add(snapshot)
    test_lp_db.commit()
    
    # Upsert allocations
    allocations = [
        AssetClassAllocationInput(
            asset_class="private_equity",
            target_weight_pct=25.0,
            current_weight_pct=27.5,
        ),
        AssetClassAllocationInput(
            asset_class="real_estate",
            target_weight_pct=15.0,
            current_weight_pct=14.0,
        ),
    ]
    
    results = upsert_asset_class_allocations(test_lp_db, snapshot.id, allocations)
    assert len(results) == 2
    assert results[0].asset_class == "private_equity"
    assert results[0].target_weight_pct == "25.0"
    assert results[1].asset_class == "real_estate"


def test_upsert_asset_class_allocations_idempotent(test_lp_db):
    """Test that allocation upserts are idempotent."""
    # Create fund and snapshot
    fund = LpFund(name="TestFund", formal_name="Test", lp_type="public_pension")
    test_lp_db.add(fund)
    test_lp_db.commit()
    
    snapshot = LpStrategySnapshot(
        lp_id=fund.id,
        program="private_equity",
        fiscal_year=2025,
        fiscal_quarter="Q3",
    )
    test_lp_db.add(snapshot)
    test_lp_db.commit()
    
    # First upsert
    allocations1 = [
        AssetClassAllocationInput(
            asset_class="private_equity",
            target_weight_pct=25.0,
            current_weight_pct=27.5,
        ),
    ]
    results1 = upsert_asset_class_allocations(test_lp_db, snapshot.id, allocations1)
    id1 = results1[0].id
    
    # Second upsert with updated values
    allocations2 = [
        AssetClassAllocationInput(
            asset_class="private_equity",
            target_weight_pct=25.0,
            current_weight_pct=28.0,  # Updated
        ),
    ]
    results2 = upsert_asset_class_allocations(test_lp_db, snapshot.id, allocations2)
    id2 = results2[0].id
    
    # Should update same record
    assert id2 == id1
    assert results2[0].current_weight_pct == "28.0"
    
    # Verify only one record exists
    count = test_lp_db.query(LpAssetClassTargetAllocation).filter(
        LpAssetClassTargetAllocation.strategy_id == snapshot.id
    ).count()
    assert count == 1


def test_upsert_thematic_tags(test_lp_db):
    """Test upserting thematic tags."""
    # Create fund and snapshot
    fund = LpFund(name="TestFund", formal_name="Test", lp_type="public_pension")
    test_lp_db.add(fund)
    test_lp_db.commit()
    
    snapshot = LpStrategySnapshot(
        lp_id=fund.id,
        program="private_equity",
        fiscal_year=2025,
        fiscal_quarter="Q3",
    )
    test_lp_db.add(snapshot)
    test_lp_db.commit()
    
    # Upsert tags
    tags = [
        ThematicTagInput(theme="ai", relevance_score=0.8),
        ThematicTagInput(theme="energy_transition", relevance_score=0.6),
    ]
    
    results = upsert_thematic_tags(test_lp_db, snapshot.id, tags)
    assert len(results) == 2
    assert results[0].theme == "ai"
    assert results[0].relevance_score == "0.8"


# =============================================================================
# TEST INPUT VALIDATION
# =============================================================================


def test_lp_fund_input_validation():
    """Test LpFundInput validation."""
    # Valid input
    valid = LpFundInput(
        name="TestFund",
        lp_type="public_pension",
    )
    assert valid.name == "TestFund"
    
    # Invalid lp_type
    with pytest.raises(ValueError, match="Invalid lp_type"):
        LpFundInput(
            name="TestFund",
            lp_type="invalid_type",
        )


def test_document_input_validation():
    """Test LpDocumentInput validation."""
    # Valid input
    valid = LpDocumentInput(
        lp_id=1,
        title="Test",
        document_type="investment_committee_presentation",
        program="private_equity",
        source_url="https://test.gov",
        file_format="pdf",
    )
    assert valid.program == "private_equity"
    
    # Invalid program
    with pytest.raises(ValueError, match="Invalid program"):
        LpDocumentInput(
            lp_id=1,
            title="Test",
            document_type="investment_committee_presentation",
            program="invalid_program",
            source_url="https://test.gov",
            file_format="pdf",
        )
    
    # Invalid fiscal_quarter
    with pytest.raises(ValueError, match="Invalid fiscal_quarter"):
        LpDocumentInput(
            lp_id=1,
            title="Test",
            document_type="investment_committee_presentation",
            program="private_equity",
            fiscal_quarter="Q5",  # Invalid
            source_url="https://test.gov",
            file_format="pdf",
        )


def test_asset_class_allocation_input_validation():
    """Test AssetClassAllocationInput validation."""
    # Valid input
    valid = AssetClassAllocationInput(
        asset_class="private_equity",
        target_weight_pct=25.0,
    )
    assert valid.asset_class == "private_equity"
    
    # Invalid asset_class
    with pytest.raises(ValueError, match="Invalid asset_class"):
        AssetClassAllocationInput(
            asset_class="invalid_class",
            target_weight_pct=25.0,
        )


# =============================================================================
# TEST RELATIONSHIPS
# =============================================================================


def test_strategy_with_full_relationships(test_lp_db):
    """Test creating a complete strategy with all related data."""
    # Create fund
    fund = LpFund(name="TestFund", formal_name="Test", lp_type="public_pension")
    test_lp_db.add(fund)
    test_lp_db.commit()
    
    # Create document
    document = LpDocument(
        lp_id=fund.id,
        title="Test Doc",
        document_type="investment_committee_presentation",
        program="private_equity",
        source_url="https://test.gov",
        file_format="pdf",
    )
    test_lp_db.add(document)
    test_lp_db.commit()
    
    # Create strategy
    strategy = LpStrategySnapshot(
        lp_id=fund.id,
        program="private_equity",
        fiscal_year=2025,
        fiscal_quarter="Q3",
        primary_document_id=document.id,
    )
    test_lp_db.add(strategy)
    test_lp_db.commit()
    
    # Add allocation
    allocation = LpAssetClassTargetAllocation(
        strategy_id=strategy.id,
        asset_class="private_equity",
        target_weight_pct="25.0",
        current_weight_pct="27.5",
    )
    test_lp_db.add(allocation)
    test_lp_db.commit()
    
    # Add projection
    projection = LpAssetClassProjection(
        strategy_id=strategy.id,
        asset_class="private_equity",
        projection_horizon="3_year",
        commitment_plan_amount="5000000000",
    )
    test_lp_db.add(projection)
    test_lp_db.commit()
    
    # Add tag
    tag = LpStrategyThematicTag(
        strategy_id=strategy.id,
        theme="ai",
        relevance_score="0.8",
    )
    test_lp_db.add(tag)
    test_lp_db.commit()
    
    # Query and verify relationships
    retrieved_strategy = test_lp_db.query(LpStrategySnapshot).filter(
        LpStrategySnapshot.id == strategy.id
    ).first()
    
    assert retrieved_strategy is not None
    
    # Verify related allocations
    allocations = test_lp_db.query(LpAssetClassTargetAllocation).filter(
        LpAssetClassTargetAllocation.strategy_id == strategy.id
    ).all()
    assert len(allocations) == 1
    assert allocations[0].asset_class == "private_equity"
    
    # Verify related projections
    projections = test_lp_db.query(LpAssetClassProjection).filter(
        LpAssetClassProjection.strategy_id == strategy.id
    ).all()
    assert len(projections) == 1
    assert projections[0].commitment_plan_amount == "5000000000"
    
    # Verify related tags
    tags = test_lp_db.query(LpStrategyThematicTag).filter(
        LpStrategyThematicTag.strategy_id == strategy.id
    ).all()
    assert len(tags) == 1
    assert tags[0].theme == "ai"


