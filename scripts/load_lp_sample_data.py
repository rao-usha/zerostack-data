"""
Load Sample LP Strategy Data - Complete Demo Dataset

This script loads sample Q3 2025 private equity strategy data for 27 institutional investors:
- 15 U.S. Public Pension Funds
- 3 University Endowments
- 5 Sovereign Wealth Funds
- 4 Canadian Pensions
- 1 European Pension (Dutch ABP)
- 2 Australian Funds

Usage:
    python load_lp_sample_data.py

Requirements:
    - PostgreSQL running (localhost:5433)
    - DATABASE_URL environment variable set
    - FastAPI app running or database schema initialized
"""
import os
from datetime import date

# Ensure environment variables are set
if 'DATABASE_URL' not in os.environ:
    os.environ['DATABASE_URL'] = 'postgresql://nexdata:nexdata_dev_password@localhost:5433/nexdata'

from app.core.database import get_db
from app.sources.public_lp_strategies.config import KNOWN_LP_FUNDS
from app.sources.public_lp_strategies.ingest import (
    register_lp_fund,
    register_lp_document,
    store_document_text_sections,
    upsert_strategy_snapshot,
    upsert_asset_class_allocations,
    upsert_asset_class_projections,
    upsert_thematic_tags,
)
from app.sources.public_lp_strategies.types import (
    LpDocumentInput,
    DocumentTextSectionInput,
    StrategySnapshotInput,
    AssetClassAllocationInput,
    AssetClassProjectionInput,
    ThematicTagInput,
)

# Sample data for all 22 LPs
SAMPLE_DATA = {
    "CalPERS": {
        "pe_target": 26.0, "pe_current": 27.5,
        "pub_eq_target": 40.0, "pub_eq_current": 38.5,
        "fi_target": 18.0, "fi_current": 17.5,
        "re_target": 10.0, "re_current": 11.0,
        "commit_3y": 15000000000.0,
        "themes": [("ai", 0.85), ("technology", 0.80), ("healthcare", 0.75)],
        "risk": "risk_on"
    },
    "Florida SBA": {
        "pe_target": 12.0, "pe_current": 13.5,
        "pub_eq_target": 45.0, "pub_eq_current": 43.0,
        "fi_target": 25.0, "fi_current": 26.0,
        "re_target": 10.0, "re_current": 11.5,
        "commit_3y": 8000000000.0,
        "themes": [("ai", 0.72), ("technology", 0.70), ("healthcare", 0.68)],
        "risk": "neutral"
    },
    "WSIB": {
        "pe_target": 20.0, "pe_current": 21.5,
        "pub_eq_target": 38.0, "pub_eq_current": 36.5,
        "fi_target": 20.0, "fi_current": 21.0,
        "re_target": 12.0, "re_current": 13.0,
        "commit_3y": 10000000000.0,
        "themes": [("ai", 0.80), ("technology", 0.78), ("energy_transition", 0.72)],
        "risk": "risk_on"
    },
    "STRS Ohio": {
        "pe_target": 17.5, "pe_current": 19.0,
        "pub_eq_target": 42.0, "pub_eq_current": 40.0,
        "fi_target": 22.0, "fi_current": 23.0,
        "re_target": 10.0, "re_current": 11.5,
        "commit_3y": 12000000000.0,
        "themes": [("ai", 0.76), ("technology", 0.74), ("healthcare", 0.70)],
        "risk": "neutral"
    },
    "Oregon PERS": {
        "pe_target": 17.0, "pe_current": 18.2,
        "pub_eq_target": 40.0, "pub_eq_current": 38.8,
        "fi_target": 20.0, "fi_current": 21.0,
        "re_target": 12.0, "re_current": 13.0,
        "commit_3y": 6000000000.0,
        "themes": [("ai", 0.74), ("energy_transition", 0.78), ("climate_resilience", 0.75)],
        "risk": "neutral"
    },
    "Massachusetts PRIM": {
        "pe_target": 15.5, "pe_current": 16.8,
        "pub_eq_target": 42.0, "pub_eq_current": 40.5,
        "fi_target": 23.0, "fi_current": 24.0,
        "re_target": 10.0, "re_current": 11.2,
        "commit_3y": 7000000000.0,
        "themes": [("ai", 0.73), ("technology", 0.72), ("healthcare", 0.75)],
        "risk": "neutral"
    },
    "Illinois TRS": {
        "pe_target": 16.0, "pe_current": 17.2,
        "pub_eq_target": 41.0, "pub_eq_current": 39.5,
        "fi_target": 24.0, "fi_current": 25.0,
        "re_target": 10.0, "re_current": 11.0,
        "commit_3y": 9000000000.0,
        "themes": [("ai", 0.75), ("technology", 0.73), ("reshoring", 0.68)],
        "risk": "neutral"
    },
    "Pennsylvania PSERS": {
        "pe_target": 15.0, "pe_current": 15.8,
        "pub_eq_target": 43.0, "pub_eq_current": 42.0,
        "fi_target": 23.0, "fi_current": 24.0,
        "re_target": 11.0, "re_current": 12.0,
        "commit_3y": 11000000000.0,
        "themes": [("ai", 0.74), ("technology", 0.72), ("energy_transition", 0.70)],
        "risk": "neutral"
    },
    "New Jersey DI": {
        "pe_target": 13.5, "pe_current": 14.5,
        "pub_eq_target": 44.0, "pub_eq_current": 43.0,
        "fi_target": 25.0, "fi_current": 25.5,
        "re_target": 10.0, "re_current": 10.5,
        "commit_3y": 8500000000.0,
        "themes": [("ai", 0.73), ("technology", 0.71), ("healthcare", 0.69)],
        "risk": "neutral"
    },
    "Ohio OPERS": {
        "pe_target": 15.5, "pe_current": 16.3,
        "pub_eq_target": 42.0, "pub_eq_current": 41.0,
        "fi_target": 23.0, "fi_current": 24.0,
        "re_target": 11.0, "re_current": 12.0,
        "commit_3y": 10500000000.0,
        "themes": [("ai", 0.74), ("technology", 0.72), ("healthcare", 0.70)],
        "risk": "neutral"
    },
    "North Carolina RS": {
        "pe_target": 15.0, "pe_current": 15.9,
        "pub_eq_target": 43.0, "pub_eq_current": 42.0,
        "fi_target": 24.0, "fi_current": 24.5,
        "re_target": 10.0, "re_current": 11.0,
        "commit_3y": 9500000000.0,
        "themes": [("ai", 0.73), ("technology", 0.71), ("healthcare", 0.69)],
        "risk": "neutral"
    },
    "Harvard": {
        "pe_target": 35.0, "pe_current": 36.5,
        "pub_eq_target": 28.0, "pub_eq_current": 26.5,
        "fi_target": 12.0, "fi_current": 11.5,
        "re_target": 8.0, "re_current": 9.0,
        "commit_3y": 18000000000.0,
        "themes": [("ai", 0.92), ("technology", 0.88), ("climate_resilience", 0.85)],
        "risk": "risk_on"
    },
    "Yale": {
        "pe_target": 39.0, "pe_current": 41.2,
        "pub_eq_target": 25.0, "pub_eq_current": 23.5,
        "fi_target": 10.0, "fi_current": 9.8,
        "re_target": 7.0, "re_current": 8.5,
        "commit_3y": 20000000000.0,
        "themes": [("ai", 0.95), ("technology", 0.90), ("climate_resilience", 0.88)],
        "risk": "risk_on"
    },
    "Stanford": {
        "pe_target": 37.0, "pe_current": 38.8,
        "pub_eq_target": 26.0, "pub_eq_current": 24.5,
        "fi_target": 11.0, "fi_current": 10.5,
        "re_target": 7.5, "re_current": 8.8,
        "commit_3y": 16000000000.0,
        "themes": [("ai", 0.94), ("technology", 0.92), ("sustainability", 0.86)],
        "risk": "risk_on"
    },
    "Norway GPFG": {
        "pe_target": 7.0, "pe_current": 8.2,
        "pub_eq_target": 70.0, "pub_eq_current": 68.5,
        "fi_target": 20.0, "fi_current": 21.0,
        "re_target": 3.0, "re_current": 3.5,
        "commit_3y": 25000000000.0,
        "themes": [("sustainability", 0.95), ("climate_resilience", 0.92), ("ai", 0.75)],
        "risk": "neutral"
    },
    "GIC Singapore": {
        "pe_target": 23.0, "pe_current": 24.5,
        "pub_eq_target": 35.0, "pub_eq_current": 33.5,
        "fi_target": 22.0, "fi_current": 23.0,
        "re_target": 12.0, "re_current": 13.0,
        "commit_3y": 35000000000.0,
        "themes": [("ai", 0.88), ("technology", 0.85), ("infrastructure", 0.82)],
        "risk": "neutral"
    },
    "ADIA": {
        "pe_target": 16.0, "pe_current": 17.2,
        "pub_eq_target": 38.0, "pub_eq_current": 37.0,
        "fi_target": 25.0, "fi_current": 26.0,
        "re_target": 12.0, "re_current": 13.0,
        "commit_3y": 30000000000.0,
        "themes": [("ai", 0.82), ("technology", 0.80), ("infrastructure", 0.85)],
        "risk": "neutral"
    },
    "CPP Investments": {
        "pe_target": 28.0, "pe_current": 30.5,
        "pub_eq_target": 30.0, "pub_eq_current": 28.5,
        "fi_target": 15.0, "fi_current": 14.0,
        "re_target": 12.0, "re_current": 13.5,
        "commit_3y": 40000000000.0,
        "themes": [("ai", 0.90), ("infrastructure", 0.88), ("technology", 0.85)],
        "risk": "risk_on"
    },
    "Ontario Teachers": {
        "pe_target": 26.0, "pe_current": 28.2,
        "pub_eq_target": 32.0, "pub_eq_current": 30.0,
        "fi_target": 18.0, "fi_current": 17.5,
        "re_target": 10.0, "re_current": 11.5,
        "commit_3y": 32000000000.0,
        "themes": [("ai", 0.87), ("infrastructure", 0.90), ("technology", 0.82)],
        "risk": "risk_on"
    },
    "Wisconsin SWIB": {
        "pe_target": 19.0, "pe_current": 20.8,
        "pub_eq_target": 42.0, "pub_eq_current": 40.5,
        "fi_target": 22.0, "fi_current": 22.5,
        "re_target": 9.0, "re_current": 10.0,
        "commit_3y": 13000000000.0,
        "themes": [("ai", 0.78), ("technology", 0.75), ("healthcare", 0.72)],
        "risk": "neutral"
    },
    "Virginia RS": {
        "pe_target": 16.5, "pe_current": 17.8,
        "pub_eq_target": 40.0, "pub_eq_current": 38.5,
        "fi_target": 24.0, "fi_current": 25.0,
        "re_target": 11.0, "re_current": 12.0,
        "commit_3y": 11000000000.0,
        "themes": [("ai", 0.75), ("technology", 0.73), ("energy_transition", 0.68)],
        "risk": "neutral"
    },
    "NZ Super Fund": {
        "pe_target": 24.0, "pe_current": 26.5,
        "pub_eq_target": 35.0, "pub_eq_current": 33.0,
        "fi_target": 15.0, "fi_current": 14.5,
        "re_target": 8.0, "re_current": 9.5,
        "commit_3y": 8000000000.0,
        "themes": [("climate_resilience", 0.92), ("sustainability", 0.90), ("ai", 0.80)],
        "risk": "risk_on"
    },
    "OMERS": {
        "pe_target": 22.0, "pe_current": 23.8,
        "pub_eq_target": 34.0, "pub_eq_current": 32.5,
        "fi_target": 18.0, "fi_current": 17.5,
        "re_target": 14.0, "re_current": 15.5,
        "commit_3y": 20000000000.0,
        "themes": [("infrastructure", 0.92), ("ai", 0.84), ("technology", 0.80)],
        "risk": "risk_on"
    },
    "CDPQ": {
        "pe_target": 25.0, "pe_current": 27.2,
        "pub_eq_target": 32.0, "pub_eq_current": 30.0,
        "fi_target": 16.0, "fi_current": 15.5,
        "re_target": 13.0, "re_current": 14.8,
        "commit_3y": 38000000000.0,
        "themes": [("infrastructure", 0.94), ("ai", 0.88), ("climate_resilience", 0.85)],
        "risk": "risk_on"
    },
    "Dutch ABP": {
        "pe_target": 14.0, "pe_current": 15.5,
        "pub_eq_target": 46.0, "pub_eq_current": 44.5,
        "fi_target": 26.0, "fi_current": 27.0,
        "re_target": 8.0, "re_current": 9.0,
        "commit_3y": 28000000000.0,
        "themes": [("sustainability", 0.96), ("climate_resilience", 0.94), ("ai", 0.76)],
        "risk": "neutral"
    },
    "AustralianSuper": {
        "pe_target": 20.0, "pe_current": 21.8,
        "pub_eq_target": 38.0, "pub_eq_current": 36.5,
        "fi_target": 20.0, "fi_current": 21.0,
        "re_target": 12.0, "re_current": 13.5,
        "commit_3y": 24000000000.0,
        "themes": [("infrastructure", 0.88), ("ai", 0.82), ("climate_resilience", 0.80)],
        "risk": "risk_on"
    },
    "Future Fund": {
        "pe_target": 18.0, "pe_current": 19.5,
        "pub_eq_target": 40.0, "pub_eq_current": 38.0,
        "fi_target": 20.0, "fi_current": 21.5,
        "re_target": 10.0, "re_current": 11.5,
        "commit_3y": 18000000000.0,
        "themes": [("climate_resilience", 0.90), ("infrastructure", 0.86), ("ai", 0.81)],
        "risk": "neutral"
    },
}


def load_sample_data():
    """Load all sample data into the database."""
    db = next(get_db())
    
    print("=" * 80)
    print("LOADING LP SAMPLE DATA - 22 Institutional Investors")
    print("=" * 80)
    print()
    
    loaded_count = 0
    
    for fund_name, data in SAMPLE_DATA.items():
        if fund_name not in KNOWN_LP_FUNDS:
            print(f"Warning: {fund_name} not in KNOWN_LP_FUNDS, skipping")
            continue
            
        print(f"Loading {fund_name}...")
        
        # Register fund
        fund = register_lp_fund(db, KNOWN_LP_FUNDS[fund_name])
        
        # Register document
        doc_input = LpDocumentInput(
            lp_id=fund.id,
            title=f"FY2025 Q3 Investment Report",
            document_type="quarterly_investment_report",
            program="private_equity",
            fiscal_year=2025,
            fiscal_quarter="Q3",
            report_period_start=date(2025, 7, 1),
            report_period_end=date(2025, 9, 30),
            source_url=f"{KNOWN_LP_FUNDS[fund_name]['website_url']}/reports/q3-2025.pdf",
            file_format="pdf",
        )
        document = register_lp_document(db, doc_input)
        
        # Store text sections
        sections_input = [
            DocumentTextSectionInput(
                section_name="Investment Strategy Overview",
                sequence_order=1,
                page_start=1,
                page_end=3,
                text=f"Q3 2025 private equity strategy. Allocation at {data['pe_current']}%.",
            ),
        ]
        store_document_text_sections(db, document.id, sections_input)
        
        # Create strategy snapshot
        strategy_input = StrategySnapshotInput(
            lp_id=fund.id,
            program="private_equity",
            fiscal_year=2025,
            fiscal_quarter="Q3",
            strategy_date=date(2025, 10, 15),
            primary_document_id=document.id,
            summary_text=f"Q3 2025 strategy for {fund_name}",
            risk_positioning=data['risk'],
        )
        strategy = upsert_strategy_snapshot(db, strategy_input)
        
        # Add allocations
        allocations_input = [
            AssetClassAllocationInput(
                asset_class="private_equity",
                target_weight_pct=data['pe_target'],
                current_weight_pct=data['pe_current'],
            ),
            AssetClassAllocationInput(
                asset_class="public_equity",
                target_weight_pct=data['pub_eq_target'],
                current_weight_pct=data['pub_eq_current'],
            ),
            AssetClassAllocationInput(
                asset_class="fixed_income",
                target_weight_pct=data['fi_target'],
                current_weight_pct=data['fi_current'],
            ),
            AssetClassAllocationInput(
                asset_class="real_estate",
                target_weight_pct=data['re_target'],
                current_weight_pct=data['re_current'],
            ),
        ]
        upsert_asset_class_allocations(db, strategy.id, allocations_input)
        
        # Add projections
        projections_input = [
            AssetClassProjectionInput(
                asset_class="private_equity",
                projection_horizon="3_year",
                commitment_plan_amount=data['commit_3y'],
                expected_return_pct=12.0,
            ),
        ]
        upsert_asset_class_projections(db, strategy.id, projections_input)
        
        # Add thematic tags
        tags_input = [
            ThematicTagInput(theme=theme, relevance_score=score)
            for theme, score in data['themes']
        ]
        upsert_thematic_tags(db, strategy.id, tags_input)
        
        loaded_count += 1
        print(f"   [OK] {fund_name} loaded")
    
    print()
    print("=" * 80)
    print(f"COMPLETE! Loaded {loaded_count}/27 institutional investors")
    print("Spanning 4 continents: North America, Europe, Asia-Pacific, Middle East")
    print("=" * 80)


if __name__ == "__main__":
    load_sample_data()

