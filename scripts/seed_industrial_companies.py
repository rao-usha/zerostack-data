#!/usr/bin/env python3
"""
Seed industrial companies from JSON file into the database.

Usage:
    python scripts/seed_industrial_companies.py

This script:
1. Loads company data from data/seeds/industrial_companies.json
2. Creates IndustrialCompany records for each company
3. Handles duplicates gracefully (skips or updates)
4. Reports summary statistics
"""

import json
import sys
from pathlib import Path
from datetime import date

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from app.core.database import get_engine, create_tables, get_session_factory
from app.core.people_models import IndustrialCompany


def load_seed_data(filepath: Path) -> dict:
    """Load seed data from JSON file."""
    with open(filepath, 'r', encoding='utf-8') as f:
        return json.load(f)


def seed_company(session: Session, company_data: dict, parent_map: dict) -> tuple[bool, str]:
    """
    Seed a single company into the database.

    Returns:
        (created: bool, message: str)
    """
    # Check if company already exists
    existing = session.query(IndustrialCompany).filter(
        IndustrialCompany.name == company_data['name']
    ).first()

    if existing:
        return False, f"Skipped (exists): {company_data['name']}"

    # Handle parent company reference
    parent_id = None
    if 'parent_company' in company_data:
        parent_name = company_data.pop('parent_company')
        parent_id = parent_map.get(parent_name)

    # Create company record
    company = IndustrialCompany(
        name=company_data.get('name'),
        legal_name=company_data.get('legal_name'),
        dba_names=company_data.get('dba_names'),
        website=company_data.get('website'),
        headquarters_city=company_data.get('headquarters_city'),
        headquarters_state=company_data.get('headquarters_state'),
        headquarters_country=company_data.get('headquarters_country', 'USA'),
        industry_segment=company_data.get('industry_segment'),
        sub_segment=company_data.get('sub_segment'),
        naics_code=company_data.get('naics_code'),
        sic_code=company_data.get('sic_code'),
        employee_count=company_data.get('employee_count'),
        employee_count_range=company_data.get('employee_count_range'),
        revenue_usd=company_data.get('revenue_usd'),
        revenue_range=company_data.get('revenue_range'),
        ownership_type=company_data.get('ownership_type'),
        ticker=company_data.get('ticker'),
        stock_exchange=company_data.get('stock_exchange'),
        cik=company_data.get('cik'),
        pe_sponsor=company_data.get('pe_sponsor'),
        pe_acquisition_date=company_data.get('pe_acquisition_date'),
        parent_company_id=parent_id,
        is_subsidiary=parent_id is not None,
        status=company_data.get('status', 'active'),
        founded_year=company_data.get('founded_year'),
        data_sources=['seed_file'],
        created_at=None,  # Let DB set default
    )

    session.add(company)
    return True, f"Created: {company_data['name']}"


def seed_all_companies(session: Session, companies: list) -> dict:
    """
    Seed all companies, handling parent references in two passes.

    Returns:
        Statistics dict
    """
    stats = {
        'total': len(companies),
        'created': 0,
        'skipped': 0,
        'errors': 0,
        'error_messages': []
    }

    # First pass: Create all companies without parent references
    # Build a map of name -> id for parent lookups
    parent_map = {}

    # Sort companies: non-subsidiaries first, then subsidiaries
    non_subs = [c for c in companies if 'parent_company' not in c]
    subs = [c for c in companies if 'parent_company' in c]

    print(f"\nPass 1: Creating {len(non_subs)} non-subsidiary companies...")
    for company_data in non_subs:
        try:
            created, message = seed_company(session, company_data.copy(), parent_map)
            if created:
                stats['created'] += 1
                # Commit to get ID and add to parent map
                session.commit()
                # Get the ID
                company = session.query(IndustrialCompany).filter(
                    IndustrialCompany.name == company_data['name']
                ).first()
                if company:
                    parent_map[company.name] = company.id
            else:
                stats['skipped'] += 1
                # Still add to parent map if it exists
                company = session.query(IndustrialCompany).filter(
                    IndustrialCompany.name == company_data['name']
                ).first()
                if company:
                    parent_map[company.name] = company.id
        except IntegrityError as e:
            session.rollback()
            stats['errors'] += 1
            stats['error_messages'].append(f"{company_data['name']}: {str(e)[:100]}")
        except Exception as e:
            session.rollback()
            stats['errors'] += 1
            stats['error_messages'].append(f"{company_data['name']}: {str(e)[:100]}")

    print(f"Pass 2: Creating {len(subs)} subsidiary companies...")
    for company_data in subs:
        try:
            created, message = seed_company(session, company_data.copy(), parent_map)
            if created:
                stats['created'] += 1
                session.commit()
            else:
                stats['skipped'] += 1
        except IntegrityError as e:
            session.rollback()
            stats['errors'] += 1
            stats['error_messages'].append(f"{company_data['name']}: {str(e)[:100]}")
        except Exception as e:
            session.rollback()
            stats['errors'] += 1
            stats['error_messages'].append(f"{company_data['name']}: {str(e)[:100]}")

    return stats


def print_summary(stats: dict):
    """Print seeding summary."""
    print("\n" + "=" * 60)
    print("SEEDING SUMMARY")
    print("=" * 60)
    print(f"Total companies in seed file: {stats['total']}")
    print(f"Created: {stats['created']}")
    print(f"Skipped (already exist): {stats['skipped']}")
    print(f"Errors: {stats['errors']}")

    if stats['error_messages']:
        print("\nErrors:")
        for msg in stats['error_messages'][:10]:  # Show first 10 errors
            print(f"  - {msg}")
        if len(stats['error_messages']) > 10:
            print(f"  ... and {len(stats['error_messages']) - 10} more")

    print("=" * 60)


def verify_data(session: Session):
    """Verify seeded data."""
    print("\nVERIFYING DATA...")

    total = session.query(IndustrialCompany).count()
    print(f"Total companies in database: {total}")

    by_ownership = session.query(
        IndustrialCompany.ownership_type,
    ).distinct().all()
    print(f"\nOwnership types: {[o[0] for o in by_ownership]}")

    public = session.query(IndustrialCompany).filter(
        IndustrialCompany.ownership_type == 'public'
    ).count()
    print(f"Public companies: {public}")

    pe_backed = session.query(IndustrialCompany).filter(
        IndustrialCompany.ownership_type == 'pe_backed'
    ).count()
    print(f"PE-backed companies: {pe_backed}")

    with_cik = session.query(IndustrialCompany).filter(
        IndustrialCompany.cik.isnot(None)
    ).count()
    print(f"Companies with SEC CIK: {with_cik}")

    # Sample companies
    print("\nSample companies:")
    samples = session.query(IndustrialCompany).limit(5).all()
    for c in samples:
        print(f"  - {c.name} ({c.ownership_type}) - {c.industry_segment}/{c.sub_segment}")


def main():
    """Main entry point."""
    print("=" * 60)
    print("INDUSTRIAL COMPANIES SEEDER")
    print("=" * 60)

    # Load seed data
    seed_file = project_root / 'data' / 'seeds' / 'industrial_companies.json'
    print(f"\nLoading seed data from: {seed_file}")

    if not seed_file.exists():
        print(f"ERROR: Seed file not found: {seed_file}")
        sys.exit(1)

    data = load_seed_data(seed_file)
    companies = data.get('companies', [])
    print(f"Loaded {len(companies)} companies from seed file")
    print(f"Metadata: {data.get('metadata', {})}")

    # Create tables if needed
    print("\nEnsuring database tables exist...")
    engine = get_engine()
    create_tables(engine)

    # Seed companies
    print("\nSeeding companies...")
    SessionLocal = get_session_factory()
    session = SessionLocal()

    try:
        stats = seed_all_companies(session, companies)
        print_summary(stats)
        verify_data(session)
    finally:
        session.close()

    print("\nDone!")


if __name__ == '__main__':
    main()
