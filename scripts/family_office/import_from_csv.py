"""
Import family office data from CSV file.

Usage:
    python import_from_csv.py [csv_file]
    
Default CSV: data/family_offices_template.csv
"""
import sys
import csv
from datetime import date
from sqlalchemy import create_engine, text
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from app.core.config import get_settings


def parse_array_field(value):
    """Parse pipe-separated values into PostgreSQL array."""
    if not value or value.strip() == "":
        return None
    # Split by pipe and clean
    items = [item.strip() for item in value.split("|") if item.strip()]
    if not items:
        return None
    # Format as PostgreSQL array
    return "{" + ",".join(f'"{item}"' for item in items) + "}"


def import_family_offices(csv_file):
    """Import family offices from CSV file."""
    settings = get_settings()
    engine = create_engine(settings.database_url)
    
    imported = 0
    errors = []
    
    print(f"📂 Reading CSV file: {csv_file}")
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        for i, row in enumerate(reader, 1):
            try:
                # Parse array fields
                investment_focus = parse_array_field(row.get('investment_focus'))
                sectors_of_interest = parse_array_field(row.get('sectors_of_interest'))
                
                # Build INSERT query with ON CONFLICT UPDATE
                sql = text("""
                    INSERT INTO family_offices (
                        name, legal_name, region, country, principal_family,
                        headquarters_address, city, state_province, postal_code,
                        main_phone, main_email, website, linkedin,
                        estimated_wealth, investment_focus, sectors_of_interest,
                        check_size_range, investment_thesis, status, notes,
                        first_researched_date, last_updated_date
                    ) VALUES (
                        :name, :legal_name, :region, :country, :principal_family,
                        :headquarters_address, :city, :state_province, :postal_code,
                        :main_phone, :main_email, :website, :linkedin,
                        :estimated_wealth, :investment_focus::text[], :sectors_of_interest::text[],
                        :check_size_range, :investment_thesis, :status, :notes,
                        :first_researched_date, :last_updated_date
                    )
                    ON CONFLICT (name) DO UPDATE SET
                        legal_name = EXCLUDED.legal_name,
                        region = EXCLUDED.region,
                        country = EXCLUDED.country,
                        principal_family = EXCLUDED.principal_family,
                        headquarters_address = EXCLUDED.headquarters_address,
                        city = EXCLUDED.city,
                        state_province = EXCLUDED.state_province,
                        postal_code = EXCLUDED.postal_code,
                        main_phone = EXCLUDED.main_phone,
                        main_email = EXCLUDED.main_email,
                        website = EXCLUDED.website,
                        linkedin = EXCLUDED.linkedin,
                        estimated_wealth = EXCLUDED.estimated_wealth,
                        investment_focus = EXCLUDED.investment_focus,
                        sectors_of_interest = EXCLUDED.sectors_of_interest,
                        check_size_range = EXCLUDED.check_size_range,
                        investment_thesis = EXCLUDED.investment_thesis,
                        status = EXCLUDED.status,
                        notes = EXCLUDED.notes,
                        last_updated_date = EXCLUDED.last_updated_date,
                        updated_at = NOW()
                """)
                
                params = {
                    'name': row.get('name'),
                    'legal_name': row.get('legal_name') or None,
                    'region': row.get('region') or None,
                    'country': row.get('country') or None,
                    'principal_family': row.get('principal_family') or None,
                    'headquarters_address': row.get('headquarters_address') or None,
                    'city': row.get('city') or None,
                    'state_province': row.get('state_province') or None,
                    'postal_code': row.get('postal_code') or None,
                    'main_phone': row.get('main_phone') or None,
                    'main_email': row.get('main_email') or None,
                    'website': row.get('website') or None,
                    'linkedin': row.get('linkedin') or None,
                    'estimated_wealth': row.get('estimated_wealth') or None,
                    'investment_focus': investment_focus,
                    'sectors_of_interest': sectors_of_interest,
                    'check_size_range': row.get('check_size_range') or None,
                    'investment_thesis': row.get('investment_thesis') or None,
                    'status': row.get('status') or 'Active',
                    'notes': row.get('notes') or None,
                    'first_researched_date': date.today(),
                    'last_updated_date': date.today()
                }
                
                with engine.connect() as conn:
                    conn.execute(sql, params)
                    conn.commit()
                
                print(f"  ✅ Row {i}: {row.get('name')}")
                imported += 1
                
            except Exception as e:
                error_msg = f"Row {i} ({row.get('name', 'Unknown')}): {str(e)}"
                print(f"  ❌ {error_msg}")
                errors.append(error_msg)
    
    print(f"\n{'='*80}")
    print(f"IMPORT COMPLETE")
    print(f"{'='*80}")
    print(f"✅ Successfully imported: {imported}")
    if errors:
        print(f"❌ Errors: {len(errors)}")
        print("\nError details:")
        for error in errors:
            print(f"  - {error}")
    
    return imported, errors


def main():
    """Main entry point."""
    if len(sys.argv) > 1:
        csv_file = sys.argv[1]
    else:
        csv_file = "data/family_offices_template.csv"
    
    if not Path(csv_file).exists():
        print(f"❌ CSV file not found: {csv_file}")
        print("\nUsage: python import_from_csv.py [csv_file]")
        print("Default: data/family_offices_template.csv")
        sys.exit(1)
    
    print("="*80)
    print("FAMILY OFFICE CSV IMPORT")
    print("="*80)
    print()
    
    imported, errors = import_family_offices(csv_file)
    
    if imported > 0:
        print(f"\n🎉 Import successful! {imported} family offices loaded.")
        print("\nQuery your data:")
        print("  docker-compose exec postgres psql -U nexdata -d nexdata")
        print("  SELECT name, city, investment_focus FROM family_offices;")
    else:
        print("\n⚠️  No records imported. Check errors above.")
        sys.exit(1)


if __name__ == "__main__":
    main()

