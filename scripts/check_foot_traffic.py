"""Check foot traffic database tables and status."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ.setdefault("DATABASE_URL", "postgresql://nexdata:nexdata_dev_password@localhost:5433/nexdata")

from sqlalchemy import text
from app.core.database import get_engine

def check_tables():
    engine = get_engine()
    with engine.connect() as conn:
        # Check for foot traffic tables
        print("\n=== FOOT TRAFFIC DATABASE STATUS ===\n")
        
        # Check if tables exist
        tables_to_check = [
            'locations',
            'foot_traffic_observations',
            'location_metadata',
            'foot_traffic_collection_jobs'
        ]
        
        for table_name in tables_to_check:
            exists = conn.execute(text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = '{table_name}'
                )
            """)).fetchone()[0]
            
            if exists:
                count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()[0]
                print(f"[OK] {table_name}: {count} records")
            else:
                print(f"[MISSING] {table_name}: TABLE NOT CREATED")
        
        # Show sample locations if any
        locations_exist = conn.execute(text("""
            SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'locations')
        """)).fetchone()[0]
        
        if locations_exist:
            count = conn.execute(text("SELECT COUNT(*) FROM locations")).fetchone()[0]
            if count > 0:
                print(f"\n=== SAMPLE LOCATIONS ({count} total) ===\n")
                rows = conn.execute(text("""
                    SELECT brand_name, city, state, COUNT(*) as loc_count
                    FROM locations
                    GROUP BY brand_name, city, state
                    ORDER BY loc_count DESC
                    LIMIT 10
                """)).fetchall()
                for r in rows:
                    print(f"  {r[0]} - {r[1]}, {r[2]}: {r[3]} locations")
        
        print("\n=== API STATUS ===\n")
        from app.core.config import get_settings
        settings = get_settings()
        
        print(f"SafeGraph API Key: {'Configured' if settings.get_safegraph_api_key() else 'NOT SET'}")
        print(f"Foursquare API Key: {'Configured' if settings.get_foursquare_api_key() else 'NOT SET'}")
        print(f"Placer API Key: {'Configured' if settings.get_placer_api_key() else 'NOT SET'}")
        print(f"Google Scraping: {'Enabled' if settings.is_google_scraping_enabled() else 'Disabled'}")
        print()

if __name__ == "__main__":
    check_tables()
